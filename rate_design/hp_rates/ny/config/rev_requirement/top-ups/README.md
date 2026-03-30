# NY revenue-requirement top-ups

This directory contains Genability/Arcadia tariff snapshots and derived charge data
used to compute delivery and supply revenue-requirement top-ups for all seven NY
electric utilities: CenHud, ConEd, National Grid (NiMo), NYSEG, O&R, PSEG-LI, and
RG&E.

## Directory layout

```
top-ups/
├── tariffs_by_utility.yaml                   # Which Genability tariff to fetch per utility
├── default_tariffs/
│   └── <key>_default_<date>.json             # Full Genability tariff JSON snapshot
├── charge_decisions/
│   └── <key>_charge_decisions.json           # Per-charge classification (add_to_drr, etc.)
└── monthly_rates/
    └── <key>_monthly_rates_<year>.yaml       # Monthly $/kWh rates for classified charges
```

## How these are produced

### 1. Fetch the base tariff snapshot

`utils/pre/rev_requirement/fetch_electric_tariffs_genability.py` downloads the full Genability tariff
JSON for each utility listed in `tariffs_by_utility.yaml`. It takes an `--effective-date`
that controls which tariff version you get — all charges effective on that date will be
included. See `context/domain/tariff_structure_and_genability.md` for details on how
Genability versioning, riders, and variable rates work.

```bash
UTILITY=coned just s ny fetch-genability-tariffs
```

Output goes to `default_tariffs/`.

### 2. Classify each charge (manual research)

The tariff snapshot contains dozens of rate line items (delivery charges, surcharges,
riders, supply components, taxes). Each one needs to be classified by whether it
belongs in the delivery revenue requirement, the supply revenue requirement, or should
be excluded from the BAT analysis.

This classification was done through extensive manual research documented in
`context/methods/bat_mc_residual/ny_residential_charges_in_bat.md`. The result is one
`<key>_charge_decisions.json` per utility in `charge_decisions/`, where every
`tariffRateId` is mapped to a `decision`:

- **`add_to_drr`** — volumetric surcharge that should be "topped up" into the delivery revenue requirement (e.g. SBC, CES Delivery, DLM Surcharge)
- **`add_to_srr`** — supply-side surcharge for the supply revenue requirement (e.g. CES Supply, MFC, bundled commodity)
- **`already_in_drr`** — base delivery rate already covered by the rate-case revenue requirement (e.g. core delivery $/kWh, customer charge)
- **`exclude_trueup`** — cost reconciliation or revenue true-up (uniform $/kWh noise): MAC, RAM, DSA, RDM, transition charges, GRT, PILOTs, etc.
- **`exclude_negligible`** — structurally a cross-subsidy but negligible in magnitude (e.g. Earnings Adjustment Mechanism)
- **`exclude_expired`** — stale or expired charge (e.g. Tax Sur-Credit)
- **`exclude_zonal`** — zonal duplicate or VRK overlap duplicate excluded solely to prevent double-counting (e.g. ConEd MSC Zone I when Zone H is the representative, NiMo MFC entries whose VRK duplicates supply commodity rates)
- **`exclude_percentage`** — QUANTITY/% charge the pipeline cannot process; `fetch_monthly_rates.py` only handles $/kWh, $/month, and $/day (e.g. GRT when QUANTITY type, CBC, PSEG-LI Shoreham SPT, NY State Assessment)
- **`exclude_redundant`** — structurally redundant charge (e.g. minimum charge / bill floor that rarely binds and is redundant with the customer charge)
- **`exclude_eligibility`** — eligibility-gated or optional charge; $0 for the default residential customer (e.g. low-income discounts, solar CBC, agricultural discounts, GreenUp)

### 3. Fetch actual monthly rates

`utils/pre/rev_requirement/fetch_monthly_rates.py` reads the charge decisions file, then hits the
Genability API once per month to get the actual $/kWh rate for every classified
CONSUMPTION_BASED charge. It handles rider fallback, variable rate resolution, and
tariffRateId version drift.

The output YAML groups charges by decision (`add_to_drr`, `add_to_srr`,
`already_in_drr`, `excluded`). Each active decision section carries a
`rate_structure` discriminator that tells consumer code how to interpret the
charge data:

- **`flat`** — standard `monthly_rates: {month: rate}` per charge. Used by
  NiMo, NYSEG, RG&E, CenHud (all sections) and by ConEd/O&R/PSEG-LI for
  `add_to_drr` surcharges.
- **`seasonal_tiered`** — tiered rates with per-season monthly values. ConEd
  and O&R delivery rates have a 250 kWh first-tier / unlimited second-tier
  structure where the summer second tier carries a premium. Each charge has
  `tiers: [{upper_limit_kwh, monthly_rates: {season: {month: rate}}}]`.
- **`seasonal_tou`** — time-of-use rates with per-season×TOU-period monthly
  values. PSEG-LI delivery and supply have on-peak/off-peak × summer/winter
  structure. Each charge has `monthly_rates: {season_tou: {month: rate}}`.

Non-tiered charges within a `seasonal_tiered` or `seasonal_tou` section
(e.g. customer charge, MFC) use the simple flat schema.

```bash
UTILITY=coned just s ny fetch-monthly-rates
```

Output goes to `monthly_rates/`. The year defaults to `MONTHLY_RATES_YEAR` (2025).

### 4. Compute topped-up revenue requirement

The monthly rates YAML feeds into `utils/pre/rev_requirement/compute_rr.py`, which reads
the `add_to_drr` and `add_to_srr` sections. For sections with `rate_structure: flat`,
it multiplies each day-weighted average monthly rate by EIA-861 residential kWh to get
an annual budget, then adds it to the rate-case delivery revenue requirement.

Sections with non-flat `rate_structure` (e.g. PSEG-LI's `seasonal_tou` supply charges)
are **skipped with a WARNING** — use the `supply_base_overrides.yaml` mechanism for
those utilities instead. This avoids double-counting TOU charges that can't be naively
summed as flat $/kWh.

```bash
UTILITY=coned just s ny compute-rr
```

## Handling zonal duplicates and seasonal variants

Genability tariffs often contain **multiple tariffRateIds for the same conceptual
charge**. These fall into two categories:

### Zonal duplicates (exclude)

Zone-specific variants (e.g. ConEd supply for Zones H/I/J, NiMo 6 service areas,
NYSEG Regular/West/LHV) where each customer is in exactly one zone. Summing all
zones would overcount. The `classify_charges.py` script handles these automatically:

- **Phase 2 (zonal supply dedup)**: Uses `variableRateKey` prefixes to identify
  zonal supply entries; keeps the representative zone (H for ConEd, Central for
  NiMo, Regular for NYSEG), marks others `exclude_zonal`.
- **Phase 4 (post-classification dedup)**: Catches remaining zone duplicates
  where entries share the same `(rate_name, decision, charge_class,
  rate_group_name, season, tou)` tuple. This handles ConEd's 3 Summer Rate
  entries (zones H/I/J) → keeps 1, marks 2 as `exclude_zonal`.

### Seasonal/TOU variants (keep)

Entries with distinct season or TOU metadata (e.g. ConEd's Summer Rate vs Winter
Rate, PSEG-LI's 4 season×TOU delivery entries) represent genuinely different rate
periods. The classify pipeline preserves these, and `fetch_monthly_rates.py` merges
them into a single combined charge entry in the YAML output:

- **`seasonal_tiered`**: ConEd/O&R delivery — Summer Rate + Winter Rate entries
  become a single `core_delivery_rate` with per-season per-tier monthly rates.
- **`seasonal_tou`**: PSEG-LI delivery and supply — 4 season×TOU entries become
  a single charge with `summer_on_peak`, `summer_off_peak`, `winter_on_peak`,
  `winter_off_peak` monthly rates.

### Other multi-entry cases

| Utility | Charge                       | Entries | Verdict                                          |
| ------- | ---------------------------- | ------: | ------------------------------------------------ |
| CenHud  | Merchant Function Charge     |       3 | Distinct components (allocation/base/admin)      |
| NiMo    | Arrears / COVID forgiveness  |       2 | Temporal phases (Phase 1 + Phase 2)              |
| NYSEG   | Arrears / COVID forgiveness  |       2 | Temporal phases (Phase 1 + Phase 2)              |
| RGE     | CES Supply Surcharge         |       2 | Distinct tiers (Tier 1 + Tier 2)                 |
| PSEG-LI | Securitization Charge/Offset |       2 | Charge + equal-and-opposite offset (nets to \$0) |

## Adding a new state

To replicate this for a new state/utility:

1. **Add the utility to `utils/utility_codes.py`** with its EIA utility ID so the fetch script can resolve it to a Genability LSE.
2. **Add an entry to `tariffs_by_utility.yaml`** in the state's `rev_requirement/top-ups/` directory (use `default` to get the residential default tariff, or a specific `masterTariffId`).
3. **Fetch the base tariff**: run `fetch-genability-tariffs` with the appropriate effective date.
4. **Classify every charge** by reading through the tariff JSON and regulatory filings. This is unavoidable manual research — see `context/methods/bat_mc_residual/ny_residential_charges_in_bat.md` for the kind of analysis required. Document the research in a `context/methods/bat_mc_residual/<state>_residential_charges_in_bat.md` file, then encode the decisions in a `<key>_charge_decisions.json`.
5. **Fetch the monthly rates**: run `fetch-monthly-rates` for the utility.
6. **Compute the revenue requirement**: run `compute-rr` to produce the topped-up `rev_requirement/<utility>.yaml`.
