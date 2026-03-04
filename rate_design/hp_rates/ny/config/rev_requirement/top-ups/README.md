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
`context/domain/ny_residential_charges_in_bat.md`. The result is one
`<key>_charge_decisions.json` per utility in `charge_decisions/`, where every
`tariffRateId` is mapped to a `decision`:

- **`add_to_drr`** — volumetric surcharge that should be "topped up" into the delivery revenue requirement (e.g. SBC, CES Delivery, DLM Surcharge)
- **`add_to_srr`** — supply-side surcharge for the supply revenue requirement (e.g. CES Supply, MFC, bundled commodity)
- **`already_in_drr`** — base delivery rate already covered by the rate-case revenue requirement (e.g. core delivery $/kWh, customer charge)
- **`exclude`** — true-up mechanisms, tax surcharges, or pass-through adjustments that should not be in the BAT (e.g. RDM, MAC, GRT)
- **`skip`** — not applicable (e.g. solar-only credits)

### 3. Fetch actual monthly rates

`utils/pre/rev_requirement/fetch_monthly_rates.py` reads the charge decisions file, then hits the
Genability API once per month to get the actual $/kWh rate for every classified
CONSUMPTION_BASED charge. It handles rider fallback, variable rate resolution, and
tariffRateId version drift. Each charge in the output YAML is labeled with its
`decision`, so downstream consumers (e.g. `compute_rr.py`) can filter for
`add_to_drr` or `add_to_srr` as needed.

```bash
UTILITY=coned just s ny fetch-monthly-rates
```

Output goes to `monthly_rates/`. The year defaults to `MONTHLY_RATES_YEAR` (2025).

### 4. Compute topped-up revenue requirement

The monthly rates YAML feeds into `utils/pre/rev_requirement/compute_rr.py`, which filters for
`add_to_drr` and `add_to_srr` charges, multiplies each day-weighted average monthly
rate by EIA-861 residential kWh to get an annual budget, then adds it to the rate-case
delivery revenue requirement. The output lives in the parent `rev_requirement/`
directory (e.g. `rev_requirement/<utility>.yaml`).

```bash
UTILITY=coned just s ny compute-rr
```

## Handling multiple entries for the same charge (zonal duplicates)

Genability tariffs often contain **multiple tariffRateIds for the same conceptual
charge**, each representing a different pricing zone or service territory variant.
When this happens, the monthly rates YAML will have entries like:

```yaml
supply_commodity_bundled_1:
  tariff_rate_id: 20918171
  master_charge: Supply commodity (bundled)
  decision: add_to_srr
  monthly_rates: { 2025-01: 0.0968, ... }

supply_commodity_bundled_2:
  tariff_rate_id: 20918172
  master_charge: Supply commodity (bundled)
  decision: add_to_srr
  monthly_rates: { 2025-01: 0.1020, ... }

supply_commodity_bundled_3:
  tariff_rate_id: 20918173
  master_charge: Supply commodity (bundled)
  decision: add_to_srr
  monthly_rates: { 2025-01: 0.1247, ... }
```

`compute_rr.py` emits a **WARNING** whenever multiple charges share the same
`master_charge` and `add_to_*` decision, so you'll see it every time the pipeline
runs. The warning doesn't auto-fix anything — it requires human review to decide
whether the entries are **true duplicates** (zonal variants that would overcount)
or **distinct additive components** (separate line items every customer pays).

### How to tell the difference

**Zonal duplicates** (each customer is in exactly one zone; summing overcounts):

- The `variableRateKey` in charge_decisions.json contains a geographic suffix:
  `marketSupplyChargeResidentialZoneH`, `...ZoneI`, `...ZoneJ` (ConEd NYISO zones),
  `supplyChargeSC1West`, `...SC1LHV` (NYSEG pricing zones),
  `electricSupplyChargeSC1Central`, `...SC1Capital` (NiMo service areas).
- Rates are similar in magnitude but not identical (they reflect local cost
  differences within the utility's territory).
- Multiplying all of them by total utility kWh produces a 2–3x overcount.

**Distinct additive components** (every customer pays all of them; summing is correct):

- The `variableRateKey` names reflect different functions, not geography:
  `AllocationMFCLostRevenueSC1`, `BaseMFCSupplyChargeSC1`, `MFCAdminChargeSC1`
  (CenHud's three MFC subcomponents).
- Rates may differ by orders of magnitude (one is tiny, another is the bulk).
- Regulatory filings confirm they are separate line items on the bill.

**Temporal phases** (overlapping but genuinely concurrent surcharges):

- Entries like `Arrears / COVID forgiveness` Phase 1 and Phase 2 may overlap
  in time (both billed Jan–Jul) but are separate PSC-authorized recovery
  surcharges. Each recovers a different pool of costs.
- Key signal: one phase changes rate or expires independently of the other
  (e.g. Phase 1 drops from $0.0012 to $0.00087 in August while Phase 2 expires
  entirely in July).

**CES tier splits** (distinct regulatory tiers, additive):

- RGE's two CES Supply Surcharge entries ($0.00103 and $0.00299) are Tier 1 and
  Tier 2 of the Clean Energy Standard. Their sum ($0.00402) matches the
  predecessor single-rate entry. Both are billed to every customer.

### What to do when you find duplicates

For confirmed zonal duplicates:

1. **Pick a representative zone.** Choose the zone that covers the largest share
   of the utility's residential load. Examples: Zone H (NYC) for ConEd, Regular
   zone for NYSEG, Central for NiMo.
2. **Exclude the others in charge_decisions.json.** Change the decision from
   `add_to_srr` (or `add_to_drr`) to `exclude` and add a `_note` explaining
   the choice:
   ```json
   "20918172": {
     "master_charge": "Supply commodity (bundled)",
     "decision": "exclude",
     "variableRateKey": "marketSupplyChargeResidentialZoneI",
     "_note": "Zonal duplicate — Zone H kept as representative; Zone I excluded to avoid overcount"
   }
   ```
3. **Update the monthly rates YAML** to match (change the `decision` field for
   the same entries).
4. **Regenerate** by re-running `compute-rr`.

Alternatively, if zone-level kWh data is available, you could population-weight
the rates. In practice, the rates within a utility are close enough (~5–15% spread)
that picking the largest zone introduces minimal error.

### Cases we've resolved

| Utility | Charge                       | Entries | Verdict                                           | Resolution                                  |
| ------- | ---------------------------- | ------: | ------------------------------------------------- | ------------------------------------------- |
| ConEd   | Supply commodity (bundled)   |       3 | Zonal (Zones H/I/J)                               | Keep H, exclude I + J (J is LIPA territory) |
| NYSEG   | Supply commodity (bundled)   |       3 | Zonal (Regular/West/LHV)                          | Keep Regular, exclude West + LHV            |
| NiMo    | Merchant Function Charge     |      27 | Zonal (6 zones × 2 rate components + 3 non-zonal) | Keep Central zone + non-zonal components    |
| CenHud  | Merchant Function Charge     |       3 | Distinct components (allocation/base/admin)       | Keep all 3 — not duplicates                 |
| NiMo    | Arrears / COVID forgiveness  |       2 | Temporal phases (Phase 1 + Phase 2)               | Keep both — genuinely concurrent            |
| NYSEG   | Arrears / COVID forgiveness  |       2 | Temporal phases (Phase 1 + Phase 2)               | Keep both — genuinely concurrent            |
| RGE     | CES Supply Surcharge         |       2 | Distinct tiers (Tier 1 + Tier 2)                  | Keep both — additive                        |
| PSEG-LI | Securitization Charge/Offset |       2 | Charge + equal-and-opposite offset                | Keep both — they net to $0                  |

## Adding a new state

To replicate this for a new state/utility:

1. **Add the utility to `utils/utility_codes.py`** with its EIA utility ID so the fetch script can resolve it to a Genability LSE.
2. **Add an entry to `tariffs_by_utility.yaml`** in the state's `rev_requirement/top-ups/` directory (use `default` to get the residential default tariff, or a specific `masterTariffId`).
3. **Fetch the base tariff**: run `fetch-genability-tariffs` with the appropriate effective date.
4. **Classify every charge** by reading through the tariff JSON and regulatory filings. This is unavoidable manual research — see `context/domain/ny_residential_charges_in_bat.md` for the kind of analysis required. Document the research in a `context/domain/<state>_residential_charges_in_bat.md` file, then encode the decisions in a `<key>_charge_decisions.json`.
5. **Fetch the monthly rates**: run `fetch-monthly-rates` for the utility.
6. **Compute the revenue requirement**: run `compute-rr` to produce the topped-up `rev_requirement/<utility>.yaml`.
