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

`utils/pre/fetch_electric_tariffs_genability.py` downloads the full Genability tariff
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

`utils/pre/fetch_monthly_rates.py` reads the charge decisions file, then hits the
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

The monthly rates YAML feeds into `utils/pre/compute_rr.py`, which filters for
`add_to_drr` and `add_to_srr` charges, multiplies each day-weighted average monthly
rate by EIA-861 residential kWh to get an annual budget, then adds it to the rate-case
delivery revenue requirement. The output lives in the parent `rev_requirement/`
directory (e.g. `rev_requirement/<utility>.yaml`).

```bash
UTILITY=coned just s ny compute-rr
```

## Adding a new state

To replicate this for a new state/utility:

1. **Add the utility to `utils/utility_codes.py`** with its EIA utility ID so the fetch script can resolve it to a Genability LSE.
2. **Add an entry to `tariffs_by_utility.yaml`** in the state's `rev_requirement/top-ups/` directory (use `default` to get the residential default tariff, or a specific `masterTariffId`).
3. **Fetch the base tariff**: run `fetch-genability-tariffs` with the appropriate effective date.
4. **Classify every charge** by reading through the tariff JSON and regulatory filings. This is unavoidable manual research — see `context/domain/ny_residential_charges_in_bat.md` for the kind of analysis required. Document the research in a `context/domain/<state>_residential_charges_in_bat.md` file, then encode the decisions in a `<key>_charge_decisions.json`.
5. **Fetch the monthly rates**: run `fetch-monthly-rates` for the utility.
6. **Compute the revenue requirement**: run `compute-rr` to produce the topped-up `rev_requirement/<utility>.yaml`.
