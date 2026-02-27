# RI revenue-requirement top-ups

This directory contains Genability/Arcadia tariff snapshots and derived charge data
used to compute delivery and supply revenue-requirement top-ups for Rhode Island
Energy (RIE).

## Directory layout

```
top-ups/
├── tariffs_by_utility.yaml                   # Which Genability tariff to fetch per utility
├── default_tariffs/
│   └── rie_default_<date>.json               # Full Genability tariff JSON snapshot
├── charge_decisions/
│   └── rie_charge_decisions.json             # Per-charge classification (add_to_drr, etc.)
└── monthly_rates/
    └── rie_monthly_rates_<year>.yaml         # Monthly $/kWh rates for classified charges
```

## How these are produced

### 1. Fetch the base tariff snapshot

`utils/pre/fetch_electric_tariffs_genability.py` downloads the full Genability tariff
JSON for RIE as listed in `tariffs_by_utility.yaml`. It takes an `--effective-date`
that controls which tariff version you get — all charges effective on that date will be
included. See `context/domain/tariff_structure_and_genability.md` for details on how
Genability versioning, riders, and variable rates work.

```bash
UTILITY=rie just s ri fetch-genability-tariffs
```

Output goes to `default_tariffs/`.

### 2. Classify each charge (manual research)

The tariff snapshot contains dozens of rate line items (delivery charges, surcharges,
riders, supply components, taxes). Each one needs to be classified by whether it
belongs in the delivery revenue requirement, the supply revenue requirement, or should
be excluded from the BAT analysis.

This classification was done through extensive manual research documented in
`context/domain/ri_residential_charges_in_bat.md`. The result is
`rie_charge_decisions.json` in `charge_decisions/`, where every `tariffRateId` is
mapped to a `decision`:

- **`add_to_drr`** — volumetric surcharge that should be "topped up" into the delivery revenue requirement (e.g. Storm Fund, Net Metering, Energy Efficiency Programs)
- **`add_to_srr`** — supply-side surcharge for the supply revenue requirement (e.g. Standard Offer Service, Renewable Energy Standard)
- **`already_in_drr`** — base delivery rate already covered by the rate-case revenue requirement (e.g. core delivery $/kWh, customer charge)
- **`exclude`** — true-up mechanisms, tax surcharges, or pass-through adjustments that should not be in the BAT (e.g. GET, transition charge)
- **`skip`** — not applicable

### 3. Fetch actual monthly rates

`utils/pre/fetch_monthly_rates.py` reads the charge decisions file, then hits the
Genability API once per month to get the actual $/kWh rate for every classified
CONSUMPTION_BASED charge. It handles rider fallback, variable rate resolution, and
tariffRateId version drift. Each charge in the output YAML is labeled with its
`decision`, so downstream consumers (e.g. `compute_rr.py`) can filter for
`add_to_drr` or `add_to_srr` as needed.

```bash
UTILITY=rie just s ri fetch-monthly-rates
```

Output goes to `monthly_rates/`. The year defaults to `MONTHLY_RATES_YEAR` (2025).

### 4. Compute topped-up revenue requirement

The monthly rates YAML feeds into `utils/pre/compute_rr.py`, which filters for
`add_to_drr` and `add_to_srr` charges, multiplies each day-weighted average monthly
rate by EIA-861 residential kWh to get an annual budget, then adds it to the rate-case
delivery revenue requirement. The output lives in the parent `rev_requirement/`
directory (`rev_requirement/rie.yaml`).

```bash
UTILITY=rie just s ri compute-rr
```
