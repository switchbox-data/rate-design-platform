# RI Genability tariff data

This directory contains Genability/Arcadia tariff snapshots and derived charge data
for Rhode Island Energy (RIE).

## Files

| File                                   | What it is                                                                                                                                    |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `rie_default_<date>.json`              | Full Genability tariff JSON, snapshotted at `<date>`                                                                                          |
| `rie_charge_decisions.json`            | Classification of every rate in the tariff: `add_to_drr`, `add_to_srr`, `already_in_drr`, `exclude`, `skip`                                   |
| `rie_monthly_rates.yaml`               | Monthly volumetric rates ($/kWh) for every classified charge, fetched from the API for each month in the requested range, labeled by decision |
| `rie_default_<date>_<start>_<end>.csv` | (Diagnostic) Tidy-long time series of all charges over a date range                                                                           |

## How these are produced

### 1. Fetch the base tariff snapshot

`utils/pre/fetch_electric_tariffs_genability.py` downloads the full Genability tariff
JSON for each utility listed in `genability_tariffs.yaml`. It takes an `--effective-date`
that controls which tariff version you get — all charges effective on that date will be
included. See `context/domain/tariff_structure_and_genability.md` for details on how
Genability versioning, riders, and variable rates work.

```bash
just -f rate_design/ri/hp_rates/config/tariffs/electric/Justfile fetch-genability 2025-01-01
```

### 2. Classify each charge (manual research)

The tariff snapshot contains dozens of rate line items (delivery charges, surcharges,
riders, supply components, taxes). Each one needs to be classified by whether it belongs
in the delivery revenue requirement, the supply revenue requirement, or should be excluded
from the BAT analysis.

This classification was done through extensive manual research documented in
`context/domain/ri_residential_charges_in_bat.md`. The result is
`rie_charge_decisions.json`, where every `tariffRateId` is mapped to a `decision`:

- **`add_to_drr`** — volumetric surcharge that should be "topped up" into the delivery revenue requirement (e.g. Infrastructure, Safety, and Reliability, Renewable Energy Growth Program)
- **`add_to_srr`** — supply-side surcharge for the supply revenue requirement (e.g. Last Resort Supply)
- **`already_in_drr`** — base delivery rate already covered by the rate-case revenue requirement (e.g. Distribution Charge, Customer Charge)
- **`exclude`** — true-up mechanisms, tax surcharges, or pass-through adjustments that should not be in the BAT (e.g. Energy Efficiency Program, Net Metering)
- **`skip`** — not applicable

### 3. Fetch actual monthly rates

`utils/pre/fetch_monthly_rates.py` reads the charge decisions file, then hits the
Genability API once per month to get the actual $/kWh rate for every classified
CONSUMPTION_BASED charge. It handles rider fallback, variable rate resolution, and
tariffRateId version drift. Each charge in the output YAML is labeled with its
`decision`, so downstream consumers (e.g. `compute_rr.py`) can filter for
`add_to_drr` or `add_to_srr` as needed.

```bash
just -f rate_design/ri/hp_rates/Justfile fetch-monthly-rates rie 2025-01 2025-12
```

Produces `rie_monthly_rates.yaml`.

### 4. Compute topped-up revenue requirement

The monthly rates YAML feeds into `utils/pre/compute_rr.py`, which filters for
`add_to_drr` charges, multiplies each monthly rate by EIA-861 residential kWh to get
an annual budget, then adds it to the rate-case revenue requirement. This is a
separate step not in this directory.

## Adding a new utility

To replicate this for a new utility:

1. **Add the utility to `utils/utility_codes.py`** with its EIA utility ID so the fetch script can resolve it to a Genability LSE.
2. **Add an entry to `genability_tariffs.yaml`** (use `default` to get the residential default tariff, or a specific `masterTariffId`).
3. **Fetch the base tariff**: run the `fetch-genability` recipe with the appropriate effective date.
4. **Classify every charge** by reading through the tariff JSON and regulatory filings. This is unavoidable manual research — see `context/domain/ri_residential_charges_in_bat.md` for the kind of analysis required (or `context/domain/ny_residential_charges_in_bat.md` for the NY equivalent). Document the research in a corresponding `context/domain/` file, then encode the decisions in a `<key>_charge_decisions.json`.
5. **Fetch the monthly rates**: run `fetch-monthly-rates` for the utility and month range.
6. **Compute the revenue requirement**: run `compute-rr` to produce the topped-up `rev_requirement/<utility>.yaml`.
