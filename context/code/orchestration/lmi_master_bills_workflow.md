# LMI discounts in master bills

How state LMI implementations are built directly into `comb_bills_year_target`. The current master-bill dispatch supports MD, NY, and RI; this document covers the NY fixed-credit path and the MD OHEP path in detail.

---

## Maryland OHEP (MEAP + EUSP)

### What it does

`utils/post/apply_md_ohep_to_master_bills.py` models the current FY26 OHEP grants. It does not implement Maryland's forthcoming Limited Income Mechanism (LIM).

For every building, it:

1. Computes FPL% from ResStock occupants and CPI-inflated representative income.
2. Assigns OHEP Poverty Level 1–5; Levels 6–7 are outside the modeled metadata scope.
3. Maps ResStock heating fuel to the OHEP matrix categories, treating heat pumps as electric heat.
4. Reads annual electric use from the consolidated `load_curve_annual` parquet and assigns the EUSP kWh band.
5. Looks up MEAP by `(level, heating fuel)` and EUSP by `(level, heating fuel, kWh band)`.
6. Samples one or more participation scenarios.
7. Applies EUSP to electric and MEAP to the heating-fuel bill. Electric-heat MEAP stacks with EUSP on electric.
8. Allocates each annual grant proportionally across the applicable fuel's monthly bills and rebuilds Annual from Jan–Dec.

### Integrated Prefect invocation

From `rate_design/hp_rates/`:

```bash
just s md build-master-bills-prefect <batch> \
  --calculate-lmi \
  --lmi-participation-rates 1.0 0.4 \
  --lmi-participation-mode weighted \
  --lmi-calculation-type monthly
```

One call processes every completed `{scenario}_{stage}` segment discovered in the batch. Each rate creates its own `{pct}`-suffixed LMI columns, so p100 and p40 can coexist in the same table.

The command rebuilds and rewrites the batch's master-bill parquet prefixes; it does not modify CAIRO run outputs. Re-running without `--calculate-lmi` reconstructs the base master tables without LMI columns.

### Prefect output location

MD OHEP is applied after the builder writes the per-utility table and before it writes the Hive-partitioned combined table. Consequently, the integrated LMI columns are in:

```
s3://data.sb/switchbox/cairo/outputs/hp_rates/md/all_utilities/<batch>/<segment>/comb_bills_year_target/
```

The per-utility path written by the same run does not contain them. Use `all_utilities` for MD OHEP analysis.

### MD output columns

Shared profile columns:

- `ohep_poverty_level`, `primary_heating_fuel`, `annual_electric_kwh`, `eusp_kwh_band`
- `elec_lmi_tier`, `gas_lmi_tier`, `oil_lmi_tier`, `propane_lmi_tier`
- `is_lmi_elec`, `is_lmi_gas`, `is_lmi_oil`, `is_lmi_propane`, `is_lmi_any`
- `has_unmodeled_meap_fuel`

For each participation suffix `{pct}`:

- `{fuel}_total_bill_lmi_{pct}` for electric, gas, oil, and propane
- `energy_total_bill_lmi_{pct}`
- `applied_discount_{fuel}_{pct}`

`is_lmi_elec` means that a modeled grant lands on electric: positive EUSP, or positive MEAP for electric heat. The other fuel flags indicate where MEAP lands. `is_lmi_any` identifies the eligible participation pool.

### MD annual-grant allocation

For each participating building and applicable fuel:

```
annual bill          = sum(Jan..Dec base bills)
annual credit        = EUSP, MEAP, or EUSP + MEAP
fraction remaining   = max(0, 1 - annual credit / annual bill)
discounted month     = base month * fraction remaining
discounted Annual    = sum(Jan..Dec discounted months)
```

This preserves the full grant when the annual bill can absorb it, avoids negative monthly bills, and avoids losing grant dollars in low-bill months. It is an analytical allocation of an annual grant, not a statement about OHEP's operational posting cadence.

### MD implementation files

- `utils/post/data/md_ohep_benefits.yaml` — FY26 level boundaries, kWh bands, and annual grant matrices.
- `utils/post/lmi_common.py` — FPL, CPI, tier/band expressions, matrix flattening, and participation helpers.
- `utils/post/apply_md_ohep_to_master_bills.py` — profile construction, fuel routing, grant allocation, validation, standalone CLI, and Hive writer.
- `utils/post/build_master_bills.py` — legacy master-bill dispatch.
- `utils/post/build_master_bills_prefect.py` — current batch/segment dispatch.
- `tests/test_md_ohep_discounts.py` — focused MD behavior and builder-dispatch tests.

### MD validation and limitations

The application validates non-null/nonnegative LMI bills, discounted bills not exceeding base bills, Annual rows equal to monthly sums, energy totals equal to fuel totals, and p100 electric application matching electric eligibility. Tests cover boundary assignments, matrix cells, fuel routing, stacking, toggles, proportional allocation, nonparticipants, participation, and both builders.

Known exclusions:

- Levels 6–7 cannot be assigned reliably from current ResStock fields.
- Wood/coal is identified and receives EUSP when eligible, but MEAP cannot be applied because master bills have no wood/coal bill column.
- The real-world OHEP take-up rate remains unknown; p100 and parameterized participation scenarios are modeling inputs.
- LIM, arrearage assistance, USPP, and private charity are not included.

See [Maryland low-income / energy affordability programs](../../domain/charges/lmi_discounts_in_md.md) for program sources and policy limitations.

---

## New York EAP / EEAP

---

## What it does

The script builds the Hive-partitioned master bills table, assigns EAP tiers to each building using ResStock metadata, applies per-utility fixed monthly credits, validates the result, and writes the final dataset to the standard master-bills output path.

For analysis, it is commonly run **twice per run pair**: once at 100% participation (p100) and once at 40% (p40), typically using distinct batch names so each output lands in its own master-bills directory.

---

## Columns added

Each LMI-enabled build adds rate-specific columns (where `{pct}` = `int(participation_rate * 100)`):

| Column                      | Type    | Description                                                                                                                 |
| --------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------- |
| `lmi_tier`                  | Int32   | Raw EAP tier (0 = ineligible, 1–7 = eligible). Same across p100 and p40 because it reflects eligibility, not participation. |
| `is_lmi`                    | Bool    | `lmi_tier > 0`. Convenience flag for filtering.                                                                             |
| `applied_discount_{pct}`    | Bool    | True if the discount was actually applied (depends on participation sampling). At p100, identical to `is_lmi`.              |
| `elec_total_bill_lmi_{pct}` | Float64 | `max(0, elec_total_bill - monthly_credit)` for monthly rows; `sum(Jan..Dec clamped)` for the Annual row.                    |
| `gas_total_bill_lmi_{pct}`  | Float64 | Same logic for gas.                                                                                                         |

Each LMI-enabled output contains the base master-bills columns plus the LMI columns for that participation scenario.

---

## Invocation

### Via Justfile (preferred)

From `rate_design/hp_rates/`:

```bash
just s ny build-master-bills-with-lmi <batch> <run_delivery> <run_supply> [participation_rate] [participation_mode] [seed] [calculation_type]
```

Example — run p100 then p40 for all 4 run pairs:

```bash
cd rate_design/hp_rates
for d s in 1 2 3 4 5 6 7 8; do
  just s ny build-master-bills-with-lmi-p100 ny_20260307_r1-8_gascalcfix $d $s
  just s ny build-master-bills-with-lmi-p40 ny_20260307_r1-8_gascalcfix $d $s
done
```

### Direct CLI

```bash
uv run python utils/post/build_master_bills.py \
  --state ny \
  --batch <batch> \
  --run-delivery <d> \
  --run-supply <s> \
  --path-resstock-release "s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb" \
  --path-load-curves-local "<local_resstock_root>" \
  --calculate-lmi \
  --lmi-fpl-year 2025 \
  --lmi-cpi-s3-path "s3://data.sb/fred/cpi/" \
  --lmi-participation-rate 1.0 \
  --lmi-participation-mode weighted \
  --lmi-seed 42 \
  --lmi-calculation-type budget
```

---

## Re-runs

The build is deterministic for a fixed set of inputs, parameters, and seed. Re-running the same command rewrites the master-bills output for that batch/run pair.

---

## Tier assignment pipeline

1. Load ResStock `metadata-sb.parquet` and `utility_assignment.parquet` for the state/upgrade.
2. Per utility: parse occupants, inflate income from 2019 dollars to `--fpl-year` using CPI, compute FPL% and SMI%.
3. Apply `assign_ny_tier_expr` from `lmi_common.py` (tiers 1–4 from FPL/vulnerability, tiers 6–7 from SMI). Tier 5 is unreachable — see Known Limitations.
4. Participation sampling:
   - **p100**: all eligible buildings participate.
   - **p40 weighted**: lower-income buildings are more likely selected (weight = 1/FPL%).

---

## Credit application

- Credits come from `utils/post/data/ny_eap_credits.yaml`, loaded via `get_ny_eap_credits_df()`.
- Electric credits join on `(sb.electric_utility, lmi_tier)`, gas on `(sb.gas_utility, lmi_tier)`.
- Row count guards after each join prevent silent row duplication.
- Monthly rows: `max(0, bill - credit)`. Annual row: sum of 12 clamped monthly values (not `max(0, annual_bill - 12 * credit)`).
- Unpublished credits (`null` in YAML for certain EEAP tiers) are treated as $0 with a warning logged.

---

## Validation checks (in-script)

The `_validate` function runs before writing and raises `AssertionError` on failure:

- No nulls in any new column.
- All discounted bills ≥ 0.
- Non-discounted buildings: `_lmi` bill == original bill (within 1e-6).
- Discounted ≤ original for all rows.
- `is_lmi` == (`lmi_tier` > 0) for all rows.
- At p100: `applied_discount` == `is_lmi` for all rows.
- Annual discounted bill == sum of 12 monthly discounted bills (within 1e-6).
- Achieved participation rate within 2pp of target (exact at p100).

---

## Companion validation scripts

- `utils/post/validate_lmi_electric_discounts.py` — EDA histograms, expected-vs-actual credit checks, cross-run (p100 vs p40) tier consistency, source column integrity.
- `utils/post/validate_lmi_gas_discounts.py` — Same for gas discounts.

Both save plots to `dev_plots/` and print summary tables to stderr.

---

## Known limitations

- **Tier 5 is unreachable** (tracked in RDP-158). EEAP Tier 5 requires area median income (AMI), but the script currently uses state median income (SMI) for all territories. In NYC / Nassau County (where AMI is significantly higher than SMI), this means some households that should qualify for Tier 5 are instead assigned Tier 6 or 7 — receiving smaller or no discounts. Implementing AMI would increase discounts for those areas.
- **Unpublished EEAP credits**. Several utility/tier combinations have `null` in `ny_eap_credits.yaml` because EEAP amounts have not yet been published (e.g., NiMo tiers 6–7, CenHud tier 6, KEDLI/KEDNY tiers 6–7, NFG tiers 6–7). These are treated as $0 and logged as warnings.
- **Annual row epsilon**. The Annual discounted bill is the sum of 12 clamped monthly values. For non-discounted buildings, this can differ from the original Annual row by up to ~4e-12 due to float accumulation. This is harmless.

---

## Related context

- `context/domain/charges/lmi_discounts_in_ny.md` — EAP/EEAP program structure, tier definitions, credit amounts.
- `context/code/data/resstock_lmi_metadata_guide.md` — ResStock columns used for tier assignment.
- `utils/post/lmi_common.py` — Shared helpers: tier assignment expressions, credit loading, participation sampling.
- `utils/post/data/ny_eap_credits.yaml` — Per-utility, per-tier fixed monthly credit amounts.
