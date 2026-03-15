# LMI discounts in master bills

How NY LMI discounts are built directly into `comb_bills_year_target` by `utils/post/build_master_bills.py`.

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

- `context/domain/lmi_discounts_in_ny.md` — EAP/EEAP program structure, tier definitions, credit amounts.
- `context/tools/data/resstock_lmi_metadata_guide.md` — ResStock columns used for tier assignment.
- `utils/post/lmi_common.py` — Shared helpers: tier assignment expressions, credit loading, participation sampling.
- `utils/post/data/ny_eap_credits.yaml` — Per-utility, per-tier fixed monthly credit amounts.
