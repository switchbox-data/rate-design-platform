# LMI discount application to master bills

How `utils/post/apply_lmi_to_master_bills.py` enriches the master `comb_bills_year_target` tables with NY EAP/EEAP bill discounts.

---

## What it does

The script reads a Hive-partitioned master bills table from S3, assigns EAP tiers to each building using ResStock metadata, applies per-utility fixed monthly credits, validates the result, and writes back — by default in-place to the same S3 path.

It is designed to run **twice per run pair**: once at 100% participation (p100) and once at 40% (p40). The second invocation detects the shared columns written by the first and verifies consistency before appending rate-specific columns.

---

## Columns added

Each invocation adds rate-specific columns (where `{pct}` = `int(participation_rate * 100)`):

| Column                      | Type    | Description                                                                                                                                                                      |
| --------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `lmi_tier`                  | Int32   | Raw EAP tier (0 = ineligible, 1–7 = eligible). Same across p100 and p40 — reflects eligibility, not participation. Written by the first invocation; verified on subsequent ones. |
| `is_lmi`                    | Bool    | `lmi_tier > 0`. Convenience flag for filtering.                                                                                                                                  |
| `applied_discount_{pct}`    | Bool    | True if the discount was actually applied (depends on participation sampling). At p100, identical to `is_lmi`.                                                                   |
| `elec_total_bill_lmi_{pct}` | Float64 | `max(0, elec_total_bill - monthly_credit)` for monthly rows; `sum(Jan..Dec clamped)` for the Annual row.                                                                         |
| `gas_total_bill_lmi_{pct}`  | Float64 | Same logic for gas.                                                                                                                                                              |

After both runs, the table grows from ~22 to **30 columns** (22 original + 2 shared + 3 per rate × 2 rates).

---

## Invocation

### Via Justfile (preferred)

From `rate_design/hp_rates/`:

```bash
just s ny apply-lmi-to-master-bills <batch> <run_delivery> <run_supply> [participation_rate] [seed] [output_path]
```

Example — run p100 then p40 for all 4 run pairs:

```bash
cd rate_design/hp_rates
for d s in 1 2 3 4 5 6 7 8; do
  just s ny apply-lmi-to-master-bills ny_20260307_r1-8_gascalcfix $d $s 1.0
  just s ny apply-lmi-to-master-bills ny_20260307_r1-8_gascalcfix $d $s 0.4
done
```

### Direct CLI

```bash
uv run python utils/post/apply_lmi_to_master_bills.py \
  --master-bills-path "s3://data.sb/switchbox/cairo/outputs/hp_rates/ny/all_utilities/<batch>/run_<d>+<s>/comb_bills_year_target/" \
  --state NY --fpl-year 2025 \
  --cpi-s3-path "s3://data.sb/fred/cpi/" \
  --participation-rate 1.0 --participation-mode weighted --seed 42
```

Pass `--output-path <s3_path>` to redirect output instead of writing in-place.

---

## Idempotency

The script handles re-runs gracefully:

- **Rate-specific columns** (`elec_total_bill_lmi_{pct}`, `gas_total_bill_lmi_{pct}`, `applied_discount_{pct}`): dropped and recomputed from scratch.
- **Shared columns** (`lmi_tier`, `is_lmi`): if already present (from a prior rate run), the script recomputes tier assignments and verifies they match existing values. If they mismatch, it raises `AssertionError`. If they match, existing values are kept and the new tier_info is joined fresh.

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
- `context/tools/resstock_lmi_metadata_guide.md` — ResStock columns used for tier assignment.
- `utils/post/lmi_common.py` — Shared helpers: tier assignment expressions, credit loading, participation sampling.
- `utils/post/data/ny_eap_credits.yaml` — Per-utility, per-tier fixed monthly credit amounts.
