# Subclass Revenue Requirement Utility (`compute_subclass_rr.py`)

## Purpose

`utils/post/compute_subclass_rr.py` computes revenue requirement totals for any customer subclass defined in CAIRO `customer_metadata.csv` (typically a `postprocess_group.*` column).

For each subclass:

`revenue_requirement = weighted_sum(annual electric target bills) - weighted_sum(selected BAT metric)`

where weights are `customer_metadata.csv.weight`.

The selected BAT metric is read from:

- `cross_subsidization/cross_subsidization_BAT_values.csv`

and can be one of:

- `BAT_vol` (volumetric residual allocation)
- `BAT_peak` (peak-based residual allocation)
- `BAT_percustomer` (equal per-customer residual allocation)

## Inputs expected under `--run-dir`

- `customer_metadata.csv` with `bldg_id` and the chosen `--group-col`
- `bills/elec_bills_year_target.csv` with `bldg_id`, `month`, `bill_level`
- `cross_subsidization/cross_subsidization_BAT_values.csv` with `bldg_id` and chosen `--cross-subsidy-col`

By default, annual bills are selected where `month == "Annual"`.

## Output

A table with columns:

- `subclass`
- `sum_bills`
- `sum_cross_subsidy`
- `revenue_requirement`

## CLI examples

Generic run directory:

```bash
uv run python utils/post/compute_subclass_rr.py \
  --run-dir "s3://.../<run>/<runtime>" \
  --group-col "has_hp" \
  --cross-subsidy-col "BAT_percustomer"
```

Alternative subclass split and BAT metric:

```bash
uv run python utils/post/compute_subclass_rr.py \
  --run-dir "s3://.../<run>/<runtime>" \
  --run-num 1 \
  --scenario-config "rate_design/ri/hp_rates/config/scenarios.yaml" \
  --differentiated-yaml-path "rate_design/ri/hp_rates/config/rev_requirement/rie_hp_vs_nonhp.yaml" \
  --default-yaml-path "rate_design/ri/hp_rates/config/rev_requirement/rie.yaml" \
  --group-col "postprocess_group.heating_type" \
  --cross-subsidy-col "BAT_peak"
```

RI Justfile recipe:

- `rate_design/ri/hp_rates/Justfile`
  - `compute-subclass-rr` (generic; always pass full run directory path)

Note: if `--group-col` has no dot (for example `has_hp`), the utility first looks for
that exact column and then falls back to `postprocess_group.<name>`.

Output behavior:

- Writes differentiated subclass RR YAML:
  `rate_design/ri/hp_rates/config/rev_requirement/rie_hp_vs_nonhp.yaml`
- Writes default RIE RR YAML (from `scenarios.yaml.utility_delivery_revenue_requirement`):
  `rate_design/ri/hp_rates/config/rev_requirement/rie.yaml`
- Both output paths are overrideable via:
  - `--differentiated-yaml-path`
  - `--default-yaml-path`
