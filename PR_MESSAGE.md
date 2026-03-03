Closes #302

## Overview

Adds NYISO bulk transmission marginal costs as a separate delivery-charge component in the NY rate design pipeline. Bulk transmission costs are derived from NYISO AC Transmission and LI Export studies using an average-secant method to compute v_z ($/kW-yr) per `gen_capacity_zone`, then smeared into 8760 hourly traces using SCR (top-40-per-season peak hours). The bulk transmission MC is combined with distribution+sub-transmission MC to form the total delivery marginal cost, which is active in delivery-only CAIRO runs.

## Key Changes

### Data pipeline

- **Raw project data**: `data/nyiso/transmission/ny_bulk_tx_projects.csv` encodes all NYISO study inputs (project-level ΔMW and annual benefits by locality)
- **v_z derivation**: `data/nyiso/transmission/derive_tx_values.py` implements the average-secant method:
  - Computes discrete marginal values v = B / (ΔMW × 1000) per project
  - Groups by (locality, scenario_family), sorts by ΔMW ascending, computes cumulative secants
  - Averages secants to get v_avg per locality, then maps to `gen_capacity_zone` (ROS, LHV, NYC, LI)
  - ROS uses NiMo 2025 MCOS data (undiluted $/kW-yr) since NYISO studies show net-negative benefits for upstate export projects
- **8760 trace generation**: `utils/pre/generate_bulk_tx_mc.py`:
  - Loads v_z table and zone mapping (with `capacity_weight` for multi-locality utilities like ConEd: 87% NYC + 13% LHV)
  - Identifies SCR hours: top 40 hours per season (summer = months 5-10, winter = months 11-12 + 1-4)
  - Applies load-weighted smear: `w_t = load_t / sum(load in SCR hours)`, `pi_t = v_z × w_t` for SCR hours only
  - Outputs to `s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/utility={u}/year={y}/data.parquet`

### MC loading integration

- **New functions in `utils/cairo.py`**:
  - `load_bulk_tx_marginal_costs(path)`: Loads bulk TX MC parquet (stored as $/kWh)
  - `load_dist_and_sub_tx_marginal_costs(path)`: Loads dist+sub-tx MC (renamed from previous loader)
    - Stores as $/kWh in parquet (no conversion needed)
  - `_align_mc_to_index(mc_series, target_index, mc_type)`: Shared alignment helper for position-based and reindexing cases
  - `add_bulk_tx_and_dist_and_sub_tx_marginal_cost(...)`: High-level function that loads both MCs, aligns to target index, sums into single delivery MC Series
- **`run_scenario.py` updates**:
  - Added `path_bulk_tx_mc: str | Path | None` to `ScenarioSettings`
  - Renamed `path_td_marginal_costs` → `path_dist_and_sub_tx_mc` (consistent naming)
  - Single call to `add_bulk_tx_and_dist_and_sub_tx_marginal_cost()` replaces previous delivery MC loading logic

### Configuration updates

- **Zone mapping**: Added `tx_locality` column to `data/nyiso/zone_mapping/generate_zone_mapping_csv.py` (currently equals `gen_capacity_zone`, kept separate for future flexibility)
- **Scenario YAMLs**: `create_scenario_yamls.py` now reads `path_bulk_tx_mc` and `path_dist_and_sub_tx_mc` from Google Sheet columns; all 7 NY utility YAML files regenerated
- **Justfiles**:
  - Shared: `path_dist_and_sub_tx_mc` variable, `create-dist-and-sub-tx-mc-data` recipe (renamed from `create-td-mc-data`)
  - NY-specific: `path_bulk_tx_mc` variable, `create-bulk-tx-mc-data` and `create-bulk-tx-mc-data-all` recipes
  - Fixed `s` dispatch to route through state Justfile so NY variable overrides are active
- **Validation**: `validate_config.py` updated to check `--path-dist-and-sub-tx-mc` (required) and `--path-bulk-tx-mc` (optional)

### Testing

- `tests/test_bulk_tx_mc.py` covers:
  - SCR hour identification (40/season, 80 total, non-overlapping, correct season assignment)
  - Load-weighted smear validation (weights sum to 1.0, 1 kW constant load recovers v_z)
  - v_z table loading and quantile resolution (single-zone, multi-zone weighted blend)
  - Output schema validation (8760 rows, correct columns, 1 kW recovery)

### Documentation

- `context/tools/ny_bulk_tx_marginal_costs.md`: Complete reference on derivation method, data sources, and zone mapping logic

## Naming Conventions

| Old name | New name |
| --- | --- |
| `path_td_marginal_costs` | `path_dist_and_sub_tx_mc` |
| `path_transmission_mc` | `path_bulk_tx_mc` |
| `create-td-mc-data` | `create-dist-and-sub-tx-mc-data` |
| S3: `.../transmission/` | S3: `.../bulk_tx/` |
| Output column: `transmission_cost_enduse` | `bulk_tx_cost_enduse` |

## Reviewer Focus

1. **v_z derivation logic**: The average-secant method (Step 2 in `derive_tx_values.py`) captures diminishing returns by sorting projects by ΔMW and averaging cumulative secants. This gives less weight to large-ΔMW low-v projects than a simple mean would. Review the locality → `gen_capacity_zone` mapping table (lines 184-197 in plan) to confirm ROS/LHV/NYC/LI assignments.

2. **SCR allocation**: The top-40-per-season approach (80 total hours) matches the seasonal discount default. Verify that load-weighted smear correctly recovers v_z for a flat 1 kW load (validation in `generate_bulk_tx_mc.py` and tests).

3. **Multi-locality utilities**: ConEd's 87% NYC + 13% LHV weighting uses the same `capacity_weight` logic as ICAP. Confirm this matches expectations for how transmission benefits are allocated.

4. **MC alignment**: The `_align_mc_to_index` helper handles both same-length position alignment (common when MC file year differs from run year) and reindexing. Review alignment logic for edge cases.

5. **ROS treatment**: ROS uses NiMo 2025 MCOS undiluted values (not NYISO studies) because NYISO shows net-negative benefits for upstate export projects. This is intentional and documented.

6. **Unit convention**: Bulk TX is stored as $/kWh in parquet, consistent with dist+sub-tx. Both delivery MCs use the same unit convention for consistency.

## S3 Status

Bulk TX MC parquets are generated and ready for upload. Run `just s ny create-bulk-tx-mc-data-all` to upload all 7 utilities to `s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/utility={u}/year=2025/data.parquet`.
