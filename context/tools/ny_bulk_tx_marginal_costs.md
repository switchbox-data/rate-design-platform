# NY bulk transmission marginal costs

How NY bulk transmission marginal costs are derived and applied with
`constraint_group` and locality-based terminology.

## Derivation script

`data/nyiso/transmission/derive_tx_values.py`

CLI:
`--path-projects-csv <input> --path-output-csv <output> [--path-constraint-groups-csv <output>]`

Outputs:

- `s3://data.sb/nyiso/bulk_tx/ny_bulk_tx_values.csv`
  - columns: `gen_capacity_zone`, `v_avg_kw_yr`
- `s3://data.sb/nyiso/bulk_tx/ny_bulk_tx_constraint_groups.csv`
  - columns: `nested_localities_str`, `constraint_group`, `v_constraint_group_kw_yr`, `tightest_nested_locality`, `paying_localities`

## Input schema

`data/nyiso/transmission/csv/ny_bulk_tx_projects.csv`

Key columns:

- `scenario`
- `project`
- `receiving_localities` (pipe-delimited nested locality tokens)
- `annual_benefit_m_yr`
- `delta_mw`
- `exclude`

## Locality model

Nested localities (overlapping, canonical NYISO zone names):

- `NYCA = WEST, GENESE, CENTRAL, NORTH, MHK_VL, CAPITL, HUD_VL, MILLWD, DUNWOD, N.Y.C., LONGIL`
- `LHV = HUD_VL, MILLWD, DUNWOD, N.Y.C.`
- `NYC = N.Y.C.`
- `LI = LONGIL`

Paying localities (disjoint):

- `ROS` (WEST through CAPITL)
- `LHV` (HUD_VL through N.Y.C.)
- `NYC` (N.Y.C.)
- `LI` (LONGIL)

Mapping from nested-locality sets to paying-locality sets is implemented in
`NESTED_TO_PAYING_LOCALITIES`.

## Derivation stages

1. `prepare_projects_for_derivation`
   - validate required columns
   - drop `exclude=True`
   - drop invalid `delta_mw`
   - map `scenario -> constraint_group`
   - canonicalize `receiving_localities -> nested_localities_str`
2. `collapse_scenario_variants`
   - average scenario variants per `(project, delta_mw, nested_localities_str, constraint_group)`
3. `compute_constraint_group_secant_vavg`
   - cumulative secant calculation per `(nested_localities_str, constraint_group)`
4. `annotate_constraint_group_paying_localities`
   - add `paying_localities`
5. `assign_constraint_groups_to_paying_localities` + `compute_paying_locality_vavg`
   - aggregate `v_avg_kw_yr` for `ROS/LHV/NYC/LI`

## Generation script

`utils/pre/generate_bulk_tx_mc.py`

CLI:
`--utility <name> --year <YYYY> [--load-year <YYYY>] --constraint-group-table-path <path> [--upload]`

High-level flow:

1. Load `ny_bulk_tx_constraint_groups.csv`.
2. Build load profiles for required `tightest_nested_locality` values from NYISO zone loads (`s3://data.sb/nyiso/hourly_demand/zones/`). When `--load-year` differs from `--year`, timestamps are remapped after allocation.
3. Compute SCR weights per nested locality.
4. Allocate each `constraint_group` value to hours using its tightest locality SCR weights.
5. Aggregate hourly signals to paying localities.
6. Blend paying localities to utility-level signal via `gen_capacity_zone` + `capacity_weight`.
7. Save 8760 output to:
   `s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/utility={utility}/year={year}/data.parquet`

## Load year / output year separation

The `--year` argument controls the output year (partition key, output timestamps). The `--load-year` argument controls which year's NYISO zone loads are used for SCR peak identification. When they differ, timestamps are remapped from load_year to year via `dt.offset_by()` after allocation.

### Justfile control

`BULK_TX_LOAD_YEAR` in `rate_design/hp_rates/ny/state.env` controls the load year for bulk TX MC generation. The NY Justfile reads it via `env_var('BULK_TX_LOAD_YEAR')` and passes it as `--load-year` to `generate_bulk_tx_mc.py`. Each MC pipeline (supply energy, supply capacity, bulk TX, dist/sub-TX) has its own load year variable in `state.env`.
