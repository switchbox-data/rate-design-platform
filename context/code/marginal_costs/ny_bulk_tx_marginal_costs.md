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

`utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py`

CLI:
`--utility <name> --year <YYYY> [--load-year <YYYY>] --constraint-group-table-path <path> [--upload]`

High-level flow:

1. Load `ny_bulk_tx_constraint_groups.csv`.
2. Average `v_constraint_group_kw_yr` over the constraint groups that list each paying locality (`ROS`, `LHV`, `NYC`, `LI`) to get one `$/kW-yr` per paying locality (`compute_paying_locality_costs`).
3. Read the utility's ICAP rows from the zone mapping (`icap_locality`, `gen_capacity_zone`, `capacity_weight`). Map each `icap_locality` to a nested locality (`NYCA`, `GHIJ` → `LHV`, `NYC`, `LI`).
4. Build a load profile for each of those nested localities by summing NYISO zone loads (`s3://data.sb/nyiso/hourly_demand/zones/`).
5. For each ICAP row, allocate the paying locality's `$/kW-yr` × `capacity_weight` onto the SCR hours of that row's nested-locality load, and sum the rows into one utility signal (`compute_utility_bulk_tx_signal`). When `--load-year` differs from `--year`, timestamps are remapped after allocation.
6. Save 8760 output to:
   `s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/utility={utility}/year={year}/data.parquet`

## Load year / output year separation

The `--year` argument controls the output year (partition key, output timestamps). The `--load-year` argument controls which year's NYISO zone loads are used for SCR peak identification. When they differ, timestamps are remapped from load_year to year via `dt.offset_by()` after allocation.

### Justfile control

`BULK_TX_LOAD_YEAR` in `rate_design/hp_rates/ny/state.env` controls the load year for bulk TX MC generation. The NY Justfile reads it via `env_var('BULK_TX_LOAD_YEAR')` and passes it as `--load-year` to `utils/data_prep/marginal_costs/bulk_tx/generate_bulk_tx_mc.py`. Each MC pipeline (supply energy, supply capacity, bulk TX, dist/sub-TX) has its own load year variable in `state.env`.
