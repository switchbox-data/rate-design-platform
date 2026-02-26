# approximate_non_hp_load.py — Unit-level approximation for MF highrise buildings without HP

Reference for `utils/pre/approximate_non_hp_load.py`: purpose, data flow, column schema, and entrypoints.

## Purpose

ResStock upgrade scenarios (especially HVAC upgrades) are not applied to some buildings—in particular **multifamily (MF) highrise (8+ stories)** or buildings whose primary heating fuel is not electricity, natural gas, propane, or fuel oil. This script **approximates** heating and cooling energy consumption and total heating/cooling load for those buildings using the **average of k nearest-neighbor** buildings. **Neighbors** are same weather station and have HPs from the upgrade; from them we take the k with **lowest RMSE** (on either total load or heating-only load) and average their HVAC-related columns. Result:

- Non-HP MF highrise buildings get synthetic “HP-like” load and consumption profiles for downstream BAT/rate design.
- **`include_cooling`**: when `False`, RMSE is computed on **heating-only** load (neighbors similar in heating shape); when `True`, RMSE is on **total** (heating + cooling) load.

**Scope**: Only load curves for **non-HP MF highrise** buildings (those in the neighbor map) are rewritten; all other buildings’ parquets are untouched. Output can go to a different directory or release (e.g. `res_2024_amy2018_2_sb`) so downstream CAIRO/BAT use the approximated curves like any other ResStock load.

Implementation: **Polars LazyFrames** where possible (scan → replace → sink); single-building load helpers return 8760-point numpy arrays for RMSE.

## Data and paths

- **Input/output**: Hourly load parquets under a `load_curve_hourly_dir` (e.g. `s3://.../load_curve_hourly/state=NY/upgrade=02/`). One file per building per upgrade: `{bldg_id}-{int(upgrade_id)}.parquet` (e.g. `12345-2.parquet`).
- **Storage**: `_parquet_storage_options(dir)` returns AWS options for `S3Path`, `{}` for local `Path`; used for `scan_parquet` / `sink_parquet`.
- **Metadata**: ResStock metadata LazyFrame must include `bldg_id`, `in.weather_file_city`, `postprocess_group.has_hp`, `in.geometry_building_type_height`, `in.geometry_story_bin` for filtering MF highrise and grouping by weather station.

## Column constants (load_curve_hourly parquet)

- **Load (energy delivered)**: `HEATING_LOAD_COLUMN` = `out.load.heating.energy_delivered.kbtu`, `COOLING_LOAD_COLUMN` = `out.load.cooling.energy_delivered.kbtu`. Total building load = heating + cooling.
- **Electricity**: `HEATING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS` (8), `COOLING_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS` (4), `TOTAL_ENERGY_CONSUMPTION_ELECTRICITY_COLUMNS` (2).
- **Other fuels**: `HEATING_ENERGY_CONSUMPTION_*` and `TOTAL_ENERGY_CONSUMPTION_*` for natural_gas, fuel_oil, propane (4 heating + 2 total each).

## Pipeline overview

1. **Identify** non-HP MF highrise: `_identify_non_hp_mf_highrise(metadata)` → LazyFrame of `bldg_id`, `in.weather_file_city`.
2. **Find neighbors**: `_find_nearest_neighbors(metadata, non_hp_mf_highrise_bldg_metadata, load_curve_hourly_dir, upgrade_id, k=..., include_cooling=...)` → `dict[bldg_id, list[(neighbor_bldg_id, rmse)]]`. For each weather station, non-HP buildings get k nearest (by RMSE) from same-station HP buildings; load curves are either total (heating+cooling) or heating-only depending on `include_cooling`.
3. **Update load curves**: `update_load_curve_hourly(nearest_neighbor_map, input_load_curve_hourly_dir, output_load_curve_hourly_dir, upgrade_id)` loads each building’s parquet and its neighbors, runs `replace_hvac_columns`, and sinks the result to the output dir (input and output dirs can differ).
4. **Validation** (optional): `validate_nearest_neighbor_approximation(metadata, input_load_curve_hourly_dir, upgrade_id, k=..., include_cooling=..., n_validation=...)` finds k nearest for a sample of HP MF highrise buildings and reports load and energy-consumption metrics (RMSE, peak diff, etc.).

## Main entrypoints

| Function | Role |
|----------|------|
| `_identify_non_hp_mf_highrise(metadata)` | Filter to ~has_hp=False, Multifamily, 8+ stories; return bldg_id + weather. |
| `group_by_weather_station_id(metadata)` | Group bldg_ids by `in.weather_file_city`. |
| `_find_nearest_neighbors(..., load_curve_hourly_dir, upgrade_id, k, include_cooling)` | Per non-HP bldg, k nearest same-weather bldgs by RMSE (total or heating-only). |
| `replace_hvac_columns(original_lf, neighbors_lf_list)` | Replace electricity, heating/cooling load, natural_gas, fuel_oil, propane columns with neighbor averages; adjust totals. |
| `update_load_curve_hourly(nearest_neighbor_map, input_dir, output_dir, upgrade_id)` | For each bldg in map: scan original + neighbors, replace HVAC, sink to output dir. |
| `validate_nearest_neighbor_approximation(metadata, input_load_curve_hourly_dir, upgrade_id, ...)` | Sample HP MF highrise, find neighbors, run load and energy-consumption validation and print summary. |

## Load helpers (single building → 8760 vec or None)

- `_load_one_total_building_load_curve(dir, bldg_id, upgrade_id)` — heating + cooling load columns summed.
- `_load_one_heating_building_load_curve` — heating load column only.
- `_load_one_cooling_building_load_curve` — cooling load column only.
- `_load_one_total_heating_cooling_energy_consumption_curve` — electricity heating + cooling consumption (summed).
- `_load_one_heating_energy_consumption_curve` / `_load_one_cooling_energy_consumption_curve` — electricity heating or cooling consumption only.

Bulk: `_load_all_total_load_curves_for_bldg_ids`, `_load_all_heating_load_curves_for_bldg_ids` (used inside `_find_nearest_neighbors`).

## Replace logic (LazyFrame, row-index join)

For each replace group (electricity, heating/cooling load, natural_gas, fuel_oil, propane):

- Add row index to original; join each neighbor on row index with aliased columns (`_n0_<col>`, `_n1_<col>`, …).
- Replace each component column with the mean over neighbors.
- Total columns are adjusted: `new_total = orig_total - orig_component_sum + avg_component_sum` (and similarly for intensity where applicable).

`replace_hvac_columns` chains: electricity → heating/cooling load → natural_gas → fuel_oil → propane.

## Validation helpers

- `_validate_one_building_load` / `_validate_one_building_energy_consumption`: load real + neighbor curves, compute RMSE/peak/diff metrics.
- `_validate_nearest_neighbors_building_load` / `_validate_nearest_neighbors_heating_cooling_energy_consumption`: run validation over a map of bldg_id → neighbor list and print aggregates.

## __main__

When run as a script, loads metadata and paths from S3 (NY, upgrade 02, `res_2024_amy2018_2`), and can:

- (Commented out) Identify non-HP MF highrise, run `_find_nearest_neighbors`, then `update_load_curve_hourly` to write approximated curves to an output release.
- Call `validate_nearest_neighbor_approximation` (e.g. k=15, include_cooling=False, n_validation=100).

## Conventions

- **Paths**: `load_curve_hourly_dir` is `S3Path | Path` everywhere; storage options are chosen from it.
- **Concurrency**: `ThreadPoolExecutor` for parallel load and per-building update; worker counts are arguments (e.g. `max_workers`, `max_workers_load_curves`, `max_workers_neighbors`).
- **RMSE**: `_rmse_8760(x, y, smooth=False)`; optional 3-point smoothing for RMSE.
