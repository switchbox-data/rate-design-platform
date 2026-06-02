# ResStock `_sb` release pipeline: `data/resstock/main.py`

This document describes the unified Python pipeline (`data/resstock/main.py`) for preparing a Switchbox `_sb` ResStock release from the standard NREL release. This script consolidates what was previously a multi-step Justfile workflow into a single orchestrated pipeline with built-in provenance tracking.

For the older Justfile-based workflow, see `context/code/data/resstock_data_preparation_run_order.md`. That document remains the canonical reference for the full end-to-end flow including steps not yet implemented in `main.py`.

---

## Relationship to the Justfile workflow

`main.py` replaces the full Justfile workflow in a single invocation with manifest-based provenance. The pipeline writes directly to the local EBS mount, so the intermediate `sudo aws s3 sync` that the Justfile needed (to pull `_sb` from S3 before building monthly curves) is not required here.

| Justfile step              | `main.py` equivalent                  | Status      |
| -------------------------- | ------------------------------------- | ----------- |
| 1. Fetch                   | Step 1a: fetch via bsf                | Implemented |
| 2. Prepare metadata        | Step 2a: `_modify_metadata`           | Implemented |
| 3. Utility assignment      | Step 2b: `_assign_utility`            | Implemented |
| 4. Copy standard -> `_sb`  | Step 1b: clone                        | Implemented |
| 5. Approximate non-HP load | Step 2c-i: `_approximate_non_hp_load` | Implemented |
| 6. Adjust MF electricity   | Step 2c-ii: `_adjust_mf_electricity`  | Implemented |
| 7. Sync `_sb` to EBS       | N/A (pipeline writes directly to EBS) | N/A         |
| 8. Add monthly load curves | Step 2d: `_add_monthly_loads`         | Implemented |
| 9. Upload monthly to S3    | Step 2d: `_add_monthly_loads`         | Implemented |
| Upload raw + `_sb` to S3   | Step 3: `_upload`                     | Implemented |

---

## Invocation

```bash
uv run python -m data.resstock.main --state <STATE> [options]
```

Release-level defaults are loaded from `data/resstock/config.yaml`. State-specific settings (FIPS code, projected CRS, PUMA shapefile vintage, polygon filenames, excluded gas utilities) are loaded from `data/resstock/state_configs.yaml`.

### Key CLI arguments

| Argument                       | Default                                        | Description                                                                         |
| ------------------------------ | ---------------------------------------------- | ----------------------------------------------------------------------------------- |
| `--state`                      | (required)                                     | One or more 2-letter state codes                                                    |
| `--upgrade-ids`                | `0 1 2 3 4 5`                                  | Upgrade IDs to download                                                             |
| `--file-types`                 | `metadata load_curve_hourly load_curve_annual` | File types to fetch from NREL                                                       |
| `--sample`                     | `0` (all)                                      | Number of buildings to download (0 = full population)                               |
| `--identify-hp-customers`      | `True`                                         | Add `postprocess_group.has_hp`                                                      |
| `--identify-heating-type`      | `True`                                         | Add heating-type columns                                                            |
| `--identify-natgas-connection` | `True`                                         | Add `has_natgas_connection`                                                         |
| `--add-vulnerability-columns`  | per-state from `state_configs.yaml`            | Add LMI columns; defaults True for NY, False for RI. Pass True/False to override.   |
| `--approximate-non-hp-load`    | `True`                                         | Run k-nearest-neighbor HVAC substitution for upgrade 02                             |
| `--adjust-mf-electricity`      | `True`                                         | Apply MF non-HVAC electricity adjustment (00 and 02)                                |
| `--assign-utility`             | `True`                                         | Assign electric/gas utilities (NY, RI only)                                         |
| `--electric-poly-filename`     | from `state_configs.yaml`                      | Electric utility polygon CSV; overrides config default                              |
| `--gas-poly-filename`          | from `state_configs.yaml`                      | Gas utility polygon CSV; overrides config default                                   |
| `--path-s3-gis-dir`            | `s3://data.sb/gis/utility_boundaries/`         | S3 directory for NY utility polygon CSVs                                            |
| `--add-monthly-loads`          | `True`                                         | Aggregate hourly → monthly and upload (needs `load_curve_hourly` in `--file-types`) |
| `--monthly-workers`            | `50`                                           | Parallel worker count for monthly aggregation                                       |
| `--path-output-dir`            | `/ebs/data/nrel/resstock`                      | Local EBS output root                                                               |
| `--path-s3-dir`                | `s3://data.sb/nrel/resstock`                   | S3 mirror root                                                                      |

### Example invocations

Full NY run:

```bash
uv run python -m data.resstock.main --state NY
```

MD sample run (10 buildings):

```bash
uv run python -m data.resstock.main \
  --state MD \
  --upgrade-ids 0 2 \
  --file-types metadata load_curve_hourly load_curve_annual \
  --sample 10
```

The `--add-vulnerability-columns` flag is not needed here — it defaults to the per-state value from `state_configs.yaml` (states without an entry default to `False`).

---

## Constants and configuration

### `data/resstock/config.yaml`

Default values for the pipeline. Anything passed as a CLI argument overrides these.

```yaml
resstock:
  release_year: 2024
  weather_file: amy2018
  release_version: 2
  upgrade_ids: ["0", "1", "2", "3", "4", "5"]
  file_types: [metadata, load_curve_hourly, load_curve_annual]
paths:
  output_dir: /ebs/data/nrel/resstock
  s3_dir: s3://data.sb/nrel/resstock
  s3_pums_dir: s3://data.sb/census/pums
pums:
  survey: acs5
  year: "2021"
```

### `data/resstock/state_configs.yaml`

Per-state configuration. Top-level keys are 2-letter state codes; each entry contains state-specific settings consumed by the utility assignment step and `assign_utility_ny.py`.

```yaml
NY:
  state_fips: "36"
  add_vulnerability_columns: true
  state_crs: 2260
  puma_year: 2019
  electric_poly_filename: ny_electric_utilities_20260309.csv
  gas_poly_filename: ny_gas_utilities_20260309.csv
  excluded_gas_utilities: [bath, chautauqua, corning, fillmore, reserve, stlaw]
RI:
  state_fips: "44"
  add_vulnerability_columns: false
  electric_poly_filename:
  gas_poly_filename:
```

**`SUPPORTED_UTILITY_STATES` is derived from this file:** Any state whose config entry contains both `electric_poly_filename` and `gas_poly_filename` keys (even if the values are null, as for RI) is automatically included in `SUPPORTED_UTILITY_STATES`. Adding a new state to this file with those two keys is sufficient to register it for utility assignment — no hardcoded set needs to be updated.

Polygon filenames default from this config. The CLI flags `--electric-poly-filename` / `--gas-poly-filename` override the config defaults when provided. For rule-based states like RI (null filenames), the GIS path is skipped entirely.

### `_SB_EXCLUDED_FILE_TYPES` (module-level constant in `main.py`)

```python
_SB_EXCLUDED_FILE_TYPES: frozenset[str] = frozenset({"load_curve_annual"})
```

File types that are fetched for the raw NREL release but **never copied to `_sb`**, never uploaded under the `_sb` prefix, and never validated against `_sb`. Currently contains only `load_curve_annual`.

**Why `load_curve_annual` is excluded:** The `_sb` release modifies `load_curve_hourly` in place (non-HP approximation, MF electricity adjustment). There is no mechanism to re-derive `load_curve_annual` from the modified hourly data, so including the unmodified raw annual in `_sb` would be misleading — it would not reflect the approximation or adjustment. The correct sub-annual aggregation is `load_curve_monthly`, which is derived from the modified hourly in step 2d.

**How to expand `_sb` to include `load_curve_annual` in the future:** If a need arises for an `_sb` annual file (e.g., a downstream consumer requires it), the steps would be:

1. Implement an hourly-to-annual aggregation script (analogous to `data/resstock/load_curve/add_monthly_loads.py` but aggregating to 1 row per building).
2. Add a pipeline step after all hourly modifications are complete (after step 2c-ii) that runs the aggregation on the `_sb` hourly files and writes `load_curve_annual/` under `path_sb`.
3. Remove `"load_curve_annual"` from `_SB_EXCLUDED_FILE_TYPES` so the clone, upload, and validation steps include it.
4. Ensure `_modify_metadata` still reads `load_curve_annual` from `path_raw` (the raw release) for the `identify_natgas_connection` step, since that runs before any hourly modifications. (Or, if the new annual file is generated after modifications, decide whether natgas identification should use the pre- or post-modification annual.)

### `data/resstock/constants.py`

Column-name constants used for schema validation after each metadata transform:

| Constant                 | Columns                                                                                                                                                      | Set by                       |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------- |
| `HP_CUSTOMERS_COLS`      | `postprocess_group.has_hp`                                                                                                                                   | `identify_hp_customers`      |
| `HEATING_TYPE_COLS`      | `postprocess_group.heating_type`, `postprocess_group.heating_type_v2`, `heats_with_electricity`, `heats_with_natgas`, `heats_with_oil`, `heats_with_propane` | `identify_heating_type`      |
| `NATGAS_CONNECTION_COLS` | `has_natgas_connection`                                                                                                                                      | `identify_natgas_connection` |
| `VULNERABILITY_COLS`     | `has_child_under_6`, `has_person_over_60`, `has_disabled_person`, `is_vulnerable`                                                                            | `add_vulnerability_columns`  |

### bsf sampling behavior (important for understanding `--sample` mode)

When `--sample N` is active, bsf downloads the full NREL source file for each file type but **filters it to only the N sampled building IDs** before writing to disk. This affects both `metadata.parquet` and `load_curve_annual`. The bsf bundled index (`METADATA_DIR`, shipped with the package) has all building IDs for every release but only contains `bldg_id`, `county`, and `puma` — not the `in.weather_file_city` column needed for weather-station grouping.

Because there is no reliable, release-independent way to obtain the full-population metadata with all columns in sample mode, the pipeline accepts this limitation: the neighbor search is restricted to the locally downloaded buildings. A warning is printed. `--sample` is a development/testing feature; production runs should always use `--sample 0` (the default).

---

## Pipeline step-by-step

### Naming conventions

The pipeline derives two release names from the CLI arguments:

- **Raw release** (`release`): e.g., `res_2024_amy2018_2` — the standard NREL release, unmodified.
- **`_sb` release** (`release_sb`): e.g., `res_2024_amy2018_2_sb` — the Switchbox-modified release.

Local paths:

- `path_raw` = `<path_output_dir>/<release>` — raw files on EBS.
- `path_sb` = `<path_output_dir>/<release_sb>` — modified files on EBS.

S3 paths:

- `s3_base_raw` = `<path_s3_dir>/<release>` — raw files on S3.
- `s3_base_sb` = `<path_s3_dir>/<release_sb>` — modified files on S3.

### `sb_file_types`

Computed at the top of `main()`:

```python
sb_file_types = [ft for ft in args.file_types if ft not in _SB_EXCLUDED_FILE_TYPES]
```

This list drives the clone, clone validation, `_sb` upload, and `_sb` S3 validation. With current defaults, `sb_file_types` = `["metadata", "load_curve_hourly"]`.

### Manifest and warnings

A run record is created immediately via `new_run_record` from `data/resstock/manifest.py`. This captures the git commit, branch, CLI arguments, and all flags. The record is updated after each step via `record_step` and `upsert_run`, providing a persistent audit trail in `manifest.yaml` (one per release directory, synced to S3).

If any file type in `--file-types` is in `_SB_EXCLUDED_FILE_TYPES`, a warning is printed to stdout and recorded in the run record under a `warnings` key. This ensures the user is aware that `load_curve_annual` (or any future excluded type) is not part of the `_sb` release.

### Step 1a: Fetch raw ResStock data

Calls `fetch_resstock_data.run()` (which wraps `bsf`) to download all requested file types, states, and upgrade IDs from NREL to `path_raw`. This includes `load_curve_annual` — it is needed by `identify_natgas_connection` in step 2a.

When `--sample N` is passed, `bsf` downloads only N load curve files per state/upgrade but downloads the full metadata parquet (it's a single file).

Validation: `validate_local_files` checks that every `(file_type, state, upgrade)` directory has at least one `.parquet` file.

### Step 1b: Clone raw release to `_sb`

Copies files from `path_raw` to `path_sb`, iterating over `sb_file_types` (not `args.file_types`). This means `load_curve_annual` is never copied to `_sb`.

A fresh `manifest.yaml` is written to `path_sb` with an empty `runs` list.

Validation: `validate_local_files` checks `path_sb` against `sb_file_types` only.

### Step 2a: Modify metadata

Iterates over all `(state, upgrade)` combinations. For each:

1. Reads `metadata.parquet` (the raw NREL metadata) from `path_sb`.
2. Applies `_modify_metadata`, which chains four transforms in dependency order:
   - **`identify_hp_customers`** — adds `postprocess_group.has_hp`.
   - **`identify_heating_type`** — adds `postprocess_group.heating_type`, `heats_with_*` flags. Requires `has_hp`.
   - **`identify_natgas_connection`** — adds `has_natgas_connection`. **Reads `load_curve_annual` from `path_raw` (the raw release), not from `path_sb`.** This is correct because `load_curve_annual` is never copied to `_sb`, and the raw annual data is the appropriate baseline for this identification. For buildings whose HVAC is later approximated (step 2c-i), `has_natgas_connection` is re-derived from the modified `load_curve_hourly`.
   - **`add_vulnerability_columns`** — adds LMI columns. Per-state default from `state_configs.yaml` (True for NY, False for RI). CLI `--add-vulnerability-columns True/False` overrides for all states.
3. Validates output schema for each active transform.
4. Writes the result to `metadata-sb.parquet` in `path_sb`.

**Important: `path_raw` vs `path_sb` in `_modify_metadata`.** The `path_raw` parameter is used exclusively for `load_curve_annual` in `identify_natgas_connection`. All metadata I/O (read `metadata.parquet`, write `metadata-sb.parquet`) is done on `path_sb` by the caller. If a future transform needs to read load curve data that should reflect `_sb` modifications, it must be placed after the relevant modification step and read from `path_sb`.

### Step 2b: Assign utilities

Runs only when `--assign-utility True` and `metadata` is in `--file-types`.

Logic is in `_assign_utility()`. For each state:

1. Reads upgrade-00 `metadata-sb.parquet` from `path_sb` (utility assignment is per-state, not per-upgrade).
2. Calls `assign_utility(state, metadata, ...)` from `data/resstock/utility/assign_utility.py`, which:
   - Looks up the state's configuration from `state_configs.yaml` internally.
   - Resolves polygon filenames: CLI override (`--electric-poly-filename` / `--gas-poly-filename`) if provided, otherwise the default from `state_configs.yaml`.
   - If filenames are empty/null (e.g. RI): uses rule-based assignment.
   - If filenames are present (e.g. NY): loads electric and gas polygon CSVs from `--path-s3-gis-dir`, fetches Census PUMA shapefiles via pygris, and calls `assign_utility_ny`.
3. **Writes only `bldg_id`, `sb.electric_utility`, and `sb.gas_utility`** to `path_sb/metadata_utility/state=<s>/utility_assignment.parquet`.
4. Immediately uploads the file to S3 via `aws s3 cp`.

**Pre-flight validation:** `validate_utility_assignment_args()` (called in step 0) checks that the utility-assignment upgrade is in `--upgrade-ids` and that all requested states are in `SUPPORTED_UTILITY_STATES`. Unsupported states or missing upgrades halt the pipeline before any data is fetched.

### Step 2c-i: Approximate non-HP load (upgrade 02)

Runs only when all three conditions are met:

- `--approximate-non-hp-load True`
- upgrade `02` is in `--upgrade-ids`
- `load_curve_hourly` is in `--file-types`

Logic is in `_approximate_non_hp_load()`. For each state:

1. **Identify targets.** Reads `metadata-sb.parquet` from `path_sb` (which already has `has_hp` and `heats_with_*` columns from step 2a). Finds non-HP multifamily buildings and non-HP "other fuel type" buildings via `_identify_non_hp_mf` and `_identify_other_fuel_types`.

2. **Restrict targets when sampling.** When `--sample > 0`, enumerates actual `.parquet` files in the local `load_curve_hourly` directory and restricts targets to only those `bldg_id`s with locally available files.

3. **Determine neighbor pool.** When sampling, the full state metadata and load curves are read from S3 (`path_s3_dir/<release>/`) so the neighbor search considers the entire population, not just the sampled subset. When not sampling, the local files are the full population.

4. **Find k nearest neighbors.** `_find_nearest_neighbors` (from `utils/pre/approximate_non_hp_load.py`) groups buildings by weather station and, for each target, finds the 15 closest HP buildings by heating-load RMSE. Internally parallelized: loads target heating curves from local disk, loads neighbor heating curves from the neighbor directory (local or S3) in parallel via `ThreadPoolExecutor(max_workers=256)`.

5. **Cache chosen neighbors (sample mode only).** After the RMSE search identifies the k=15 best neighbors per target, if neighbors were read from S3, their full parquets are downloaded to a temporary directory (`tempfile.mkdtemp`) via `_cache_s3_parquets` (parallel, up to 64 threads). This avoids re-reading them from S3 in the next step. The cache is cleaned up in a `finally` block.

6. **Replace HVAC columns.** `update_load_curve_hourly` (from `utils/pre/approximate_non_hp_load.py`) processes each target building in parallel. For each target, loads the original parquet from local disk, loads the k neighbor parquets (from cache or local disk), replaces HVAC-related columns (heating/cooling load, electricity, natural gas, fuel oil, propane, and their totals) with the neighbor average, and writes the modified parquet back to `path_sb`. Also determines whether each building uses natural gas after the swap.

7. **Update metadata.** `update_non_hp_metadata` marks approximated buildings as HP in `metadata-sb.parquet`: sets `postprocess_group.has_hp = True`, `heats_with_electricity = True`, other fuel flags to `False`, assigns HP HVAC labels, adds `approximated_hp_load = True`, and updates `has_natgas_connection` based on the post-swap hourly data.

   The metadata is materialized with `.collect()` and written with `.write_parquet()` (not `.sink_parquet()`) because the scan source and output target are the same file.

### Step 2c-ii: Adjust MF electricity

Runs only when `--adjust-mf-electricity True` and at least one of upgrades 00/02 is in `--upgrade-ids` and `load_curve_hourly` is in `--file-types`.

Logic is in `_adjust_mf_electricity()`. For each (state, upgrade) pair in `["00", "02"]`:

1. Reads `metadata-sb.parquet` from `path_sb` and `load_curve_annual` from `path_raw`.
2. Computes MF/SF non-HVAC electricity ratios (mean kWh/sqft) via `_get_non_hvac_mf_to_sf_ratios`.
3. For each unadjusted multifamily building, scales hourly non-HVAC consumption and intensity columns by dividing by the MF/SF ratio, recomputes totals, and writes back.
4. Marks adjusted buildings with `mf_non_hvac_electricity_adjusted = True` in `metadata-sb.parquet`.

In sample mode, a warning is printed that ratios are derived from the sampled buildings only. If the sample has no MF buildings, the step is skipped; if fewer than 2 SF buildings, ratios default to 1.0.

### Step 2d: Add monthly load curves

**Function:** `_add_monthly_loads`

**Gate condition:** `args.add_monthly_loads and "load_curve_hourly" in args.file_types`

Aggregates the modified `_sb` hourly load curves into monthly load curves and uploads them to S3. This step runs after all hourly modifications (steps 2c-i, 2c-ii) so that monthly curves reflect non-HP approximation and MF electricity adjustment.

**Logic:**

1. Loads bsf column aggregation rules via `load_aggregation_rules(release)` (using the **raw** release name, e.g. `res_2024_amy2018_2`, not the `_sb` variant). The rules CSV lives in the bsf package (`buildstock_fetch.constants.LOAD_CURVE_COLUMN_AGGREGATION`).
2. For each (state, upgrade) pair, calls `process_upgrade(path_sb, path_sb, state, upgrade, agg_rules, workers)`, which:
   - Reads all hourly parquets from `path_sb/load_curve_hourly/state=<s>/upgrade=<uid>/`
   - Groups by `month`, applies sum/mean/first rules per column, reconstructs a `timestamp` datetime column
   - Writes one monthly parquet per building to `path_sb/load_curve_monthly/state=<s>/upgrade=<uid>/`
   - Runs up to `--monthly-workers` (default 50) files in parallel via `ThreadPoolExecutor`
3. After all upgrades for a state are done, uploads `path_sb/load_curve_monthly/state=<s>/` to `s3://.../load_curve_monthly/state=<s>/` via `aws s3 sync`. The upload is per state (covers all upgrades in one sync call).
4. Returns the list of `"state=<s> upgrade=<uid>"` labels that were processed, for manifest recording.

**Important:** `load_curve_monthly` is **not** in `args.file_types` (which controls what is fetched from NREL and uploaded by the main `_upload` call). Monthly files are generated locally and uploaded exclusively by `_add_monthly_loads`. The main `_upload` step at the end does not touch `load_curve_monthly`.

**Sample mode:** When `--sample N` is active, only N hourly files exist for each (state, upgrade). `process_upgrade` processes whatever files are present, producing N monthly files. A `NOTE` is printed but the step is not skipped. This is expected behavior for development/testing.

### Step 3: Upload to S3

Uploads both releases to S3 via `aws s3 sync`:

- **Raw release**: all `args.file_types` (including `load_curve_annual`).
- **`_sb` release**: only `sb_file_types` (excludes `load_curve_annual`).

Validation: `validate_s3_objects` spot-checks up to 5 S3 objects per `(file_type, state, upgrade)`. The `_sb` validation uses `sb_file_types`.

Manifests are uploaded separately via `_upload_manifest`.

### Finalization

On success, the run record is marked `completed` and written to both manifests. On failure, the run is marked `failed` with the error message, written to whichever manifest directories exist, and the process exits with code 1.

---

## File types in `_sb` releases

### Currently included

| File type            | Source                                                                                 | Notes                                                             |
| -------------------- | -------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `metadata`           | Cloned from raw, then modified in place (`metadata-sb.parquet`)                        | Contains all SB-specific columns                                  |
| `load_curve_hourly`  | Cloned from raw, then modified in place by approximation and MF electricity adjustment | One parquet per building                                          |
| `metadata_utility`   | Generated by `_assign_utility` (step 2b), uploaded to S3 immediately                   | Contains only `bldg_id`, `sb.electric_utility`, `sb.gas_utility`  |
| `load_curve_monthly` | Derived by `_add_monthly_loads` (step 2d) from the modified `load_curve_hourly`        | One parquet per building; synced to S3 per state after generation |

### Currently excluded

| File type           | Reason                                                                                             | Path to inclusion                                                                                                 |
| ------------------- | -------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `load_curve_annual` | No mechanism to re-derive from modified hourly; raw annual would be inconsistent with `_sb` hourly | Build an hourly-to-annual aggregation step, run it after all modifications, remove from `_SB_EXCLUDED_FILE_TYPES` |

---

## Data flow and which release provides what

Understanding which release provides data to which step is critical for correctness:

| Data                               | Read from                                                  | Written to              | Step                              |
| ---------------------------------- | ---------------------------------------------------------- | ----------------------- | --------------------------------- |
| `metadata.parquet` (raw NREL)      | `path_raw` via clone                                       | `path_sb`               | 1b                                |
| `load_curve_hourly` (raw NREL)     | `path_raw` via clone                                       | `path_sb`               | 1b                                |
| `load_curve_annual` (raw NREL)     | `path_raw` (read directly, never cloned to `_sb`)          | N/A                     | 2a (`identify_natgas_connection`) |
| `metadata-sb.parquet`              | `path_sb`                                                  | `path_sb`               | 2a, 2b, 2c-i, 2c-ii               |
| `load_curve_hourly` (modified)     | `path_sb`                                                  | `path_sb` (overwritten) | 2c-i, 2c-ii                       |
| `load_curve_annual` (ratios)       | `path_raw`                                                 | N/A                     | 2c-ii                             |
| Neighbor load curves (sample mode) | `path_sb` (same as targets — limited to sampled buildings) | N/A                     | 2c-i                              |
| `utility_assignment.parquet`       | `path_sb/metadata_utility/state=<s>/`                      | `path_sb` + S3          | 2b                                |
| `load_curve_monthly` (derived)     | `path_sb/load_curve_hourly/` (modified)                    | `path_sb` + S3          | 2d                                |

---

## Sample mode (`--sample N`)

When `--sample N` is passed (N > 0):

- **bsf** downloads the full NREL parquet for metadata and `load_curve_annual`, but **filters both to only the N sampled building IDs** before writing to disk. The local `metadata.parquet` and `load_curve_annual` will each have only N rows. Load curves (`load_curve_hourly`) are downloaded as individual per-building files, so only N files are present.
- **Neighbor search is limited to the sampled buildings.** A warning is printed. There is no release-independent way to obtain the full-population metadata with all needed columns (e.g. `in.weather_file_city`) in sample mode — bsf filters every file type, the bsf bundled index lacks `in.*` columns, and NREL OEDI URL patterns change across releases. This limitation is acceptable because `--sample` is a development/testing feature; production runs should always use `--sample 0`.
- **Target identification** (step 2c-i) restricts non-HP targets to only buildings with locally available load curve files.
- **All I/O is local** — no S3 reads for neighbor curves, no temp caching. This makes sample-mode runs fast and simple.

---

## Supporting modules

| Module                                                 | Purpose                                                                                                                 |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| `data/resstock/config.yaml`                            | Pipeline defaults (release, paths, file types, PUMS)                                                                    |
| `data/resstock/constants.py`                           | Column-name constants for validation                                                                                    |
| `data/resstock/manifest.py`                            | Provenance: run records, YAML I/O, status CLI                                                                           |
| `data/resstock/validations.py`                         | Post-step validation (local files, S3 objects, metadata schema)                                                         |
| `data/resstock/nrel/fetch_resstock_data.py`            | bsf wrapper                                                                                                             |
| `data/resstock/nrel/copy_resstock_data.py`             | Directory copy utility (`copy_dir`)                                                                                     |
| `data/resstock/metadata/identify_hp_customers.py`      | Adds `postprocess_group.has_hp`                                                                                         |
| `data/resstock/metadata/identify_heating_type.py`      | Adds heating-type and fuel-flag columns                                                                                 |
| `data/resstock/metadata/identify_natgas_connection.py` | Adds `has_natgas_connection` from `load_curve_annual`                                                                   |
| `data/resstock/metadata/add_vulnerability_columns.py`  | Adds LMI vulnerability columns from PUMS                                                                                |
| `data/resstock/load_curve/approximate_non_hp_load.py`  | Re-exports from `utils/pre/approximate_non_hp_load.py`                                                                  |
| `utils/pre/approximate_non_hp_load.py`                 | Core approximation: neighbor search, HVAC replacement, metadata update                                                  |
| `data/resstock/load_curve/adjust_mf_electricity.py`    | Re-exports from `utils/pre/adjust_mf_electricity.py`                                                                    |
| `utils/pre/adjust_mf_electricity.py`                   | MF non-HVAC electricity scaling, per-building hourly adjustment                                                         |
| `data/resstock/state_configs.yaml`                     | Per-state config: FIPS, CRS, polygon filenames, excluded gas utilities                                                  |
| `data/resstock/validations.py`                         | Post-step and pre-flight validation (local files, S3 objects, utility assignment)                                       |
| `data/resstock/utility/utils.py`                       | State-generic GIS helpers: PUMA overlap, probability tables, sampling, diagnostics                                      |
| `data/resstock/utility/assign_utility.py`              | Central utility assignment facade; loads `state_configs.yaml`, routes to NY/RI impl                                     |
| `data/resstock/utility/assign_utility_ny.py`           | NY-specific thin wrapper: builds name map, passes excluded gas utilities to generic `create_hh_utilities` in `utils.py` |
| `data/resstock/utility/assign_utility_ri.py`           | Deterministic utility assignment for RI (single utility)                                                                |
| `data/resstock/load_curve/add_monthly_loads.py`        | Hourly-to-monthly aggregation; called directly by `_add_monthly_loads` (step 2d)                                        |

---

## Known limitations and TODO items

1. **No `load_curve_annual` in `_sb`**: Intentional. See the `_SB_EXCLUDED_FILE_TYPES` section above for how to change this if needed.

2. **`has_natgas_connection` has two sources of truth**: For non-approximated buildings, it comes from `load_curve_annual` in the raw release (step 2a). For approximated buildings, it is re-derived from the modified `load_curve_hourly` in `_sb` (step 2c-i). This is correct behavior but worth understanding when debugging metadata values.

3. **k and include_cooling are hardcoded**: `_approximate_non_hp_load` uses `k=15` and `include_cooling=False`. These should eventually become CLI arguments if they need to vary.

4. **Utility assignment only supports NY and RI**: `SUPPORTED_UTILITY_STATES` is derived dynamically from `data/resstock/state_configs.yaml` — any state whose config entry contains both `electric_poly_filename` and `gas_poly_filename` keys is included. Adding a new state requires: (a) adding an entry to `state_configs.yaml` with those keys (null values for rule-based, filenames for GIS-based), and (b) implementing a state-specific assignment function and wiring it in `assign_utility.py`.

5. **Monthly loads in sample mode produce N files**: When `--sample N` is active, only N hourly parquets exist locally, so only N monthly parquets are generated. This is expected — sample mode is for development/testing only.
