# Marginal cost 8760 provenance manifests

Every marginal cost (MC) 8760 lives at one standard path per type, utility, and year, for example
`s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/utility=ct_eversource/year=2025/data.parquet`.
Several methods can write that same path. A `manifest.yaml` in the same directory records which
method produced the file that is there now.

Implementation: `utils/data_prep/marginal_costs/mc_manifest.py`.

## How it works

1. **Writers append.** Each writer, after it writes a parquet, appends one entry to `manifest.yaml`
   beside it (S3 and, when the writer also writes a local mirror, the local copy). Nothing is ever
   removed, so the manifest is the full history of that partition.
2. **The latest entry wins.** Entries name the file they describe (`file: data.parquet` or
   `file: zero.parquet`). The last entry for a file name describes the file currently at that path.
   There is no separate ID: the key plus the file's sha256 identify it.
3. **The pipeline YAML declares the key it expects.** A `marginal_costs` entry is either a bare
   path (not verified; preflight logs a warning) or `{path, key}`:

   ```yaml
   marginal_costs:
     dist_and_sub_tx:
       path: s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/utility=ct_eversource/year=2025/data.parquet
       key: pop_occ863att3_100h_load2025_2026usd
   ```

   Preflight (`validate_marginal_cost_manifests` in `rate_design/hp_rates/pipeline_config.py`) stops
   the batch when the file is missing, has no manifest entry, its latest entry's key differs from
   the declared key, or its sha256 differs from the one recorded in that entry (the file was
   changed without going through a writer). When `supply_energy` or `supply_capacity` declares a
   key and points at `data.parquet`, delivery runs read the `zero.parquet` beside it, so preflight
   also requires that file to carry `key: zero`.
4. **Each CAIRO run records what it read.** `run_scenario.py` hashes every MC file before loading
   it and writes `marginal_costs_used.json` to the run output directory: path, sha256, and the
   latest manifest entry's key, method, source, `written_at`, `git_commit`, and whether the hash
   matches the manifest (`matches_manifest`).

## Entry fields

| Field                     | Meaning                                                                                                                                                                                                                                                                                                                           |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `file`                    | File name in the partition the entry describes                                                                                                                                                                                                                                                                                    |
| `key`                     | Structured key built from `params` (tables below)                                                                                                                                                                                                                                                                                 |
| `mc_type`                 | `dist_and_sub_tx`, `bulk_tx`, `supply_energy`, `supply_capacity`, or `supply_ancillary`                                                                                                                                                                                                                                           |
| `method`                  | Method name; together with `mc_type` it picks the key builder                                                                                                                                                                                                                                                                     |
| `utility`, `year`         | Partition values. `year` is the year stamped on the timestamps (for CT, the 2025 test year). For `month_hour`, the day-type calendar is `params.calendar_year`, which may differ                                                                                                                                                  |
| `source`                  | Citation for the cost, e.g. "PURA Docket 26-05-10, MCOS-2 Table 3 secondary ..." (`--source`)                                                                                                                                                                                                                                     |
| `file_sha256`             | sha256 of the parquet as written                                                                                                                                                                                                                                                                                                  |
| `written_at`              | UTC timestamp of the write                                                                                                                                                                                                                                                                                                        |
| `params`, `params_sha256` | Every structured parameter of the method, including ones not in the key (e.g. the CPI factor), and their hash                                                                                                                                                                                                                     |
| `inputs`                  | One record per input: `role`, `path`, `sha256`. A hive partition input (a directory) hashes each parquet's relative path and content hash, and records `n_files`                                                                                                                                                                  |
| `summary`                 | `rows`, `value_column`, `sum` (the annual `$/kW-yr` a flat 1 kW load pays), `max`, `min`, `nonzero_hours`                                                                                                                                                                                                                         |
| `provenance`              | `script`, the full `command`, `git_commit`, `git_branch`, `git_dirty`, `git_dirty_files`. When the tree is dirty, also `git_diff` (`git diff HEAD` for `*.py` and `*Justfile`) and `git_untracked_code` (contents of untracked `.py` files), so the exact code can be recovered. Commit before uploading to keep manifests short. |

## Methods

| `mc_type`         | `method`         | Writer                                                                      | What it does                                                                                                                                                                                                                                               |
| ----------------- | ---------------- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dist_and_sub_tx` | `pop`            | `generate_utility_tx_dx_mc.py --method pop`                                 | Probability of peak: one `$/kW-yr` from the MC table CSV (CPI-adjusted to the target dollar year when its `dollar_year` differs) spread over the top `n_hours` of a load profile, weighted by load.                                                        |
| `dist_and_sub_tx` | `month_hour`     | `generate_utility_tx_dx_mc.py --method month_hour`                          | Looks up a utility's month-hour `$/kWh` tables (weekday; weekend and holiday) onto the `calendar_year` calendar, stamped as `year`, then multiplies every hour by one factor so the 8760 sums to the MC table's `$/kW-yr`. CT only (OCC-863 Attachment 2). |
| `bulk_tx`         | `isone_aesc_ptf` | `generate_bulk_tx_mc.py --iso isone`                                        | AESC avoided PTF `$/kW-yr` spread over the top `n_peak_hours` of the allocation load by exceedance weighting.                                                                                                                                              |
| `bulk_tx`         | `pjm_nits_pcaf`  | `generate_bulk_tx_mc.py --iso pjm`                                          | Blended PJM NITS rate allocated by PCAF load share over the top `k_peak_hours` of utility load.                                                                                                                                                            |
| `bulk_tx`         | `nyiso_scr`      | `generate_bulk_tx_mc.py --iso nyiso`                                        | NYISO constraint-group costs allocated over `scr_hours_per_season` SCR hours per season on nested-locality loads.                                                                                                                                          |
| any               | `zero`           | `generate_zero_supply_mc.py`, and the supply energy and capacity generators | All-zero placeholder (`zero.parquet`) for delivery-only runs, or for utilities with no real supply MC (CT).                                                                                                                                                |

Real supply energy, capacity, and ancillary 8760s (`data.parquet` from `generate_supply_*_mc.py`)
do not write manifest entries yet. Adding one means registering a key builder in
`mc_manifest.KEY_BUILDERS`, building an `McSpec` in the generator, calling `record_mc_write` after
the write, and adding a row to the tables here.

## Keys

| `method`         | Key pattern                                                                     | Fields                                                                                                                                                                                                                                                                                                                                               | Example                                            |
| ---------------- | ------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| `pop`            | `pop_{load_source}_{n_hours}h_load{load_year}_{target_dollar_year}usd`          | `load_source`: `--load-source`, or the load file's stem, or `<iso>_utility` for a hive base. `n_hours`: `--n-hours`. `load_year`: `--load-year`. `target_dollar_year`: `--target-dollar-year`                                                                                                                                                        | `pop_occ863att3_100h_load2025_2026usd`             |
| `month_hour`     | `month_hour_{table_source}_{voltage}_{holidays\|noholidays}_cal{calendar_year}` | `table_source`: `--table-source` (default `occ863att2`). `voltage`: `--voltage`, or read from the table file names. Holidays: `--include-holidays`. `calendar_year`: `--calendar-year` (default `--year`), the calendar that classifies weekdays, weekends, and holidays; `params` also records `timestamp_year` and `calendar_to_timestamp_mapping` | `month_hour_occ863att2_secondary_holidays_cal2025` |
| `isone_aesc_ptf` | `aesc_ptf{ptf_kw_year}_{allocation_load}_{n_peak_hours}h_load{load_year}`       | `ptf_kw_year`: `--aesc-ptf-kw-year`, with `.` written as `p` (`184.5` is `184p5`). `allocation_load`: `ne_system` or `utility_zone`                                                                                                                                                                                                                  | `aesc_ptf69_ne_system_100h_load2025`               |
| `pjm_nits_pcaf`  | `pjm_nits_pcaf_{k_peak_hours}h_load{load_year}`                                 | `k_peak_hours`: `--k-peak-hours`                                                                                                                                                                                                                                                                                                                     | `pjm_nits_pcaf_150h_load2025`                      |
| `nyiso_scr`      | `nyiso_scr_{scr_hours_per_season}h_load{load_year}`                             | `scr_hours_per_season`: `--scr-hours-per-season`                                                                                                                                                                                                                                                                                                     | `nyiso_scr_40h_load2024`                           |
| `zero`           | `zero`                                                                          | none                                                                                                                                                                                                                                                                                                                                                 | `zero`                                             |

The key holds the choices that distinguish one method run from another at the same path. Values
that come from the inputs (e.g. the `$21.22/kW-yr` scalar, which is in the MC table CSV) are in
`params` and in the input hashes instead.

### CT Eversource keys

The ct Justfile's `create-dist-mc-data` writes all three methods to
`dist_and_sub_tx/utility=ct_eversource/year=<year>/data.parquet`, with `--load-source`,
`--table-source`, and `--source` set per method:

| `dist_mc_method` | Key (year 2025, `upstream_hours=100`)              |
| ---------------- | -------------------------------------------------- |
| `month_hour`     | `month_hour_occ863att2_secondary_holidays_cal2025` |
| `pop_occ863`     | `pop_occ863att3_100h_load2025_2026usd`             |
| `pop_isone_zone` | `pop_isone_zone_100h_load2025_2026usd`             |

`include_holidays=false` changes the `month_hour` key to `..._noholidays_cal2025`. The calendar
year is 2025, not the AMY2018 weather year, because CAIRO rotates the 2018 loads onto the run
year's weekdays (see the methodology doc §7.2). `month_hour_calendar_year=<y>` overrides it. The bulk TX file
(`create-bulk-tx-mc-data`) is `aesc_ptf69_ne_system_100h_load2025`, and the supply placeholders
(`create-zero-supply-mc-data`) are `zero`. Methodology: `context/methods/marginal_costs/ct_eversource_dist_mc_methodology.md`.
