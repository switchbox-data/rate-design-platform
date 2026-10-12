# Marginal cost 8760 provenance manifests

Every marginal cost (MC) 8760 lives at one standard path per type, utility, and year, for example
`s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/utility=ct_eversource/year=2025/data.parquet`.
Several methods can write that same path. A `manifest.yaml` in the same directory records which
method produced the file that is there now.

Implementation: `utils/data_prep/marginal_costs/mc_manifest.py`.

## How it works

0. **Uploads need published code.** Every generator, when run with `--upload`, first calls
   `require_published_code()` and stops unless all of these hold:
   - the working tree is clean (no modified, staged, or untracked files);
   - `git fetch origin` succeeds;
   - some `origin` branch contains `HEAD`.

   It then creates the tag `mc/<first 12 of the commit sha>` on `HEAD` and pushes it to `origin`.
   The tag keeps the commit reachable even if its branch is later rebased, force-pushed, or
   deleted. `record_mc_write` runs the same check, so a writer that skips the upfront call still
   cannot record an entry. Runs without `--upload` are not checked. Because of this, every entry's
   `git_commit` (or `git_tag`) plus its `command` rebuilds the file. To verify a file, check out the
   tag and re-run the command without `--upload`; with `--upload` it would overwrite the shared
   file and append a new entry. Inputs on S3 can still change after the fact; their hashes in
   `inputs` show whether they did.
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

   For each file that passes, preflight also checks whether the latest entry's `git_commit` is an
   ancestor of (or equal to) the checkout's `HEAD` (`commit_in_history`, fetching tags from
   `origin` when the commit is not local). This is a **warning, never an error**. A key names a
   method, not a version of its code, so another branch can change how a key is built and upload
   under the same key. The warning says that the file was built by code this branch doesn't have.
   "Cannot tell" (no commit recorded, or not fetchable) is also a warning.
4. **Each CAIRO run records what it read.** `run_scenario.py` hashes every MC file before loading
   it and writes `marginal_costs_used.json` to the run output directory:

   ```json
   {
     "run_git": {"git_commit": "...", "git_branch": "...", "git_dirty": false, "git_dirty_files": []},
     "marginal_costs": {
       "dist_and_sub_tx": {
         "path": "s3://...", "sha256": "...", "key": "...", "method": "...", "source": "...",
         "written_at": "...", "git_commit": "...", "git_tag": "mc/...",
         "matches_manifest": true, "sha256_in_manifest": true,
         "generator_commit_in_run_history": true,
         "manifest_entry": {"...": "the full manifest entry"}
       }
     }
   }
   ```

   `run_git` is the commit the run itself ran from (dirty or not; runs are not required to be
   clean). `manifest_entry` is the full entry whose `file_sha256` equals the bytes the run read,
   including `command`, `params`, and `inputs`, so the run output alone is enough to rebuild that
   8760. `sha256_in_manifest` is true when some entry has that sha256, and `matches_manifest` when
   that entry is also the latest. When no entry matches, the latest entry is recorded with both
   false. `generator_commit_in_run_history` is `true`, `false`, or `null` (unknown, e.g. the commit
   is not in the run's clone); a `false` is also logged as a warning. The run does not fetch.

## Rebuilding an MC 8760 from a run's record

Start from a CAIRO run directory, e.g.
`s3://data.sb/switchbox/cairo/outputs/hp_rates/ct/ct_eversource/<batch>/<cairo_ts>_<run_name>/`
(also mounted at `/data.sb/...`). Its `marginal_costs_used.json` names the exact file the run
read and the code that built it. The steps below rebuild that file and check it, without
touching the shared path on S3.

1. **Read the record.** Pick the MC (`dist_and_sub_tx`, `bulk_tx`, ...) and pull out the tag,
   the command, and the recorded summary:

   ```bash
   RUN_DIR=/data.sb/switchbox/cairo/outputs/hp_rates/ct/ct_eversource/<batch>/<run>
   jq '.marginal_costs.dist_and_sub_tx | {sha256, key, git_tag, matches_manifest,
       command: .manifest_entry.provenance.command, summary: .manifest_entry.summary,
       inputs: .manifest_entry.inputs}' "$RUN_DIR/marginal_costs_used.json"
   ```

   `git_tag` looks like `mc/2bcbd2301664`. For entries written before the upload guard, it is
   `null`; use `git_commit` instead. `command` is the full `uv run python ...` line, including
   `--upload` and the shared `--output-s3-base`.

2. **Check out that code in a separate worktree**, so the current checkout is undisturbed:

   ```bash
   cd "$(git rev-parse --show-toplevel)"
   git fetch --tags origin
   git worktree add /tmp/mc-rebuild mc/2bcbd2301664   # or the git_commit sha
   cd /tmp/mc-rebuild && uv sync --locked
   ```

3. **Re-run the command without `--upload`.** Dropping `--upload` makes the generator compute
   and print the 8760 but write nothing, so it cannot overwrite the shared file or append a
   manifest entry. Keep every other argument exactly as recorded:

   ```bash
   uv run python utils/data_prep/marginal_costs/generate_utility_tx_dx_mc.py \
     --state CT --utility ct_eversource --year 2025 ... \
     # same arguments as `command`, minus --upload
   ```

   The printout includes the sum over the 8760, the number of nonzero hours, and the top hours.
   Compare them with `manifest_entry.summary` (`sum`, `nonzero_hours`, `max`, `min`). Equal
   values mean the code at that tag still produces the recorded file.

4. **If they differ, check the inputs first.** Each `manifest_entry.inputs[*]` has a `path` and
   the `sha256` it had at upload time. Re-hash the current file at that path
   (`mc_manifest.file_sha256(path)` or `aws s3 cp ... - | sha256sum`). A changed hash means the
   source data on S3 moved, not the code; the manifest cannot restore those bytes.

5. **For a byte-for-byte check**, the rebuilt file has to be written somewhere other than the
   live path. Steps 1–4 work today. A dedicated archive location for these rebuilds is planned
   (see "Archive path for rebuilds (planned)" below). Until it exists, run with `--upload` but
   point both output bases at a scratch location, e.g.
   `--output-s3-base s3://data.sb/scratch/<you>/mc/` and
   `--path-local-output-base /tmp/mc-rebuild-out/`. The upload guard still applies (the
   worktree is at a pushed tag, so it passes), and the file and a manifest entry land in
   scratch, not at the shared path. Compare the new file's sha256 with the run's `sha256`.
   Parquet writer metadata can change with a polars upgrade, so if the summaries match but
   the sha256 differs, compare the `timestamp` and value columns instead.

6. **Clean up:** `git worktree remove /tmp/mc-rebuild`.

In short: `marginal_costs_used.json` → `manifest_entry.provenance.git_tag` and `command` →
check out the tag → re-run the command without `--upload` → compare with
`manifest_entry.summary`.

### Archive path for rebuilds (planned)

**Status: not implemented.** This describes how rebuilds of old MC 8760s should be stored. It
is planned for a separate PR; the generators do not do this yet.

A rebuilt 8760 goes under an `archive/` folder beside the live data, with a `tag=` partition
for the commit that built it. The rest of the path matches the live path:

```text
live:    s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/utility=ct_eversource/year=2025/data.parquet
archive: s3://data.sb/switchbox/marginal_costs/ct/archive/dist_and_sub_tx/tag=mc_2bcbd2301664/utility=ct_eversource/year=2025/data.parquet

live:    s3://data.sb/switchbox/marginal_costs/ct/supply/capacity/utility=ct_eversource/year=2025/zero.parquet
archive: s3://data.sb/switchbox/marginal_costs/ct/archive/supply/capacity/tag=mc_2bcbd2301664/utility=ct_eversource/year=2025/zero.parquet
```

Why this layout:

- **`archive/` sits beside the live MC folders** (`dist_and_sub_tx/`, `bulk_tx/`, `supply/`), not
  inside them. A scan of a live partition, or a pipeline path, never picks up an archived file.
- **`tag=` keeps every rebuild.** Its value is the entry's `mc/<sha12>` tag with `/` replaced by
  `_`, since `/` cannot appear in a partition value. Without it, `archive/.../year=2025/data.parquet`
  would be a second mutable path, and each rebuild would replace the last. Rebuilding the same
  tag writes the same object, which is correct when the bytes match.
- **The manifest goes with it.** The rebuild's `manifest.yaml` entry lands in the archive
  directory, next to the file it describes. The live manifest is not touched.

When it is implemented, the behavior should be:

- An explicit flag on every generator (e.g. `--rebuild`) is the only thing that writes to the
  archive. It requires `--upload`, and the upload guard still runs.
- With that flag, every write in the run (S3 and the local mirror) goes to the archive path for
  the current commit's tag. The live path is never written.
- A plain `--upload` behaves as it does today and writes only the live path. Production uploads
  are not copied to the archive automatically; that would be a further, separate decision.

With this in place, step 5 above becomes: check out the tag, re-run the recorded command with
`--rebuild` added, and compare the archived file's sha256 with the run's `sha256`.

## Entry fields

| Field                     | Meaning                                                                                                                                                                                                                                                                                                                                                        |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `file`                    | File name in the partition the entry describes                                                                                                                                                                                                                                                                                                                 |
| `key`                     | Structured key built from `params` (tables below)                                                                                                                                                                                                                                                                                                              |
| `mc_type`                 | `dist_and_sub_tx`, `bulk_tx`, `supply_energy`, `supply_capacity`, or `supply_ancillary`                                                                                                                                                                                                                                                                        |
| `method`                  | Method name; together with `mc_type` it picks the key builder                                                                                                                                                                                                                                                                                                  |
| `utility`, `year`         | Partition values. `year` is the year stamped on the timestamps (for CT, the 2025 test year). For `month_hour`, the day-type calendar is `params.calendar_year`, which may differ                                                                                                                                                                               |
| `source`                  | Citation for the cost, e.g. "PURA Docket 26-05-10, MCOS-2 Table 3 secondary ..." (`--source`)                                                                                                                                                                                                                                                                  |
| `file_sha256`             | sha256 of the parquet as written                                                                                                                                                                                                                                                                                                                               |
| `written_at`              | UTC timestamp of the write                                                                                                                                                                                                                                                                                                                                     |
| `params`, `params_sha256` | Every structured parameter of the method, including ones not in the key (e.g. the CPI factor), and their hash                                                                                                                                                                                                                                                  |
| `inputs`                  | One record per input: `role`, `path`, `sha256`. A hive partition input (a directory) hashes each parquet's relative path and content hash, and records `n_files`                                                                                                                                                                                               |
| `summary`                 | `rows`, `value_column`, `sum` (the annual `$/kW-yr` a flat 1 kW load pays), `max`, `min`, `nonzero_hours`                                                                                                                                                                                                                                                      |
| `provenance`              | `script`, the full `command`, `git_commit`, `git_branch`, `git_tag` (`mc/<sha12>`, pushed), `git_remote`, `git_remote_url`, and `git_remote_branches` (the `origin` branches containing the commit at upload time). Entries written before the upload guard instead have `git_dirty`, `git_dirty_files`, and, when dirty, `git_diff` and `git_untracked_code`. |

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
do not write manifest entries yet, though their `--upload` runs already require published code.
Adding an entry means registering a key builder in `mc_manifest.KEY_BUILDERS`, building an
`McSpec` in the generator, calling `record_mc_write` after the write, and adding a row to the tables
here. A new generator should call `require_published_code()` right after parsing arguments when
`--upload` is set, so it fails before computing anything.

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
