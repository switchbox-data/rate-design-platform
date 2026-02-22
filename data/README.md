# Data pipeline conventions

Scripts in `data/` fetch, convert, and upload datasets to S3. Each subdirectory (e.g. `data/cambium/`, `data/hud/ami/`) is one pipeline with its own Justfile.

**Directory naming**: Use `org/dataproduct` (e.g. `eia/861`, `hud/ami`, `fred/cpi`, `nrel/cambium`). This matches the S3 path under the bucket: `s3://data.sb/<org>/<dataproduct>/` (e.g. `s3://data.sb/eia/861/`, `s3://data.sb/hud/ami/`).

**When adding or changing a data pipeline, follow the conventions below** so all pipelines stay consistent and LLMs/tools can follow the same patterns.

## Pipeline shape

- **Fetch and convert locally**; **upload to S3 as a separate step**. Do not upload from inside the fetch/convert script unless the pipeline is special (e.g. EIA zone loads write directly to S3). Prefer:
  - `fetch` (and optionally `convert`) → write to local staging dirs (e.g. `parquet/`, `csv/`, `json/`, `xlsx/`, `zips/`).
  - `upload` → `aws s3 sync` (or equivalent) from local staging to S3.
- Optional **`prepare`** recipe: fetch + convert (upload is then a separate step).
- **Clean**: Every pipeline that creates local staging directories **must** have a **`clean`** recipe that removes those directories. Use the same path variables as in fetch/convert/upload so one place defines the paths.

## Recipe names

- **`fetch`** — main recipe that downloads (and optionally converts to parquet, if it can be done in one go) data into local staging.
- **`test-download`** — optional; verify that a previous download is present/correct (e.g. ResStock).
- **`convert`** — convert raw staging (e.g. csv, json, xlsx) to parquet (or other final format) locally, used when the conversion can't be done in the fetch, because fetch involves getting separate raw files.
- **`upload`** — sync local output to S3 (no upload inside fetch/convert scripts unless justified).
- **`prepare`** — fetch + convert (no upload).
- **`clean`** — remove all local staging dirs for this pipeline.

## Path variables (Justfile)

Use consistent names so every pipeline looks the same:

- **`path_local_repo`** — repo root: `` `git rev-parse --show-toplevel` ``. Use for script paths (e.g. `{{path_local_repo}}/data/.../script.py`).
- **`path_local_*`** — local dirs/files:
  - `path_local_parquet` — directory for parquet output (or BSF/output dir when it holds parquet).
  - `path_local_csv`, `path_local_json`, `path_local_xlsx`, `path_local_zip` — staging dirs by format.
  - `path_local_base` — pipeline directory (e.g. `justfile_directory()` in census/pums).
  - `path_local_fpl_yaml` — single output file (e.g. FPL guidelines YAML).
- **`path_s3_*`** — S3 locations:
  - **`path_s3_parquet`** — S3 prefix for the parquet dataset (partition root). Use for upload destination and for scripts that read/write that dataset.
  - **`path_s3_zone_parquet`** / **`path_s3_utility_parquet`** — when a pipeline has two distinct parquet roots (e.g. EIA zone vs utility loads).

Do **not** use ad hoc names like `project_root`, `s3_base`, `local_parquet_dir` — use the names above. Define path variables at the top of the Justfile and use them in every recipe (fetch, convert, upload, clean).

## Script names

- **Fetch scripts**: `fetch_<dataproduct>_<dataformat>.py`
  - Examples: `fetch_smi_json.py`, `fetch_ami_xlsx.py`, `fetch_cpi_parquet.py`, `fetch_zone_loads_parquet.py`, `fetch_electric_utility_stat_parquets.py`, `fetch_fpl_yaml.py`.
- **Convert scripts**: `convert_<source>_to_parquet.py` or similar (e.g. `convert_hud_smi_json_to_parquet.py`, `convert_cambium_csv_to_parquet.py`).

Scripts live in the same directory as the Justfile that invokes them.

## Script CLI and Justfile

- Pass **absolute paths** (or paths from path variables) to scripts; **do not rely on `cd`** in the Justfile to set the working directory. Scripts should work when given paths as arguments.
- Prefer CLI argument names that match the Just variables (e.g. `--path-s3-zone-parquet`, `--path-local-parquet`) so the Justfile and script stay aligned.
- **Safeguard**: If a script accepts an output directory (e.g. `--output-dir`), reject values that look like uninterpolated Just (e.g. contain `{{` or `}}`) so we never write into a literal `{{path_local_parquet}}` directory.

## Parallelism

- **Fetch and convert in parallel when possible** (e.g. many independent URLs or many files to convert). Use a thread pool or process pool so full-range runs finish in reasonable time.
- Prefer a configurable concurrency limit (e.g. `--workers N`) so runs can be tuned for the host and to avoid overloading the source or disk.
- For reference implementations in this repo, see e.g. `data/nyiso/lbmp/fetch_lbmp_zonal_zips.py` (parallel fetch via `concurrent.futures.ThreadPoolExecutor`).

## One dataset per directory

- One pipeline per dataset (or logical product). Do not duplicate; e.g. HUD Section 8 income limits live in `data/hud/ami/` only.

## S3 partition layouts (notable)

- **EIA-861 electric utility stats** (`data/eia/861/`): Output is at `s3://data.sb/eia/861/electric_utility_stats/` with Hive-style partitions **`year=<year>/state=<state>/data.parquet`**. Downstream consumers must point at a specific year, e.g. `year=2024/state=NY/data.parquet` (not `state=NY/data.parquet`).

## .gitignore

- Ignore local staging dirs (e.g. `data/fred/cpi/parquet/`, `data/eia/861/parquet/`, `data/hud/ami/xlsx/`, `data/hud/ami/parquet/`, `data/census/pums/zips/`, etc.) so they are never committed.
