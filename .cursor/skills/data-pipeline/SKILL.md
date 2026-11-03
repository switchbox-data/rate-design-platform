---
name: data-pipeline
description: >-
  Add or change a dataset pipeline under data/ in rate-design-platform: Just
  recipes, fetch and convert scripts, local staging, validation, and S3 upload.
  Use when creating a data subdirectory, editing a data Justfile, or naming
  fetch, convert, validate, update, upload, prepare, or clean recipes.
---

# Data pipeline

One dataset per directory, named `data/<org>/<dataproduct>/` so it matches `s3://data.sb/<org>/<dataproduct>/`. The Justfile in that directory is the interface. Scripts live beside it.

A ResStock `_sb` release is the `resstock-sb-release` skill. This recipe table does not apply to it.

`data/README.md` points here for the procedure. It also keeps the notes this file does not: the EIA zone-load upload exception in more detail, `path_local_fpl_yaml`, which pipelines use `update`, the EIA-861 object path, and the script filenames already in the repo. Read that file when the pipeline is one of those.

## Output

- Local staging only (parquet, csv, json, xlsx, or zips). Nothing uploaded from inside fetch or convert, except a pipeline that already writes straight to S3 (EIA zone loads).
- After a separate upload step, a Parquet dataset at the pipeline's `path_s3_*` prefix.
- A `clean` recipe that deletes the staging directories.
- Staging paths listed in `.gitignore`.

Done means `prepare` (or `fetch`, then `convert`, then `validate`) succeeds locally, `upload` succeeds, and `clean` removes staging.

## Procedure

1. Copy the shape of a nearby pipeline (for example `data/fred/cpi/` or `data/hud/ami/`) rather than inventing recipe names.
2. Write fetch and convert so they only touch local paths passed as arguments. Reject an output directory whose value still contains `{{` or `}}`.
3. Add `validate` when the pipeline should check schema, nulls, row counts, or partitions before upload. Validation reads local files and does not write to S3. Exit non-zero on failure.
4. Wire `prepare` as fetch, then convert when raw staging is separate, then validate when it exists. `prepare` does not upload.
5. Wire `upload` as `aws s3 sync` (or the existing equivalent) from local staging to `path_s3_parquet`.
6. Wire `clean` with the same path variables, deleting every local staging directory this pipeline creates.
7. For a Hive dataset partitioned by time, add `update` only when the pipeline should fill gaps since the latest S3 (and local) partition. The script derives the range. `update` does not upload.
8. Fetch and convert independent files in a pool with a `--workers` limit. See `data/nyiso/lbmp/fetch_lbmp_zonal_zips.py`.

Run scripts with `uv run python`. Pass absolute paths from the Just variables. Do not `cd` in the recipe to make a relative path work.

## Recipe names

| Recipe          | Does                                                              | Does not |
| --------------- | ----------------------------------------------------------------- | -------- |
| `fetch`         | Download, and convert in the same step when that is one operation | Upload   |
| `convert`       | Turn raw staging into local Parquet when fetch cannot             | Upload   |
| `validate`      | QA local output                                                   | Touch S3 |
| `prepare`       | fetch, then convert, then validate                                | Upload   |
| `upload`        | Sync local output to S3                                           | Fetch    |
| `update`        | Fetch and convert only missing periods                            | Upload   |
| `clean`         | Remove local staging                                              | Touch S3 |
| `test-download` | Check a prior download (optional; ResStock uses this)             | Upload   |

## Names

Just path variables, defined at the top of the Justfile and used in every recipe:

- `path_local_repo` — `` `git rev-parse --show-toplevel` ``, for script paths.
- `path_local_parquet`, `path_local_csv`, `path_local_json`, `path_local_xlsx`, `path_local_zip` — staging by format.
- `path_local_base` — the pipeline directory, when a pipeline uses that name.
- `path_s3_parquet` — the S3 prefix uploaded to and scanned later.
- `path_s3_zone_parquet` and `path_s3_utility_parquet` — only when one pipeline has two Parquet roots.

Do not invent names like `project_root`, `s3_base`, or `local_parquet_dir`.

Script filenames:

- `fetch_<dataproduct>_<dataformat>.py`
- `convert_<source>_to_parquet.py`
- `validate_<dataproduct>_parquet.py`
- `update_<dataproduct>_to_latest.py`

CLI flags use the same words as the Just variables (`--path-s3-parquet`, `--path-local-parquet`). A path argument is `path_*` in argparse, or a long option that says it is a path (`--output-dir`).

## S3 object names

Prefer Parquet. A single-file dataset uses a lowercase underscore name ending in `_YYYYMMDD` (the download date): `s3://data.sb/<org>/<dataset>/<filename_YYYYMMDD.parquet>`. Hive datasets keep the partition layout the pipeline already documents (EIA-861 is `year=<year>/state=<state>/data.parquet`; readers must include `year=`).
