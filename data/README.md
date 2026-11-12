# Data pipeline conventions

Each directory under `data/` is one dataset, named `data/<org>/<dataproduct>/` (for example `eia/861`, `hud/ami`, `fred/cpi`, `nrel/cambium`) so it matches `s3://data.sb/<org>/<dataproduct>/`.

Recipe names, path variables, and the fetch-then-upload order are the `data-pipeline` skill (`.cursor/skills/data-pipeline/SKILL.md`). The notes below are the exceptions and the existing pipelines that skill does not list.

## Exceptions

- EIA zone loads (`data/eia/hourly_loads/`) write directly to S3 from the fetch. Other pipelines keep upload as a separate step.
- HUD Section 8 income limits live in `data/hud/ami/` only. Do not add a second copy.
- `path_local_parquet` is sometimes the BSF output directory, when that directory holds the parquet.
- `path_local_base` is the pipeline directory. Census PUMS sets it with `justfile_directory()`.
- `path_local_fpl_yaml` is a single file, the FPL guidelines YAML, not a staging directory.
- `path_s3_zone_parquet` and `path_s3_utility_parquet` are the two roots in the EIA zone-loads pipeline (zone loads vs utility loads).

## Where `validate` and `update` are used

`validate` is in the ISO-NE, NYISO, and EIA heating-fuel pipelines. Wire it into `prepare` when the pipeline has it.

`update` lists existing partitions on S3 (`aws s3 ls … --recursive`) and in local staging, takes the latest watermark, and fetches only missing periods through the last complete calendar month. The script derives that range. For an explicit range, use `fetch` with `start`/`end`. It does not upload. Pipelines that do this today:

- `data/eia/heating_fuel_prices/`
- `data/isone/ancillary/`
- `data/isone/lmp/`
- `data/isone/hourly_demand/`
- `data/isone/capacity/ara/`
- `data/isone/capacity/mra/`
- `data/nyiso/lbmp/`
- `data/nyiso/icap/`

## Script names in use

`fetch_smi_json.py`, `fetch_ami_xlsx.py`, `fetch_cpi_parquet.py`, `fetch_zone_loads_parquet.py`, `fetch_electric_utility_stat_parquets.py`, `fetch_fpl_yaml.py`, `convert_hud_smi_json_to_parquet.py`, `convert_cambium_csv_to_parquet.py`, `validate_isone_ancillary_parquet.py`, `validate_fca_reference.py`.

Parallel fetch in this repo: `data/nyiso/lbmp/fetch_lbmp_zonal_zips.py` (`concurrent.futures.ThreadPoolExecutor`).

## EIA-861 layout

`data/eia/861/` writes `s3://data.sb/eia/861/electric_utility_stats/` with Hive partitions `year=<year>/state=<state>/data.parquet`. Readers must include the year, for example `year=2024/state=NY/data.parquet`, not `state=NY/data.parquet`.

## Gitignore

Local staging directories are gitignored. Examples: `data/fred/cpi/parquet/`, `data/eia/861/parquet/`, `data/hud/ami/xlsx/`, `data/hud/ami/parquet/`, `data/census/pums/zips/`.
