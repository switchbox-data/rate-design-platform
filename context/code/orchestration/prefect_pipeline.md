# Prefect pipeline for hp_rates (WIP)

Status: **work in progress** — covers the default scenario quartet (runs 1-4) today. Multi-scenario support (runs 5-8, evaluator/designer tasks, allocation-aware naming) is designed but not yet implemented. See `pipeline_evaluators_designers_merged` plan.

## What this replaces

The Justfile shell orchestration (`just run-1` ... `just run-all-sequential`) with a Python-native Prefect pipeline that:

- Calls CAIRO's `run_scenario.run()` directly instead of shelling out
- Derives all file paths from a compact YAML config and naming conventions
- Passes output paths between tasks explicitly (no S3 output scanning via `latest_run_output.sh`)
- Uses Prefect caching instead of manual S3 duplicate guards

## Files

| File                                                         | Purpose                                                                 |
| ------------------------------------------------------------ | ----------------------------------------------------------------------- |
| `rate_design/hp_rates/run_pipeline.py`                       | Prefect pipeline: config loader, settings derivation, tasks, flows, CLI |
| `rate_design/hp_rates/ri/config/scenarios/pipeline_rie.yaml` | Compact pipeline YAML for RIE                                           |
| `rate_design/hp_rates/md/config/scenarios/pipeline_bge.yaml` | Compact pipeline YAML for BGE                                           |

## Current architecture (default scenario only)

```
hp_rates_pipeline (flow)
  └─ quartet (subflow)
       ├─ precalc_task  ──  delivery + supply in parallel (concurrent.futures)
       │    └─ extracts calibrated tariffs from tariff_final_config.json
       └─ calibrated_task  ──  delivery + supply in parallel
            └─ depends on precalc tariff output (explicit Prefect dependency)
```

Each task runs two CAIRO invocations in parallel: delivery (`billing_kwh=True`) and supply (`billing_kwh=False`), splitting `process_workers` across the two.

## Pipeline YAML structure

The compact YAML eliminates per-run path duplication. One file per utility:

```yaml
state: ri
utility: rie
year: 2025
solar_pv_compensation: net_metering
process_workers: 8

resstock:
  base: /ebs/data/nrel/resstock/res_2024_amy2018_2_sb
  upgrade_precalc: "00"
  upgrade_calibrated: "02"

marginal_costs:
  dist_and_sub_tx: s3://data.sb/switchbox/marginal_costs/ri/dist_and_sub_tx/utility=rie/year=2025/data.parquet
  bulk_tx: s3://data.sb/switchbox/marginal_costs/ri/bulk_tx/utility=rie/year=2025/data.parquet
  supply_energy: s3://data.sb/switchbox/marginal_costs/ri/supply/energy/utility=rie/year=2025/data.parquet
  supply_capacity: s3://data.sb/switchbox/marginal_costs/ri/supply/capacity/utility=rie/year=2025/data.parquet

revenue_requirement:
  precalc: rev_requirement/rie_rate_case_test_year.yaml
  calibrated: rev_requirement/rie_large_number_rate_case_test_year.yaml

residual_allocation:
  delivery: percustomer
  supply: passthrough

scenario: default
```

All tariff JSON/map paths are **derived** from `utility` + `scenario` + naming conventions. For example, `scenario: default` produces:

- Tariff JSON: `tariffs/electric/rie_default.json` (delivery), `rie_default_supply.json` (supply)
- Tariff map: `tariff_maps/electric/rie_default.csv` (precalc delivery)
- Calibrated: `tariff_maps/electric/rie_default_calibrated.csv` (calibrated delivery)

## Invocation

### Start Prefect server (one-time, runs in background)

```bash
uv run prefect server start
```

The server UI is available at `http://localhost:4200`. On a remote EC2 instance, forward the port:

```bash
ssh -L 4200:localhost:4200 <host>
```

Or start the server with a specific host to bind to all interfaces:

```bash
uv run prefect server start --host 0.0.0.0
```

### Run the pipeline

From the repo root:

```bash
uv run python rate_design/hp_rates/run_pipeline.py \
  --yaml rate_design/hp_rates/ri/config/scenarios/pipeline_rie.yaml \
  --batch ri_20260722_r1-4
```

For BGE:

```bash
uv run python rate_design/hp_rates/run_pipeline.py \
  --yaml rate_design/hp_rates/md/config/scenarios/pipeline_bge.yaml \
  --batch md_20260722_r1-4
```

The `--batch` argument follows the existing batch naming convention (see `run_orchestration.md`).

### Run without Prefect server (ephemeral)

The pipeline will run without a server — Prefect falls back to an ephemeral local API. Tasks still execute, caching still works (persisted to `~/.prefect/`), but the web UI is not available.

### Worker count override

Set `RDP_NUM_WORKERS` to override the YAML's `process_workers`:

```bash
RDP_NUM_WORKERS=4 uv run python rate_design/hp_rates/run_pipeline.py \
  --yaml rate_design/hp_rates/ri/config/scenarios/pipeline_rie.yaml \
  --batch ri_20260722_test
```

### Output location

Outputs go to the FUSE mount at `/data.sb/switchbox/cairo/outputs/hp_rates/{state}/{utility}/{batch}/`. This mirrors the S3 path `s3://data.sb/switchbox/cairo/outputs/hp_rates/...`.

## Caching

Prefect caching replaces the old `check_s3_duplicate` logic. The cache key for each task is `{state}/{utility}/{batch}/{scenario}/{stage}`. If a task has already run with the same key, Prefect skips re-execution and returns the cached result.

To force re-execution, either:

- Change the `--batch` name
- Clear the Prefect result store: `rm -rf ~/.prefect/storage/`

## Relationship to Justfile orchestration

The pipeline does **not** replace the Justfile — both coexist. The Justfile remains the canonical way to run the full suite (`just run-all-sequential`), including pre-processing steps (`all-pre`). The Prefect pipeline covers the CAIRO run portion only (runs 1-4 currently) and assumes tariff JSONs, maps, and RR YAMLs already exist on disk.

Pre-processing (`create-scenario-yamls`, `create-electric-tariff-maps-all`, `compute-rr`, `create-flat-tariffs`, etc.) is still done via `just all-pre`.

## Planned extensions

See the `pipeline_evaluators_designers_merged` plan for the full multi-scenario architecture:

- **Evaluator tasks**: `single_rate_evaluator` (runs 1-4), `multi_rate_evaluator` (runs 5-6+)
- **Designer tasks**: `derive_seasonal_tariff` (produces HP seasonal + non-HP tariff inputs)
- **Bridge tasks**: `compute_subclass_rr` (subclass RR YAML from default BAT output)
- **Scenario flows**: `default_flow`, `hp_seasonal_flow`, wired in a master `hp_rates_pipeline`
- **Allocation-aware naming**: tariff filenames encode the residual allocation method (e.g. `rie_hp_seasonal_percustomer.json`)
