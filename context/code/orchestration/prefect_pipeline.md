# Prefect pipeline for hp_rates

Status: redesigned pipeline with generic quartet-based orchestration, structure handler dispatch, and subprocess-based CAIRO invocation.

## Vocabulary

- **Scenario** — one rate design to evaluate (`default`, `hp_seasonal_percustomer_passthrough`, …), declared with a `quartet` kind in the pipeline YAML.
- **Variant** — the cost scope of one CAIRO run: `delivery` (`billing_kwh=True`) or `supply` (`billing_kwh=False`).
- **Stage** — calibration lifecycle position: `precalc` (CAIRO solves tariffs on baseline population) then `calibrated` (target population uses promoted `*_calibrated.json` as input).
- **Run** — one CAIRO invocation = one (stage, variant) pair. Identified by a canonical run name.
- **Quartet** — the four runs that fully evaluate one scenario: 2 stages × 2 variants.
- **Tariff promotion seam** — the handoff joining precalc → calibrated: precalc outputs' `tariff_final_config.json` → `*_calibrated.json` files written to config dir.

### Quartet kinds

| quartet                | precalc | calibrated | subgroups | description                                             |
| ---------------------- | ------- | ---------- | --------- | ------------------------------------------------------- |
| `single_rate`          | single  | single     | no        | Calibrate one tariff on up00, evaluate on up02          |
| `multi_rate_collapsed` | multi   | single     | yes       | Calibrate per-subgroup, promote one to calibrated stage |
| `multi_rate_preserved` | multi   | multi      | yes       | Keep all subgroup tariffs through calibrated stage      |

## Files

| File                                                                    | Purpose                                                       |
| ----------------------------------------------------------------------- | ------------------------------------------------------------- |
| `rate_design/hp_rates/run_pipeline.py`                                  | Prefect flows, tasks, CLI (~760 lines)                        |
| `rate_design/hp_rates/pipeline_config.py`                               | PipelineConfig, ScenarioConfig, YAML loader, naming functions |
| `rate_design/hp_rates/pipeline_derive.py`                               | Structure handler registry (seasonal, flat, + future TOU)     |
| `rate_design/hp_rates/{state}/config/scenarios/pipeline_{utility}.yaml` | Per-utility pipeline config                                   |

## Architecture

```
run_batch @flow (master)
  ├─ preflight @flow
  │    ├─ validate inputs (MC paths, ResStock, RR YAMLs, tariff JSONs)
  │    ├─ generate scenarios YAML (pipeline YAML → per-run format for run_scenario.py)
  │    └─ generate electric tariff maps (write_tariff_maps_from_scenario)
  ├─ independent scenarios (no depends_on):
  │    └─ run_quartet @flow
  │         ├─ precalc: cairo_run(delivery) ∥ cairo_run(supply)
  │         ├─ tariff promotion seam
  │         └─ calibrated: cairo_run(delivery) ∥ cairo_run(supply)
  └─ dependent scenarios (has depends_on):
       ├─ check_dependency (verify dependency's quartet completed)
       ├─ derive_tariffs (compute subclass RR + dispatch tariff creation by structure)
       └─ run_quartet @flow
```

- `cairo_run` is the atomic `@task`: shells out to `run_scenario.py` as a subprocess for full memory isolation.
- `run_quartet` is a `@flow` with `ThreadPoolTaskRunner(max_workers=2)` so delivery + supply run concurrently within each stage.
- `derive_tariffs` dispatches to `pipeline_derive.py` handlers (no if-chains in the pipeline).

## Canonical run naming

Format: `{batch}_{scenario_name}_{stage}_{variant}`

Example: `md_bge_default_precalc_delivery`

The batch encodes state + utility + date/version info (e.g. `md_20260728`). This name is passed as `--run-num` to `run_scenario.py` and used as the key in the run index.

## CAIRO invocation: subprocess model

CAIRO is invoked via `subprocess.run()`, not in-process. Rationale:

- CAIRO + Dask + pandas consume several GB per run
- Python's allocator does not return memory to the OS after large allocations
- Running in-process caused OOM on larger utilities when multiple runs accumulated
- Subprocess gives full memory isolation — each run starts and finishes clean

The subprocess calls: `python -m rate_design.hp_rates.run_scenario --state {state} --scenario-config {yaml} --run-num {canonical_name} [--billing-kwh]`

## Concurrency gating: threading.Semaphore

A module-level `threading.Semaphore` (initialized by `run_batch` from `max_concurrent_cairo_runs`) gates how many CAIRO subprocesses run simultaneously. This replaced Prefect's Global Concurrency Limit for simplicity:

- No external state to manage (no `prefect gcl create`)
- Works identically for local and server-backed runs
- The semaphore lives in the Prefect worker process; `cairo_run` tasks execute in the same process via `ThreadPoolTaskRunner` and acquire the semaphore before `subprocess.run()`

Size constraint: `max_concurrent_cairo_runs × process_workers ≤ available cores/memory`.

## Run index (resume mechanism)

On successful completion, `run_scenario.py` writes a one-line file:

```
{batch_dir}/.runs/{canonical_run_name}.path
```

containing the absolute path to the timestamped output directory. This enables:

- **Resume**: `cairo_run` checks for the index file before launching. If present, the run is skipped.
- **Cross-invocation state**: no Prefect result cache needed; the filesystem is the source of truth.
- **Output discovery**: `derive_tariffs` and `check_dependency` read index files to locate predecessor outputs.

## Tariff derivation: handler dispatch

`pipeline_derive.py` implements a registry pattern:

```python
STRUCTURE_HANDLERS: dict[str, StructureHandler] = {
    "seasonal": _handle_seasonal,
    "flat": _handle_flat,
}
```

Each handler receives a `DeriveContext` dataclass with all inputs (run_dir, base tariff, stem, winter_months from periods.yaml, etc.) and produces a tariff JSON. Adding a new structure (e.g. TOU) means writing one handler function and adding one entry to the dict — no pipeline changes needed.

The `base` structure is handled directly in the pipeline (copy + relabel) since it's not a derivation.

## Pipeline YAML structure

```yaml
state: md
utility: bge
year: 2025
output_base: /data.sb/switchbox/cairo/outputs/hp_rates
process_workers: 8
max_concurrent_cairo_runs: 2
resstock:
  base: /ebs/data/nrel/resstock/res_2024_amy2018_2_sb
  upgrade_precalc: "00"
  upgrade_calibrated: "02"
marginal_costs:
  dist_and_sub_tx: s3://...
  bulk_tx: s3://...
  supply_energy: s3://...
  supply_capacity: s3://...
  supply_ancillary: s3://...
revenue_requirement:
  single_rate: rev_requirement/bge_rate_case_test_year.yaml
  single_rate_calibrated: rev_requirement/bge_large_number_rate_case_test_year.yaml
  multi_rate_calibrated: rev_requirement/bge_large_number_rate_case_test_year.yaml
scenarios:
  default:
    quartet: single_rate
    tariff_base: default
  hp_seasonal_percustomer_passthrough:
    quartet: multi_rate_collapsed
    depends_on: default
    promote: hp
    residual_allocation:
      delivery: percustomer
      supply: passthrough
    subclass_config:
      group_col: has_hp
      subgroups:
        hp:   { values: ["true"],  structure: seasonal }
        non-hp: { values: ["false"], structure: base }
```

### Key config fields

- `output_base` — root S3/FUSE path for outputs; batch dir = `{output_base}/{state}/{utility}/{batch}`
- `tariff_base` — explicit stem component for single-rate tariff filenames (required for `single_rate` quartet)
- `periods_yaml` — utility periods config (winter months); defaults to `periods/{utility}.yaml`
- `depends_on` — names the dependency scenario whose outputs feed `derive_tariffs`
- `promote` — which subgroup's calibrated tariff to promote for `multi_rate_collapsed`

## Invocation

```bash
uv run python -m rate_design.hp_rates.run_pipeline \
  --yaml rate_design/hp_rates/md/config/scenarios/pipeline_bge.yaml \
  --batch md_20260728 \
  [--scenarios default]
```

The `--scenarios` filter restricts which scenarios run (preflight is also scoped). Omit for all.

## Derived path anatomy

Every path the pipeline constructs is listed below with its template, source components, and a concrete BGE example.

### Batch output directory

```
{output_base}/{state}/{utility}/{batch}
```

| Component     | Source                      | Example           |
| ------------- | --------------------------- | ----------------- |
| `output_base` | `PipelineConfig.output_base` (YAML top-level) | `/data.sb/switchbox/cairo/outputs/hp_rates` |
| `state`       | `PipelineConfig.state`      | `md`              |
| `utility`     | `PipelineConfig.utility`    | `bge`             |
| `batch`       | CLI `--batch`               | `md_20260728`     |

Example: `/data.sb/switchbox/cairo/outputs/hp_rates/md/bge/md_20260728`

### Canonical run name

```
{batch}_{scenario_name}_{stage}_{variant}
```

| Component       | Source                         | Example                              |
| --------------- | ------------------------------ | ------------------------------------ |
| `batch`         | CLI `--batch`                  | `md_20260728`                        |
| `scenario_name` | scenario key in pipeline YAML  | `hp_seasonal_percustomer_passthrough` |
| `stage`         | quartet position               | `precalc` or `calibrated`            |
| `variant`       | cost scope                     | `delivery` or `supply`               |

Example: `md_20260728_hp_seasonal_percustomer_passthrough_precalc_delivery`

### Run index file

```
{batch_dir}/.runs/{canonical_run_name}.path
```

Example: `/data.sb/.../md_20260728/.runs/md_20260728_default_precalc_delivery.path`

### Run output directory (CAIRO writes here)

```
{batch_dir}/{canonical_run_name}
```

Example: `/data.sb/.../md_20260728/md_20260728_default_precalc_delivery`

(The actual timestamped subdir is written by CAIRO; the run index file records its absolute path.)

### Tariff JSON stem

**Single-rate:**

```
{utility}_{tariff_base}[_supply][_calibrated]
```

| Component     | Source                           | Example               |
| ------------- | -------------------------------- | --------------------- |
| `utility`     | `PipelineConfig.utility`         | `bge`                 |
| `tariff_base` | `ScenarioConfig.tariff_base`     | `default`             |
| `_supply`     | appended when `variant=supply`   |                       |
| `_calibrated` | appended when `stage=calibrated` |                       |

Examples: `bge_default`, `bge_default_supply`, `bge_default_calibrated`, `bge_default_supply_calibrated`

**Multi-rate (per subgroup):**

```
{utility}_{alias}_{structure}_{delivery_allocation}_{supply_allocation}[_supply][_calibrated]
```

| Component             | Source                                       | Example        |
| --------------------- | -------------------------------------------- | -------------- |
| `utility`             | `PipelineConfig.utility`                     | `bge`          |
| `alias`               | `SubgroupConfig.alias`                       | `hp`           |
| `structure`           | `SubgroupConfig.structure`                   | `seasonal`     |
| `delivery_allocation` | `ScenarioConfig.residual_allocation_delivery` | `percustomer`  |
| `supply_allocation`   | `ScenarioConfig.residual_allocation_supply`  | `passthrough`  |
| `_supply`             | appended when `variant=supply`               |                |
| `_calibrated`         | appended when `stage=calibrated`             |                |

Examples: `bge_hp_seasonal_percustomer_passthrough`, `bge_hp_seasonal_percustomer_passthrough_supply_calibrated`, `bge_non-hp_base_percustomer_passthrough_calibrated`

### Tariff JSON path (on disk)

```
{state_config_dir}/tariffs/electric/{stem}.json
```

Where `state_config_dir` = `rate_design/hp_rates/{state}/config`

Example: `rate_design/hp_rates/md/config/tariffs/electric/bge_default_calibrated.json`

### Tariff map stem (electric)

**Single-rate:** same as tariff JSON stem.

**Multi-rate:** maps are allocation-independent (they only partition buildings by subgroup membership):

```
{utility}_{alias1}_{structure1}_vs_{alias2}_{structure2}[_supply][_calibrated]
```

| Component         | Source                           | Example      |
| ----------------- | -------------------------------- | ------------ |
| `alias1`, `alias2` | Subgroup aliases (declaration order) | `hp`, `non-hp` |
| `structure1`, `structure2` | Subgroup structures     | `seasonal`, `base` |

Example: `bge_hp_seasonal_vs_non-hp_base`, `bge_hp_seasonal_vs_non-hp_base_supply_calibrated`

### Tariff map CSV path (on disk)

```
{state_config_dir}/tariff_maps/electric/{map_stem}.csv
```

Example: `rate_design/hp_rates/md/config/tariff_maps/electric/bge_hp_seasonal_vs_non-hp_base.csv`

### Gas tariff map path

```
{state_config_dir}/tariff_maps/gas/{utility}_u{upgrade}.csv
```

| Component | Source                                         | Example     |
| --------- | ---------------------------------------------- | ----------- |
| `utility`  | `PipelineConfig.utility`                      | `bge`       |
| `upgrade`  | `RunDefaults.upgrade_precalc` or `upgrade_calibrated` | `00`, `02`  |

Example: `rate_design/hp_rates/md/config/tariff_maps/gas/bge_u00.csv`

### ResStock paths

```
{resstock_base}/metadata/state={STATE}/upgrade={upgrade}/metadata-sb.parquet
{resstock_base}/load_curve_hourly/state={STATE}/upgrade={upgrade}/
{resstock_base}/metadata_utility/state={STATE}/utility_assignment.parquet
```

| Component       | Source                        | Example                                    |
| --------------- | ----------------------------- | ------------------------------------------ |
| `resstock_base` | `YAML RunDefaults.resstock_base`   | `/ebs/data/nrel/resstock/res_2024_amy2018_2_sb` |
| `STATE`         | `config.state.upper()`        | `MD`                                       |
| `upgrade`       | `YAML upgrade_precalc` or `upgrade_calibrated` | `00`, `02`                   |

### Multi-rate revenue requirement YAML

```
rev_requirement/{utility}_{alias1}_vs_{alias2}.yaml
```

Derived from subgroup aliases in declaration order. Relative to `state_config_dir`.

Example: `rev_requirement/bge_hp_vs_nonhp.yaml`

### EIA utility stats

```
s3://data.sb/eia/861/electric_utility_stats/year={year-1}/state={STATE}/data.parquet
```

| Component | Source                     | Example |
| --------- | -------------------------- | ------- |
| `year-1`  | `PipelineConfig.year - 1`  | `2024`  |
| `STATE`   | `config.state.upper()`     | `MD`    |

### Generated scenarios YAML

```
{state_config_dir}/scenarios/scenarios_{utility}.yaml
```

Example: `rate_design/hp_rates/md/config/scenarios/scenarios_bge.yaml`

### Periods YAML

```
{state_config_dir}/{periods_yaml}
```

| Component      | Source                                              | Example              |
| -------------- | --------------------------------------------------- | -------------------- |
| `periods_yaml` | `YAML RunDefaults.periods_yaml` (default: `periods/{utility}.yaml`) | `periods/bge.yaml`   |

### Supply MC path (zeroed for delivery-only)

```
delivery: {mc_path} → replaced suffix /data.parquet with /zero.parquet
supply:   {mc_path} → used as-is
```

Applies to `mc_supply_energy` and `mc_supply_capacity`.

## Deferred

- End-to-end BGE verification (next session)
- `multi_rate_preserved` in production
- Concurrent independent scenario fan-out (currently sequential)
- TOU structure handler
