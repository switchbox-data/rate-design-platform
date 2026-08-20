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
  │    ├─ validate inputs (FUSE mount, MC paths, ResStock, RR YAMLs, tariff JSONs)
  │    ├─ generate scenarios YAML (pipeline YAML → per-run format for run_scenario.py)
  │    └─ generate electric tariff maps (write_tariff_maps_from_scenario)
  ├─ independent scenarios (no depends_on):
  │    └─ run_quartet @flow
  │         ├─ precalc: cairo_run(delivery) → cairo_run(supply)
  │         ├─ tariff promotion seam
  │         └─ calibrated: cairo_run(delivery) → cairo_run(supply)
  └─ dependent scenarios (has depends_on):
       ├─ check_dependency (verify dependency's quartet completed)
       ├─ derive_tariffs (compute subclass RR + dispatch tariff creation by structure)
       └─ run_quartet @flow
```

- `cairo_run` is the atomic `@task`: shells out to `run_scenario.py` as a subprocess for full memory isolation.
- `run_quartet` is a `@flow` with `ThreadPoolTaskRunner(max_workers=2)`. Whether each stage's delivery + supply pair actually overlaps is controlled by `concurrent_variants` (see below); the arrows above show the default sequential mode.
- `derive_tariffs` dispatches to `pipeline_derive.py` handlers (no if-chains in the pipeline).

## Canonical run naming

Format: `{state}_{utility}_{scenario_name}_{stage}_{variant}`

Example: `md_bge_default_precalc_delivery`

The run name identifies (state, utility, scenario, stage, variant) — it is batch-independent. The batch is already encoded in the output directory path. This name is passed as `--run-num` to `run_scenario.py` and used as the key in the run index.

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

The semaphore is a batch-wide backstop, deliberately kept independent of `concurrent_variants`. With `concurrent_variants: false` it is a no-op — `run_batch` iterates scenarios sequentially and each quartet gates its own delivery/supply pair, so only one subprocess is ever in flight — but it still bounds things once concurrent scenario fan-out lands (see Deferred).

### `concurrent_variants`: sequential vs. concurrent delivery/supply

`concurrent_variants` (pipeline YAML, default `false`) decides whether each stage's delivery and supply runs overlap. It is the main lever for trading throughput against peak memory.

**Sequential (default).** Only one CAIRO subprocess is alive at a time, so no `--num-workers` override is passed and `run_scenario.py` applies its own `min(process_workers, os.cpu_count())` — the run gets the whole box. This is the safe default: memory, not CPU, is the binding constraint, and each CAIRO _parent_ process loads the full hourly load set for the utility before dispatching to Dask. Overlapping two runs means two of those parents resident at once, which is what OOM-killed runs on a 4-vCPU/15 GiB instance with no swap.

**Concurrent.** Both subprocesses run at once, so each gets half the effective worker count via `--num-workers`:

```python
num_workers = max(1, min(process_workers, os.cpu_count() or 1) // 2) if concurrent else None
```

**Halving `process_workers` alone is not sufficient** — it must be halved _after_ clamping to `os.cpu_count()`. On a 4-vCPU instance with `process_workers=8`, a single run already clamps to `min(8, 4) = 4`; halving `process_workers` itself (8 → 4) produces the same value (4) and changes nothing, so two concurrent subprocesses still spawn 4 Dask workers each (8 total on 4 cores) and still OOM. Halving the already-clamped value instead (`min(8, 4) // 2 = 2`) keeps the pair within the box's actual core count.

Note that even the correctly halved count is not always enough: multi-rate scenarios evaluate more tariffs per run and OOM'd at 2 workers each on a 4-vCPU box where the single-rate scenario survived. If a run OOMs in sequential mode too, lower `process_workers` in the YAML — no code change needed.

`--num-workers` fully overrides `process_workers` for that subprocess — `run_scenario.py`'s `run()` uses the CLI value as-is when provided, without re-clamping it against `os.cpu_count()`. The halving only covers the delivery/supply pair within one quartet; it does not account for multiple scenarios or quartets running concurrently (see Deferred).

### How the sequential gate works

`_run_stage` submits delivery, and in sequential mode calls `d_future.wait()` before submitting supply:

```python
d_future = cairo_run.submit(delivery_name, ..., num_workers=num_workers)
if not concurrent:
    d_future.wait()
s_future = cairo_run.submit(supply_name, ..., num_workers=num_workers)
return d_future.result(), s_future.result()
```

`PrefectFuture.wait()` blocks until the task reaches a final state and **does not raise** — failures surface at `.result()`. Two consequences, both intentional:

- A delivery failure still lets supply run and record its run index, so a re-run only redoes delivery. This matches the pre-toggle behavior.
- The gate lives in the flow thread, so the supply task is not created until delivery finishes — it does not sit in `Running` while idle.

**Why not `wait_for=[d_future]`?** Two reasons. It raises `UpstreamTaskError` when the upstream is not `COMPLETED`, so a delivery failure would skip supply entirely and forfeit that partial progress. And it resolves the dependency _inside_ the downstream task run, so the supply task is created immediately and occupies one of the two `ThreadPoolTaskRunner` slots while doing nothing.

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
concurrent_variants: false
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
bill_change_baseline:
  scenario: default
  stage: precalc
```

### Key config fields

- `output_base` — root S3/FUSE path for outputs; batch dir = `{output_base}/{state}/{utility}/{batch}`
- `tariff_base` — explicit stem component for single-rate tariff filenames (required for `single_rate` quartet)
- `periods_yaml` — utility periods config (winter months); defaults to `periods/{utility}.yaml`
- `depends_on` — names the dependency scenario whose outputs feed `derive_tariffs`
- `promote` — which subgroup's calibrated tariff to promote for `multi_rate_collapsed`
- `bill_change_baseline` — the one `(scenario, stage)` every run's bills are compared against. Read only by post-processing, so the pipeline runs without it; the master-table builders raise if it is missing. It is a single stage, not a quartet: usually `default` + `precalc`, i.e. today's rates on the pre-upgrade population.

## Invocation

### 1. Start the Prefect server

The pipeline requires a running Prefect server to track flows, tasks, and run state. Start it in a **separate terminal** before launching any pipeline runs:

```bash
uv run prefect server start --port 4200
```

The server UI is then available at `http://127.0.0.1:4200`. Keep this terminal open for the duration of the batch.

### 2. Point the pipeline at the server

In the terminal where you will launch the pipeline, tell the Prefect client where to send flow/task state. Either set it for the current shell session:

```bash
export PREFECT_API_URL=http://127.0.0.1:4200/api
```

Or persist it in a Prefect profile (survives new shells):

```bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

If the port differs (e.g. another Prefect instance is already using 4200), pass a different `--port` to `server start` and update `PREFECT_API_URL` to match. The port in both commands must agree — if they don't, the pipeline will either fail to connect or silently fall back to an ephemeral server that loses all run history.

### 3. Launch the pipeline

```bash
uv run python -m rate_design.hp_rates.run_pipeline \
  --yaml rate_design/hp_rates/md/config/scenarios/pipeline_bge.yaml \
  --batch md_20260728 \
  [--scenarios default]
```

The `--scenarios` filter restricts which scenarios run (preflight is also scoped). Omit for all.

## Post-processing: master tables

Once a batch finishes, two builders consolidate its CAIRO outputs into the master tables that notebooks and reports read. They are plain CLIs driven by Just — not Prefect flows — and one invocation covers a whole batch:

```bash
cd rate_design/hp_rates
just s md build-all-master-prefect md_20260803_a
```

That runs bills then BAT (in that order, because BAT joins the baseline bills). Either can be run alone with `build-master-bills-prefect` / `build-master-bat-prefect`, and both accept `--scenarios` to narrow the work.

To build Maryland master bills with FY26 OHEP MEAP/EUSP columns for multiple participation scenarios:

```bash
just s md build-master-bills-prefect <batch> \
  --calculate-lmi \
  --lmi-participation-rates 1.0 0.4 \
  --lmi-participation-mode weighted \
  --lmi-calculation-type monthly
```

For MD, `monthly` means that annual grants are allocated proportionally across Jan–Dec bills; it does not mean equal twelfths. The command appends both p100 and p40 column sets in one pass. It rewrites master-bill outputs but does not alter CAIRO run directories.

The current Prefect builder writes each per-utility table before applying LMI, then applies LMI to the concatenated table before its final write. Therefore MD OHEP columns are present only in the `all_utilities` table:

```
{output_base}/md/all_utilities/{batch}/{segment}/comb_bills_year_target/
```

Use that path for MD LMI analysis. See [LMI discounts in master bills](lmi_master_bills_workflow.md) for the complete data flow and column definitions.

| Script                                     | Output                            | Grain             |
| ------------------------------------------ | --------------------------------- | ----------------- |
| `utils/post/build_master_bills_prefect.py` | `comb_bills_year_target/`         | building × month  |
| `utils/post/build_master_bat_prefect.py`   | `cross_subsidization_BAT_values/` | building (annual) |

### One master table per segment

A **segment** is one `{scenario}_{stage}` pair — `default_precalc`, `hp_seasonal_percustomer_passthrough_calibrated`, and so on. Each becomes its own master table, written twice: once per utility and once for the batch:

```
{output_base}/{state}/{utility}/{batch}/{segment}/{table}/
{output_base}/{state}/all_utilities/{batch}/{segment}/{table}/
```

The `all_utilities` copy is Hive-partitioned by `sb.electric_utility` and is what analysis reads. This replaces the legacy Justfile layout, which keyed tables by `run_{delivery}+{supply}` and needed one invocation per run pair.

The delivery and supply runs of a segment are **joined**, not kept separate: the delivery-only run supplies electric delivery (and gas/oil/propane) figures, the delivery+supply run supplies electric supply, and supply-only BAT is derived as `total − delivery`.

### Run discovery

Builders never take run numbers. For each utility they load `{state}/config/scenarios/pipeline_{utility}.yaml` by convention, expand its scenarios into segments, and look up each run's output directory in the batch's run index (`{batch_dir}/.runs/{canonical_run_name}.path`). Index files hold FUSE paths, which `utils/post/pipeline_runs.py` maps to `s3://` URIs.

A segment whose **both** variants are missing is skipped with a log line, so a partially-run batch still post-processes. A segment with **one** variant missing raises: that table can never be built, and skipping it would hide a failed CAIRO run.

### Baseline bill columns

Every row in both tables carries the baseline segment's annual electric bill, so bill changes can be computed without a second read:

`baseline_elec_fixed_charge`, `baseline_elec_delivery_bill`, `baseline_elec_supply_bill`

The baseline segment comes from `bill_change_baseline` in the pipeline YAML, and its table is built first so the others can join it. On the baseline segment itself, the columns are copies of its own `elec_*` values. The builders assert that the baseline table's `upgrade` matches the upgrade its configured stage implies, so a mismatched or stale baseline fails loudly instead of silently attaching the wrong bills.

Building BAT without the baseline bills raises with the path to build first — hence `build-all-master-prefect`.

### Building attributes come from upgrade 00

`postprocess_group.has_hp`, `postprocess_group.heating_type`, the `heats_with_*` flags, income, and cooling are read from the **baseline** upgrade's `metadata-sb.parquet` on every segment, joined to `utility_assignment.parquet` for the utility mapping. ResStock marks every building in the heat-pump upgrade as a heat pump, so a calibrated segment's own metadata would erase what the home heated with before the retrofit — the dimension most analyses slice on. The `upgrade` column identifies the stage instead.

### Reading the BAT tables

BAT metrics in **calibrated** segments are dominated by the deliberately large revenue requirement those runs use (`residual_share_total` lands in the hundreds of thousands per customer, versus roughly a thousand in precalc). Bills in calibrated segments are unaffected and correct. The builders write what CAIRO produced without special-casing; interpret cross-subsidy metrics from precalc segments.

## Derived path anatomy

Every path the pipeline constructs is listed below with its template, source components, and a concrete BGE example.

### Batch output directory

```
{output_base}/{state}/{utility}/{batch}
```

| Component     | Source                                        | Example                                     |
| ------------- | --------------------------------------------- | ------------------------------------------- |
| `output_base` | `PipelineConfig.output_base` (YAML top-level) | `/data.sb/switchbox/cairo/outputs/hp_rates` |
| `state`       | `PipelineConfig.state`                        | `md`                                        |
| `utility`     | `PipelineConfig.utility`                      | `bge`                                       |
| `batch`       | CLI `--batch`                                 | `md_20260728`                               |

Example: `/data.sb/switchbox/cairo/outputs/hp_rates/md/bge/md_20260728`

### Canonical run name

```
{state}_{utility}_{scenario_name}_{stage}_{variant}
```

| Component       | Source                        | Example                               |
| --------------- | ----------------------------- | ------------------------------------- |
| `state`         | `PipelineConfig.state`        | `md`                                  |
| `utility`       | `PipelineConfig.utility`      | `bge`                                 |
| `scenario_name` | scenario key in pipeline YAML | `hp_seasonal_percustomer_passthrough` |
| `stage`         | quartet position              | `precalc` or `calibrated`             |
| `variant`       | cost scope                    | `delivery` or `supply`                |

The run name is batch-independent — the batch is already in the directory path.

Example: `md_bge_hp_seasonal_percustomer_passthrough_precalc_delivery`

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

| Component     | Source                           | Example   |
| ------------- | -------------------------------- | --------- |
| `utility`     | `PipelineConfig.utility`         | `bge`     |
| `tariff_base` | `ScenarioConfig.tariff_base`     | `default` |
| `_supply`     | appended when `variant=supply`   |           |
| `_calibrated` | appended when `stage=calibrated` |           |

Examples: `bge_default`, `bge_default_supply`, `bge_default_calibrated`, `bge_default_supply_calibrated`

**Multi-rate (per subgroup):**

```
{utility}_{alias}_{structure}_{delivery_allocation}_{supply_allocation}[_supply][_calibrated]
```

| Component             | Source                                        | Example       |
| --------------------- | --------------------------------------------- | ------------- |
| `utility`             | `PipelineConfig.utility`                      | `bge`         |
| `alias`               | `SubgroupConfig.alias`                        | `hp`          |
| `structure`           | `SubgroupConfig.structure`                    | `seasonal`    |
| `delivery_allocation` | `ScenarioConfig.residual_allocation_delivery` | `percustomer` |
| `supply_allocation`   | `ScenarioConfig.residual_allocation_supply`   | `passthrough` |
| `_supply`             | appended when `variant=supply`                |               |
| `_calibrated`         | appended when `stage=calibrated`              |               |

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

| Component                  | Source                               | Example            |
| -------------------------- | ------------------------------------ | ------------------ |
| `alias1`, `alias2`         | Subgroup aliases (declaration order) | `hp`, `non-hp`     |
| `structure1`, `structure2` | Subgroup structures                  | `seasonal`, `base` |

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

| Component | Source                                                | Example    |
| --------- | ----------------------------------------------------- | ---------- |
| `utility` | `PipelineConfig.utility`                              | `bge`      |
| `upgrade` | `RunDefaults.upgrade_precalc` or `upgrade_calibrated` | `00`, `02` |

Example: `rate_design/hp_rates/md/config/tariff_maps/gas/bge_u00.csv`

### ResStock paths

```
{resstock_base}/metadata/state={STATE}/upgrade={upgrade}/metadata-sb.parquet
{resstock_base}/load_curve_hourly/state={STATE}/upgrade={upgrade}/
{resstock_base}/metadata_utility/state={STATE}/utility_assignment.parquet
```

| Component       | Source                                         | Example                                         |
| --------------- | ---------------------------------------------- | ----------------------------------------------- |
| `resstock_base` | `YAML RunDefaults.resstock_base`               | `/ebs/data/nrel/resstock/res_2024_amy2018_2_sb` |
| `STATE`         | `config.state.upper()`                         | `MD`                                            |
| `upgrade`       | `YAML upgrade_precalc` or `upgrade_calibrated` | `00`, `02`                                      |

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

| Component | Source                    | Example |
| --------- | ------------------------- | ------- |
| `year-1`  | `PipelineConfig.year - 1` | `2024`  |
| `STATE`   | `config.state.upper()`    | `MD`    |

### Generated scenarios YAML

```
{state_config_dir}/scenarios/scenarios_{utility}.yaml
```

Example: `rate_design/hp_rates/md/config/scenarios/scenarios_bge.yaml`

### Periods YAML

```
{state_config_dir}/{periods_yaml}
```

| Component      | Source                                                              | Example            |
| -------------- | ------------------------------------------------------------------- | ------------------ |
| `periods_yaml` | `YAML RunDefaults.periods_yaml` (default: `periods/{utility}.yaml`) | `periods/bge.yaml` |

### Supply MC path (zeroed for delivery-only)

```
delivery: {mc_path} → replaced suffix /data.parquet with /zero.parquet
supply:   {mc_path} → used as-is
```

Applies to `mc_supply_energy` and `mc_supply_capacity`.

## Deferred

- `multi_rate_preserved` in production
- Concurrent independent scenario fan-out (currently sequential)
- OOM retry for `cairo_run`: retry on subprocess exit `-9`/`137` with adaptive worker reduction (via `prefect.runtime.task_run.run_count`). Only worth building when re-enabling `concurrent_variants` on a larger box — a retry at unchanged concurrency would likely OOM again, since the failure is deterministic under overlap rather than transient.
- TOU structure handler
