# Prefect pipeline for hp_rates

Status: covers the **default** single-rate scenario and **multi-rate** seasonal scenarios. `quartet_evaluator` always runs both stages (precalc and calibrated) for every quartet kind. See the `merged_pipeline_evaluators_subgroups` plan for the design.

## Vocabulary and anatomy of a quartet

The pipeline is described in canonical terms — not the legacy "run 1..8" numbering. A scenario is identified by name and its `quartet` kind, not a position in a run list.

- **Scenario** — one rate design to evaluate (`default`, `hp_seasonal_percustomer`, …), declared with a `quartet` kind.
- **Variant** — the cost scope of one CAIRO run: `delivery` (delivery only, supply MC zeroed, `billing_kwh=True`) or `supply` (real supply MC, `billing_kwh=False`).
- **Stage** — the calibration lifecycle position: `precalc` (baseline population; CAIRO solves the tariffs) then `calibrated` (target population; **stage-1 outputs** `*_calibrated.json` **as input, never stage-1 inputs**).
- **Rate mode** — how many tariffs a stage runs: `single` (one `all` tariff) or `multi` (per-subgroup tariffs + a bldg→tariff map).
- **Run** — one CAIRO invocation = one (stage, variant) pair; the atomic unit of work (`cairo_run`).
- **Quartet** — the four runs that fully evaluate one scenario: two stages × two variants. `quartet_evaluator` owns one.
- **tariff_promotion seam** — the handoff joining a quartet's two stages: the precalc stage's calibrated tariffs become the calibrated stage's inputs (`promote_subgroup_tariff` → `PromotionResult`).



### Quartet kinds

A scenario picks exactly one `quartet` kind, which fixes both stages' rate modes and the derived promotion policy (no separate promotion knob):


| quartet                | precalc | calibrated | tariff_promotion      | subgroups | run number equivalent                                                                                                                          |
| ---------------------- | ------- | ---------- | --------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `single_rate`          | single  | single     | `identity`            | no        | Runs 1-4 - calibrate default tariff on up00, evaluate on up02                                                                                  |
| `multi_rate_collapsed` | multi   | single     | `collapse_to_derived` | yes       | Runs 5-8 - derive HP rate, calibrate HP vs Non-HP tariff on up00, evaluate only the HP rate on up02                                            |
| `multi_rate_preserved` | multi   | multi      | `keep_subgroups`      | yes       | Not currently done. Use case could be calibrate on existing default vs TOU tariff, evaluate change when people modify equipment but keep rates |


- `multi_rate_collapsed` — calibrate N per-subgroup tariffs, then evaluate the target population on **one** promoted subgroup's calibrated tariff (`promote_subgroup`, defaulting to the sole `source: derived` subgroup). Right for HP-adoption scenarios (the target world is all-HP).
- `multi_rate_preserved` — keep every subgroup's calibrated tariff through the calibrated stage. Right when the grouping dimension persists across the population change (income tier, geography, building type).

```
quartet(scenario)
  ├─ precalc stage      (run_stage; single or multi per quartet kind)
  │     ├─ run: precalc · delivery      cairo_run.submit(billing_kwh=True)
  │     └─ run: precalc · supply        cairo_run.submit(billing_kwh=False)
  │           └─ writes tariff_final_config.json → *_calibrated.json
  ├─ tariff_promotion seam  (promote_subgroup_tariff → PromotionResult)
  └─ calibrated stage   (run_stage; single or multi per quartet kind)
        ├─ run: calibrated · delivery   (tariffs = *_calibrated.json)
        └─ run: calibrated · supply
```

The two variants within a stage run concurrently; the two stages run in sequence. `quartet_evaluator` runs the whole quartet for any kind — both stages always execute.

CAIRO matches a tariff map's `tariff_key` to the tariff **file stem**, so the calibrated stage needs maps keyed by `*_calibrated` stems. `resolve_subgroups` generates these calibrated-stage maps for the multi kinds (a single "all→promoted" map for `collapse`, a per-subgroup map for `preserve`); the single-rate calibrated map is produced by `just all-pre`.

## What this replaces

The Justfile shell orchestration (`just run-1` ... `just run-all-sequential`) with a Python-native Prefect pipeline that:

- Calls CAIRO's `run_scenario.run()` directly instead of shelling out
- Derives all file paths from a compact YAML config and naming conventions
- Passes output paths between tasks/flows explicitly (no S3 output scanning via `latest_run_output.sh`)
- Uses Prefect caching instead of manual S3 duplicate guards
- Bounds memory with a global Prefect concurrency limit instead of hand-rolled parallelism



## Files


| File                                                         | Purpose                                                                 |
| ------------------------------------------------------------ | ----------------------------------------------------------------------- |
| `rate_design/hp_rates/run_pipeline.py`                       | Prefect pipeline: config loader, settings derivation, tasks, flows, CLI |
| `rate_design/hp_rates/ri/config/scenarios/pipeline_rie.yaml` | Compact multi-scenario pipeline YAML for RIE                            |
| `rate_design/hp_rates/md/config/scenarios/pipeline_bge.yaml` | Compact multi-scenario pipeline YAML for BGE                            |




## Architecture (three tiers)

```
hp_rates_pipeline (master flow, sets provenance tags)
  ├─ default_flow (flow)
  │    ├─ quartet_evaluator (subflow, ThreadPoolTaskRunner(2))   # single_rate
  │    │    ├─ precalc:     cairo_run.submit(delivery) ∥ cairo_run.submit(supply)
  │    │    │                 └─ extract calibrated tariffs (tariff_final_config.json)
  │    │    ├─ tariff_promotion seam (promote_subgroup_tariff → PromotionResult)
  │    │    └─ calibrated:  cairo_run.submit(delivery) ∥ cairo_run.submit(supply)
  │    └─ compute_subclass_rr (task) → differentiated RR YAML (all allocations)
  └─ hp_seasonal_flow (flow, per multi-rate scenario)   ← depends on default_flow
       ├─ resolve_subgroups (task) → SubclassResolution (stems, precalc+calibrated maps, value map)
       ├─ derive_seasonal_tariff (task) → per-subgroup tariff JSONs
       └─ quartet_evaluator (subflow) → both stages run (precalc multi, calibrated per kind)
```

- `cairo_run` is the atomic `@task`: one CAIRO `run()`, cached and gated (see below). Delivery uses `billing_kwh=True`, supply `billing_kwh=False`.
- `quartet_evaluator` **is a subflow** with `ThreadPoolTaskRunner(max_workers=2)`, so `run_stage` can submit the delivery+supply pair to run concurrently. One evaluator handles all three quartet kinds and always runs both stages.
- **Designers/bridges are tasks**: `resolve_subgroups`, `compute_subclass_rr`, `derive_seasonal_tariff`.
- **The calibrated stage always runs**: `quartet_evaluator` crosses the `tariff_promotion` seam (`promote_subgroup_tariff`) to pick the calibrated stage's tariffs (always `*_calibrated.json`) and map, then runs it. Every subgroup's calibrated tariff from the precalc stage is persisted even when only one is promoted forward (`collapse_to_derived`).



## Global concurrency gate (memory bound)

Each `cairo_run` wraps `run()` in `with concurrency("cairo-runs", occupy=1):`. This caps the number of CAIRO processes resident in memory regardless of how many runs are submitted concurrently. Create the limit once (value from the YAML's `max_concurrent_cairo_runs`):

```bash
uv run prefect concurrency-limit create cairo-runs 2
```

The gate is non-strict: if the limit does not exist, Prefect logs a warning and proceeds ungated. Size `process_workers` so `max_concurrent_cairo_runs × process_workers` fits the box (each stage already splits `process_workers` across its delivery/supply pair).

## Pipeline YAML structure (multi-scenario)

One file per utility. Utility-level defaults, then a `scenarios` dict. All tariff JSON/map paths are **derived** from naming conventions — no per-run path duplication.

```yaml
state: ri
utility: rie
year: 2025
solar_pv_compensation: net_metering
process_workers: 8
max_concurrent_cairo_runs: 2
resstock: { base: /ebs/data/nrel/resstock/res_2024_amy2018_2_sb, upgrade_precalc: "00", upgrade_calibrated: "02" }
marginal_costs: { dist_and_sub_tx: s3://…, bulk_tx: s3://…, supply_energy: s3://…, supply_capacity: s3://… }
revenue_requirement:
  single_rate: rev_requirement/rie_rate_case_test_year.yaml
  single_rate_calibrated: rev_requirement/rie_large_number_rate_case_test_year.yaml
  multi_rate: rev_requirement/rie_hp_vs_nonhp_rate_case_test_year.yaml   # written by compute_subclass_rr
  multi_rate_calibrated: rev_requirement/rie_large_number_rate_case_test_year.yaml
scenarios:
  default:
    quartet: single_rate
  hp_seasonal_percustomer:
    quartet: multi_rate_collapsed          # multi precalc → single promoted calibrated
    depends_on: default
    # promote_subgroup: hp                 # optional; defaults to the sole derived subgroup
    residual_allocation: { delivery: percustomer, supply: passthrough }
    subclass_config:
      group_col: has_hp
      subgroups:
        hp:     { values: ["true"],  structure: seasonal, source: derived }
        non-hp: { values: ["false"], structure: default,  source: default_calibrated }
```



### Naming derivation

- Single-rate JSON: `{utility}_default[_supply][_calibrated].json`; map `{utility}_default[_calibrated][_supply].csv`.
- Multi-rate JSON: `{utility}_{alias}_{structure}_{allocation}[_supply][_calibrated].json` (e.g. `rie_hp_seasonal_percustomer.json`).
- Multi-rate map (scenario-based, expresses N groups): `{utility}_{scenario}[_calibrated][_supply].csv`.
- `ALLOCATION_TO_BAT_COL`: `percustomer→BAT_percustomer`, `epmc→BAT_epmc`, `volumetric→BAT_vol`.



## Invocation



### Start Prefect server (one-time, background)

```bash
uv run prefect server start          # UI at http://localhost:4200
uv run prefect concurrency-limit create cairo-runs 2   # one-time gate setup
```

On a remote EC2 instance, forward the port (`ssh -L 4200:localhost:4200 <host>`) or bind all interfaces (`--host 0.0.0.0`).

### Run the pipeline

```bash
uv run python rate_design/hp_rates/run_pipeline.py \
  --yaml rate_design/hp_rates/ri/config/scenarios/pipeline_rie.yaml \
  --batch ri_20260722_r1-6
```

The `--batch` argument follows the batch naming convention (see `run_orchestration.md`). Outputs go to the FUSE mount `/data.sb/switchbox/cairo/outputs/hp_rates/{state}/{utility}/{batch}/`.

## Caching

Each `cairo_run` is cached on `{run_name}|{path_results}` (run_name encodes state/utility/scenario/stage/variant; `path_results` encodes the batch). Re-runs with the same key are skipped. To force re-execution, change `--batch` or clear `~/.prefect/storage/`.

## Provenance

The master flow attaches Prefect **tags** to every run: `batch:…`, `utility:…`, `commit:<short-sha>`, and per multi-rate branch `scenario:…`. Filter by these in the UI instead of reading a log file.

## Relationship to Justfile orchestration

The pipeline does **not** replace the Justfile — both coexist. The Prefect pipeline covers the CAIRO run portion and the seasonal designer/subclass-RR bridges; it assumes the single-rate tariff JSONs, maps, and base RR YAMLs already exist on disk (still produced by `just all-pre`). Multi-rate maps and derived tariff JSONs are written by `resolve_subgroups` / `derive_seasonal_tariff` at run time.

## Deferred

- `multi_rate_preserved` in production — the kind is implemented (precalc multi → calibrated multi), but no shipped scenario uses it yet; it also needs a per-subgroup `multi_rate_calibrated` RR YAML (the current one is the single "large number" rate case).
- Concurrent scenario fan-out (async subflows) — the global gate already bounds memory, so flipping the master flow to concurrent needs no run-task changes.
- Additional allocations (`epmc`, `volumetric`) and designers (TOU) — add scenario blocks / designer tasks.

