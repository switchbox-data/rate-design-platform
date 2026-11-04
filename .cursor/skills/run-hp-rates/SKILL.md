---
name: run-hp-rates
description: >-
  Run, resume, or read a heat-pump rate CAIRO batch in rate-design-platform.
  Use when launching a Prefect pipeline, naming a batch, building master bills
  or master BAT, resuming a failed run, or interpreting delivery vs supply,
  precalc vs calibrated, or hp_rates output paths. Legacy runs 1–16 use the
  same skill.
---

# Run an hp_rates batch

Current batches are the Prefect pipeline. Commands run from `rate_design/hp_rates/`. Column lists and per-run filenames are in [reference.md](reference.md). Open that file when a column or filename is needed. Open [prefect_pipeline.md](../../../context/code/orchestration/prefect_pipeline.md) for quartet kinds, concurrency, or the path-template catalog. Do not load either file to launch a batch.

## Output

A batch directory and, after post-processing, the master tables analysis reads.

```text
s3://data.sb/switchbox/cairo/outputs/hp_rates/<state>/<utility>/<batch>/
s3://data.sb/switchbox/cairo/outputs/hp_rates/<state>/<utility>/<batch>/<segment>/<table>/
s3://data.sb/switchbox/cairo/outputs/hp_rates/<state>/all_utilities/<batch>/<segment>/<table>/
```

`<segment>` is `<scenario>_<stage>`, for example `default_precalc`. Analysis reads the `all_utilities` copy, Hive-partitioned by `sb.electric_utility`.

| Table                             | Grain                                 | Script                                     |
| --------------------------------- | ------------------------------------- | ------------------------------------------ |
| `comb_bills_year_target/`         | building × month (Jan–Dec and Annual) | `utils/post/build_master_bills_prefect.py` |
| `cross_subsidization_BAT_values/` | building, annual                      | `utils/post/build_master_bat_prefect.py`   |

One CAIRO run is one stage (`precalc` or `calibrated`) and one variant (`delivery` or `supply`). Its directory is `{cairo_ts}_{run_name}` inside the utility batch directory. The run name is `{state}_{utility}_{scenario}_{stage}_{variant}`.

## Procedure

1. **Name the batch** `{state}_{YYYYMMDD}{letter}` when more than one batch shares a date, for example `md_20260803_a`. The name is the directory. Reusing it resumes. Do not append `_r1-8`; that suffix is the legacy orchestrator below.
2. **Start Prefect once per session**, in its own terminal, and leave it running:

```bash
cd rate_design/hp_rates
just serve-prefect
```

The UI is `http://127.0.0.1:4200`. Other terminals pick up the API URL from the Prefect profile.

3. **Launch** from a second terminal:

```bash
cd rate_design/hp_rates
just s <state> run-pipeline <batch>
just s <state> run-pipeline <batch> <scenario>   # one scenario
```

The state Justfile selects `config/scenarios/pipeline_<utility>.yaml`. Preflight checks marginal-cost manifests, ResStock, revenue-requirement YAMLs, and tariff JSON before any CAIRO run.

4. **Resume** by repeating step 3 with the same batch name. A run whose `{batch}/.runs/{run_name}.path` exists is skipped. A failed run has no index file and is redone. Passing a scenario limits which scenarios are considered. It is not required for a resume.

5. **Build master tables after the runs**, bills first. BAT reads the baseline segment's master bills and fails with the path to build if they are missing.

```bash
just s <state> build-all-master-prefect <batch>
```

`build-master-bills-prefect` and `build-master-bat-prefect` are the two steps. Builders discover runs from `.runs/` and the pipeline YAML. They do not take run numbers. A segment missing one of delivery or supply raises. A segment missing both is skipped.

6. **Read the tables with these rules.**
   - Delivery figures (and gas, oil, propane) come from the delivery run. Electric supply comes from the supply run. Supply BAT is total minus delivery.
   - `baseline_elec_fixed_charge`, `baseline_elec_delivery_bill`, and `baseline_elec_supply_bill` are the `bill_change_baseline` segment, usually `default` + `precalc`. The builders raise if that key is missing from the pipeline YAML.
   - Heating type and the `heats_with_*` flags are taken from upgrade 00 metadata on every segment, so a heat-pump run stays sliceable by the pre-retrofit fuel. The `upgrade` column is the stage.
   - Cross-subsidy (BAT) on a **calibrated** segment is not a cross-subsidy. Those runs use a deliberately large revenue requirement. Read BAT from **precalc** segments. Bills on calibrated segments are the billed amounts.

## LMI columns

MD and CT fold LMI into the batch with `just s <state> run-with-lmi <batch>`. That runs the pipeline, then bills with that state's participation arguments, then BAT. LMI columns are on the `all_utilities` bills table only. Participation rates and flags are state-specific. When the task asks for LMI, read [lmi_master_bills_workflow.md](../../../context/code/orchestration/lmi_master_bills_workflow.md) and use the state's `run-with-lmi` recipe rather than typing rates from memory. Other states use `run-pipeline` and `build-all-master-prefect` with no LMI flag unless that workflow doc says otherwise.

## Legacy runs 1–16

Older NY and RI batches were one Just recipe per run number, not a Prefect quartet. Use this path only when the batch already uses it or the user names run numbers.

- Set `RDP_BATCH={state}_{YYYYMMDD}{letter}_r{start}-{end}` (example `ny_20260305c_r1-8`). `run-scenario` errors if it is unset, and errors if that utility and run already exist under the batch. A new name is required. Resume-by-same-name is the Prefect behavior, not this one.
- From `rate_design/hp_rates/`: `RDP_BATCH=<batch> just s <state> run-all-sequential`, or `run-subset 1,2,5,6`.
- Master tables take run numbers: `just build-master-bills <batch> <run_delivery> <run_supply>`, then `just build-master-bat` with the same pair. The segment directory is `run_<delivery>+<supply>`.
- Naming, dependency order, and demand-flex runs 13–16: [run_orchestration.md](../../../context/code/orchestration/run_orchestration.md).

## Done

You can point to the `all_utilities` bills path and the BAT path for the segment the user asked about, and say whether BAT on that segment is a precalc cross-subsidy or a calibrated bill run.
