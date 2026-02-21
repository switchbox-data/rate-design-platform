# Run Orchestration (RI runs 1–12)

## Purpose

Automate the full sequence of 12 CAIRO runs for RI heat-pump rate design
(`just run-all-sequential` from `rate_design/ri/hp_rates/`). Each run has
pre-processing steps (tariff creation, calibrated tariff promotion, seasonal
discount derivation) that depend on outputs from earlier runs. The
orchestration encodes these dependencies so the entire pipeline can execute
unattended.

## Architecture

Three layers work together:

| Layer | Path | Role |
| --- | --- | --- |
| Generic recipes | `utils/Justfile` | Reusable pre/post-processing recipes with no state-specific paths. RI Justfile delegates here. |
| RI orchestration | `rate_design/ri/hp_rates/Justfile` | Defines `run-1` … `run-12`, `global-pre`, and `run-all-sequential`. Binds RI-specific paths and wires the dependency chain. |
| Output resolver | `utils/runtime/latest_run_output.sh` | Shell script that finds the most recent CAIRO output directory for a given run on S3. |

## Dependency chain

Runs come in delivery/supply pairs. Odd-numbered runs are delivery, even are
supply. Within each tariff tier the pattern is: **precalc → calibrate →
precalc-with-new-tariff → calibrate-with-new-tariff**.

```text
global-pre  (create-scenario-yamls, write-tariff-maps-all)
  │
  ├─ run-1  (precalc flat, delivery)
  │    ├─ run-3  (default flat calibrated, delivery)  ← copies calibrated tariff from run-1
  │    └─ run-5  (precalc hp_seasonal vs flat)         ← seasonal discount inputs from run-1
  │         └─ run-7  (default hp_seasonal calibrated) ← copies calibrated tariff from run-5
  │
  ├─ run-2  (precalc flat, supply)
  │    ├─ run-4  (default flat calibrated, supply)     ← copies calibrated tariff from run-2
  │    └─ run-6  (precalc hp_seasonal vs flat, supply) ← seasonal discount inputs from run-2
  │         └─ run-8  (default hp_seasonal calibrated, supply) ← copies from run-6
  │
  ├─ run-9  (precalc hp_seasonalTOU vs flat, delivery)  ← derives TOU tariff from marginal costs
  │    └─ run-11 (default hp_seasonalTOU calibrated)     ← copies calibrated tariff from run-9
  │
  └─ run-10 (precalc hp_seasonalTOU vs flat, supply)     ← derives TOU tariff from marginal costs
       └─ run-12 (default hp_seasonalTOU calibrated, supply) ← copies from run-10
```

## How `latest_run_output.sh` works

CAIRO writes outputs to timestamped directories like
`{path_outputs_parent}/{YYYYMMDD_HHMMSS}_{run_name}/`. After a run completes,
downstream runs need to find that directory. The script:

1. Parses the scenario YAML (`scenarios_rie.yaml`) for the given `run_num` to
   extract `run_name` and `path_outputs`.
2. Converts the local FUSE mount path (`/data.sb/…`) to an S3 URI
   (`s3://data.sb/…`).
3. Runs `aws s3 ls` on the parent directory, filters for `PRE` entries (S3
   directory markers) matching `run_name`, sorts lexicographically (timestamps
   sort naturally), and takes the last (most recent) entry.
4. Prints the full S3 URI to stdout. Downstream recipes capture it via
   `$(bash latest_run_output.sh …)`.

### Why `aws s3 ls` instead of the FUSE mount

The S3 FUSE mount (`/data.sb/`) uses `s3fs` with a default 900-second stat
cache TTL. A directory written by CAIRO seconds ago may not be visible via the
mount for up to 15 minutes. `aws s3 ls` always hits the S3 API directly,
avoiding staleness.

## How to run

From `rate_design/ri/hp_rates/`:

```bash
# Full pipeline
just run-all-sequential

# Individual run (after its dependencies have completed)
just run-5

# Just the global pre-processing
just global-pre
```

## Monitoring

Each recipe that resolves a predecessor output prints a diagnostic line to
stderr:

```text
>> run-3: resolved run-1 output → s3://data.sb/switchbox/cairo/outputs/hp_rates/ri/20260221_165459_ri_rie_run1.../
```

This lets you confirm at a glance that each run is picking up the correct
(most recent) predecessor output rather than a stale directory from a previous
pipeline execution.

## Design decisions

**Shebang recipes for runtime resolution.** Justfile variables (`x := …`) are
evaluated at load time, before any recipe runs. Runs that depend on outputs
from earlier runs in the same pipeline need runtime evaluation. Shebang
recipes (`#!/usr/bin/env bash`) with `$(…)` command substitution ensure
`latest_run_output.sh` executes when the recipe runs, not when the Justfile
is parsed.

**Generic vs. RI-specific split.** `utils/Justfile` contains recipes that are
parameterized and reusable across jurisdictions (RI, NY, etc.). The RI
Justfile delegates with `just -f utils/Justfile <recipe> <ri-specific-args>`,
keeping state-specific paths out of the generic layer.

**Sequential execution.** Delivery runs (odd) and supply runs (even) are
independent and could theoretically run in parallel. For now the pipeline runs
sequentially via `run-all-sequential` for simplicity and debuggability.
Parallelization is a future improvement.
