# Run Orchestration (RI runs 1-12)

## Purpose

Automate the full sequence of 12 CAIRO runs for heat-pump rate design
(`just run-all-sequential` from `rate_design/ri/hp_rates/`). Each run has
pre-processing steps (tariff creation, calibrated tariff promotion, seasonal
discount derivation) that depend on outputs from earlier runs. The
orchestration encodes these dependencies so the entire pipeline can execute
unattended.

## Parameterization

The Justfile is parameterized in three tiers so it can be reused across
states and utilities without copy-paste:

| Tier | What                 | Examples                                                   |
| ---- | -------------------- | ---------------------------------------------------------- |
| 1    | Identity (env vars)  | `state`, `utility` via `env_var_or_default('STATE', 'ri')` |
| 2    | Utility-specific cfg | `region`, `cambium_ba`, `default_tariff`, ...              |
| 3    | Derived paths        | `path_scenario_config`, `path_cambium`, `path_td_mc`, ...  |

To replicate for a new utility, change the Tier 1 + 2 values at the top of
the file. All Tier 3 paths are computed automatically.

## Architecture

Three layers work together:

| Layer            | Path                               | Role                                                                                                                  |
| ---------------- | ---------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Generic recipes  | `utils/Justfile`                   | Reusable pre/mid recipes with no state-specific paths. RI Justfile delegates here.                                    |
| RI orchestration | `rate_design/ri/hp_rates/Justfile` | Defines `run-1` ... `run-12`, `all-pre`, and `run-all-sequential`. Binds utility-specific config and wires the chain. |
| Output resolver  | `utils/mid/latest_run_output.sh`   | Shell script that finds the most recent CAIRO output directory for a given run on S3.                                 |
| Config validator | `utils/pre/validate_config.py`     | Checks Justfile vars against scenario YAML values. Warn-only by default, `--strict` exits non-zero for CI.            |

## Dependency chain

Runs come in delivery/supply pairs. Odd-numbered runs are delivery, even are
supply. Within each tariff tier the pattern is: **precalc -> calibrate ->
precalc-with-new-tariff -> calibrate-with-new-tariff**.

```text
all-pre  (create-scenario-yamls, create-electric-tariff-maps-all, validate-config)
  │
  ├─ run-1  (precalc flat, delivery)
  │    │
  │    ├─ compute-rev-requirements  ← computes differentiated rev requirement YAML from run-1 BAT output
  │    │    (needed by runs 5, 6, 9, 10 -- all multi-tariff precalc runs)
  │    │
  │    ├─ run-3  (default flat calibrated, delivery)    ← copies calibrated tariff from run-1
  │    ├─ run-5  (precalc hp_seasonal vs flat)           ← seasonal discount inputs from run-1
  │    │    └─ run-7  (default hp_seasonal calibrated)   ← copies calibrated tariff from run-5
  │    └─ run-9  (precalc hp_seasonalTOU vs flat)        ← uses run-1 calibrated tariff as reference
  │         └─ run-11 (default hp_seasonalTOU calibrated) ← copies calibrated tariff from run-9
  │
  └─ run-2  (precalc flat, supply)
       ├─ run-4  (default flat calibrated, supply)       ← copies calibrated tariff from run-2
       ├─ run-6  (precalc hp_seasonal vs flat, supply)   ← seasonal discount inputs from run-2
       │    └─ run-8  (default hp_seasonal calibrated, supply) ← copies from run-6
       └─ run-10 (precalc hp_seasonalTOU vs flat, supply) ← uses run-2 calibrated tariff as reference
            └─ run-12 (default hp_seasonalTOU calibrated, supply) ← copies from run-10
```

## How `latest_run_output.sh` works

CAIRO writes outputs to timestamped directories like
`{path_outputs_parent}/{YYYYMMDD_HHMMSS}_{run_name}/`. After a run completes,
downstream runs need to find that directory. The script:

1. Parses the scenario YAML for the given `run_num` to extract `run_name`
   and `path_outputs`.
2. Converts the local FUSE mount path (`/data.sb/...`) to an S3 URI
   (`s3://data.sb/...`).
3. Runs `aws s3 ls` on the parent directory, filters for `PRE` entries (S3
   directory markers) matching `run_name`, sorts lexicographically (timestamps
   sort naturally), and takes the last (most recent) entry.
4. Prints the full S3 URI to stdout. Downstream recipes capture it via
   `$(bash latest_run_output.sh ...)`.

### Why `aws s3 ls` instead of the FUSE mount

The S3 FUSE mount (`/data.sb/`) uses `s3fs` with a default 900-second stat
cache TTL. A directory written by CAIRO seconds ago may not be visible via the
mount for up to 15 minutes. `aws s3 ls` always hits the S3 API directly,
avoiding staleness.

## How `validate_config.py` works

Compares Justfile top-level variables against canonical values in the scenario
YAML (run 1 for most fields, run 2 for the Cambium path since delivery runs
use `zero_marginal_costs.csv`). Checked fields: `state`, `utility`, `upgrade`,
`year`, `path_td_mc`, `path_cambium`, `path_electric_utility_stats`,
`path_resstock_loads`.

Runs as part of `all-pre` so mismatches are caught before any CAIRO run starts.
For CI, call `just validate-config strict` to exit non-zero on mismatch.

## How to run

From `rate_design/ri/hp_rates/`:

```bash
# Full pipeline
# Just the pre-processing + validation
just all-pre

# Runs 1-12 in sequential order
just run-all-sequential

# Individual run (after its dependencies have completed)
just run-5
```

## Monitoring

Each recipe that resolves a predecessor output prints a diagnostic line to
stderr:

```text
>> run-3: resolved run-1 output -> s3://data.sb/switchbox/cairo/outputs/hp_rates/ri/20260221_165459_ri_rie_run1.../
```

This lets you confirm at a glance that each run is picking up the correct
(most recent) predecessor output rather than a stale directory from a previous
pipeline execution.

## Design decisions

**Shebang recipes for runtime resolution.** Justfile variables (`x := ...`) are
evaluated at load time, before any recipe runs. Runs that depend on outputs
from earlier runs in the same pipeline need runtime evaluation. Shebang
recipes (`#!/usr/bin/env bash`) with `$(...)` command substitution ensure
`latest_run_output.sh` executes when the recipe runs, not when the Justfile
is parsed.

**Generic vs. state-specific split.** `utils/Justfile` contains recipes that
are parameterized and reusable across jurisdictions (RI, NY, etc.). The RI
Justfile delegates with `just -f utils/Justfile <recipe> <args>`, keeping
state-specific paths out of the generic layer.

**`utils/` directory mirrors run phases.** `utils/pre/` holds scripts that run
before any CAIRO run (tariff creation, scenario YAML generation, marginal cost
allocation). `utils/mid/` holds scripts that run between runs and consume
earlier run outputs (calibrated tariff promotion, subclass revenue requirements,
seasonal discount derivation). `utils/post/` holds scripts that run after all
runs complete (LMI discount application). `utils/mid/` also holds the
`latest_run_output.sh` shell helper for resolving predecessor output
directories between runs.

**Tiered parameterization.** Identity (Tier 1) and utility config (Tier 2) are
defined at the top of the Justfile. All paths (Tier 3) are derived from them
via Just string composition. This avoids scattered hardcoded values and makes
it possible to replicate the entire orchestration for a new utility by changing
only the top section.

**Reference tariff for TOU derivation.** Runs 9 and 10 derive seasonal TOU
tariffs using a calibrated flat tariff as reference (from runs 1 and 2
respectively). The `--reference-tariff` flag on `derive_seasonal_tou.py`
extracts the base rate and fixed charge from this tariff rather than using
hardcoded defaults.

**Sequential execution.** Delivery runs (odd) and supply runs (even) are
independent and could theoretically run in parallel. For now the pipeline runs
sequentially via `run-all-sequential` for simplicity and debuggability.
Parallelization is a future improvement.

**Parallel tracks.** `run-all-parallel-tracks` runs delivery and supply runs as concurrent
pairs (6 waves of 2), each pair using half the available CPUs. This beats the sequential
strategy when T4/T8 < 1.8 (see `cairo_parallelism_and_workers.md` for measured ratio).
File conflicts: none — each wave pair writes to distinct tariff files and timestamped S3
output directories. The `compute-rev-requirements` step remains serial between wave 1 and
wave 3 (it reads run-1 output and is fast; runs 5/6 depend on it).
