# Run Orchestration (RI runs 1-16)

## Purpose

Automate the full sequence of 16 CAIRO runs for heat-pump rate design
(`just run-all-sequential` from `rate_design/hp_rates/ri/`). Each run has
pre-processing steps (tariff creation, calibrated tariff promotion, seasonal
discount derivation) that depend on outputs from earlier runs. The
orchestration encodes these dependencies so the entire pipeline can execute
unattended.

Runs 1-12 cover flat, seasonal, and seasonal-TOU tariff tiers with no demand
response. Runs 13-16 add a demand-flexibility (load-shifting) variant of the
seasonal-TOU tariff with `elasticity = -0.1`.

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
| RI orchestration | `rate_design/hp_rates/ri/Justfile` | Defines `run-1` ... `run-16`, `all-pre`, and `run-all-sequential`. Binds utility-specific config and wires the chain. |
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
  │    │    (needed by runs 5, 6, 9, 10, 13, 14 -- all multi-tariff precalc runs)
  │    │
  │    ├─ run-3  (default flat calibrated, delivery)    ← copies calibrated tariff from run-1
  │    ├─ run-5  (precalc hp_seasonal vs flat)           ← seasonal discount inputs from run-1
  │    │    └─ run-7  (default hp_seasonal calibrated)   ← copies calibrated tariff from run-5
  │    ├─ run-9  (precalc hp_seasonalTOU vs flat)        ← uses run-1 calibrated tariff as reference
  │    │    └─ run-11 (default hp_seasonalTOU calibrated) ← copies calibrated tariff from run-9
  │    └─ run-13 (precalc hp_seasonalTOU_flex vs flat, e=-0.1) ← copies TOU tariff from run-9
  │         └─ run-15 (default hp_seasonalTOU_flex calibrated, e=-0.1) ← copies calibrated tariff from run-13
  │
  └─ run-2  (precalc flat, supply)
       ├─ run-4  (default flat calibrated, supply)       ← copies calibrated tariff from run-2
       ├─ run-6  (precalc hp_seasonal vs flat, supply)   ← seasonal discount inputs from run-2
       │    └─ run-8  (default hp_seasonal calibrated, supply) ← copies from run-6
       ├─ run-10 (precalc hp_seasonalTOU vs flat, supply) ← uses run-2 calibrated tariff as reference
       │    └─ run-12 (default hp_seasonalTOU calibrated, supply) ← copies from run-10
       └─ run-14 (precalc hp_seasonalTOU_flex vs flat, supply, e=-0.1) ← copies TOU supply tariff from run-10
            └─ run-16 (default hp_seasonalTOU_flex calibrated, supply, e=-0.1) ← copies from run-14
```

## Demand flexibility (runs 13-16)

Runs 13-16 are the demand-flex variants of the seasonal-TOU runs (9-12).
The `_flex` tariff JSONs are structurally identical to the `_seasonalTOU`
tariffs — same periods, same peak/off-peak structure. The behavioral
difference comes entirely from the nonzero `elasticity: -0.1` in the
scenario YAML, which triggers the two-pass revenue requirement recalibration
at runtime in `run_scenario.py`.

When `elasticity != 0`, the scenario runner:

1. **Phase 1a:** Freezes the residual from original (unshifted) loads.
   `frozen_residual = full_RR_orig - MC_orig`.
2. **Phase 1.5:** Applies demand-response load shifting to TOU customers
   only (energy-conserving, zero-sum within each season).
3. **Phase 2:** Recomputes the revenue requirement as
   `new_RR = MC_shifted + frozen_residual`. The residual (embedded
   infrastructure costs) is invariant to short-run demand response; only the
   marginal cost component adjusts.
4. **Phase 3:** Runs CAIRO simulation with the shifted loads and recalibrated
   RR.

For full details see `context/tools/cairo_demand_flexibility_workflow.md`.

`run-all-sequential` now executes runs 1-16 in order, including the demand-flex
runs.

## How `latest_run_output.sh` works

CAIRO outputs are grouped by execution-time batch under a utility directory:
`s3://data.sb/.../hp_rates/{state}/{utility}/{execution_time}/{cairo_ts}_{run_name}/`.
The scenario YAML `path_outputs` contains an explicit `<execution_time>` placeholder,
e.g. `/data.sb/.../ny/coned/<execution_time>/ny_coned_run1_up00_precalc__flat`.

After a run completes, downstream runs need to find that directory. The script:

1. Parses the scenario YAML for the given `run_num` to extract `run_name`
   and `path_outputs`.
2. Goes two levels up from `path_outputs` (past `<execution_time>/` and the
   run name) to get the utility base dir.
3. Converts the local FUSE mount path (`/data.sb/...`) to an S3 URI
   (`s3://data.sb/...`).
4. Finds the **latest** execution_time directory under the utility dir.
5. Searches **only within** that execution_time dir for a CAIRO output
   matching `run_name`. Crashes with a helpful error if not found (prevents
   silently picking up stale outputs from a prior batch).
6. Prints the full S3 URI to stdout. Downstream recipes capture it via
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

From `rate_design/hp_rates/ri/`:

```bash
# Just the pre-processing + validation
just all-pre

# Runs 1-16 in sequential order (single execution-time batch)
just run-all-sequential

# Subset of runs as a single batch
just run-subset 1,2,5,6

# Individual run (after its dependencies have completed)
just run-5
```

## Execution-time batching

All runs in a batch share a single execution-time timestamp (`YYYYMMDDTHHMMSSZ`
format, UTC), which determines the output directory:
`s3://data.sb/.../hp_rates/{state}/{utility}/{execution_time}/`.

The `RDP_EXECUTION_TIME` environment variable propagates the batch timestamp:

- **Orchestration recipes** (`run-all-sequential`, `run-all-parallel-tracks`,
  `run-subset`, `run-all-utilities-sequential`) set
  `export RDP_EXECUTION_TIME="${RDP_EXECUTION_TIME:-$(date -u +%Y%m%dT%H%M%SZ)}"`
  at the top. The `:-` fallback means: generate a new timestamp when called
  standalone, inherit the parent's timestamp when called from a higher-level
  recipe.
- **`run-scenario`** reads `RDP_EXECUTION_TIME` (with the same fallback) and
  passes `--output-dir {path_outputs_base}/{execution_time}` to
  `run_scenario.py`.
- **Individual `run-<N>` recipes** don't touch `RDP_EXECUTION_TIME` — they
  inherit it from whatever called them.

### `run-subset`

Runs a comma-separated list of runs as a single batch:

```bash
just run-subset 1,2,5,6
```

Delegates to `run-<N>` recipes, so dependency logic (copy calibrated tariffs,
etc.) is preserved. Note: `compute-rev-requirements` is not auto-inserted; if
the subset spans runs 1-2 and 3+, run it separately.

## Monitoring

Each recipe that resolves a predecessor output prints a diagnostic line to
stderr:

```text
>> run-3: resolved run-1 output -> s3://data.sb/.../ny/coned/20260304T120000Z/20260304_120001_ny_coned_run1.../
```

Orchestration recipes also print the batch execution_time at startup:

```text
>> run-all-sequential [20260304T120000Z]
```

This lets you confirm at a glance that each run is picking up the correct
predecessor output and that all runs share the same batch.

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
tariffs using calibrated flat tariffs as references (from runs 1 and 2
respectively). The `--reference-tariff` flag on `derive_seasonal_tou.py`
extracts the base rate and fixed charge from this tariff rather than using
hardcoded defaults. Runs 13 and 14 do not re-derive TOU; they copy the run-9
and run-10 TOU tariffs to `_flex` tariff filenames.

**Demand-flex tariff identity.** The `_flex` tariffs are structurally identical
to their `_seasonalTOU` counterparts. Demand-flex behavior is triggered
entirely by the `elasticity` field in the scenario YAML, not by any tariff
structure difference. This keeps tariff creation simple and isolates the
behavioral modeling in the scenario runner.

**Sequential execution.** Delivery runs (odd) and supply runs (even) are
independent and could theoretically run in parallel. `run-all-sequential` runs them
serially for simplicity and debuggability. Use `run-all-parallel-tracks` for faster
end-to-end wall time (see Parallel tracks below).

**Parallel tracks.** `run-all-parallel-tracks` runs delivery and supply runs as concurrent
pairs (6 waves of 2), each pair using half the available CPUs. This beats the sequential
strategy when T4/T8 < 1.8 (see `cairo_parallelism_and_workers.md` for measured ratio).
File conflicts: none — each wave pair writes to distinct tariff files and timestamped S3
output directories. The `compute-rev-requirements` step remains serial between wave 1 and
wave 2 (it reads run-1 output and is fast; runs 5, 6, 9, and 10 all depend on it).
