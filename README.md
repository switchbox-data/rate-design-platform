# rate-design-platform

[![Build status](https://img.shields.io/github/actions/workflow/status/switchbox-data/rate-design-platform/ci-runner-native.yml?branch=main)](https://github.com/switchbox-data/rate-design-platform/actions/workflows/ci-runner-native.yml?query=branch%3Amain)
[![Commit activity](https://img.shields.io/github/commit-activity/m/switchbox-data/rate-design-platform)](https://github.com/switchbox-data/rate-design-platform)
[![License](https://img.shields.io/github/license/switchbox-data/rate-design-platform)](https://github.com/switchbox-data/rate-design-platform)

Simulation platform for electric rate design (heat-pump-friendly rates, Bill Alignment Test). Runs [CAIRO](https://github.com/natlabrockies/cairo) on ResStock building loads and Cambium marginal costs; outputs calibrated tariffs, customer bills, and BAT results to S3 for use in [reports2](https://github.com/switchbox-data/reports2). Covers New York and Rhode Island.

## Where things are

| Path               | Purpose                                                                                                                                                                                                                        |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **`rate_design/`** | Jurisdiction-specific logic. `ny/hp_rates/` and `ri/hp_rates/` each have a **Justfile** (main task interface), `config/` (tariffs, tariff_maps, marginal_costs, scenarios), and scenario entrypoints (e.g. `run_scenario.py`). |
| **`data/`**        | Data pipelines: fetch, convert, and upload datasets (Cambium, ResStock, EIA, HUD, FRED, etc.) to S3. Each subdir has its own Justfile.                                                                                         |
| **`utils/`**       | Shared code by phase: `pre/` (tariff mapping, scenario YAMLs, marginal costs), `mid/` (post-CAIRO steps), `post/` (e.g. LMI discounts).                                                                                        |
| **`context/`**     | Reference docs and papers (BAT, Cambium, ResStock, CAIRO).                                                                                                                                                                     |
| **`tests/`**       | Pytest tests.                                                                                                                                                                                                                  |
| **`infra/`**       | Terraform and scripts for EC2/dev.                                                                                                                                                                                             |

Large inputs/outputs (buildstock, CAIRO cases) are gitignored; sync via S3 or keep local. See **AGENTS.md** for detailed layout and S3 paths.

## Install

From the repo root:

```bash
just install
```

Uses **uv** for Python (see `pyproject.toml`). CAIRO is a private Git dependency; set `GH_PAT` for clone. Optional env vars (e.g. `ARCADIA_APP_ID`, `HUD_API_KEY`, `EIA_API_KEY`) are in `.env.example`—copy to `.env` and fill as needed. For AWS (S3), run `just aws` to refresh SSO when needed.

## Run sims

Use **Just** from the jurisdiction directory. Examples:

- **RI:** `cd rate_design/ri/hp_rates && just run-scenario 1` (and other run numbers; see that Justfile).
- **NY:** Pre-steps in `rate_design/ny/hp_rates/Justfile`: create scenario YAMLs, write tariff maps, create marginal-cost data; then run the scenario (see recipes and `run_scenario.py` there).

Root Justfile: `just check` (lint/format/typecheck), `just test` (pytest). For full task list, run `just` in the root or in a jurisdiction folder.
