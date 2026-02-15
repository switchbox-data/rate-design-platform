# Agent guide: rate-design-platform

This file orients AI agents (e.g. Cursor) so they can work effectively in this repo without reading the entire codebase.

## What this repo is

**rate-design-platform** is [Switchbox's](https://switch.box/) simulation platform for electric rate design, starting with heat pump friendly rates that eliminate cross-subsidies. It's main job is to do CAIRO runs and creates simulation outputs on s3 that are then analyzed by Switchbox's [reports2 repo](https://github.com/switchbox-data/reports2), a repo that contains all our reports in quarto notebook format.

The main inputs are:

- NREL's ResStock metadata and loads in parquet format stored in s3 at `s3://data.sb/nrel/resstock/`, downloaded with Switchbox's [buildstock-fetch](https://github.com/switchbox-data/buildstock-fetch) library.
  - metadata lives at `s3://data.sb/nrel/resstock/res_2024_amy2018_2/metadata/state=<2 char state abbreviation]/upgrade=<0 padded integer>/*.parquet`
  - hourly loads live at `s3://data.sb/nrel/resstock/res_2024_amy2018_2/load_curve_hourly/state=<2 char state abbreviation]/upgrade=<0 padded integer>/<bldg_id>_<upgrade_id>.parquet` and there are typically thousands of loads files
- NREL's [Cambium dataset](https://docs.nlr.gov/docs/fy25osti/93005.pdf) for marginal energy, generation capacity, and bulk transmission capacity costs, which lives in a parquet file on s3 at `s3://data.sb/nrel/cambium/`
- Marginal sub-distribution and distribution costs, and hourly allocation logic, drawn from utility-specific PUC filings like MCOS studies.
- NREL's [CAIRO rate-simulation engine](https://github.com/natlabrockies/cairo/), which implements the Simenone et. al's bill alignment test paper (https://www.sciencedirect.com/science/article/abs/pii/S0957178723000516?via%3Dihub)
- Electric and gas tariffs in URDB JSON format ([short guide to this format](https://switchbox-data.github.io/tariff_fetch/providers/nrel/), [official docs](https://openei.org/services/doc/rest/util_rates/?version=7)), downloaded with Switchbox's [tariff-fetch](https://github.com/switchbox-data/tariff_fetch) library
- ISO loads gathered from the EIA API

The main outputs are calibrated tariffs (when CAIRO is run in pre-calc mode), customer-level bills / marginal cost / residual cost allocation / bill alignments, aggregated bill alignment tariffs grouped by post-processing group, and so on. This data lives on s3 at `s3://data.sb/switchbox/cairo/<scenario_name>/<run>/`, and contains the following files:

| Path                                                   | Purpose                                                                                   |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------- |
| bill_assistance_metrics.csv                            | Metrics for bill assistance programs (e.g., LMI customer impacts)                         |
| bills/                                                 | Customer-level bill calculations                                                          |
| bills/comb_bills_year_run.csv                          | Annual combined (electric + gas) bills under the proposed rate structure                  |
| bills/comb_bills_year_target.csv                       | Annual combined (electric + gas) bills under the baseline/target rate structure           |
| bills/elec_bills_year_run.csv                          | Annual electric-only bills under the proposed rate structure                              |
| bills/elec_bills_year_target.csv                       | Annual electric-only bills under the baseline/target rate structure                       |
| bills/gas_bills_year_run.csv                           | Annual gas-only bills under the proposed rate structure                                   |
| bills/gas_bills_year_target.csv                        | Annual gas-only bills under the baseline/target rate structure                            |
| cross_subsidization/                                   | Bill Alignment Test (BAT) results                                                         |
| cross_subsidization/cross_subsidization_BAT_values.csv | Customer-level bill alignment metrics showing marginal cost recovery and cross-subsidies  |
| customer_metadata.csv                                  | ResStock building metadata (heating type, location, demographics, etc.) for each customer |
| tariff_final_config.json                               | Final calibrated tariff structure in URDB JSON format                                     |

## Layout

| Path                                | Purpose                                                                                                                                                                                                                                                              |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`rate_design/`**                  | Package root. Jurisdiction-specific logic and data live under `ny/` and `ri/`, each with `hp_rates/` (heat pump rate scenarios).                                                                                                                                     |
| **`rate_design/{ny,ri}/hp_rates/`** | Scenario entrypoints (`run_scenario.py`), **Justfiles** (primary task interface), and `data/` (tariff_structure JSON, tariff_map CSV, marginal_costs, resstock). Large artifacts (buildstock raw/processed, cairo_cases) are git-ignored; sync via S3 or keep local. |
| **`utils/`**                        | Cross-jurisdiction utilities: EIA zone load fetch/aggregation, utility marginal-cost allocation, ResStock metadata (e.g. identify HP customers, heating type), tariff mapping generators, CAIRO helpers. All runnable as CLI or imported by `rate_design`.           |
| **`docs/`**                         | Notes on domain, software, adn data (e.g. on rate design, lmi programs, how CAIRO works, what data is available on ResStock, etc.).                                                                                                                                  |
| **`tests/`**                        | Pytest tests; mirror `utils/` and key `rate_design` behavior.                                                                                                                                                                                                        |
| **`.devcontainer/`**                | Dev container and install scripts. CI uses runner-native workflow (`just install` then `just check` / `just test`); optional devcontainer for local/DevPod.                                                                                                          |
| **`infra/`**                        | Terraform and scripts for EC2/dev environment (e.g. `dev-setup`, `dev-teardown`).                                                                                                                                                                                    |

## How to work in this repo

- **Tasks**: Use **Just** as the main interface. Root `Justfile` defines `install`, `check`, `test`, `check-deps`, and dev/DevPod targets. Jurisdiction- and data-specific tasks (e.g. identify HP customers, create marginal-cost data, map tariffs) live in `rate_design/{ny,ri}/hp_rates/Justfile` and under `rate_design/.../data/*/Justfile`. Ad hoc scripts should typically by invoked via `just` recipes. Just syntax is tricky, especially for inline shell code. See the syntax [here](https://github.com/casey/just), and prefer external shell scripts to inline shell recipes if they go from command invocation to full-on scripts.
- **Python**: The project uses **uv** for dependency and env management (see `pyproject.toml`). The resulting virtualenv is created in root of the project (at `.venv/`) but it is .gitignored. CAIRO is a **private Git dependency**; CI and devcontainer rely on `GH_PAT` for cloning. Run commands via `uv run` (e.g. `uv run python -m pytest tests/`, `uv run python utils/...`). Use **Python 3.12+**.
- **Data**: Versioned inputs are under `rate_design/.../data/tariff_structure/` (JSON) and `.../data/tariff_map/` (CSV). Don’t commit large buildstock or CAIRO case outputs; use `.gitignore` and S3/local paths as in existing Justfiles.
- **AWS authentication**: we rely heavily on reading and writing data to s3. We use short-lived AWS SOO config; if it must be refreshed, use `just aws` in the root.

## Computing contexts

- Data scientists' laptops, usually Macs with Apple Silicon
- EC2 instances luaunched by terraform scripts in `infra/`
- devcontainers running on a laptop or on an instance using Devpod
- When relevant, be aware of what context you are in.

## Code Style

Match existing style: Ruff for formatting/lint, **ty** for type checking, dprint for md formatting using `.mardownlint.json` and shfmt for shell scripts. Keep new code consistent with current patterns in `utils/` and `rate_design/`.

## Code Quality (required before every commit)

- Run `just check` — no linter errors, no type errors, no warnings
  - `just check` runs lock validation (`uv lock --locked`) and **prek** (ruff, formatting, type checking)
  - Pre-commit hooks enforce: ruff-check, ruff-format, ty-check, trailing whitespace,
    end-of-file newline, YAML/JSON/TOML validation, no large files (>600KB),
    no merge conflict markers
- Run `just test` — all tests pass; Aad or extend tests in `tests/` for new or changed behavior.

## Data Conventions (S3)

- Path format: `s3://data.sb/<org>/<dataset>/<filename_YYYYMMDD.parquet>`
- Prefer Parquet format
- Filenames: lowercase with underscores, end with `_YYYYMMDD` (download date)
- Use lazy evaluation (polars `scan_parquet` / arrow `open_dataset`) and filter before collecting

## Dependencies

- `uv add <package>` (updates pyproject.toml + uv.lock); never use `pip install
- Commit lock files (uv.lock) when adding dependencies

## MCP Tools

### Context7

When writing or modifying code that uses a library, use the Context7 MCP server to fetch
up-to-date documentation for that library. Do not rely on training data for API signatures,
function arguments, or usage patterns — always resolve against Context7 first.

### Linear

When a task involves creating, updating, or referencing issues, use the Linear MCP server
to interact with our Linear workspace directly. See the ticket conventions below.

## New Issue Checklist

All work is tracked with Linear issues (which sync to GitHub Issues automatically).
When asked to create or update a ticket, use the Linear MCP tools.
Every new issue MUST satisfy all of the following before it is created:

- [ ] **Title** follows the format `Brief description` that starts with a verb (e.g., `Add winter peak analysis`).
- [ ] **What** is filled in: a concise, high-level description of what is being built,
      changed, or decided. Anyone should be able to understand the scope at a glance.
- [ ] **Why** is filled in: context, importance, and value — why this matters, what
      problem it solves, and what it unblocks.
- [ ] **How** is filled in (skip only when the What is self-explanatory and
      implementation is trivial) via numbered implementation steps, trade-offs, dependencies.
- [ ] **Deliverables** lists concrete, verifiable outputs that define "done", basically acceptance criteria:
  - Code: "PR that adds …", "Tests for …", "Data in `s3://...`"
  - Never vague ("Finish the analysis") or unmeasurable ("Make it better").
- [ ] **Project** is set, ask the user if unsure.
- [ ] **Status** is set. Default to **Backlog**. Options: Backlog, To Do, In Progress,
      Under Review, Done.
- [ ] **Milestone** is set when one applies (strongly encouraged — milestones are how we track progress toward major goals), ask the user if unclear.
- [ ] **Assignee** is set if the person doing the work is known.

### Status Transitions

Keep status updated as work progresses — this is critical for team visibility:

- **Backlog** → **To Do**: Picked for the current sprint
- **To Do** → **In Progress**: Work has started (branch created for code issues)
- **In Progress** → **Under Review**: PR ready for review, or findings documented
- **Under Review** → **Done**: PR merged (auto-closes), or reviewer approves and closes

## Libraries

This is a scientific computing python codebase. We make heavy use of polars, prefer it to pandas unless there's no other choice. (CAIRO is implemented in pandas though.)

## Conventions agents should follow

1. **Prefer existing entrypoints**: Add or use `just` recipes and `utils` CLIs rather than one-off scripts at the repo root.
2. **Respect data boundaries**: Don’t assume large data is in git; follow S3/local paths and env (e.g. AWS, `GH_PAT`) documented in Justfiles and CI.
3. **Keep docs and code aligned**: If you change behavior that `docs/` describes (e.g. CAIRO LMI, ResStock columns), update the relevant doc.
4. **Type and style**: Use type hints and Ruff; run `just check` before considering a change done.

## Quick reference

- **Install deps**: `just install`
- **Lint / format / typecheck**: `just check`
- **Tests**: `just test`
- **Dependency hygiene**: `just check-deps`
- **Project root (scripts)**: `utils.get_project_root()` or `git rev-parse --show-toplevel`
