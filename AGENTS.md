# Agent guide: rate-design-platform

This file orients AI agents (e.g. Cursor) so they can work effectively in this repo without reading the entire codebase.

## What this repo is

**rate-design-platform** is [Switchbox's](https://switch.box/) simulation platform for electric rate design, starting with heat pump friendly rates that eliminate cross-subsidies. It's main job is to do CAIRO runs and creates simulation outputs on s3 that are then analyzed by Switchbox's [reports2 repo](https://github.com/switchbox-data/reports2), a repo that contains all our reports in quarto notebook format. The platform centers on running the Bill Alignment Test (BAT) on ResStock building loads and Cambium marginal costs; CAIRO also performs bill calculations.

The main inputs are:

- NREL's ResStock metadata and loads in parquet format stored in s3 at `s3://data.sb/nrel/resstock/`, downloaded with Switchbox's [buildstock-fetch](https://github.com/switchbox-data/buildstock-fetch) library.
  - metadata lives at `s3://data.sb/nrel/resstock/res_2024_amy2018_2/metadata/state=<2 char state abbreviation]/upgrade=<0 padded integer>/*.parquet`
  - hourly loads live at `s3://data.sb/nrel/resstock/res_2024_amy2018_2/load_curve_hourly/state=<2 char state abbreviation]/upgrade=<0 padded integer>/<bldg_id>_<upgrade_id>.parquet` and there are typically thousands of loads files
- NREL's [Cambium dataset](https://docs.nlr.gov/docs/fy25osti/93005.pdf) for marginal energy, generation capacity, and bulk transmission capacity costs. Parquet on S3 at `s3://data.sb/nrel/cambium/` with Hive-style partitions: `{release_year}/scenario={name}/t={year}/gea={region}/r={ba}/data.parquet` (e.g. `2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet` for balancing area p133).
- Marginal sub-distribution and distribution costs, and hourly allocation logic, drawn from utility-specific PUC filings like MCOS studies.
- NREL's [CAIRO rate-simulation engine](https://github.com/natlabrockies/cairo/), which implements the Simenone et. al's bill alignment test paper (https://www.sciencedirect.com/science/article/abs/pii/S0957178723000516?via%3Dihub)
- Electric and gas tariffs in URDB JSON format ([short guide to this format](https://switchbox-data.github.io/tariff_fetch/providers/nrel/), [official docs](https://openei.org/services/doc/rest/util_rates/?version=7)), downloaded with Switchbox's [tariff-fetch](https://github.com/switchbox-data/tariff_fetch) library
- HUD Section 8 Income Limits (area-level AMI and income limits by household size), used for LMI/AMI in rate design. Parquet on S3 at `s3://data.sb/hud/ami/` with Hive-style partition `fy={year}/data.parquet` (e.g. fy=2016 … fy=2025). Schema harmonized across release years. Fetched and converted via `data/hud/ami/` (Justfile: `just prepare`, `just upload`).
- HUD State Median Income (SMI): state-level only, one row per state per year. Parquet on S3 at `s3://data.sb/hud/smi/`, partition `fy={year}/data.parquet` (fy=2017 … fy=2025), 50 states. Schema is a subset of AMI (same column names and types for overlapping cols: fy, state_fips, state_abbr, state_name, median_income, l50_1…l50_8, eli_1…eli_8, l80_1…l80_8). Pipeline in `data/hud/smi/` (Justfile: `just fetch`, `just convert`, `just upload`). Source: HUD API `il/statedata/{statecode}`; requires `HUD_API_KEY`.
- ISO loads: EIA zone loads (data/eia/hourly_loads/), EIA-861 utility stats (data/eia/861/)
- Census ACS PUMS (person and housing microdata) in parquet on S3 at `s3://data.sb/census/pums/`. There are two surveys (**acs1** 1-year and **acs5** 5-year), each identified by **end_year** (e.g. 2023). Under each survey/year, data is split into **person**- and **housing**-level tables; within each, data is Hive-partitioned by **state** (51 partitions: 50 states + DC). Path pattern: `s3://data.sb/census/pums/{survey}/{end_year}/{person|housing}/state={XX}/data.parquet` (e.g. `s3://data.sb/census/pums/acs1/2023/housing/state=NY/data.parquet`). Pipeline: `data/census/pums/` Justfile (fetch zips → unzip → convert CSV to parquet → upload).

The main outputs are calibrated tariffs (when CAIRO is run in pre-calc mode), customer-level bills / marginal cost / residual cost allocation / bill alignments, aggregated bill alignment tariffs grouped by post-processing group, and so on. This data lives on s3 at `s3://data.sb/switchbox/cairo/<scenario_name>/<run>/`, and contains the following files:

| Path                                                   | Purpose                                                                                                                                                           |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| bill_assistance_metrics.csv                            | Metrics for bill assistance programs (e.g., LMI customer impacts)                                                                                                 |
| bills/                                                 | Customer-level bill calculations                                                                                                                                  |
| bills/comb_bills_year_run.csv                          | Annual combined (electric + gas) bills under the proposed rate structure                                                                                          |
| bills/comb_bills_year_target.csv                       | Annual combined (electric + gas) bills under the baseline/target rate structure                                                                                   |
| bills/elec_bills_year_run.csv                          | Annual electric-only bills under the proposed rate structure                                                                                                      |
| bills/elec_bills_year_target.csv                       | Annual electric-only bills under the baseline/target rate structure                                                                                               |
| bills/gas_bills_year_run.csv                           | Annual gas-only bills under the proposed rate structure                                                                                                           |
| bills/gas_bills_year_target.csv                        | Annual gas-only bills under the baseline/target rate structure                                                                                                    |
| cross_subsidization/                                   | Bill Alignment Test (BAT) results                                                                                                                                 |
| cross_subsidization/cross_subsidization_BAT_values.csv | Customer-level bill alignment metrics showing marginal cost recovery and cross-subsidies                                                                          |
| customer_metadata.csv                                  | ResStock building metadata (heating type, location, demographics, etc.) for each customer                                                                         |
| tariff_final_config.json                               | Final calibrated tariff structure (CAIRO internal shape; one key per tariff). Copy utility writes one \<key\>_calibrated.json per key to config/tariffs/electric. |

## Layout

| Path                                | Purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`data/`**                         | Data engineering scripts for ingesting and preparing datasets on S3. Each subdirectory (e.g. `data/cambium/`) holds scripts to fetch, convert, and optionally upload a dataset; run via that directory’s Justfile (e.g. `just prepare` in `data/cambium/`). **When adding or editing data pipelines or scripts, follow the conventions in `data/README.md`** (recipe names, path variables, fetch→upload split, clean recipe, script naming).                       |
| **`rate_design/`**                  | Package root. Jurisdiction-specific logic and data live under `ny/` and `ri/`, each with `hp_rates/` (heat pump rate scenarios).                                                                                                                                                                                                                                                                                                                                    |
| **`rate_design/{ny,ri}/hp_rates/`** | Scenario entrypoints (`run_scenario.py`), **Justfiles** (primary task interface), and `config/` (tariffs JSON in tariffs/electric and tariffs/gas, tariff_maps CSV in tariff_maps/electric and tariff_maps/gas, marginal_costs). Large artifacts (buildstock raw/processed, cairo_cases) are git-ignored; sync via S3 or keep local.                                                                                                                                |
| **`data/eia/hourly_loads/`**        | EIA zone load fetch and utility load aggregation; eia_region_config (state/utility config, get_aws_storage_options); Justfile for fetch-zone-data and aggregate-utility-loads.                                                                                                                                                                                                                                                                                      |
| **`data/eia/861/`**                 | EIA-861 utility stats (PUDL yearly sales); fetch_electric_utility_stat_parquets.py; Justfile build-utility-stats (local parquet), update (upload to s3://data.sb/eia/861/electric_utility_stats/), fetch-utility-stats STATE (CSV to stdout).                                                                                                                                                                                                                       |
| **`data/fred/cpi/`**                | FRED CPI series; Justfile fetch-cpi (local parquet/), upload (sync to s3://data.sb/fred/cpi/).                                                                                                                                                                                                                                                                                                                                                                      |
| **`data/aspe/fpl/`**                | ASPE Federal Poverty Guidelines fetch; Justfile fetch. Output: utils/post/data/fpl_guidelines.yaml (used by LMI discount logic).                                                                                                                                                                                                                                                                                                                                    |
| **`data/resstock/`**                | ResStock metadata: identify HP customers, heating type, assign_utility_ny (NY). Justfile for fetch, test-download, resstock-identify-hp-customers, assign-utility-ny. Data is put on S3 separately; rate_design Justfiles do not invoke data pipelines.                                                                                                                                                                                                             |
| **`utils/`**                        | Cross-jurisdiction utilities split by run phase: `utils/pre/` (tariff creation, scenario YAMLs, marginal-cost allocation, config validation), `utils/mid/` (mid-run scripts consuming earlier CAIRO outputs: calibrated tariff promotion, subclass revenue requirements, seasonal discount derivation, output resolution), `utils/post/` (post-run: LMI discount application). CAIRO helpers in `utils/cairo.py`. All runnable as CLI or imported by `rate_design`. |
| **`context/`**                      | Reference docs and research notes for agents; see **Reference context** below and **`context/README.md`** for what lives where.                                                                                                                                                                                                                                                                                                                                     |
| **`tests/`**                        | Pytest tests; mirror `utils/` and key `rate_design` behavior.                                                                                                                                                                                                                                                                                                                                                                                                       |
| **`.devcontainer/`**                | Dev container and install scripts. CI uses runner-native workflow (`just install` then `just check` / `just test`); optional devcontainer for local/DevPod.                                                                                                                                                                                                                                                                                                         |
| **`infra/`**                        | Terraform and scripts for EC2/dev environment (e.g. `dev-setup`, `dev-teardown`).                                                                                                                                                                                                                                                                                                                                                                                   |

## Reference context

We run BAT on ResStock and Cambium; key reference material lives in `context/` so agents can use it without loading full PDFs or hunting through the repo. Treat these paths as first-class context (like the S3 input/output paths above).

**Conventions:**

- **`context/papers/`** — Academic papers (e.g. Bill Alignment Test). Extracted from PDFs via the pdf-to-markdown command.
- **`context/docs/`** — Technical documentation (e.g. Cambium, ResStock dataset docs). Extracted from PDFs via the pdf-to-markdown command.
- **`context/domain/`** — Important research notes on the domain (rate design, LMI programs, policy by state). Written or curated markdown.
- **`context/tools/`** — Research notes on tools, data, or implementation (e.g. CAIRO, ResStock metadata, how BAT is implemented). Written or curated markdown.

**When working on marginal costs, ResStock metadata/loads, BAT/cross-subsidization, LMI logic, state-specific programs, or Census PUMS data or documentation, read the relevant file(s) in `context/`.** In particular, read **`context/docs/`** and **`context/papers/`** when working on Cambium, ResStock dataset semantics, or the Bill Alignment Test—these are core inputs to the platform. PUMS docs in `context/docs/` are release-specific (1-year vs 5-year, by year); pick the file that matches your release. By using those docs, you may know more about the datasets than the team does; **if you see code or assumptions that conflict with the ResStock or Cambium documentation, proactively say so** so we can correct them.

For the current list of files and when to use each, see **`context/README.md`**.

To add or refresh extracted PDF content: use the **extract-pdf-to-markdown** slash command (`.cursor/commands/extract-pdf-to-markdown.md`) and place output under `context/docs/` or `context/papers/` as appropriate.

## How to work in this repo

- **Tasks**: Use **Just** as the main interface. Root `Justfile` defines `install`, `check`, `test`, `check-deps`, and dev/DevPod targets. Jurisdiction- and data-specific tasks (e.g. identify HP customers, create marginal-cost data, map tariffs) live in `rate_design/{ny,ri}/hp_rates/Justfile` and in data subdirectories (e.g. `data/eia/hourly_loads/Justfile`, `data/resstock/Justfile`, `data/fred/cpi/Justfile`). Ad hoc scripts should typically by invoked via `just` recipes. Just syntax is tricky, especially for inline shell code. See the syntax [here](https://github.com/casey/just), and prefer external shell scripts to inline shell recipes if they go from command invocation to full-on scripts.
- **Python**: The project uses **uv** for dependency and env management (see `pyproject.toml`). The resulting virtualenv is created in root of the project (at `.venv/`) but it is .gitignored. CAIRO is a **private Git dependency**; CI and devcontainer rely on `GH_PAT` for cloning. Run commands via `uv run` (e.g. `uv run python -m pytest tests/`, `uv run python utils/...`). Use **Python 3.12+**.
- **Data**: Versioned inputs are under `rate_design/.../config/tariffs/electric/` and `.../tariffs/gas/` (JSON) and `.../config/tariff_maps/electric/` and `.../config/tariff_maps/gas/` (CSV). Don’t commit large buildstock or CAIRO case outputs; use `.gitignore` and S3/local paths as in existing Justfiles.
- **AWS authentication**: we rely heavily on reading and writing data to s3. We use short-lived AWS SOO config; if it must be refreshed, use `just aws` in the root.

## Best practices for Justfiles

- **Path variables**: Any Just variable that holds a file or directory path should be named with a `path_` prefix (e.g. `path_project_root`, `path_output_dir`, `path_rateacuity_yaml`). This makes it clear which variables are paths and keeps naming consistent across Justfiles.
- **Recipe args and script args**: Parameter names in a Just recipe should match the script’s CLI argument names they are wired to (e.g. recipe `path_yaml` and `path_output_dir` → script `--yaml` and `--output-dir`). Use the same naming convention (path_ prefix for paths) in both so the wiring is obvious.

## Best practices for Python scripts (CLI)

- **Path arguments**: CLI arguments that are file or directory paths should use a `path_` prefix in the argparse name (e.g. `path_yaml`, `path_output_dir`), or a long option that makes the path explicit (e.g. `--output-dir`). When the script is invoked from a Justfile, use the same names as the Just variables (path_…) so recipe and script stay in sync.

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

## Best practices for working with data (for agents and contributors)

### Polars

- **Prefer LazyFrame:** Use `scan_parquet` and lazy operations; only materialize (e.g. `.collect()` or `read_parquet`) when the operation cannot be done lazily (e.g. control flow that depends on data, or a library that requires a DataFrame).
- **LazyFrame vs DataFrame:** Only `LazyFrame` has `.collect()`. A `DataFrame` from `read_parquet`, `.collect()`, or `df.join()` does not—calling `.collect()` on it will raise. Use `.group_by().agg()` on a DataFrame directly; no `.collect()`.
- **Joins:** With default `coalesce=True`, Polars keeps only the **left** join key column and drops the right. If you need both key columns in the result, use `coalesce=False` in the join; otherwise select/alias from the left key as needed.

### S3 and parquet

- **Prefer a single path for scan_parquet:** Pass the hive-partition root (or directory) to `pl.scan_parquet(path, ...)` so Polars reads the dataset as one logical table; do not pre-list files with s3fs just to pass a list of paths unless you have confirmed the row identity or grouping is not in the data (e.g. only in the path).

### Data and S3

- **Always inspect the data before coding.** When writing code that reads from S3 (or any data source), open the actual dataset—e.g. read one parquet and print schema and a few rows (`df.schema`, `df.head()`)—instead of assuming column names, presence of IDs, or file layout. Do not infer schema or row identity from file paths or other code alone.
- **Check context/docs first.** Before assuming a dataset's structure, look in `context/docs/` for data dictionaries, dataset docs, or release notes (e.g. ResStock, Cambium, EIA, PUMS). Use that as the source of truth; if docs and data disagree, note it.

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

- [ ] Title follows the format `Brief description` that starts with a verb (e.g., `Add winter peak analysis`).
- [ ] `## What` is filled in: a concise, high-level description of what is being built,
      changed, or decided. Anyone should be able to understand the scope at a glance.
- [ ] `## Why` is filled in: context, importance, and value — why this matters, what
      problem it solves, and what it unblocks.
- [ ] `## How` is filled in (skip only when the What is self-explanatory and
      implementation is trivial) via numbered implementation steps, trade-offs, dependencies.
- [ ] `## Deliverables` lists concrete, verifiable outputs that define "done", basically acceptance criteria:
      c - Code: "PR that adds …", "Tests for …", "Data in `s3://...`"
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

1. **Do not add intermediates to context:** Agent plans, GitHub (or Linear) issue bodies, design drafts, and other working artifacts should not be added under `context/`. Do not commit issue-body or issue-template markdown files to the repo (not in `context/`, not in `.github/`). `context/` is for reference material only (see `context/README.md`).
2. **Prefer existing entrypoints**: Add or use `just` recipes and `utils` CLIs rather than one-off scripts at the repo root.
3. **Respect data boundaries**: Don't assume large data is in git; follow S3/local paths and env (e.g. AWS, `GH_PAT`) documented in Justfiles and CI.
4. **Data pipeline conventions**: When creating or changing scripts or Justfiles under `data/`, read and follow **`data/README.md`** (recipe names, path variable naming, fetch→upload split, clean recipe, script naming). This keeps all data pipelines consistent.
5. **Update the context index**: When adding or removing files under `context/`, update `context/README.md` so the index stays accurate.
6. **Type and style**: Use type hints and Ruff; run `just check` before considering a change done.

## Pull request descriptions

- **Always close the GitHub issue**: Include `Closes #<number>` in the PR body so the **GitHub** issue is auto-closed when the PR is merged. Use the GitHub issue number, not the Linear issue identifier (e.g. use `Closes #263`, not `Closes RDP-126`). When work was tracked in Linear, look up the corresponding GitHub issue (e.g. `gh issue list` or the synced issue in the repo) and put that number in `Closes #<number>`.

Do not duplicate the issue in the PR body. Instead, write a concise description that gives the reviewer enough context to review without having to ask you questions:

- **High-level overview** of what the PR contains (a few sentences).
- **Reviewer focus**: Anything you want explicit feedback on (trade-offs, alternatives, design choices).
- **Non-obvious implementation details** and the "why" behind them (so the reviewer understands intent, not just the diff).

Keep it short. Do not add "Made with Cursor", "Generated by …", or any other LLM attribution.

## Quick reference

- **Install deps**: `just install`
- **Lint / format / typecheck**: `just check`
- **Tests**: `just test`
- **Dependency hygiene**: `just check-deps`
- **Project root (scripts)**: `utils.get_project_root()` or `git rev-parse --show-toplevel`
