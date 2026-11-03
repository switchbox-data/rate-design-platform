# Agent guide: rate-design-platform

This file orients AI agents (e.g. Cursor) so they can work effectively in this repo without reading the entire codebase.

## What this repo is

**rate-design-platform** is [Switchbox's](https://switch.box/) simulation platform for electric rate design, starting with heat pump friendly rates that eliminate cross-subsidies. It's main job is to do CAIRO runs and creates simulation outputs on s3 that are then analyzed by Switchbox's [reports2 repo](https://github.com/switchbox-data/reports2), a repo that contains all our reports in quarto notebook format. The platform centers on running the Bill Alignment Test (BAT) on ResStock building loads and Cambium marginal costs; CAIRO also performs bill calculations.

The main inputs are:

- NREL's ResStock metadata and loads in parquet format stored in s3 at `s3://data.sb/nrel/resstock/`, downloaded with Switchbox's [buildstock-fetch](https://github.com/switchbox-data/buildstock-fetch) library.
  - metadata lives at `s3://data.sb/nrel/resstock/res_2024_amy2018_2/metadata/state=<XX>/upgrade=<00>/*.parquet`
  - hourly loads live at `s3://data.sb/nrel/resstock/res_2024_amy2018_2/load_curve_hourly/state=<XX>/upgrade=<00>/<bldg_id>-<upgrade>.parquet` (one file per building; the directory upgrade is zero-padded, the filename upgrade is not)
- NREL's [Cambium dataset](https://docs.nlr.gov/docs/fy25osti/93005.pdf) for marginal energy, generation capacity, and bulk transmission capacity costs. Parquet on S3 at `s3://data.sb/nrel/cambium/` with Hive-style partitions: `{release_year}/scenario={name}/t={year}/gea={region}/r={ba}/data.parquet` (e.g. `2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet` for balancing area p133).
- Marginal sub-distribution and distribution costs, and hourly allocation logic, drawn from utility-specific PUC filings like MCOS studies.
- NREL's [CAIRO rate-simulation engine](https://github.com/natlabrockies/cairo/), which implements the Simenone et. al's bill alignment test paper (https://www.sciencedirect.com/science/article/abs/pii/S0957178723000516?via%3Dihub)
- Electric and gas tariffs in URDB JSON format ([short guide to this format](https://switchbox-data.github.io/tariff_fetch/providers/nrel/), [official docs](https://openei.org/services/doc/rest/util_rates/?version=7)), downloaded with Switchbox's [tariff-fetch](https://github.com/switchbox-data/tariff_fetch) library
- HUD Section 8 Income Limits (area-level AMI and income limits by household size), used for LMI/AMI in rate design. Parquet on S3 at `s3://data.sb/hud/ami/` with Hive-style partition `fy={year}/data.parquet` (e.g. fy=2016 … fy=2025). Schema harmonized across release years. Fetched and converted via `data/hud/ami/` (Justfile: `just prepare`, `just upload`).
- HUD State Median Income (SMI): state-level only, one row per state per year. Parquet on S3 at `s3://data.sb/hud/smi/`, partition `fy={year}/data.parquet` (fy=2017 … fy=2025), 50 states. Schema is a subset of AMI (same column names and types for overlapping cols: fy, state_fips, state_abbr, state_name, median_income, l50_1…l50_8, eli_1…eli_8, l80_1…l80_8). Pipeline in `data/hud/smi/` (Justfile: `just fetch`, `just convert`, `just upload`). Source: HUD API `il/statedata/{statecode}`; requires `HUD_API_KEY`.
- ISO loads: EIA zone loads (data/eia/hourly_loads/), EIA-861 utility stats (data/eia/861/)
- Census ACS PUMS (person and housing microdata) in parquet on S3 at `s3://data.sb/census/pums/`. There are two surveys (**acs1** 1-year and **acs5** 5-year), each identified by **end_year** (e.g. 2023). Under each survey/year, data is split into **person**- and **housing**-level tables; within each, data is Hive-partitioned by **state** (51 partitions: 50 states + DC). Path pattern: `s3://data.sb/census/pums/{survey}/{end_year}/{person|housing}/state={XX}/data.parquet` (e.g. `s3://data.sb/census/pums/acs1/2023/housing/state=NY/data.parquet`). Pipeline: `data/census/pums/` Justfile (fetch zips → unzip → convert CSV to parquet → upload).

CAIRO writes run outputs under `s3://data.sb/switchbox/cairo/outputs/hp_rates/<state>/<utility>/<batch>/`. Master tables for analysis live at `.../hp_rates/<state>/all_utilities/<batch>/<segment>/`. Running a batch, resuming it, and reading those tables is the `run-hp-rates` skill.

## Layout

| Path                                | Purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`data/`**                         | One dataset per directory (`data/<org>/<dataproduct>/`), with its own Justfile. Adding or changing a pipeline is the `data-pipeline` skill.                                                                                                                                                                                                                                                                                                                                                 |
| **`rate_design/`**                  | Package root. Heat pump rate design lives under `rate_design/hp_rates/`.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| **`rate_design/hp_rates/`**         | Shared scenario entrypoint (`run_scenario.py`), shared **Justfile** (primary task interface for all states). State-specific thin Justfiles and `config/` dirs live under `rate_design/hp_rates/{ny,ri}/`.                                                                                                                                                                                                                                                                                   |
| **`rate_design/hp_rates/{ny,ri}/`** | State-specific thin Justfile (imports shared), `state.env`, and `config/` (tariffs JSON in tariffs/electric and tariffs/gas, tariff_maps CSV in tariff_maps/electric and tariff_maps/gas, marginal_costs). Large artifacts (buildstock raw/processed, cairo_cases) are git-ignored; sync via S3 or keep local.                                                                                                                                                                              |
| **`data/eia/hourly_loads/`**        | EIA zone load fetch and utility load aggregation; eia_region_config (state/utility config); Justfile for fetch-zone-data and aggregate-utility-loads.                                                                                                                                                                                                                                                                                                                                       |
| **`data/eia/861/`**                 | EIA-861 utility stats (PUDL yearly sales); fetch_electric_utility_stat_parquets.py; Justfile build-utility-stats (local parquet), update (upload to s3://data.sb/eia/861/electric_utility_stats/), fetch-utility-stats STATE (CSV to stdout).                                                                                                                                                                                                                                               |
| **`data/fred/cpi/`**                | FRED CPI series; Justfile fetch-cpi (local parquet/), upload (sync to s3://data.sb/fred/cpi/).                                                                                                                                                                                                                                                                                                                                                                                              |
| **`data/aspe/fpl/`**                | ASPE Federal Poverty Guidelines fetch; Justfile fetch. Output: utils/post/data/fpl_guidelines.yaml (used by LMI discount logic).                                                                                                                                                                                                                                                                                                                                                            |
| **`data/resstock/`**                | ResStock metadata: identify HP customers, heating type, assign_utility_ny (NY). Justfile for fetch, test-download, resstock-identify-hp-customers, assign-utility-ny. Data is put on S3 separately; rate_design Justfiles do not invoke data pipelines.                                                                                                                                                                                                                                     |
| **`utils/`**                        | Cross-jurisdiction utilities split by run phase: `utils/pre/` (tariff creation, scenario YAMLs, marginal-cost allocation, config validation), `utils/mid/` (mid-run scripts consuming earlier CAIRO outputs: calibrated tariff promotion, subclass revenue requirements, seasonal discount derivation, ICOS tariff derivation, output resolution), `utils/post/` (post-run: LMI discount application). CAIRO helpers in `utils/cairo.py`. All runnable as CLI or imported by `rate_design`. |
| **`context/`**                      | Reference docs and research notes for agents; see **Reference context** below and **`context/README.md`** for what lives where.                                                                                                                                                                                                                                                                                                                                                             |
| **`tests/`**                        | Pytest tests; mirror `utils/` and key `rate_design` behavior.                                                                                                                                                                                                                                                                                                                                                                                                                               |
| **`.devcontainer/`**                | Dev container and install scripts. CI uses runner-native workflow (`just install` then `just check` / `just test`); optional devcontainer for local/DevPod.                                                                                                                                                                                                                                                                                                                                 |
| **`infra/`**                        | Terraform and scripts for EC2/dev environment (e.g. `dev-setup`, `dev-teardown`).                                                                                                                                                                                                                                                                                                                                                                                                           |

## Reference context

Repeatable procedures live in `.cursor/skills/` and load when their description matches the task. `context/` is reference for a file a skill or a task names. Do not open `context/README.md` to find a procedure.

**Conventions:**

- **`context/sources/papers/`** — Academic papers (e.g. Bill Alignment Test). Extracted from PDFs via the pdf-to-markdown command.
- **`context/docs/`** — Technical documentation (e.g. Cambium, ResStock dataset docs). Extracted from PDFs via the pdf-to-markdown command.
- **`context/domain/`** — General domain knowledge: policy explainers, program guides, regulatory and institutional background. Documents that answer "how does this work in the real world?" (rate design, LMI programs, bulk transmission, ECOS vs MCOS). Subdirs: `bat_mc_residual/` (fairness, ECOS/MCOS, residual allocation), `charges/` (LMI programs, gas heating rates), `marginal_costs/` (bulk transmission cost recovery).
- **`context/methods/`** — Methodology writeups: conceptual framing, formulas, literature, design choices that feed our methodology. Documents that answer "how do we justify and operationalize this?" (BAT and residual allocation, TOU design, deriving BAT inputs from MCOS). Subdirs: `bat_mc_residual/`, `tou_and_rates/`, `marginal_costs/`.
- **`context/code/`** — Implementation notes: how libraries and pipelines work, how to run and wire code. Documents that answer "how do I implement or run this?" (CAIRO behavior, orchestration, data sources, marginal-cost pipelines). Subdirs: `orchestration/`, `cairo/`, `data/`, `marginal_costs/`.

For Cambium, ResStock dataset semantics, or the Bill Alignment Test, read the matching file under `context/docs/` or `context/sources/papers/`. PUMS dictionaries are release-specific (1-year vs 5-year, by year). If code disagrees with those docs, say so. The file list is `context/README.md`; open the one row the task needs. Parquet reads use the `polars-parquet` skill. Marginal-cost, LMI, and charge-classification procedures are still in `context/code/` and `context/methods/` until they have skills.

To add or refresh extracted PDF content: use the **extract-pdf-to-markdown** slash command, which lives in the [reports2 repo](https://github.com/switchbox-data/reports2) (`.cursor/commands/extract-pdf-to-markdown.md`; run it there, then copy the markdown here), and place output under `context/docs/` or `context/sources/papers/` as appropriate.

## How to work in this repo

- **Tasks**: Use **Just** as the main interface. Root `Justfile` defines `install`, `check`, `test`, `check-deps`, and dev/DevPod targets. Shared rate design recipes live in `rate_design/hp_rates/Justfile`; state-specific thin wrappers in `rate_design/hp_rates/{ny,ri}/Justfile` (import the shared file, add state-only recipes). Data-specific tasks live in data subdirectories (e.g. `data/eia/hourly_loads/Justfile`, `data/resstock/Justfile`, `data/fred/cpi/Justfile`). Ad hoc scripts should typically by invoked via `just` recipes. Just syntax is tricky, especially for inline shell code. See the syntax [here](https://github.com/casey/just), and prefer external shell scripts to inline shell recipes if they go from command invocation to full-on scripts. **Invocation patterns** (from `rate_design/hp_rates/`): `just s <state> <recipe>` (dispatch recipe that sources `<state>/state.env`), `just -f <state>/Justfile <recipe>` (state wrapper directly), or `source <state>/state.env && just <recipe>` (manual env loading).
- **Python**: The project uses **uv** for dependency and env management (see `pyproject.toml`). The resulting virtualenv is created in root of the project (at `.venv/`) but it is .gitignored. CAIRO is a **private Git dependency**; CI and devcontainer rely on `GH_PAT` for cloning. **Always use `uv run python` (never bare `python3` or `python`)** — the system Python does not have project dependencies like pyyaml, polars, etc. Examples: `uv run python -m pytest tests/`, `uv run python utils/...`, `uv run python3 -c "import yaml; ..."`. This applies to shell scripts, Justfile recipes, and inline python snippets alike. Use **Python 3.12+**.
- **Data**: Versioned inputs are under `rate_design/.../config/tariffs/electric/` and `.../tariffs/gas/` (JSON) and `.../config/tariff_maps/electric/` and `.../config/tariff_maps/gas/` (CSV). Don’t commit large buildstock or CAIRO case outputs; use `.gitignore` and S3/local paths as in existing Justfiles.
- **AWS authentication**: we rely heavily on reading and writing data to s3. We use short-lived AWS SOO config; if it must be refreshed, use `just aws` in the root.

## Computing contexts

- Data scientists' laptops, usually Macs with Apple Silicon
- EC2 instances luaunched by terraform scripts in `infra/`
- devcontainers running on a laptop or on an instance using Devpod
- When relevant, be aware of what context you are in.

## Code Style

Match existing style: Ruff for formatting/lint, **ty** for type checking, dprint for md formatting using `.mardownlint.json` and shfmt for shell scripts. Keep new code consistent with current patterns in `utils/` and `rate_design/`.

**LaTeX in markdown:** GitHub's MathJax renderer does not support escaped underscores inside `\text{}` (e.g. `\text{avg\_mc\_peak}` will fail). Use proper math symbols instead: `\overline{MC}_{\text{peak}}`, `MC_h`, `L_h`, etc. Bare subscripts and `\text{}` with simple words (no underscores) are fine.

**LaTeX in Cursor chat:** When running inside Cursor, use `$$...$$` for display math and `$...$` for inline math. Cursor's chat renderer does not support `\[...\]` or `\(...\)` — the backslashes are consumed by the markdown parser, leaving bare brackets that the math renderer ignores.

**Currency dollar signs in chat:** Cursor's chat renderer treats `$...$` as LaTeX math delimiters. A bare `$` used for currency (e.g. `$1.4M`) will pair with the next `$` on the same or a later line, and everything in between renders as garbled math. **In chat output, always wrap ANY bare `$` in backticks** — not just obvious currency amounts like `$1.4M` and `$6/month`, but also unit notations like `$/kWh`, `$/month`, `$/day`, and any other string containing `$`. If it has a dollar sign and it's not LaTeX math, it needs backticks. This only applies to Cursor chat responses — `.md` files committed to the repo should use bare `$` for currency as usual.

## Code Quality (required before every commit)

- Run `just check` — no linter errors, no type errors, no warnings
  - `just check` runs lock validation (`uv lock --locked`) and **prek** (ruff, formatting, type checking)
  - Pre-commit hooks enforce: ruff-check, ruff-format, ty-check, trailing whitespace,
    end-of-file newline, YAML/JSON/TOML validation, no large files (>600KB),
    no merge conflict markers
- Run `just test` — all tests pass; Aad or extend tests in `tests/` for new or changed behavior.

## Best practices

### Justfiles

- **Path variables**: Any Just variable that holds a file or directory path should be named with a `path_` prefix (e.g. `path_project_root`, `path_output_dir`, `path_rateacuity_yaml`). This makes it clear which variables are paths and keeps naming consistent across Justfiles.
- **Recipe args and script args**: Parameter names in a Just recipe should match the script’s CLI argument names they are wired to (e.g. recipe `path_yaml` and `path_output_dir` → script `--yaml` and `--output-dir`). Use the same naming convention (path_ prefix for paths) in both so the wiring is obvious.

### Python scripts (CLI)

- **Path arguments**: CLI arguments that are file or directory paths should use a `path_` prefix in the argparse name (e.g. `path_yaml`, `path_output_dir`), or a long option that makes the path explicit (e.g. `--output-dir`). When the script is invoked from a Justfile, use the same names as the Just variables (path_…) so recipe and script stay in sync.

Recipe names, `path_local_*` / `path_s3_*`, and the fetch-then-upload order for a pipeline under `data/` are the `data-pipeline` skill.

## Dependencies

- `uv add <package>` (updates pyproject.toml + uv.lock); never use `pip install
- Commit lock files (uv.lock) when adding dependencies

## MCP Tools

### Context7

When writing or modifying code that uses a library, use the Context7 MCP server to fetch
up-to-date documentation for that library. Do not rely on training data for API signatures,
function arguments, or usage patterns — always resolve against Context7 first.

### Linear

Creating or updating a Linear issue is the `linear-issue` skill.

## Libraries

This is a scientific computing Python codebase. Prefer Polars; the `polars-parquet` skill is how to read and collect it. CAIRO itself is pandas.

## Conventions agents should follow

1. **Do not add intermediates to context:** Agent plans, GitHub (or Linear) issue bodies, design drafts, and other working artifacts should not be added under `context/`. Do not commit issue-body or issue-template markdown files to the repo (not in `context/`, not in `.github/`). `context/` is for reference material only (see `context/README.md`).
2. **Prefer existing entrypoints**: Add or use `just` recipes and `utils` CLIs rather than one-off scripts at the repo root.
3. **Respect data boundaries**: Don't assume large data is in git; follow S3/local paths and env (e.g. AWS, `GH_PAT`) documented in Justfiles and CI.
4. **Data pipeline conventions**: Adding or changing a pipeline under `data/` is the `data-pipeline` skill.
5. **Update the context index**: When adding or removing files under `context/`, update `context/README.md` so the index stays accurate.
6. **Type and style**: Use type hints and Ruff; run `just check` before considering a change done.

## Pull requests

Opening a pull request is the `pull-request` skill.

## Quick reference

- **Install deps**: `just install`
- **Lint / format / typecheck**: `just check`
- **Tests**: `just test`
- **Dependency hygiene**: `just check-deps`
- **Project root (scripts)**: `utils.get_project_root()` or `git rev-parse --show-toplevel`
