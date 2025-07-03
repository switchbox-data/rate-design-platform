# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

This project uses [just](https://github.com/casey/just) as a command runner and [uv](https://docs.astral.sh/uv/) for Python package management.

### Common Development Tasks
- `just install` - Set up development environment and install pre-commit hooks
- `just check` - Run all code quality tools (ruff, mypy, pre-commit, deptry)
- `just test` - Run pytest tests with doctests
- `just docs` - Serve documentation locally with live reload
- `just docs-test` - Build documentation to check for errors
- `just build` - Build wheel package
- `uv add <package>` - Add runtime dependency
- `uv add --dev <package>` - Add development dependency

### Running Individual Tools
- `uv run pytest tests/test_specific.py::test_function` - Run single test
- `uv run mypy src/` - Type check specific directory
- `uv run ruff check src/` - Lint specific directory
- `uv run ruff format src/` - Format specific directory

## Architecture Overview

This is a simulation testbed for analyzing the impact of electric rate designs on energy bills for households with distributed energy resources (DERs) and all-electric appliances.

### Core Components

**TOU HPWH Simulation (`src/first_pass.py`)**
- Implements first-pass time-of-use (TOU) scheduling decision model for Heat Pump Water Heaters (HPWHs)
- Uses OCHRE building physics simulation framework
- Models consumer response to TOU electricity rates with monthly decision cycles
- Key architecture: monthly simulation loop with human decision controller and building physics controller

**Key Simulation Elements:**
- **Monthly Decision Loop**: Consumers make TOU adoption/continuation decisions monthly based on bill feedback
- **Dual Schedule Types**: Default (always-on) vs TOU-adapted (peak-hour restricted)
- **Realized vs Unrealized Savings**: Distinction between anticipated savings (pre-adoption) and actual performance (post-adoption)
- **Comfort Penalties**: Models consumer discomfort from unmet hot water demand during peak restrictions

**Input Data Structure:**
- Building schedules: CSV with 15-minute interval data (35,040 intervals/year)
- Hot water usage patterns from `hot_water_fixtures` column
- Weather data: EPW files for ambient conditions
- Building models: XML configuration files

**Mathematical Framework:**
- Peak hours typically 2 PM - 8 PM (intervals 56-128 daily)
- TOU rates: `r_on` (peak) vs `r_off` (off-peak)
- Decision variables: `S_m^current` (schedule state), `x_m^switch` (switching decision)
- Cost components: electricity bills, switching costs, comfort penalties

### Testing Approach
- Tests live in `tests/` directory using pytest
- Follow test-driven development: write failing tests first, then implement functionality
- Test both static controller returns and full simulation logic

### Documentation
- Primary documentation in `docs/` directory using mkdocs
- Mathematical formulation detailed in `docs/docs_tou_hpwh_schedule_basic.qmd`
- Quarto documents (.qmd) are rendered to markdown for documentation site

### Package Management
- Dependencies managed in `pyproject.toml`
- Lockfile maintained in `uv.lock`
- Development dependencies separated into `dependency-groups.dev`
- Key dependencies: `ochre>=0.4.0`, `polars>=1.30.0`, `jupyter>=1.1.1`
