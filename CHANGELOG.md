# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-21

Initial release of the rate-design-platform package.

### Added

- ResStock data pipeline (`data/resstock/`) — fetch, modify, and upload NREL ResStock
  metadata and load curves to S3; supports NY and RI; includes HP customer identification,
  heating type classification, natural gas connection detection, vulnerability columns,
  non-HP load approximation, MF electricity adjustment, utility assignment, and monthly
  load aggregation.
- Cambium marginal-cost pipeline (`data/cambium/`) — fetch and process NREL Cambium
  marginal energy, generation capacity, and bulk transmission cost data.
- EIA data pipelines (`data/eia/`) — hourly zone loads and EIA-861 utility stats.
- HUD AMI and SMI pipelines (`data/hud/`) — area median income and state median income
  for LMI rate design.
- Census ACS PUMS pipeline (`data/census/pums/`) — person and housing microdata for
  vulnerability analysis.
- FRED CPI pipeline (`data/fred/cpi/`) — CPI series for real-dollar adjustments.
- ASPE Federal Poverty Guidelines pipeline (`data/aspe/fpl/`).
- HP rates scenario runner (`rate_design/hp_rates/`) — shared entrypoint and Justfile
  for NY and RI heat-pump-friendly rate design simulations.
- Pre-processing utilities (`utils/pre/`) — tariff mapping, scenario YAML generation,
  marginal-cost allocation, config validation.
- Mid-run utilities (`utils/mid/`) — calibrated tariff promotion, subclass revenue
  requirements, seasonal discount derivation, output resolution.
- Post-processing utilities (`utils/post/`) — LMI discount application, master bill and
  BAT table construction across utilities.
- Manifest and provenance tracking for the ResStock pipeline
  (`data/resstock/manifest.py`).

### Known limitations

- **CAIRO dependency**: CAIRO (the bill-alignment simulation engine) is a private Git
  dependency resolved via `[tool.uv.sources]`. This override is transparent to pip, so
  `pip install rate-design-platform` will attempt to install the unrelated `cairo` PyPI
  package (Cairo graphics bindings) rather than the correct engine. Team members should
  install via `uv` in a clone of this repository, which resolves the Git source
  automatically. A future release will address this by publishing CAIRO to a package
  index under a distinct name.

[Unreleased]: https://github.com/switchbox-data/rate-design-platform/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/switchbox-data/rate-design-platform/releases/tag/v0.1.0
