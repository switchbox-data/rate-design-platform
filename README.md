# rate-design-platform

This repository is a clean scaffold for rate design analysis, focused on New York State. It provides a structured starting point for implementing rate design logic, data handling, and testing. For old ochre sims code see the [ochre-sims-rate-design](https://github.com/switchbox-data/rate-design-platform-archive) repository.

## Layout

- `rate_design/` — package root.
  - `ny/hp_rates/`
    - `config/` — local inputs/outputs; `buildstock_*` and `cairo_cases/` are git-ignored. Configs under `tariffs/` (electricity and gas) and `tariff_maps/` (electric and gas) stay versioned.
    - `scenarios/` — YAML configs selecting tariffs/mappings and other simulation parameters.
    - `scripts/` — helpers such as customer selection, tariff builders, and case path helpers.
    - `Justfile` — NY HP-specific recipes (stub).
  - `ny/ev_rates/` — stubbed EV structure (data, scenarios, scripts, Justfile).
- `utils/` — cross-jurisdiction utilities (buildstock IO, S3 sync, conversions).
- `tests/` — placeholder test files to expand alongside code.

## Notes

- Data under `rate_design/ny/hp_rates/config/` (buildstock raw/processed, cairo cases) should remain local or synced via S3 tooling you add; keep large artifacts out of git.
