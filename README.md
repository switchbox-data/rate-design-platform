# rate-design-platform

This repository is a clean scaffold for rate design analysis. It preserves the folder layout used by the prior project while leaving code and data empty so new implementation work can start fresh.

## Layout

- `src/rate_design/` — package skeleton for shared logic, utilities, and New York–specific code.
- `data/ny/` — local cache for BuildStock and CAIRO inputs/outputs (kept out of git).
- `scripts/` — helper scripts (e.g., running a NY heat pump rate scenario).
- `tests/` — placeholder test files to fill in alongside new code.

## Getting started

1) Create and activate a virtual environment (e.g., `python -m venv .venv && source .venv/bin/activate`).
2) Install in editable mode once dependencies are added: `pip install -e .[dev]`.
3) Fill in modules under `src/rate_design` and expand tests under `tests`.

## Notes

- Add real dependencies to `pyproject.toml` as you build out functionality.
- Data under `data/` should remain local or synced via S3 tooling you add; keep large artifacts out of git.
