# rate-design-platform

This repository is a clean scaffold for rate design analysis, focused on New York State. It provides a structured starting point for implementing rate design logic, data handling, and testing. For old ochre sims code see the [ochre-sims-rate-design](https://github.com/switchbox-data/rate-design-platform-archive) repository.

## Layout

- `src/rate_design/` — package skeleton for shared logic, utilities, and New York–specific code.
- `data/ny/` — local cache for BuildStock and CAIRO inputs/outputs (kept out of git).
- `scripts/` — helper scripts (e.g., running a NY heat pump rate scenario).
- `tests/` — placeholder test files to fill in alongside new code.

## Notes
- Data under `data/` should remain local or synced via S3 tooling you add; keep large artifacts out of git.
