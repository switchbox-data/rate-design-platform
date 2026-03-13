## Summary
This PR generalizes post-run validation across the full NY/RI scenario matrix and updates the shared validator to match the actual seasonal TOU and demand-flex contracts.

## What Changed
- Removed `1-8` assumptions from the shared validation flow:
  - dynamic run-block discovery from scenario YAMLs in [`utils/post/validate/config.py`](/ebs/home/sherry_switch_box/rate-design-platform/utils/post/validate/config.py)
  - `--runs` is optional and defaults to all scenario runs in [`utils/post/validate/__main__.py`](/ebs/home/sherry_switch_box/rate-design-platform/utils/post/validate/__main__.py)
  - shared `validate-runs` recipe no longer hardcodes run numbers in [`rate_design/hp_rates/Justfile`](/ebs/home/sherry_switch_box/rate-design-platform/rate_design/hp_rates/Justfile)
- Kept NY and RI on the same validation logic by driving checks from mechanism, not state.
- Updated seasonal tariff validation in [`utils/post/validate/checks.py`](/ebs/home/sherry_switch_box/rate-design-platform/utils/post/validate/checks.py):
  - skips non-seasonal companion flat tariffs in subclass runs
  - validates the right TOU invariants instead of assuming every summer period exceeds every winter period
  - clarifies CAIRO 1-based period normalization in logs/docstrings
- Updated demand-flex validation behavior in [`utils/post/validate/__main__.py`](/ebs/home/sherry_switch_box/rate-design-platform/utils/post/validate/__main__.py) and [`utils/post/validate/checks.py`](/ebs/home/sherry_switch_box/rate-design-platform/utils/post/validate/checks.py):
  - flex runs use subclass revenue expectations instead of no-flex subclass RR neutrality
  - skips legacy BAT checks that are not meaningful for delivery-only flex runs
  - adds an explicit cross-run check that HP weighted subclass revenue falls in flex runs versus the matching no-flex run
- Improved validator reporting:
  - failure summaries now include the run or run-pair that produced the failure
  - per-block `checks_summary.csv` includes block/run context columns
- Added regression coverage in [`tests/test_validate_checks.py`](/ebs/home/sherry_switch_box/rate-design-platform/tests/test_validate_checks.py)

## Validation
- `UV_CACHE_DIR=/tmp/uv-cache uv run ruff check utils/post/validate/config.py utils/post/validate/__main__.py utils/post/validate/__init__.py utils/post/validate/checks.py tests/test_validate_checks.py`
- `UV_CACHE_DIR=/tmp/uv-cache uv run pytest tests/test_validate_checks.py`
- `UV_CACHE_DIR=/tmp/uv-cache uv run python -m utils.post.validate --help`
- Verified dynamic blocks resolve for both NY and RI scenario layouts
- Verified current RI validation outputs for runs `1-8` are clean and that flex HP subclass revenue falls in runs `13/14` versus `9/10`

## Notes
- The worktree contains unrelated modified tariff and revenue files; they are intentionally excluded from this PR.
- Full end-to-end RI validation was not re-run after the last validator edits.

Closes #<ISSUE_NUMBER>
