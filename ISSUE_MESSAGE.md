Title: Generalize post-run validation to support NY and RI scenario run sets

Status: Backlog
Project: rate-design-platform (confirm)
Milestone: (confirm)
Assignee: (confirm)

## What

Update `utils/post/validate` and validation Just recipes so validation works for both NY and RI without assuming fixed runs `1-8`.

## Why

Both NY and RI scenarios now include extended run sets (up to `1-16`). The validation flow currently relies on hardcoded run blocks and defaults that only cover `1-8`, which causes incomplete validation coverage and extra manual overrides.

## How

1. Replace hardcoded run block definitions in `utils/post/validate/config.py` with dynamic block discovery from scenario YAML run configs.
2. Pair runs by shared attributes and opposite `cost_scope` (`delivery` vs `delivery+supply`) to build validation blocks for flat, seasonal, seasonal TOU, and TOU flex sequences.
3. Update CLI defaults in `utils/post/validate/__main__.py` so `--runs` is optional and defaults to all runs present in the scenario YAML.
4. Update shared `rate_design/hp_rates/Justfile` `validate-runs` recipe to only pass `--runs` when explicitly provided.
5. Update NY wrapper comments to reflect all-scenario-run validation behavior.

## Deliverables

- PR updating dynamic run-block construction in [`utils/post/validate/config.py`](/ebs/home/sherry_switch_box/rate-design-platform/utils/post/validate/config.py).
- PR updating run selection behavior in [`utils/post/validate/__main__.py`](/ebs/home/sherry_switch_box/rate-design-platform/utils/post/validate/__main__.py).
- PR updating shared validation recipe in [`rate_design/hp_rates/Justfile`](/ebs/home/sherry_switch_box/rate-design-platform/rate_design/hp_rates/Justfile).
- Validation command `just s ri validate-runs` uses latest complete RI batch and validates all runs in `scenarios_rie.yaml`.
