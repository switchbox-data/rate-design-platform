This PR has two distinct themes: fixing a tariff-naming bug that corrupted calibrated flat rates in multi-tariff runs, and generalizing the post-run validator to cover the full NY/RI scenario matrix.

## Fix: non-HP flat tariff naming in multi-tariff runs

Multi-tariff (subclass) runs write every tariff in `tariff_final_config.json` back to `config/tariffs/electric/`. When the non-HP subclass tariff shared the same key as the system-wide flat tariff (e.g. `rie_flat`), the subclass-calibrated version overwrote the system-wide rate, silently corrupting downstream runs that depend on it.

**Changes:**

- Added `{utility}_nonhp_flat.json` / `{utility}_nonhp_flat_supply.json` tariff files for all NY utilities (cenhud, coned, nimo, nyseg, or, psegli, rge) and RIE. These are identical to their `flat` counterparts except for the `label`/`name` field.
- Updated all NY and RI scenario YAMLs to use `{utility}_nonhp_flat` for the non-HP subclass in multi-tariff runs (5, 6, 9, 10, 13, 14). Single-tariff flat runs (1–4) are unchanged.
- Regenerated all NY and RI tariff maps to reference the new `nonhp_flat` keys.
- Added `utils/pre/fix_nonhp_flat_tariff_naming.py`: a reusable script that applies this rename across any utility/state. `utils/pre/update_sheet_formulas.py` now calls it automatically when generating Google Sheet path columns.
- Added RI nonhp_flat calibrated tariff files from the latest CAIRO run.

## Generalize post-run validation for NY and RI

The validator previously assumed exactly runs 1–8 and hardcoded delivery/supply block pairs. Both NY and RI now run up to 16 scenarios, so this hardcoding caused incomplete coverage and required manual `--runs` overrides.

**Changes:**

- `utils/post/validate/config.py`: replaced hardcoded run-block definitions with dynamic discovery from the scenario YAML. Blocks are now built by pairing runs with matching attributes and opposite `cost_scope` (`delivery` vs `delivery+supply`).
- `utils/post/validate/__main__.py`: `--runs` is now optional and defaults to all runs present in the scenario YAML. Flex runs use subclass revenue expectations (not no-flex RR neutrality). Adds an explicit cross-run check that HP weighted subclass revenue falls in flex runs versus the matching no-flex run. Failure summaries now include the run or run-pair that produced the failure.
- `utils/post/validate/checks.py`: seasonal tariff checks skip non-seasonal companion flat tariffs in subclass runs; validates correct TOU invariants instead of assuming every summer period exceeds every winter period; per-block `checks_summary.csv` includes block/run context columns.
- `rate_design/hp_rates/Justfile`: `validate-runs` recipe no longer hardcodes `1,2,3,4,5,6,7,8` — omitting `--runs` now validates all scenario runs.
- `rate_design/hp_rates/ri/Justfile`: added `validate-mc-data` recipe for convenient RI MC data validation.
- `tests/test_validate_checks.py`: new regression coverage for the updated check logic.

## Fix: explicit parquet paths in MC data validation

`utils/post/validate_ny_mc_data.py` previously passed directory paths to `scan_parquet`, which inadvertently read both `data.parquet` and `zero.parquet` when both existed in a partition, producing duplicate timestamps. Now always reads `data.parquet` explicitly with `read_parquet`; added path validation to prevent directory reads.

## Tariff and config updates from CAIRO runs

Updated RI and NY calibrated tariff files, scenario YAMLs, and the RI revenue requirement config to reflect outputs from the latest CAIRO runs.
