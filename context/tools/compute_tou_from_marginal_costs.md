# MC-driven TOU derivation and tariff builders

## Architecture

TOU logic is split by responsibility:

- `utils/pre/compute_tou.py` — **derivation math** and season helpers.
- `utils/pre/create_tariff.py` — **URDB tariff constructors** (`create_*` functions).
- `utils/pre/derive_seasonal_tou.py` — **standalone preprocessing CLI** that writes TOU tariff JSON and season derivation spec JSON.

This mirrors `utils/pre` conventions where `create_*` modules construct artifacts and `compute_*` modules derive metrics/inputs.

## Core derivation functions (`utils/pre/compute_tou.py`)

- `combine_marginal_costs(bulk_mc, distribution_mc)` combines bulk and distribution MC into one hourly `$ / kWh` series.
- `find_tou_peak_window(combined_mc, hourly_system_load, window_hours)` finds the contiguous peak window with the highest demand-weighted MC.
- `compute_tou_cost_causation_ratio(combined_mc, hourly_system_load, peak_hours)` computes the peak/off-peak demand-weighted MC ratio.
- `compute_seasonal_base_rates(...)` derives season-specific base rates while preserving the configured annual average base rate.

## Tariff constructors (`utils/pre/create_tariff.py`)

- `create_tou_tariff(...)` builds a 2-period annual TOU tariff.
- `create_seasonal_tou_tariff(...)` builds a 2N-period seasonal TOU tariff from per-season specs.
- `create_seasonal_tariff(...)` builds an N-period seasonal flat tariff.
- `create_default_flat_tariff(...)` builds a single-period flat tariff.

## Base rate and fixed charge: reference tariff

Instead of hardcoding `tou_base_rate` and `tou_fixed_charge`, the CLI accepts `--reference-tariff` — a path to an existing URDB v7 tariff JSON. The script infers the base rate and fixed charge from that tariff. Explicit `--tou-base-rate` / `--tou-fixed-charge` flags still exist as overrides but are optional when a reference tariff is provided.

## Customer count: dynamic lookup

The CLI no longer accepts `--customer-count`. Instead it takes `--path-electric-utility-stats` (an EIA-861 utility stats parquet) and looks up the residential customer count at runtime using `utils.mid.data_parsing.get_residential_customer_count_from_utility_stats`.

## RI scenario integration

`rate_design/ri/hp_rates/run_scenario.py` supports optional runtime derivation through `tou_derivation` in YAML:

```yaml
tou_derivation:
  enabled: true
  tou_tariff_key: rie_tou_hp
  flat_tariff_key: rie_a16
  tou_window_hours: 4
```

When enabled, `run_scenario.py` derives the TOU tariff and map, writes them under `config/tariffs/electric/` and `config/tariff_maps/electric/`, and then runs normal CAIRO simulation using those artifacts.

## Standalone CLI

```bash
uv run python -m utils.pre.derive_seasonal_tou \
  --cambium-path <path> \
  --state RI --utility rie --year 2025 \
  --path-td-marginal-costs <path> \
  --resstock-metadata-path <path> \
  --resstock-loads-path <path> \
  --path-electric-utility-stats <path> \
  --reference-tariff <path-to-urdb-json> \
  --tou-tariff-key rie_seasonal_tou_hp \
  --output-dir <path>
```

Optional flags: `--winter-months`, `--tou-window-hours`, `--tou-base-rate` (override), `--tou-fixed-charge` (override), `--periods-yaml`.

## Justfile recipe

The `create-seasonal-tou` recipe in `utils/Justfile` wraps the CLI. The RI Justfile calls it like:

```just
create-seasonal-tou reference_tariff:
    just {{path_repo}}/utils/Justfile create-seasonal-tou \
      ... \
      {{path_electric_utility_stats}} \
      {{reference_tariff}}
```

The `reference_tariff` argument is passed from recipes like `run-9` and `run-10` which resolve the calibrated flat tariff path from a prior CAIRO run.
