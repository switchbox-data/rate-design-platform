# MC-driven TOU derivation and tariff builders

## Architecture

TOU logic is split by responsibility:

- `utils/pre/compute_tou.py` — **derivation math** and season helpers.
- `utils/pre/create_tariff.py` — **URDB tariff constructors** (`create_*` functions).
- `utils/pre/derive_seasonal_tou.py` — **standalone preprocessing CLI** that writes TOU tariff JSON and season derivation spec JSON.

This mirrors `utils/pre` conventions where `create_*` modules construct artifacts and `compute_*` modules derive metrics/inputs.

## Core derivation functions (`utils/pre/compute_tou.py`)

- `combine_marginal_costs(bulk_mc, distribution_mc)` combines bulk and distribution MC into one hourly `$ / kWh` series.
- `find_tou_peak_window(combined_mc, hourly_load, window_hours)` finds the contiguous peak window with the highest demand-weighted MC. Used by both runtime derivation and the window-width sweep.
- `compute_tou_cost_causation_ratio(combined_mc, hourly_load, peak_hours)` computes the peak/off-peak demand-weighted MC ratio.
- See `context/tools/tou_window_optimization.md` for how `window_hours` (the window width $N$) is selected per utility before runtime.
- `compute_seasonal_base_rates(...)` derives season-specific base rates while preserving the configured annual average base rate.

## Tariff constructors (`utils/pre/create_tariff.py`)

- `create_tou_tariff(...)` builds a 2-period annual TOU tariff.
- `create_seasonal_tou_tariff(...)` builds a 2N-period seasonal TOU tariff from per-season specs.
- `create_seasonal_tariff(...)` builds an N-period seasonal flat tariff.
- `create_default_flat_tariff(...)` builds a single-period flat tariff.

## Base rate and fixed charge: reference tariff

Instead of hardcoding `tou_base_rate` and `tou_fixed_charge`, the CLI accepts `--reference-tariff` — a path to an existing URDB v7 tariff JSON. The script infers the base rate and fixed charge from that tariff. Explicit `--tou-base-rate` / `--tou-fixed-charge` flags still exist as overrides but are optional when a reference tariff is provided.

## Customer count: dynamic lookup

The CLI no longer accepts `--customer-count`. Instead it takes `--path-electric-utility-stats` (an EIA-861 utility stats parquet) and looks up the residential customer count at runtime using `utils.scenario_config.get_residential_customer_count_from_utility_stats`.

## RI scenario integration

`rate_design/hp_rates/run_scenario.py` supports optional runtime derivation through `tou_derivation` in YAML:

```yaml
tou_derivation:
  enabled: true
  tou_tariff_key: rie_tou_hp
  flat_tariff_key: rie_a16
  tou_window_hours: 4
```

When enabled, `run_scenario.py` derives the TOU tariff and map, writes them under `config/tariffs/electric/` and `config/tariff_maps/electric/`, and then runs normal CAIRO simulation using those artifacts.

## Marginal cost loading for TOU

### `derive_seasonal_tou.py` (standalone CLI / `create-seasonal-tou` recipe)

The `create-seasonal-tou` Justfile recipe passes `path_supply_energy_mc` and
`path_supply_capacity_mc` directly from the Justfile variables — which always point to real
supply MC files (e.g. NYISO LBMP + ICAP parquets for NY). No separate TOU supply path is
needed.

### Phase 1.75 demand-flex recomputation (`utils/demand_flex.py`)

TOU cost-causation ratios must be identical between delivery-only and supply runs so that
peak windows and peak/off-peak multipliers are consistent. Phase 1.75 therefore always uses
**real (non-zero)** bulk supply MCs, regardless of whether the CAIRO run is delivery-only.
The scenario's `bulk_marginal_costs` (which may be zeros for delivery-only runs) is used
unchanged everywhere else (Phase 1a, Phase 2 RR computation).

`apply_demand_flex` accepts two optional params: `path_tou_supply_energy_mc` and
`path_tou_supply_capacity_mc`. When provided, it loads these via `_load_supply_marginal_costs`
specifically for Phase 1.75 cost-causation computation. If omitted, it falls back to the
scenario's `bulk_marginal_costs` (which works correctly for supply runs where that is
already real).

These paths come from the **Justfile** `run-scenario` recipe via CLI args
(`--path-tou-supply-energy-mc`, `--path-tou-supply-capacity-mc`), wired to the
Justfile-level `path_supply_energy_mc` / `path_supply_capacity_mc` variables — the same
real paths used by `create-seasonal-tou`. They are **not** YAML fields or Google Sheet
columns.

There is no `path_tou_supply_mc` field in scenario YAMLs or Google Sheet columns.

### Main supply MC loading

The main supply MC loader (`utils.cairo._load_supply_marginal_costs()`) supports both separate energy/capacity files and Cambium files for backward compatibility:

- **Separate files**: If both `path_supply_energy_mc` and `path_supply_capacity_mc` are provided and neither contains "cambium", they're loaded separately and combined.
- **Cambium files**: If either path contains "cambium", the loader automatically uses `_load_cambium_marginal_costs()` to load the combined file, skipping the separate file loading. This provides backward compatibility for legacy RI runs that used Cambium paths.

## Standalone CLI

```bash
uv run python -m utils.pre.derive_seasonal_tou \
  --path-supply-energy-mc <path> \
  --path-supply-capacity-mc <path> \
  --state RI --utility rie --year 2025 \
  --path-dist-and-sub-tx-mc <path> \
  --path-utility-assignment <path> \
  --path-electric-utility-stats <path> \
  --reference-tariff <path-to-urdb-json> \
  --tou-tariff-key rie_seasonal_tou_hp \
  --output-dir <path>
```

For Cambium-based states (RI), pass the same Cambium path to both `--path-supply-energy-mc` and `--path-supply-capacity-mc`; the loader detects "cambium" in the path and routes to the combined Cambium loader automatically.

Optional flags: `--winter-months`, `--tou-window-hours`, `--tou-base-rate` (override), `--tou-fixed-charge` (override), `--periods-yaml`, `--path-bulk-tx-mc` (NY-only).

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
