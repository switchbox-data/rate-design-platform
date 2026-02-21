# MC-driven TOU derivation and tariff builders

## What changed

TOU logic is now split by responsibility:

- `utils/pre/compute_tou.py` contains **derivation math** and season helpers.
- `utils/pre/create_tariff.py` contains **URDB tariff constructors** (`create_*` functions).
- `utils/pre/derive_seasonal_tou.py` is the **standalone preprocessing CLI** that writes:
  - TOU tariff JSON
  - season derivation spec JSON

This mirrors existing `utils/pre` conventions where `create_*` modules construct artifacts and `compute_*` modules derive metrics/inputs.

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

## RI scenario integration

`rate_design/ri/hp_rates/run_scenario.py` supports optional runtime derivation through `tou_derivation` in YAML:

```yaml
tou_derivation:
  enabled: true
  tou_tariff_key: rie_tou_hp
  flat_tariff_key: rie_a16
  tou_window_hours: 4
  tou_base_rate: 0.06
  tou_fixed_charge: 6.75
```

When enabled, `run_scenario.py` derives the TOU tariff and map, writes them under `config/tariffs/electric/` and `config/tariff_maps/electric/`, and then runs normal CAIRO simulation using those artifacts.

## Standalone commands

Derive seasonal TOU artifacts end-to-end:

```bash
uv run python -m utils.pre.derive_seasonal_tou <args>
```
