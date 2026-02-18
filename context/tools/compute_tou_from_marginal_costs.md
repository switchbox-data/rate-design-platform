# MC-driven seasonal TOU tariff derivation

## What it does

The TOU derivation workflow produces a seasonal Time-of-Use (TOU) electricity tariff directly from marginal cost data and system load. It always produces a seasonal+TOU tariff — winter and summer each get their own peak window and cost-causation-based price differential, resulting in a 4-period tariff (winter off-peak, winter peak, summer off-peak, summer peak).

The workflow is split across two modules:

- **`utils/pre/compute_tou.py`** — composable building blocks (data structures, primitives, tariff builders, serialization helpers).
- **`utils/pre/derive_seasonal_tou.py`** — standalone script that loads marginal costs + ResStock data, calls the building blocks, and writes outputs.

## Why

Static TOU tariffs embed assumptions about when system costs are highest. By computing the TOU structure from the same Cambium + distribution marginal costs that feed the Bill Alignment Test, the tariff automatically reflects the cost reality of the modeled year and region. The seasonal structure captures the different cost profiles between heating-dominated winter and cooling-dominated summer, which is critical for heat-pump rate design.

## Architecture: composable primitives (`compute_tou.py`)

### Data structures

- **`Season(name, months)`** — a named season defined by a list of 1-indexed calendar months.
- **`SeasonTouSpec(season, base_rate, peak_hours, peak_offpeak_ratio)`** — per-season TOU derivation result. Produced by the caller, consumed by `make_seasonal_tou_tariff`.

### Serialization

- **`save_season_specs(specs, path)`** — serialize `SeasonTouSpec` list to JSON.
- **`load_season_specs(path)`** — deserialize JSON back to `SeasonTouSpec` list. Used by `run_scenario.py` to load the derivation spec for demand shifting.

### Season helpers

- **`make_winter_summer_seasons(summer_months)`** — returns `[Season("winter", …), Season("summer", …)]`. Defaults to June–September as summer.
- **`season_mask(index, season)`** — boolean array for timestamps in a season's months.
- **`compute_seasonal_base_rates(combined_mc, hourly_system_load, seasons, base_rate)`** — derives per-season flat rates from demand-weighted MC ratios, scaled so the load-weighted average equals `base_rate`.

### Primitives (work on any hourly slice)

1. **`combine_marginal_costs(bulk_mc, distribution_mc)`** — aligns Cambium bulk MC (energy + capacity) with distribution MC and sums them into a single `combined_mc` Series ($/kWh).

2. **`find_tou_peak_window(combined_mc, hourly_system_load, window_hours=4)`** — finds the contiguous block of `window_hours` hours with the highest demand-weighted average MC across the 24-hour day. Operates on whatever hourly slice is passed in (full year or a single season's hours).

3. **`compute_tou_cost_causation_ratio(combined_mc, hourly_system_load, peak_hours)`** — computes `(demand-weighted avg MC during peak) / (demand-weighted avg MC during off-peak)`. Also operates on any hourly slice.

### Tariff builders

- **`make_seasonal_tariff(label, seasons, …)`** — N-period seasonal flat URDB v7 tariff (no TOU differentiation within a season). One energy-rate period per season.
- **`make_seasonal_tou_tariff(label, specs, …)`** — 2N-period seasonal+TOU URDB v7 tariff. For each `SeasonTouSpec` at index *i*: period `2·i` = off-peak at `base_rate`, period `2·i+1` = peak at `base_rate × ratio`.

### Tariff map

- **`generate_tou_tariff_map(customer_metadata, tou_tariff_key, flat_tariff_key)`** — HP customers → TOU tariff key, everyone else → flat key.

## Standalone derivation script (`derive_seasonal_tou.py`)

`utils/pre/derive_seasonal_tou.py` is the end-to-end script that:

1. Loads Cambium (bulk) marginal costs from S3 or local.
2. Loads distribution marginal costs from S3.
3. Loads ResStock metadata and building loads; computes aggregate system load.
4. Calls the composable primitives to derive per-season peak windows and ratios.
5. Writes three output files:

| Output | Path (relative to `--output-dir`) | Purpose |
| --- | --- | --- |
| Tariff JSON | `tariffs/electric/{tou_tariff_key}.json` | URDB v7 tariff for CAIRO |
| Tariff map CSV | `tariff_maps/electric/{tou_tariff_key}_tariff_map.csv` | HP → TOU, non-HP → flat |
| Derivation spec JSON | `tou_derivation/{tou_tariff_key}_derivation.json` | Season specs for demand shifting |

### Usage

Via Justfile (from `rate_design/ri/hp_rates/`):

```bash
just derive-seasonal-tou rie_seasonal_tou_hp rie_a16
```

Or directly:

```bash
uv run python -m utils.pre.derive_seasonal_tou \
  --cambium-path s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet \
  --state RI --region isone --utility rie --year 2025 \
  --resstock-metadata-path /data.sb/nrel/resstock/.../metadata-sb.parquet \
  --resstock-loads-path /data.sb/nrel/resstock/.../load_curve_hourly/state=RI/upgrade=00 \
  --customer-count 451381 \
  --tou-tariff-key rie_seasonal_tou_hp --flat-tariff-key rie_a16 \
  --output-dir rate_design/ri/hp_rates/config
```

## Integration with `run_scenario.py`

The RI `run_scenario.py` is organized into phases:

1. **Load data** — customer metadata, building loads, bulk + distribution marginal costs.
2. **Demand shifting** (optional) — when `demand_flex.enabled` is set in the YAML, reads the pre-computed TOU tariff JSON and derivation spec JSON, then applies seasonal demand response to HP-customer loads.
3. **Initialize tariffs** — `_initialize_tariffs`, precalc mapping, export compensation.
4. **Run CAIRO** — `MeetRevenueSufficiencySystemWide.simulate()`.

The `demand_flex` block in `scenarios.yaml`:

```yaml
demand_flex:
  enabled: true
  demand_elasticity: -0.1
  tou_tariff_key: rie_seasonal_tou_hp
  tou_derivation_path: tou_derivation/rie_seasonal_tou_hp_derivation.json
```

The tariff JSON and tariff map paths are specified directly in the YAML (under `path_tariffs_electric` and `path_tariff_maps_electric`), just like any other tariff. The TOU derivation is a **pre-processing step** run before `run_scenario`.

## Key assumptions and limitations

- **Window is contiguous and wraps midnight.** The peak window is always a single contiguous block (e.g. 4 consecutive hours). It can wrap around midnight (e.g. `[22, 23, 0, 1]`).
- **Two seasons (winter/summer).** The current helper produces exactly two seasons. The primitives and tariff builders support N seasons but the convenience function is two-season.
- **Base rate is an input, not derived from revenue requirement.** The `tou_base_rate` is set in config (default $0.06/kWh). CAIRO's precalc/calibration step will adjust the actual rate to meet the revenue requirement, but the *ratios* between seasons and between peak/off-peak are fixed by cost causation.
- **HP-only assignment.** Only customers with `postprocess_group.has_hp == True` are assigned the TOU tariff. All others get the flat tariff.
