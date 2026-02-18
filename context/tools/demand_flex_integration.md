# Demand flexibility integration into `run_scenario`

## What it does

`utils/cairo.py` implements a seasonal demand-response load-shifting module that models how heat-pump customers adjust their hourly consumption in response to a seasonal TOU price signal. Only buildings assigned to the TOU tariff (HP customers) are shifted; flat-tariff customers keep their original load profiles. Demand shifting is applied independently within each season (winter and summer), using the off-peak rate for that season as the equivalent flat rate. The shifted loads from all seasons are stitched into a single 8760-hour output that feeds into the revenue-requirement calculation and CAIRO billing engine.

## Why

CAIRO has demand-flexibility functions (`process_residential_hourly_demand_response_shift`, `_shift_building_hourly_demand` in `cairo/rates_tool/loads.py`) but they are **commented out** with no active call path from the simulator. Our implementation re-implements the same constant-elasticity model in a cleaner, vectorized form that:

- Works directly on the `raw_load_elec` DataFrame (MultiIndex `[bldg_id, time]`, column `electricity_net`) rather than requiring CAIRO-internal column names and Dask parallelism.
- Handles the 2N-period seasonal+TOU tariff produced by `derive_seasonal_tou`, applying shifting independently within each season.
- Integrates into the `run_scenario.py` pipeline as Phase 2 between data loading and revenue-requirement calculation.

## Mathematical framework

For each season, for each TOU building *b*, period *p*:

1. **Period consumption**: `Q_{b,p} = Σ_h electricity_net_{b,h}` for hours *h* in period *p*.
2. **Equivalent flat rate**: the off-peak rate for the current season (i.e. `SeasonTouSpec.base_rate`).
3. **Target consumption**: `Q*_{b,p} = Q_{b,p} · (P_p / P_flat)^ε`
4. **Period shift**: `Δ_{b,p} = Q*_{b,p} − Q_{b,p}`
5. **Zero-sum enforcement**: the receiver period (off-peak) absorbs `−Σ_{p≠r} Δ_{b,p}`.
6. **Hourly distribution** (proportional): `δ_{b,h} = Δ_{b,p} · (Q_{b,h} / Q_{b,p})`
7. **Shifted load**: `Q'_{b,h} = Q_{b,h} + δ_{b,h}`

The **realised elasticity** per building per period is tracked as a diagnostic: `ε_realised = log(Q_new / Q_orig) / log(P_new / P_orig)`.

## Functions in `utils/cairo.py`

| Function | Purpose |
| --- | --- |
| `extract_tou_period_rates(tou_tariff)` | Reads `energyratestructure` from a URDB v7 tariff JSON; returns a DataFrame of `(energy_period, tier, rate)`. |
| `assign_hourly_periods(hourly_index, tou_tariff)` | Maps each timestamp in an 8760-hour DatetimeIndex to its TOU `energy_period` using the tariff's weekday/weekend schedule matrices. Vectorized via numpy. |
| `apply_seasonal_demand_response(raw_load_elec, tou_bldg_ids, tou_tariff, demand_elasticity, season_specs)` | Main entry point. Iterates over `SeasonTouSpec` list, applies constant-elasticity shift within each season's months, stitches results into a single 8760 output. Returns `(shifted_load_elec, elasticity_tracker)`. |
| `load_distribution_marginal_costs(state, region, utility, year_run)` | Loads distribution marginal costs from S3 Hive-partitioned parquet. Returns a tz-aware Series. Used by both `derive_seasonal_tou.py` and `run_scenario.py`. |

## Two-step workflow

TOU derivation and demand shifting are now separate steps:

1. **Pre-processing**: Run `derive_seasonal_tou.py` (via `just derive-seasonal-tou`) to produce the tariff JSON, tariff map CSV, and derivation spec JSON. This is a one-time step per scenario configuration.
2. **Simulation**: Run `run_scenario.py` (via `just run-scenario`). When `demand_flex.enabled`, it reads the pre-computed outputs and applies demand shifting.

## Integration with `run_scenario.py`

The RI `run_scenario.py` is organized into phases:

1. **Phase 1 — Load data**: customer metadata, building loads, bulk + distribution marginal costs.
2. **Phase 2 — Demand shifting** (optional): when `demand_flex.enabled`, load the TOU tariff JSON and derivation spec JSON, apply `apply_seasonal_demand_response` to HP-customer loads. Produces `effective_load_elec` (shifted) and `elasticity_tracker`.
3. **Phase 3 — Initialize tariffs and system requirements**: `_initialize_tariffs`, precalc mapping, export compensation, and `_return_revenue_requirement_target` — all using `effective_load_elec` so the revenue requirement reflects the demand-response load shape.
4. **Phase 4 — Run CAIRO**: `bs.simulate()` with `customer_electricity_load=effective_load_elec`, so billing sees the shifted loads.

When `demand_flex.enabled` is `false` (or absent), `effective_load_elec = raw_load_elec` and the pipeline behaves exactly as before.

## Configuration

The `demand_flex` block in `scenarios.yaml` controls this feature:

```yaml
demand_flex:
  enabled: true
  demand_elasticity: -0.1        # short-run price elasticity (negative)
  tou_tariff_key: rie_seasonal_tou_hp   # key into path_tariffs_electric
  tou_derivation_path: tou_derivation/rie_seasonal_tou_hp_derivation.json
```

Each season uses its own off-peak rate as the equivalent flat rate automatically — no manual override is needed. The `tou_derivation_path` points to the derivation spec JSON produced by `derive_seasonal_tou.py`.

## Which buildings are shifted

Only buildings with `postprocess_group.has_hp == True` (heat-pump customers) are shifted. These are the same buildings assigned to the TOU tariff by `generate_tou_tariff_map`. Non-HP (flat-tariff) buildings keep their original load profiles.

## Downstream effects

- **Revenue requirement**: computed from `effective_load_elec`, so the system load profile reflects demand response. If load shifts from expensive peak hours to cheaper off-peak hours, the marginal-cost-weighted revenue requirement changes.
- **Billing**: CAIRO's `process_building_demand_by_period` and billing engine see the shifted loads, so customer bills reflect post-response consumption patterns.
- **BAT / cross-subsidisation**: the Bill Alignment Test uses the shifted loads and the revenue requirement derived from them, so the BAT metrics reflect the cost-causative outcome of both TOU pricing and demand response.

## Outputs

When `demand_flex.enabled`:

- `demand_flex_elasticity_tracker.csv` saved alongside CAIRO outputs — one row per TOU building, one column per period (4 periods for seasonal+TOU), showing realised elasticity.
- Log messages report total original vs. shifted kWh (should be equal within rounding) and mean achieved elasticity per period.

## Key assumptions and limitations

- **Constant elasticity**: the same `ε` applies to all buildings, all hours, all consumption levels. A simplification; real elasticity varies by building type, weather, and time of year.
- **Per-season receiver period**: the off-peak period for each season absorbs the shifted load within that season. No cross-season shifting occurs.
- **Zero-sum (temporal substitution, not conservation)**: total energy per building is preserved within each season. Load is shifted in time, not reduced.
- **Proportional hourly distribution**: hours with higher consumption receive proportionally more of the period shift. This preserves the intra-period load shape.
- **No physical constraints**: the model does not prevent unrealistic shifts (e.g., heating load cannot be arbitrarily deferred). In practice, conservative elasticity values (−0.1) keep shifts small.
- **Requires pre-computed TOU derivation**: demand shifting only applies when the seasonal TOU tariff has been derived by `derive_seasonal_tou.py`. Run the derivation step first.

## Relationship to CAIRO's implementation

See `context/tools/cairo_demand_flexibility_workflow.md` for CAIRO's commented-out demand-flex code. Our implementation:

- Uses the same mathematical framework (constant-elasticity period-level shift → proportional hourly allocation → zero-sum enforcement).
- Replaces CAIRO's per-building Dask dispatch with vectorized pandas operations.
- Works on `electricity_net` (the column used by `process_residential_hourly_demand` for system load) rather than `out.electricity.total.energy_consumption`.
- Handles seasonal+TOU tariffs natively (4 periods: winter off-peak/peak, summer off-peak/peak).
- Uses proper logging instead of debug `print` statements.
- Returns an elasticity tracker for validation.
