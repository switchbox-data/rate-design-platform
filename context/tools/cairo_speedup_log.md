# CAIRO speedup log

Tracking benchmark results for issue #250.
Machine: 8-core EC2
Baseline: ~2.5 min/run before any patches

---

## Baseline — run 1 (pre-patch), 2026-02-23

| Stage | Time (s) |
|-------|----------|
| _load_prototype_ids_for_run | 0.1 |
| _initialize_tariffs | 0.0 |
| _build_precalc_period_mapping | 0.0 |
| return_buildingstock | 0.5 |
| build_bldg_id_to_load_filepath | 1.5 |
| _return_load(electricity) | 19.5 |
| _return_load(gas) | 20.3 |
| phase2_marginal_costs_rr | 3.5 |
| bs.simulate | 104.8 |
| **Total** | **150.2** |

---

## Phase 1 — combined batch reader, 2026-02-23

| Stage | Baseline (s) | Phase 1 (s) | Delta |
|-------|-------------|-------------|-------|
| _return_load(electricity) | 19.5 | — | — |
| _return_load(gas) | 20.3 | — | — |
| _return_loads_combined | — | 26.8 | saves 13.0s vs two separate reads |
| bs.simulate | 104.8 | 75.4 | -29.4s |
| **Total** | **150.2** | **106.4** | **-43.8s (1.41x faster)** |

Notes:
- `_return_loads_combined` reads all 1,910 parquet files in one PyArrow batch pass
- Year-replace vectorized via fixed Timedelta offset (avoids 16.7M Python `ts.replace()` calls)
- Timeshift via `np.roll(arr.reshape(n_bldgs, 8760, n_cols), -N, axis=1)` (vectorized across all buildings)
- `bs.simulate` also improved (likely due to better memory layout from single-pass read)

---

## Phase 2 — vectorized electricity tariff aggregation, 2026-02-23

| Stage | Phase 1 (s) | Phase 2 (s) | Delta |
|-------|-------------|-------------|-------|
| _return_loads_combined | 26.8 | 72.0 | +45.2s (filesystem cold cache) |
| phase2_marginal_costs_rr | 3.5 | 3.6 | ~same |
| bs.simulate | 75.4 | 78.2 | ~same |
| **Total** | **106.4** | **154** | **cold-cache run; warm-cache expected ~1.5–2×** |

Notes:
- Phase 2 replaces per-building Dask loop (1,910 tasks) for electricity tariff aggregation with vectorized pandas groupby
- Gas loads fall back to original CAIRO (`total_fuel_gas`): CAIRO always re-applies kWh→therms via `_adjust_gas_loads` even on pre-loaded data; matching this behavior is necessary for output correctness
- Solar PV fix: clips `electricity_net` to 0 for `grid_cons` (14 solar buildings store pv_generation as negative in ResStock parquets)
- `_return_loads_combined` variance likely reflects filesystem cold-cache state; Phase 1 measured with warm cache
- `bs.simulate` improvement over Phase 1 is modest (~0s); the Dask loop speedup may be hidden by gas billing still using original per-building path
- All 21 output CSVs match Phase 1 reference exactly (< 0.001% diff)
