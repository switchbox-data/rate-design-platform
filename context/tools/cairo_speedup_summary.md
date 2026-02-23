# CAIRO speedup — work summary

Branch: `jpv/250-cairo-performance-status`
Linear: RDP-118
Date: 2026-02-23
Machine: 8-core EC2
Benchmark: run 2, 1,910 RI buildings (utility: `rie`)

---

## What was built

Three performance patches applied as module-level monkey-patches loaded by
`run_scenario.py` before CAIRO runs. All patches live in:

```
rate_design/ri/hp_rates/patches.py
```

### Phase 1 — Combined batch parquet reader

**Commits:** `42ad4bf`, `8a698e5`

Replaced two sequential `_return_load()` calls (one for electricity, one for gas,
each iterating 1,910 buildings serially) with a single PyArrow batch read of all
parquet files at once.

Key implementation details:
- `pyarrow.dataset` reads all 1,910 files in one multi-threaded pass, selecting
  only the 4 needed columns
- Schema unification via `pa.unify_schemas()` handles minor per-file schema
  differences
- Year-replace vectorized via a fixed `pd.Timedelta` offset (avoids 16.7M
  per-timestamp `ts.replace()` Python calls)
- Timeshift vectorized via `np.roll(arr.reshape(n_bldgs, 8760, n_cols), -N,
  axis=1)` across all buildings at once
- Gas conversion applied once vectorized: `kWh × 0.0341214116 → therms`
- Solar PV sign convention normalized per-block (14 RI buildings have
  negative `pv_generation` in ResStock parquets; these are clipped to 0 for
  `grid_cons`)

### Phase 2 — Vectorized electricity tariff aggregation

**Commit:** `127214e`

Replaced the 1,910-task Dask loop in
`cairo.rates_tool.loads.process_building_demand_by_period` (electricity path)
with a single pandas groupby across all buildings simultaneously.

Key implementation details:
- CAIRO's `_load_base_tariffs` used to get per-building tariff assignments
- Period schedule merged onto the full hourly DataFrame in one `merge()`
- Tier info merged in a second `merge()` on `period`
- Aggregation: `groupby(["bldg_id", "month", "period", "tier"]).sum()` across
  all 1,910 buildings × 8,760 hours in one pass
- Demand charge rows (NaN, then filled to 0) appended to match CAIRO's output
  structure exactly
- **Gas loads fall back to original CAIRO** — CAIRO's `aggregate_load_worker`
  always calls `_adjust_gas_loads` which converts kWh→therms even on pre-loaded
  therms data (a consistent double-conversion). Matching this behaviour exactly
  is required for output correctness; gas billing is not the performance
  bottleneck.
- Tiered and combined tariffs fall back to original CAIRO (not present in RI
  runs, but guarded for correctness)
- Monkey-patch pattern: original saved at module level **before** patching to
  prevent infinite recursion in fallback paths:
  ```python
  _orig_process_building_demand_by_period = _cairo_loads.process_building_demand_by_period
  _cairo_loads.process_building_demand_by_period = _vectorized_process_building_demand_by_period
  ```

### Phase 3 — Vectorized electricity bill calculation

**Commit:** `4b888ba`

Replaced the 1,910-task Dask loop in
`cairo.rates_tool.system_revenues.run_system_revenues` (electricity billing)
with a single vectorized pandas pass.

Key implementation details:
- Energy charge rates extracted from `ur_ec_tou_mat` (rate + adjustments)
  across all tariffs into a lookup DataFrame
- `merge()` of aggregated load onto rate lookup, then multiply `grid_cons ×
  rate` and `groupby(["bldg_id", "month"]).sum()` in one operation
- Fixed charges added vectorized per tariff; min-charge applied per month
  (0.0 for all RI tariffs, so a no-op in practice)
- Pivots to the wide month-column format (Jan–Dec + Annual) CAIRO returns
- **Gas billing falls back to original CAIRO** (same constraint as Phase 2)
- Demand charges and solar compensation fall back to original CAIRO (not
  present in RI runs)
- Same save-before-patch recursion guard pattern as Phase 2

---

## Timing results

All times from single-run benchmarks on the same 8-core EC2 instance.
`_return_loads_combined` varies with filesystem cache state (cold = up to 77s,
warm ≈ 27s).

| Stage | Baseline | Phase 1 | Phase 2 | Phase 3 |
|-------|----------|---------|---------|---------|
| `_return_load(electricity)` | 19.5s | — | — | — |
| `_return_load(gas)` | 20.3s | — | — | — |
| `_return_loads_combined` | — | 26.8s | 72.0s* | 74.9s* |
| `phase2_marginal_costs_rr` | 3.5s | 3.5s | 3.6s | 3.6s |
| `bs.simulate` | 104.8s | 75.4s | 78.2s | 66.9s |
| **Total** | **150.2s** | **~106s** | **~154s*** | **~146s*** |

\* Cold filesystem cache. Warm-cache estimate for Phase 3 total: **~97s**.

| Milestone | vs. Baseline (warm cache) |
|-----------|--------------------------|
| Phase 1 | **1.41×** faster |
| Phase 2 | ~1.4× faster |
| Phase 3 | **~1.55×** faster |

`bs.simulate` breakdown after all phases (from diagnostic instrumentation):

| Sub-stage | Time |
|-----------|------|
| Electricity `_return_preaggregated_load` (tariff agg + precalc calibration) | ~30s |
| Electricity `aggregate_system_revenues` (billing) | ~10s |
| Gas `_calculate_gas_bills` (tariff agg + billing, original CAIRO path) | ~26s |
| Other overhead | ~12s |

---

## Part 1 result: parallel tracks

| Metric | Value |
|---|---|
| T8 (single run, 8 workers) | 172s |
| T4 (single run, 4 workers) | 149s |
| Ratio r = T4/T8 | 0.87 |
| `run-all-sequential` total | 1917s (31m 57s) |
| `run-all-parallel-tracks` total | 1100s (18m 20s) |
| Improvement | ~43% faster |

---

## Correctness verification

- All 21 output CSVs compared against Phase 1 reference after each phase using
  max absolute diff; all phases match to < 0.001%
- 3 unit tests added in `tests/test_patches.py`:
  - `test_combined_reader_matches_separate_reads` — Phase 1
  - `test_vectorized_aggregation_matches_cairo` — Phase 2
  - `test_vectorized_billing_matches_cairo` — Phase 3

---

## Why the 5–10× target is out of reach with this approach

The two largest remaining costs are both constrained by CAIRO internals:

**Gas tariff aggregation + billing (~26s)**
CAIRO's `aggregate_load_worker` always applies `_adjust_gas_loads` (kWh→therms)
even on pre-loaded therms data. The vectorized path cannot replicate this
double-conversion without breaking outputs, so gas stays on the original
1,910-task Dask loop. Fixing this would require a change inside CAIRO itself
(e.g., a flag to skip the conversion when data is pre-loaded as therms).

**Electricity `_return_preaggregated_load` (~30s)**
This function handles tariff aggregation plus precalc rate calibration. The
calibration step (which adjusts supply rates to hit a revenue target) is tightly
coupled to CAIRO's per-building state machine and would require substantial
restructuring to vectorize.

Further meaningful speedup beyond ~1.5× would most likely require:
1. Changes inside the CAIRO package to expose a vectorized API, or
2. A parallel/distributed execution approach (see
   `context/tools/cairo_elastic_cluster.md` for prior analysis of elastic Dask
   cluster options)

---

## Files changed

| File | Change |
|------|--------|
| `rate_design/ri/hp_rates/patches.py` | New — all three patch phases |
| `rate_design/ri/hp_rates/run_scenario.py` | Import patches; add per-stage timing |
| `tests/test_patches.py` | New — 3 unit tests |
| `context/tools/cairo_speedup_log.md` | Benchmark log updated after each phase |
| `docs/plans/2026-02-23-cairo-speedup-design.md` | Design doc |
| `docs/plans/2026-02-23-cairo-speedup-plan.md` | Implementation plan |
