# CAIRO speedup — design doc

**Date:** 2026-02-23
**Issue:** [#250](https://github.com/switchbox-data/rate-design-platform/issues/250)
**Target:** 5–10× speedup on `just run 1` (and all 12 RI runs) via monkey-patches in the platform layer, without modifying the CAIRO package.

## Context

- 1,910 buildings for RIE utility
- 8-core EC2 instance (upgradeable later)
- Baseline: ~2.5 min/run × 12 runs = ~30 min total
- Tier 0 (Dask process scheduler, `num_workers=8`) already implemented in `run_scenario.py:613`
- Correctness requirement: numerical equivalence within floating-point rounding tolerance (not bit-for-bit)

## What's not on the table

- Modifying the CAIRO package directly (patches only; changes migrate to CAIRO later)
- Changing output schema or column names
- Breaking any of runs 1–12

## File structure

```
rate_design/ri/hp_rates/
  run_scenario.py        # entry point — import patches, add timing
  patches.py             # NEW: all monkey-patches live here
context/tools/
  cairo_speedup_log.md   # NEW: benchmark log updated after each phase
```

## Phase 0 — Baseline instrumentation

Add `time.perf_counter()` timing around each major call in `run_scenario.py::run()`:

- `_return_load` (electricity)
- `_return_load` (gas)
- `_return_revenue_requirement_target`
- `bs.simulate` (whole CAIRO sim)
- Within simulate, if accessible: `process_building_demand_by_period`, `run_system_revenues`, `_precalc_customer_rates`

Log stage timings to `context/tools/cairo_speedup_log.md` after the first timed run.

## Phase 1 — File I/O: combined batch reader (Tier 2a + 2b)

**Problem:** `_return_load("electricity")` and `_return_load("gas")` each read all 1,910 parquet files independently. The same files contain both fuel-type columns. Net cost: 3,820 parquet reads.

**Fix:** Replace the two `_return_load` calls in `run_scenario.py` with a new function `_return_loads_combined()` in `patches.py` that:

1. Uses `pyarrow.dataset.dataset(list_of_paths, format="parquet")` to open all files in one call
2. Reads the union of electricity + gas columns in one multi-threaded I/O pass (`dataset.to_table(columns=[...])`)
3. Splits the resulting table into per-building per-fuel DataFrames
4. Applies timeshift + TZ localization once per building (vectorized, not per-file)
5. Returns `(raw_load_elec, raw_load_gas)` with identical structure to what the two `_return_load` calls currently return

CAIRO internals are not touched. The two variables passed downstream are identical in structure.

**Expected speedup:** ~1.5–2× on Phase 1 (halved reads + batch I/O vs 1,910 separate `pd.read_parquet` calls).

## Phase 2 — Vectorized tariff aggregation (Tier 2c)

**Problem:** `process_building_demand_by_period` loops over 1,910 buildings, creates a `dask.delayed(aggregate_load_worker)` per building, and calls `dask.compute`. Each worker runs `_add_datetime_indicators` (copy + datetime extraction), period merge, groupby. With the process scheduler this is ~8× vs single-core but still has per-task serialization overhead and 1,910 small DataFrames.

**Fix:** Monkey-patch `cairo.rates_tool.loads.process_building_demand_by_period` with a vectorized version that:

1. Takes the full N×8760 DataFrame (already in memory as `raw_load_elec`)
2. Extracts `month` (int8), `hour` (int8), `is_weekday` (bool) once across all rows
3. Merges period schedule once across all buildings (`bldg_id` → `tariff_key` → period schedule)
4. Groups by `[bldg_id, month, period, tier]` in one `groupby().sum()`
5. Uses `float32` for load columns to halve memory bandwidth
6. Returns identical `(agg_load, agg_solar)` structure

Scope: flat/TOU path (covers all 12 RI runs). Tiered/combined path is a stretch goal.

**Expected speedup:** 3–10× on this stage (eliminates 1,910 Dask task round-trips + serialization + per-task Python overhead).

## Phase 3 — Vectorized bill calculation (Tier 2d)

**Problem:** `run_system_revenues` loops over 1,910 buildings, each a `dask.delayed(return_monthly_bills_year1)` call. Same serialization overhead pattern as Phase 2.

**Fix:** Monkey-patch `cairo.rates_tool.system_revenues.run_system_revenues` with a vectorized version that:

1. Merges aggregated load with tariff rate matrices across all buildings in one pass
2. Computes `energy_cost = grid_cons × rate` vectorially
3. Adds fixed charges and demand charges
4. Returns identical `comp_df` (index=bldg_id, columns=Jan–Dec + Annual)

**Expected speedup:** 5–15× on this stage.

## Testing strategy

After each phase:

1. Run the patched version on `just run 1` (or `run 2` as a clean check)
2. Compare all output CSVs/parquets against the pre-patch baseline using `pandas.testing.assert_frame_equal(check_exact=False, rtol=1e-4)`
3. Record stage timings in `context/tools/cairo_speedup_log.md`
4. Only proceed to the next phase once the current phase passes

## Expected cumulative speedup

| After phase | Speedup estimate |
| ----------- | ---------------- |
| Phase 0     | 0× (baseline)    |
| Phase 1     | ~1.5–2×          |
| Phase 2     | ~3–6×            |
| Phase 3     | ~5–10×           |

## Migration path

Once patches are validated across all 12 runs, open a PR to the CAIRO repo with the equivalent changes to the package source. The monkey-patches in `patches.py` can then be removed from the platform layer.
