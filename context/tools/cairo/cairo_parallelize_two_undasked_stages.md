# Parallelize the two non-Dask stages in CAIRO

Handoff for an agent implementing parallelization of the two compute-intensive stages that currently run single-threaded: **process_residential_hourly_demand** and **cross-subsidization (BAT)**. Use this as a starting point; do your own analysis of the CAIRO codebase and call graphs before implementing.

## Full picture: which stages use Dask today

CAIRO’s heavy work falls into two groups.

### Four stages that already use Dask (but run single-core today)

These four stages **do** use `dask.delayed` and `dask.compute()` in the CAIRO codebase. They wrap per-building worker functions and call `dask.compute()` on the list of delayed tasks. So the _structure_ for parallelism is there.

| Stage                       | Module               | What it does                                                                                            | Why it’s effectively single-core today                                                                                                                                                            |
| --------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Load reading**            | `loads.py`           | `_return_load` → `process_hourly_demand_by_bldg`: read parquet + timestamp ops per building             | No scheduler is configured; Dask uses the **threaded** scheduler by default. Python’s GIL means only one thread runs Python bytecode at a time. So these run single-core (I/O can overlap a bit). |
| **Demand by period**        | `loads.py`           | `process_building_demand_by_period`: tariff period aggregation per building via `aggregate_load_worker` | Same: `dask.compute()` with no `scheduler=` → threaded scheduler → GIL-bound, single-core.                                                                                                        |
| **System revenues (bills)** | `system_revenues.py` | `run_system_revenues`: bill calculation per building via `return_monthly_bills_year1`                   | Same: threaded default, single-core.                                                                                                                                                              |
| **Precalc bills**           | `system_revenues.py` | `_precalc_customer_rates`: unity-rate precalc per building via `run_aggregator_precalculation`          | Same: threaded default, single-core.                                                                                                                                                              |

**Fix for these four (no CAIRO code change):** The **caller** (e.g. rate-design-platform’s `run_scenario.py`) can set `dask.config.set(scheduler="processes", num_workers=settings.process_workers)` before calling into CAIRO. Then every existing `dask.compute()` inside CAIRO uses a process pool and gets real multi-core parallelism. CAIRO’s `process_workers` is currently stored but never passed to Dask; the caller’s config applies to all of CAIRO’s Dask calls.

### Two stages that do not use Dask (this document)

These two are implemented as **single-pass pandas** operations on one big DataFrame (all buildings × 8760 or similar). There is no per-building loop and no `dask.delayed` / `dask.compute`. So they always run on one thread and are unaffected by the scheduler fix above. To parallelize them, Dask (or equivalent) must be **added** inside CAIRO, as described below.

| Stage                                 | Module              | What it does                                                                                                                        |
| ------------------------------------- | ------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| **process_residential_hourly_demand** | `loads.py`          | Aggregate building-level hourly loads to system-wide hourly net electricity demand (one merge + groupby on full N×8760).            |
| **Cross-subsidization (BAT)**         | `postprocessing.py` | Bill Alignment Test: customer-level economic burden, residual cost allocation, BAT values (N×8760 merge + groupby and related ops). |

---

## Background and source of truth

- The overall performance picture (which stages use Dask, why they’re single-core, etc.) is in **context/tools/cairo_performance_analysis.md** in the rate-design-platform repo.
- CAIRO is a **separate package** (dependency of rate-design-platform). Its code lives in the environment at `site-packages/cairo/rates_tool/` (e.g. under `rate-design-platform/.venv/...` or wherever the agent’s CAIRO clone/vendor lives). The agent will work in the **CAIRO repo/codebase**, not in rate-design-platform.
- This doc summarizes what we learned in rate-design-platform about _what_ to change and _how_; the agent should re-verify paths, line numbers, and call sites inside CAIRO.

## The two stages to parallelize (recap)

| Stage                                 | Module                         | Purpose                                                                                                   | Current parallelism                                                                                      |
| ------------------------------------- | ------------------------------ | --------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| **process_residential_hourly_demand** | `rates_tool/loads.py`          | Aggregate building-level hourly loads to system-wide hourly net electricity demand (weighted sum by time) | Single-core: one pandas merge + groupby on full N×8760 DataFrame                                         |
| **Cross-subsidization (BAT)**         | `rates_tool/postprocessing.py` | Bill Alignment Test: customer-level economic burden, residual cost allocation, BAT values                 | Single-core: N×8760 merge + groupby and related ops in `_return_cross_subsidization_metrics` and helpers |

Both are “one big DataFrame” operations (no per-building Dask tasks today), so they don’t benefit from the Dask process-scheduler fix. Parallelizing them requires **adding** Dask (or equivalent) around these stages inside CAIRO.

## Chosen approach: chunk by building + dask.delayed (Option A)

- **Idea:** Split the main DataFrame(s) by `bldg_id` into K chunks. Run the **existing** pandas logic on each chunk in a `dask.delayed` task. After `dask.compute()`, combine results (sum or concat). No rewrite of the core math; only split/combine and scheduling.
- **Why this over Dask DataFrame (Option B):** Easier to implement (reuse existing functions), easier to keep correct (same code path, compare to current results), and for this codebase likely as fast or faster (no Dask DataFrame rewrite or partition/shuffle overhead).
- **Scheduler:** Ensure the process scheduler is used for these new `dask.compute()` calls (either CAIRO sets `dask.config.set(scheduler="processes", num_workers=...)` at entry, or callers like rate-design-platform do so before calling CAIRO). The agent should not assume a global config; document or add a config/scheduler hook if needed.

## Stage 1: process_residential_hourly_demand

### Where it lives and what it does

- **Function:** `process_residential_hourly_demand(bldg_load, sample_weights)` in **loads.py** (around line 522 in the version we inspected).
- **Inputs:**
  - `bldg_load`: pandas DataFrame with MultiIndex `['bldg_id', 'time']`, 8760 rows per building, must include `electricity_net` (and possibly other cols).
  - `sample_weights`: DataFrame with `bldg_id` and `weight`.
- **Current logic (simplified):**\
  `bldg_load.copy().reset_index().merge(sample_weights, on="bldg_id")` → multiply `electricity_net` by `weight` → `groupby(["time"])["electricity_net"].sum()` → return a single Series (8760 values).
- **Output:** One pandas Series: system-wide hourly net electricity demand (DatetimeIndex / time index).

### How to parallelize (chunk + delayed + combine)

1. **Split:** Split `bldg_load` by `bldg_id` into K chunks (e.g. K = number of workers, or a fixed chunk size). Each chunk is a subset of rows with the same MultiIndex structure. Keep `sample_weights` as-is (or pass it whole; each worker will merge with the chunk’s `bldg_id`s only).
2. **Worker:** Write a small wrapper that, for one chunk `bldg_chunk`, runs the **same** logic as the current function body: e.g. `bldg_chunk.reset_index().merge(sample_weights, on="bldg_id")`, then `["electricity_net"] * weight`, then `groupby("time")["electricity_net"].sum()`. Returns one Series (indexed by time) per chunk.
3. **Dask:** Build a list of `dask.delayed(wrapper)(chunk, sample_weights)` for each chunk. Call `dask.compute(list_of_delayed)` (with the same scheduler/num_workers as the rest of CAIRO).
4. **Combine:** Sum the K Series element-wise (same time index). Result is identical to the current single-threaded result (associative sum).

### Implementation notes for the agent

- Re-read the exact implementation in **loads.py** (copy, reset_index, merge, column ops, groupby). The wrapper must mirror it exactly so results are bitwise identical when combined.
- Chunking: ensure every `bldg_id` appears in exactly one chunk. Use e.g. `bldg_load.index.get_level_values("bldg_id").unique()` then split that list into K groups, then `bldg_load.loc[idx_bldg_ids]` per group.
- `sample_weights` can be passed to every delayed task (it’s small). Alternatively, subset `sample_weights` per chunk to only the `bldg_id`s in that chunk to reduce serialization; either way, the merge must be on the chunk’s buildings only.
- Number of chunks K: can tie to `process_workers` (or a new config) so it matches the Dask process pool size. Don’t create many more tasks than workers.
- Add a test or script that compares the output of the new parallel path to the current serial path (same inputs) and asserts equality (or near-equality if float order differs).

## Stage 2: Cross-subsidization (BAT) — _return_cross_subsidization_metrics and helpers

### Where it lives and what it does

- **Entry:** Method `_return_cross_subsidization_metrics(...)` on the postprocessing class in **postprocessing.py** (around line 624). It’s called from somewhere in the same class during the simulation/postprocessing flow.
- **Heavy work** is delegated to:
  - `_return_customer_level_economic_burden_and_residual_share(building_metadata, raw_hourly_load, marginal_system_prices, costs_by_type)` (around line 785), which in turn calls:
    - `_determine_marginal_cost_allocation(raw_hourly_load, marginal_system_prices)` → annual customer economic burden;
    - `_determine_residual_cost_allocation(building_metadata, raw_hourly_load, marginal_system_prices, costs_by_type)` → residual share (volumetric, peak, per-customer), which calls:
      - `_allocate_residual_volumetric`, `_allocate_residual_peak`, `_allocate_residual_percustomer`, then merges results.
- **Inputs:** Building metadata, **raw_hourly_load** (N buildings × 8760 hours), marginal_system_prices, costs_by_type, and later customer_bills. The N×8760 merge and groupby work is in these helpers.
- **Output:** A BAT DataFrame (per-building BAT values and related columns).

### How to parallelize (chunk + delayed + combine)

1. **Split:** Split `raw_hourly_load` by `bldg_id` into K chunks. Split `building_metadata` so each chunk of metadata corresponds to the buildings in that load chunk (same bldg_ids). Other inputs (`marginal_system_prices`, `costs_by_type`) are typically not building-indexed in the same way—the agent must inspect whether they can be passed whole or need to be subset per chunk (e.g. marginal_system_prices is likely time-indexed and shared).
2. **Worker:** A wrapper that, for one chunk, calls the **same** internal logic that produces “economic burden + residual share” for that subset of buildings. That may mean:
   - Calling a refactored version of `_return_customer_level_economic_burden_and_residual_share` (or the underlying allocation helpers) that operates on a subset of `raw_hourly_load` and the corresponding `building_metadata` subset; or
   - Extracting a pure function that takes (building_metadata_chunk, raw_hourly_load_chunk, marginal_system_prices, costs_by_type) and returns the per-building results for that chunk (e.g. two DataFrames or one combined structure).
3. **Dask:** `dask.delayed(wrapper)(metadata_chunk, load_chunk, marginal_system_prices, costs_by_type)` for each chunk; then `dask.compute(list_of_delayed)`.
4. **Combine:** Concatenate the per-building results (e.g. `pd.concat(..., axis=0)`) so the index is bldg_id and columns match the current BAT output. Then the rest of `_return_cross_subsidization_metrics` (merging with customer_bills, computing BAT_vol, BAT_peak, BAT_percustomer, QA checks) can run on the combined DataFrame as it does today—no need to parallelize that part unless profiling shows otherwise.

### Implementation notes for the agent

- **Call graph:** Trace who calls `_return_cross_subsidization_metrics` and what happens with its return value. The parallel implementation should be a drop-in replacement (same inputs, same output shape and semantics).
- **Refactor for chunking:** The current code may assume “one big raw_hourly_load”. The agent may need to:
  - Extract a function that takes (building_metadata, raw_hourly_load, marginal_system_prices, costs_by_type) and returns (economic_burden, residual_share) for exactly those buildings; then call it from both the existing single-big-DataFrame path and the new chunked path; or
  - Implement the chunked path by splitting and calling the same helpers per chunk, then concatenating. Careful: some allocations (e.g. per-customer residual) use global totals (e.g. `building_metadata["weight"].sum()`). Those may need to be computed once and passed in, or the chunked version may need to do a two-pass or different formula—the agent must read the math and preserve correctness.
- **Peak / volumetric / per-customer:** Ensure that any global totals (e.g. system-wide weight sum, residual cost totals) are computed once and shared across chunks, not recomputed per chunk with wrong denominators.
- **Testing:** Compare the full BAT output (and if possible intermediate economic_burden / residual_share) of the new parallel path vs the current serial path on the same inputs; assert frame equality or close numerical match.

## Configuration and scheduler

- **process_workers:** CAIRO already has a `process_workers` (or similar) setting from scenario YAML; it’s stored but was not previously passed to Dask. When adding new `dask.compute()` calls, use the same worker count (e.g. from config or `self.process_workers`) so the new parallel stages use the same process pool size as the existing Dask stages (load, demand-by-period, bills, precalc).
- **Scheduler:** If CAIRO does not set `dask.config.set(scheduler="processes", num_workers=...)` at simulation entry, then callers (e.g. rate-design-platform) do. The new code should not assume threads; it should behave correctly under the process scheduler (picklable arguments, no shared mutable state across tasks).

## Order of work and testing

1. **Stage 1 first:** Implement and test parallel `process_residential_hourly_demand`; validate against current implementation. Then do Stage 2.
2. **Tests:** Add or extend tests that (a) run both serial and parallel paths on the same fixture, (b) compare outputs (Series for stage 1, BAT DataFrame for stage 2). Prefer small but realistic inputs (e.g. 2–3 buildings, 8760 hours) so tests stay fast.
3. **Backward compatibility:** Keep the existing function signatures and return types. Optionally support a “force serial” or “num_chunks=1” for debugging or environments where multiprocessing is problematic.

## What the agent should do on their own

- Re-open **loads.py** and **postprocessing.py** in the actual CAIRO codebase and confirm line numbers, method names, and call sites.
- Trace the full call graph for `process_residential_hourly_demand` and for `_return_cross_subsidization_metrics` (who calls them, with what arguments, and what uses the return value).
- Inspect `_determine_marginal_cost_allocation`, `_determine_residual_cost_allocation`, and the allocate_* helpers to see exactly what they need (e.g. global sums) and how to split/combine without changing results.
- Decide chunk size and K (e.g. from `process_workers` or a new setting) and document the choice.
- Run the existing test suite after changes; add the comparison tests above.
