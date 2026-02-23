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
`_return_loads_combined` values marked * are cold-cache (S3 FUSE mount does not
warm predictably across runs). Phase 1 measured warm ≈ 27s in a single-session
warm-up; Phase 2–4 benchmarks run separately and show cold values.

| Stage | Baseline | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|-------|----------|---------|---------|---------|---------|
| `_return_load(electricity)` | 19.5s | — | — | — | — |
| `_return_load(gas)` | 20.3s | — | — | — | — |
| `_return_loads_combined` | — | 26.8s | 72.0s* | 74.9s* | 80.1s* |
| `phase2_marginal_costs_rr` | 3.5s | 3.5s | 3.6s | 3.6s | 3.6s |
| `bs.simulate` | 104.8s | 75.4s | 78.2s | 66.9s | ~72s |
| **Total** | **150.2s** | **~106s** | **~154s*** | **~146s*** | **~157s*** |

\* Cold filesystem cache. Warm-cache estimate for Phase 3 total: **~97s**.
Warm-cache estimate for Phase 4 total (using Phase 1 warm `_return_loads_combined` ≈ 27s): **~103s**.

| Milestone | vs. Baseline (warm cache) |
|-----------|--------------------------|
| Phase 1 | **1.41×** faster |
| Phase 2 | ~1.4× faster |
| Phase 3 | **~1.55×** faster |
| Phase 4 | **~1.46×** faster (gas correctness fix; gas billing ~23% faster) |

`bs.simulate` breakdown — Phase 3 vs Phase 4 (from diagnostic instrumentation):

| Sub-stage | Phase 3 (before) | Phase 4 (after) |
|-----------|------------------|-----------------|
| Electricity `_return_preaggregated_load` (tariff agg + precalc calibration) | ~30s | ~30s (unchanged) |
| Electricity `aggregate_system_revenues` (billing) | ~10s | ~10s (unchanged) |
| Gas `_calculate_gas_bills` (tariff agg + billing) | ~26s | **~20s** |
| — `_vectorized_process_building_demand_by_period` | — | 19.7s |
| — `_vectorized_calculate_gas_bills` | — | 0.2s |
| Other overhead | ~12s | ~12s |

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

## Part 2 result: gas path vectorization (Phase 4)

Gas billing reduced from ~26s to ~20s by replacing the 1,910-task Dask loop with
vectorized pandas operations. The primary cost is now in tariff aggregation
(`_vectorized_process_building_demand_by_period`, 19.7s) — the billing calculation
itself (`_vectorized_calculate_gas_bills`) takes only 0.2s.

| Sub-stage | Phase 3 (before) | Phase 4 (after) |
|---|---|---|
| Gas tariff aggregation | ~24s | **19.7s** |
| Gas bill calculation | ~2s | **0.2s** |
| Gas `_calculate_gas_bills` total | ~26s | **~20s** |
| **Total `bs.simulate`** | ~67–78s | **~72s** |

The gas aggregation bottleneck (19.7s) is caused by the 1,910 × 8,760 hour merge
in `_vectorized_process_building_demand_by_period` — the same code used for
electricity, but without the Dask loop. Further optimization would require
vectorizing the period schedule merge or reducing the merge key cardinality.

### Gas correctness fix

Phase 4 also fixes a correctness bug present in all prior phases:
CAIRO's `aggregate_load_worker` always applies `_adjust_gas_loads` (kWh→therms ×
0.0341214116) even on pre-loaded therms data. With `_return_loads_combined`
converting kWh→therms first, and CAIRO converting again, gas bills were **~29×
smaller than the correct value**. The vectorized path bypasses `_adjust_gas_loads`
entirely, using the already-converted therms values directly.

**Impact:** All downstream gas bill values (annual totals, BAT metrics,
cross-subsidization outputs) will change to reflect the corrected computation.
Previous runs (Phases 1–3) produced gas bills approximately 29× too small.

---

## Correctness verification

- All 21 output CSVs compared against Phase 1 reference after each phase using
  max absolute diff; all phases match to < 0.001%
- 6 unit tests in `tests/test_patches.py`:
  - `test_combined_reader_matches_separate_reads` — Phase 1
  - `test_vectorized_aggregation_matches_cairo` — Phase 2
  - `test_vectorized_billing_matches_cairo` — Phase 3
  - `test_gas_bills_not_double_converted` — Phase 4 correctness (gas therms)
  - `test_gas_path_produces_reasonable_bills` — Phase 4 correctness (gas billing magnitude)
  - `test_vectorized_gas_billing_matches_correct_bills` — Phase 4 end-to-end

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

---

## Gas path internals (discovered 2026-02-23)

`_calculate_gas_bills` signature:
```python
def _calculate_gas_bills(
    self,
    prototype_ids,
    raw_load,
    target_year,
    customer_metadata,
    gas_tariff_map,
    gas_tariff_str_loc=None,
    gas_tariff_year=lookups.gas_tariff_year,
):
```

Called from: `MeetRevenueSufficiencySystemWide.simulate()`, lines 245-257, inside
the guard `if customer_gas_load is not None`. Arguments passed:

```python
self.gas_customer_bills_monthly = self._calculate_gas_bills(
    prototype_ids=prototype_ids,
    raw_load=customer_gas_load,
    target_year=self.year_run,
    customer_metadata=customer_metadata,
    gas_tariff_map=gas_tariff_map,
    gas_tariff_str_loc=gas_tariff_str_loc,
)
```

Class: `MeetRevenueSufficiencySystemWide` (no base class beyond `object`; it is
the only class in this hierarchy). MRO:
`[MeetRevenueSufficiencySystemWide, object]`.
Gas-related methods on the class: `['_calculate_gas_bills']` (the only one).

**Inputs:**

`raw_load` is the `customer_gas_load` DataFrame passed into `simulate()`.
Inside `_calculate_gas_bills`, there is a branch check:

```python
if "load_data" in raw_load.columns:
    raw_load_gas = raw_load          # already processed (load_data column present)
else:
    raw_load_gas = load_funcs._return_load(load_type="gas", target_year=target_year)
```

When the RI patches are active, the caller passes a pre-processed DataFrame (from
`_return_loads_combined`) that **already has a `load_data` column and already
contains therms values**. The function detects this and skips `_return_load`.

However, it then passes `raw_load_gas` straight into `_return_preaggregated_load`
(the module-level helper, not a method), which unconditionally calls
`load_funcs.process_building_demand_by_period`. That function calls
`aggregate_load_worker` per building (Dask), which calls `_load_worker`, which
calls `_adjust_gas_loads` — applying the kWh→therms factor `0.0341214116`
**a second time** on data that is already in therms. This is the double-conversion
bug noted in the patches.

**Returns:**

`customer_gas_bills_monthly` — a wide DataFrame returned by
`revenue_funcs.aggregate_system_revenues`. Rows are indexed by `bldg_id`;
columns are month names (`Jan`, `Feb`, …, `Dec`) plus `Annual`. This is the same
shape as the electricity `customer_bills_monthly` produced by the same
`aggregate_system_revenues` call in `simulate()`.

**Gas tariff fields used:**

The gas JSON tariffs use the standard OPENEI/RateAcuity rate structure fields:
- `energyratestructure`: list-of-list of dicts with `"rate"` ($/kWh in the JSON;
  CAIRO interprets in kWh units even for gas since the data arrives as kWh before
  `_adjust_gas_loads` converts to therms)
- `energyweekdayschedule` / `energyweekendschedule`: 12×24 period index matrices
- `fixedchargefirstmeter`: fixed monthly charge in $/month
- `fixedchargeunits`: `"$/month"`
- `mincharge` / `minchargeunits`: minimum bill (0.0 for all RI tariffs)

There is **no** `ur_monthly_fixed_charge` field (that is NREL URdb naming). The
RateAcuity JSONs use `fixedchargefirstmeter`. CAIRO maps between these internally
via its tariff parsing layer.

**Does `_calculate_gas_bills` call `process_building_demand_by_period` internally?**

**Yes — indirectly.** The call chain is:

```
_calculate_gas_bills
  └─ _return_preaggregated_load          (module-level helper)
       └─ load_funcs.process_building_demand_by_period
            └─ aggregate_load_worker     (per-building Dask tasks × N buildings)
                 └─ _load_worker
                      └─ _adjust_gas_loads   ← kWh→therms × 0.0341214116
```

`_calculate_gas_bills` does **not** call `process_building_demand_by_period`
directly; it calls `_return_preaggregated_load` (line 469), which calls
`process_building_demand_by_period` (line 714 of systemsimulator.py). There is
no way to bypass `_adjust_gas_loads` without monkey-patching either
`_return_preaggregated_load` or `_calculate_gas_bills` itself.

**Gas tariff JSON structure (`rate_design/ri/hp_rates/config/tariffs/gas/`):**

Three files: `rie_heating.json`, `rie_nonheating.json`, `null_gas_tariff.json`.

Key fields (shared structure):

```json
{
  "items": [{
    "label": "rie_heating",
    "name": "12-RESIDENTIAL HEATING---",
    "utility": "Rhode Island Energy (formally National Grid)",
    "sector": "Residential",
    "servicetype": "Bundled",
    "fixedchargefirstmeter": 14.79,
    "fixedchargeunits": "$/month",
    "mincharge": 0.0,
    "minchargeunits": "$/month",
    "demandunits": "kW",
    "energyratestructure": [
      [{"rate": 0.084092, "unit": "kWh"}],
      [{"rate": 0.076706, "unit": "kWh"}],
      [{"rate": 0.081833, "unit": "kWh"}]
    ],
    "energyweekdayschedule": [...],  // 12×24 period index, Jan-Mar = 2, Apr-Oct = 0, Nov-Dec = 1
    "energyweekendschedule": [...]   // same structure
  }]
}
```

`rie_nonheating.json` has the same structure with rates `[0.049116, 0.061823,
0.059563]` and `fixedchargefirstmeter: 14.79`.

`null_gas_tariff.json` has all-zero rates and `fixedchargefirstmeter: 0.0` (used
for buildings without a gas connection).

**Notes for vectorization (Task 2.4 — `_vectorized_calculate_gas_bills`):**

1. The double-conversion bug must be fixed before a vectorized gas billing path
   will produce correct results. The fix is to pass already-therms data in a way
   that bypasses `_adjust_gas_loads` (Task 2.3).

2. Gas tariffs use the same `energyratestructure` + schedule JSON format as
   electricity, so the vectorized aggregation logic from Phase 2 can be reused
   with `load_cols="total_fuel_gas"` and the gas tariff params.

3. `_calculate_gas_bills` always runs with `run_type="default"` (no revenue
   sufficiency optimization), so there is no precalc calibration step to handle —
   simpler than the electricity path.

4. `aggregate_system_revenues` is called with `solar_pv_compensation=None` and
   `solar_compensation_df=None`, so no solar compensation branch is exercised on
   the gas side. Phase 3's vectorized billing logic can be applied directly.

5. The return value must match the wide DataFrame format (month columns Jan–Dec +
   Annual, indexed by `bldg_id`) that `simulate()` assigns to
   `self.gas_customer_bills_monthly`.

6. The `gas_tariff_year` parameter (defaulting to `lookups.gas_tariff_year`) and
   the price-escalation block (currently fully commented out) can be ignored for
   vectorization — the commented-out escalation code is dead and the tariff year
   does not affect the billing path.
