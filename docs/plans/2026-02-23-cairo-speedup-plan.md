# CAIRO Speedup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` to execute this plan.
> Dispatch one subagent per phase (0, 1, 2, 3). Review between phases. Each subagent gets its phase tasks only plus the Orientation section. Do NOT skip the review gate between phases — output correctness must be verified before proceeding.

## How to start (after context clear)

1. Open a fresh session in this repo
2. Say: **"Execute the CAIRO speedup plan at `docs/plans/2026-02-23-cairo-speedup-plan.md`, subagent-driven, starting with Phase 0"**
3. Claude will invoke `superpowers:subagent-driven-development` and dispatch Phase 0
4. After Phase 0 completes, review the timing log at `context/tools/cairo_speedup_log.md`, then say **"proceed with Phase 1"**
5. Repeat through Phase 3

## Review gates

| After phase | What to check before proceeding                                                                           |
| ----------- | --------------------------------------------------------------------------------------------------------- |
| Phase 0     | `context/tools/cairo_speedup_log.md` has baseline timings; `run_scenario.py` has TIMING log lines         |
| Phase 1     | Output CSVs for run 2 match pre-patch baseline; `_return_loads_combined` timing < combined original reads |
| Phase 2     | Output CSVs for run 2 match; `bs.simulate` timing improved; all 3 unit tests pass                         |
| Phase 3     | Output CSVs for runs 1 and 2 match; total time ≥ 3× faster than baseline                                  |

---

**Goal:** Achieve 5–10× speedup on `just run-scenario N` for RI heat-pump rates via monkey-patches in the platform layer, without modifying the CAIRO package.

**Architecture:** Four phases — (0) baseline instrumentation, (1) combined batch file reader replacing the double-read of 1,910 parquets, (2) vectorized tariff aggregation replacing 1,910-task Dask loop, (3) vectorized bill calculation replacing 1,910-task Dask loop. All patches live in `rate_design/ri/hp_rates/patches.py` and are imported at startup in `run_scenario.py`. After each phase, outputs for all 12 runs must match within numerical tolerance.

**Tech Stack:** Python 3.13, pandas, numpy, pyarrow.dataset, cairo (installed package), pytest, uv

---

## Orientation

Key files:

- `rate_design/ri/hp_rates/run_scenario.py` — entry point; `run()` function is what we time
- `rate_design/ri/hp_rates/patches.py` — NEW: all monkey-patches live here
- `context/tools/cairo_speedup_log.md` — NEW: benchmark log
- `tests/test_patches.py` — NEW: unit/integration tests for patches
- `.venv/lib/python3.13/site-packages/cairo/rates_tool/loads.py` — CAIRO source (read-only reference)
- `.venv/lib/python3.13/site-packages/cairo/rates_tool/system_revenues.py` — CAIRO source (read-only reference)

Key facts:

- 1,910 buildings for RIE utility
- 8 cores; `dask.config.set(scheduler="processes", num_workers=8)` already set at `run_scenario.py:613`
- Parquet files at `/data.sb/nrel/resstock/res_2024_amy2018_2/load_curve_hourly/state=RI/upgrade=00/`
- Each parquet contains columns including `bldg_id`, `timestamp`, `out.electricity.total.energy_consumption`, `out.electricity.pv.energy_consumption`, `out.natural_gas.total.energy_consumption`
- All RI buildings use AMY2018 data (2018 source year); target year is 2025; timeshift offset = 48 hours (constant for all buildings)
- Output lives at `/data.sb/switchbox/cairo/outputs/hp_rates/ri/` in a timestamped subdirectory

Run command: `cd rate_design/ri/hp_rates && uv run python run_scenario.py --run-num N --utility rie`
Or via just: `just -f rate_design/ri/hp_rates/Justfile run-scenario N`

---

## Phase 0 — Baseline instrumentation

### Task 0.1: Create the speedup log file

**Files:**

- Create: `context/tools/cairo_speedup_log.md`

**Step 1: Create the file**

```markdown
# CAIRO speedup log

Tracking benchmark results for issue #250.
Machine: 8-core EC2
Baseline: ~2.5 min/run before any patches

---
```

**Step 2: Commit**

```bash
git add context/tools/cairo_speedup_log.md
git commit -m "Add CAIRO speedup benchmark log"
```

---

### Task 0.2: Add per-stage timing to `run()` in `run_scenario.py`

**Files:**

- Modify: `rate_design/ri/hp_rates/run_scenario.py`

**Step 1: Read the current `run()` function** (already done — lines 605–769)

**Step 2: Add timing**

Add `import time` at the top of the file (after existing imports).

Wrap each major call in `run()` with `t0 = time.perf_counter()` / `log.info("TIMING ...")` pairs. Replace the existing body of `run()` with this version (keep all logic identical, only add timing):

```python
def run(settings: ScenarioSettings) -> None:
    log.info(
        ".... Beginning RI residential (non-LMI) rate scenario simulation: %s",
        settings.run_name,
    )

    dask.config.set(scheduler="processes", num_workers=settings.process_workers)

    # Phase 1 ---------------------------------------------------------------
    t0 = time.perf_counter()
    prototype_ids = _load_prototype_ids_for_run(
        settings.path_utility_assignment,
        settings.utility,
        settings.sample_size,
    )
    log.info("TIMING _load_prototype_ids_for_run: %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    tariffs_params, tariff_map_df = _initialize_tariffs(
        tariff_map=settings.path_tariff_maps_electric,
        building_stock_sample=prototype_ids,
        tariff_paths=settings.path_tariffs_electric,
    )
    log.info("TIMING _initialize_tariffs: %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    precalc_mapping = _build_precalc_period_mapping(settings.path_tariffs_electric)
    log.info("TIMING _build_precalc_period_mapping: %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    customer_count = get_residential_customer_count_from_utility_stats(
        settings.path_electric_utility_stats,
        settings.utility,
        storage_options=STORAGE_OPTIONS,
    )
    customer_metadata = return_buildingstock(
        load_scenario=settings.path_resstock_metadata,
        building_stock_sample=prototype_ids,
        customer_count=customer_count,
        columns=[
            "applicability",
            "postprocess_group.has_hp",
            "postprocess_group.heating_type",
            "in.vintage_acs",
        ],
    )
    log.info("TIMING return_buildingstock: %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    bldg_id_to_load_filepath = build_bldg_id_to_load_filepath(
        path_resstock_loads=settings.path_resstock_loads,
        building_ids=prototype_ids,
    )
    log.info("TIMING build_bldg_id_to_load_filepath: %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    raw_load_elec = _return_load(
        load_type="electricity",
        target_year=settings.year_run,
        building_stock_sample=prototype_ids,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )
    log.info("TIMING _return_load(electricity): %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    raw_load_gas = _return_load(
        load_type="gas",
        target_year=settings.year_run,
        building_stock_sample=prototype_ids,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )
    log.info("TIMING _return_load(gas): %.1fs", time.perf_counter() - t0)

    # Phase 2 ---------------------------------------------------------------
    t0 = time.perf_counter()
    bulk_marginal_costs = _load_cambium_marginal_costs(
        settings.path_cambium_marginal_costs,
        settings.year_run,
    )
    distribution_marginal_costs = load_distribution_marginal_costs(
        settings.path_td_marginal_costs,
    )
    log.info(
        ".... Loaded distribution marginal costs rows=%s",
        len(distribution_marginal_costs),
    )
    sell_rate = _return_export_compensation_rate(
        year_run=settings.year_run,
        solar_pv_compensation=settings.solar_pv_compensation,
        solar_pv_export_import_ratio=1.0,
        tariff_dict=tariffs_params,
    )
    rr_setting = settings.utility_delivery_revenue_requirement
    if isinstance(rr_setting, dict):
        rr_total = sum(rr_setting.values())
        rr_ratios: dict[str, float] | None = {
            k: v / rr_total for k, v in rr_setting.items()
        }
    else:
        rr_total = rr_setting
        rr_ratios = None
    (
        revenue_requirement_raw,
        marginal_system_prices,
        marginal_system_costs,
        costs_by_type,
    ) = _return_revenue_requirement_target(
        building_load=raw_load_elec,
        sample_weight=customer_metadata[["bldg_id", "weight"]],
        revenue_requirement_target=rr_total,
        residual_cost=None,
        residual_cost_frac=None,
        bulk_marginal_costs=bulk_marginal_costs,
        distribution_marginal_costs=distribution_marginal_costs,
        low_income_strategy=None,
        delivery_only_rev_req_passed=settings.add_supply_revenue_requirement,
    )
    if rr_ratios is not None:
        revenue_requirement = {
            k: v * revenue_requirement_raw for k, v in rr_ratios.items()
        }
    else:
        revenue_requirement = revenue_requirement_raw
    log.info("TIMING phase2_marginal_costs_rr: %.1fs", time.perf_counter() - t0)

    # Phase 3 ---------------------------------------------------------------
    t0 = time.perf_counter()
    bs = MeetRevenueSufficiencySystemWide(
        run_type=settings.run_type,
        year_run=settings.year_run,
        year_dollar_conversion=settings.year_dollar_conversion,
        process_workers=settings.process_workers,
        building_stock_sample=prototype_ids,
        run_name=settings.run_name,
        output_dir=settings.path_results,
    )
    bs.simulate(
        revenue_requirement=revenue_requirement,
        tariffs_params=tariffs_params,
        tariff_map=tariff_map_df,
        precalc_period_mapping=precalc_mapping,
        customer_metadata=customer_metadata,
        customer_electricity_load=raw_load_elec,
        customer_gas_load=raw_load_gas,
        gas_tariff_map=settings.path_tariff_maps_gas,
        gas_tariff_str_loc=settings.path_tariffs_gas,
        load_cols="total_fuel_electricity",
        marginal_system_prices=marginal_system_prices,
        costs_by_type=costs_by_type,
        solar_pv_compensation=None,
        sell_rate=sell_rate,
        low_income_strategy=None,
        low_income_participation_target=None,
        low_income_bill_assistance_program=None,
    )
    log.info("TIMING bs.simulate: %.1fs", time.perf_counter() - t0)

    save_file_loc = getattr(bs, "save_file_loc", None)
    if save_file_loc is not None:
        distribution_mc_path = Path(save_file_loc) / "distribution_marginal_costs.csv"
        distribution_marginal_costs.to_csv(distribution_mc_path, index=True)
        log.info(".... Saved distribution marginal costs: %s", distribution_mc_path)

    log.info(".... Completed RI residential (non-LMI) rate scenario simulation")
```

Also add `import time` near the top of the file, after `import random`.

**Step 3: Enable INFO logging so TIMING lines are visible**

Check how logging is configured. If needed, add this before calling `run()` in `main()`:

```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
```

Check if logging is already configured elsewhere — if so, don't duplicate.

**Step 4: Run to confirm timing output appears**

```bash
cd rate_design/ri/hp_rates && uv run python run_scenario.py --run-num 1 --utility rie 2>&1 | grep -E "TIMING|Beginning|Completed"
```

Expected: lines like `TIMING _return_load(electricity): 45.2s`

**Step 5: Record baseline timings in the log**

Add a section to `context/tools/cairo_speedup_log.md`:

```markdown
## Baseline — run 1 (pre-patch), DATE

| Stage                          | Time (s) |
| ------------------------------ | -------- |
| _load_prototype_ids_for_run    | X.X      |
| _initialize_tariffs            | X.X      |
| _build_precalc_period_mapping  | X.X      |
| return_buildingstock           | X.X      |
| build_bldg_id_to_load_filepath | X.X      |
| _return_load(electricity)      | X.X      |
| _return_load(gas)              | X.X      |
| phase2_marginal_costs_rr       | X.X      |
| bs.simulate                    | X.X      |
| **Total**                      | **X.X**  |
```

**Step 6: Commit**

```bash
git add rate_design/ri/hp_rates/run_scenario.py context/tools/cairo_speedup_log.md
git commit -m "Phase 0: add per-stage timing to run_scenario.py"
```

---

## Phase 1 — Combined batch file reader (Tier 2a + 2b)

**What we're replacing:** Two sequential `_return_load` calls each reading 1,910 parquet files → one batch read of 1,910 files returning both electricity and gas DataFrames.

**Key insight:** All RI buildings share AMY2018 data. The timeshift offset from 2018→2025 is (2 - 0) % 7 = 2 days = 48 hours, constant for every building. We can vectorize the timeshift across all buildings in one numpy roll.

### Task 1.1: Create `patches.py` skeleton and write failing test

**Files:**

- Create: `rate_design/ri/hp_rates/patches.py`
- Create: `tests/test_patches.py`

**Step 1: Create `patches.py` skeleton**

```python
"""
Monkey-patches on top of CAIRO for performance.
See docs/plans/2026-02-23-cairo-speedup-design.md and context/tools/cairo_speedup_log.md.

Import this module at the top of run_scenario.py (after all other imports):
    import rate_design.ri.hp_rates.patches  # noqa: F401
"""
from __future__ import annotations
```

**Step 2: Write a failing test for the combined reader**

```python
# tests/test_patches.py
"""Tests for rate_design/ri/hp_rates/patches.py — CAIRO performance monkey-patches."""
from __future__ import annotations

import pandas as pd
import pytest
from pathlib import Path

LOAD_DIR = Path("/data.sb/nrel/resstock/res_2024_amy2018_2/load_curve_hourly/state=RI/upgrade=00/")
SAMPLE_IDS = [100147, 100151, 100312]  # first 3 buildings from the upgrade dir


@pytest.fixture
def sample_filepaths():
    """Return {bldg_id: path} for SAMPLE_IDS. Skip if data not accessible."""
    from utils.cairo import build_bldg_id_to_load_filepath
    if not LOAD_DIR.exists():
        pytest.skip("ResStock load dir not accessible")
    fps = build_bldg_id_to_load_filepath(
        path_resstock_loads=LOAD_DIR,
        building_ids=SAMPLE_IDS,
    )
    if len(fps) < len(SAMPLE_IDS):
        pytest.skip("Not all sample building IDs found")
    return fps


def test_combined_reader_matches_separate_reads(sample_filepaths):
    """_return_loads_combined returns same data as two separate _return_load calls."""
    from cairo.rates_tool.loads import _return_load
    from rate_design.ri.hp_rates.patches import _return_loads_combined

    target_year = 2025
    bldg_ids = list(sample_filepaths.keys())

    # reference: two separate reads
    ref_elec = _return_load(
        load_type="electricity",
        target_year=target_year,
        building_stock_sample=bldg_ids,
        load_filepath_key=sample_filepaths,
        force_tz="EST",
    )
    ref_gas = _return_load(
        load_type="gas",
        target_year=target_year,
        building_stock_sample=bldg_ids,
        load_filepath_key=sample_filepaths,
        force_tz="EST",
    )

    # patched: one combined read
    new_elec, new_gas = _return_loads_combined(
        target_year=target_year,
        building_ids=bldg_ids,
        load_filepath_key=sample_filepaths,
        force_tz="EST",
    )

    pd.testing.assert_frame_equal(
        new_elec.sort_index(),
        ref_elec.sort_index(),
        check_exact=False,
        rtol=1e-4,
        check_names=True,
    )
    pd.testing.assert_frame_equal(
        new_gas.sort_index(),
        ref_gas.sort_index(),
        check_exact=False,
        rtol=1e-4,
        check_names=True,
    )
```

**Step 3: Run to verify test fails**

```bash
uv run pytest tests/test_patches.py::test_combined_reader_matches_separate_reads -v
```

Expected: `FAILED` with `ImportError: cannot import name '_return_loads_combined'`

---

### Task 1.2: Implement `_return_loads_combined` in `patches.py`

**Files:**

- Modify: `rate_design/ri/hp_rates/patches.py`

**Step 1: Implement the function**

```python
"""
Monkey-patches on top of CAIRO for performance.
See docs/plans/2026-02-23-cairo-speedup-design.md and context/tools/cairo_speedup_log.md.

Import this module at the top of run_scenario.py (after all other imports):
    import rate_design.ri.hp_rates.patches  # noqa: F401  (currently no-op; patches added below)
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path, PurePath

import numpy as np
import pandas as pd
import pyarrow.dataset as pad

# Columns to read from each parquet file in one pass
_ELEC_RAW_COLS = [
    "bldg_id",
    "timestamp",
    "out.electricity.total.energy_consumption",
    "out.electricity.pv.energy_consumption",
]
_GAS_RAW_COLS = [
    "bldg_id",
    "timestamp",
    "out.natural_gas.total.energy_consumption",
]
_ALL_COLS = list(dict.fromkeys(_ELEC_RAW_COLS + _GAS_RAW_COLS))  # deduplicated, ordered

_GAS_KWH_TO_THERM = 0.0341214116  # from CAIRO _adjust_gas_loads


def _return_loads_combined(
    target_year: int,
    building_ids: list[int],
    load_filepath_key: dict[int, Path],
    force_tz: str | None = "EST",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read electricity and gas loads for all buildings in one PyArrow batch read.

    Replaces two sequential _return_load() calls (one per fuel type) with a single
    multi-threaded read of all parquet files, returning the same DataFrames that
    _return_load("electricity") and _return_load("gas") would return.

    Returns
    -------
    (raw_load_elec, raw_load_gas) — same structure as _return_load outputs:
        MultiIndex [bldg_id, time], 8760 rows per building.
        Electricity: columns [load_data, pv_generation, electricity_net, grid_cons]
        Gas: columns [load_data]  (units: therms)
    """
    # 1. Collect file paths in building_ids order (preserves determinism)
    paths = [str(load_filepath_key[bid]) for bid in building_ids if bid in load_filepath_key]
    present_ids = [bid for bid in building_ids if bid in load_filepath_key]

    # 2. Batch read: all files, only the columns we need
    ds = pad.dataset(paths, format="parquet")
    table = ds.to_table(columns=_ALL_COLS)
    df = table.to_pandas()

    # 3. Rename timestamp → time, set MultiIndex [bldg_id, time]
    df = df.rename(columns={"timestamp": "time"})
    df = df.sort_values(["bldg_id", "time"]).reset_index(drop=True)

    # 4. Vectorized timeshift: same offset for all buildings (AMY2018 → target_year)
    source_year = df["time"].dt.year.iloc[0]
    offset_days = (
        dt.datetime(target_year, 1, 1).weekday()
        - dt.datetime(source_year, 1, 1).weekday()
    ) % 7
    offset_hours = offset_days * 24

    if offset_hours > 0:
        # Roll each building's 8760 rows by offset_hours positions
        # groupby preserves order; apply roll per group
        def _roll(grp: pd.DataFrame) -> pd.DataFrame:
            data_cols = [c for c in grp.columns if c not in ("bldg_id", "time")]
            grp = grp.copy()
            grp[data_cols] = np.roll(grp[data_cols].values, offset_hours, axis=0)
            return grp

        df = df.groupby("bldg_id", group_keys=False).apply(_roll)

    # 5. Replace year in timestamps with target_year
    df["time"] = df["time"].apply(lambda ts: ts.replace(year=target_year))

    # 6. Set MultiIndex [bldg_id, time]
    df = df.set_index(["bldg_id", "time"])

    # 7. Apply timezone
    if force_tz is not None:
        df.index = df.index.set_levels(
            df.index.get_level_values("time").tz_localize(force_tz),
            level="time",
        )

    # 8. Build electricity DataFrame
    elec = df[["out.electricity.total.energy_consumption", "out.electricity.pv.energy_consumption"]].copy()
    elec = elec.rename(columns={
        "out.electricity.total.energy_consumption": "load_data",
        "out.electricity.pv.energy_consumption": "pv_generation",
    })
    # Make pv_generation non-negative (CAIRO convention)
    elec["pv_generation"] = elec["pv_generation"].abs()
    # Compute net electricity
    if elec["pv_generation"].eq(0).all():
        elec["electricity_net"] = elec["load_data"]
    else:
        elec["electricity_net"] = elec["load_data"] - elec["pv_generation"]
    elec["grid_cons"] = elec["electricity_net"].clip(lower=0)
    # Keep only columns CAIRO expects
    elec = elec[["load_data", "pv_generation", "electricity_net", "grid_cons"]]

    # 9. Build gas DataFrame (units: therms)
    gas = df[["out.natural_gas.total.energy_consumption"]].copy()
    gas = gas.rename(columns={"out.natural_gas.total.energy_consumption": "load_data"})
    gas["load_data"] = gas["load_data"] * _GAS_KWH_TO_THERM
    # Gas load data dtype: float64 to match CAIRO output

    return elec, gas
```

**Step 2: Run the test**

```bash
uv run pytest tests/test_patches.py::test_combined_reader_matches_separate_reads -v
```

Expected: `PASSED`

If failing, debug by printing shapes and dtypes:

```python
print(new_elec.dtypes, ref_elec.dtypes)
print(new_elec.index[:3], ref_elec.index[:3])
```

Common issues:

- `electricity_net` / `grid_cons` not computed by `_return_load` for non-solar → check what columns ref_elec actually has and match them
- Index level ordering (bldg_id vs time) mismatch → check `ref_elec.index.names`

---

### Task 1.3: Hook combined reader into `run_scenario.py`

**Files:**

- Modify: `rate_design/ri/hp_rates/run_scenario.py`

**Step 1: Add import at top of file**

After the existing imports block, add:

```python
from rate_design.ri.hp_rates.patches import _return_loads_combined
```

**Step 2: Replace the two `_return_load` calls**

Find and replace this block in `run()`:

```python
    t0 = time.perf_counter()
    raw_load_elec = _return_load(
        load_type="electricity",
        target_year=settings.year_run,
        building_stock_sample=prototype_ids,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )
    log.info("TIMING _return_load(electricity): %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    raw_load_gas = _return_load(
        load_type="gas",
        target_year=settings.year_run,
        building_stock_sample=prototype_ids,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )
    log.info("TIMING _return_load(gas): %.1fs", time.perf_counter() - t0)
```

With:

```python
t0 = time.perf_counter()
raw_load_elec, raw_load_gas = _return_loads_combined(
    target_year=settings.year_run,
    building_ids=prototype_ids,
    load_filepath_key=bldg_id_to_load_filepath,
    force_tz="EST",
)
log.info("TIMING _return_loads_combined: %.1fs", time.perf_counter() - t0)
```

**Step 3: Save a reference run 2 output before applying the patch**

Before running with the patch, capture a reference output from `run-scenario 2` using the unpatched code (if not already saved). Do this FIRST by temporarily reverting the import and running:

```bash
# If you haven't already, run unpatched run 2 and note the output directory
cd rate_design/ri/hp_rates && uv run python run_scenario.py --run-num 2 --utility rie 2>&1 | tail -5
# Note the output path shown in logs
```

**Step 4: Run patched `run-scenario 2` and compare**

```bash
cd rate_design/ri/hp_rates && uv run python run_scenario.py --run-num 2 --utility rie 2>&1 | grep -E "TIMING|Completed"
```

**Step 5: Verify output equality**

Write a quick comparison inline:

```python
# In a scratch Python session or add to test_patches.py as test_phase1_output_equality
import pandas as pd
from pathlib import Path

# Replace with actual timestamped paths from the two runs
ref_dir = Path("/data.sb/switchbox/cairo/outputs/hp_rates/ri/TIMESTAMP_REF_ri_rie_run2_up00_precalc_supply__flat/")
new_dir = Path("/data.sb/switchbox/cairo/outputs/hp_rates/ri/TIMESTAMP_NEW_ri_rie_run2_up00_precalc_supply__flat/")

for csv_file in ref_dir.glob("*.csv"):
    ref = pd.read_csv(csv_file)
    new = pd.read_csv(new_dir / csv_file.name)
    pd.testing.assert_frame_equal(ref, new, check_exact=False, rtol=1e-4)
    print(f"OK: {csv_file.name}")
```

**Step 6: Record Phase 1 timings in log**

Add to `context/tools/cairo_speedup_log.md`:

```markdown
## Phase 1 — combined batch reader, DATE

| Stage                     | Baseline (s) | Phase 1 (s) | Δ             |
| ------------------------- | ------------ | ----------- | ------------- |
| _return_load(electricity) | X.X          | —           | —             |
| _return_load(gas)         | X.X          | —           | —             |
| _return_loads_combined    | —            | X.X         | saves X.Xs    |
| bs.simulate               | X.X          | X.X         | X.Xs          |
| **Total**                 | **X.X**      | **X.X**     | **X.Xs (X×)** |
```

**Step 7: Commit**

```bash
git add rate_design/ri/hp_rates/patches.py rate_design/ri/hp_rates/run_scenario.py tests/test_patches.py context/tools/cairo_speedup_log.md
git commit -m "Phase 1: combined batch parquet reader, halves file I/O"
```

---

## Phase 2 — Vectorized tariff aggregation (Tier 2c)

**What we're replacing:** `process_building_demand_by_period` runs 1,910 `dask.delayed(aggregate_load_worker)` calls. Each worker: copies the building's 8760-row DataFrame, extracts month/hour/day_type, merges with period schedule, groups by period/tier. Total: N copies + N merges + N groupbys.

**Replacement:** One pass over the full N×8760 DataFrame — extract datetime columns once, broadcast tariff → period mapping, one `groupby().sum()`.

**Scope:** flat and time-of-use tariffs (no tiered/combined). This covers all 12 RI runs.

### Task 2.1: Write failing test for vectorized aggregation

**Files:**

- Modify: `tests/test_patches.py`

**Step 1: Add the test**

```python
def test_vectorized_aggregation_matches_cairo(sample_filepaths):
    """_vectorized_process_building_demand_by_period returns same agg_load as CAIRO."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "rate_design" / "ri" / "hp_rates"))

    from cairo.rates_tool.loads import _return_load, process_building_demand_by_period
    from rate_design.ri.hp_rates.patches import _vectorized_process_building_demand_by_period

    # Load the flat tariff used in run 1
    tariff_path = Path(__file__).resolve().parent.parent / "rate_design/ri/hp_rates/config/tariffs/electric/rie_flat.json"
    if not tariff_path.exists():
        pytest.skip("rie_flat.json not found; run `just create-flat-tariff` first")

    import json
    with tariff_path.open() as f:
        tariff_dict = json.load(f)

    tariff_base = {"rie_flat": tariff_dict}
    bldg_ids = list(sample_filepaths.keys())
    tariff_map = pd.DataFrame({"bldg_id": bldg_ids, "tariff_key": "rie_flat"})

    # Load electricity
    raw_load = _return_load(
        load_type="electricity",
        target_year=2025,
        building_stock_sample=bldg_ids,
        load_filepath_key=sample_filepaths,
        force_tz="EST",
    )

    # Reference: CAIRO's version
    ref_agg_load, ref_agg_solar = process_building_demand_by_period(
        target_year=2025,
        load_col_key="total_fuel_electricity",
        prototype_ids=bldg_ids,
        tariff_base=tariff_base,
        tariff_map=tariff_map,
        prepassed_load=raw_load,
        solar_pv_compensation=None,
    )

    # Patched: vectorized version
    new_agg_load, new_agg_solar = _vectorized_process_building_demand_by_period(
        target_year=2025,
        load_col_key="total_fuel_electricity",
        prototype_ids=bldg_ids,
        tariff_base=tariff_base,
        tariff_map=tariff_map,
        prepassed_load=raw_load,
        solar_pv_compensation=None,
    )

    # Check agg_load shape, index, columns, values
    ref_sorted = ref_agg_load.sort_values(["bldg_id", "month", "period", "tier", "charge_type"]) \
        .reset_index(drop=True) if "bldg_id" in ref_agg_load.columns \
        else ref_agg_load.reset_index().sort_values(["bldg_id", "month", "period", "tier", "charge_type"]).reset_index(drop=True)
    new_sorted = new_agg_load.reset_index().sort_values(["bldg_id", "month", "period", "tier", "charge_type"]).reset_index(drop=True)

    for col in ["grid_cons", "load_data"]:
        if col in ref_sorted.columns and col in new_sorted.columns:
            pd.testing.assert_series_equal(
                ref_sorted[col].reset_index(drop=True),
                new_sorted[col].reset_index(drop=True),
                check_exact=False,
                rtol=1e-4,
                check_names=False,
            )
```

**Step 2: Run to verify it fails**

```bash
uv run pytest tests/test_patches.py::test_vectorized_aggregation_matches_cairo -v
```

Expected: `FAILED` with `ImportError: cannot import name '_vectorized_process_building_demand_by_period'`

---

### Task 2.2: Inspect actual `agg_load` structure before implementing

**Step 1: Run a quick inspection to understand the exact output columns and index**

```bash
uv run python -c "
import json, pandas as pd
from pathlib import Path
from cairo.rates_tool.loads import _return_load, process_building_demand_by_period
from utils.cairo import build_bldg_id_to_load_filepath

LOAD_DIR = Path('/data.sb/nrel/resstock/res_2024_amy2018_2/load_curve_hourly/state=RI/upgrade=00/')
SAMPLE_IDS = [100147, 100151]
fps = build_bldg_id_to_load_filepath(path_resstock_loads=LOAD_DIR, building_ids=SAMPLE_IDS)
raw = _return_load(load_type='electricity', target_year=2025, building_stock_sample=SAMPLE_IDS, load_filepath_key=fps, force_tz='EST')

tariff_path = Path('rate_design/ri/hp_rates/config/tariffs/electric/rie_flat.json')
with tariff_path.open() as f:
    td = json.load(f)
tariff_base = {'rie_flat': td}
tariff_map = pd.DataFrame({'bldg_id': SAMPLE_IDS, 'tariff_key': 'rie_flat'})

agg_load, agg_solar = process_building_demand_by_period(
    target_year=2025, load_col_key='total_fuel_electricity',
    prototype_ids=SAMPLE_IDS, tariff_base=tariff_base, tariff_map=tariff_map,
    prepassed_load=raw, solar_pv_compensation=None,
)
print('agg_load.index.names:', agg_load.index.names)
print('agg_load.columns:', list(agg_load.columns))
print('agg_load dtypes:'); print(agg_load.dtypes)
print(agg_load.head(5))
print()
print('agg_solar.columns:', list(agg_solar.columns))
print(agg_solar.head(3))
"
```

Record the exact index names and column names — the vectorized replacement must return identical structure.

---

### Task 2.3: Implement `_vectorized_process_building_demand_by_period`

**Files:**

- Modify: `rate_design/ri/hp_rates/patches.py`

**Step 1: Add the implementation**

Append to `patches.py`:

```python
# ---------------------------------------------------------------------------
# Phase 2: vectorized tariff aggregation
# ---------------------------------------------------------------------------

def _vectorized_process_building_demand_by_period(
    target_year: int,
    load_col_key: str,
    prototype_ids: list[int],
    tariff_base: dict,
    tariff_map,
    prepassed_load: pd.DataFrame,
    solar_pv_compensation=None,
):
    """
    Vectorized replacement for cairo.rates_tool.loads.process_building_demand_by_period.

    Handles flat and time-of-use tariffs (covers all 12 RI runs).
    Tiered/combined tariffs fall back to CAIRO's original implementation.
    """
    import cairo.rates_tool.tariffs as tariff_funcs
    from cairo.rates_tool.loads import (
        process_building_demand_by_period as _orig_pbdbp,
        _return_energy_charge_aggregation_method,
    )

    # Load tariff mapping: bldg_id → tariff_key, tariff_key → tariff_dict
    tariff_map_dict, tariff_dicts = tariff_funcs._load_base_tariffs(
        tariff_base=tariff_base, tariff_map=tariff_map, prototype_ids=prototype_ids
    )

    # Check all tariffs are flat or TOU — fall back if any are tiered/combined
    for tariff_key, tariff_dict in tariff_dicts.items():
        method = _return_energy_charge_aggregation_method(tariff_dict)
        if method not in ("flat", "time-of-use"):
            import logging
            logging.getLogger("rates_analysis").warning(
                "Vectorized aggregation: tariff %s has method %s, falling back to CAIRO",
                tariff_key, method,
            )
            return _orig_pbdbp(
                target_year=target_year,
                load_col_key=load_col_key,
                prototype_ids=prototype_ids,
                tariff_base=tariff_base,
                tariff_map=tariff_map,
                prepassed_load=prepassed_load,
                solar_pv_compensation=solar_pv_compensation,
            )

    # Build combined period schedule: (tariff_key, month, hour, day_type) → period
    schedule_parts = []
    for tariff_key, tariff_dict in tariff_dicts.items():
        sched = tariff_funcs._charge_period_mapping(tariff_dict).copy()
        sched["tariff"] = tariff_key
        schedule_parts.append(sched)
    combined_schedule = pd.concat(schedule_parts, ignore_index=True)
    # combined_schedule columns: month, hour, day_type, period, tariff

    # Build full load DataFrame from prepassed_load (MultiIndex [bldg_id, time])
    load_full = prepassed_load.reset_index()
    # load_full columns: bldg_id, time, load_data, [pv_generation, electricity_net, grid_cons]

    # Add datetime indicators (int8 for memory efficiency)
    time_col = load_full["time"]
    load_full = load_full.copy()
    load_full["month"] = time_col.dt.month.astype("int8")
    load_full["hour"] = time_col.dt.hour.astype("int8")
    load_full["day_type"] = time_col.dt.weekday.lt(5).map({True: "weekday", False: "weekend"})

    # Map bldg_id → tariff_key
    tariff_series = pd.Series(tariff_map_dict, name="tariff").rename_axis("bldg_id").reset_index()
    load_full = load_full.merge(tariff_series, on="bldg_id", how="left")

    # Merge with period schedule
    load_full = load_full.merge(
        combined_schedule,
        on=["tariff", "month", "hour", "day_type"],
        how="left",
    )

    # Identify available load columns
    avail_load_cols = [c for c in ["grid_cons", "load_data"] if c in load_full.columns]

    # Add tier=1 (flat/TOU: single tier per period)
    load_full["tier"] = 1
    load_full["charge_type"] = "energy_charge"

    # Aggregate energy charges: sum by (bldg_id, tariff, month, period, tier, charge_type)
    group_cols = ["bldg_id", "tariff", "month", "period", "tier", "charge_type"]
    agg_energy = (
        load_full
        .groupby(group_cols)[avail_load_cols]
        .sum()
        .reset_index()
    )

    # Demand charges: monthly peak demand per period
    # For flat tariff: no demand charge (demand charge = 0 or NaN per CAIRO convention)
    # Check whether any tariff has demand charges
    has_demand_charge = any(
        tariff_dict.get("ur_dc_enable", 0) == 1
        for tariff_dict in tariff_dicts.values()
    )

    if has_demand_charge:
        # Peak demand: max grid_cons per (bldg_id, tariff, month, period)
        if "grid_cons" in load_full.columns:
            peak = (
                load_full
                .groupby(["bldg_id", "tariff", "month", "period"])["grid_cons"]
                .max()
                .reset_index()
                .rename(columns={"grid_cons": "grid_cons"})
            )
            peak["tier"] = 1
            peak["charge_type"] = "demand_charge"
            for col in avail_load_cols:
                if col != "grid_cons":
                    peak[col] = np.nan
            agg_load = pd.concat([agg_energy, peak], ignore_index=True)
        else:
            agg_load = agg_energy
    else:
        # No demand charges: add NaN demand_charge rows to match CAIRO structure
        demand_rows = agg_energy[["bldg_id", "tariff", "month", "period", "tier"]].drop_duplicates().copy()
        demand_rows["charge_type"] = "demand_charge"
        for col in avail_load_cols:
            demand_rows[col] = np.nan
        agg_load = pd.concat([agg_energy, demand_rows], ignore_index=True)

    # Set index to bldg_id (matching CAIRO output)
    agg_load = agg_load.set_index("bldg_id")
    agg_load = agg_load.fillna(0.0)

    # Solar: for non-solar case (pv_generation all zeros), return empty-ish DataFrame
    # matching CAIRO structure
    if "pv_generation" not in load_full.columns or load_full.get("pv_generation", pd.Series([0])).eq(0).all():
        agg_solar = pd.DataFrame(
            columns=["bldg_id", "tariff", "month", "period", "tier",
                     "net_exports", "self_cons", "pv_generation", "charge_type"]
        ).set_index("bldg_id")
        # CAIRO returns one solar row per building even when pv=0 — replicate structure
        # by building a zeros DataFrame with same groupby keys
        solar_rows = agg_load.reset_index()[["bldg_id", "tariff", "month", "period", "tier"]].drop_duplicates().copy()
        solar_rows["net_exports"] = 0.0
        solar_rows["self_cons"] = 0.0
        solar_rows["pv_generation"] = 0.0
        solar_rows["charge_type"] = "solar_compensation"
        agg_solar = solar_rows.set_index("bldg_id")
    else:
        # Has solar: fall back to CAIRO for solar aggregation (rare for RI)
        _, agg_solar = _orig_pbdbp(
            target_year=target_year,
            load_col_key=load_col_key,
            prototype_ids=prototype_ids,
            tariff_base=tariff_base,
            tariff_map=tariff_map,
            prepassed_load=prepassed_load,
            solar_pv_compensation=solar_pv_compensation,
        )

    return agg_load, agg_solar
```

**Step 2: Run the test**

```bash
uv run pytest tests/test_patches.py::test_vectorized_aggregation_matches_cairo -v
```

Expected: `PASSED`

If failing on structure mismatch, re-run the inspection from Task 2.2 and adjust column names / index names to match exactly.

---

### Task 2.4: Monkey-patch `process_building_demand_by_period` into CAIRO

**Files:**

- Modify: `rate_design/ri/hp_rates/patches.py`

**Step 1: Add the monkey-patch at the bottom of `patches.py`**

```python
# ---------------------------------------------------------------------------
# Apply monkey-patches
# ---------------------------------------------------------------------------
import cairo.rates_tool.loads as _cairo_loads

_cairo_loads.process_building_demand_by_period = _vectorized_process_building_demand_by_period
```

**Step 2: Verify the patch is applied when `patches.py` is imported**

```bash
uv run python -c "
from rate_design.ri.hp_rates.patches import _vectorized_process_building_demand_by_period
import cairo.rates_tool.loads as loads
print('patched:', loads.process_building_demand_by_period is _vectorized_process_building_demand_by_period)
"
```

Expected: `patched: True`

**Step 3: Run `run-scenario 2` end-to-end and compare outputs against reference**

```bash
cd rate_design/ri/hp_rates && uv run python run_scenario.py --run-num 2 --utility rie 2>&1 | grep -E "TIMING|Completed"
```

Compare all output CSVs against the Phase 1 baseline for run 2 (same comparison script as Task 1.3 Step 5).

**Step 4: Record Phase 2 timings in log**

Add section to `context/tools/cairo_speedup_log.md` with updated timings.

**Step 5: Commit**

```bash
git add rate_design/ri/hp_rates/patches.py tests/test_patches.py context/tools/cairo_speedup_log.md
git commit -m "Phase 2: vectorized tariff aggregation, replaces per-building Dask loop"
```

---

## Phase 3 — Vectorized bill calculation (Tier 2d)

**What we're replacing:** `run_system_revenues` loops over 1,910 buildings, one `dask.delayed(return_monthly_bills_year1)` per building. Each call: merges aggregated load with rate matrix, sums to monthly bills, adds fixed charges.

**Replacement:** Merge all building loads with the rate matrix in one pass; compute costs vectorially; pivot to monthly bill DataFrame.

### Task 3.1: Inspect the bill calculation output structure

**Step 1: Run inspection**

```bash
uv run python -c "
import json, pandas as pd
from pathlib import Path
from cairo.rates_tool.loads import _return_load, process_building_demand_by_period
from cairo.rates_tool.system_revenues import run_system_revenues
from utils.cairo import build_bldg_id_to_load_filepath

LOAD_DIR = Path('/data.sb/nrel/resstock/res_2024_amy2018_2/load_curve_hourly/state=RI/upgrade=00/')
SAMPLE_IDS = [100147, 100151, 100312]
fps = build_bldg_id_to_load_filepath(path_resstock_loads=LOAD_DIR, building_ids=SAMPLE_IDS)
raw = _return_load(load_type='electricity', target_year=2025, building_stock_sample=SAMPLE_IDS, load_filepath_key=fps, force_tz='EST')

tariff_path = Path('rate_design/ri/hp_rates/config/tariffs/electric/rie_flat.json')
with tariff_path.open() as f:
    td = json.load(f)
tariff_base = {'rie_flat': td}
tariff_map = pd.DataFrame({'bldg_id': SAMPLE_IDS, 'tariff_key': 'rie_flat'})

agg_load, agg_solar = process_building_demand_by_period(
    target_year=2025, load_col_key='total_fuel_electricity',
    prototype_ids=SAMPLE_IDS, tariff_base=tariff_base, tariff_map=tariff_map,
    prepassed_load=raw, solar_pv_compensation=None,
)

comp_df = run_system_revenues(
    aggregated_load=agg_load,
    aggregated_solar=agg_solar,
    solar_compensation_df=None,
    prototype_ids=SAMPLE_IDS,
    tariff_config=tariff_base,
    tariff_strategy=tariff_map,
)
print('comp_df.index.names:', comp_df.index.names)
print('comp_df.columns:', list(comp_df.columns))
print('comp_df dtypes:'); print(comp_df.dtypes)
print(comp_df.head())
"
```

Record exact output structure before implementing.

---

### Task 3.2: Write failing test for vectorized billing

**Files:**

- Modify: `tests/test_patches.py`

**Step 1: Add test**

```python
def test_vectorized_billing_matches_cairo(sample_filepaths):
    """_vectorized_run_system_revenues returns same bills as CAIRO."""
    import json
    from cairo.rates_tool.loads import _return_load, process_building_demand_by_period
    from cairo.rates_tool.system_revenues import run_system_revenues
    from rate_design.ri.hp_rates.patches import _vectorized_run_system_revenues

    tariff_path = Path(__file__).resolve().parent.parent / "rate_design/ri/hp_rates/config/tariffs/electric/rie_flat.json"
    if not tariff_path.exists():
        pytest.skip("rie_flat.json not found")
    with tariff_path.open() as f:
        td = json.load(f)

    bldg_ids = list(sample_filepaths.keys())
    tariff_base = {"rie_flat": td}
    tariff_map = pd.DataFrame({"bldg_id": bldg_ids, "tariff_key": "rie_flat"})

    raw = _return_load(
        load_type="electricity", target_year=2025, building_stock_sample=bldg_ids,
        load_filepath_key=sample_filepaths, force_tz="EST",
    )
    agg_load, agg_solar = process_building_demand_by_period(
        target_year=2025, load_col_key="total_fuel_electricity",
        prototype_ids=bldg_ids, tariff_base=tariff_base,
        tariff_map=tariff_map, prepassed_load=raw, solar_pv_compensation=None,
    )

    ref_bills = run_system_revenues(
        aggregated_load=agg_load, aggregated_solar=agg_solar,
        solar_compensation_df=None, prototype_ids=bldg_ids,
        tariff_config=tariff_base, tariff_strategy=tariff_map,
    )
    new_bills = _vectorized_run_system_revenues(
        aggregated_load=agg_load, aggregated_solar=agg_solar,
        solar_compensation_df=None, prototype_ids=bldg_ids,
        tariff_config=tariff_base, tariff_strategy=tariff_map,
    )

    pd.testing.assert_frame_equal(
        ref_bills.sort_index(),
        new_bills.sort_index(),
        check_exact=False,
        rtol=1e-4,
    )
```

**Step 2: Run to verify failure**

```bash
uv run pytest tests/test_patches.py::test_vectorized_billing_matches_cairo -v
```

Expected: `FAILED` with ImportError

---

### Task 3.3: Implement `_vectorized_run_system_revenues`

**Files:**

- Modify: `rate_design/ri/hp_rates/patches.py`

**Step 1: Understand the billing structure**

From the inspection in Task 3.1, `run_system_revenues` returns a DataFrame where:

- Index: `bldg_id`
- Columns: `[Jan, Feb, ..., Dec, Annual]` (monthly + annual bill totals)
- Each value: total monthly bill in $

The per-building bill calculation in `return_monthly_bills_year1`:

- Energy charges: `sum(grid_cons * energy_rate)` grouped by month
- Fixed charges: flat $/month
- Demand charges: `peak_demand * demand_rate` per month

**Step 2: Implement**

```python
# ---------------------------------------------------------------------------
# Phase 3: vectorized bill calculation
# ---------------------------------------------------------------------------

def _vectorized_run_system_revenues(
    aggregated_load: pd.DataFrame,
    aggregated_solar,
    solar_compensation_df,
    solar_compensation_style=None,
    process_agg_load: bool = True,
    prototype_ids=None,
    tariff_config=None,
    tariff_strategy=None,
):
    """
    Vectorized replacement for cairo.rates_tool.system_revenues.run_system_revenues.

    Handles flat and TOU tariffs with energy + fixed charges.
    Falls back to CAIRO for demand charges or solar compensation.
    """
    import cairo.rates_tool.tariffs as tariff_funcs
    import cairo.rates_tool.lookups as lookups
    from cairo.rates_tool.system_revenues import run_system_revenues as _orig_rsr

    tariff_map_dict, tariff_dicts = tariff_funcs._load_base_tariffs(
        tariff_base=tariff_config, tariff_map=tariff_strategy, prototype_ids=prototype_ids
    )

    # Check for features requiring fallback
    has_demand = any(td.get("ur_dc_enable", 0) == 1 for td in tariff_dicts.values())
    has_solar = solar_compensation_df is not None and any(
        v is not None for v in solar_compensation_df.values()
    ) if isinstance(solar_compensation_df, dict) else solar_compensation_df is not None

    if has_demand or has_solar:
        return _orig_rsr(
            aggregated_load=aggregated_load,
            aggregated_solar=aggregated_solar,
            solar_compensation_df=solar_compensation_df,
            solar_compensation_style=solar_compensation_style,
            process_agg_load=process_agg_load,
            prototype_ids=prototype_ids,
            tariff_config=tariff_config,
            tariff_strategy=tariff_strategy,
        )

    # Build rate lookup: (tariff_key, period, tier) → energy_rate
    rate_rows = []
    fixed_charge_rows = []
    for tariff_key, td in tariff_dicts.items():
        # Energy charge matrix: ur_ec_tou_mat is list of [period, tier, kWh_max, kWh_buy, kWh_sell, $/kWh]
        ec_mat = td.get("ur_ec_tou_mat", [])
        for row in ec_mat:
            rate_rows.append({
                "tariff": tariff_key,
                "period": int(row[0]),
                "tier": int(row[1]),
                "energy_rate": float(row[5]),
            })
        # Fixed charge: ur_monthly_fixed_charge ($/month)
        fixed_rows = [
            {"tariff": tariff_key, "month": m + 1, "fixed_charge": float(td.get("ur_monthly_fixed_charge", 0.0))}
            for m in range(12)
        ]
        fixed_charge_rows.extend(fixed_rows)

    rate_df = pd.DataFrame(rate_rows)  # columns: tariff, period, tier, energy_rate
    fixed_df = pd.DataFrame(fixed_charge_rows)  # columns: tariff, month, fixed_charge

    # Map bldg_id → tariff_key
    tariff_series = pd.Series(tariff_map_dict, name="tariff").rename_axis("bldg_id").reset_index()

    # Get energy charge rows from aggregated_load
    agg = aggregated_load.reset_index()
    energy = agg[agg["charge_type"] == "energy_charge"].copy()

    # Merge bldg → tariff
    energy = energy.merge(tariff_series, on="bldg_id", how="left")
    # Merge with rates
    energy = energy.merge(rate_df, on=["tariff", "period", "tier"], how="left")
    energy["energy_cost"] = energy["grid_cons"] * energy["energy_rate"]

    # Monthly energy bills
    monthly_energy = (
        energy.groupby(["bldg_id", "month"])["energy_cost"]
        .sum()
        .reset_index()
        .rename(columns={"energy_cost": "bill"})
    )

    # Add fixed charges
    monthly_energy = monthly_energy.merge(tariff_series, on="bldg_id", how="left")
    monthly_energy = monthly_energy.merge(fixed_df, on=["tariff", "month"], how="left")
    monthly_energy["bill"] = monthly_energy["bill"] + monthly_energy["fixed_charge"].fillna(0.0)

    # Pivot to wide format: index=bldg_id, columns=month (1..12)
    bills_wide = monthly_energy.pivot(index="bldg_id", columns="month", values="bill")
    bills_wide.columns = [int(c) for c in bills_wide.columns]

    # Rename columns to month names matching CAIRO (Jan=0? or by name? check lookups.months)
    # lookups.months maps integer keys to month name strings
    month_map = {i + 1: name for i, name in enumerate(lookups.months)}
    bills_wide = bills_wide.rename(columns=month_map)

    # Add Annual column
    bills_wide["Annual"] = bills_wide.sum(axis=1)

    return bills_wide
```

**Step 3: Run the test**

```bash
uv run pytest tests/test_patches.py::test_vectorized_billing_matches_cairo -v
```

Expected: `PASSED`

If the month columns don't match CAIRO's naming: print `lookups.months` and adjust the rename map.

---

### Task 3.4: Add monkey-patch for `run_system_revenues`

**Files:**

- Modify: `rate_design/ri/hp_rates/patches.py`

**Step 1: Add to the monkey-patch block at the bottom of `patches.py`**

```python
import cairo.rates_tool.system_revenues as _cairo_sysrev

_cairo_sysrev.run_system_revenues = _vectorized_run_system_revenues
```

**Step 2: Verify patch is applied**

```bash
uv run python -c "
from rate_design.ri.hp_rates.patches import _vectorized_run_system_revenues
import cairo.rates_tool.system_revenues as sr
print('patched:', sr.run_system_revenues is _vectorized_run_system_revenues)
"
```

Expected: `patched: True`

**Step 3: Run `run-scenario 2` end-to-end and compare against Phase 2 reference**

```bash
cd rate_design/ri/hp_rates && uv run python run_scenario.py --run-num 2 --utility rie 2>&1 | grep -E "TIMING|Completed"
```

Compare all output CSVs against pre-patch baseline.

**Step 4: Run run 1 as well to confirm**

```bash
cd rate_design/ri/hp_rates && just run-1 2>&1 | grep -E "TIMING|Completed"
```

**Step 5: Record Phase 3 timings in log**

Add final section to `context/tools/cairo_speedup_log.md`:

```markdown
## Phase 3 — vectorized billing, DATE

| Stage                  | Baseline (s) | Phase 3 (s) | Δ              |
| ---------------------- | ------------ | ----------- | -------------- |
| _return_loads_combined | X.X          | X.X         | —              |
| bs.simulate            | X.X          | X.X         | Xs             |
| **Total**              | **X.X**      | **X.X**     | **X× speedup** |
```

**Step 6: Commit**

```bash
git add rate_design/ri/hp_rates/patches.py tests/test_patches.py context/tools/cairo_speedup_log.md
git commit -m "Phase 3: vectorized bill calculation, replaces per-building Dask loop"
```

---

## Troubleshooting Guide

**`CAIRO's _load_base_tariffs` KeyError on bldg_id:**
The tariff_map passed to it can be a DataFrame with column `tariff_key` (not `tariff`). Check what column name CAIRO expects by printing `tariff_map.columns` in the test.

**Month column naming mismatch in bills:**
`lookups.months` may be 0-indexed. Run `uv run python -c "from cairo.rates_tool import lookups; print(lookups.months)"` to see exact mapping.

**`agg_load` has unexpected demand_charge rows:**
The flat tariff may return empty demand rows (NaN). Run the structure inspection and match exactly.

**`_charge_period_mapping` not accessible:**
Check with `uv run python -c "from cairo.rates_tool import tariffs; print(dir(tariffs))"`. It may be `tariffs._charge_period_mapping` or similar.

**PyArrow dataset column not found:**
Some buildings may have different schemas. Use `pad.dataset(paths, format="parquet", unify_schemas=True)` to coerce to a common schema.

**Timeshift produces wrong timestamps:**
Verify with: `uv run python -c "import datetime as dt; print(dt.datetime(2018,1,1).weekday(), dt.datetime(2025,1,1).weekday())"`. Expect 0 and 2 (Mon and Wed).

**test_combined_reader fails on electricity_net mismatch:**
`_return_load` may not include `electricity_net` or `grid_cons` in its output for non-solar buildings. Print `ref_elec.columns` and only assert equality on the columns that are present.
