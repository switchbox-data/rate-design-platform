"""
Monkey-patches on top of CAIRO for performance.
See docs/plans/2026-02-23-cairo-speedup-design.md and context/tools/cairo_speedup_log.md.

Import this module at the top of run_scenario.py (after all other imports):
    import rate_design.ri.hp_rates.patches  # noqa: F401  (currently no-op; patches added below)
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

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

# kWh -> therms conversion factor (from CAIRO _adjust_gas_loads docstring)
_GAS_KWH_TO_THERM = 0.0341214116


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
        Electricity: columns ['load_data', 'pv_generation', 'electricity_net']
        Gas: columns ['load_data'] (units: therms)
    """
    # 1. Collect file paths in building_ids order (preserves determinism)
    present_ids = [bid for bid in building_ids if bid in load_filepath_key]
    paths = [str(load_filepath_key[bid]) for bid in present_ids]

    # 2. Batch read: all files, only the columns we need, in one pass
    ds = pad.dataset(paths, format="parquet")
    table = ds.to_table(columns=_ALL_COLS)
    df = table.to_pandas()

    # 3. Sort by [bldg_id, timestamp] to ensure consistent ordering
    df = df.sort_values(["bldg_id", "timestamp"]).reset_index(drop=True)

    # 4. Vectorized timeshift — same offset for all buildings (AMY2018 -> target_year)
    #    CAIRO __timeshift__ does: data[48:] concat data[:48], i.e. np.roll with negative offset
    source_year = int(df["timestamp"].dt.year.iloc[0])
    start_day_orig = dt.datetime(source_year, 1, 1).weekday()
    start_day_target = dt.datetime(target_year, 1, 1).weekday()
    offset_days = (start_day_target - start_day_orig) % 7
    offset_hours = offset_days * 24

    data_col_names = [
        "out.electricity.total.energy_consumption",
        "out.electricity.pv.energy_consumption",
        "out.natural_gas.total.energy_consumption",
    ]

    if offset_hours > 0:
        # All buildings share the same 8760-row structure and the same offset.
        # Reshape to (n_bldgs, 8760, n_cols), roll axis=1, then flatten back.
        # This matches CAIRO __timeshift__: pd.concat([data.iloc[N:], data.iloc[:N]])
        # which equals np.roll(data, -N, axis=0).
        n_rows = len(df)
        n_bldgs = n_rows // 8760
        arr = df[data_col_names].values.reshape(n_bldgs, 8760, len(data_col_names))
        arr = np.roll(arr, -offset_hours, axis=1)
        df[data_col_names] = arr.reshape(n_rows, len(data_col_names))

    # 5. Replace year in timestamps with target_year — vectorized via fixed time offset.
    #    Since all source timestamps are in source_year (no leap-year complication between
    #    non-leap source and non-leap target), adding (Jan 1 target - Jan 1 source) gives
    #    the same result as ts.replace(year=target_year) for every hour.
    year_offset = pd.Timestamp(f"{target_year}-01-01") - pd.Timestamp(f"{source_year}-01-01")
    df["time"] = df["timestamp"] + year_offset
    df = df.drop(columns=["timestamp"])

    # 6. Set MultiIndex [bldg_id, time]
    df = df.set_index(["bldg_id", "time"])

    # 7. Apply timezone (tz_localize on the unique time level values only)
    if force_tz is not None:
        # set_levels requires unique level values; localize the level, not the flat values
        unique_time_level = df.index.levels[df.index.names.index("time")].tz_localize(force_tz)
        df.index = df.index.set_levels(unique_time_level, level="time")

    # 8. Build electricity DataFrame — match _return_load("electricity") structure exactly
    #    Output columns: ['load_data', 'pv_generation', 'electricity_net']
    elec = pd.DataFrame(index=df.index)
    elec["load_data"] = df["out.electricity.total.energy_consumption"]
    elec["pv_generation"] = df["out.electricity.pv.energy_consumption"]

    # Replicate CAIRO __load_buildingprofile__ electricity_net logic per building:
    # - if all pv_generation == 0: electricity_net = load_data
    # - if pv_generation < 0 (ResStock convention): electricity_net = load_data + pv_generation
    # - if pv_generation >= 0 (CAIRO convention): electricity_net = load_data - pv_generation
    # Use vectorized per-block check; blocks are contiguous since df is sorted by bldg_id.
    load_arr = elec["load_data"].values
    pv_arr = elec["pv_generation"].values
    elec_net = np.empty(len(elec), dtype=np.float64)

    for start_idx in range(0, len(elec), 8760):
        pv_block = pv_arr[start_idx : start_idx + 8760]
        ld_block = load_arr[start_idx : start_idx + 8760]
        if (pv_block == 0.0).all():
            elec_net[start_idx : start_idx + 8760] = ld_block
        elif (pv_block < 0.0).any():
            elec_net[start_idx : start_idx + 8760] = ld_block + pv_block
        else:
            elec_net[start_idx : start_idx + 8760] = ld_block - pv_block

    elec["electricity_net"] = elec_net

    # 9. Build gas DataFrame — match _return_load("gas") structure exactly
    #    Output column: ['load_data'] in therms
    gas = pd.DataFrame(index=df.index)
    gas["load_data"] = df["out.natural_gas.total.energy_consumption"] * _GAS_KWH_TO_THERM

    return elec, gas
