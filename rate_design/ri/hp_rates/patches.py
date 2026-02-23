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
import pyarrow as pa
import pyarrow.dataset as pad
import pyarrow.parquet as pq

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

    # 2. Batch read: all files, only the columns we need, in one pass.
    # Unify schemas across files so that files with minor schema differences
    # (e.g. extra metadata or reordered fields) are read safely.
    # PyArrow 23 does not support unify_schemas= on dataset(); we achieve the
    # same effect by passing an explicit unified schema.
    unified_schema = pa.unify_schemas([pq.read_schema(p) for p in paths])
    ds = pad.dataset(paths, format="parquet", schema=unified_schema)
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
        assert n_rows % 8760 == 0, (
            f"Expected all buildings to have 8760 rows, but total row count {n_rows} "
            f"is not divisible by 8760. Check for leap-year or sub-hourly parquet files."
        )
        assert n_rows // 8760 == len(present_ids), (
            f"Row count implies {n_rows // 8760} buildings but {len(present_ids)} IDs were requested."
        )
        n_bldgs = n_rows // 8760
        arr = df[data_col_names].values.reshape(n_bldgs, 8760, len(data_col_names))
        arr = np.roll(arr, -offset_hours, axis=1)
        df[data_col_names] = arr.reshape(n_rows, len(data_col_names))

    # 5. Replace year in timestamps with target_year — vectorized via fixed time offset.
    #    Since all source timestamps are in source_year (no leap-year complication between
    #    non-leap source and non-leap target), adding (Jan 1 target - Jan 1 source) gives
    #    the same result as ts.replace(year=target_year) for every hour.
    # NOTE: CAIRO's __timeshift__ skips year replacement when weekday_diff == 0
    # (i.e., when source and target year start on the same weekday). We always
    # replace the year here because the Timedelta offset already handles the
    # no-shift case (offset_hours=0 leaves data unchanged). For the current RI
    # runs (2018→2025, offset=48h), this divergence is not triggered.
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

    Handles flat and time-of-use tariffs (covers all 12 RI runs, which are all flat or TOU
    with ur_tou_tier_comb_type=0 and ur_dc_enable=0).
    Tiered/combined tariffs fall back to CAIRO's original implementation.

    Returns
    -------
    (agg_load, agg_solar) with identical structure to CAIRO's process_building_demand_by_period:
        agg_load:  Index=['bldg_id'], columns=['month','period','tier','grid_cons',
                   'load_data','charge_type','tariff']
        agg_solar: Index=['bldg_id'], columns=['month','period','tier','net_exports',
                   'self_cons','pv_generation','charge_type','tariff']
    """
    from cairo.rates_tool.loads import (
        _return_energy_charge_aggregation_method,
        extract_energy_charge_map,
        _calculate_pv_load_columns,
    )
    from cairo.rates_tool import tariffs as tariff_funcs
    # Use the saved-before-patching original to avoid infinite recursion.
    _orig_pbdbp = _orig_process_building_demand_by_period

    # Gas loads: fall back to CAIRO's original implementation.
    # CAIRO's aggregate_load_worker always calls _load_worker → _adjust_gas_loads,
    # which converts kWh→therms even on pre-loaded therms data (a consistent
    # double-conversion that we must replicate to match Phase 1 output exactly).
    # Gas billing is not the performance bottleneck, so we don't need to vectorize it.
    if load_col_key == "total_fuel_gas":
        return _orig_pbdbp(
            target_year=target_year,
            load_col_key=load_col_key,
            prototype_ids=prototype_ids,
            tariff_base=tariff_base,
            tariff_map=tariff_map,
            prepassed_load=prepassed_load,
            solar_pv_compensation=solar_pv_compensation,
        )

    # Use CAIRO's _load_base_tariffs to get the prototype->tariff mapping and
    # the converted tariff dicts (deep copies already in PySAM format)
    tariff_map_dict, tariff_dicts = tariff_funcs._load_base_tariffs(
        tariff_base=tariff_base, tariff_map=tariff_map, prototype_ids=prototype_ids
    )

    # Classify each tariff once
    tier_tou_check = {
        k: _return_energy_charge_aggregation_method(v) for k, v in tariff_dicts.items()
    }

    # Fall back to CAIRO for any tiered or combined tariff
    has_tiered_or_combined = any(
        v in ("tiered", "combined") for v in tier_tou_check.values()
    )
    if has_tiered_or_combined:
        return _orig_pbdbp(
            target_year=target_year,
            load_col_key=load_col_key,
            prototype_ids=prototype_ids,
            tariff_base=tariff_base,
            tariff_map=tariff_map,
            prepassed_load=prepassed_load,
            solar_pv_compensation=solar_pv_compensation,
        )

    # --- Vectorized path for flat / time-of-use tariffs ---
    #
    # prepassed_load has MultiIndex [bldg_id, time] and columns:
    #   electricity: ['load_data', 'pv_generation', 'electricity_net']
    #   gas:         ['load_data']
    #
    # For electricity with no solar (all RI buildings), _calculate_pv_load_columns
    # renames electricity_net -> grid_cons, keeps load_data and pv_generation.
    # avail_load_cols = ['grid_cons', 'load_data']
    # avail_pv_cols   = ['net_exports', 'self_cons', 'pv_generation']
    #   but only pv_generation will be present (no net_exports/self_cons unless solar)

    # 1. Prepare a flat (non-indexed) DataFrame with all buildings
    all_loads = prepassed_load.reset_index()  # columns: bldg_id, time, load_data, ...

    # 2. Apply pv sign correction (same logic as aggregate_load_worker)
    if load_col_key == "total_fuel_gas":
        all_loads["pv_generation"] = 0.0
    if "pv_generation" in all_loads.columns:
        all_loads["pv_generation"] = all_loads["pv_generation"].abs()
    elif "net_exports" in all_loads.columns:
        all_loads["net_exports"] = all_loads["net_exports"].abs()

    # 3. Derive grid_cons etc. via _calculate_pv_load_columns (operates per-building)
    #    For electricity_net present (no solar): electricity_net -> grid_cons
    #    For buildings with pv_generation: grid_cons = load_data - pv_generation
    #    We can do this vectorized since it's column arithmetic.
    if "electricity_net" in all_loads.columns:
        all_loads["grid_cons"] = all_loads["electricity_net"].clip(lower=0)
        all_loads = all_loads.drop(columns=["electricity_net"])
    if "pv_generation" in all_loads.columns and "grid_cons" not in all_loads.columns:
        all_loads["grid_cons"] = all_loads["load_data"] - all_loads["pv_generation"]
    if "pv_generation" in all_loads.columns and "grid_cons" in all_loads.columns:
        # net_exports = max(0, pv_generation - load_data); self_cons = min(pv_gen, load_data)
        all_loads["net_exports"] = (
            all_loads["pv_generation"] - all_loads["load_data"]
        ).clip(lower=0.0)
        all_loads["self_cons"] = all_loads["pv_generation"].clip(upper=all_loads["load_data"])

    avail_load_cols = [c for c in ["grid_cons", "load_data"] if c in all_loads.columns]
    avail_pv_cols = [
        c for c in ["net_exports", "self_cons", "pv_generation"] if c in all_loads.columns
    ]

    # 4. Add datetime indicators (same as _add_datetime_indicators)
    all_loads["month"] = all_loads["time"].dt.month
    all_loads["hour"] = all_loads["time"].dt.hour
    all_loads["day_type"] = (all_loads["time"].dt.weekday < 5).map(
        {True: "weekday", False: "weekend"}
    )

    # 5. Per tariff: merge period schedule, groupby, assemble result
    energy_parts: list[pd.DataFrame] = []
    solar_parts: list[pd.DataFrame] = []

    # Build reverse map: tariff_key -> list of bldg_ids
    tariff_to_bldgs: dict[str, list[int]] = {}
    for bid in prototype_ids:
        tk = tariff_map_dict[bid]
        tariff_to_bldgs.setdefault(tk, []).append(bid)

    for tariff_key, bldg_ids_for_tariff in tariff_to_bldgs.items():
        td = tariff_dicts[tariff_key]

        # Get the period schedule for this tariff
        charge_period_map = tariff_funcs._charge_period_mapping(td)

        # Extract tier info (flat/TOU: one tier per period)
        energy_charge_usage_map = extract_energy_charge_map(td)

        # Subset to buildings for this tariff
        bldg_mask = all_loads["bldg_id"].isin(bldg_ids_for_tariff)
        loads_sub = all_loads.loc[bldg_mask].copy()

        # Merge period assignment
        loads_sub = loads_sub.merge(
            charge_period_map,
            on=["month", "hour", "day_type"],
            how="left",
        )

        # Merge tier info (flat/TOU path: merge on period only)
        loads_sub = loads_sub.merge(
            energy_charge_usage_map,
            on=["period"],
            how="left",
        )

        # Aggregate: sum by [bldg_id, month, period, tier]
        agg_cols = avail_load_cols + avail_pv_cols
        full_agg = loads_sub.groupby(
            ["bldg_id", "month", "period", "tier"], as_index=False
        )[agg_cols].sum()
        full_agg["tariff"] = tariff_key

        # Energy charge: drop PV columns (matches CAIRO's _energy_charge_aggregation behavior)
        energy_agg = full_agg.drop(columns=avail_pv_cols, errors="ignore").copy()
        energy_agg["charge_type"] = "energy_charge"
        energy_parts.append(energy_agg)

        # Solar aggregation (net_metering / None): PV columns kept, load cols dropped,
        # filter to rows where pv_generation > 0 (matches _solar_compensation_aggregation)
        solar_agg = full_agg.drop(columns=avail_load_cols, errors="ignore").copy()
        solar_agg["charge_type"] = "solar_compensation"
        if "pv_generation" in avail_pv_cols:
            solar_agg = solar_agg.loc[solar_agg["pv_generation"] > 0.0]
        elif "net_exports" in avail_pv_cols:
            solar_agg = solar_agg.loc[solar_agg["net_exports"] > 0.0]
        solar_parts.append(solar_agg)

    # 6. Concatenate energy parts
    all_energy = pd.concat(energy_parts, ignore_index=True)

    # 7. Build demand charge rows (ur_dc_enable=0 for all RI tariffs → NaN rows, then filled to 0)
    #    Matches CAIRO's _demand_charge_aggregation output: columns are month, period, tier,
    #    grid_cons, charge_type — load_data added as NaN for concat compatibility.
    demand_rows = []
    for bid in prototype_ids:
        tk = tariff_map_dict[bid]
        for month in range(1, 13):
            demand_rows.append({
                "bldg_id": bid,
                "month": month,
                "period": np.nan,
                "tier": np.nan,
                "grid_cons": np.nan,
                "charge_type": "demand_charge",
                "tariff": tk,
            })
    demand_df = pd.DataFrame(demand_rows)
    # Add any other load cols that are in all_energy but not yet in demand_df
    for col in avail_load_cols:
        if col not in demand_df.columns:
            demand_df[col] = np.nan
    # Do NOT add PV cols to demand_df — energy charge rows don't have them

    # 8. Combine energy + demand, fillna(0.0) as CAIRO does
    combined = pd.concat([all_energy, demand_df], ignore_index=True).fillna(0.0)

    # 9. Set index to bldg_id
    combined = combined.set_index("bldg_id")

    # 10. Assemble agg_solar
    all_solar = pd.concat(solar_parts, ignore_index=True) if solar_parts else pd.DataFrame(
        columns=["bldg_id", "month", "period", "tier"] + avail_pv_cols + ["charge_type", "tariff"]
    )
    # Add any missing pv columns to solar df
    for col in ["net_exports", "self_cons", "pv_generation"]:
        if col not in all_solar.columns:
            all_solar[col] = np.nan
    # Set index
    all_solar = all_solar.set_index("bldg_id")

    return combined, all_solar


# ---------------------------------------------------------------------------
# Apply monkey-patches
# ---------------------------------------------------------------------------
import cairo.rates_tool.loads as _cairo_loads

# Save the original BEFORE replacing, so fallback calls inside
# _vectorized_process_building_demand_by_period don't recurse into the patch.
_orig_process_building_demand_by_period = _cairo_loads.process_building_demand_by_period

_cairo_loads.process_building_demand_by_period = _vectorized_process_building_demand_by_period
