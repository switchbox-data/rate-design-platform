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

    # Gas loads: handled in the vectorized path below.
    # prepassed_load for gas has column ['load_data'] in therms (converted in _return_loads_combined).
    # By handling gas here we bypass aggregate_load_worker → _adjust_gas_loads, which would
    # otherwise convert therms to therms×0.034 (a bug present in the original CAIRO path).
    # This is intentional: gas bills will now be correct (not doubly-scaled).
    is_gas = load_col_key == "total_fuel_gas"

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

    # 2. Apply pv sign correction and derive grid_cons — electricity only
    #    Gas has no pv columns and no electricity_net; load_data (therms) is the only column.
    if not is_gas:
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
    if not is_gas and "pv_generation" in all_loads.columns and "grid_cons" in all_loads.columns:
        # net_exports = max(0, pv_generation - load_data); self_cons = min(pv_gen, load_data)
        all_loads["net_exports"] = (
            all_loads["pv_generation"] - all_loads["load_data"]
        ).clip(lower=0.0)
        all_loads["self_cons"] = all_loads["pv_generation"].clip(upper=all_loads["load_data"])

    if is_gas:
        avail_load_cols = ["load_data"]
        avail_pv_cols = []
    else:
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
        # Gas has no solar data — skip entirely.
        if not is_gas:
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
            row = {
                "bldg_id": bid,
                "month": month,
                "period": np.nan,
                "tier": np.nan,
                "charge_type": "demand_charge",
                "tariff": tk,
            }
            # For electricity tariffs, CAIRO always emits a grid_cons column in demand rows.
            # For gas (is_gas), grid_cons is not part of avail_load_cols so we skip it.
            if not is_gas:
                row["grid_cons"] = np.nan
            demand_rows.append(row)
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

    Replaces the 1,910-task Dask loop (one dask.delayed per building) with a single
    vectorized pandas pass that:
      1. Joins energy charge rates onto aggregated_load
      2. Sums costs by (bldg_id, month) in one groupby
      3. Adds fixed charges ($/month) vectorized across all buildings
      4. Applies min_charge per month if needed
      5. Pivots to the wide month-column format CAIRO returns

    Handles flat and TOU tariffs (all RI runs). Falls back to original CAIRO
    implementation for demand charges or solar compensation.
    """
    import cairo.rates_tool.lookups as lookups
    from cairo.rates_tool import tariffs as tariff_funcs
    # Reference saved at module level before monkey-patch to avoid recursion.
    _orig_rsr = _orig_run_system_revenues

    tariff_map_dict, tariff_dicts = tariff_funcs._load_base_tariffs(
        tariff_base=tariff_config, tariff_map=tariff_strategy, prototype_ids=prototype_ids
    )

    # Normalize solar_compensation_df the same way CAIRO does
    if solar_compensation_df is None:
        solar_compensation_df_norm = {k: None for k in set(tariff_map_dict.values())}
    else:
        solar_compensation_df_norm = solar_compensation_df

    # Check for features requiring fallback
    has_demand = any(td.get("ur_dc_enable", 0) == 1 for td in tariff_dicts.values())
    has_solar = any(v is not None for v in solar_compensation_df_norm.values())

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

    # Build a rate lookup DataFrame from ur_ec_tou_mat across all tariffs.
    # ur_ec_tou_mat rows are tuples: (period, tier, max_usage, max_usage_units, rate, adjustments[, sell_rate])
    # Effective rate = rate + adjustments (same as calculate_energy_charges in CAIRO).
    rate_rows = []
    for tariff_key, td in tariff_dicts.items():
        for row in td["ur_ec_tou_mat"]:
            rate_rows.append({
                "tariff": tariff_key,
                "period": float(row[0]),
                "tier": float(row[1]),
                "rate": float(row[4]) + float(row[5]),  # rate + adjustments
            })
    rate_lookup = pd.DataFrame(rate_rows)

    # Process agg_load the same way CAIRO does in run_system_revenues.
    # When process_agg_load=True, CAIRO does:
    #   agg_load_df = aggregated_load.loc[prototype_id]  (single building slice)
    # and passes it to return_monthly_bills_year1, which calls
    # calculate_energy_charges(agg_load_df, td) — which filters to charge_type=="energy_charge"
    # and merges on [period, tier], then sums to get costs per month.
    #
    # We replicate this in bulk: filter energy_charge rows, merge rates, multiply.

    if not process_agg_load:
        # Non-standard path — fall back to CAIRO
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

    # --- Vectorized energy charge calculation ---
    # Filter to energy_charge rows only
    energy_rows = aggregated_load[aggregated_load["charge_type"] == "energy_charge"].copy()
    energy_rows = energy_rows.reset_index()  # bring bldg_id into columns

    # Merge energy rates (by tariff + period + tier)
    energy_rows = energy_rows.merge(rate_lookup, on=["tariff", "period", "tier"], how="left")
    # For electricity: bill on grid_cons; for gas: bill on load_data (no grid_cons column)
    billing_col = "grid_cons" if "grid_cons" in energy_rows.columns else "load_data"
    energy_rows["costs"] = energy_rows[billing_col] * energy_rows["rate"]

    # Sum energy costs by (bldg_id, month)
    monthly_energy = (
        energy_rows.groupby(["bldg_id", "month"], as_index=False)["costs"].sum()
    )

    # Pivot to wide format: rows=bldg_id, columns=month (1..12)
    monthly_wide = monthly_energy.pivot(index="bldg_id", columns="month", values="costs")
    # Ensure all 12 months are present
    for m in range(1, 13):
        if m not in monthly_wide.columns:
            monthly_wide[m] = 0.0
    monthly_wide = monthly_wide[[m for m in range(1, 13)]]

    # --- Vectorized fixed charge addition ---
    # fixed_charge = ur_monthly_fixed_charge per month per building
    # min_charge = ur_monthly_min_charge per month (applied per month after fixed+energy)
    for bldg_id in prototype_ids:
        tk = tariff_map_dict[bldg_id]
        td = tariff_dicts[tk]
        fixed = td.get("ur_monthly_fixed_charge", 0.0) or 0.0
        min_ch = td.get("ur_monthly_min_charge", 0.0) or 0.0

        if bldg_id not in monthly_wide.index:
            # Building had zero load — fill all months with fixed/min
            monthly_wide.loc[bldg_id, :] = 0.0

        # Add fixed charge to each month
        monthly_wide.loc[bldg_id, :] += fixed

        # Apply min_charge: bill = max(bill, min_charge) per month
        # CAIRO checks: any(fixed_df["min_charge"] > -1e38) — 0.0 > -1e38 is True,
        # but for min_charge=0.0 the max() has no effect on positive bills.
        # We still apply it for correctness.
        if min_ch > 0.0:
            monthly_wide.loc[bldg_id, :] = monthly_wide.loc[bldg_id, :].clip(lower=min_ch)

    # Reorder to match prototype_ids order (CAIRO preserves insertion order)
    monthly_wide = monthly_wide.reindex(prototype_ids)

    # --- Rename columns from 1..12 to month abbreviations and add Annual ---
    monthly_wide.columns = lookups.months
    monthly_wide["Annual"] = monthly_wide.sum(axis=1)

    # Clear the index name (CAIRO's output has index.names=[None])
    monthly_wide.index.name = None

    return monthly_wide


# ---------------------------------------------------------------------------
# Phase 4b: vectorized gas bill calculation
# ---------------------------------------------------------------------------

def _vectorized_calculate_gas_bills(
    aggregated_gas_load: pd.DataFrame,
    prototype_ids: list,
    gas_tariff_base: dict,
    gas_tariff_map,
) -> pd.DataFrame:
    """
    Vectorized gas bill calculation — replaces the per-building Dask loop in
    cairo.rates_tool.system_revenues.run_system_revenues for gas.

    Follows the same pattern as _vectorized_run_system_revenues but operates on
    gas aggregated load (therms). Fixed charge is added per building per month;
    result is pivoted to wide [Jan...Dec, Annual] format with index=bldg_id.

    Parameters
    ----------
    aggregated_gas_load : DataFrame
        Output of _vectorized_process_building_demand_by_period for gas.
        Index=bldg_id, columns=[month, period, tier, load_data, tariff, charge_type].
        energy_charge rows carry consumption in therms.
    prototype_ids : list
        Ordered list of bldg_ids.
    gas_tariff_base : dict
        tariff_key -> PySAM-format tariff dict (from get_default_tariff_structures).
        Keys used: ur_ec_tou_mat, ur_monthly_fixed_charge, ur_monthly_min_charge.
    gas_tariff_map : DataFrame
        Must have columns [bldg_id, tariff_key].

    Returns
    -------
    DataFrame
        Index=bldg_id, columns=[Jan, Feb, ..., Dec, Annual].  Same structure as
        CAIRO's aggregate_system_revenues returns for customer_gas_bills_monthly.
    """
    import cairo.rates_tool.lookups as lookups
    from cairo.rates_tool import tariffs as tariff_funcs

    # Resolve tariff_map_dict: {bldg_id: tariff_key}
    if isinstance(gas_tariff_map, pd.DataFrame):
        tariff_map_df = gas_tariff_map
    else:
        raise TypeError(f"gas_tariff_map must be a DataFrame, got {type(gas_tariff_map)}")

    tariff_map_dict, tariff_dicts = tariff_funcs._load_base_tariffs(
        tariff_base=gas_tariff_base, tariff_map=tariff_map_df, prototype_ids=prototype_ids
    )

    # Build a rate lookup DataFrame from ur_ec_tou_mat across all tariffs.
    # ur_ec_tou_mat rows: (period, tier, max_usage, max_usage_units, rate, adjustments, ...)
    # Effective rate = rate + adjustments (matching CAIRO's calculate_energy_charges).
    rate_rows = []
    for tariff_key, td in tariff_dicts.items():
        tou_mat = td.get("ur_ec_tou_mat", [])
        for row in tou_mat:
            rate_rows.append({
                "tariff": tariff_key,
                "period": float(row[0]),
                "tier": float(row[1]),
                "rate": float(row[4]) + float(row[5]),  # rate + adjustments
            })
    rate_lookup = pd.DataFrame(rate_rows)

    # Filter to energy_charge rows; reset_index to bring bldg_id into columns
    energy_rows = aggregated_gas_load[
        aggregated_gas_load["charge_type"] == "energy_charge"
    ].copy()
    energy_rows = energy_rows.reset_index()  # bldg_id now a column

    # Merge rates on [tariff, period, tier]
    energy_rows = energy_rows.merge(rate_lookup, on=["tariff", "period", "tier"], how="left")

    # Gas consumption is in load_data (therms); no grid_cons column
    energy_rows["costs"] = energy_rows["load_data"] * energy_rows["rate"]

    # Sum energy costs by (bldg_id, month)
    monthly_energy = (
        energy_rows.groupby(["bldg_id", "month"], as_index=False)["costs"].sum()
    )

    # Pivot to wide format: rows=bldg_id, columns=month (1..12)
    monthly_wide = monthly_energy.pivot(index="bldg_id", columns="month", values="costs")
    # Ensure all 12 months present
    for m in range(1, 13):
        if m not in monthly_wide.columns:
            monthly_wide[m] = 0.0
    monthly_wide = monthly_wide[[m for m in range(1, 13)]]

    # Vectorized fixed charge addition + min_charge per building
    for bldg_id in prototype_ids:
        tk = tariff_map_dict[bldg_id]
        td = tariff_dicts[tk]
        fixed = td.get("ur_monthly_fixed_charge", 0.0) or 0.0
        min_ch = td.get("ur_monthly_min_charge", 0.0) or 0.0

        if bldg_id not in monthly_wide.index:
            monthly_wide.loc[bldg_id, :] = 0.0

        monthly_wide.loc[bldg_id, :] += fixed

        if min_ch > 0.0:
            monthly_wide.loc[bldg_id, :] = monthly_wide.loc[bldg_id, :].clip(lower=min_ch)

    # Reorder to match prototype_ids order
    monthly_wide = monthly_wide.reindex(prototype_ids)

    # Rename columns 1..12 to month abbreviations and add Annual
    monthly_wide.columns = lookups.months
    monthly_wide["Annual"] = monthly_wide.sum(axis=1)

    # Clear the index name (CAIRO's output has index.names=[None])
    monthly_wide.index.name = None

    return monthly_wide


# ---------------------------------------------------------------------------
# Apply monkey-patches
# ---------------------------------------------------------------------------
import cairo.rates_tool.loads as _cairo_loads

# Save the original BEFORE replacing, so fallback calls inside
# _vectorized_process_building_demand_by_period don't recurse into the patch.
_orig_process_building_demand_by_period = _cairo_loads.process_building_demand_by_period

_cairo_loads.process_building_demand_by_period = _vectorized_process_building_demand_by_period

import cairo.rates_tool.system_revenues as _cairo_sysrev

# Save the original BEFORE replacing, so fallback calls inside
# _vectorized_run_system_revenues don't recurse into the patch.
_orig_run_system_revenues = _cairo_sysrev.run_system_revenues

_cairo_sysrev.run_system_revenues = _vectorized_run_system_revenues

# ---------------------------------------------------------------------------
# Phase 4b: monkey-patch _calculate_gas_bills on MeetRevenueSufficiencySystemWide
# ---------------------------------------------------------------------------
import logging as _logging
import cairo.rates_tool.systemsimulator as _cairo_sim
import cairo.rates_tool.lookups as _cairo_sim_lookups

# Import _initialize_tariffs from systemsimulator (module-level function)
_cairo_initialize_tariffs = _cairo_sim._initialize_tariffs

_orig_calculate_gas_bills = _cairo_sim.MeetRevenueSufficiencySystemWide._calculate_gas_bills


def _patched_calculate_gas_bills(
    self,
    prototype_ids,
    raw_load,
    target_year,
    customer_metadata,
    gas_tariff_map,
    gas_tariff_str_loc=None,
    gas_tariff_year=None,
):
    """Vectorized replacement for _calculate_gas_bills.

    Aggregates gas load using the vectorized path (bypasses aggregate_load_worker
    and CAIRO's _adjust_gas_loads double-conversion), then computes bills using
    _vectorized_calculate_gas_bills.

    Falls back to the original CAIRO implementation on any exception.
    """
    try:
        # Use _initialize_tariffs (same as the original) to get PySAM tariff dicts
        # and a normalized tariff_map DataFrame.
        # gas_tariff_map: Path to CSV (or DataFrame)
        # gas_tariff_str_loc: dict[str, Path] (tariff_key -> JSON path)
        params_grid_gas, tariff_map_df = _cairo_initialize_tariffs(
            tariff_map=gas_tariff_map,
            building_stock_sample=prototype_ids,
            tariff_paths=gas_tariff_str_loc,
        )

        # raw_load is the output of _return_loads_combined gas side:
        # MultiIndex [bldg_id, time], column ['load_data'] in therms.
        # _vectorized_process_building_demand_by_period handles gas correctly
        # (bypasses CAIRO's double-conversion via aggregate_load_worker).
        agg_gas, _ = _vectorized_process_building_demand_by_period(
            target_year=target_year,
            load_col_key="total_fuel_gas",
            prototype_ids=prototype_ids,
            tariff_base=params_grid_gas,
            tariff_map=tariff_map_df,
            prepassed_load=raw_load,
            solar_pv_compensation=None,
        )

        gas_bills = _vectorized_calculate_gas_bills(
            aggregated_gas_load=agg_gas,
            prototype_ids=prototype_ids,
            gas_tariff_base=params_grid_gas,
            gas_tariff_map=tariff_map_df,
        )

        # Postprocessing expects a 'weight' column (same as aggregate_system_revenues adds
        # via customer_break_down_revenues.join(customer_metadata["weight"])).
        gas_bills = gas_bills.join(
            customer_metadata.set_index("bldg_id")["weight"],
            how="left",
        )

        return gas_bills

    except Exception:
        _logging.getLogger("rates_analysis").warning(
            "Vectorized gas billing failed, falling back to CAIRO", exc_info=True
        )
        return _orig_calculate_gas_bills(
            self,
            prototype_ids=prototype_ids,
            raw_load=raw_load,
            target_year=target_year,
            customer_metadata=customer_metadata,
            gas_tariff_map=gas_tariff_map,
            gas_tariff_str_loc=gas_tariff_str_loc,
            gas_tariff_year=gas_tariff_year or _cairo_sim_lookups.gas_tariff_year,
        )


_cairo_sim.MeetRevenueSufficiencySystemWide._calculate_gas_bills = _patched_calculate_gas_bills
