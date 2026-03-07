"""
Monkey-patches on top of CAIRO for performance.
See docs/plans/2026-02-23-cairo-speedup-design.md and context/tools/cairo_speedup_log.md.

Import this module at the top of run_scenario.py (after all other imports):
    import utils.mid.patches  # noqa: F401
"""

from __future__ import annotations

import datetime as dt
import logging
import resource
import time
from pathlib import Path
from typing import Any, cast

import cairo.rates_tool.loads as _cairo_loads
import cairo.rates_tool.lookups as _cairo_sim_lookups
import cairo.rates_tool.system_revenues as _cairo_sysrev
import cairo.rates_tool.systemsimulator as _cairo_sim
import numpy as np
import pandas as pd
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

log = logging.getLogger("rates_analysis").getChild("patches")

_mem_t0: float = time.perf_counter()


def _log_mem(label: str) -> None:
    """Log peak RSS at DEBUG level. Activate with LOG_LEVEL=DEBUG or equivalent."""
    if not log.isEnabledFor(logging.DEBUG):
        return
    peak_gb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6  # Linux kB
    elapsed = time.perf_counter() - _mem_t0
    log.debug("MEM [%+6.1fs] %-42s peak_rss=%.2f GB", elapsed, label, peak_gb)


def _return_loads_combined(
    target_year: int,
    building_ids: list[int],
    load_filepath_key: dict[int, Path],
    force_tz: str | None = "EST",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read electricity and gas loads for all buildings via Arrow-native processing.

    Reads only the 3 data columns across all files (skips bldg_id and timestamp
    from the bulk read — bldg_id is known from path order, timestamps are read
    from a single file since all buildings share the same 8760-hour series).
    All processing is done on numpy arrays extracted from the Arrow table; no
    intermediate pandas DataFrame is created for the full dataset.

    Returns
    -------
    (raw_load_elec, raw_load_gas) — same structure as _return_load outputs:
        MultiIndex [bldg_id, time], 8760 rows per building.
        Electricity: columns ['load_data', 'pv_generation', 'electricity_net']
        Gas: columns ['load_data'] (units: therms)
    """
    log.info(
        "PATCH_CALL _return_loads_combined target_year=%s buildings=%s force_tz=%s",
        target_year,
        len(building_ids),
        force_tz,
    )
    global _mem_t0
    _mem_t0 = time.perf_counter()

    # 1. Collect file paths in building_ids order (preserves determinism)
    present_ids = [bid for bid in building_ids if bid in load_filepath_key]
    paths = [str(load_filepath_key[bid]) for bid in present_ids]
    n_bldgs = len(present_ids)
    n_rows = n_bldgs * 8760
    _log_mem("after path collection")

    # 2. Read timestamps from the first file only — all ResStock buildings share
    #    the same 8760-hour series, so one file is sufficient.
    first_table = pq.read_table(paths[0], columns=["timestamp"])
    ts_first = first_table.column("timestamp").to_numpy()
    assert len(ts_first) == 8760, (
        f"Expected 8760 rows in first file, got {len(ts_first)}"
    )
    del first_table

    # 3. Compute timeshift parameters from source timestamps
    source_year = int(pd.Timestamp(ts_first[0]).year)
    start_day_orig = dt.datetime(source_year, 1, 1).weekday()
    start_day_target = dt.datetime(target_year, 1, 1).weekday()
    offset_days = (start_day_target - start_day_orig) % 7
    offset_hours = offset_days * 24

    # 4. Build the time index (8760 unique values, shared by all buildings)
    year_offset = pd.Timestamp(f"{target_year}-01-01") - pd.Timestamp(
        f"{source_year}-01-01"
    )
    unique_times = pd.DatetimeIndex(ts_first) + year_offset
    del ts_first
    if force_tz is not None:
        unique_times = unique_times.tz_localize(force_tz)

    # 5. Read only the 3 data columns from all files (skip bldg_id and timestamp).
    #    Use the first file's schema — ResStock load files share identical schemas.
    schema = pq.read_schema(paths[0])
    _DATA_COLS = [
        "out.electricity.total.energy_consumption",
        "out.electricity.pv.energy_consumption",
        "out.natural_gas.total.energy_consumption",
    ]
    ds = pad.dataset(paths, format="parquet", schema=schema)
    table = ds.to_table(columns=_DATA_COLS)
    _log_mem("after to_table (3 data cols, arrow)")

    assert len(table) == n_rows, (
        f"Expected {n_rows} rows ({n_bldgs} bldgs × 8760) but got {len(table)}"
    )

    # 6. Extract numpy arrays and free the Arrow table.
    #    combine_chunks inside to_numpy creates contiguous copies; after del table
    #    the chunked Arrow buffers are freed, leaving only the ~3.2 GB numpy arrays.
    elec_total = table.column(_DATA_COLS[0]).to_numpy()
    elec_pv = table.column(_DATA_COLS[1]).to_numpy()
    gas_total = table.column(_DATA_COLS[2]).to_numpy()
    del table, ds
    _log_mem("after extract numpy + del table")

    # 7. Vectorized timeshift (AMY2018 → target_year)
    if offset_hours > 0:
        elec_total = np.roll(
            elec_total.reshape(n_bldgs, 8760), -offset_hours, axis=1
        ).ravel()
        elec_pv = np.roll(elec_pv.reshape(n_bldgs, 8760), -offset_hours, axis=1).ravel()
        gas_total = np.roll(
            gas_total.reshape(n_bldgs, 8760), -offset_hours, axis=1
        ).ravel()
        _log_mem("after timeshift")

    # 8. PV sign correction per building block (replicates CAIRO __load_buildingprofile__)
    elec_net = np.empty(n_rows, dtype=np.float64)
    for i in range(n_bldgs):
        s, e = i * 8760, (i + 1) * 8760
        pv_block = elec_pv[s:e]
        ld_block = elec_total[s:e]
        if (pv_block == 0.0).all():
            elec_net[s:e] = ld_block
        elif (pv_block < 0.0).any():
            elec_net[s:e] = ld_block + pv_block
        else:
            elec_net[s:e] = ld_block - pv_block

    # 9. Build MultiIndex [bldg_id, time] from codes — avoids materializing
    #    135M bldg_id + time values as columns.
    bldg_level = pd.Index(present_ids, name="bldg_id")
    bldg_codes = np.repeat(np.arange(n_bldgs, dtype=np.intp), 8760)
    time_codes = np.tile(np.arange(8760, dtype=np.intp), n_bldgs)
    mi = pd.MultiIndex(
        levels=[bldg_level, unique_times],
        codes=[bldg_codes, time_codes],
        names=["bldg_id", "time"],
    )
    _log_mem("after build MultiIndex")

    # 10. Build electricity DataFrame (wraps numpy arrays, no copy)
    elec = pd.DataFrame(
        {
            "load_data": elec_total,
            "pv_generation": elec_pv,
            "electricity_net": elec_net,
        },
        index=mi,
        copy=False,
    )

    # 11. Build gas DataFrame (therms conversion)
    gas_therms = gas_total * _GAS_KWH_TO_THERM
    del gas_total
    gas = pd.DataFrame({"load_data": gas_therms}, index=mi, copy=False)

    _log_mem("end of _return_loads_combined")
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
    )
    from cairo.rates_tool import tariffs as tariff_funcs

    # Use the saved-before-patching original to avoid infinite recursion.
    _orig_pbdbp = _orig_process_building_demand_by_period

    # Gas loads arrive in therms (converted from kWh in _return_loads_combined).
    # The vectorized flat/TOU path below uses therms directly, bypassing CAIRO's
    # _adjust_gas_loads.  When we fall back to CAIRO's original path for tiered
    # tariffs, we must undo the conversion first because _load_worker will apply
    # _adjust_gas_loads (×0.0341) again — see the tiered fallback block below.
    is_gas = load_col_key == "total_fuel_gas"
    log.info(
        "PATCH_CALL _vectorized_process_building_demand_by_period load_col_key=%s buildings=%s is_gas=%s",
        load_col_key,
        len(prototype_ids),
        is_gas,
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
        log.info(
            "PATCH_FALLBACK _vectorized_process_building_demand_by_period reason=tiered_or_combined"
        )
        if is_gas:
            # _return_loads_combined already converted kWh → therms, but CAIRO's
            # original path will call _load_worker → _adjust_gas_loads which
            # multiplies by _GAS_KWH_TO_THERM again.  Undo the first conversion
            # so that CAIRO sees kWh and performs a single correct conversion.
            # In-place is safe: the caller never reads prepassed_load after this.
            prepassed_load["load_data"] /= _GAS_KWH_TO_THERM
        return _orig_pbdbp(
            target_year=target_year,
            load_col_key=load_col_key,
            prototype_ids=prototype_ids,
            tariff_base=tariff_base,
            tariff_map=tariff_map,
            prepassed_load=prepassed_load,
            solar_pv_compensation=solar_pv_compensation,
        )

    # --- Numpy-vectorized path for flat / time-of-use tariffs ---
    #
    # Instead of reset_index() (creates a 135M-row DataFrame for 15k buildings),
    # extract numpy arrays reshaped to (n_bldg, 8760), compute derived columns
    # with numpy, and aggregate via matrix multiplication:
    #   (n_bldg_tariff, 8760) @ (8760, n_groups) → (n_bldg_tariff, n_groups)
    # where n_groups = unique (month, period, tier) combos (≤36 for TOU).
    # This reduces peak memory from ~15 GB to ~3-4 GB.

    bldg_ids_all = prepassed_load.index.get_level_values("bldg_id").unique()
    n_bldg = len(bldg_ids_all)
    n_hours = 8760
    bldg_to_row: dict[int, int] = {int(bid): i for i, bid in enumerate(bldg_ids_all)}

    load_data_2d = prepassed_load["load_data"].values.reshape(n_bldg, n_hours)
    cols_present = set(prepassed_load.columns)

    has_elec_net = not is_gas and "electricity_net" in cols_present
    has_pv = not is_gas and "pv_generation" in cols_present
    has_net_exp_raw = not is_gas and "net_exports" in cols_present

    elec_net_2d = (
        prepassed_load["electricity_net"].values.reshape(n_bldg, n_hours)
        if has_elec_net
        else None
    )
    pv_gen_2d = (
        np.abs(prepassed_load["pv_generation"].values.reshape(n_bldg, n_hours))
        if has_pv
        else None
    )
    if has_net_exp_raw and not has_pv:
        pv_gen_2d = np.abs(
            prepassed_load["net_exports"].values.reshape(n_bldg, n_hours)
        )

    grid_cons_2d: np.ndarray | None = None
    net_exports_2d: np.ndarray | None = None
    self_cons_2d: np.ndarray | None = None

    if not is_gas:
        if has_elec_net and elec_net_2d is not None:
            grid_cons_2d = np.maximum(elec_net_2d, 0)
            del elec_net_2d
        if (
            (has_pv or has_net_exp_raw)
            and grid_cons_2d is None
            and pv_gen_2d is not None
        ):
            grid_cons_2d = load_data_2d - pv_gen_2d
        if pv_gen_2d is not None and grid_cons_2d is not None:
            net_exports_2d = np.clip(pv_gen_2d - load_data_2d, 0, None)
            self_cons_2d = np.minimum(pv_gen_2d, load_data_2d)

    if is_gas:
        load_col_arrays: dict[str, np.ndarray] = {"load_data": load_data_2d}
        pv_col_arrays: dict[str, np.ndarray] = {}
    else:
        load_col_arrays = {}
        if grid_cons_2d is not None:
            load_col_arrays["grid_cons"] = grid_cons_2d
        load_col_arrays["load_data"] = load_data_2d
        pv_col_arrays = {}
        if net_exports_2d is not None:
            pv_col_arrays["net_exports"] = net_exports_2d
        if self_cons_2d is not None:
            pv_col_arrays["self_cons"] = self_cons_2d
        if pv_gen_2d is not None:
            pv_col_arrays["pv_generation"] = pv_gen_2d

    avail_load_cols = list(load_col_arrays.keys())
    avail_pv_cols = list(pv_col_arrays.keys())

    # Time vectors (built once from the first building's 8760 timestamps)
    time_idx = prepassed_load.index.get_level_values("time")[:n_hours]
    months_8760 = time_idx.month.values.astype(np.int32)
    hours_8760 = time_idx.hour.values.astype(np.int32)
    weekday_8760 = time_idx.weekday.values  # 0=Mon..6=Sun
    is_weekday_8760 = weekday_8760 < 5

    _log_mem(f"demand_by_period numpy ready n_bldg={n_bldg}")

    # Per-tariff aggregation via matmul
    tariff_to_bldgs: dict[str, list[int]] = {}
    for bid in prototype_ids:
        tk = tariff_map_dict[bid]
        tariff_to_bldgs.setdefault(tk, []).append(bid)

    energy_parts: list[pd.DataFrame] = []
    solar_parts: list[pd.DataFrame] = []

    for tariff_key, bldg_ids_for_tariff in tariff_to_bldgs.items():
        td = tariff_dicts[tariff_key]
        charge_period_map = tariff_funcs._charge_period_mapping(td)
        energy_charge_usage_map = extract_energy_charge_map(td)

        # 3-D lookup: [month_idx (0-11), hour (0-23), day_type (0=wd,1=we)] → period
        period_lut = np.zeros((12, 24, 2), dtype=np.int32)
        for _, row in charge_period_map.iterrows():
            m_idx = int(row["month"]) - 1
            h_idx = int(row["hour"])
            d_idx = 0 if row["day_type"] == "weekday" else 1
            period_lut[m_idx, h_idx, d_idx] = int(row["period"])

        day_type_idx = (~is_weekday_8760).astype(np.int32)
        hour_periods = period_lut[months_8760 - 1, hours_8760, day_type_idx]

        # period → tier lookup
        max_period = int(energy_charge_usage_map["period"].max())
        tier_lut = np.zeros(max_period + 1, dtype=np.int32)
        for _, row in energy_charge_usage_map.iterrows():
            tier_lut[int(row["period"])] = int(row["tier"])
        hour_tiers = tier_lut[hour_periods]

        # Encode (month, period, tier) as a single int for np.unique grouping.
        # Safe when period < 100 and tier < 100 (URDB tariffs use single-digit
        # values; the max observed is ~12 periods × ~6 tiers).
        assert hour_periods.max() < 100 and hour_tiers.max() < 100, (
            f"composite encoding overflow: period_max={hour_periods.max()}, "
            f"tier_max={hour_tiers.max()}"
        )
        composite = months_8760 * 10000 + hour_periods * 100 + hour_tiers
        unique_composites, hour_group_ids = np.unique(composite, return_inverse=True)
        n_groups = len(unique_composites)
        group_months = unique_composites // 10000
        group_periods = (unique_composites % 10000) // 100
        group_tiers = unique_composites % 100

        # Indicator matrix: (8760, n_groups) — tiny since n_groups ≤ ~36
        indicator = np.zeros((n_hours, n_groups), dtype=np.float64)
        indicator[np.arange(n_hours), hour_group_ids] = 1.0

        row_indices = np.array([bldg_to_row[bid] for bid in bldg_ids_for_tariff])
        n_tariff_bldg = len(row_indices)

        # (n_tariff_bldg, 8760) @ (8760, n_groups) → (n_tariff_bldg, n_groups)
        energy_agg: dict[str, np.ndarray] = {}
        for col_name, col_2d in load_col_arrays.items():
            energy_agg[col_name] = col_2d[row_indices] @ indicator

        bids_expanded = np.repeat(np.asarray(bldg_ids_for_tariff), n_groups)
        month_expanded = np.tile(group_months, n_tariff_bldg)
        period_expanded = np.tile(group_periods, n_tariff_bldg)
        tier_expanded = np.tile(group_tiers, n_tariff_bldg)

        edf = pd.DataFrame(
            {
                "bldg_id": bids_expanded,
                "month": month_expanded,
                "period": period_expanded,
                "tier": tier_expanded,
                **{c: a.ravel() for c, a in energy_agg.items()},
                "charge_type": "energy_charge",
                "tariff": tariff_key,
            }
        )
        energy_parts.append(edf)

        if not is_gas and pv_col_arrays:
            solar_agg: dict[str, np.ndarray] = {}
            for col_name, col_2d in pv_col_arrays.items():
                solar_agg[col_name] = col_2d[row_indices] @ indicator

            sdf = pd.DataFrame(
                {
                    "bldg_id": bids_expanded.copy(),
                    "month": month_expanded.copy(),
                    "period": period_expanded.copy(),
                    "tier": tier_expanded.copy(),
                    **{c: a.ravel() for c, a in solar_agg.items()},
                    "charge_type": "solar_compensation",
                    "tariff": tariff_key,
                }
            )
            if "pv_generation" in pv_col_arrays:
                sdf = sdf.loc[sdf["pv_generation"] > 0.0]
            elif "net_exports" in pv_col_arrays:
                sdf = sdf.loc[sdf["net_exports"] > 0.0]
            solar_parts.append(sdf)

    all_energy = pd.concat(energy_parts, ignore_index=True)

    # Demand charge rows (vectorized)
    n_proto = len(prototype_ids)
    tariff_per_bldg = np.array([tariff_map_dict[bid] for bid in prototype_ids])
    demand_dict: dict[str, Any] = {
        "bldg_id": np.repeat(prototype_ids, 12),
        "month": np.tile(np.arange(1, 13), n_proto),
        "period": np.nan,
        "tier": np.nan,
        "charge_type": "demand_charge",
        "tariff": np.repeat(tariff_per_bldg, 12),
    }
    if not is_gas:
        demand_dict["grid_cons"] = np.nan
    for col in avail_load_cols:
        if col not in demand_dict:
            demand_dict[col] = np.nan
    demand_df = pd.DataFrame(demand_dict)

    combined = pd.concat([all_energy, demand_df], ignore_index=True).fillna(0.0)
    combined = combined.set_index("bldg_id")

    all_solar = (
        pd.concat(solar_parts, ignore_index=True)
        if solar_parts
        else pd.DataFrame(
            columns=["bldg_id", "month", "period", "tier"]
            + avail_pv_cols
            + ["charge_type", "tariff"]
        )
    )
    for col in ["net_exports", "self_cons", "pv_generation"]:
        if col not in all_solar.columns:
            all_solar[col] = np.nan
    all_solar = all_solar.set_index("bldg_id")

    _log_mem("demand_by_period done")

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
    prototype_ids: list[int] | None = None,
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
    log.info(
        "PATCH_CALL _vectorized_run_system_revenues process_agg_load=%s buildings=%s",
        process_agg_load,
        len(prototype_ids) if prototype_ids is not None else 0,
    )

    tariff_map_dict, tariff_dicts = tariff_funcs._load_base_tariffs(
        tariff_base=tariff_config,
        tariff_map=tariff_strategy,
        prototype_ids=prototype_ids,
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
        log.info(
            "PATCH_FALLBACK _vectorized_run_system_revenues reason=unsupported_features has_demand=%s has_solar=%s",
            has_demand,
            has_solar,
        )
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
            rate_rows.append(
                {
                    "tariff": tariff_key,
                    "period": float(row[0]),
                    "tier": float(row[1]),
                    "rate": float(row[4]) + float(row[5]),  # rate + adjustments
                }
            )
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
        log.info(
            "PATCH_FALLBACK _vectorized_run_system_revenues reason=process_agg_load_false"
        )
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

    if prototype_ids is None:
        log.info(
            "PATCH_FALLBACK _vectorized_run_system_revenues reason=prototype_ids_none"
        )
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
    energy_rows = aggregated_load[
        aggregated_load["charge_type"] == "energy_charge"
    ].copy()
    energy_rows = energy_rows.reset_index()  # bring bldg_id into columns

    # Merge energy rates (by tariff + period + tier)
    energy_rows = energy_rows.merge(
        rate_lookup, on=["tariff", "period", "tier"], how="left"
    )
    # For electricity: bill on grid_cons; for gas: bill on load_data (no grid_cons column)
    billing_col = "grid_cons" if "grid_cons" in energy_rows.columns else "load_data"
    energy_rows["costs"] = energy_rows[billing_col] * energy_rows["rate"]

    # Sum energy costs by (bldg_id, month)
    monthly_energy = energy_rows.groupby(["bldg_id", "month"], as_index=False)[
        "costs"
    ].sum()

    # Pivot to wide format: rows=bldg_id, columns=month (1..12)
    monthly_wide = monthly_energy.pivot(
        index="bldg_id", columns="month", values="costs"
    )
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
            monthly_wide.loc[bldg_id, :] = monthly_wide.loc[bldg_id, :].clip(
                lower=min_ch
            )

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
        raise TypeError(
            f"gas_tariff_map must be a DataFrame, got {type(gas_tariff_map)}"
        )

    tariff_map_dict, tariff_dicts = tariff_funcs._load_base_tariffs(
        tariff_base=gas_tariff_base,
        tariff_map=tariff_map_df,
        prototype_ids=prototype_ids,
    )

    # Build a rate lookup DataFrame from ur_ec_tou_mat across all tariffs.
    # ur_ec_tou_mat rows: (period, tier, max_usage, max_usage_units, rate, adjustments, ...)
    # Effective rate = rate + adjustments (matching CAIRO's calculate_energy_charges).
    rate_rows = []
    for tariff_key, td in tariff_dicts.items():
        tou_mat = td.get("ur_ec_tou_mat", [])
        for row in tou_mat:
            rate_rows.append(
                {
                    "tariff": tariff_key,
                    "period": float(row[0]),
                    "tier": float(row[1]),
                    "rate": float(row[4]) + float(row[5]),  # rate + adjustments
                }
            )
    rate_lookup = pd.DataFrame(rate_rows)

    # Filter to energy_charge rows; reset_index to bring bldg_id into columns
    energy_rows = aggregated_gas_load[
        aggregated_gas_load["charge_type"] == "energy_charge"
    ].copy()
    energy_rows = energy_rows.reset_index()  # bldg_id now a column

    # Merge rates on [tariff, period, tier]
    energy_rows = energy_rows.merge(
        rate_lookup, on=["tariff", "period", "tier"], how="left"
    )

    # Gas consumption is in load_data (therms); no grid_cons column
    energy_rows["costs"] = energy_rows["load_data"] * energy_rows["rate"]

    # Sum energy costs by (bldg_id, month)
    monthly_energy = energy_rows.groupby(["bldg_id", "month"], as_index=False)[
        "costs"
    ].sum()

    # Pivot to wide format: rows=bldg_id, columns=month (1..12)
    monthly_wide = monthly_energy.pivot(
        index="bldg_id", columns="month", values="costs"
    )
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
            monthly_wide.loc[bldg_id, :] = monthly_wide.loc[bldg_id, :].clip(
                lower=min_ch
            )

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
# Save the original BEFORE replacing, so fallback calls inside
# _vectorized_process_building_demand_by_period don't recurse into the patch.
_orig_process_building_demand_by_period = _cairo_loads.process_building_demand_by_period

_cairo_loads.process_building_demand_by_period = cast(
    Any, _vectorized_process_building_demand_by_period
)
log.info(
    "PATCH_APPLIED cairo.rates_tool.loads.process_building_demand_by_period -> %s",
    _vectorized_process_building_demand_by_period.__name__,
)

# Save the original BEFORE replacing, so fallback calls inside
# _vectorized_run_system_revenues don't recurse into the patch.
_orig_run_system_revenues = _cairo_sysrev.run_system_revenues

_cairo_sysrev.run_system_revenues = cast(Any, _vectorized_run_system_revenues)
log.info(
    "PATCH_APPLIED cairo.rates_tool.system_revenues.run_system_revenues -> %s",
    _vectorized_run_system_revenues.__name__,
)

# ---------------------------------------------------------------------------
# Phase 4b: monkey-patch _calculate_gas_bills on MeetRevenueSufficiencySystemWide
# ---------------------------------------------------------------------------
# Import _initialize_tariffs from systemsimulator (module-level function)
_cairo_initialize_tariffs = _cairo_sim._initialize_tariffs

_orig_calculate_gas_bills = (
    _cairo_sim.MeetRevenueSufficiencySystemWide._calculate_gas_bills
)


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
    log.info(
        "PATCH_CALL _patched_calculate_gas_bills target_year=%s buildings=%s",
        target_year,
        len(prototype_ids),
    )
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
        log.warning(
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


_cairo_sim.MeetRevenueSufficiencySystemWide._calculate_gas_bills = cast(
    Any, _patched_calculate_gas_bills
)
log.info(
    "PATCH_APPLIED MeetRevenueSufficiencySystemWide._calculate_gas_bills -> %s",
    _patched_calculate_gas_bills.__name__,
)

# ---------------------------------------------------------------------------
# Phase 5: memory-efficient process_residential_hourly_demand
# ---------------------------------------------------------------------------
# CAIRO's original does bldg_load.copy().reset_index().merge(weights, ...)
# which creates ~8 GB of temporaries for 15k buildings × 8760 hours.
# This replacement uses numpy reshape + broadcast to stay under ~2 GB.

_orig_process_residential_hourly_demand = _cairo_loads.process_residential_hourly_demand


def _patched_process_residential_hourly_demand(
    bldg_load: pd.DataFrame,
    sample_weights: pd.DataFrame,
) -> pd.Series:
    """Memory-efficient weighted hourly system load aggregation.

    Computes the same result as CAIRO's process_residential_hourly_demand
    (weighted sum of electricity_net across buildings for each hour) without
    copying the full DataFrame.
    """
    log.info(
        "PATCH_CALL _patched_process_residential_hourly_demand buildings=%s",
        bldg_load.index.get_level_values("bldg_id").nunique(),
    )
    _log_mem("before process_residential_hourly_demand")

    weights = sample_weights.set_index("bldg_id")["weight"]
    unique_bldgs = bldg_load.index.get_level_values("bldg_id").unique()
    n_bldgs = len(unique_bldgs)

    arr = bldg_load["electricity_net"].values.reshape(n_bldgs, 8760)
    w = weights.reindex(unique_bldgs).values
    hourly_sum = (arr * w[:, np.newaxis]).sum(axis=0)

    time_idx = bldg_load.index.get_level_values("time").unique()
    result = pd.Series(hourly_sum, index=time_idx, name="electricity_net")

    _log_mem("after process_residential_hourly_demand")
    return result


_cairo_loads.process_residential_hourly_demand = cast(
    Any, _patched_process_residential_hourly_demand
)
log.info(
    "PATCH_APPLIED cairo.rates_tool.loads.process_residential_hourly_demand -> %s",
    _patched_process_residential_hourly_demand.__name__,
)
