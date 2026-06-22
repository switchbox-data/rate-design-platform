"""Demand-flex orchestration: load shifting, TOU recalibration, and RR recomputation.

This module encapsulates the multi-phase demand-flexibility workflow that
optionally runs inside a CAIRO scenario when elasticity != 0.  It is called
from run_scenario.py and delegates low-level per-building load shifting to
utils.cairo.apply_runtime_tou_demand_response.

See context/code/cairo/cairo_demand_flexibility_workflow.md for design docs.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd
from cairo.rates_tool.systemsimulator import _return_revenue_requirement_target

from utils.cairo import (
    _load_supply_marginal_costs,
    _log_rss,
    apply_runtime_tou_demand_response,
)
from utils.pre.compute_tou import (
    SeasonTouSpec,
    combine_marginal_costs,
    compute_tou_cost_causation_ratio,
    load_season_specs,
    season_mask,
)

log = logging.getLogger("rates_analysis").getChild("demand_flex")


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class DemandFlexResult:
    """Outputs of the demand-flex pipeline consumed by the rest of run()."""

    effective_load_elec: pd.DataFrame
    elasticity_tracker: pd.DataFrame
    revenue_requirement_raw: float
    marginal_system_prices: Any
    marginal_system_costs: Any
    costs_by_type: Any
    precalc_mapping: pd.DataFrame
    tou_tariff_keys: list[str]
    mc_delta_by_tou_tariff: dict[str, float]


# ---------------------------------------------------------------------------
# Helpers (moved from run_scenario.py)
# ---------------------------------------------------------------------------


def is_diurnal_tou(tariff_path: Path) -> bool:
    """Return True if the URDB tariff has rate variation within any day.

    A tariff is diurnal TOU when any row (month) in energyweekdayschedule or
    energyweekendschedule has more than one distinct period value — meaning the
    rate changes within a single day.
    """
    with open(tariff_path) as f:
        data = json.load(f)
    item = data["items"][0]
    for sched_key in ("energyweekdayschedule", "energyweekendschedule"):
        for row in item.get(sched_key, []):
            if len(set(row)) > 1:
                return True
    return False


def find_tou_derivation_path(tariff_key: str, tou_derivation_dir: Path) -> Path | None:
    """Find the TOU derivation JSON for a tariff key, if one exists.

    *tariff_key* is normally the tariff JSON **stem** (not the YAML selector key
    ``all`` / ``hp``): :func:`utils.scenario_config._parse_path_tariffs` rekeys
    ``path_tariffs_electric`` by ``path.stem`` before :func:`apply_demand_flex`
    runs. Strip trailing ``_calibrated`` and ``_flex`` from that stem, then look
    for ``{tou_derivation_dir}/{base}_derivation.json``.

    Supply tariff stems often end in ``…_flex_supply_calibrated``, which strips
    to ``…_supply``. There is typically **no** ``{utility}_…_supply_derivation.json``;
    in that case this returns ``None`` and the caller skips
    :func:`load_season_specs`. **Do not** fall back to the delivery derivation:
    applying delivery winter/summer slices while shifting against **supply** TOU
    rates can explode ``bill_level`` on supply runs.
    """
    base = re.sub(r"_(calibrated|flex)", "", tariff_key)
    base = re.sub(r"__+", "_", base).strip("_")
    candidate = tou_derivation_dir / f"{base}_derivation.json"
    return candidate if candidate.exists() else None


def recompute_tou_precalc_mapping(
    precalc_mapping: pd.DataFrame,
    shifted_load_raw: pd.Series,
    bulk_marginal_costs: pd.DataFrame,
    dist_and_sub_tx_marginal_costs: pd.Series,
    tou_season_specs: dict[str, list[SeasonTouSpec]],
) -> pd.DataFrame:
    """Recompute precalc rel_values for TOU tariffs using shifted-load MC weights.

    After demand flex shifts load from peak to off-peak, the demand-weighted
    marginal cost profile changes.  This function recomputes the per-season
    peak/off-peak cost-causation ratios and seasonal base rates from the
    *shifted* system load, then updates the precalc_mapping rel_values so
    that CAIRO calibrates rate ratios that reflect post-flex MC responsibility.

    Non-TOU tariff entries in the mapping are left unchanged.

    Args:
        shifted_load_raw: Pre-aggregated hourly demand
            (weight x kWh summed across buildings, indexed by time).
    """
    combined_mc_raw = combine_marginal_costs(
        bulk_marginal_costs, dist_and_sub_tx_marginal_costs
    )

    mc_index = pd.DatetimeIndex(combined_mc_raw.index)
    combined_mc = pd.Series(
        combined_mc_raw.values, index=mc_index, name="total_mc_per_kwh"
    )
    shifted_load = pd.Series(
        shifted_load_raw.values[: len(mc_index)],
        index=mc_index,
        name="load",
    )

    updated = precalc_mapping.copy()

    if len(tou_season_specs) > 1:
        log.warning(
            ".... Phase 1.75 received %d TOU tariffs but only one aggregated "
            "shifted TOU load curve. Recomputed rel_values will reuse the same "
            "class weights across tariffs; this is only theory-consistent for "
            "a single treated TOU class.",
            len(tou_season_specs),
        )

    for tou_key, specs in tou_season_specs.items():
        avg_base_rate = sum(spec.base_rate for spec in specs) / len(specs)

        new_season_rates: dict[str, float] = {}
        new_ratios: dict[str, float] = {}

        for spec in specs:
            mask = season_mask(mc_index, spec.season)
            mc_season = combined_mc[mask]
            load_season = shifted_load[mask]

            season_mc_total = float((mc_season * load_season).sum())
            if abs(season_mc_total) < 1e-12:
                log.warning(
                    ".... Season %s has zero combined MC during Phase 1.75 "
                    "recalibration; defaulting to flat ratio 1.0 and flat "
                    "seasonal base rate. Original derivation values are not "
                    "preserved in this fallback.",
                    spec.season.name,
                )
                new_season_rates[spec.season.name] = 1.0
                new_ratios[spec.season.name] = 1.0
            else:
                total_load = float(load_season.sum())
                dw_avg = season_mc_total / total_load if total_load > 0 else 0.0
                new_season_rates[spec.season.name] = dw_avg
                new_ratios[spec.season.name] = compute_tou_cost_causation_ratio(
                    mc_season,
                    load_season,
                    spec.peak_hours,
                )

        total_load_all = float(shifted_load.sum())
        raw_weighted = sum(
            new_season_rates[spec.season.name]
            * float(shifted_load[season_mask(mc_index, spec.season)].sum())
            for spec in specs
        )
        scale = (
            avg_base_rate * total_load_all / raw_weighted if raw_weighted != 0 else 1.0
        )
        new_season_rates = {k: v * scale for k, v in new_season_rates.items()}

        new_rates: list[float] = []
        for spec in specs:
            offpeak = new_season_rates[spec.season.name]
            peak = offpeak * new_ratios[spec.season.name]
            new_rates.append(offpeak)
            new_rates.append(peak)

        min_rate = min(new_rates) if new_rates else 1.0

        tariff_mask = updated["tariff"] == tou_key
        for period_idx, rate in enumerate(new_rates, start=1):
            period_mask = tariff_mask & (updated["period"] == period_idx)
            updated.loc[period_mask, "rel_value"] = round(rate / min_rate, 4)

        log.info(
            ".... Recomputed TOU precalc mapping for %s: "
            "ratios=%s, rates=%s, rel_values=%s",
            tou_key,
            {k: round(v, 4) for k, v in new_ratios.items()},
            [round(r, 6) for r in new_rates],
            [round(r / min_rate, 4) for r in new_rates],
        )

    return updated


# ---------------------------------------------------------------------------
# Per-TOU-subclass MC helper
# ---------------------------------------------------------------------------


def _compute_tou_subclass_mc(
    load_elec: pd.DataFrame,
    tou_bldg_ids: set[int],
    customer_metadata: pd.DataFrame,
    mc_prices: pd.Series,
) -> float:
    """Compute the weighted annual marginal cost for one TOU subclass.

    Filters *load_elec* to *tou_bldg_ids*, weights by building sample weight,
    aggregates to an hourly system load, and sums load × MC over the year.
    The MC price series index is used for alignment (truncating the load to
    match its length, consistent with how Phase 1.75 handles alignment).
    """
    sample_weights = customer_metadata[["bldg_id", "weight"]]
    bldg_level = load_elec.index.get_level_values("bldg_id")
    sub = load_elec.loc[bldg_level.isin(tou_bldg_ids)]
    weighted = sub.reset_index().merge(sample_weights, on="bldg_id")
    weighted["electricity_net"] = weighted["electricity_net"] * weighted["weight"]
    sys_load_raw = weighted.groupby("time")["electricity_net"].sum()
    sys_load = pd.Series(
        sys_load_raw.values[: len(mc_prices)],
        index=pd.DatetimeIndex(mc_prices.index),
    )
    return float((mc_prices * sys_load).sum())


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def apply_demand_flex(
    *,
    elasticity: float | dict[str, float],
    run_type: str,
    year_run: int,
    path_tariffs_electric: dict[str, Path],
    tou_derivation_dir: Path,
    raw_load_elec: pd.DataFrame,
    customer_metadata: pd.DataFrame,
    tariff_map_df: pd.DataFrame,
    precalc_mapping: pd.DataFrame,
    rr_total: float,
    bulk_marginal_costs: pd.DataFrame,
    dist_and_sub_tx_marginal_costs: pd.Series,
    path_tou_supply_energy_mc: str | Path | None = None,
    path_tou_supply_capacity_mc: str | Path | None = None,
    run_includes_subclasses: bool = False,
) -> DemandFlexResult:
    """Run the full demand-flex pipeline (phases 1a, 1.5, 1.75, 2).

    Returns a DemandFlexResult with shifted loads, updated precalc mapping,
    recomputed revenue requirement, and marginal cost outputs.
    """
    # Identify which tariffs are diurnal TOU
    tou_tariff_keys = [
        key for key, path in path_tariffs_electric.items() if is_diurnal_tou(path)
    ]
    if not tou_tariff_keys:
        raise ValueError(
            f"elasticity={elasticity} (demand flex enabled) but no "
            f"diurnal TOU tariffs found in path_tariffs_electric "
            f"keys={sorted(path_tariffs_electric)}"
        )
    log.info(
        ".... Demand flex enabled (elasticity=%s); detected %d TOU tariff(s): %s",
        elasticity,
        len(tou_tariff_keys),
        tou_tariff_keys,
    )

    if (
        "bldg_id" not in tariff_map_df.columns
        or "tariff_key" not in tariff_map_df.columns
    ):
        raise ValueError(
            "Electric tariff map must include 'bldg_id' and 'tariff_key' columns"
        )

    # -- Phase 1a: freeze residual from original loads --
    # Run the RR decomposition exactly as we would without demand flex:
    # same raw loads, same MC prices → same total MC, same residual.
    # We capture the residual here and freeze it so that when loads shift
    # later, only the MC component changes — the residual (embedded infra
    # costs) stays fixed. See context/code/cairo/demand_flex_residual_treatment.md.
    _log_rss("apply_demand_flex: before Phase 1a")
    log.info(".... Phase 1a: computing frozen residual from original loads")
    (
        full_rr_orig,
        _msp_orig,
        _msc_orig,
        costs_by_type_orig,
    ) = _return_revenue_requirement_target(
        building_load=raw_load_elec,
        sample_weight=customer_metadata[["bldg_id", "weight"]],
        revenue_requirement_target=rr_total,
        residual_cost=None,
        residual_cost_frac=None,
        bulk_marginal_costs=bulk_marginal_costs,
        distribution_marginal_costs=dist_and_sub_tx_marginal_costs,
        low_income_strategy=None,
    )
    total_mc_orig = float(costs_by_type_orig["Total Marginal Costs ($)"])
    frozen_residual: float = float(full_rr_orig) - total_mc_orig
    log.info(
        ".... Frozen residual from original loads: $%.2f "
        "(full_RR_orig=$%.2f, MC_original=$%.2f)",
        frozen_residual,
        float(full_rr_orig),
        total_mc_orig,
    )
    del full_rr_orig, _msp_orig, _msc_orig, costs_by_type_orig
    _log_rss("apply_demand_flex: after Phase 1a del")

    # -- Phase 1.5: shift TOU customers (per TOU tariff) --
    # Now simulate what happens when customers actually respond to the TOU
    # price signal: shift load away from peak hours based on the elasticity.
    # This changes the load shapes, which will change total MC downstream.
    elasticity_tracker = pd.DataFrame()
    tou_season_specs: dict[str, list[SeasonTouSpec]] = {}
    all_tou_bldg_ids: set[int] = set()

    # Precompute per-TOU-key weighted system loads from the original
    # (pre-shift) loads.  Phase 2.5 needs these to compute mc_orig_k,
    # but only as tiny 8760-element Series — not the full load DataFrame.
    # By capturing them now we can free raw_load_elec before the shift.
    # Use weight-map multiplication to avoid DataFrame copies that would
    # double memory for large utilities (e.g. ConEd with ~15k buildings).
    weight_map = customer_metadata.set_index("bldg_id")["weight"]
    bldg_level = raw_load_elec.index.get_level_values("bldg_id")
    time_level = raw_load_elec.index.get_level_values("time")
    orig_sys_loads: dict[str, pd.Series] = {}
    for tou_key in tou_tariff_keys:
        tou_rows = tariff_map_df[tariff_map_df["tariff_key"] == tou_key]
        tou_bldg_ids = cast(list[int], tou_rows["bldg_id"].astype(int).tolist())
        all_tou_bldg_ids.update(tou_bldg_ids)
        mask = bldg_level.isin(set(tou_bldg_ids))
        weights = bldg_level[mask].map(weight_map)
        weighted_load = raw_load_elec.loc[mask, "electricity_net"] * weights.values
        orig_sys_loads[tou_key] = weighted_load.groupby(time_level[mask]).sum()
        del mask, weights, weighted_load

    # Make one copy; shifts write in-place.  The original raw_load_elec
    # (caller's reference) is no longer needed inside this function.
    effective_load_elec = raw_load_elec.copy()
    del raw_load_elec
    _log_rss("apply_demand_flex: after copy + del raw_load_elec")

    # A scenario may have multiple TOU tariffs (e.g. HP TOU + seasonal TOU),
    # each with its own rate structure and set of assigned buildings.
    for tou_key in tou_tariff_keys:
        tou_tariff_path = path_tariffs_electric[tou_key]
        with open(tou_tariff_path) as f:
            tou_tariff = json.load(f)

        tou_rows = tariff_map_df[tariff_map_df["tariff_key"] == tou_key]
        tou_bldg_ids = cast(list[int], tou_rows["bldg_id"].astype(int).tolist())

        derivation_path = find_tou_derivation_path(tou_key, tou_derivation_dir)
        season_specs = None
        if derivation_path is not None:
            season_specs = load_season_specs(derivation_path)
            tou_season_specs[tou_key] = season_specs
            log.info(".... Using TOU derivation spec: %s", derivation_path.name)

        log.info(
            ".... Phase 1.5: applying demand response to %d bldgs on tariff %s",
            len(tou_bldg_ids),
            tou_key,
        )
        effective_load_elec, tracker = apply_runtime_tou_demand_response(
            raw_load_elec=effective_load_elec,
            tou_bldg_ids=tou_bldg_ids,
            tou_tariff=tou_tariff,
            demand_elasticity=elasticity,
            season_specs=season_specs,
            inplace=True,
        )
        if elasticity_tracker.empty:
            elasticity_tracker = tracker
        else:
            elasticity_tracker = pd.concat([elasticity_tracker, tracker], axis=0)

    # -- Phase 1.75: recompute TOU peak/off-peak cost-causation ratios --
    # The TOU ratio (peak rate / off-peak rate) was derived from the original
    # load shape. Now that loads have shifted, the cost per kWh in each period
    # has changed — less load in peak means a lower demand-weighted peak MC,
    # so the old ratio overstates the true peak/off-peak cost difference.
    # Recompute it from the shifted loads to keep the tariff cost-reflective.
    # See context/methods/tou_and_rates/cost_reflective_tou_rate_design.md § "Demand flexibility".
    # Only applies to precalc (which calibrates tariff structure); default
    # runs use a pre-calibrated tariff and skip this.
    updated_precalc = precalc_mapping
    if tou_season_specs and run_type == "precalc":
        # TOU cost-causation must always use real (non-zero) bulk supply MCs so that
        # delivery-only and supply runs share identical TOU windows and peak/off-peak
        # ratios. If the Justfile-level real supply MC paths were passed in (via CLI),
        # load them specifically for this step; otherwise fall back to the scenario's
        # already-loaded bulk_marginal_costs (correct for supply runs).
        tou_bulk_mc = bulk_marginal_costs
        if (
            path_tou_supply_energy_mc is not None
            and path_tou_supply_capacity_mc is not None
        ):
            log.info(
                ".... Phase 1.75: loading real supply MC for TOU recalibration "
                "(energy=%s, capacity=%s)",
                path_tou_supply_energy_mc,
                path_tou_supply_capacity_mc,
            )
            tou_bulk_mc = _load_supply_marginal_costs(
                path_tou_supply_energy_mc,
                path_tou_supply_capacity_mc,
                target_year=year_run,
            )

        log.info(".... Phase 1.75: recomputing TOU precalc mapping from shifted load")
        # Aggregate only TOU-assigned buildings (the HP class) into the
        # hourly load curve used for cost-causation ratio recomputation.
        # The welfare derivation proves HP demand is the correct weight
        # for the HP tariff — non-HP terms vanish because those customers
        # face a flat rate, not the TOU price.
        sample_weights = customer_metadata[["bldg_id", "weight"]]
        bldg_level = effective_load_elec.index.get_level_values("bldg_id")
        tou_load = effective_load_elec.loc[bldg_level.isin(all_tou_bldg_ids)]
        shifted_weighted = tou_load.reset_index().merge(sample_weights, on="bldg_id")
        shifted_weighted["electricity_net"] *= shifted_weighted["weight"]
        shifted_load = shifted_weighted.groupby("time")["electricity_net"].sum()
        log.info(
            ".... Phase 1.75: using %d TOU buildings (of %d total) for load aggregation",
            len(all_tou_bldg_ids),
            bldg_level.nunique(),
        )

        updated_precalc = recompute_tou_precalc_mapping(
            precalc_mapping=precalc_mapping,
            shifted_load_raw=shifted_load,
            bulk_marginal_costs=tou_bulk_mc,
            dist_and_sub_tx_marginal_costs=dist_and_sub_tx_marginal_costs,
            tou_season_specs=tou_season_specs,
        )

    # -- Phase 2: new_RR = MC_shifted + frozen_residual --
    # Re-run the decomposition on the shifted loads. Total MC is now lower
    # (less peak load × expensive prices), but the residual is the same one
    # we froze in Phase 1a. The new RR = lower MC + same residual, so the
    # MC savings from load shifting flow through as a lower total RR.
    log.info(".... Phase 2: recomputing RR with shifted loads + frozen residual")
    (
        _rr_shifted,
        marginal_system_prices,
        marginal_system_costs,
        costs_by_type,
    ) = _return_revenue_requirement_target(
        building_load=effective_load_elec,
        sample_weight=customer_metadata[["bldg_id", "weight"]],
        revenue_requirement_target=None,
        residual_cost=frozen_residual,
        residual_cost_frac=None,
        bulk_marginal_costs=bulk_marginal_costs,
        distribution_marginal_costs=dist_and_sub_tx_marginal_costs,
        low_income_strategy=None,
        delivery_only_rev_req_passed=False,
    )
    revenue_requirement_raw = float(costs_by_type["Total System Costs ($)"])
    log.info(
        ".... Recalibrated RR=$%.2f  (MC_shifted=$%.2f + frozen_residual=$%.2f)",
        revenue_requirement_raw,
        float(costs_by_type["Total Marginal Costs ($)"]),
        frozen_residual,
    )

    # -- Phase 2.5: per-TOU-subclass MC delta for revenue requirement splitting --
    # Only needed when the run has HP/non-HP subclasses; run_scenario.py guards
    # consumption of mc_delta_by_tou_tariff behind run_includes_subclasses too.
    mc_delta_by_tou_tariff: dict[str, float] = {}
    if run_includes_subclasses:
        # For each TOU subclass independently:
        #   frozen_residual_k = subclass_RR_k - subclass_MC_orig_k
        #   new_RR_k = subclass_MC_shifted_k + frozen_residual_k
        #            = subclass_RR_k + (MC_shifted_k - MC_orig_k)
        # So the MC delta is all run_scenario.py needs to build per-subclass RRs.
        # mc_orig_k uses the precomputed original system loads (tiny 8760-row
        # Series captured before the shift), avoiding the need to keep the full
        # original load DataFrame alive.
        mc_prices = marginal_system_prices["Total Marginal Costs ($/kWh)"]
        for tou_key in tou_tariff_keys:
            tou_bldg_ids_k = set(
                tariff_map_df[tariff_map_df["tariff_key"] == tou_key]["bldg_id"]
                .astype(int)
                .tolist()
            )
            sys_load_orig = orig_sys_loads[tou_key]
            sys_load_aligned = pd.Series(
                sys_load_orig.values[: len(mc_prices)],
                index=pd.DatetimeIndex(mc_prices.index),
            )
            mc_orig_k = float((mc_prices * sys_load_aligned).sum())
            mc_shifted_k = _compute_tou_subclass_mc(
                effective_load_elec, tou_bldg_ids_k, customer_metadata, mc_prices
            )
            mc_delta_by_tou_tariff[tou_key] = mc_shifted_k - mc_orig_k
            log.info(
                ".... Subclass MC delta for %s: $%.2f  (orig=$%.2f → shifted=$%.2f)",
                tou_key,
                mc_delta_by_tou_tariff[tou_key],
                mc_orig_k,
                mc_shifted_k,
            )
    else:
        log.info(".... Phase 2.5 skipped (run_includes_subclasses=False)")

    return DemandFlexResult(
        effective_load_elec=effective_load_elec,
        elasticity_tracker=elasticity_tracker,
        revenue_requirement_raw=revenue_requirement_raw,
        marginal_system_prices=marginal_system_prices,
        marginal_system_costs=marginal_system_costs,
        costs_by_type=costs_by_type,
        precalc_mapping=updated_precalc,
        tou_tariff_keys=tou_tariff_keys,
        mc_delta_by_tou_tariff=mc_delta_by_tou_tariff,
    )
