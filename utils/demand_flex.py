"""Demand-flex orchestration: load shifting, TOU recalibration, and RR recomputation.

This module encapsulates the multi-phase demand-flexibility workflow that
optionally runs inside a CAIRO scenario when elasticity != 0.  It is called
from run_scenario.py and delegates low-level per-building load shifting to
utils.cairo.apply_runtime_tou_demand_response.

See context/tools/cairo_demand_flexibility_workflow.md for design docs.
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
    _load_cambium_marginal_costs,
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

    Convention: strip ``_calibrated`` and ``_flex`` suffixes from the tariff key
    to get the base TOU name, then look for
    ``{tou_derivation_dir}/{base}_derivation.json``.
    """
    base = re.sub(r"_(calibrated|flex)", "", tariff_key)
    base = re.sub(r"__+", "_", base).strip("_")
    candidate = tou_derivation_dir / f"{base}_derivation.json"
    return candidate if candidate.exists() else None


def recompute_tou_precalc_mapping(
    precalc_mapping: pd.DataFrame,
    shifted_system_load_raw: pd.Series,
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
        shifted_system_load_raw: Pre-aggregated system-level hourly demand
            (weight x kWh summed across buildings, indexed by time).
    """
    combined_mc_raw = combine_marginal_costs(
        bulk_marginal_costs, dist_and_sub_tx_marginal_costs
    )

    mc_index = pd.DatetimeIndex(combined_mc_raw.index)
    combined_mc = pd.Series(
        combined_mc_raw.values, index=mc_index, name="total_mc_per_kwh"
    )
    shifted_system_load = pd.Series(
        shifted_system_load_raw.values[: len(mc_index)],
        index=mc_index,
        name="system_load",
    )

    updated = precalc_mapping.copy()

    for tou_key, specs in tou_season_specs.items():
        avg_base_rate = sum(spec.base_rate for spec in specs) / len(specs)

        new_season_rates: dict[str, float] = {}
        new_ratios: dict[str, float] = {}

        for spec in specs:
            mask = season_mask(mc_index, spec.season)
            mc_season = combined_mc[mask]
            load_season = shifted_system_load[mask]

            season_mc_total = float((mc_season * load_season).sum())
            if abs(season_mc_total) < 1e-12:
                log.info(
                    ".... Season %s has zero MC; using flat ratio 1.0",
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

        total_load_all = float(shifted_system_load.sum())
        raw_weighted = sum(
            new_season_rates[spec.season.name]
            * float(shifted_system_load[season_mask(mc_index, spec.season)].sum())
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
# Main orchestrator
# ---------------------------------------------------------------------------


def apply_demand_flex(
    *,
    elasticity: float,
    run_type: str,
    year_run: int,
    add_supply_revenue_requirement: bool,
    path_tariffs_electric: dict[str, Path],
    path_tou_supply_mc: str | Path | None,
    tou_derivation_dir: Path,
    raw_load_elec: pd.DataFrame,
    customer_metadata: pd.DataFrame,
    tariff_map_df: pd.DataFrame,
    precalc_mapping: pd.DataFrame,
    rr_total: float,
    bulk_marginal_costs: pd.DataFrame,
    dist_and_sub_tx_marginal_costs: pd.Series,
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
        ".... Demand flex enabled (elasticity=%.4f); detected %d TOU tariff(s): %s",
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
        delivery_only_rev_req_passed=add_supply_revenue_requirement,
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

    # -- Phase 1.5: shift TOU customers (per TOU tariff) --
    effective_load_elec = raw_load_elec
    elasticity_tracker = pd.DataFrame()
    tou_season_specs: dict[str, list[SeasonTouSpec]] = {}

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
        )
        if elasticity_tracker.empty:
            elasticity_tracker = tracker
        else:
            elasticity_tracker = pd.concat([elasticity_tracker, tracker], axis=0)

    # -- Phase 1.75: recompute TOU cost-causation ratios from shifted load --
    updated_precalc = precalc_mapping
    if tou_season_specs and run_type == "precalc":
        if path_tou_supply_mc is not None:
            tou_bulk_mc = _load_cambium_marginal_costs(path_tou_supply_mc, year_run)
            log.info(
                ".... Phase 1.75: using real supply MC from %s",
                path_tou_supply_mc,
            )
        else:
            tou_bulk_mc = bulk_marginal_costs
            log.info(".... Phase 1.75: using scenario bulk MC (no path_tou_supply_mc)")

        log.info(".... Phase 1.75: recomputing TOU precalc mapping from shifted load")
        sample_weights = customer_metadata[["bldg_id", "weight"]]
        shifted_weighted = effective_load_elec.reset_index().merge(
            sample_weights, on="bldg_id"
        )
        shifted_weighted["electricity_net"] *= shifted_weighted["weight"]
        shifted_system_load = shifted_weighted.groupby("time")["electricity_net"].sum()

        updated_precalc = recompute_tou_precalc_mapping(
            precalc_mapping=precalc_mapping,
            shifted_system_load_raw=shifted_system_load,
            bulk_marginal_costs=tou_bulk_mc,
            distribution_marginal_costs=dist_and_sub_tx_marginal_costs,
            tou_season_specs=tou_season_specs,
        )

    # -- Phase 2: new_RR = MC_shifted + frozen_residual --
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

    return DemandFlexResult(
        effective_load_elec=effective_load_elec,
        elasticity_tracker=elasticity_tracker,
        revenue_requirement_raw=revenue_requirement_raw,
        marginal_system_prices=marginal_system_prices,
        marginal_system_costs=marginal_system_costs,
        costs_by_type=costs_by_type,
        precalc_mapping=updated_precalc,
        tou_tariff_keys=tou_tariff_keys,
    )
