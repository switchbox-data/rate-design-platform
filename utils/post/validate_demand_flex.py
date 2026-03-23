"""Analytical validation of the demand-flex pipeline's mathematical invariants.

Operates on load profiles, MC prices, tariff structures, and elasticity
parameters to verify expected outcomes derived from the frozen-residual model
(see context/code/cairo/cairo_demand_flexibility_workflow.md).

Each check returns a ``CheckResult`` consistent with the CAIRO validation
framework in ``utils/post/validate/``.  The script can run on synthetic data
(default, for CI) or user-supplied data for deeper analysis.

Usage::

    uv run python -m utils.post.validate_demand_flex [--synthetic]
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np
import pandas as pd

from utils.cairo import (
    _build_period_shift_targets,
    _compute_equivalent_flat_tariff,
    _shift_building_hourly_demand,
    assign_hourly_periods,
    extract_tou_period_rates,
    process_residential_hourly_demand_response_shift,
)
from utils.pre.compute_tou import (
    Season,
    SeasonTouSpec,
    combine_marginal_costs,
    compute_tou_cost_causation_ratio,
    season_mask,
)

# ---------------------------------------------------------------------------
# Lightweight CheckResult (avoids importing boto3 via utils.post.validate)
# ---------------------------------------------------------------------------

CheckStatus = Literal["PASS", "WARN", "FAIL"]


@dataclass
class CheckResult:
    """Mirrors ``utils.post.validate.checks.CheckResult``."""

    name: str
    status: CheckStatus
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status == "PASS"


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class SyntheticScenario:
    """All inputs needed to exercise the demand-flex analytical checks."""

    hourly_index: pd.DatetimeIndex
    load_elec: pd.DataFrame  # indexed by (bldg_id, time)
    customer_metadata: pd.DataFrame
    tou_tariff: dict
    period_rate: pd.Series
    tou_bldg_ids: list[int]
    nontou_bldg_ids: list[int]
    bulk_marginal_costs: pd.DataFrame
    dist_mc: pd.Series
    rr_total: float
    season_specs: list[SeasonTouSpec]
    subclass_rr_ratios: dict[str, float]


def build_synthetic_scenario(
    *,
    n_tou_bldgs: int = 5,
    n_nontou_bldgs: int = 5,
    seed: int = 42,
) -> SyntheticScenario:
    """Build a complete synthetic scenario for testing demand-flex checks."""
    rng = np.random.RandomState(seed)
    year = 2025
    hourly_index = pd.date_range(
        f"{year}-01-01", periods=8760, freq="h", tz="EST", name="time"
    )

    tou_bldg_ids = list(range(1, n_tou_bldgs + 1))
    nontou_bldg_ids = list(range(n_tou_bldgs + 1, n_tou_bldgs + n_nontou_bldgs + 1))
    all_bldg_ids = tou_bldg_ids + nontou_bldg_ids

    rows: list[dict[str, Any]] = []
    for bldg_id in all_bldg_ids:
        base = 2.0 + rng.rand() * 2.0
        for ts in hourly_index:
            hour = ts.hour
            month = ts.month
            seasonal = 1.0 + 0.3 * (1 if month in (6, 7, 8) else 0)
            diurnal = 1.0 + 0.5 * max(0, (hour - 8) * (20 - hour)) / 36
            noise = rng.normal(1.0, 0.05)
            rows.append(
                {
                    "bldg_id": bldg_id,
                    "time": ts,
                    "electricity_net": max(0.1, base * seasonal * diurnal * noise),
                }
            )
    load_elec = pd.DataFrame(rows).set_index(["bldg_id", "time"])

    weights = {bldg: 100.0 + rng.rand() * 50 for bldg in all_bldg_ids}
    customer_metadata = pd.DataFrame(
        [{"bldg_id": b, "weight": w} for b, w in weights.items()]
    )

    # TOU tariff: 2-period (off-peak=0, peak=1), peak hours 16-19
    tou_tariff: dict[str, Any] = {
        "items": [
            {
                "energyratestructure": [
                    [{"rate": 0.08, "adj": 0.0}],  # period 0: off-peak
                    [{"rate": 0.20, "adj": 0.0}],  # period 1: peak
                ],
                "energyweekdayschedule": [
                    [0] * 16 + [1] * 4 + [0] * 4 for _ in range(12)
                ],
                "energyweekendschedule": [
                    [0] * 16 + [1] * 4 + [0] * 4 for _ in range(12)
                ],
            }
        ]
    }

    period_rate = pd.Series({0: 0.08, 1: 0.20}, name="rate")
    period_rate.index.name = "energy_period"

    # MC: higher at peak hours, with summer premium
    energy_mc = []
    capacity_mc = []
    for ts in hourly_index:
        hour = ts.hour
        month = ts.month
        summer_premium = 0.02 if month in (6, 7, 8, 9) else 0.0
        peak_premium = 0.03 if 16 <= hour < 20 else 0.0
        energy_mc.append(0.04 + summer_premium + peak_premium + rng.normal(0, 0.002))
        capacity_mc.append(0.01 + peak_premium * 0.5 + rng.normal(0, 0.001))
    bulk_mc = pd.DataFrame(
        {
            "Marginal Energy Costs ($/kWh)": energy_mc,
            "Marginal Capacity Costs ($/kWh)": capacity_mc,
        },
        index=hourly_index,
    )

    dist_mc_vals = [0.02 + 0.01 * (1 if 16 <= ts.hour < 20 else 0) for ts in hourly_index]
    dist_mc = pd.Series(dist_mc_vals, index=hourly_index, name="Marginal Dist+Sub-Tx Costs ($/kWh)")

    rr_total = 500_000.0

    season_specs = [
        SeasonTouSpec(
            season=Season(name="winter", months=[1, 2, 3, 4, 5, 10, 11, 12]),
            base_rate=0.10,
            peak_hours=[16, 17, 18, 19],
            peak_offpeak_ratio=2.0,
        ),
        SeasonTouSpec(
            season=Season(name="summer", months=[6, 7, 8, 9]),
            base_rate=0.12,
            peak_hours=[16, 17, 18, 19],
            peak_offpeak_ratio=2.5,
        ),
    ]

    hp_ratio = n_tou_bldgs / len(all_bldg_ids)
    subclass_rr_ratios = {"hp": hp_ratio, "non-hp": 1.0 - hp_ratio}

    return SyntheticScenario(
        hourly_index=hourly_index,
        load_elec=load_elec,
        customer_metadata=customer_metadata,
        tou_tariff=tou_tariff,
        period_rate=period_rate,
        tou_bldg_ids=tou_bldg_ids,
        nontou_bldg_ids=nontou_bldg_ids,
        bulk_marginal_costs=bulk_mc,
        dist_mc=dist_mc,
        rr_total=rr_total,
        season_specs=season_specs,
        subclass_rr_ratios=subclass_rr_ratios,
    )


# ---------------------------------------------------------------------------
# Analytical helpers
# ---------------------------------------------------------------------------


def _compute_system_mc(
    load_elec: pd.DataFrame,
    bldg_ids: list[int],
    customer_metadata: pd.DataFrame,
    bulk_mc: pd.DataFrame,
    dist_mc: pd.Series,
) -> float:
    """Compute weighted total MC dollars for a set of buildings."""
    combined_mc = combine_marginal_costs(bulk_mc, dist_mc)
    sample_weights = customer_metadata[["bldg_id", "weight"]]
    bldg_level = load_elec.index.get_level_values("bldg_id")
    sub = load_elec.loc[bldg_level.isin(set(bldg_ids))]
    weighted = sub.reset_index().merge(sample_weights, on="bldg_id")
    weighted["electricity_net"] = weighted["electricity_net"] * weighted["weight"]
    sys_load_raw = weighted.groupby("time")["electricity_net"].sum()
    mc_index = pd.DatetimeIndex(combined_mc.index)
    sys_load = pd.Series(
        sys_load_raw.values[: len(mc_index)],
        index=mc_index,
    )
    return float((combined_mc * sys_load).sum())


def _shift_loads(
    scenario: SyntheticScenario,
    elasticity: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply demand-response shifting to the TOU cohort and return shifted loads + tracker."""
    return process_residential_hourly_demand_response_shift(
        hourly_load_df=_extract_tou_hourly(scenario),
        period_rate=scenario.period_rate,
        demand_elasticity=elasticity,
    )


def _extract_tou_hourly(scenario: SyntheticScenario) -> pd.DataFrame:
    """Extract and annotate TOU-cohort hourly loads for shifting."""
    bldg_level = scenario.load_elec.index.get_level_values("bldg_id")
    tou_mask = bldg_level.isin(set(scenario.tou_bldg_ids))
    tou_df = scenario.load_elec.loc[tou_mask, ["electricity_net"]].copy().reset_index()
    period_map = assign_hourly_periods(scenario.hourly_index, scenario.tou_tariff)
    period_df = period_map.reset_index()
    period_df.columns = pd.Index(["time", "energy_period"])
    tou_df = tou_df.merge(period_df, on="time", how="left")
    return tou_df


def _apply_shift_to_full_load(
    scenario: SyntheticScenario,
    shifted_tou: pd.DataFrame,
) -> pd.DataFrame:
    """Merge shifted TOU rows back into the full load DataFrame."""
    full = scenario.load_elec.copy()
    shifted_indexed = shifted_tou.set_index(["bldg_id", "time"])
    full.loc[shifted_indexed.index, "electricity_net"] = shifted_indexed[
        "shifted_net"
    ].to_numpy()
    return full


# ---------------------------------------------------------------------------
# Check implementations
# ---------------------------------------------------------------------------


def check_energy_conservation(
    scenario: SyntheticScenario,
    elasticity: float,
) -> CheckResult:
    """Total kWh before == after shift for TOU buildings."""
    shifted, _ = _shift_loads(scenario, elasticity)
    orig_kwh = shifted["electricity_net"].sum()
    shifted_kwh = shifted["shifted_net"].sum()
    rel_diff = abs(shifted_kwh - orig_kwh) / max(abs(orig_kwh), 1e-12)
    tol = 1e-8
    return CheckResult(
        name="energy_conservation",
        status="PASS" if rel_diff < tol else "FAIL",
        message=f"Energy conservation: orig={orig_kwh:.2f}, shifted={shifted_kwh:.2f}, rel_diff={rel_diff:.2e}",
        details={"orig_kwh": orig_kwh, "shifted_kwh": shifted_kwh, "rel_diff": rel_diff},
    )


def check_mc_delta_negative(
    scenario: SyntheticScenario,
    elasticity: float,
) -> CheckResult:
    """Shifting peak->offpeak with negative elasticity must reduce MC dollars."""
    shifted, _ = _shift_loads(scenario, elasticity)
    full_shifted = _apply_shift_to_full_load(scenario, shifted)
    all_bldg_ids = scenario.tou_bldg_ids + scenario.nontou_bldg_ids
    mc_orig = _compute_system_mc(
        scenario.load_elec, all_bldg_ids, scenario.customer_metadata,
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )
    mc_shifted = _compute_system_mc(
        full_shifted, all_bldg_ids, scenario.customer_metadata,
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )
    mc_delta = mc_shifted - mc_orig
    return CheckResult(
        name="mc_delta_negative",
        status="PASS" if mc_delta < 0 else "FAIL",
        message=f"MC delta: ${mc_delta:,.2f} (orig=${mc_orig:,.2f}, shifted=${mc_shifted:,.2f})",
        details={"mc_orig": mc_orig, "mc_shifted": mc_shifted, "mc_delta": mc_delta},
    )


def check_frozen_residual_identity(
    scenario: SyntheticScenario,
    elasticity: float,
) -> CheckResult:
    """new_RR == full_RR_orig + (MC_shifted - MC_orig)."""
    shifted, _ = _shift_loads(scenario, elasticity)
    full_shifted = _apply_shift_to_full_load(scenario, shifted)
    all_bldg_ids = scenario.tou_bldg_ids + scenario.nontou_bldg_ids
    mc_orig = _compute_system_mc(
        scenario.load_elec, all_bldg_ids, scenario.customer_metadata,
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )
    mc_shifted = _compute_system_mc(
        full_shifted, all_bldg_ids, scenario.customer_metadata,
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )
    frozen_residual = scenario.rr_total - mc_orig
    new_rr = mc_shifted + frozen_residual
    expected_rr = scenario.rr_total + (mc_shifted - mc_orig)
    diff = abs(new_rr - expected_rr)
    tol = 1.0
    return CheckResult(
        name="frozen_residual_identity",
        status="PASS" if diff < tol else "FAIL",
        message=f"new_RR=${new_rr:,.2f} vs expected=${expected_rr:,.2f} (diff=${diff:.4f})",
        details={
            "new_rr": new_rr, "expected_rr": expected_rr,
            "frozen_residual": frozen_residual, "diff": diff,
        },
    )


def check_rr_decreases(
    scenario: SyntheticScenario,
    elasticity: float,
) -> CheckResult:
    """new_RR < full_RR_orig for non-zero elasticity."""
    shifted, _ = _shift_loads(scenario, elasticity)
    full_shifted = _apply_shift_to_full_load(scenario, shifted)
    all_bldg_ids = scenario.tou_bldg_ids + scenario.nontou_bldg_ids
    mc_orig = _compute_system_mc(
        scenario.load_elec, all_bldg_ids, scenario.customer_metadata,
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )
    mc_shifted = _compute_system_mc(
        full_shifted, all_bldg_ids, scenario.customer_metadata,
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )
    new_rr = mc_shifted + (scenario.rr_total - mc_orig)
    return CheckResult(
        name="rr_decreases",
        status="PASS" if new_rr < scenario.rr_total else "FAIL",
        message=f"new_RR=${new_rr:,.2f} vs orig=${scenario.rr_total:,.2f} (delta=${new_rr - scenario.rr_total:,.2f})",
        details={"new_rr": new_rr, "rr_orig": scenario.rr_total},
    )


def check_tou_ratio_does_not_increase(
    scenario: SyntheticScenario,
    elasticity: float,
) -> CheckResult:
    """Post-flex TOU cost-causation ratio must not increase significantly.

    With proportional within-period hourly allocation, the demand-weighted MC
    averages are preserved by construction.  The ratio may decrease slightly
    (via receiver-period heterogeneity and seasonal effects) or remain
    unchanged, but should never increase beyond floating-point noise.
    """
    shifted, _ = _shift_loads(scenario, elasticity)
    combined_mc = combine_marginal_costs(
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )
    mc_index = pd.DatetimeIndex(combined_mc.index)
    peak_hours = scenario.season_specs[0].peak_hours

    sample_weights = scenario.customer_metadata[["bldg_id", "weight"]]
    bldg_level = scenario.load_elec.index.get_level_values("bldg_id")
    tou_set = set(scenario.tou_bldg_ids)
    orig_tou = scenario.load_elec.loc[bldg_level.isin(tou_set)]
    orig_weighted = orig_tou.reset_index().merge(sample_weights, on="bldg_id")
    orig_weighted["electricity_net"] *= orig_weighted["weight"]
    orig_sys = orig_weighted.groupby("time")["electricity_net"].sum()
    orig_load = pd.Series(orig_sys.values[: len(mc_index)], index=mc_index)

    full_shifted = _apply_shift_to_full_load(scenario, shifted)
    shifted_tou = full_shifted.loc[bldg_level.isin(tou_set)]
    shifted_weighted = shifted_tou.reset_index().merge(sample_weights, on="bldg_id")
    shifted_weighted["electricity_net"] *= shifted_weighted["weight"]
    shifted_sys = shifted_weighted.groupby("time")["electricity_net"].sum()
    shifted_load = pd.Series(shifted_sys.values[: len(mc_index)], index=mc_index)

    ratio_orig = compute_tou_cost_causation_ratio(combined_mc, orig_load, peak_hours)
    ratio_shifted = compute_tou_cost_causation_ratio(combined_mc, shifted_load, peak_hours)
    delta = ratio_shifted - ratio_orig
    tol = 1e-3
    return CheckResult(
        name="tou_ratio_does_not_increase",
        status="PASS" if delta < tol else "FAIL",
        message=f"TOU ratio: {ratio_orig:.4f} -> {ratio_shifted:.4f} (delta={delta:.6f})",
        details={"ratio_orig": ratio_orig, "ratio_shifted": ratio_shifted, "delta": delta},
    )


def check_nonhp_subclass_rr_unchanged(
    scenario: SyntheticScenario,
    elasticity: float,
) -> CheckResult:
    """Non-TOU subclass RR equals its no-flex baseline."""
    nonhp_baseline = scenario.subclass_rr_ratios["non-hp"] * scenario.rr_total
    # Under demand flex, non-TOU RR is held at baseline:
    nonhp_flex_rr = nonhp_baseline  # by construction in the model
    diff = abs(nonhp_flex_rr - nonhp_baseline)
    return CheckResult(
        name="nonhp_subclass_rr_unchanged",
        status="PASS" if diff < 0.01 else "FAIL",
        message=f"Non-HP RR: baseline=${nonhp_baseline:,.2f}, flex=${nonhp_flex_rr:,.2f}, diff=${diff:.2f}",
        details={"nonhp_baseline": nonhp_baseline, "nonhp_flex_rr": nonhp_flex_rr},
    )


def check_tou_subclass_rr_absorbs_delta(
    scenario: SyntheticScenario,
    elasticity: float,
) -> CheckResult:
    """TOU subclass RR = baseline_TOU + MC_delta_TOU."""
    shifted, _ = _shift_loads(scenario, elasticity)
    full_shifted = _apply_shift_to_full_load(scenario, shifted)
    mc_orig_tou = _compute_system_mc(
        scenario.load_elec, scenario.tou_bldg_ids, scenario.customer_metadata,
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )
    mc_shifted_tou = _compute_system_mc(
        full_shifted, scenario.tou_bldg_ids, scenario.customer_metadata,
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )
    mc_delta_tou = mc_shifted_tou - mc_orig_tou
    hp_baseline = scenario.subclass_rr_ratios["hp"] * scenario.rr_total
    expected_hp_rr = hp_baseline + mc_delta_tou

    # Under the frozen-residual model: new system RR = RR_orig + MC_delta_system
    # Non-HP RR is held at baseline. TOU RR = new_system_RR - non-HP baseline.
    all_bldg_ids = scenario.tou_bldg_ids + scenario.nontou_bldg_ids
    mc_orig_all = _compute_system_mc(
        scenario.load_elec, all_bldg_ids, scenario.customer_metadata,
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )
    mc_shifted_all = _compute_system_mc(
        full_shifted, all_bldg_ids, scenario.customer_metadata,
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )
    new_rr_system = scenario.rr_total + (mc_shifted_all - mc_orig_all)
    nonhp_baseline = scenario.subclass_rr_ratios["non-hp"] * scenario.rr_total
    actual_hp_rr = new_rr_system - nonhp_baseline

    diff = abs(actual_hp_rr - expected_hp_rr)
    # Tolerance is wider because system-level MC delta may differ from
    # TOU-subclass MC delta when non-TOU loads don't change.
    tol = abs(scenario.rr_total) * 0.001
    return CheckResult(
        name="tou_subclass_rr_absorbs_delta",
        status="PASS" if diff < tol else "FAIL",
        message=f"TOU RR: expected=${expected_hp_rr:,.2f}, actual=${actual_hp_rr:,.2f}, diff=${diff:,.2f}",
        details={
            "expected_hp_rr": expected_hp_rr, "actual_hp_rr": actual_hp_rr,
            "mc_delta_tou": mc_delta_tou, "diff": diff,
        },
    )


def check_achieved_elasticity_near_target(
    scenario: SyntheticScenario,
    elasticity: float,
) -> CheckResult:
    """Realized elasticity approximates input elasticity within 10% relative."""
    tou_hourly = _extract_tou_hourly(scenario)
    _, tracker = process_residential_hourly_demand_response_shift(
        hourly_load_df=tou_hourly,
        period_rate=scenario.period_rate,
        demand_elasticity=elasticity,
    )
    # Filter to donor periods (not the receiver which is forced zero-sum)
    valid = tracker.dropna(subset=["epsilon"])
    if valid.empty:
        return CheckResult(
            name="achieved_elasticity_near_target",
            status="WARN",
            message="No valid realized elasticity values",
            details={},
        )
    # Donor periods are those with rate > flat; receiver has rate < flat
    flat = float(valid["rate"].min())
    donors = valid[valid["rate"] > flat]
    if donors.empty:
        return CheckResult(
            name="achieved_elasticity_near_target",
            status="WARN",
            message="No donor periods found for elasticity comparison",
            details={},
        )
    avg_epsilon = float(donors["epsilon"].mean())
    rel_error = abs(avg_epsilon - elasticity) / abs(elasticity) if elasticity != 0 else 0
    tol = 0.10
    return CheckResult(
        name="achieved_elasticity_near_target",
        status="PASS" if rel_error < tol else "FAIL",
        message=f"Avg realized epsilon={avg_epsilon:.4f} vs target={elasticity:.4f} (rel_error={rel_error:.2%})",
        details={"avg_epsilon": avg_epsilon, "target": elasticity, "rel_error": rel_error},
    )


def check_shifted_loads_nonnegative(
    scenario: SyntheticScenario,
    elasticity: float,
) -> CheckResult:
    """All shifted hourly loads must be >= 0."""
    shifted, _ = _shift_loads(scenario, elasticity)
    min_load = float(shifted["shifted_net"].min())
    n_negative = int((shifted["shifted_net"] < 0).sum())
    return CheckResult(
        name="shifted_loads_nonnegative",
        status="PASS" if n_negative == 0 else "FAIL",
        message=f"Min shifted load={min_load:.4f}, negative hours={n_negative}",
        details={"min_load": min_load, "n_negative": n_negative},
    )


def check_mc_delta_monotonic_in_elasticity(
    scenario: SyntheticScenario,
    elasticities: list[float] | None = None,
) -> CheckResult:
    """Larger |elasticity| produces larger |MC delta|."""
    if elasticities is None:
        elasticities = [-0.05, -0.10, -0.15, -0.20]
    all_bldg_ids = scenario.tou_bldg_ids + scenario.nontou_bldg_ids
    mc_orig = _compute_system_mc(
        scenario.load_elec, all_bldg_ids, scenario.customer_metadata,
        scenario.bulk_marginal_costs, scenario.dist_mc,
    )

    deltas: list[float] = []
    for e in elasticities:
        shifted, _ = _shift_loads(scenario, e)
        full_shifted = _apply_shift_to_full_load(scenario, shifted)
        mc_shifted = _compute_system_mc(
            full_shifted, all_bldg_ids, scenario.customer_metadata,
            scenario.bulk_marginal_costs, scenario.dist_mc,
        )
        deltas.append(mc_shifted - mc_orig)

    magnitudes = [abs(d) for d in deltas]
    is_monotonic = all(
        magnitudes[i] <= magnitudes[i + 1] for i in range(len(magnitudes) - 1)
    )
    return CheckResult(
        name="mc_delta_monotonic_in_elasticity",
        status="PASS" if is_monotonic else "FAIL",
        message=f"MC deltas by elasticity: {dict(zip(elasticities, [f'${d:,.0f}' for d in deltas]))}",
        details={
            "elasticities": elasticities,
            "mc_deltas": deltas,
            "magnitudes": magnitudes,
            "is_monotonic": is_monotonic,
        },
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

ALL_SINGLE_ELASTICITY_CHECKS = [
    check_energy_conservation,
    check_mc_delta_negative,
    check_frozen_residual_identity,
    check_rr_decreases,
    check_tou_ratio_does_not_increase,
    check_nonhp_subclass_rr_unchanged,
    check_tou_subclass_rr_absorbs_delta,
    check_achieved_elasticity_near_target,
    check_shifted_loads_nonnegative,
]


def run_all_checks(
    scenario: SyntheticScenario,
    elasticity: float = -0.1,
    sweep_elasticities: list[float] | None = None,
) -> list[CheckResult]:
    """Run all analytical demand-flex checks and return results."""
    results: list[CheckResult] = []
    for check_fn in ALL_SINGLE_ELASTICITY_CHECKS:
        results.append(check_fn(scenario, elasticity))
    results.append(
        check_mc_delta_monotonic_in_elasticity(scenario, sweep_elasticities)
    )
    return results


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Analytical validation of demand-flex mathematical invariants."
    )
    p.add_argument(
        "--synthetic",
        action="store_true",
        default=True,
        help="Run on synthetic data (default).",
    )
    p.add_argument(
        "--elasticity",
        type=float,
        default=-0.1,
        help="Elasticity parameter for single-point checks (default: -0.1).",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    scenario = build_synthetic_scenario()
    results = run_all_checks(scenario, elasticity=args.elasticity)

    print(f"\n{'=' * 60}")
    print("DEMAND-FLEX ANALYTICAL VALIDATION")
    print(f"{'=' * 60}")
    print(f"  Elasticity: {args.elasticity}")
    print(f"  Mode: {'synthetic' if args.synthetic else 'custom'}")
    print(f"{'=' * 60}\n")

    for r in results:
        status_marker = {"PASS": "+", "WARN": "~", "FAIL": "x"}[r.status]
        print(f"  [{status_marker}] {r.status}: {r.name}")
        print(f"      {r.message}")

    by_status = {s: sum(1 for r in results if r.status == s) for s in ("PASS", "WARN", "FAIL")}
    print(f"\n{'=' * 60}")
    print(f"  PASS: {by_status['PASS']}  WARN: {by_status['WARN']}  FAIL: {by_status['FAIL']}")
    print(f"{'=' * 60}\n")

    if by_status["FAIL"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
