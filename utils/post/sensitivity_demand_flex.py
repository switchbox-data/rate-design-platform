"""Elasticity sensitivity analysis for the demand-flex pipeline.

Sweeps over a range of elasticity values and computes key demand-flex
metrics at each point: load shift magnitude, MC delta, RR change,
TOU ratio change, per-customer bill impact, and non-negativity margin.

Can run on synthetic data (for CI) or real utility load/MC data.

Usage::

    uv run python -m utils.post.sensitivity_demand_flex \
        --elasticities -0.05,-0.10,-0.15,-0.20 [--synthetic] [--output-dir /tmp]
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd

from utils.cairo import (
    assign_hourly_periods,
    process_residential_hourly_demand_response_shift,
)
from utils.post.validate_demand_flex import (
    SyntheticScenario,
    _apply_shift_to_full_load,
    _compute_system_mc,
    _extract_tou_hourly,
    _shift_loads,
    build_synthetic_scenario,
)
from utils.pre.compute_tou import (
    combine_marginal_costs,
    compute_tou_cost_causation_ratio,
)


@dataclass(slots=True)
class SweepResult:
    """Metrics computed at a single elasticity point."""

    elasticity: float
    load_shift_kwh: float
    load_shift_pct: float
    mc_delta_dollars: float
    mc_delta_pct: float
    rr_change_dollars: float
    rr_change_pct: float
    tou_ratio_orig: float
    tou_ratio_shifted: float
    tou_ratio_change: float
    per_customer_bill_change: float
    min_shifted_load: float


def compute_sweep_point(
    scenario: SyntheticScenario,
    elasticity: float,
) -> SweepResult:
    """Compute all metrics for one elasticity value."""
    # Shift loads
    shifted, _ = _shift_loads(scenario, elasticity)
    full_shifted = _apply_shift_to_full_load(scenario, shifted)

    # Load shift magnitude
    period_map = assign_hourly_periods(scenario.hourly_index, scenario.tou_tariff)
    tou_hourly = _extract_tou_hourly(scenario)
    peak_periods = set()
    rate_df = scenario.period_rate.reset_index()
    max_rate = scenario.period_rate.max()
    for _, row in rate_df.iterrows():
        if row["rate"] == max_rate:
            peak_periods.add(int(row["energy_period"]))

    peak_mask = tou_hourly["energy_period"].isin(peak_periods)
    orig_peak_kwh = float(tou_hourly.loc[peak_mask, "electricity_net"].sum())
    shifted_merged = shifted if "energy_period" in shifted.columns else tou_hourly.copy()
    if "energy_period" not in shifted.columns:
        period_df = period_map.reset_index()
        period_df.columns = pd.Index(["time", "energy_period"])
        shifted_merged = shifted.merge(period_df, on="time", how="left")

    shifted_peak_mask = shifted_merged["energy_period"].isin(peak_periods)
    shifted_peak_kwh = float(shifted_merged.loc[shifted_peak_mask, "shifted_net"].sum())
    load_shift_kwh = orig_peak_kwh - shifted_peak_kwh
    load_shift_pct = (load_shift_kwh / orig_peak_kwh * 100) if orig_peak_kwh > 0 else 0.0

    # MC delta
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
    mc_delta_pct = (mc_delta / mc_orig * 100) if mc_orig != 0 else 0.0

    # RR change (frozen residual model)
    frozen_residual = scenario.rr_total - mc_orig
    new_rr = mc_shifted + frozen_residual
    rr_change = new_rr - scenario.rr_total
    rr_change_pct = (rr_change / scenario.rr_total * 100) if scenario.rr_total != 0 else 0.0

    # TOU ratio change
    combined_mc = combine_marginal_costs(scenario.bulk_marginal_costs, scenario.dist_mc)
    mc_index = pd.DatetimeIndex(combined_mc.index)
    peak_hours = scenario.season_specs[0].peak_hours
    sample_weights = scenario.customer_metadata[["bldg_id", "weight"]]
    bldg_level = scenario.load_elec.index.get_level_values("bldg_id")
    tou_set = set(scenario.tou_bldg_ids)

    orig_tou = scenario.load_elec.loc[bldg_level.isin(tou_set)]
    orig_w = orig_tou.reset_index().merge(sample_weights, on="bldg_id")
    orig_w["electricity_net"] *= orig_w["weight"]
    orig_sys = orig_w.groupby("time")["electricity_net"].sum()
    orig_load = pd.Series(orig_sys.values[: len(mc_index)], index=mc_index)

    shifted_tou = full_shifted.loc[bldg_level.isin(tou_set)]
    shifted_w = shifted_tou.reset_index().merge(sample_weights, on="bldg_id")
    shifted_w["electricity_net"] *= shifted_w["weight"]
    shifted_sys = shifted_w.groupby("time")["electricity_net"].sum()
    shifted_load_s = pd.Series(shifted_sys.values[: len(mc_index)], index=mc_index)

    ratio_orig = compute_tou_cost_causation_ratio(combined_mc, orig_load, peak_hours)
    ratio_shifted = compute_tou_cost_causation_ratio(combined_mc, shifted_load_s, peak_hours)

    # Per-customer bill impact (approximate: RR change / total customer weights)
    total_tou_weight = float(
        scenario.customer_metadata[
            scenario.customer_metadata["bldg_id"].isin(tou_set)
        ]["weight"].sum()
    )
    per_customer_bill_change = rr_change / total_tou_weight if total_tou_weight > 0 else 0.0

    # Non-negativity margin
    min_shifted_load = float(shifted["shifted_net"].min())

    return SweepResult(
        elasticity=elasticity,
        load_shift_kwh=load_shift_kwh,
        load_shift_pct=load_shift_pct,
        mc_delta_dollars=mc_delta,
        mc_delta_pct=mc_delta_pct,
        rr_change_dollars=rr_change,
        rr_change_pct=rr_change_pct,
        tou_ratio_orig=ratio_orig,
        tou_ratio_shifted=ratio_shifted,
        tou_ratio_change=ratio_shifted - ratio_orig,
        per_customer_bill_change=per_customer_bill_change,
        min_shifted_load=min_shifted_load,
    )


def run_sweep(
    scenario: SyntheticScenario,
    elasticities: list[float],
) -> list[SweepResult]:
    """Run the sensitivity sweep across all elasticity values."""
    return [compute_sweep_point(scenario, e) for e in elasticities]


def results_to_dataframe(results: list[SweepResult]) -> pd.DataFrame:
    """Convert sweep results to a DataFrame for output/plotting."""
    records = [
        {
            "elasticity": r.elasticity,
            "load_shift_kwh": r.load_shift_kwh,
            "load_shift_pct": r.load_shift_pct,
            "mc_delta_dollars": r.mc_delta_dollars,
            "mc_delta_pct": r.mc_delta_pct,
            "rr_change_dollars": r.rr_change_dollars,
            "rr_change_pct": r.rr_change_pct,
            "tou_ratio_orig": r.tou_ratio_orig,
            "tou_ratio_shifted": r.tou_ratio_shifted,
            "tou_ratio_change": r.tou_ratio_change,
            "per_customer_bill_change": r.per_customer_bill_change,
            "min_shifted_load": r.min_shifted_load,
        }
        for r in results
    ]
    return pd.DataFrame(records)


def format_table(df: pd.DataFrame) -> str:
    """Render the sweep results as a human-readable table."""
    buf = StringIO()
    fmt: dict[str, str] = {
        "elasticity": "{:.3f}",
        "load_shift_kwh": "{:,.0f}",
        "load_shift_pct": "{:.2f}%",
        "mc_delta_dollars": "${:,.0f}",
        "mc_delta_pct": "{:.3f}%",
        "rr_change_dollars": "${:,.0f}",
        "rr_change_pct": "{:.3f}%",
        "tou_ratio_orig": "{:.4f}",
        "tou_ratio_shifted": "{:.4f}",
        "tou_ratio_change": "{:.4f}",
        "per_customer_bill_change": "${:,.2f}",
        "min_shifted_load": "{:.4f}",
    }
    formatted = df.copy()
    for col, f in fmt.items():
        if col in formatted.columns:
            formatted[col] = formatted[col].map(lambda v, f=f: f.format(v))
    formatted.to_string(buf, index=False)
    return buf.getvalue()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Elasticity sensitivity analysis for demand-flex pipeline."
    )
    p.add_argument(
        "--elasticities",
        type=str,
        default="-0.05,-0.10,-0.15,-0.20",
        help="Comma-separated elasticity values (default: -0.05,-0.10,-0.15,-0.20).",
    )
    p.add_argument(
        "--synthetic",
        action="store_true",
        default=True,
        help="Use synthetic data (default).",
    )
    p.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to write CSV output. If omitted, prints to stdout.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    elasticities = [float(e.strip()) for e in args.elasticities.split(",")]
    scenario = build_synthetic_scenario()
    results = run_sweep(scenario, elasticities)
    df = results_to_dataframe(results)

    print(f"\n{'=' * 72}")
    print("DEMAND-FLEX ELASTICITY SENSITIVITY ANALYSIS")
    print(f"{'=' * 72}")
    print(f"  Elasticities: {elasticities}")
    print(f"  Mode: {'synthetic' if args.synthetic else 'custom'}")
    print(f"{'=' * 72}\n")
    print(format_table(df))
    print()

    if args.output_dir is not None:
        out_path = Path(args.output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        csv_path = out_path / "demand_flex_sensitivity.csv"
        df.to_csv(csv_path, index=False)
        print(f"  CSV written to: {csv_path}")


if __name__ == "__main__":
    main()
