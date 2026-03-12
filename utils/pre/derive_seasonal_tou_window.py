"""Select the optimal TOU window width (number of on-peak hours) per utility.

Sweeps N = 1..23 contiguous on-peak hours, evaluates each candidate using a
load-weighted squared MC residual metric, and writes the optimal N to the
utility's periods YAML.  This is a one-time pre-processing step; the existing
``derive_seasonal_tou.py`` reads ``tou_window_hours`` from the same YAML at
run time, so no runtime changes are needed.

The metric for each candidate window width N in a given season is:

    metric(N, season) = sum_h [ (MC_h - rate_period(h))^2 * L_h ]

where ``rate_period(h)`` is the demand-weighted average MC of hour h's
assigned period (peak or off-peak) and ``L_h`` is system load.  The combined
metric sums across seasons, naturally weighting by load volume.

Usage::

    uv run python -m utils.pre.derive_seasonal_tou_window \\
        --path-supply-energy-mc <path> \\
        --path-supply-capacity-mc <path> \\
        --state NY --utility coned --year 2025 \\
        --path-dist-and-sub-tx-mc <path> \\
        --path-utility-assignment <path> \\
        --resstock-base <path> \\
        --path-electric-utility-stats <path> \\
        --has-hp true \\
        --output-dir <path>
"""

from __future__ import annotations

import argparse
import csv
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import yaml

from utils.pre.compute_tou import (
    Season,
    combine_marginal_costs,
    compute_tou_fit_metric,
    compute_tou_cost_causation_ratio,
    find_tou_peak_window,
    make_winter_summer_seasons,
    season_mask,
)
from utils.pre.derive_seasonal_tou import load_tou_inputs
from utils.pre.season_config import (
    DEFAULT_TOU_WINTER_MONTHS,
    get_utility_periods_yaml_path,
    load_winter_months_from_periods,
    parse_months_arg,
    resolve_winter_summer_months,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sweep
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class TouWindowSweepResult:
    """Result for one candidate window width across all seasons."""

    window_hours: int
    peak_hours_by_season: dict[str, list[int]]
    ratio_by_season: dict[str, float]
    metric_by_season: dict[str, float]
    metric_total: float


def sweep_tou_window_hours(
    combined_mc: pd.Series,
    hourly_load: pd.Series,
    seasons: list[Season],
    window_range: range = range(1, 24),
) -> list[TouWindowSweepResult]:
    """Sweep candidate TOU window widths and return per-N metrics.

    For each N in *window_range*, for each season: finds the optimal
    contiguous peak window, computes the cost-causation ratio, and evaluates
    the fit metric.  Returns results sorted by ``metric_total`` (ascending).
    """
    mc_index = pd.DatetimeIndex(combined_mc.index)
    results: list[TouWindowSweepResult] = []

    for n in window_range:
        peak_hours_by_season: dict[str, list[int]] = {}
        ratio_by_season: dict[str, float] = {}
        metric_by_season: dict[str, float] = {}

        for s in seasons:
            mask = season_mask(mc_index, s)
            mc_s = combined_mc[mask]
            load_s = hourly_load[mask]

            peak_hours = find_tou_peak_window(mc_s, load_s, n)
            ratio = compute_tou_cost_causation_ratio(mc_s, load_s, peak_hours)
            metric = compute_tou_fit_metric(mc_s, load_s, peak_hours)

            peak_hours_by_season[s.name] = peak_hours
            ratio_by_season[s.name] = ratio
            metric_by_season[s.name] = metric

        results.append(
            TouWindowSweepResult(
                window_hours=n,
                peak_hours_by_season=peak_hours_by_season,
                ratio_by_season=ratio_by_season,
                metric_by_season=metric_by_season,
                metric_total=sum(metric_by_season.values()),
            )
        )

    results.sort(key=lambda r: r.metric_total)
    return results


# ---------------------------------------------------------------------------
# YAML writer
# ---------------------------------------------------------------------------


def update_periods_yaml(periods_yaml_path: Path, tou_window_hours: int) -> None:
    """Update ``tou_window_hours`` in an existing periods YAML file."""
    if periods_yaml_path.exists():
        with periods_yaml_path.open(encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}
    data["tou_window_hours"] = tou_window_hours
    periods_yaml_path.parent.mkdir(parents=True, exist_ok=True)
    with periods_yaml_path.open("w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=None, sort_keys=False)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Sweep TOU window widths (1-23 hours) and select the width that "
            "minimizes load-weighted squared MC residuals.  Writes the optimal "
            "tou_window_hours to the utility's periods YAML."
        ),
    )

    p.add_argument(
        "--path-supply-energy-mc",
        required=True,
        help="Path (local or s3://) to supply energy MC parquet.",
    )
    p.add_argument(
        "--path-supply-capacity-mc",
        required=True,
        help="Path (local or s3://) to supply capacity MC parquet.",
    )
    p.add_argument("--state", required=True, help="State code (e.g. NY).")
    p.add_argument("--utility", required=True, help="Utility short name (e.g. coned).")
    p.add_argument(
        "--year", type=int, required=True, help="Target year for marginal cost data."
    )
    p.add_argument(
        "--path-dist-and-sub-tx-mc",
        required=True,
        help="Path (local or s3://) to dist+sub-tx marginal cost parquet.",
    )
    p.add_argument(
        "--path-bulk-tx-mc",
        default=None,
        help="Optional path to bulk transmission MC parquet.",
    )
    p.add_argument(
        "--path-utility-assignment",
        required=True,
        help="Path to utility_assignment.parquet (bldg_id, weight, sb.electric_utility, has_hp).",
    )
    p.add_argument(
        "--resstock-base",
        required=True,
        help="Base path to ResStock release.",
    )
    p.add_argument("--upgrade", default="00", help="Upgrade partition for loads.")
    p.add_argument(
        "--path-electric-utility-stats",
        required=True,
        help="Path to EIA-861 utility stats parquet.",
    )
    p.add_argument(
        "--has-hp",
        default="true",
        help=(
            "Filter buildings by HP status: 'true' (HP only, default), "
            "'false' (non-HP only), or 'all' (no filter)."
        ),
    )
    p.add_argument(
        "--winter-months",
        default=None,
        help="Comma-separated 1-indexed winter months.",
    )
    p.add_argument(
        "--periods-yaml",
        default=None,
        help=(
            "Path to periods YAML.  When omitted, resolves "
            "rate_design/hp_rates/<state>/config/periods/<utility>.yaml."
        ),
    )
    p.add_argument(
        "--run-dir",
        default=None,
        help="Optional CAIRO output directory to restrict building set.",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Directory for sweep results CSV.  Defaults to periods YAML directory.",
    )
    p.add_argument(
        "--no-write-yaml",
        action="store_true",
        help="Report results without updating the periods YAML.",
    )

    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(name)s %(levelname)s: %(message)s"
    )
    args = _parse_args()

    project_root = Path(__file__).resolve().parents[2]
    periods_yaml_path = (
        Path(args.periods_yaml)
        if args.periods_yaml
        else get_utility_periods_yaml_path(
            project_root=project_root,
            state=args.state,
            utility=args.utility,
        )
    )

    default_winter_months = list(DEFAULT_TOU_WINTER_MONTHS)
    if periods_yaml_path.exists():
        default_winter_months = load_winter_months_from_periods(
            periods_yaml_path,
            default_winter_months=DEFAULT_TOU_WINTER_MONTHS,
        )

    winter_months, _summer_months = resolve_winter_summer_months(
        parse_months_arg(args.winter_months) if args.winter_months else None,
        default_winter_months=default_winter_months,
    )

    # -- Parse has_hp filter ---------------------------------------------------
    has_hp_raw = args.has_hp.strip().lower()
    if has_hp_raw == "all":
        has_hp_filter: set[bool] | None = None
    elif has_hp_raw == "true":
        has_hp_filter = {True}
    elif has_hp_raw == "false":
        has_hp_filter = {False}
    else:
        raise SystemExit(
            f"--has-hp must be 'true', 'false', or 'all', got '{args.has_hp}'"
        )

    # -- Load data -----------------------------------------------------------
    bulk_mc, dist_mc, bulk_tx_mc, hourly_load = load_tou_inputs(
        path_supply_energy_mc=args.path_supply_energy_mc,
        path_supply_capacity_mc=args.path_supply_capacity_mc,
        year=args.year,
        path_dist_and_sub_tx_mc=args.path_dist_and_sub_tx_mc,
        path_utility_assignment=args.path_utility_assignment,
        resstock_base=args.resstock_base,
        state=args.state,
        upgrade=args.upgrade,
        path_electric_utility_stats=args.path_electric_utility_stats,
        utility=args.utility,
        path_bulk_tx_mc=args.path_bulk_tx_mc,
        run_dir=args.run_dir,
        has_hp_filter=has_hp_filter,
    )

    combined_mc = combine_marginal_costs(bulk_mc, dist_mc, bulk_tx_mc)

    if not hourly_load.index.equals(combined_mc.index):
        if len(hourly_load) == len(combined_mc):
            hourly_load = pd.Series(
                hourly_load.values,
                index=combined_mc.index,
                name=hourly_load.name,
            )
        else:
            hourly_load = hourly_load.reindex(combined_mc.index, method="ffill")

    seasons = make_winter_summer_seasons(winter_months)

    # -- Sweep ---------------------------------------------------------------
    log.info("Sweeping TOU window widths 1-23 for utility=%s", args.utility)
    results = sweep_tou_window_hours(combined_mc, hourly_load, seasons)

    best = results[0]

    # -- Report --------------------------------------------------------------
    log.info("")
    log.info("=" * 78)
    log.info("TOU window sweep results for %s (sorted by total metric)", args.utility)
    log.info("=" * 78)
    log.info(
        "%-6s  %-20s  %-20s  %-14s  %-14s  %s",
        "N",
        "peak_winter",
        "peak_summer",
        "ratio_winter",
        "ratio_summer",
        "metric_total",
    )
    log.info("-" * 78)
    for r in results:
        log.info(
            "%-6d  %-20s  %-20s  %-14.4f  %-14.4f  %.6e",
            r.window_hours,
            str(r.peak_hours_by_season.get("winter", [])),
            str(r.peak_hours_by_season.get("summer", [])),
            r.ratio_by_season.get("winter", 0.0),
            r.ratio_by_season.get("summer", 0.0),
            r.metric_total,
        )
    log.info("-" * 78)

    # -- Prominent summary ---------------------------------------------------
    runner_up = results[1] if len(results) > 1 else None
    pct_gap = (
        (runner_up.metric_total - best.metric_total) / best.metric_total * 100
        if runner_up and best.metric_total > 0
        else 0.0
    )

    border = "+" + "-" * 68 + "+"
    log.info("")
    log.info(border)
    log.info(
        "|  %-66s|", f"OPTIMAL TOU WINDOW for {args.utility}: {best.window_hours} hours"
    )
    log.info("|  %-66s|", "")
    for sn in [s.name for s in seasons]:
        peak = best.peak_hours_by_season.get(sn, [])
        ratio = best.ratio_by_season.get(sn, 0.0)
        metric = best.metric_by_season.get(sn, 0.0)
        log.info(
            "|  %-66s|",
            f"{sn:>8} peak hours: {peak}",
        )
        log.info(
            "|  %-66s|",
            f"{'':>8} ratio: {ratio:.4f}   metric: {metric:.4e}",
        )
    log.info("|  %-66s|", "")
    log.info("|  %-66s|", f"Total metric: {best.metric_total:.6e}")
    if runner_up:
        log.info(
            "|  %-66s|",
            f"Runner-up: {runner_up.window_hours} hrs "
            f"(metric {runner_up.metric_total:.6e}, +{pct_gap:.1f}%)",
        )
    log.info(border)

    # -- Write CSV -----------------------------------------------------------
    default_output_dir = project_root / "utils" / "pre" / "tou_window"
    output_dir = Path(args.output_dir) if args.output_dir else default_output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{args.utility}_tou_window_sweep.csv"

    season_names = [s.name for s in seasons]
    fieldnames = ["window_hours"]
    for sn in season_names:
        fieldnames.extend([f"peak_hours_{sn}", f"ratio_{sn}", f"metric_{sn}"])
    fieldnames.append("metric_total")

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            row: dict[str, object] = {"window_hours": r.window_hours}
            for sn in season_names:
                row[f"peak_hours_{sn}"] = str(r.peak_hours_by_season.get(sn, []))
                row[f"ratio_{sn}"] = f"{r.ratio_by_season.get(sn, 0.0):.4f}"
                row[f"metric_{sn}"] = f"{r.metric_by_season.get(sn, 0.0):.6e}"
            row["metric_total"] = f"{r.metric_total:.6e}"
            writer.writerow(row)
    log.info("Wrote sweep results to %s", csv_path)

    # -- Update periods YAML -------------------------------------------------
    if not args.no_write_yaml:
        update_periods_yaml(periods_yaml_path, best.window_hours)
        log.info(
            "Updated %s: tou_window_hours = %d", periods_yaml_path, best.window_hours
        )
    else:
        log.info("--no-write-yaml set; periods YAML not updated")

    log.info("Done.")


if __name__ == "__main__":
    main()
