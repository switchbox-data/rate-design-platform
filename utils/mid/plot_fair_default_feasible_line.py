"""Plot the (C1 ∧ C2) feasible 1-D family of (F, r_win, r_sum) fair-default tariffs.

For any fixed charge F, there is exactly one (r_winter, r_summer) pair that satisfies
both the class revenue-sufficiency constraint (C1) and the subclass cross-subsidy-
elimination constraint (C2) — provided the seasonal kWh winter shares of the subclass
and class differ (which is always true for heat-pump customers).

This produces an affine line in (F, r_win, r_sum)-space:
    r_win(F) = winter_intercept + winter_slope * F
    r_sum(F) = summer_intercept + summer_slope * F

The feasibility region is the segment of that line where r_win ≥ 0 and r_sum ≥ 0 and
F ≥ F_floor.  This script visualises:

  - Two stacked 2-D panels: r_win(F) and r_sum(F) with the feasible band shaded.
  - A 3-D side panel showing the line in (F, r_win, r_sum)-space with the feasible
    segment highlighted and r_win=0 / r_sum=0 reference planes.
  - Strategy A / B / C marker points on all three panels.

Inputs are the run-1 (delivery) and run-2 (delivery+supply) CAIRO output directories.
Reads load_curve_monthly (~12 rows/building) — cheap enough to run after every batch.
Strategy C markers are included only when --mc-seasonal-ratio-delivery / --mc-seasonal-ratio-supply
are supplied.

Usage example:
    uv run python utils/mid/plot_fair_default_feasible_line.py \\
        --run-dir-delivery s3://data.sb/.../run-1/ \\
        --run-dir-supply   s3://data.sb/.../run-2/ \\
        --resstock-base    s3://data.sb/nrel/resstock/res_2024_amy2018_2/ \\
        --state NY --upgrade 00 \\
        --base-tariff-delivery rate_design/hp_rates/ny/config/tariffs/electric/nyseg_default_calibrated.json \\
        --base-tariff-supply   rate_design/hp_rates/ny/config/tariffs/electric/nyseg_default_supply_calibrated.json \\
        --variant delivery --output-png /tmp/nyseg_delivery.png
"""

from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path

import matplotlib.pyplot as plt
from dotenv import load_dotenv
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401  (registers 3D projection)

from utils.mid.compute_fair_default_inputs import (
    AffineLine,
    FeasibleLineData,
    StrategyPoint,
    compute_feasible_line_from_runs,
)

LOGGER = logging.getLogger(__name__)

_INF = math.inf


# ──────────────────────────────────────────────────────────────────────────────
# Sanity checks
# ──────────────────────────────────────────────────────────────────────────────

def _verify_affine_vs_strategies(data: FeasibleLineData) -> None:
    """Warn when affine reconstruction deviates from stored strategy rates."""
    tol = 1e-6
    for sp in data.strategies:
        if sp.label == "A":
            # Strategy A: seasonal rates are derived from affine; no stored value to check
            continue
        computed_win = data.r_win.at(sp.fixed_charge)
        computed_sum = data.r_sum.at(sp.fixed_charge)
        if sp.winter_rate is not None and not math.isnan(sp.winter_rate):
            err = abs(computed_win - sp.winter_rate)
            if err > tol:
                LOGGER.warning(
                    "Strategy %s winter rate mismatch: affine=%.9f stored=%.9f diff=%.2e",
                    sp.label, computed_win, sp.winter_rate, err,
                )
        if sp.summer_rate is not None and not math.isnan(sp.summer_rate):
            err = abs(computed_sum - sp.summer_rate)
            if err > tol:
                LOGGER.warning(
                    "Strategy %s summer rate mismatch: affine=%.9f stored=%.9f diff=%.2e",
                    sp.label, computed_sum, sp.summer_rate, err,
                )


# ──────────────────────────────────────────────────────────────────────────────
# Plot
# ──────────────────────────────────────────────────────────────────────────────

# Colour / style constants
_CMAP = {
    "A": "#e6820e",  # orange
    "B": "#2471a3",  # blue
    "C": "#1e8449",  # green
}
_MARKER_SIZE = 8
_FEASIBLE_ALPHA = 0.12
_FEASIBLE_COLOR = "#27ae60"
_INFEASIBLE_COLOR = "#e74c3c"
_PLANE_ALPHA = 0.10


def _sweep_range(data: FeasibleLineData, f_min: float | None, f_max: float | None) -> tuple[float, float]:
    """Determine the F sweep range, padding beyond the feasible interval."""
    fc_floor = max(0.0, data.fixed_charge_floor)

    # Upper bound: where r_sum hits zero (or feasible_max if infinite)
    r_sum_zero = data.r_sum.zero_crossing()
    r_win_zero = data.r_win.zero_crossing()
    natural_upper = max(
        data.feasible_max if math.isfinite(data.feasible_max) else 0.0,
        r_sum_zero if (math.isfinite(r_sum_zero) and r_sum_zero > 0) else 0.0,
        r_win_zero if (math.isfinite(r_win_zero) and r_win_zero > 0) else 0.0,
    )
    # Also include all strategy points
    for sp in data.strategies:
        if math.isfinite(sp.fixed_charge):
            natural_upper = max(natural_upper, sp.fixed_charge)

    lo = f_min if f_min is not None else max(0.0, fc_floor - 5.0)
    hi = f_max if f_max is not None else natural_upper + 10.0
    if hi <= lo:
        hi = lo + 20.0
    return lo, hi


def _linspace(lo: float, hi: float, n: int = 400) -> list[float]:
    step = (hi - lo) / (n - 1)
    return [lo + i * step for i in range(n)]


def _rate_cents(rate: float) -> float:
    """Convert $/kWh to ¢/kWh for display."""
    return rate * 100.0


def plot_feasible_line(
    data: FeasibleLineData,
    output_png: Path,
    f_min_override: float | None = None,
    f_max_override: float | None = None,
) -> None:
    """Render the three-panel feasibility plot and save to output_png."""
    lo, hi = _sweep_range(data, f_min_override, f_max_override)
    fs = _linspace(lo, hi)
    r_wins = [_rate_cents(data.r_win.at(f)) for f in fs]
    r_sums = [_rate_cents(data.r_sum.at(f)) for f in fs]

    # Feasible segment
    feas_lo = data.feasible_min if math.isfinite(data.feasible_min) else lo
    feas_hi = data.feasible_max if math.isfinite(data.feasible_max) else hi
    feas_lo = max(feas_lo, lo)
    feas_hi = min(feas_hi, hi)

    fig = plt.figure(figsize=(16, 7))
    fig.suptitle(data.title, fontsize=11, y=1.01)

    # constrained_layout + 3-D axes don't mix — use explicit spacing instead
    gs = fig.add_gridspec(2, 2, width_ratios=[1, 1.3], hspace=0.08, wspace=0.35)
    ax_win = fig.add_subplot(gs[0, 0])
    ax_sum = fig.add_subplot(gs[1, 0], sharex=ax_win)
    ax3d = fig.add_subplot(gs[:, 1], projection="3d")

    # ── 2-D panels ────────────────────────────────────────────────────────────
    for ax, vals, ylabel, label_text in (
        (ax_win, r_wins, "r_winter  (¢/kWh)", "winter"),
        (ax_sum, r_sums, "r_summer  (¢/kWh)", "summer"),
    ):
        ax.plot(fs, vals, color="#555", linewidth=1.5, zorder=2)
        ax.axhline(0, color="#999", linewidth=0.8, linestyle="--")

        if data.feasible_exists and feas_hi > feas_lo:
            ax.axvspan(feas_lo, feas_hi, alpha=_FEASIBLE_ALPHA, color=_FEASIBLE_COLOR,
                       label="feasible region" if label_text == "winter" else None)

        # Strategy markers
        multi_variant = len({sp.variant for sp in data.strategies if sp.variant}) > 1
        for sp in data.strategies:
            if not math.isfinite(sp.fixed_charge):
                continue
            stored = sp.winter_rate if label_text == "winter" else sp.summer_rate
            affine_line = data.r_win if label_text == "winter" else data.r_sum
            if stored is not None and not math.isnan(stored):
                rate_val = _rate_cents(stored)
            else:
                rate_val = _rate_cents(affine_line.at(sp.fixed_charge))
            color = _CMAP.get(sp.label, "#333")
            marker = "o" if sp.feasible else "x"
            legend_label = (
                f"Strategy {sp.label} ({sp.variant})  F={sp.fixed_charge:.1f}"
                if multi_variant and sp.variant
                else f"Strategy {sp.label}  F={sp.fixed_charge:.1f}"
            )
            ax.scatter(
                sp.fixed_charge, rate_val,
                color=color, s=60, zorder=5, marker=marker,
                label=legend_label,
            )
            annot = f"{sp.label}" + (f"\n{sp.variant}" if multi_variant and sp.variant else "")
            ax.annotate(
                annot,
                (sp.fixed_charge, rate_val),
                textcoords="offset points", xytext=(5, 4),
                fontsize=7, color=color,
            )

        ax.set_ylabel(ylabel, fontsize=9)
        ax.tick_params(labelsize=8)
        ax.grid(True, linewidth=0.4, alpha=0.5)

        # Slope/intercept annotation
        sign = "+" if (data.r_win if label_text == "winter" else data.r_sum).slope >= 0 else "-"
        slope_abs = abs(
            (data.r_win if label_text == "winter" else data.r_sum).slope * 100
        )
        intercept_c = (
            (data.r_win if label_text == "winter" else data.r_sum).intercept * 100
        )
        ax.text(
            0.02, 0.95,
            f"r = {intercept_c:.3f} {sign} {slope_abs:.4f}·F  (¢/kWh, F in $/mo)",
            transform=ax.transAxes, fontsize=7, verticalalignment="top",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.7),
        )

    ax_win.tick_params(labelbottom=False)
    ax_sum.set_xlabel("Fixed charge F  ($/month)", fontsize=9)
    ax_win.legend(fontsize=7, loc="upper right")

    # Feasible interval annotation
    if data.feasible_exists:
        ax_win.set_title(
            f"Feasible F: [{feas_lo:.1f}, {feas_hi:.1f}] $/mo",
            fontsize=8, pad=3,
        )

    # ── 3-D panel ─────────────────────────────────────────────────────────────
    ax3d.set_xlabel("F  ($/mo)", fontsize=8, labelpad=4)
    ax3d.set_ylabel("r_win  (¢/kWh)", fontsize=8, labelpad=4)
    ax3d.set_zlabel("r_sum  (¢/kWh)", fontsize=8, labelpad=4)
    ax3d.tick_params(labelsize=7)

    # Full line (grey)
    ax3d.plot(fs, r_wins, r_sums, color="#aaa", linewidth=1.2, zorder=1, label="full line")

    # Feasible segment (thick green)
    if data.feasible_exists and feas_hi > feas_lo:
        fs_feas = _linspace(max(lo, feas_lo), min(hi, feas_hi), 200)
        rw_feas = [_rate_cents(data.r_win.at(f)) for f in fs_feas]
        rs_feas = [_rate_cents(data.r_sum.at(f)) for f in fs_feas]
        ax3d.plot(
            fs_feas, rw_feas, rs_feas,
            color=_FEASIBLE_COLOR, linewidth=3.0, zorder=3,
            label="feasible segment",
        )

    # r_win = 0 and r_sum = 0 reference planes
    f_grid = [lo, hi]
    r_other_grid = [
        min(r_wins) - 1.0,
        max(r_wins) + 1.0,
    ]
    # r_win = 0 plane: F × r_sum
    import numpy as np  # noqa: PLC0415 — local import to avoid top-level dep
    f_mesh, r_sum_mesh = np.meshgrid(
        np.linspace(lo, hi, 4),
        np.linspace(min(r_sums) - 1.0, max(r_sums) + 1.0, 4),
    )
    ax3d.plot_surface(
        f_mesh, np.zeros_like(f_mesh), r_sum_mesh,
        alpha=_PLANE_ALPHA, color="#e74c3c", zorder=0,
    )
    # r_sum = 0 plane: F × r_win
    f_mesh2, r_win_mesh2 = np.meshgrid(
        np.linspace(lo, hi, 4),
        np.linspace(min(r_wins) - 1.0, max(r_wins) + 1.0, 4),
    )
    ax3d.plot_surface(
        f_mesh2, r_win_mesh2, np.zeros_like(f_mesh2),
        alpha=_PLANE_ALPHA, color="#2980b9", zorder=0,
    )

    # Strategy markers on 3-D
    multi_variant_3d = len({sp.variant for sp in data.strategies if sp.variant}) > 1
    for sp in data.strategies:
        if not math.isfinite(sp.fixed_charge):
            continue
        rw = (
            _rate_cents(sp.winter_rate)
            if sp.winter_rate is not None
            else _rate_cents(data.r_win.at(sp.fixed_charge))
        )
        rs = (
            _rate_cents(sp.summer_rate)
            if sp.summer_rate is not None
            else _rate_cents(data.r_sum.at(sp.fixed_charge))
        )
        color = _CMAP.get(sp.label, "#333")
        marker = "o" if sp.feasible else "^"
        legend_label_3d = (
            f"Strategy {sp.label} ({sp.variant})"
            if multi_variant_3d and sp.variant
            else f"Strategy {sp.label}"
        )
        ax3d.scatter(
            [sp.fixed_charge], [rw], [rs],
            color=color, s=60, marker=marker, zorder=6,
            label=legend_label_3d,
        )
        annot_3d = f"  {sp.label}" + (f"\n  {sp.variant}" if multi_variant_3d and sp.variant else "")
        ax3d.text(
            sp.fixed_charge, rw, rs, annot_3d,
            fontsize=7, color=color,
        )

    ax3d.legend(fontsize=7, loc="upper left")

    # tight_layout doesn't work with mixed 2-D/3-D axes — adjust manually
    fig.subplots_adjust(left=0.08, right=0.98, top=0.92, bottom=0.10)
    output_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_png, dpi=150, bbox_inches="tight")
    plt.close(fig)
    LOGGER.info("Wrote %s", output_png)
    print(f"Wrote {output_png}")


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--run-dir-delivery", required=True,
                        help="CAIRO output dir for delivery (run-1).")
    parser.add_argument("--run-dir-supply", required=True,
                        help="CAIRO output dir for delivery+supply (run-2).")
    parser.add_argument("--resstock-base", required=True,
                        help="Base path to ResStock release (local or S3).")
    parser.add_argument("--state", required=True,
                        help="State abbreviation (e.g. NY).")
    parser.add_argument("--upgrade", default="00",
                        help="ResStock upgrade partition (default: 00).")
    parser.add_argument("--base-tariff-delivery", required=True,
                        help="Calibrated delivery base tariff JSON.")
    parser.add_argument("--base-tariff-supply", required=True,
                        help="Calibrated supply base tariff JSON.")
    parser.add_argument("--group-col", default="has_hp",
                        help="Metadata column identifying the subclass (default: has_hp).")
    parser.add_argument("--subclass-value", default="true",
                        help="Value of group_col for the subclass (default: true).")
    parser.add_argument("--cross-subsidy-col", default="BAT_percustomer",
                        help="BAT column for cross-subsidy (default: BAT_percustomer).")
    parser.add_argument("--periods-yaml", type=Path,
                        help="Optional periods YAML for winter-month configuration.")
    parser.add_argument("--mc-seasonal-ratio-delivery", type=float, default=None,
                        help="Delivery rho_MC (winter_rate/summer_rate). Omit to skip strategy C.")
    parser.add_argument("--mc-seasonal-ratio-supply", type=float, default=None,
                        help="Supply rho_MC. Omit to skip strategy C.")
    parser.add_argument("--variant", choices=["delivery", "supply"], default="delivery",
                        help="Which variant to plot (default: delivery).")
    parser.add_argument("--output-png", type=Path, required=True,
                        help="Output PNG path.")
    parser.add_argument("--fixed-charge-min", type=float)
    parser.add_argument("--fixed-charge-max", type=float)
    parser.add_argument("--title")

    args = parser.parse_args()

    all_data = compute_feasible_line_from_runs(
        run_dir_delivery=args.run_dir_delivery,
        run_dir_supply=args.run_dir_supply,
        resstock_base=args.resstock_base,
        state=args.state,
        upgrade=args.upgrade,
        path_base_tariff_delivery=args.base_tariff_delivery,
        path_base_tariff_supply=args.base_tariff_supply,
        group_col=args.group_col,
        subclass_value=args.subclass_value,
        cross_subsidy_col=args.cross_subsidy_col,
        path_periods_yaml=args.periods_yaml,
        mc_seasonal_ratio_delivery=args.mc_seasonal_ratio_delivery,
        mc_seasonal_ratio_supply=args.mc_seasonal_ratio_supply,
        title_delivery=args.title if args.variant == "delivery" else None,
        title_supply=args.title if args.variant == "supply" else None,
    )
    data = all_data[args.variant]
    _verify_affine_vs_strategies(data)
    plot_feasible_line(data, args.output_png, args.fixed_charge_min, args.fixed_charge_max)


if __name__ == "__main__":
    main()
