"""Fit logistic S-curves to digitized NYISO data and write adoption config YAML.

Source: NYISO Gold Book 2025, "Number of Residential Households Converted to
Electric Heating By Technology (NYCA)" stacked-area chart.

Parametric form: f(t) = L / (1 + exp(-k * (t - t0)))
  L  = long-run saturation fraction of all NYCA housing units
  k  = growth rate
  t0 = inflection year

Fractions are normalized by 7,900,000 total NYCA occupied housing units
(Census ACS / NYISO Gold Book 2025 estimate). 2025 is forced to 0.0 — all
buildings remain at upgrade-0 baseline — regardless of the logistic value.

Technology → ResStock upgrade mapping:
  ASHP Full Capacity  → 2  (cold-climate ASHP, 90% capacity @ 5F, elec backup)
  ASHP Dual Fuel      → 4  (ENERGY STAR ASHP + existing fossil backup)
  Ground Source HP    → 5  (geothermal heat pump)
  Supplemental Heat   → 1  (ENERGY STAR ASHP, 50% capacity @ 5F, elec backup)
  Electric Resistance → baseline upgrade 0, already captured there

Usage::

    uv run python utils/pre/fit_adoption_config.py \\
        --output rate_design/hp_rates/ny/config/adoption/nyca_electrification.yaml \\
        --plot-output rate_design/hp_rates/ny/config/adoption/nyca_electrification_curves.png
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import polars as pl
from plotnine import (
    aes,
    element_line,
    element_text,
    geom_area,
    geom_line,
    geom_point,
    geom_vline,
    ggplot,
    labs,
    scale_color_manual,
    scale_fill_manual,
    scale_x_continuous,
    scale_y_continuous,
    theme,
    theme_minimal,
)
from scipy.optimize import curve_fit

from buildstock_fetch.scenarios import validate_scenario

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source data (NYISO Gold Book 2025)
# ---------------------------------------------------------------------------

SCENARIO_NAME = "nyca_electrification"
RANDOM_SEED = 42

# Total NYCA occupied housing units used as the fraction denominator.
TOTAL_HU = 7_900_000.0

# Digitized from the NYISO Gold Book 2025 NYCA stacked-area chart.
# Each entry: (calendar_year, individual_technology_housing_units_in_thousands).
# 2025 is forced to 0.0 by the evaluation logic below.
_RAW_DATA: dict[int, list[tuple[int, float]]] = {
    2: [  # ASHP Full Capacity
        (2030, 75),
        (2035, 250),
        (2040, 640),
        (2045, 1050),
        (2050, 1400),
        (2057, 1650),
    ],
    4: [  # ASHP Dual Fuel
        (2030, 65),
        (2035, 205),
        (2040, 440),
        (2045, 660),
        (2050, 750),
        (2057, 850),
    ],
    5: [  # Ground Source HP
        (2030, 10),
        (2035, 20),
        (2040, 50),
        (2045, 75),
        (2050, 85),
        (2057, 90),
    ],
    1: [  # Supplemental Heat
        (2030, 35),
        (2035, 210),
        (2040, 440),
        (2045, 870),
        (2050, 950),
        (2057, 1000),
    ],
}

_UPGRADE_LABELS: dict[int, str] = {
    2: "ASHP full capacity",
    4: "ASHP dual fuel",
    5: "ground source HP",
    1: "supplemental heat",
}

# Wong colorblind-friendly palette matched to NYISO chart hues.
_UPGRADE_COLORS: dict[int, str] = {
    2: "#D55E00",  # vermillion / orange
    4: "#999999",  # gray
    5: "#0072B2",  # blue
    1: "#009E73",  # green
}

_DEFAULT_RUN_YEARS: list[int] = [2025, 2030, 2035, 2040, 2045, 2050]

# ---------------------------------------------------------------------------
# Logistic model
# ---------------------------------------------------------------------------


def _logistic(t: np.ndarray, L: float, k: float, t0: float) -> np.ndarray:
    return L / (1.0 + np.exp(-k * (t - t0)))


def _fit_logistic(years: np.ndarray, fracs: np.ndarray) -> tuple[float, float, float]:
    """Fit logistic to (years, fracs); return (L, k, t0)."""
    L_min = float(fracs.max()) * 1.01
    p0 = [fracs.max() * 1.5, 0.10, 2045.0]
    bounds = ([L_min, 0.001, 2020.0], [1.0, 1.0, 2080.0])
    popt, _ = curve_fit(_logistic, years, fracs, p0=p0, bounds=bounds, maxfev=20_000)
    return float(popt[0]), float(popt[1]), float(popt[2])


# ---------------------------------------------------------------------------
# Fit
# ---------------------------------------------------------------------------


def fit_all(
    run_years: list[int],
) -> tuple[dict[int, list[float]], dict[int, tuple[float, float, float]]]:
    """Fit logistic curves; return ``(scenario_fracs, params)``.

    ``scenario_fracs[upgrade_id][i]`` is the adoption fraction at
    ``run_years[i]``.  2025 is forced to ``0.0``.
    """
    scenario: dict[int, list[float]] = {}
    params: dict[int, tuple[float, float, float]] = {}

    for upgrade_id, pts in _RAW_DATA.items():
        years_arr = np.array([y for y, _ in pts], dtype=float)
        fracs_arr = np.array([hu * 1_000 / TOTAL_HU for _, hu in pts])

        L, k, t0 = _fit_logistic(years_arr, fracs_arr)
        params[upgrade_id] = (L, k, t0)
        log.info(
            "upgrade %d (%s): L=%.4f  k=%.4f  t0=%.1f",
            upgrade_id,
            _UPGRADE_LABELS[upgrade_id],
            L,
            k,
            t0,
        )

        year_fracs: list[float] = []
        for yr in run_years:
            if yr <= 2025:
                year_fracs.append(0.0)
            else:
                val = float(_logistic(np.array([float(yr)]), L, k, t0)[0])
                year_fracs.append(round(val, 4))
        scenario[upgrade_id] = year_fracs

    return scenario, params


# ---------------------------------------------------------------------------
# YAML writer
# ---------------------------------------------------------------------------


def write_yaml(
    path_output: Path,
    scenario: dict[int, list[float]],
    params: dict[int, tuple[float, float, float]],
    run_years: list[int],
) -> None:
    """Write adoption config YAML with full methodology commentary."""
    param_block = "\n".join(
        f"#   upgrade {uid} ({_UPGRADE_LABELS[uid]}):  "
        f"L={params[uid][0]:.4f}  k={params[uid][1]:.4f}  t0={params[uid][2]:.1f}"
        for uid in [2, 4, 5, 1]
    )

    scenario_block = "\n".join(
        f"  {uid}: [{', '.join(f'{v:.4f}' for v in scenario[uid])}]"
        f"  # {_UPGRADE_LABELS[uid]}"
        for uid in [2, 4, 5, 1]
    )

    year_labels_str = "[" + ", ".join(str(y) for y in run_years) + "]"

    lines = [
        "# NYCA building electrification adoption trajectory (NYISO Gold Book 2025).",
        "# Generated by utils/pre/fit_adoption_config.py — do not edit by hand.",
        "#",
        "# Fractions represent the share of total NYCA buildings assigned to each",
        "# ResStock upgrade at each year. Remaining buildings stay at upgrade 0 (baseline).",
        "# Year indices map to calendar years via year_labels.",
        "#",
        "# Technology → ResStock upgrade mapping:",
        "#   ASHP Full Capacity  → 2  (cold-climate ASHP, 90% capacity @ 5F, elec backup)",
        "#   ASHP Dual Fuel      → 4  (ENERGY STAR ASHP + existing fossil backup)",
        "#   Ground Source HP    → 5  (geothermal heat pump)",
        "#   Supplemental Heat   → 1  (ENERGY STAR ASHP, 50% capacity @ 5F, elec backup)",
        "#   Electric Resistance → baseline upgrade 0, already captured there",
        "#",
        "# Methodology: logistic S-curves  f(t) = L / (1 + exp(-k * (t - t0)))  fit",
        "# (scipy curve_fit) to housing-unit counts digitized from the NYISO Gold",
        f"# Book 2025 NYCA stacked-area chart. Denominator: {TOTAL_HU:,.0f} total NYCA",
        "# occupied housing units (Census ACS / NYISO estimate). 2025 forced to 0.0",
        "# (all buildings at upgrade-0 baseline).",
        "#",
        "# Fitted parameters:",
        param_block,
        "",
        f"scenario_name: {SCENARIO_NAME}",
        f"random_seed: {RANDOM_SEED}",
        "",
        "scenario:",
        scenario_block,
        "",
        "# Calendar years for each scenario index (= run years).",
        "# Aligns with Cambium 5-year MC intervals; 2025 is baseline.",
        f"year_labels: {year_labels_str}",
        "",
    ]

    path_output.parent.mkdir(parents=True, exist_ok=True)
    path_output.write_text("\n".join(lines), encoding="utf-8")
    log.info("wrote %s", path_output)


# ---------------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------------

_PLOT_YEARS_DENSE = np.linspace(2024, 2058, 400)


def make_plot(
    params: dict[int, tuple[float, float, float]],
    run_years: list[int],
    path_plot: Path,
) -> None:
    """Save a plotnine figure: continuous logistic curves + digitized points."""
    # Build long-format DataFrame for fitted curves.
    curve_rows: list[dict] = []
    for uid, (L, k, t0) in params.items():
        fracs = _logistic(_PLOT_YEARS_DENSE, L, k, t0)
        for yr, frac in zip(_PLOT_YEARS_DENSE, fracs):
            pct = float(frac) * 100.0
            # Clip negative values that arise from logistic tails near 2024.
            curve_rows.append(
                {
                    "year": float(yr),
                    "technology": _UPGRADE_LABELS[uid],
                    "pct": max(pct, 0.0),
                }
            )

    curves_df = pl.DataFrame(curve_rows)

    # Build long-format DataFrame for digitized source points (excluding 2025=0).
    point_rows: list[dict] = []
    for uid, pts in _RAW_DATA.items():
        for yr, hu_k in pts:
            point_rows.append(
                {
                    "year": float(yr),
                    "technology": _UPGRADE_LABELS[uid],
                    "pct": hu_k * 1_000 / TOTAL_HU * 100.0,
                }
            )

    points_df = pl.DataFrame(point_rows)

    # Ordered technology names for the legend (matches NYISO chart order, bottom→top).
    tech_order = [_UPGRADE_LABELS[uid] for uid in [2, 4, 5, 1]]
    color_map = {_UPGRADE_LABELS[uid]: _UPGRADE_COLORS[uid] for uid in [2, 4, 5, 1]}

    # Convert to pandas for plotnine; use pandas Categorical for legend order.
    import pandas as pd  # noqa: PLC0415

    curves_pd = curves_df.to_pandas()
    curves_pd["technology"] = pd.Categorical(
        curves_pd["technology"], categories=tech_order
    )

    points_pd = points_df.to_pandas()
    points_pd["technology"] = pd.Categorical(
        points_pd["technology"], categories=tech_order
    )

    vlines_df = pl.DataFrame(
        {"year": [float(y) for y in run_years if y > 2025]}
    ).to_pandas()

    p = (
        ggplot()
        + geom_vline(
            data=vlines_df,
            mapping=aes(xintercept="year"),
            color="#cccccc",
            linetype="dashed",
            size=0.5,
        )
        + geom_line(
            data=curves_pd,
            mapping=aes(x="year", y="pct", color="technology"),
            size=1.0,
        )
        + geom_point(
            data=points_pd,
            mapping=aes(x="year", y="pct", color="technology"),
            size=2.5,
            shape="o",
            fill="white",
            stroke=1.2,
        )
        + scale_color_manual(values=color_map, breaks=tech_order)
        + scale_x_continuous(
            breaks=list(range(2025, 2060, 5)),
            limits=(2024, 2058),
        )
        + scale_y_continuous(
            labels=lambda x: [f"{v:.0f}%" for v in x],
        )
        + labs(
            title="NYCA HP adoption trajectory — NYISO Gold Book 2025 logistic fit",
            x="Year",
            y="Share of NYCA housing units",
            color="Technology",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=11),
            axis_title=element_text(size=10),
            legend_title=element_text(size=9),
            legend_text=element_text(size=9),
            panel_grid_minor=element_line(size=0),
        )
    )

    path_plot.parent.mkdir(parents=True, exist_ok=True)
    p.save(str(path_plot), dpi=150, width=9, height=5)
    log.info("wrote %s", path_plot)


def make_stacked_plot(
    params: dict[int, tuple[float, float, float]],
    run_years: list[int],
    path_plot: Path,
) -> None:
    """Save a stacked area chart matching the NYISO Gold Book visual style."""
    import pandas as pd  # noqa: PLC0415

    # Stacking order bottom→top mirrors the NYISO chart.
    stack_order = [_UPGRADE_LABELS[uid] for uid in [2, 4, 5, 1]]
    fill_map = {_UPGRADE_LABELS[uid]: _UPGRADE_COLORS[uid] for uid in [2, 4, 5, 1]}

    curve_rows: list[dict] = []
    for uid, (L, k, t0) in params.items():
        fracs = _logistic(_PLOT_YEARS_DENSE, L, k, t0)
        for yr, frac in zip(_PLOT_YEARS_DENSE, fracs):
            curve_rows.append(
                {
                    "year": float(yr),
                    "technology": _UPGRADE_LABELS[uid],
                    "pct": max(float(frac) * 100.0, 0.0),
                }
            )

    curves_pd = pl.DataFrame(curve_rows).to_pandas()
    # Reverse stack order so the first level sits at the bottom in geom_area.
    curves_pd["technology"] = pd.Categorical(
        curves_pd["technology"], categories=stack_order
    )

    vlines_df = pl.DataFrame(
        {"year": [float(y) for y in run_years if y > 2025]}
    ).to_pandas()

    p = (
        ggplot(curves_pd, aes(x="year", y="pct", fill="technology"))
        + geom_area(position="stack", alpha=0.9)
        + geom_vline(
            data=vlines_df,
            mapping=aes(xintercept="year"),
            color="white",
            linetype="dashed",
            size=0.5,
            alpha=0.7,
        )
        + scale_fill_manual(values=fill_map, breaks=stack_order)
        + scale_x_continuous(
            breaks=list(range(2025, 2060, 5)),
            limits=(2024, 2058),
        )
        + scale_y_continuous(
            labels=lambda x: [f"{v:.0f}%" for v in x],
        )
        + labs(
            title="NYCA HP adoption trajectory — NYISO Gold Book 2025 logistic fit (stacked)",
            x="Year",
            y="Share of NYCA housing units",
            fill="Technology",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(size=11),
            axis_title=element_text(size=10),
            legend_title=element_text(size=9),
            legend_text=element_text(size=9),
            panel_grid_minor=element_line(size=0),
        )
    )

    path_plot.parent.mkdir(parents=True, exist_ok=True)
    p.save(str(path_plot), dpi=150, width=9, height=5)
    log.info("wrote %s", path_plot)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fit NYISO adoption S-curves and write adoption config YAML.",
    )
    parser.add_argument(
        "--output",
        required=True,
        metavar="PATH",
        dest="path_output",
        help="Destination YAML path (e.g. rate_design/hp_rates/ny/config/adoption/nyca_electrification.yaml).",
    )
    parser.add_argument(
        "--plot-output",
        metavar="PATH",
        dest="path_plot",
        default=None,
        help="Optional path for the curve-fit line plot (.png).",
    )
    parser.add_argument(
        "--stacked-plot-output",
        metavar="PATH",
        dest="path_stacked_plot",
        default=None,
        help="Optional path for the stacked area plot (.png).",
    )
    parser.add_argument(
        "--run-years",
        metavar="YEARS",
        default=",".join(str(y) for y in _DEFAULT_RUN_YEARS),
        help=(
            "Comma-separated calendar years to evaluate and write to the YAML. "
            f"Default: {','.join(str(y) for y in _DEFAULT_RUN_YEARS)}"
        ),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_years = [int(y.strip()) for y in args.run_years.split(",")]

    scenario, params = fit_all(run_years)

    validate_scenario({uid: scenario[uid] for uid in scenario})

    # Log per-year totals.
    for i, yr in enumerate(run_years):
        total = sum(scenario[uid][i] for uid in scenario)
        log.info("year %d: total fraction = %.4f", yr, total)

    write_yaml(Path(args.path_output), scenario, params, run_years)

    if args.path_plot:
        make_plot(params, run_years, Path(args.path_plot))

    if args.path_stacked_plot:
        make_stacked_plot(params, run_years, Path(args.path_stacked_plot))


if __name__ == "__main__":
    main()
