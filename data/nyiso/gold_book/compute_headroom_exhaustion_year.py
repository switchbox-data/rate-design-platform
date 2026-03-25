"""Compute the year each NY utility exhausts its distribution winter headroom.

Uses NYISO Gold Book Table I-4b (Winter Non-Coincident Peak Demand by Zone)
and utility-to-zone mappings from generate_zone_mapping_csv.py to project when
each utility's winter peak growth will consume available distribution headroom.

Also plots building electrification load growth by utility from Table I-13c
(Building Electrification Winter Coincident Peak Demand by Zone).

Source for headroom data: Table 2 from Switchbox distribution headroom analysis.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import openpyxl

ZONES = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"]

UTILITY_ZONES: dict[str, list[str]] = {
    "National Grid": ["A", "B", "C", "D", "E", "F"],
    "Central Hudson Gas and Electric": ["G"],
    "NYS Electric & Gas and RGE": ["A", "B", "C", "D", "E", "F"],
    "Con Edison": ["G", "H", "I", "J"],
    "Orange and Rockland Utilities": ["G"],
}

# Distribution winter peak (MW) and total estimated winter headroom (MW)
# from Table 2 of the Switchbox distribution headroom analysis.
HEADROOM_TABLE: dict[str, dict[str, int | float]] = {
    "National Grid": {"dist_peak_mw": 4276, "headroom_mw": 4477, "headroom_pct": 1.05},
    "Central Hudson Gas and Electric": {
        "dist_peak_mw": 796,
        "headroom_mw": 279,
        "headroom_pct": 0.35,
    },
    "NYS Electric & Gas and RGE": {
        "dist_peak_mw": 3786,
        "headroom_mw": 3235,
        "headroom_pct": 0.85,
    },
    "Con Edison": {"dist_peak_mw": 4691, "headroom_mw": 1346, "headroom_pct": 0.29},
    "Orange and Rockland Utilities": {
        "dist_peak_mw": 1123,
        "headroom_mw": 1071,
        "headroom_pct": 0.95,
    },
}


def parse_gold_book_winter_peak(
    xlsx_path: Path,
) -> dict[str, dict[str, int]]:
    """Parse Table I-4b (Winter Non-Coincident Peak Demand by Zone) from the
    Gold Book Excel file.

    Returns {year_str: {zone_letter: MW}}.
    """
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb["I-4b"]

    zone_data: dict[str, dict[str, int]] = {}
    for row in ws.iter_rows(min_row=7, values_only=True):
        year = row[2]
        if year is None or not isinstance(year, str) or "-" not in year:
            continue
        vals = row[3:14]
        if any(v is None for v in vals):
            continue
        zone_data[year] = {z: int(v) for z, v in zip(ZONES, vals)}

    wb.close()
    return zone_data


def compute_headroom_exhaustion(
    zone_data: dict[str, dict[str, int]],
    baseline_year: str = "2024-25",
) -> list[dict[str, str | int]]:
    """For each utility, find the first forecast year where zone-sum peak
    exceeds baseline * (1 + headroom_pct)."""
    results: list[dict[str, str | int]] = []

    for utility, zones in UTILITY_ZONES.items():
        info = HEADROOM_TABLE[utility]
        gb_baseline = sum(zone_data[baseline_year][z] for z in zones)
        threshold = gb_baseline * (1 + info["headroom_pct"])

        hit_year = None
        hit_peak = None
        last_year = None
        last_peak = None

        for year_str in sorted(zone_data.keys()):
            start_yr = int(year_str.split("-")[0])
            if start_yr < 2024:
                continue
            peak = sum(zone_data[year_str][z] for z in zones)
            if peak >= threshold and hit_year is None:
                hit_year = year_str
                hit_peak = peak
            last_year = year_str
            last_peak = peak

        results.append(
            {
                "utility": utility,
                "zones": ", ".join(zones),
                "dist_winter_peak_2024_mw": info["dist_peak_mw"],
                "headroom_mw": info["headroom_mw"],
                "headroom_pct_of_winter_peak": f"{info['headroom_pct'] * 100:.0f}%",
                "gb_2024_zone_sum_mw": gb_baseline,
                "gb_threshold_mw": round(threshold),
                "year_headroom_exhausted": hit_year
                if hit_year
                else f"Beyond {last_year}",
                "gb_peak_at_exhaustion_mw": hit_peak if hit_peak else last_peak,
            }
        )

    return results


def parse_gold_book_elec_load(
    xlsx_path: Path,
) -> dict[str, dict[str, int]]:
    """Parse Table I-13c (Building Electrification Winter Coincident Peak Demand
    by Zone) from the Gold Book Excel file.

    Returns {year_str: {zone_letter: MW}}.
    """
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb["I-13c"]

    zone_data: dict[str, dict[str, int]] = {}
    for row in ws.iter_rows(min_row=7, values_only=True):
        year = row[2]
        if year is None or not isinstance(year, str) or "-" not in year:
            continue
        vals = row[3:14]
        if any(v is None for v in vals):
            continue
        zone_data[year] = {z: int(v) for z, v in zip(ZONES, vals)}

    wb.close()
    return zone_data


def plot_elec_load_by_utility(
    elec_data: dict[str, dict[str, int]],
    headroom_results: list[dict[str, str | int]],
    path_output: Path,
) -> None:
    """Plot building electrification winter coincident peak demand by utility,
    with a vertical line marking each utility's headroom exhaustion year."""
    # Aggregate by utility over forecast horizon
    years = sorted(elec_data.keys())
    x = [int(y.split("-")[0]) for y in years]

    exhaustion_years: dict[str, int | None] = {
        r["utility"]: (
            int(str(r["year_headroom_exhausted"]).split("-")[0])
            if not str(r["year_headroom_exhausted"]).startswith("Beyond")
            else None
        )
        for r in headroom_results
    }

    n = len(UTILITY_ZONES)
    fig, axes = plt.subplots(n, 1, figsize=(10, 3.2 * n), sharex=True)
    fig.suptitle(
        "Building Electrification Winter Coincident Peak Demand by Utility\n"
        "(NYISO Gold Book Table I-13c, zone sums)",
        fontsize=13,
        fontweight="bold",
        y=1.01,
    )

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]

    for ax, (utility, zones), color in zip(axes, UTILITY_ZONES.items(), colors):
        elec_mw = [sum(elec_data[yr][z] for z in zones) for yr in years]

        ax.fill_between(x, elec_mw, alpha=0.2, color=color)
        ax.plot(x, elec_mw, color=color, linewidth=2)

        ex_yr = exhaustion_years.get(utility)
        if ex_yr is not None:
            ax.axvline(ex_yr, color="red", linestyle="--", linewidth=1.4, alpha=0.8)
            ax.text(
                ex_yr + 0.4,
                max(elec_mw) * 0.05,
                f"Headroom\nexhausted\n{ex_yr}–{ex_yr - 1999:02d}",
                color="red",
                fontsize=7.5,
                va="bottom",
            )

        ax.set_title(
            f"{utility}  (zones {', '.join(zones)})",
            fontsize=10,
            loc="left",
            pad=4,
        )
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
        ax.set_ylabel("MW", fontsize=9)
        ax.grid(axis="y", linestyle=":", alpha=0.5)
        ax.set_xlim(x[0], x[-1])

    axes[-1].set_xlabel("Winter season starting year", fontsize=10)

    fig.tight_layout()
    fig.savefig(path_output, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved plot to {path_output}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path-xlsx",
        type=Path,
        default=Path(__file__).parent
        / "2024-Gold-Book-Baseline-Forecast-Tables (2).xlsx",
        help="Path to the Gold Book Excel file",
    )
    parser.add_argument(
        "--path-output",
        type=Path,
        default=Path(__file__).parent / "headroom_exhaustion_year_by_utility.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--path-output-plot",
        type=Path,
        default=Path(__file__).parent
        / "building_electrification_load_growth_by_utility.png",
        help="Output PNG path for electrification load growth plot",
    )
    args = parser.parse_args()

    zone_data = parse_gold_book_winter_peak(args.path_xlsx)
    results = compute_headroom_exhaustion(zone_data)

    with open(args.path_output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"Saved {len(results)} rows to {args.path_output}")
    for r in results:
        print(
            f"  {r['utility']}: headroom exhausted {r['year_headroom_exhausted']}"
            f" (zones {r['zones']}, threshold {r['gb_threshold_mw']} MW)"
        )

    elec_data = parse_gold_book_elec_load(args.path_xlsx)
    plot_elec_load_by_utility(elec_data, results, args.path_output_plot)


if __name__ == "__main__":
    main()
