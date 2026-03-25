"""Compute the year each NY utility exhausts its distribution winter headroom.

Uses NYISO Gold Book Table I-4b (Winter Non-Coincident Peak Demand by Zone)
and utility-to-zone mappings from generate_zone_mapping_csv.py to project when
each utility's winter peak growth will consume available distribution headroom.

Also produces two plots:
1. Stacked area of all I-1d demand components (additions and reductions)
   as % of 2023-24 coincident winter peak, by utility.
2. Building electrification only (I-13c) as % of 2023-24 coincident winter peak.

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


def _parse_zone_sheet_winter_str(
    xlsx_path: Path,
    sheet: str,
    min_row: int = 7,
) -> dict[str, dict[str, int]]:
    """Parse a Gold Book zone-by-year table where the year column contains
    "YYYY-YY" winter season strings (e.g. "2024-25").

    Returns {year_str: {zone_letter: MW}}.
    """
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb[sheet]

    zone_data: dict[str, dict[str, int]] = {}
    for row in ws.iter_rows(min_row=min_row, values_only=True):
        year = row[2]
        if year is None or not isinstance(year, str) or "-" not in year:
            continue
        vals = row[3:14]
        if any(v is None for v in vals):
            continue
        zone_data[year] = {z: int(v) for z, v in zip(ZONES, vals)}

    wb.close()
    return zone_data


def _parse_zone_sheet_calendar_int(
    xlsx_path: Path,
    sheet: str,
    min_row: int = 7,
) -> dict[str, dict[str, int]]:
    """Parse a Gold Book zone-by-year table where the year column is an integer
    calendar year. Returns keys normalised to "YYYY-YY" winter season strings
    matching the convention used by the other tables (year N -> "N-NN").
    """
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb[sheet]

    zone_data: dict[str, dict[str, int]] = {}
    for row in ws.iter_rows(min_row=min_row, values_only=True):
        year = row[2]
        if not isinstance(year, int):
            continue
        vals = row[3:14]
        if any(v is None for v in vals):
            continue
        # Map calendar year N to winter season "N-NN" (e.g. 2024 -> "2024-25")
        key = f"{year}-{(year + 1) % 100:02d}"
        zone_data[key] = {z: int(v) for z, v in zip(ZONES, vals)}

    wb.close()
    return zone_data


def parse_gold_book_winter_peak(xlsx_path: Path) -> dict[str, dict[str, int]]:
    """Parse Table I-4b (Winter Non-Coincident Peak Demand by Zone)."""
    return _parse_zone_sheet_winter_str(xlsx_path, "I-4b")


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


def parse_gold_book_elec_load(xlsx_path: Path) -> dict[str, dict[str, int]]:
    """Parse Table I-13c (Building Electrification Winter Coincident Peak Demand
    by Zone) — cumulative future load additions in MW."""
    return _parse_zone_sheet_winter_str(xlsx_path, "I-13c")


def parse_gold_book_coincident_winter_peak(
    xlsx_path: Path,
) -> dict[str, dict[str, int]]:
    """Parse Table I-3b (Baseline Winter Coincident Peak Demand by Zone)."""
    return _parse_zone_sheet_winter_str(xlsx_path, "I-3b")


def parse_gold_book_ee_reductions(xlsx_path: Path) -> dict[str, dict[str, int]]:
    """Parse Table I-8c (EE & Codes/Standards Winter Coincident Peak Reductions
    by Zone, relative to 2023-24) — positive values = reductions."""
    return _parse_zone_sheet_winter_str(xlsx_path, "I-8c")


def parse_gold_book_ev_load(xlsx_path: Path) -> dict[str, dict[str, int]]:
    """Parse Table I-11d (EV Winter Coincident Peak Demand by Zone)."""
    return _parse_zone_sheet_winter_str(xlsx_path, "I-11d")


def parse_gold_book_storage_reductions(xlsx_path: Path) -> dict[str, dict[str, int]]:
    """Parse Table I-12c (Storage Winter Coincident Peak Reductions by Zone)
    — uses integer calendar year; keys are normalised to "YYYY-YY"."""
    return _parse_zone_sheet_calendar_int(xlsx_path, "I-12c")


def parse_gold_book_nondg_reductions(xlsx_path: Path) -> dict[str, dict[str, int]]:
    """Parse Table I-10c (Non-Solar DG Winter Coincident Peak Reductions by Zone)
    — uses integer calendar year; keys are normalised to "YYYY-YY"."""
    return _parse_zone_sheet_calendar_int(xlsx_path, "I-10c")


def parse_gold_book_large_loads(xlsx_path: Path) -> dict[str, dict[str, int]]:
    """Parse the 'Winter Peak Demand by Zone' section of Table I-14
    (Large Loads Forecast) — starts at row 41 of the I-14 sheet."""
    return _parse_zone_sheet_winter_str(xlsx_path, "I-14", min_row=41)


def _zone_sum_series(
    data: dict[str, dict[str, int]],
    years: list[str],
    zones: list[str],
) -> list[float]:
    """Return a list of zone-summed values for each year, defaulting to 0 if
    a year is absent from the data (some tables have a shorter horizon)."""
    return [
        float(sum(data[yr][z] for z in zones)) if yr in data else 0.0 for yr in years
    ]


def plot_demand_components_by_utility(
    coincident_peak_data: dict[str, dict[str, int]],
    ee_data: dict[str, dict[str, int]],
    nondg_data: dict[str, dict[str, int]],
    storage_data: dict[str, dict[str, int]],
    ev_data: dict[str, dict[str, int]],
    elec_data: dict[str, dict[str, int]],
    large_load_data: dict[str, dict[str, int]],
    headroom_results: list[dict[str, str | int]],
    path_output: Path,
    baseline_year: str = "2023-24",
) -> None:
    """Stacked area chart of all I-1d winter coincident peak demand components
    by utility, expressed as % of the 2023-24 coincident winter peak.

    Additions (EV, building electrification, large loads) stack above zero.
    Reductions (EE, non-solar DG, storage) stack below zero.
    """
    # Use the union of years from all datasets that start 2024 or later,
    # anchored to the elec_data years as the reference set.
    all_years = sorted({yr for yr in elec_data if int(yr.split("-")[0]) >= 2024})
    x = [int(y.split("-")[0]) for y in all_years]

    exhaustion_years: dict[str, int | None] = {
        r["utility"]: (
            int(str(r["year_headroom_exhausted"]).split("-")[0])
            if not str(r["year_headroom_exhausted"]).startswith("Beyond")
            else None
        )
        for r in headroom_results
    }

    # Component definitions: (label, data_dict, sign, color)
    # sign=+1 → addition (stacks above 0), sign=-1 → reduction (stacks below 0)
    components: list[tuple[str, dict[str, dict[str, int]], int, str]] = [
        ("Building electrification", elec_data, +1, "#e6550d"),
        ("EV load", ev_data, +1, "#fdae6b"),
        ("Large loads", large_load_data, +1, "#756bb1"),
        ("EE & codes/standards", ee_data, -1, "#2ca02c"),
        ("Non-solar DG (BTM)", nondg_data, -1, "#74c476"),
        ("BTM storage", storage_data, -1, "#9edae5"),
    ]

    n = len(UTILITY_ZONES)
    fig, axes = plt.subplots(n, 1, figsize=(11, 3.6 * n), sharex=True)
    fig.suptitle(
        "Winter Coincident Peak Demand Components by Utility\n"
        f"as % of {baseline_year} coincident winter peak (I-3b) — NYISO Gold Book I-1d",
        fontsize=12,
        fontweight="bold",
        y=1.01,
    )

    for ax, (utility, zones) in zip(axes, UTILITY_ZONES.items()):
        base_mw = sum(coincident_peak_data[baseline_year][z] for z in zones)

        pos_bottom = [0.0] * len(all_years)
        neg_bottom = [0.0] * len(all_years)

        legend_handles = []
        for label, data, sign, color in components:
            raw = _zone_sum_series(data, all_years, zones)
            # pct is always a positive magnitude; sign controls which side of 0
            pct = [100.0 * v / base_mw for v in raw]

            if sign == +1:
                top = [b + v for b, v in zip(pos_bottom, pct)]
                handle = ax.fill_between(
                    x, pos_bottom, top, alpha=0.85, color=color, label=label
                )
                pos_bottom = top
            else:
                bottom = [b - v for b, v in zip(neg_bottom, pct)]
                handle = ax.fill_between(
                    x, bottom, neg_bottom, alpha=0.85, color=color, label=label
                )
                neg_bottom = bottom
            legend_handles.append(handle)

        ax.axhline(0, color="black", linewidth=0.8, linestyle="-")

        ex_yr = exhaustion_years.get(utility)
        if ex_yr is not None:
            ax.axvline(ex_yr, color="red", linestyle="--", linewidth=1.4, alpha=0.9)
            ax.text(
                ex_yr + 0.3,
                max(pos_bottom) * 0.97,
                f"Headroom\nexhausted\n{ex_yr}–{ex_yr - 1999:02d}",
                color="red",
                fontsize=7.5,
                va="top",
            )

        ax.set_title(
            f"{utility}  (zones {', '.join(zones)},  "
            f"{baseline_year} base = {base_mw:,} MW)",
            fontsize=9.5,
            loc="left",
            pad=4,
        )
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
        ax.set_ylabel(f"% of {baseline_year} peak", fontsize=9)
        ax.grid(axis="y", linestyle=":", alpha=0.5)
        ax.set_xlim(x[0], x[-1])

    axes[-1].set_xlabel("Winter season starting year", fontsize=10)

    # Single legend below the bottom panel
    axes[-1].legend(
        loc="upper left",
        fontsize=8.5,
        framealpha=0.9,
        ncol=3,
    )

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
        default=Path(__file__).parent / "demand_components_by_utility.png",
        help="Output PNG path for demand components stacked area plot",
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

    coincident_peak_data = parse_gold_book_coincident_winter_peak(args.path_xlsx)
    elec_data = parse_gold_book_elec_load(args.path_xlsx)
    ee_data = parse_gold_book_ee_reductions(args.path_xlsx)
    ev_data = parse_gold_book_ev_load(args.path_xlsx)
    storage_data = parse_gold_book_storage_reductions(args.path_xlsx)
    nondg_data = parse_gold_book_nondg_reductions(args.path_xlsx)
    large_load_data = parse_gold_book_large_loads(args.path_xlsx)

    plot_demand_components_by_utility(
        coincident_peak_data,
        ee_data,
        nondg_data,
        storage_data,
        ev_data,
        elec_data,
        large_load_data,
        results,
        args.path_output_plot,
    )


if __name__ == "__main__":
    main()
