#!/usr/bin/env python3
"""Compare ISO-NE Connecticut zone load to Eversource billed kWh.

``utility=ct_eversource`` hourly demand is the ISO-NE Connecticut load zone
(location 4004), the same series stored under ``utility=ct_ui``. Each hour is
``load_mw``, so a monthly sum is megawatt-hours and times 1,000 is kilowatt-hours.
The filing Total row is Eversource billed kilowatt-hours by month (CIEC-020
Attachment 2, test year 2025), which excludes United Illuminating, the
Connecticut municipal utilities, and losses.

Usage:
    uv run python data/isone/hourly_demand/compare_ct_zone_load_to_eversource_sales.py \\
        --path-csv data/isone/hourly_demand/ct_eversource_ciec020_kwh_ty2025.csv \\
        --path-local-utilities data/isone/hourly_demand/utilities \\
        --year 2025 \\
        --utility ct_eversource
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import polars as pl

MONTHS: tuple[str, ...] = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
MONTH_LABELS: tuple[str, ...] = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)


def parse_kwh(raw: str) -> int:
    """Parse a filing cell such as ``"  1,932,841,046 "`` or ``"(28,934)"``."""
    text = "".join(raw.split()).replace(",", "")
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]
    try:
        value = int(text)
    except ValueError as exc:
        raise ValueError(f"Cannot parse kWh value {raw!r}") from exc
    return -value if negative else value


def load_filing_monthly_kwh(path_csv: Path) -> dict[int, int]:
    """Return month number -> Total-row kilowatt-hours from the filing CSV."""
    with path_csv.open(newline="") as handle:
        rows = list(csv.reader(handle))
    header_idx = next(
        (i for i, row in enumerate(rows) if row and row[0].strip() == "Rate"),
        None,
    )
    if header_idx is None:
        raise ValueError(f"{path_csv} has no Rate header row")
    header = rows[header_idx]
    missing = [name for name in MONTHS if name not in header]
    if missing:
        raise ValueError(f"{path_csv} is missing month columns: {missing}")
    total_rows = [
        row for row in rows[header_idx + 1 :] if row and row[0].strip() == "Total"
    ]
    if len(total_rows) != 1:
        raise ValueError(f"{path_csv} has {len(total_rows)} Total rows, expected 1")
    total = total_rows[0]
    return {
        month_num: parse_kwh(total[header.index(name)])
        for month_num, name in enumerate(MONTHS, start=1)
    }


def monthly_zone_kwh(
    path_local_utilities: Path, utility: str, year: int
) -> pl.DataFrame:
    """Sum hourly ``load_mw`` to kilowatt-hours for one utility-year.

    Returns columns ``month`` (1-12), ``hours``, and ``zone_kwh``.
    """
    root = path_local_utilities / f"utility={utility}" / f"year={year}"
    if not root.is_dir():
        raise FileNotFoundError(
            f"No local load parquet at {root}. "
            "Run the hourly_demand download or aggregate recipe first."
        )
    monthly = (
        pl.scan_parquet(root, hive_partitioning=True)
        .group_by("month")
        .agg(
            (pl.col("load_mw").sum() * 1000).alias("zone_kwh"),
            pl.len().alias("hours"),
        )
        .with_columns(pl.col("month").cast(pl.Int32))
        .sort("month")
        .collect()
    )
    if not isinstance(monthly, pl.DataFrame):
        raise TypeError("Expected DataFrame from zone load collect()")
    months = set(monthly["month"].to_list())
    missing = sorted(set(range(1, 13)) - months)
    if missing:
        raise ValueError(
            f"{root} is missing month(s) {missing}. All 12 months are required."
        )
    return monthly


def compare_monthly(
    filing_kwh_by_month: dict[int, int], zone: pl.DataFrame
) -> pl.DataFrame:
    """Join filing kilowatt-hours to zone kilowatt-hours and add the ratio."""
    filing = pl.DataFrame(
        {
            "month": list(filing_kwh_by_month),
            "filing_kwh": list(filing_kwh_by_month.values()),
        }
    )
    compared = (
        zone.with_columns(pl.col("month").cast(pl.Int32))
        .join(
            filing.with_columns(pl.col("month").cast(pl.Int32)),
            on="month",
            how="left",
        )
        .with_columns(
            (pl.col("zone_kwh") / pl.col("filing_kwh")).alias("zone_over_filing")
        )
    )
    if compared["filing_kwh"].null_count() > 0:
        raise ValueError("Zone months are missing a filing Total value")
    return compared.sort("month")


def format_report(compared: pl.DataFrame, utility: str, year: int) -> str:
    """Render the monthly comparison, including an annual total."""
    lines = [
        (
            f"ISO-NE Connecticut zone load stored as utility={utility} "
            f"year={year}, versus Eversource billed kWh "
            "(CIEC-020 Attachment 2 Total row)."
        ),
        (
            "The zone series includes United Illuminating, municipal utilities, "
            "and losses. The filing Total is Eversource billed sales."
        ),
        "",
        f"{'Month':<6} {'Hours':>6} {'Zone kWh':>18} {'Filing kWh':>18} {'Zone/filing':>12}",
    ]
    total_hours = 0
    total_zone = 0.0
    total_filing = 0
    for row in compared.iter_rows(named=True):
        month_num = int(row["month"])
        hours = int(row["hours"])
        zone_kwh = float(row["zone_kwh"])
        filing_kwh = int(row["filing_kwh"])
        ratio = float(row["zone_over_filing"])
        total_hours += hours
        total_zone += zone_kwh
        total_filing += filing_kwh
        lines.append(
            f"{MONTH_LABELS[month_num - 1]:<6} {hours:6d} {zone_kwh:18,.0f} "
            f"{filing_kwh:18,d} {ratio:12.3f}"
        )
    annual_ratio = total_zone / total_filing
    lines.append(
        f"{'Year':<6} {total_hours:6d} {total_zone:18,.0f} "
        f"{total_filing:18,d} {annual_ratio:12.3f}"
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compare ISO-NE Connecticut zone hourly load to the Eversource "
            "CIEC-020 test-year billed kWh Total row."
        )
    )
    parser.add_argument(
        "--path-csv",
        type=Path,
        required=True,
        help="Eversource CIEC-020 kWh-by-rate-class CSV.",
    )
    parser.add_argument(
        "--path-local-utilities",
        type=Path,
        required=True,
        help="Local root with utility=*/year=*/month=*/data.parquet.",
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--utility",
        default="ct_eversource",
        help="Utility partition to read (default: ct_eversource).",
    )
    args = parser.parse_args()

    filing = load_filing_monthly_kwh(args.path_csv)
    zone = monthly_zone_kwh(args.path_local_utilities, args.utility, args.year)
    print(format_report(compare_monthly(filing, zone), args.utility, args.year))


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
