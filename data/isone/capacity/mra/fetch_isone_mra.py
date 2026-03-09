#!/usr/bin/env python3
"""Fetch ISO-NE FCM Monthly Reconfiguration Auction (MRA) clearing prices → Hive-partitioned parquet.

Downloads MRA results from ISO Express CSV endpoints for each month and writes
tidy long-format parquet to:
    <output_dir>/year={YYYY}/month={M}/data.parquet

The CSV endpoint is:
    https://www.iso-ne.com/transform/csv/fcmmra?cp={cp}&month={YYYYMM}

where `cp` is the commitment period in human-readable format (e.g. "2024-25") and
`month` is the calendar month in YYYYMM format.

The CSV uses row-type markers: "C" for comment, "H" for header, "D" for data,
"T" for total.  Each response contains three sections:
  1. Capacity zones (first col = zone type: "ROP", "Import", "Export")
  2. External interfaces (first col = interface name)
  3. Totals (skipped)

Data availability: September 2018 (CP 2018-19) through the present.  Earlier CPs
return empty responses.  Zone structure has changed over time (e.g. 8501/8502/8504
in 2018-19 → 8503/8505/8506 in later CPs).

Usage:
    uv run python data/isone/capacity/mra/fetch_isone_mra.py \
        --start-year 2018 --end-year 2026 \
        --path-local-parquet data/isone/capacity/mra/parquet
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import polars as pl

BASE_URL = "https://www.iso-ne.com/transform/csv/fcmmra"

SCHEMA = {
    "year": pl.Int16,
    "month": pl.Int8,
    "cp": pl.String,
    "entity_type": pl.String,
    "capacity_zone_id": pl.Int32,
    "capacity_zone_name": pl.String,
    "entity_name": pl.String,
    "supply_submitted_mw": pl.Float64,
    "demand_submitted_mw": pl.Float64,
    "supply_cleared_mw": pl.Float64,
    "demand_cleared_mw": pl.Float64,
    "net_capacity_cleared_mw": pl.Float64,
    "clearing_price_per_kw_month": pl.Float64,
}


def _reject_just_placeholders(val: str) -> None:
    if "{{" in val or "}}" in val:
        print(
            f"ERROR: looks like an uninterpolated Just variable: {val}", file=sys.stderr
        )
        sys.exit(1)


def _cp_for_month(year: int, month: int) -> str:
    """Derive the commitment period string for a calendar month.

    FCM commitment periods run June–May.  June–December of year Y belongs to
    CP "{Y}-{Y+1 last 2 digits}".  January–May of year Y belongs to
    CP "{Y-1}-{Y last 2 digits}".
    """
    if month >= 6:
        return f"{year}-{(year + 1) % 100:02d}"
    return f"{year - 1}-{year % 100:02d}"


def _fetch_csv(cp: str, year: int, month: int) -> str | None:
    """Fetch raw CSV text for a single month.  Returns None on HTTP error."""
    month_str = f"{year}{month:02d}"
    url = f"{BASE_URL}?cp={cp}&month={month_str}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        return resp.read().decode("utf-8")
    except Exception:
        return None


def _parse_csv(raw: str, year: int, month: int, cp: str) -> list[dict[str, object]]:
    """Parse MRA CSV into a list of record dicts.

    Tracks section boundaries via H rows to distinguish zone vs interface data.
    Skips the totals section entirely.
    """
    records: list[dict[str, object]] = []
    section: str | None = None

    for line in raw.strip().split("\n"):
        reader = csv.reader(io.StringIO(line))
        fields = next(reader)
        row_type = fields[0]

        if row_type == "H" and len(fields) > 1:
            if "Capacity Zone Type" in fields[1]:
                section = "zone"
            elif "External Interface" in fields[1]:
                section = "interface"
            elif "Total Supply" in fields[1]:
                section = "totals"
            continue

        if row_type != "D" or section is None or section == "totals":
            continue

        if section == "zone":
            records.append(
                {
                    "year": year,
                    "month": month,
                    "cp": cp,
                    "entity_type": "zone",
                    "capacity_zone_id": int(fields[2]),
                    "capacity_zone_name": fields[3],
                    "entity_name": fields[3],
                    "supply_submitted_mw": float(fields[4]),
                    "demand_submitted_mw": float(fields[5]),
                    "supply_cleared_mw": float(fields[6]),
                    "demand_cleared_mw": float(fields[7]),
                    "net_capacity_cleared_mw": float(fields[8]),
                    "clearing_price_per_kw_month": float(fields[9]),
                }
            )
        elif section == "interface":
            records.append(
                {
                    "year": year,
                    "month": month,
                    "cp": cp,
                    "entity_type": "external_interface",
                    "capacity_zone_id": int(fields[2]),
                    "capacity_zone_name": fields[3],
                    "entity_name": fields[1],
                    "supply_submitted_mw": float(fields[4]),
                    "demand_submitted_mw": float(fields[5]),
                    "supply_cleared_mw": float(fields[6]),
                    "demand_cleared_mw": float(fields[7]),
                    "net_capacity_cleared_mw": float(fields[8]),
                    "clearing_price_per_kw_month": float(fields[9]),
                }
            )
    return records


def _fetch_and_parse(year: int, month: int) -> tuple[int, int, list[dict[str, object]]]:
    """Fetch and parse a single month.  Returns (year, month, records)."""
    cp = _cp_for_month(year, month)
    raw = _fetch_csv(cp, year, month)
    if raw is None:
        return year, month, []
    return year, month, _parse_csv(raw, year, month, cp)


def _write_partition(df: pl.DataFrame, output_dir: Path, year: int, month: int) -> None:
    part_dir = output_dir / f"year={year}" / f"month={month}"
    part_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(part_dir / "data.parquet", compression="snappy")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ISO-NE MRA clearing prices → parquet",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        required=True,
        help="First year to fetch (e.g. 2018)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        required=True,
        help="Last year to fetch (e.g. 2026)",
    )
    parser.add_argument(
        "--path-local-parquet",
        type=str,
        required=True,
        help="Output directory for Hive-partitioned parquet",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel fetch threads (default: 4)",
    )
    args = parser.parse_args()

    _reject_just_placeholders(args.path_local_parquet)
    output_dir = Path(args.path_local_parquet)
    output_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()

    # Build list of (year, month) pairs to fetch
    tasks: list[tuple[int, int]] = []
    for year in range(args.start_year, args.end_year + 1):
        for month in range(1, 13):
            if year == now.year and month > now.month:
                break
            part_dir = output_dir / f"year={year}" / f"month={month}"
            if (part_dir / "data.parquet").exists():
                continue
            tasks.append((year, month))

    if not tasks:
        print("All partitions already exist. Nothing to fetch.")
        return

    print(f"Fetching {len(tasks)} months with {args.workers} workers...")

    fetched = 0
    skipped = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_fetch_and_parse, y, m): (y, m) for y, m in tasks}
        for future in as_completed(futures):
            y, m = futures[future]
            try:
                _, _, records = future.result()
            except Exception as e:
                print(f"  {y}-{m:02d}: ERROR {e}")
                skipped += 1
                continue

            if not records:
                skipped += 1
                continue

            df = pl.DataFrame(records, schema=SCHEMA)
            _write_partition(df, output_dir, y, m)
            fetched += 1
            cp = _cp_for_month(y, m)
            print(f"  {y}-{m:02d} (CP {cp}): {len(records)} records")

    print(f"\nDone. {fetched} partitions written, {skipped} months skipped (no data).")
    print(f"Output: {output_dir}")


if __name__ == "__main__":
    main()
