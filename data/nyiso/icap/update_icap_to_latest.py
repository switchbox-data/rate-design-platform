#!/usr/bin/env python3
"""Discover latest NYISO ICAP partition on S3, then fetch new months through the present.

Lists S3 partitions under path_s3_parquet, finds the latest (year, month),
then fetches each subsequent month until gridstatus reports that a month's
ICAP Market Report is not yet available. Writes new partitions locally; does
not upload (run `just upload` after).

Usage:
    uv run python data/nyiso/icap/update_icap_to_latest.py \
        --path-local-parquet data/nyiso/icap/parquet \
        --path-s3-parquet s3://data.sb/nyiso/icap/
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from gridstatus import NYISO

from fetch_icap_clearing_prices import SCHEMA

PARTITION_RE = re.compile(r"year=(\d{4})/month=(\d{1,2})")


def _list_s3_partitions(prefix: str) -> list[tuple[int, int]]:
    """List (year, month) partitions under an S3 prefix."""
    result = subprocess.run(
        ["aws", "s3", "ls", prefix.rstrip("/") + "/", "--recursive"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        return []
    parts: set[tuple[int, int]] = set()
    for line in result.stdout.splitlines():
        for m in PARTITION_RE.finditer(line):
            parts.add((int(m.group(1)), int(m.group(2))))
    return sorted(parts)


def _next_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return (year + 1, 1)
    return (year, month + 1)


def _last_complete_month() -> tuple[int, int]:
    today = datetime.now()
    if today.month == 1:
        return (today.year - 1, 12)
    return (today.year, today.month - 1)


def _months_in_range(
    start: tuple[int, int], end: tuple[int, int]
) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    current = start
    while current <= end:
        months.append(current)
        current = _next_month(*current)
    return months


def _fetch_single_month(iso: NYISO, year: int, month: int) -> pl.DataFrame | None:
    """Fetch a single month's ICAP data from gridstatus.

    Returns a tidy Polars DataFrame with just the requested month's rows,
    or None if the report is not yet available.
    """
    date = pd.Timestamp(year=year, month=month, day=1)
    try:
        raw = iso.get_capacity_prices(date=date, verbose=False)
    except Exception:
        return None

    records: list[dict[str, object]] = []
    for ts in raw.index:
        if ts.year != year or ts.month != month:
            continue
        for locality, auction_type in raw.columns:
            price = raw.loc[ts, (locality, auction_type)]
            if pd.isna(price):
                continue
            records.append(
                {
                    "year": year,
                    "month": month,
                    "locality": locality,
                    "auction_type": auction_type,
                    "price_per_kw_month": float(price),
                }
            )
    if not records:
        return None
    return pl.DataFrame(records, schema=SCHEMA).sort(
        "month", "locality", "auction_type"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update NYISO ICAP parquet: discover S3 latest, fetch new months.",
    )
    parser.add_argument(
        "--path-local-parquet",
        type=str,
        required=True,
        help="Local parquet root for Hive-partitioned output",
    )
    parser.add_argument(
        "--path-s3-parquet",
        type=str,
        required=True,
        help="S3 prefix for ICAP parquet (e.g. s3://data.sb/nyiso/icap/)",
    )
    args = parser.parse_args()

    output_dir = Path(args.path_local_parquet)
    output_dir.mkdir(parents=True, exist_ok=True)

    partitions = _list_s3_partitions(args.path_s3_parquet)
    if not partitions:
        print(
            "No partitions on S3. Use `just fetch` for initial load.", file=sys.stderr
        )
        return 1

    latest = max(partitions)
    start = _next_month(*latest)
    end = _last_complete_month()

    print(f"Latest on S3: {latest[0]}-{latest[1]:02d}")
    print(f"Target: through {end[0]}-{end[1]:02d}")

    if start > end:
        print(f"Already up to date through {end[0]}-{end[1]:02d}. Nothing to do.")
        return 0

    months_to_fetch = _months_in_range(start, end)
    print(f"Months to fetch: {len(months_to_fetch)}")

    iso = NYISO()
    fetched: list[tuple[int, int]] = []
    not_available: tuple[int, int] | None = None

    for year, month in months_to_fetch:
        label = f"{year}-{month:02d}"
        df = _fetch_single_month(iso, year, month)
        if df is None:
            print(f"  {label}: not yet available — stopping")
            not_available = (year, month)
            break

        part_dir = output_dir / f"year={year}" / f"month={month}"
        part_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(part_dir / "data.parquet", compression="snappy")
        fetched.append((year, month))
        print(f"  {label}: {len(df)} rows")

    print(f"\nFetched {len(fetched)} new months")
    if fetched:
        first = fetched[0]
        last = fetched[-1]
        print(f"  {first[0]}-{first[1]:02d} through {last[0]}-{last[1]:02d}")
    if not_available:
        print(f"  {not_available[0]}-{not_available[1]:02d} onward not yet published")
    if fetched:
        print("\nRun `just validate` then `just upload` when ready.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
