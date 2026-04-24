#!/usr/bin/env python3
"""Discover latest NYISO AS price partition on disk or S3, fetch newer months.

Lists local and S3 partitions under the given roots, takes the latest ``(year, month)``
from their union, then fetches each subsequent month through the last complete month
(same behavior as ``data/isone/ancillary/update_isone_ancillary_to_latest.py``).
Does not upload (run ``just upload`` after).

Default resolution matches fetch: **real-time (``rt``, 5-minute)** unless ``--markets``
overrides (``dam`` / ``both``).

Usage:
    uv run python data/nyiso/ancillary/update_nyiso_as_prices_to_latest.py \\
        --path-local-parquet data/nyiso/ancillary/parquet \\
        --path-s3-parquet s3://data.sb/nyiso/ancillary/
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
from gridstatus import NYISO

from fetch_nyiso_as_prices_parquet import (
    _write_month_partition,
    fetch_as_month,
    parse_markets_cli,
)

PARTITION_RE = re.compile(r"year=(\d{4})/month=(\d{1,2})")


def _list_s3_partitions(prefix: str) -> list[tuple[int, int]]:
    """List (year, month) partitions under an S3 prefix."""
    result = subprocess.run(
        ["aws", "s3", "ls", prefix.rstrip("/") + "/", "--recursive"],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        return []
    parts: set[tuple[int, int]] = set()
    for line in result.stdout.splitlines():
        for m in PARTITION_RE.finditer(line):
            parts.add((int(m.group(1)), int(m.group(2))))
    return sorted(parts)


def _list_local_partitions(root: Path) -> list[tuple[int, int]]:
    """List (year, month) partitions under a local directory."""
    parts: set[tuple[int, int]] = set()
    for f in root.glob("year=*/month=*/data.parquet"):
        m = PARTITION_RE.search(str(f))
        if m:
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update NYISO AS parquet: discover latest (local ∪ S3), fetch new months.",
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
        help="S3 prefix (e.g. s3://data.sb/nyiso/ancillary/)",
    )
    parser.add_argument(
        "--markets",
        type=str,
        default="rt",
        help="rt (default, 5-min), dam (hourly), or both",
    )
    args = parser.parse_args()

    output_dir = Path(args.path_local_parquet)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        markets = parse_markets_cli(args.markets)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    local_parts = _list_local_partitions(output_dir)
    s3_parts = _list_s3_partitions(args.path_s3_parquet)
    all_parts = sorted(set(local_parts) | set(s3_parts))

    if not all_parts:
        print(
            "No partitions found locally or on S3. Use `just fetch` for initial load.",
            file=sys.stderr,
        )
        return 1

    latest = max(all_parts)
    start = _next_month(*latest)
    end = _last_complete_month()

    print(f"Latest partition: {latest[0]}-{latest[1]:02d}")
    print(f"Target: through {end[0]}-{end[1]:02d}")

    if start > end:
        print(f"Already up to date through {end[0]}-{end[1]:02d}. Nothing to do.")
        return 0

    months_to_fetch = _months_in_range(start, end)
    print(f"Months to fetch: {len(months_to_fetch)}")

    iso = NYISO()
    fetched: list[tuple[int, int]] = []

    for year, month in months_to_fetch:
        label = f"{year}-{month:02d}"
        print(f"  {label}: fetching...", end=" ", flush=True)
        df = fetch_as_month(iso, year, month, markets=markets)
        if df is None:
            print("no data — stopping")
            break

        _write_month_partition(df, output_dir)
        part_file = output_dir / f"year={year}" / f"month={month:02d}" / "data.parquet"
        n_rows = pl.read_parquet(part_file).height
        print(f"{n_rows} rows")
        fetched.append((year, month))

    print(f"\nFetched {len(fetched)} new months")
    if fetched:
        first = fetched[0]
        last = fetched[-1]
        print(f"  {first[0]}-{first[1]:02d} through {last[0]}-{last[1]:02d}")
        print("\nRun `just validate` then `just upload` when ready.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
