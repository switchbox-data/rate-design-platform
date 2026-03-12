#!/usr/bin/env python3
"""Discover latest ISO-NE MRA partition on S3 (or local), then fetch newer months.

Lists partitions under path_s3_parquet (falling back to local) to find the latest
(year, month), then fetches each subsequent month until the present.  Writes new
partitions locally; does not upload (run `just upload` after).

Usage:
    uv run python data/isone/capacity/mra/update_isone_mra.py \
        --path-local-parquet data/isone/capacity/mra/parquet \
        --path-s3-parquet s3://data.sb/isone/capacity/mra/
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from fetch_isone_mra import (
    SCHEMA,
    _cp_for_month,
    _fetch_csv,
    _parse_csv,
    _write_partition,
)

import polars as pl

PARTITION_RE = re.compile(r"year=(\d{4})/month=(\d{1,2})")


def _list_s3_partitions(prefix: str) -> list[tuple[int, int]]:
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


def _list_local_partitions(root: Path) -> list[tuple[int, int]]:
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
        description="Update ISO-NE MRA parquet: discover latest, fetch new months.",
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
        help="S3 prefix for MRA parquet (e.g. s3://data.sb/isone/capacity/mra/)",
    )
    args = parser.parse_args()

    output_dir = Path(args.path_local_parquet)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Try S3 first, fall back to local
    partitions = _list_s3_partitions(args.path_s3_parquet)
    source = "S3"
    if not partitions:
        partitions = _list_local_partitions(output_dir)
        source = "local"
    if not partitions:
        print(
            "No partitions found on S3 or locally. Use `just fetch` for initial load.",
            file=sys.stderr,
        )
        return 1

    latest = max(partitions)
    now = datetime.now()
    end = (now.year, now.month)
    start = _next_month(*latest)

    print(f"Latest on {source}: {latest[0]}-{latest[1]:02d}")
    print(f"Target: through {end[0]}-{end[1]:02d}")

    if start > end:
        print(f"Already up to date through {end[0]}-{end[1]:02d}. Nothing to do.")
        return 0

    months_to_fetch = _months_in_range(start, end)
    print(f"Months to fetch: {len(months_to_fetch)}")

    fetched = 0
    for year, month in months_to_fetch:
        label = f"{year}-{month:02d}"
        cp = _cp_for_month(year, month)
        raw = _fetch_csv(cp, year, month)
        if raw is None:
            print(f"  {label}: fetch failed — stopping")
            break

        records = _parse_csv(raw, year, month, cp)
        if not records:
            print(f"  {label}: no data — stopping")
            break

        df = pl.DataFrame(records, schema=SCHEMA)
        _write_partition(df, output_dir, year, month)
        fetched += 1
        print(f"  {label} (CP {cp}): {len(records)} records")

    print(f"\nFetched {fetched} new months")
    if fetched:
        print("Run `just validate` then `just upload` when ready.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
