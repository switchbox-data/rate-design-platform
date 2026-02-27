#!/usr/bin/env python3
"""Discover latest EIA-877 partition on S3, then fetch new months through the present.

Lists S3 partitions under path_s3_parquet, finds the latest (product, year, month),
then fetches all weekly data from the next month onward, aggregates to monthly,
and writes new partitions locally. Does not upload (run `just upload` after).

Usage:
    uv run python data/eia/heating_fuel_prices/update_heating_fuel_prices_to_latest.py \
        --path-local-parquet data/eia/heating_fuel_prices/parquet \
        --path-s3-parquet s3://data.sb/eia/heating_fuel_prices/
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

from fetch_heating_fuel_prices_parquet import (
    fetch_weekly_prices,
    load_api_key,
    to_monthly_parquet,
)

PARTITION_RE = re.compile(r"product=(\w+)/year=(\d{4})/month=(\d{1,2})")


def list_s3_partitions(prefix: str) -> list[tuple[str, int, int]]:
    """List (product, year, month) partitions under an S3 prefix."""
    result = subprocess.run(
        ["aws", "s3", "ls", prefix.rstrip("/") + "/", "--recursive"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        return []
    parts: set[tuple[str, int, int]] = set()
    for line in result.stdout.splitlines():
        for m in PARTITION_RE.finditer(line):
            parts.add((m.group(1), int(m.group(2)), int(m.group(3))))
    return sorted(parts)


def latest_year_month(partitions: list[tuple[str, int, int]]) -> tuple[int, int]:
    """Find the latest (year, month) across all products."""
    return max((y, m) for _, y, m in partitions)


def current_year_month() -> tuple[int, int]:
    """Return the current (year, month)."""
    now = datetime.now()
    return (now.year, now.month)


def next_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return (year + 1, 1)
    return (year, month + 1)


def write_update_partitions(
    df: pl.DataFrame, path_local_parquet: str, start_ym: tuple[int, int]
) -> int:
    """Write only partitions at or after start_ym. Returns count of partitions written."""
    out = Path(path_local_parquet)
    data_cols = ["state", "price_type", "price_per_gallon"]
    written = 0

    partitions = (
        df.filter(
            (pl.col("year") > start_ym[0])
            | ((pl.col("year") == start_ym[0]) & (pl.col("month") >= start_ym[1]))
        )
        .select(["product", "year", "month"])
        .unique()
        .sort(["product", "year", "month"])
    )

    for row in partitions.iter_rows(named=True):
        product, year, month = row["product"], row["year"], row["month"]
        part_dir = out / f"product={product}" / f"year={year}" / f"month={month}"
        part_dir.mkdir(parents=True, exist_ok=True)

        part_df = df.filter(
            (pl.col("product") == product)
            & (pl.col("year") == year)
            & (pl.col("month") == month)
        ).select(data_cols)

        part_df.write_parquet(part_dir / "data.parquet", compression="zstd")
        print(f"  product={product}/year={year}/month={month}: {len(part_df)} rows")
        written += 1

    return written


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update EIA-877 heating fuel prices: discover S3 latest, fetch new months.",
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
        help="S3 prefix for heating fuel price parquet",
    )
    parser.add_argument(
        "--eia-api-key",
        default=None,
        help="EIA API key (optional if set in .env)",
    )
    args = parser.parse_args()

    output_dir = Path(args.path_local_parquet)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Discovering latest partition on S3...")
    partitions = list_s3_partitions(args.path_s3_parquet)
    if not partitions:
        print(
            "No partitions on S3. Use `just fetch` for initial load.", file=sys.stderr
        )
        return 1

    latest = latest_year_month(partitions)
    start = next_month(*latest)
    end = current_year_month()

    print(f"Latest on S3: {latest[0]}-{latest[1]:02d}")
    print(f"Target: through {end[0]}-{end[1]:02d}")

    if start > end:
        print(f"Already up to date through {end[0]}-{end[1]:02d}. Nothing to do.")
        return 0

    start_str = f"{start[0]:04d}-{start[1]:02d}"
    end_str = f"{end[0]:04d}-{end[1]:02d}"
    print(f"Fetching {start_str} through {end_str}...")

    api_key = load_api_key(args.eia_api_key)
    rows = fetch_weekly_prices(api_key, start=start_str, end=end_str)

    if not rows:
        print("No new data returned from API.")
        return 0

    df = to_monthly_parquet(rows)
    print(f"\nAggregated to {len(df)} monthly rows")

    print("\nWriting new partitions:")
    n_written = write_update_partitions(df, args.path_local_parquet, start)

    print(f"\nWrote {n_written} new partitions")
    if n_written > 0:
        print("Run `just validate` then `just upload` when ready.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
