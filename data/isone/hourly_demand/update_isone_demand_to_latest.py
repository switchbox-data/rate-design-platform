#!/usr/bin/env python3
"""Discover latest ISO-NE hourly demand partition on disk/S3, fetch newer months.

Scans local partitions (and optionally S3) to find the latest (year, month)
already downloaded, then fetches each subsequent month through the last complete
month. Writes new partitions locally; does not upload (run `just upload` after).

Usage:
    uv run python data/isone/hourly_demand/update_isone_demand_to_latest.py \\
        --path-local-zones data/isone/hourly_demand/zones \\
        --path-s3-zones s3://data.sb/isone/hourly_demand/zones/
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

PARTITION_RE = re.compile(r"zone=(\w+)/year=(\d{4})/month=(\d{2})")
EXPECTED_ZONES = {"ME", "NH", "VT", "CT", "RI", "SEMA", "WCMA", "NEMA"}


def _list_s3_partitions(prefix: str) -> set[tuple[int, int]]:
    """List (year, month) fully covered partitions on S3."""
    result = subprocess.run(
        ["aws", "s3", "ls", prefix.rstrip("/") + "/", "--recursive"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        return set()
    zone_months: dict[tuple[int, int], set[str]] = {}
    for line in result.stdout.splitlines():
        for m in PARTITION_RE.finditer(line):
            zone = m.group(1)
            ym = (int(m.group(2)), int(m.group(3)))
            zone_months.setdefault(ym, set()).add(zone)
    return {ym for ym, zones in zone_months.items() if zones >= EXPECTED_ZONES}


def _list_local_partitions(root: Path) -> set[tuple[int, int]]:
    """List (year, month) fully covered partitions on local disk."""
    zone_months: dict[tuple[int, int], set[str]] = {}
    for f in root.glob("zone=*/year=*/month=*/data.parquet"):
        m = PARTITION_RE.search(str(f))
        if m:
            zone = m.group(1)
            ym = (int(m.group(2)), int(m.group(3)))
            zone_months.setdefault(ym, set()).add(zone)
    return {ym for ym, zones in zone_months.items() if zones >= EXPECTED_ZONES}


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
        description="Update ISO-NE hourly demand: find latest, fetch new months.",
    )
    parser.add_argument(
        "--path-local-zones",
        type=str,
        required=True,
        help="Local parquet root for Hive-partitioned output.",
    )
    parser.add_argument(
        "--path-s3-zones",
        type=str,
        required=True,
        help="S3 prefix for demand parquet (e.g. s3://data.sb/isone/hourly_demand/zones/).",
    )
    args = parser.parse_args()

    local_root = Path(args.path_local_zones)
    local_root.mkdir(parents=True, exist_ok=True)

    print("Scanning for existing partitions...")
    local_parts = _list_local_partitions(local_root)
    s3_parts = _list_s3_partitions(args.path_s3_zones)
    all_parts = local_parts | s3_parts

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
    print(f"Target: through  {end[0]}-{end[1]:02d}")

    if start > end:
        print(f"Already up to date through {end[0]}-{end[1]:02d}. Nothing to do.")
        return 0

    months_to_fetch = _months_in_range(start, end)
    start_str = f"{months_to_fetch[0][0]}-{months_to_fetch[0][1]:02d}"
    end_str = f"{months_to_fetch[-1][0]}-{months_to_fetch[-1][1]:02d}"
    print(f"Months to fetch: {len(months_to_fetch)} ({start_str} to {end_str})")

    # Import and call the fetch function
    from fetch_isone_zone_loads import _load_env, fetch

    repo_root = Path(__file__).resolve().parents[3]
    auth = _load_env(repo_root)

    fetch(start_str, end_str, local_root, workers=4, auth=auth)

    print("\nRun `just validate` then `just upload` when ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
