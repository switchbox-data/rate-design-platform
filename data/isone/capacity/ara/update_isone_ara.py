#!/usr/bin/env python3
"""Discover latest ISO-NE ARA partition on S3 (or local), fetch newer CP/ARAs.

Lists existing partitions, determines the latest CP, then tries all CPs from
there through the present (and a few years ahead, since FCM auctions run in
advance). Writes new partitions locally; does not upload (run `just upload`).

Usage:
    uv run python data/isone/capacity/ara/update_isone_ara.py \
        --path-local-parquet data/isone/capacity/ara/parquet \
        --path-s3-parquet s3://data.sb/isone/capacity/ara/
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from fetch_isone_ara import ARA_NUMBERS, _fetch_and_parse

PARTITION_RE = re.compile(r"cp=([\d]+-[\d]+)/ara_number=(\d)")

LOOKAHEAD_YEARS = 5


def _list_s3_partitions(prefix: str) -> list[tuple[str, int]]:
    """List (cp, ara_number) partitions under an S3 prefix."""
    result = subprocess.run(
        ["aws", "s3", "ls", prefix.rstrip("/") + "/", "--recursive"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        return []
    parts: set[tuple[str, int]] = set()
    for line in result.stdout.splitlines():
        for m in PARTITION_RE.finditer(line):
            parts.add((m.group(1), int(m.group(2))))
    return sorted(parts)


def _list_local_partitions(root: Path) -> list[tuple[str, int]]:
    """List (cp, ara_number) partitions under a local directory."""
    parts: set[tuple[str, int]] = set()
    for f in root.glob("cp=*/ara=*/data.parquet"):
        m = PARTITION_RE.search(str(f))
        if m:
            parts.add((m.group(1), int(m.group(2))))
    return sorted(parts)


def _cp_start_year(cp: str) -> int:
    return int(cp.split("-")[0])


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update ISO-NE ARA parquet: discover latest, fetch new CP/ARAs.",
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
        help="S3 prefix for ARA parquet (e.g. s3://data.sb/isone/capacity/ara/)",
    )
    args = parser.parse_args()

    output_dir = Path(args.path_local_parquet)
    output_dir.mkdir(parents=True, exist_ok=True)

    s3_parts = _list_s3_partitions(args.path_s3_parquet)
    local_parts = _list_local_partitions(output_dir)
    all_existing = set(s3_parts) | set(local_parts)

    if not all_existing:
        print(
            "No partitions found on S3 or locally. Use `just fetch` for initial load.",
            file=sys.stderr,
        )
        return 1

    latest_cp = max(all_existing, key=lambda x: x[0])[0]
    latest_year = _cp_start_year(latest_cp)

    now = datetime.now()
    end_year = now.year + LOOKAHEAD_YEARS

    print(f"Latest existing CP: {latest_cp}")
    print(
        f"Scanning CPs from {latest_cp} through {end_year}-{(end_year + 1) % 100:02d}"
    )

    tasks: list[tuple[str, int]] = []
    for y in range(latest_year, end_year + 1):
        suffix = str(y + 1)[-2:]
        cp = f"{y}-{suffix}"
        for ara in ARA_NUMBERS:
            if (cp, ara) not in all_existing:
                tasks.append((cp, ara))

    if not tasks:
        print("Already up to date. Nothing to do.")
        return 0

    print(f"Checking {len(tasks)} CP/ARA combinations...\n")

    total_written = 0
    for cp, ara in tasks:
        label, rows, _ = _fetch_and_parse(cp, ara, output_dir)
        if rows > 0:
            total_written += 1
            print(f"  {label}: {rows} rows")

    print(f"\nFetched {total_written} new partitions")
    if total_written:
        print("Run `just validate` then `just upload` when ready.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
