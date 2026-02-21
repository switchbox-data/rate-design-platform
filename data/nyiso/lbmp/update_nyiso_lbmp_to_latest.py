#!/usr/bin/env python3
"""Discover latest NYISO LBMP partition on S3, then fetch and convert new months only.

Lists both day_ahead and real_time roots, parses zone=Z/year=YYYY/month=MM,
takes max (year, month), then fetches and converts from (latest+1 month) through
last complete calendar month. Does not run upload.

Usage:
    uv run python data/nyiso/lbmp/update_nyiso_lbmp_to_latest.py \\
        --path-local-zip /path/to/zips --path-local-parquet /path/to/parquet \\
        --path-s3-day-ahead s3://data.sb/nyiso/lbmp/day_ahead/zones/ \\
        --path-s3-real-time s3://data.sb/nyiso/lbmp/real_time/zones/
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


# Run from repo root: uv run python data/nyiso/lbmp/update_nyiso_lbmp_to_latest.py ...
# so script dir must be on path when invoked as main
def _last_complete_month() -> str:
    from datetime import datetime

    today = datetime.now()
    if today.month == 1:
        y, m = today.year - 1, 12
    else:
        y, m = today.year, today.month - 1
    return f"{y}-{m:02d}"


# Partition pattern: zone=Z/year=YYYY/month=MM
PARTITION_RE = re.compile(r"zone=[^/]+/year=(\d{4})/month=(\d{2})")


def _list_s3_partitions(prefix: str) -> list[tuple[int, int]]:
    """List partition (year, month) under an S3 prefix. Uses aws s3 ls recursively."""
    result = subprocess.run(
        ["aws", "s3", "ls", prefix, "--recursive"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        return []
    parts: list[tuple[int, int]] = []
    for line in result.stdout.splitlines():
        # We get object keys; we only need unique zone/year/month
        for m in PARTITION_RE.finditer(line):
            y, mo = int(m.group(1)), int(m.group(2))
            if (y, mo) not in parts:
                parts.append((y, mo))
    return list(set(parts))


def latest_month_on_s3(
    path_s3_day_ahead: str, path_s3_real_time: str
) -> tuple[int, int] | None:
    """Return (year, month) of the latest partition across both series, or None if empty."""
    day_ahead = _list_s3_partitions(path_s3_day_ahead.rstrip("/") + "/")
    real_time = _list_s3_partitions(path_s3_real_time.rstrip("/") + "/")
    all_parts = day_ahead + real_time
    if not all_parts:
        return None
    return max(all_parts)


def next_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return (year + 1, 1)
    return (year, month + 1)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update local NYISO LBMP to latest: discover S3 latest, fetch+convert new months."
    )
    parser.add_argument("--path-local-zip", type=Path, required=True)
    parser.add_argument("--path-local-parquet", type=Path, required=True)
    parser.add_argument("--path-s3-day-ahead", type=str, required=True)
    parser.add_argument("--path-s3-real-time", type=str, required=True)
    args = parser.parse_args()

    latest = latest_month_on_s3(args.path_s3_day_ahead, args.path_s3_real_time)
    if latest is None:
        start_yyyy_mm = "2000-01"
        print("No partitions on S3; starting from 2000-01", file=sys.stderr)
    else:
        y, m = next_month(*latest)
        start_yyyy_mm = f"{y}-{m:02d}"
        print(
            f"Latest on S3: {latest[0]}-{latest[1]:02d}; will fetch from {start_yyyy_mm}",
            file=sys.stderr,
        )

    end_yyyy_mm = _last_complete_month()
    if start_yyyy_mm > end_yyyy_mm:
        print(
            f"NYISO LBMP already up to date through {end_yyyy_mm}. Nothing to do.",
            file=sys.stderr,
        )
        return 0

    script_dir = Path(__file__).resolve().parent
    path_local_zip = args.path_local_zip.resolve()
    path_local_parquet = args.path_local_parquet.resolve()

    print(f"Fetching {start_yyyy_mm} through {end_yyyy_mm}...", file=sys.stderr)
    r1 = subprocess.run(
        [
            sys.executable,
            str(script_dir / "fetch_lbmp_zonal_zips.py"),
            "--start",
            start_yyyy_mm,
            "--end",
            end_yyyy_mm,
            "--series",
            "both",
            "--path-local-zip",
            str(path_local_zip),
        ],
    )
    if r1.returncode != 0:
        return r1.returncode

    print("Converting to parquet...", file=sys.stderr)
    r2 = subprocess.run(
        [
            sys.executable,
            str(script_dir / "convert_lbmp_zonal_zips_to_parquet.py"),
            "--path-local-zip",
            str(path_local_zip),
            "--path-local-parquet",
            str(path_local_parquet),
            "--start",
            start_yyyy_mm,
            "--end",
            end_yyyy_mm,
        ],
    )
    if r2.returncode != 0:
        return r2.returncode

    print("Done. Run 'just upload' when ready to sync to S3.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
