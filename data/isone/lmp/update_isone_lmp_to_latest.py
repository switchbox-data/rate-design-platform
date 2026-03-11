#!/usr/bin/env python3
"""Update ISO-NE zonal LMP parquet to the latest available month.

Discovers the latest month already on disk (or S3), then fetches newer months
through the last complete calendar month.

Usage:
    uv run python data/isone/lmp/update_isone_lmp_to_latest.py \
        --path-local-parquet data/isone/lmp/parquet \
        --path-s3-day-ahead s3://data.sb/isone/lmp/day_ahead/zones/ \
        --path-s3-real-time s3://data.sb/isone/lmp/real_time/zones/
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

MONTH_RE = re.compile(r"month=(\d{2})")
YEAR_RE = re.compile(r"year=(\d{4})")

SERIES_MAP = {
    "day_ahead": "--path-s3-day-ahead",
    "real_time": "--path-s3-real-time",
}


def _reject_just_placeholders(val: str) -> None:
    if "{{" in val or "}}" in val:
        print(
            f"ERROR: looks like an uninterpolated Just variable: {val}", file=sys.stderr
        )
        sys.exit(1)


def _last_complete_month() -> tuple[int, int]:
    today = datetime.now()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def _latest_local_month(parquet_dir: Path, series: str) -> tuple[int, int] | None:
    """Find latest (year, month) partition on local disk for a series."""
    series_dir = parquet_dir / series
    if not series_dir.is_dir():
        return None
    latest: tuple[int, int] | None = None
    for month_dir in series_dir.glob("zone=*/year=*/month=*"):
        ym = YEAR_RE.search(str(month_dir))
        mm = MONTH_RE.search(str(month_dir))
        if ym and mm:
            y, m = int(ym.group(1)), int(mm.group(1))
            if latest is None or (y, m) > latest:
                latest = (y, m)
    return latest


def _latest_s3_month(s3_prefix: str) -> tuple[int, int] | None:
    """Find latest (year, month) on S3 by listing partition directories."""
    try:
        out = subprocess.check_output(
            ["aws", "s3", "ls", s3_prefix, "--recursive"],
            text=True,
            timeout=30,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    latest: tuple[int, int] | None = None
    for line in out.splitlines():
        ym = YEAR_RE.search(line)
        mm = MONTH_RE.search(line)
        if ym and mm:
            y, m = int(ym.group(1)), int(mm.group(1))
            if latest is None or (y, m) > latest:
                latest = (y, m)
    return latest


def _next_month(y: int, m: int) -> tuple[int, int]:
    if m == 12:
        return y + 1, 1
    return y, m + 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update ISO-NE LMP parquet to latest month."
    )
    parser.add_argument(
        "--path-local-parquet",
        type=Path,
        required=True,
        help="Local parquet root.",
    )
    parser.add_argument(
        "--path-s3-day-ahead",
        type=str,
        default="",
        help="S3 prefix for day_ahead (optional, used to discover latest on S3).",
    )
    parser.add_argument(
        "--path-s3-real-time",
        type=str,
        default="",
        help="S3 prefix for real_time (optional, used to discover latest on S3).",
    )
    args = parser.parse_args()

    _reject_just_placeholders(str(args.path_local_parquet))
    parquet_dir = args.path_local_parquet.resolve()
    last_y, last_m = _last_complete_month()

    s3_paths = {
        "day_ahead": args.path_s3_day_ahead,
        "real_time": args.path_s3_real_time,
    }

    for series in ("day_ahead", "real_time"):
        latest_local = _latest_local_month(parquet_dir, series)
        latest_s3 = _latest_s3_month(s3_paths[series]) if s3_paths[series] else None

        if latest_local and latest_s3:
            latest = max(latest_local, latest_s3)
        else:
            latest = latest_local or latest_s3

        if latest is None:
            print(
                f"  {series}: no existing data found. "
                f"Use fetch with --start to bootstrap.",
                file=sys.stderr,
            )
            continue

        start_y, start_m = _next_month(*latest)
        if (start_y, start_m) > (last_y, last_m):
            print(
                f"  {series}: already up to date (latest: {latest[0]}-{latest[1]:02d})."
            )
            continue

        start_str = f"{start_y}-{start_m:02d}"
        end_str = f"{last_y}-{last_m:02d}"
        print(f"  {series}: fetching {start_str} through {end_str}...")

        # Delegate to fetch script
        fetch_script = Path(__file__).resolve().parent / "fetch_isone_lmp_parquet.py"
        cmd = [
            sys.executable,
            str(fetch_script),
            "--start",
            start_str,
            "--end",
            end_str,
            "--series",
            series,
            "--path-local-parquet",
            str(parquet_dir),
        ]
        subprocess.run(cmd, check=True)

    print("\nUpdate complete.")


if __name__ == "__main__":
    main()
