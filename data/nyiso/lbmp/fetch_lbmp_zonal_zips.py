#!/usr/bin/env python3
"""Fetch NYISO Day-Ahead and/or Real-Time zonal LBMP monthly ZIP files.

Downloads monthly ZIP archives from NYISO MIS (mis.nyiso.com). Uses a thread
pool (default 8 workers) for parallel downloads — no API key, no rate limits.

Usage:
    uv run python data/nyiso/lbmp/fetch_lbmp_zonal_zips.py \\
        --start 2000-01 --end 2024-12 --series both \\
        --path-local-zip /path/to/zips
    uv run python ... --workers 16  # more concurrent downloads
"""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests

BASE_URL = "https://mis.nyiso.com/public/csv/"
DAM_ZIP_SUFFIX = "damlbmp_zone_csv.zip"
RT_ZIP_SUFFIX = "realtime_zone_csv.zip"
# CLI series -> internal market key and zip suffix
SERIES_TO_MARKET = {
    "day_ahead": ("damlbmp", DAM_ZIP_SUFFIX),
    "real_time": ("realtime", RT_ZIP_SUFFIX),
}
USER_AGENT = "Switchbox-rate-design-platform/1.0 (NYISO LBMP)"


def _reject_just_placeholders(val: str) -> None:
    if "{{" in val or "}}" in val:
        raise ValueError(
            f"Output path looks like uninterpolated Just: {val!r}. "
            "Pass a resolved path (e.g. from Just path_local_zip)."
        )


def _last_complete_month() -> str:
    """Return YYYY-MM for the last complete calendar month."""
    today = datetime.now()
    if today.month == 1:
        y, m = today.year - 1, 12
    else:
        y, m = today.year, today.month - 1
    return f"{y}-{m:02d}"


def _parse_yyyy_mm(s: str) -> tuple[int, int]:
    """Parse YYYY-MM to (year, month)."""
    parts = s.split("-")
    if len(parts) != 2:
        raise ValueError(f"Expected YYYY-MM, got {s!r}")
    try:
        y, m = int(parts[0]), int(parts[1])
    except ValueError as e:
        raise ValueError(f"Invalid YYYY-MM {s!r}") from e
    if not (1 <= m <= 12):
        raise ValueError(f"Month must be 01-12, got {s!r}")
    return y, m


def _month_range(start_yyyy_mm: str, end_yyyy_mm: str) -> list[str]:
    """Return list of YYYYMM01 for each month in [start, end] inclusive."""
    y1, m1 = _parse_yyyy_mm(start_yyyy_mm)
    y2, m2 = _parse_yyyy_mm(end_yyyy_mm)
    if (y1, m1) > (y2, m2):
        raise ValueError(f"Start {start_yyyy_mm} must be <= end {end_yyyy_mm}")
    out: list[str] = []
    y, m = y1, m1
    while (y, m) <= (y2, m2):
        out.append(f"{y}{m:02d}01")
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def _url(subdir: str, suffix: str, month_yyyymm01: str) -> str:
    return f"{BASE_URL}{subdir}/{month_yyyymm01}{suffix}"


def _fetch_one(url: str, out_file: Path) -> str | None:
    """Download one URL to out_file. Returns None on success, else error message."""
    try:
        r = requests.get(url, timeout=60, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        out_file.write_bytes(r.content)
        return None
    except requests.RequestException as e:
        return f"{url} -> {e}"


def fetch(
    start_yyyy_mm: str,
    end_yyyy_mm: str,
    series: list[str],
    path_local_zip: Path,
    workers: int = 8,
) -> None:
    path_local_zip = path_local_zip.resolve()
    _reject_just_placeholders(str(path_local_zip))
    path_local_zip.mkdir(parents=True, exist_ok=True)

    months = _month_range(start_yyyy_mm, end_yyyy_mm)
    tasks: list[tuple[str, Path]] = []

    for s in series:
        if s not in SERIES_TO_MARKET:
            raise ValueError(
                f"series must be one of {list(SERIES_TO_MARKET)}, got {s!r}"
            )
        subdir, suffix = SERIES_TO_MARKET[s]
        out_dir = path_local_zip / s
        out_dir.mkdir(parents=True, exist_ok=True)
        for month in months:
            url = _url(subdir, suffix, month)
            out_file = out_dir / f"{month}_{suffix}"
            if out_file.exists():
                continue
            tasks.append((url, out_file))

    if not tasks:
        return

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_fetch_one, url, out_file): (url, out_file)
            for url, out_file in tasks
        }
        for future in as_completed(futures):
            err = future.result()
            if err:
                print(f"Warning: {err}", file=sys.stderr)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch NYISO zonal LBMP monthly ZIPs (day_ahead and/or real_time)."
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2000-01",
        help="Start month YYYY-MM (default: 2000-01).",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End month YYYY-MM (default: last complete calendar month).",
    )
    parser.add_argument(
        "--series",
        type=str,
        default="both",
        choices=["day_ahead", "real_time", "both"],
        help="Series to fetch: day_ahead, real_time, or both (default: both).",
    )
    parser.add_argument(
        "--path-local-zip",
        type=Path,
        required=True,
        help="Local directory for ZIPs (e.g. zips/). Subdirs day_ahead/ and real_time/.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Max concurrent downloads (default: 8).",
    )
    args = parser.parse_args()
    if args.end is None or args.end == "":
        args.end = _last_complete_month()
    if args.series == "both":
        args.series_list = ["day_ahead", "real_time"]
    else:
        args.series_list = [args.series]
    return args


def main() -> None:
    args = _parse_args()
    fetch(
        start_yyyy_mm=args.start,
        end_yyyy_mm=args.end,
        series=args.series_list,
        path_local_zip=args.path_local_zip,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
