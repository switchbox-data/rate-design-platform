#!/usr/bin/env python3
"""Fetch ISO-NE Day-Ahead and/or Real-Time zonal LMP data → Hive-partitioned parquet.

Downloads hourly LMP data from the ISO-NE Web Services API for all 8 load zones
and writes tidy parquet to:
    <output_dir>/{series}/zone={ZONE}/year={YYYY}/month={MM}/data.parquet

API endpoints (day-level, one call per day per zone):
  - Day-Ahead:  /hourlylmp/da/final/day/{YYYYMMDD}/location/{loc_id}
  - Real-Time:  /hourlylmp/rt/final/day/{YYYYMMDD}/location/{loc_id}

Both return the same JSON shape: HourlyLmps.HourlyLmp[] with LmpTotal,
EnergyComponent, CongestionComponent, LossComponent.

Auth: HTTP Basic with ISONE_USERNAME + ISONE_PASSWORD from .env at repo root.

Usage:
    uv run python data/isone/lmp/fetch_isone_lmp_parquet.py \\
        --start 2018-01 --end 2025-02 --series both \\
        --path-local-parquet data/isone/lmp/parquet
"""

from __future__ import annotations

import argparse
import calendar
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import polars as pl
import requests

BASE_URL = "https://webservices.iso-ne.com/api/v1.1"
USER_AGENT = "Switchbox-rate-design-platform/1.0 (ISO-NE LMP)"
MAX_RETRIES = 5
RETRY_BACKOFF_SECS = [5, 15, 30, 60, 120]

ZONES: dict[str, int] = {
    "ME": 4001,
    "NH": 4002,
    "VT": 4003,
    "CT": 4004,
    "RI": 4005,
    "SEMA": 4006,
    "WCMA": 4007,
    "NEMA": 4008,
}

SERIES_RT = "real_time"
SERIES_DA = "day_ahead"

SERIES_TO_API_PREFIX = {
    SERIES_DA: "da",
    SERIES_RT: "rt",
}

PARQUET_SCHEMA = {
    "interval_start_et": pl.Datetime("us", "America/New_York"),
    "zone": pl.String,
    "location_id": pl.Int32,
    "lmp_usd_per_mwh": pl.Float64,
    "lmp_energy_usd_per_mwh": pl.Float64,
    "marginal_cost_congestion_usd_per_mwh": pl.Float64,
    "marginal_cost_losses_usd_per_mwh": pl.Float64,
}


def _reject_just_placeholders(val: str) -> None:
    if "{{" in val or "}}" in val:
        print(
            f"ERROR: looks like an uninterpolated Just variable: {val}", file=sys.stderr
        )
        sys.exit(1)


def _load_env(repo_root: Path) -> tuple[str, str]:
    """Load ISONE_USERNAME and ISONE_PASSWORD from .env at repo root."""
    env_path = repo_root / ".env"
    if not env_path.is_file():
        print(f"ERROR: .env not found at {env_path}", file=sys.stderr)
        sys.exit(1)
    env: dict[str, str] = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        env[key.strip()] = val.strip()
    user = env.get("ISONE_USERNAME")
    pw = env.get("ISONE_PASSWORD")
    if not user or not pw:
        print(
            "ERROR: ISONE_USERNAME or ISONE_PASSWORD missing from .env", file=sys.stderr
        )
        sys.exit(1)
    return user, pw


def _last_complete_month() -> str:
    today = datetime.now()
    if today.month == 1:
        y, m = today.year - 1, 12
    else:
        y, m = today.year, today.month - 1
    return f"{y}-{m:02d}"


def _parse_yyyy_mm(s: str) -> tuple[int, int]:
    parts = s.split("-")
    if len(parts) != 2:
        raise ValueError(f"Expected YYYY-MM, got {s!r}")
    y, m = int(parts[0]), int(parts[1])
    if not (1 <= m <= 12):
        raise ValueError(f"Month must be 01-12, got {s!r}")
    return y, m


def _month_range(start: str, end: str) -> list[tuple[int, int]]:
    y1, m1 = _parse_yyyy_mm(start)
    y2, m2 = _parse_yyyy_mm(end)
    if (y1, m1) > (y2, m2):
        raise ValueError(f"Start {start} must be <= end {end}")
    out: list[tuple[int, int]] = []
    y, m = y1, m1
    while (y, m) <= (y2, m2):
        out.append((y, m))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def _partition_path(base: Path, series: str, zone: str, year: int, month: int) -> Path:
    return base / series / f"zone={zone}" / f"year={year}" / f"month={month:02d}"


class _RateLimiter:
    """Thread-safe global rate limiter: at most 1 request per `min_interval` seconds."""

    def __init__(self, min_interval: float) -> None:
        self._min_interval = min_interval
        self._lock = threading.Lock()
        self._last_time = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_time
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_time = time.monotonic()


def _api_get_with_retry(
    session: requests.Session,
    url: str,
    rate_limiter: _RateLimiter,
) -> requests.Response:
    """GET with global rate limiting and exponential backoff on 429."""
    for attempt in range(MAX_RETRIES + 1):
        rate_limiter.wait()
        r = session.get(url, timeout=60)
        if r.status_code != 429:
            return r
        if attempt < MAX_RETRIES:
            backoff = RETRY_BACKOFF_SECS[attempt]
            print(
                f"    429 rate limited, retry {attempt + 1}/{MAX_RETRIES} "
                f"in {backoff}s...",
                file=sys.stderr,
            )
            time.sleep(backoff)
    return r


def _fetch_month_for_zone(
    session: requests.Session,
    series: str,
    year: int,
    month: int,
    zone: str,
    loc_id: int,
    rate_limiter: _RateLimiter,
) -> pl.DataFrame | None:
    """Fetch one month of LMP for a single zone via day-by-day API calls."""
    api_prefix = SERIES_TO_API_PREFIX[series]
    _, ndays = calendar.monthrange(year, month)
    all_items: list[dict] = []

    for day in range(1, ndays + 1):
        yyyymmdd = f"{year}{month:02d}{day:02d}"
        url = (
            f"{BASE_URL}/hourlylmp/{api_prefix}/final/day/{yyyymmdd}/location/{loc_id}"
        )
        try:
            r = _api_get_with_retry(session, url, rate_limiter)
            r.raise_for_status()
        except requests.RequestException as e:
            print(
                f"  {series} {zone} {year}-{month:02d}-{day:02d}: {e}",
                file=sys.stderr,
            )
            continue
        data = r.json()
        items = data.get("HourlyLmps", {}).get("HourlyLmp", [])
        all_items.extend(items)

    if not all_items:
        return None
    return _parse_lmp_items(all_items, zone, loc_id)


def _parse_lmp_items(items: list[dict], zone: str, loc_id: int) -> pl.DataFrame:
    rows = []
    for item in items:
        rows.append(
            {
                "interval_start_et": item["BeginDate"],
                "zone": zone,
                "location_id": loc_id,
                "lmp_usd_per_mwh": float(item["LmpTotal"]),
                "lmp_energy_usd_per_mwh": float(item["EnergyComponent"]),
                "marginal_cost_congestion_usd_per_mwh": float(
                    item["CongestionComponent"]
                ),
                "marginal_cost_losses_usd_per_mwh": float(item["LossComponent"]),
            }
        )
    df = pl.DataFrame(rows)
    df = df.with_columns(
        pl.col("interval_start_et")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%:z")
        .dt.convert_time_zone("America/New_York")
    )
    return df.cast({"location_id": pl.Int32})


def _fetch_zone_month(
    auth: tuple[str, str],
    series: str,
    zone: str,
    loc_id: int,
    year: int,
    month: int,
    base_dir: Path,
    rate_limiter: _RateLimiter,
) -> str | None:
    """Fetch one zone/month/series and write parquet. Returns error string or None."""
    part_dir = _partition_path(base_dir, series, zone, year, month)
    if (part_dir / "data.parquet").exists():
        return None

    session = requests.Session()
    session.auth = auth
    session.headers.update({"Accept": "application/json", "User-Agent": USER_AGENT})

    df = _fetch_month_for_zone(session, series, year, month, zone, loc_id, rate_limiter)

    if df is None or df.height == 0:
        return f"{series} {zone} {year}-{month:02d}: no data returned"

    part_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(part_dir / "data.parquet", compression="snappy")
    print(f"  {series} {zone} {year}-{month:02d}: {df.height} rows")
    return None


def fetch(
    start: str,
    end: str,
    series_list: list[str],
    path_local_parquet: Path,
    workers: int,
    auth: tuple[str, str],
) -> None:
    path_local_parquet = path_local_parquet.resolve()
    _reject_just_placeholders(str(path_local_parquet))
    path_local_parquet.mkdir(parents=True, exist_ok=True)

    months = _month_range(start, end)

    tasks: list[tuple[str, str, int, int, int]] = []
    for s in series_list:
        for year, month in months:
            for zone, loc_id in ZONES.items():
                part_dir = _partition_path(path_local_parquet, s, zone, year, month)
                if (part_dir / "data.parquet").exists():
                    continue
                tasks.append((s, zone, loc_id, year, month))

    if not tasks:
        print("All partitions already exist. Nothing to fetch.")
        return

    # Global rate limiter: ~1 req/s across all workers to avoid 429s.
    # ISO-NE CloudFront enforces strict rate limits; be conservative.
    rate_limiter = _RateLimiter(min_interval=1.0)

    print(f"Fetching {len(tasks)} zone-months with {workers} workers...")

    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _fetch_zone_month,
                auth,
                s,
                zone,
                loc_id,
                year,
                month,
                path_local_parquet,
                rate_limiter,
            ): (s, zone, year, month)
            for s, zone, loc_id, year, month in tasks
        }
        for future in as_completed(futures):
            s, zone, year, month = futures[future]
            try:
                err = future.result()
                if err:
                    errors.append(err)
            except Exception as e:
                errors.append(f"{s} {zone} {year}-{month:02d}: {e}")

    if errors:
        print(f"\n{len(errors)} errors:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)

    fetched = len(tasks) - len(errors)
    print(f"\nDone. {fetched} partitions written, {len(errors)} errors.")


def _find_repo_root() -> Path:
    """Walk up from this file to find the git repo root."""
    d = Path(__file__).resolve().parent
    while d != d.parent:
        if (d / ".git").is_dir():
            return d
        d = d.parent
    return Path(__file__).resolve().parent


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch ISO-NE zonal LMP (day_ahead and/or real_time) → parquet."
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2018-01",
        help="Start month YYYY-MM (default: 2018-01).",
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
        "--path-local-parquet",
        type=Path,
        required=True,
        help="Local directory for parquet output.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Max concurrent fetch threads (default: 2).",
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
    repo_root = _find_repo_root()
    auth = _load_env(repo_root)
    fetch(
        start=args.start,
        end=args.end,
        series_list=args.series_list,
        path_local_parquet=args.path_local_parquet,
        workers=args.workers,
        auth=auth,
    )


if __name__ == "__main__":
    main()
