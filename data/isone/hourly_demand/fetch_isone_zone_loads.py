#!/usr/bin/env python3
"""Fetch ISO-NE zonal real-time hourly demand from the ISO-NE Web Services API.

Downloads hourly demand for all 8 ISO-NE load zones via the per-day endpoint:
    /realtimehourlydemand/day/{YYYYMMDD}/location/{loc_id}

Writes Hive-partitioned parquet:
    <path_local_zones>/zone={ZONE}/year={YYYY}/month={MM}/data.parquet

Schema:
    interval_start_et  Datetime[us, America/New_York]  — hour-beginning timestamp
    zone               String                          — zone abbreviation (ME, NH, …)
    location_id        Int32                           — ISO-NE location ID (4001–4008)
    load_mw            Float64                         — hourly load in MW

Auth: HTTP Basic with ISONE_USERNAME + ISONE_PASSWORD from .env at repo root.

Usage:
    uv run python data/isone/hourly_demand/fetch_isone_zone_loads.py \\
        --start 2025-01 --end 2025-01 \\
        --path-local-zones data/isone/hourly_demand/zones/
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
USER_AGENT = "Switchbox-rate-design-platform/1.0 (ISO-NE demand)"

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

RATE_LIMIT_BACKOFFS = [60, 180, 600]


class _RateLimiter:
    """Thread-safe rate limiter enforcing a global minimum interval between API calls."""

    def __init__(self, min_interval: float) -> None:
        self._min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()
        self.total_calls = 0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()
            self.total_calls += 1


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


def _partition_path(base: Path, zone: str, year: int, month: int) -> Path:
    return base / f"zone={zone}" / f"year={year}" / f"month={month:02d}"


class RateLimitError(Exception):
    pass


def _api_get(
    session: requests.Session,
    url: str,
    limiter: _RateLimiter,
    label: str,
) -> requests.Response | None:
    """GET with global rate limiting and exponential backoff on 429."""
    for attempt, backoff in enumerate([0] + RATE_LIMIT_BACKOFFS):
        if backoff > 0:
            print(
                f"  429 on {label}, backing off {backoff}s (attempt {attempt + 1})…",
                file=sys.stderr,
            )
            time.sleep(backoff)

        limiter.wait()
        try:
            r = session.get(url, timeout=60)
        except requests.RequestException as e:
            print(f"  {label}: {e}", file=sys.stderr)
            return None

        if r.status_code != 429:
            break
    else:
        raise RateLimitError(
            f"Still 429 after {len(RATE_LIMIT_BACKOFFS)} retries on {label}. "
            "The API rate limit window may be longer. Try again later."
        )

    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        print(f"  {label}: {e}", file=sys.stderr)
        return None
    return r


def _fetch_month_for_zone(
    session: requests.Session,
    year: int,
    month: int,
    zone: str,
    loc_id: int,
    limiter: _RateLimiter,
) -> pl.DataFrame | None:
    """Fetch one month of hourly demand for a single zone via day-by-day API calls."""
    _, ndays = calendar.monthrange(year, month)
    all_items: list[dict] = []

    for day in range(1, ndays + 1):
        yyyymmdd = f"{year}{month:02d}{day:02d}"
        url = f"{BASE_URL}/realtimehourlydemand/day/{yyyymmdd}/location/{loc_id}"
        label = f"{zone} {year}-{month:02d}-{day:02d}"

        r = _api_get(session, url, limiter, label)
        if r is None:
            continue

        data = r.json()
        items = data.get("HourlyRtDemands", {}).get("HourlyRtDemand", [])
        all_items.extend(items)

    if not all_items:
        return None
    return _parse_demand_items(all_items, zone, loc_id)


def _parse_demand_items(items: list[dict], zone: str, loc_id: int) -> pl.DataFrame:
    rows = []
    for item in items:
        rows.append(
            {
                "interval_start_et": item["BeginDate"],
                "zone": zone,
                "location_id": loc_id,
                "load_mw": float(item["Load"]),
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
    zone: str,
    loc_id: int,
    year: int,
    month: int,
    base_dir: Path,
    limiter: _RateLimiter,
) -> str | None:
    """Fetch one zone/month and write parquet. Returns error string or None."""
    part_dir = _partition_path(base_dir, zone, year, month)
    if (part_dir / "data.parquet").exists():
        return None

    session = requests.Session()
    session.auth = auth
    session.headers.update({"Accept": "application/json", "User-Agent": USER_AGENT})

    df = _fetch_month_for_zone(session, year, month, zone, loc_id, limiter)

    if df is None or df.height == 0:
        return f"{zone} {year}-{month:02d}: no data returned"

    part_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(part_dir / "data.parquet", compression="zstd")
    print(f"  {zone} {year}-{month:02d}: {df.height} rows")
    return None


def fetch(
    start: str,
    end: str,
    path_local_zones: Path,
    workers: int,
    auth: tuple[str, str],
) -> None:
    path_local_zones = path_local_zones.resolve()
    _reject_just_placeholders(str(path_local_zones))
    path_local_zones.mkdir(parents=True, exist_ok=True)

    months = _month_range(start, end)

    tasks: list[tuple[str, int, int, int]] = []
    for year, month in months:
        for zone, loc_id in ZONES.items():
            part_dir = _partition_path(path_local_zones, zone, year, month)
            if (part_dir / "data.parquet").exists():
                continue
            tasks.append((zone, loc_id, year, month))

    if not tasks:
        print("All partitions already exist. Nothing to fetch.")
        return

    n_api_calls = sum(calendar.monthrange(y, m)[1] for _, _, y, m in tasks)
    print(
        f"Fetching {len(tasks)} zone-months (~{n_api_calls} API calls) "
        f"with {workers} workers..."
    )

    limiter = _RateLimiter(2.0)
    errors: list[str] = []
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _fetch_zone_month,
                auth,
                zone,
                loc_id,
                year,
                month,
                path_local_zones,
                limiter,
            ): (zone, year, month)
            for zone, loc_id, year, month in tasks
        }
        for future in as_completed(futures):
            zone, year, month = futures[future]
            try:
                err = future.result()
                if err:
                    errors.append(err)
            except Exception as e:
                errors.append(f"{zone} {year}-{month:02d}: {e}")
            done += 1
            if done % 10 == 0 or done == len(tasks):
                print(f"  Progress: {done}/{len(tasks)} zone-months")

    if errors:
        print(f"\n{len(errors)} errors:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ISO-NE zonal hourly demand from Web Services API."
    )
    parser.add_argument("--start", required=True, help="Start month (YYYY-MM).")
    parser.add_argument(
        "--end",
        default="",
        help="End month (YYYY-MM). Default: last complete month.",
    )
    parser.add_argument(
        "--path-local-zones",
        dest="path_local_zones",
        required=True,
        help="Local directory for zone parquet output.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Max concurrent zone-month fetches (default: 1).",
    )
    args = parser.parse_args()

    end = args.end if args.end else _last_complete_month()
    repo_root = Path(__file__).resolve().parents[3]
    auth = _load_env(repo_root)

    print("=" * 60)
    print("ISO-NE Zonal Hourly Demand Fetch")
    print("=" * 60)
    months = _month_range(args.start, end)
    print(f"Range:   {args.start} to {end} ({len(months)} months)")
    print(f"Zones:   {len(ZONES)} ({', '.join(ZONES)})")
    print(f"Workers: {args.workers}")
    print(f"Output:  {args.path_local_zones}")
    print("=" * 60)

    t0 = time.monotonic()
    fetch(args.start, end, Path(args.path_local_zones), args.workers, auth)
    elapsed = time.monotonic() - t0

    print(f"\nCompleted in {elapsed:.1f}s ({elapsed / 60:.1f} min)")


if __name__ == "__main__":
    main()
