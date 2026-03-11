#!/usr/bin/env python3
"""Fetch ISO-NE ancillary data (regulation clearing prices + system load) → Hive-partitioned parquet.

Downloads 5-minute regulation clearing prices and hourly system load from
the ISO-NE Web Services API, aggregates RCP data to hourly, joins with
system load, and writes to:
    <output_dir>/year={YYYY}/month={MM}/data.parquet

The two endpoints are:
    /fiveminutercp/final/day/{YYYYMMDD}  — 5-min regulation clearing prices
    /hourlysysload/day/{YYYYMMDD}        — hourly system load

Rate limit: 1s between API calls, with exponential backoff on HTTP 429.

Usage:
    uv run python data/isone/ancillary/fetch_isone_ancillary_parquet.py \
        --start 2025-01 --end 2025-01 \
        --path-local-parquet data/isone/ancillary/parquet
"""

from __future__ import annotations

import argparse
import base64
import calendar
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime
from json import loads
from pathlib import Path
from typing import Any

import polars as pl

ISONE_BASE_URL = "https://webservices.iso-ne.com/api/v1.1"

SCHEMA = {
    "interval_start_et": pl.Datetime("us", "America/New_York"),
    "reg_service_price_usd_per_mwh": pl.Float64,
    "reg_capacity_price_usd_per_mwh": pl.Float64,
    "system_load_mw": pl.Float64,
    "native_load_mw": pl.Float64,
    "ard_demand_mw": pl.Float64,
}


def _reject_just_placeholders(val: str) -> None:
    if "{{" in val or "}}" in val:
        print(
            f"ERROR: looks like an uninterpolated Just variable: {val}", file=sys.stderr
        )
        sys.exit(1)


def _load_credentials() -> tuple[str, str]:
    """Load ISO-NE credentials from .env at repo root."""
    repo_root = Path(__file__).resolve().parents[3]
    env_file = repo_root / ".env"
    if not env_file.exists():
        print(f"ERROR: .env not found at {env_file}", file=sys.stderr)
        sys.exit(1)

    username = password = ""
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line.startswith("ISONE_USERNAME="):
            username = line.split("=", 1)[1]
        elif line.startswith("ISONE_PASSWORD="):
            password = line.split("=", 1)[1]

    if not username or not password:
        print(
            "ERROR: ISONE_USERNAME / ISONE_PASSWORD not found in .env", file=sys.stderr
        )
        sys.exit(1)
    return username, password


def _api_get(
    endpoint: str, username: str, password: str, max_retries: int = 5
) -> dict[str, Any]:
    """Make an authenticated GET request to the ISO-NE API with retry on 429."""
    url = f"{ISONE_BASE_URL}{endpoint}"
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {credentials}",
    }

    for attempt in range(max_retries):
        req = urllib.request.Request(url, headers=headers)
        try:
            resp = urllib.request.urlopen(req, timeout=60)
            return loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries - 1:
                wait = min(2 ** (attempt + 1), 16)
                time.sleep(wait)
                continue
            raise
    msg = f"Unreachable: all {max_retries} retries exhausted for {endpoint}"
    raise RuntimeError(msg)


def _days_in_month(year: int, month: int) -> list[date]:
    """Return all dates in a given month."""
    n_days = calendar.monthrange(year, month)[1]
    return [date(year, month, d) for d in range(1, n_days + 1)]


def _parse_rcp_response(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse 5-minute RCP JSON response into flat records.

    Response shape: {"FiveMinRcps": {"FiveMinRcp": [...]}}
    Each item: {BeginDate, RegServiceClearingPrice, RegCapacityClearingPrice, HourEnd}
    """
    records: list[dict[str, Any]] = []
    rcp_list = data.get("FiveMinRcps", {}).get("FiveMinRcp", [])
    if not rcp_list:
        return records

    for item in rcp_list:
        begin = item.get("BeginDate")
        if begin is None:
            continue
        svc = item.get("RegServiceClearingPrice")
        cap = item.get("RegCapacityClearingPrice")
        if svc is None and cap is None:
            continue
        records.append(
            {
                "begin_date": begin,
                "reg_service_price": float(svc) if svc is not None else None,
                "reg_capacity_price": float(cap) if cap is not None else None,
            }
        )
    return records


def _parse_sysload_response(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse hourly system load JSON response into flat records.

    Response shape: {"HourlySystemLoads": {"HourlySystemLoad": [...]}}
    Each item: {BeginDate, Location: {..}, Load, NativeLoad, ArdDemand}
    """
    records: list[dict[str, Any]] = []
    load_list = data.get("HourlySystemLoads", {}).get("HourlySystemLoad", [])
    if not load_list:
        return records

    for item in load_list:
        begin = item.get("BeginDate")
        if begin is None:
            continue
        load_val = item.get("Load")
        native = item.get("NativeLoad")
        ard = item.get("ArdDemand")
        records.append(
            {
                "begin_date": begin,
                "system_load_mw": float(load_val) if load_val is not None else None,
                "native_load_mw": float(native) if native is not None else None,
                "ard_demand_mw": float(ard) if ard is not None else None,
            }
        )
    return records


def _fetch_day(
    day: date, username: str, password: str
) -> tuple[date, list[dict[str, Any]], list[dict[str, Any]]]:
    """Fetch both RCP and system load for a single day. Rate-limited."""
    day_str = day.strftime("%Y%m%d")

    rcp_data = _api_get(f"/fiveminutercp/final/day/{day_str}", username, password)
    time.sleep(1.0)

    sysload_data = _api_get(f"/hourlysysload/day/{day_str}", username, password)
    time.sleep(1.0)

    rcp_records = _parse_rcp_response(rcp_data)
    sysload_records = _parse_sysload_response(sysload_data)

    return day, rcp_records, sysload_records


def _aggregate_rcp_to_hourly(records: list[dict[str, Any]]) -> pl.DataFrame:
    """Aggregate 5-minute RCP records to hourly means."""
    if not records:
        return pl.DataFrame(
            schema={
                "interval_start_et": pl.Datetime("us", "America/New_York"),
                "reg_service_price_usd_per_mwh": pl.Float64,
                "reg_capacity_price_usd_per_mwh": pl.Float64,
            }
        )

    df = pl.DataFrame(records)
    df = df.with_columns(
        pl.col("begin_date")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S.000%z")
        .dt.convert_time_zone("America/New_York")
        .alias("timestamp")
    )
    df = df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))

    hourly = df.group_by("hour").agg(
        pl.col("reg_service_price").mean().alias("reg_service_price_usd_per_mwh"),
        pl.col("reg_capacity_price").mean().alias("reg_capacity_price_usd_per_mwh"),
    )
    return hourly.rename({"hour": "interval_start_et"}).sort("interval_start_et")


def _build_sysload_df(records: list[dict[str, Any]]) -> pl.DataFrame:
    """Build hourly system load DataFrame."""
    if not records:
        return pl.DataFrame(
            schema={
                "interval_start_et": pl.Datetime("us", "America/New_York"),
                "system_load_mw": pl.Float64,
                "native_load_mw": pl.Float64,
                "ard_demand_mw": pl.Float64,
            }
        )

    df = pl.DataFrame(records)
    df = df.with_columns(
        pl.col("begin_date")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S.000%z")
        .dt.convert_time_zone("America/New_York")
        .alias("interval_start_et")
    )
    return df.select(
        "interval_start_et", "system_load_mw", "native_load_mw", "ard_demand_mw"
    ).sort("interval_start_et")


def _parse_month(s: str) -> tuple[int, int]:
    """Parse 'YYYY-MM' string into (year, month)."""
    dt = datetime.strptime(s, "%Y-%m")
    return dt.year, dt.month


def _month_range(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    """Generate list of (year, month) from start through end inclusive."""
    months: list[tuple[int, int]] = []
    y, m = start
    while (y, m) <= end:
        months.append((y, m))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return months


def _last_complete_month() -> tuple[int, int]:
    today = datetime.now()
    if today.month == 1:
        return (today.year - 1, 12)
    return (today.year, today.month - 1)


def fetch_month(
    year: int,
    month: int,
    username: str,
    password: str,
    output_dir: Path,
    workers: int,
) -> bool:
    """Fetch, aggregate, and write one month of ancillary data. Returns True on success."""
    days = _days_in_month(year, month)

    all_rcp: list[dict[str, Any]] = []
    all_sysload: list[dict[str, Any]] = []

    # Sequential fetching to respect rate limits
    for day in days:
        try:
            _, rcp_records, sysload_records = _fetch_day(day, username, password)
            all_rcp.extend(rcp_records)
            all_sysload.extend(sysload_records)
        except urllib.error.HTTPError as e:
            print(f"    HTTP {e.code} for {day} — skipping day")
            continue
        except Exception as e:
            print(f"    Error fetching {day}: {e}")
            continue

    if not all_rcp and not all_sysload:
        return False

    rcp_hourly = _aggregate_rcp_to_hourly(all_rcp)
    sysload_df = _build_sysload_df(all_sysload)

    if rcp_hourly.height == 0 and sysload_df.height == 0:
        return False

    merged = rcp_hourly.join(sysload_df, on="interval_start_et", how="inner")
    merged = merged.sort("interval_start_et")

    merged = merged.select(
        pl.col("interval_start_et").cast(pl.Datetime("us", "America/New_York")),
        pl.col("reg_service_price_usd_per_mwh").cast(pl.Float64),
        pl.col("reg_capacity_price_usd_per_mwh").cast(pl.Float64),
        pl.col("system_load_mw").cast(pl.Float64),
        pl.col("native_load_mw").cast(pl.Float64),
        pl.col("ard_demand_mw").cast(pl.Float64),
    )

    part_dir = output_dir / f"year={year}" / f"month={month:02d}"
    part_dir.mkdir(parents=True, exist_ok=True)
    merged.write_parquet(part_dir / "data.parquet", compression="snappy")

    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch ISO-NE ancillary data (regulation prices + system load) → parquet",
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="First month to fetch (YYYY-MM)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="",
        help="Last month to fetch (YYYY-MM). Defaults to last complete month.",
    )
    parser.add_argument(
        "--path-local-parquet",
        type=str,
        required=True,
        help="Output directory for Hive-partitioned parquet",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )
    args = parser.parse_args()

    _reject_just_placeholders(args.path_local_parquet)
    output_dir = Path(args.path_local_parquet)
    output_dir.mkdir(parents=True, exist_ok=True)

    username, password = _load_credentials()

    start = _parse_month(args.start)
    end = _parse_month(args.end) if args.end else _last_complete_month()

    months = _month_range(start, end)
    if not months:
        print("No months in range.")
        return 0

    # Skip months that already exist
    to_fetch: list[tuple[int, int]] = []
    for y, m in months:
        part_file = output_dir / f"year={y}" / f"month={m:02d}" / "data.parquet"
        if part_file.exists():
            continue
        to_fetch.append((y, m))

    if not to_fetch:
        print("All partitions already exist. Nothing to fetch.")
        return 0

    print(f"Fetching {len(to_fetch)} months...")
    fetched = 0
    failed = 0

    for i, (y, m) in enumerate(to_fetch):
        if i > 0:
            print("  (cooling down 60s...)", flush=True)
            time.sleep(60)
        label = f"{y}-{m:02d}"
        print(f"  {label}: fetching...", end=" ", flush=True)
        ok = fetch_month(y, m, username, password, output_dir, args.workers)
        if ok:
            part_file = output_dir / f"year={y}" / f"month={m:02d}" / "data.parquet"
            n_rows = pl.read_parquet(part_file).height
            print(f"{n_rows} rows")
            fetched += 1
        else:
            print("no data")
            failed += 1

    print(f"\nDone. {fetched} partitions written, {failed} months with no data.")
    print(f"Output: {output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
