#!/usr/bin/env python3
"""Fetch NYISO zonal load data from the NYISO MIS portal.

Downloads monthly ZIP archives of "Integrated Real-Time Actual Load" from
mis.nyiso.com, extracts daily CSVs, normalizes to canonical NYISO zone names,
and writes to local partitioned parquet.

This script exists because EIA's API does not serve pre-2019 hourly demand
data. NYISO MIS archives go back to ~2001 with sub-MW precision.

Output schema:
    timestamp (tz-aware America/New_York), zone (canonical NYISO name), load_mw

Output layout:
    <path_local_zone_parquet>/zone={NAME}/year=YYYY/month=M/data.parquet

Zone naming matches the LBMP pipeline (data/nyiso/lbmp/): WEST, GENESE,
CENTRAL, NORTH, MHK_VL, CAPITL, HUD_VL, MILLWD, DUNWOD, N.Y.C., LONGIL.

Usage:
    uv run python data/nyiso/hourly_demand/fetch_nyiso_zone_loads.py \\
        --year 2018 \\
        --path-local-zones data/nyiso/hourly_demand/zones/
"""

from __future__ import annotations

import argparse
import calendar
import io
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl
import requests

BASE_URL = "https://mis.nyiso.com/public/csv/palIntegrated/"
ZIP_SUFFIX = "palIntegrated_csv.zip"
USER_AGENT = "Switchbox-rate-design-platform/1.0 (NYISO load)"

# Raw NYISO zone name misspellings -> canonical (same as LBMP pipeline)
ZONE_NAME_NORMALIZE: dict[str, str] = {
    "CENTRL": "CENTRAL",
}

EXPECTED_ZONES = frozenset(
    {
        "WEST",
        "GENESE",
        "CENTRAL",
        "NORTH",
        "MHK_VL",
        "CAPITL",
        "HUD_VL",
        "MILLWD",
        "DUNWOD",
        "N.Y.C.",
        "LONGIL",
    }
)


def _reject_just_placeholders(val: str) -> None:
    if "{{" in val or "}}" in val:
        raise ValueError(
            f"Path looks like uninterpolated Just: {val!r}. Pass a resolved path."
        )


def _zip_url(year: int, month: int) -> str:
    return f"{BASE_URL}{year}{month:02d}01{ZIP_SUFFIX}"


def _fetch_one_zip(url: str) -> bytes | None:
    """Download one ZIP; return bytes or None on failure."""
    try:
        r = requests.get(url, timeout=60, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        return r.content
    except requests.RequestException as e:
        print(f"Warning: {url} -> {e}", file=sys.stderr)
        return None


def fetch_zips(year: int, workers: int = 8) -> dict[int, bytes]:
    """Download 12 monthly ZIPs for *year*. Returns ``{month: zip_bytes}``."""
    urls = {m: _zip_url(year, m) for m in range(1, 13)}
    result: dict[int, bytes] = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_fetch_one_zip, url): month for month, url in urls.items()
        }
        for future in as_completed(futures):
            month = futures[future]
            data = future.result()
            if data is not None:
                result[month] = data
                print(f"  Downloaded {year}-{month:02d}")
            else:
                print(f"  FAILED {year}-{month:02d}", file=sys.stderr)

    return result


def _read_csv_from_bytes(data: bytes) -> pl.DataFrame:
    """Parse one daily CSV from a palIntegrated ZIP."""
    lines = data.decode("utf-8", errors="replace").splitlines()
    if not lines or len(lines) < 2:
        return pl.DataFrame()

    raw_header = [c.strip().rstrip("\r").strip('"') for c in lines[0].split(",")]
    body = "\n".join(lines[1:])
    return pl.read_csv(
        io.BytesIO(body.encode("utf-8")),
        has_header=False,
        new_columns=raw_header,
        infer_schema_length=0,
    )


def extract_and_parse_zip(zip_bytes: bytes) -> pl.DataFrame:
    """Extract daily CSVs from a monthly ZIP, concatenate, parse, and normalize."""
    dfs: list[pl.DataFrame] = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            for name in sorted(z.namelist()):
                if not name.endswith(".csv"):
                    continue
                with z.open(name) as f:
                    df = _read_csv_from_bytes(f.read())
                    if not df.is_empty():
                        dfs.append(df)
    except (zipfile.BadZipFile, KeyError) as e:
        print(f"Warning: bad zip: {e}", file=sys.stderr)
        return pl.DataFrame()

    if not dfs:
        return pl.DataFrame()

    combined = pl.concat(dfs)

    # Parse timestamps -- same approach as LBMP pipeline: tz-aware with
    # ambiguous="earliest" for DST fall-back hour.
    ts = pl.col("Time Stamp")
    with_sec = ts.str.to_datetime(
        format="%m/%d/%Y %H:%M:%S",
        time_zone="America/New_York",
        strict=False,
        ambiguous="earliest",
    )
    without_sec = ts.str.to_datetime(
        format="%m/%d/%Y %H:%M",
        time_zone="America/New_York",
        strict=False,
        ambiguous="earliest",
    )
    combined = combined.with_columns(
        pl.coalesce(with_sec, without_sec).alias("timestamp")
    )

    combined = combined.with_columns(
        pl.col("Integrated Load").cast(pl.Float64).alias("load_mw")
    )

    # Normalize zone names: fix misspellings, then spaces -> _ (same as LBMP pipeline)
    combined = combined.with_columns(
        pl.col("Name")
        .str.strip_chars()
        .replace(ZONE_NAME_NORMALIZE)
        .str.replace_all(" ", "_")
        .alias("zone")
    )

    return combined.select(["timestamp", "zone", "load_mw"])


def validate_zone_data(df: pl.DataFrame, zones: list[str], year: int) -> None:
    """Validate completeness and data quality per zone."""
    print(f"\nValidating {year} zone data...")

    for zone in zones:
        zone_df = df.filter(pl.col("zone") == zone)
        n = len(zone_df)

        if n == 0:
            print(f"  WARNING: Zone {zone}: No data found")
            continue

        # 8760 for non-leap, 8784 for leap (before Cairo normalization)
        is_leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
        expected = 8784 if is_leap else 8760
        if n != expected:
            print(f"  WARNING: Zone {zone}: Expected {expected} hours, got {n}")
        else:
            print(f"  Zone {zone}: {n} hours")

        null_count = zone_df["load_mw"].null_count()
        if null_count > 0:
            print(f"  WARNING: Zone {zone}: {null_count} null load values")

        negative_count = (zone_df["load_mw"] < 0).sum()
        if negative_count > 0:
            print(f"  WARNING: Zone {zone}: {negative_count} negative load values")

        high_count = (zone_df["load_mw"] > 50000).sum()
        if high_count > 0:
            print(f"  WARNING: Zone {zone}: {high_count} values > 50,000 MW")


def fill_missing_hours(df: pl.DataFrame, zones: list[str], year: int) -> pl.DataFrame:
    """Fill missing hours with linear interpolation (max 2 consecutive gap).

    Generates the expected hourly grid for the year, identifies gaps per zone,
    and interpolates. Raises if any gap exceeds 2 consecutive hours.
    """
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("America/New_York")

    from datetime import datetime

    start_dt = datetime(year, 1, 1, tzinfo=tz)
    last_day = calendar.monthrange(year, 12)[1]
    end_dt = datetime(year, 12, last_day, hour=23, tzinfo=tz)

    all_ts: list[datetime] = []
    current = start_dt
    while current <= end_dt:
        for hour in range(24):
            try:
                candidate = current.replace(
                    hour=hour, minute=0, second=0, microsecond=0
                )
                # Round-trip through UTC to verify the hour exists
                utc = candidate.astimezone(ZoneInfo("UTC"))
                back = utc.astimezone(tz)
                if back.hour == candidate.hour:
                    all_ts.append(candidate)
            except (ValueError, OSError):
                pass
        current += timedelta(days=1)

    print(f"\nChecking for missing hours ({len(all_ts)} expected)...")

    all_zones_data: list[pl.DataFrame] = []

    for zone in zones:
        zone_df = df.filter(pl.col("zone") == zone).sort("timestamp")
        actual = set(zone_df["timestamp"].to_list())
        expected = set(all_ts)
        missing = sorted(expected - actual)

        if not missing:
            print(f"  Zone {zone}: Complete")
            all_zones_data.append(zone_df)
            continue

        # Check consecutive gap lengths
        gaps: list[list[datetime]] = []
        current_gap = [missing[0]]
        for i in range(1, len(missing)):
            if missing[i] - missing[i - 1] == timedelta(hours=1):
                current_gap.append(missing[i])
            else:
                gaps.append(current_gap)
                current_gap = [missing[i]]
        gaps.append(current_gap)

        max_gap = max(len(g) for g in gaps)
        if max_gap > 2:
            raise ValueError(
                f"Zone {zone} has {max_gap} consecutive missing hours "
                f"(first gap at {gaps[0][0]}). Cannot safely interpolate."
            )

        print(
            f"  Zone {zone}: {len(missing)} missing hour(s), max consecutive: {max_gap}"
        )

        complete_df = pl.DataFrame({"timestamp": all_ts, "zone": [zone] * len(all_ts)})
        filled_df = complete_df.join(zone_df, on=["timestamp", "zone"], how="left")
        filled_df = filled_df.with_columns(
            pl.col("load_mw").interpolate(method="linear").alias("load_mw")
        )
        all_zones_data.append(filled_df)
        print("    Filled via linear interpolation")

    return pl.concat(all_zones_data)


def write_zone_data_local(df: pl.DataFrame, local_base: str) -> None:
    """Write zone load data to local partitioned parquet.

    Layout: <local_base>/zone={NAME}/year=YYYY/month=M/data.parquet
    """
    df = df.with_columns(
        [
            pl.col("timestamp").dt.year().alias("year"),
            pl.col("timestamp").dt.month().alias("month"),
        ]
    )
    partition_count = df.select(["zone", "year", "month"]).n_unique()
    print(f"\nWriting {partition_count} partitions ({len(df):,} rows) to {local_base}")
    Path(local_base).mkdir(parents=True, exist_ok=True)
    df.write_parquet(
        str(local_base),
        compression="zstd",
        partition_by=["zone", "year", "month"],
    )
    print(f"Wrote partitioned zone data to {local_base}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch NYISO zonal load data from MIS portal, "
        "write to local parquet with canonical NYISO zone names."
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Calendar year to fetch (e.g. 2018).",
    )
    parser.add_argument(
        "--path-local-zones",
        dest="path_local_zones",
        type=str,
        required=True,
        help="Local directory for zone parquet output.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Max concurrent downloads (default: 8).",
    )

    args = parser.parse_args()
    _reject_just_placeholders(args.path_local_zones)
    year = args.year
    zones = sorted(EXPECTED_ZONES)

    print("=" * 60)
    print("NYISO Zonal Load Fetch (MIS Portal)")
    print("=" * 60)
    print(f"Year: {year}")
    print(f"Source: {BASE_URL}")
    print(f"Zones: {len(zones)} NYISO load zones")
    print(f"Output: {args.path_local_zones}")
    print("=" * 60)

    # 1. Download monthly ZIPs
    print(f"\nDownloading {year} monthly ZIPs...")
    zips = fetch_zips(year, workers=args.workers)

    if len(zips) != 12:
        missing = sorted(set(range(1, 13)) - set(zips.keys()))
        print(f"ERROR: Missing months: {missing}", file=sys.stderr)
        sys.exit(1)

    print("  All 12 months downloaded")

    # 2. Extract and parse
    print("\nExtracting and parsing CSVs...")
    monthly_dfs: list[pl.DataFrame] = []

    for month in sorted(zips.keys()):
        df = extract_and_parse_zip(zips[month])
        if df.is_empty():
            print(f"  WARNING: No data for {year}-{month:02d}", file=sys.stderr)
        else:
            print(f"  {year}-{month:02d}: {len(df):,} rows")
            monthly_dfs.append(df)

    if not monthly_dfs:
        print("ERROR: No data extracted", file=sys.stderr)
        sys.exit(1)

    df = pl.concat(monthly_dfs)

    # Keep only recognized NYISO load zones (drop aggregates/totals if any)
    unmapped = df.filter(~pl.col("zone").is_in(zones))
    if len(unmapped) > 0:
        unmapped_zones = unmapped["zone"].unique().to_list()
        print(
            f"\n  Dropping {len(unmapped):,} rows with unknown zones: {unmapped_zones}"
        )
    df = df.filter(pl.col("zone").is_in(zones))

    print(f"\n  Parsed {len(df):,} rows across {df['zone'].n_unique()} zones")
    print(f"  Zones: {sorted(df['zone'].unique().to_list())}")

    # 3. Fill missing hours
    df = fill_missing_hours(df, zones, year)

    # 4. Validate
    validate_zone_data(df, zones, year)

    # 5. Summary
    print("\n" + "=" * 60)
    print("DATA SUMMARY")
    print("=" * 60)
    print(f"Total rows: {len(df):,}")
    print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Zones: {df['zone'].unique().sort().to_list()}")
    print("\nLoad statistics (MW):")
    print(f"  Min:  {df['load_mw'].min():.2f}")
    print(f"  Max:  {df['load_mw'].max():.2f}")
    print(f"  Mean: {df['load_mw'].mean():.2f}")
    print("\nSample data (first 5 rows):")
    print(df.head(5))
    print("\nSample data (last 5 rows):")
    print(df.tail(5))

    # 6. Write to local parquet
    print("\n" + "=" * 60)
    print("WRITING LOCAL PARQUET")
    print("=" * 60)
    write_zone_data_local(df, args.path_local_zones)

    print("\n" + "=" * 60)
    print(f"NYISO {year} zonal load fetch complete")
    print("  Next: aggregate to utility loads, then upload")
    print("=" * 60)


if __name__ == "__main__":
    main()
