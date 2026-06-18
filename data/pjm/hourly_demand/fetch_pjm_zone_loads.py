#!/usr/bin/env python3
"""Fetch PJM zonal hourly metered load from the Data Miner 2 API.

Pulls the `hrl_load_metered`
(https://dataminer2.pjm.com/feed/hrl_load_metered/definition) feed for the
Maryland transmission zones, aggregates the per-`load_area` rows up to the
zone level, and writes local partitioned parquet (uploaded to S3 via the
Justfile `upload` recipe).

Why PJM-native (not EIA-930): EIA only re-publishes the demand PJM submits and
has recurring ~24-hour gaps around DST transitions for PJM subregions. Data
Miner is the authoritative upstream source, complete across those days, and
ships a native `datetime_beginning_utc` column.

Key feed facts (confirmed against the live feed):
- Zones use PJM Data Miner *legacy* codes — BGE is `zone="BC"` (not "BGE").
- The feed is at `load_area` granularity; a zone can span several load areas
  (e.g. `PEP -> {PEPCO, SMECO}`, `DPL -> {DPLCO, EASTON}`), so a zone series is
  the sum of `mw` over its load areas per hour.
- The calendar year is taken from `datetime_beginning_ept` (Eastern Prevailing
  Time), NOT from the UTC column, to avoid a ~5-hour calendar misalignment.

Output schema:
    timestamp (tz-aware America/New_York), zone (Data Miner code), load_mw

Output layout:
    <path_local_zones>/zone={CODE}/year=YYYY/data.parquet

Usage:
    uv run python data/pjm/hourly_demand/fetch_pjm_zone_loads.py \\
        --year 2025 \\
        --path-local-zones data/pjm/hourly_demand/zones/
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import polars as pl

from data.pjm.dataminer import (
    ROW_COUNT,
    ept_date_filter,
    fetch_date_range,
    parse_ts_column,
)
from data.pjm.hourly_demand.validate_pjm_demand_parquet import validate_zone_loads
from data.pjm.zone_mapping.generate_zone_mapping_csv import build_zone_mapping

_FEED = "hrl_load_metered"
_TIMEZONE = "America/New_York"


def _reject_just_placeholders(val: str) -> None:
    if "{{" in val or "}}" in val:
        raise ValueError(
            f"Path looks like uninterpolated Just: {val!r}. Pass a resolved path."
        )


def md_zone_codes() -> list[str]:
    """Return the sorted set of PJM Data Miner zone codes for Maryland utilities."""
    mapping = build_zone_mapping().filter(pl.col("state") == "md")
    return sorted(mapping["dataminer_zone"].unique().to_list())


def fetch_zone_loads(
    year: int,
    zones: list[str],
    *,
    api_key: str | None = None,
    page_delay_seconds: float = 11.0,
) -> pl.DataFrame:
    """Fetch and aggregate hourly metered load for *zones* over calendar *year*.

    Returns a tidy DataFrame with one row per (zone, hour):
    ``timestamp`` (tz-aware Eastern), ``zone`` (Data Miner code), ``load_mw``.
    """

    def build_params(
        chunk_start: date, chunk_end: date, is_archive: bool
    ) -> dict[str, str | int]:
        # The load feed has no `type`/`pnode_id`; filter to zones client-side so
        # the same params work for both archive and standard chunks.
        return {
            "datetime_beginning_ept": ept_date_filter(chunk_start, chunk_end),
            "rowCount": ROW_COUNT,
        }

    rows = fetch_date_range(
        _FEED,
        date(year, 1, 1),
        date(year, 12, 31),
        build_params,
        api_key=api_key,
        page_delay_seconds=page_delay_seconds,
    )
    if not rows:
        raise SystemExit(f"ERROR: no {_FEED} rows returned for year {year}.")

    df = pl.DataFrame(rows)
    df = df.rename({c: c.lower() for c in df.columns})

    required = {"datetime_beginning_utc", "zone", "load_area", "mw"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{_FEED} response missing expected columns: {sorted(missing)}"
        )

    df = df.filter(pl.col("zone").is_in(zones))
    if df.is_empty():
        raise SystemExit(
            f"ERROR: no rows for zones {zones} in {year}. "
            f"Returned zones: {sorted(pl.DataFrame(rows)['zone'].unique().to_list())}"
        )

    # UTC instant -> tz-aware Eastern. Deriving the wall-clock from UTC makes the
    # fall-back DST hours two distinct instants (no collisions) and the year
    # filter below operate on Eastern wall-clock time.
    df = parse_ts_column(df, "datetime_beginning_utc")
    df = df.with_columns(
        pl.col("datetime_beginning_utc")
        .dt.replace_time_zone("UTC")
        .dt.convert_time_zone(_TIMEZONE)
        .alias("timestamp"),
        pl.col("mw").cast(pl.Float64),
    )

    # Sum load areas to a single series per zone-hour.
    df = (
        df.group_by("zone", "timestamp")
        .agg(pl.col("mw").sum().alias("load_mw"))
        .filter(pl.col("timestamp").dt.year() == year)
        .select("timestamp", "zone", "load_mw")
        .sort("zone", "timestamp")
    )
    return df


def write_zone_data_local(df: pl.DataFrame, local_base: str) -> None:
    """Write zone load data to local Hive parquet (zone={CODE}/year=YYYY/data.parquet).

    Writes one ``data.parquet`` per partition (matching the documented S3 layout
    and the ISO-NE pipeline) rather than relying on Polars' auto-named partition
    files. Partition columns are encoded in the path, not the file.
    """
    df = df.with_columns(pl.col("timestamp").dt.year().alias("year"))
    base = Path(local_base)
    partitions = df.partition_by(["zone", "year"], as_dict=True)
    print(f"\nWriting {len(partitions)} partitions ({len(df):,} rows) to {base}")
    for (zone, year), part_df in partitions.items():
        part_dir = base / f"zone={zone}" / f"year={year}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_df.select("timestamp", "load_mw").write_parquet(
            part_dir / "data.parquet", compression="zstd"
        )
    print(f"Wrote partitioned zone data to {base}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PJM zonal hourly metered load from Data Miner 2."
    )
    parser.add_argument(
        "--year", type=int, required=True, help="Calendar year to fetch (Eastern)."
    )
    parser.add_argument(
        "--path-local-zones",
        dest="path_local_zones",
        type=str,
        required=True,
        help="Local directory for zone parquet output.",
    )
    parser.add_argument(
        "--zones",
        type=str,
        default=None,
        help="Comma-separated Data Miner zone codes (default: all MD zones).",
    )
    parser.add_argument(
        "--pjm-api-key",
        dest="pjm_api_key",
        type=str,
        default=None,
        help="PJM Data Miner API key (optional if PJM_API_PRIMARY_KEY is in .env).",
    )
    parser.add_argument(
        "--page-delay",
        type=float,
        default=11.0,
        metavar="SECONDS",
        help="Seconds between paginated API requests (default: 11.0, ~5.5 req/min).",
    )

    args = parser.parse_args()
    _reject_just_placeholders(args.path_local_zones)
    year = args.year
    zones = (
        [z.strip() for z in args.zones.split(",")] if args.zones else md_zone_codes()
    )

    print("=" * 60)
    print("PJM Zonal Metered Load Fetch (Data Miner 2)")
    print("=" * 60)
    print(f"Year: {year}")
    print(f"Feed: {_FEED}")
    print(f"Zones: {zones}")
    print(f"Output: {args.path_local_zones}")
    print("=" * 60)

    df = fetch_zone_loads(
        year,
        zones,
        api_key=args.pjm_api_key,
        page_delay_seconds=args.page_delay,
    )

    print(f"\nFetched {len(df):,} zone-hour rows across {df['zone'].n_unique()} zones")
    print("\nLoad statistics (MW):")
    print(f"  Min:  {df['load_mw'].min():.2f}")
    print(f"  Max:  {df['load_mw'].max():.2f}")
    print(f"  Mean: {df['load_mw'].mean():.2f}")

    print("\nValidating completeness...")
    ok, msgs = validate_zone_loads(df, zones, year)
    for m in msgs:
        print(f"  {m}")
    if not ok:
        sys.exit("\nERROR: validation failed; not writing output.")

    write_zone_data_local(df, args.path_local_zones)

    print("\n" + "=" * 60)
    print(f"PJM {year} zonal load fetch complete")
    print("  Next: aggregate to utility loads, then upload")
    print("=" * 60)


if __name__ == "__main__":
    main()
