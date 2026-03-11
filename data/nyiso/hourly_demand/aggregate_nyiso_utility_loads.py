#!/usr/bin/env python3
"""Aggregate NYISO zone loads to utility-level profiles.

Reads zone parquet from local (partitioned by zone/year/month with canonical
NYISO zone names), maps zones to utilities via the NYISO zone mapping CSV,
and writes utility-level aggregated load to local partitioned parquet.

Upload to S3 via data/nyiso/hourly_demand Justfile upload recipe.

Input:  local zone parquet: zone={NAME}/year=YYYY/month=M/data.parquet
Output: local utility parquet: utility={name}/year=YYYY/month=M/data.parquet

Usage:
    uv run python data/nyiso/hourly_demand/aggregate_nyiso_utility_loads.py \\
        --year 2018 --utility all \\
        --path-zone-mapping-csv data/nyiso/zone_mapping/csv/ny_utility_zone_mapping.csv \\
        --path-local-zones data/nyiso/hourly_demand/zones/ \\
        --path-local-utilities data/nyiso/hourly_demand/utilities/
"""

from __future__ import annotations

import argparse
import io
from pathlib import Path

import polars as pl
from cloudpathlib import S3Path


def load_zone_mapping(path: str) -> pl.DataFrame:
    """Load NYISO zone mapping CSV (local or S3).

    Returns DataFrame with at least: utility, lbmp_zone_name.
    """
    if path.startswith("s3://"):
        csv_bytes = S3Path(path).read_bytes()
        df = pl.read_csv(io.BytesIO(csv_bytes))
    else:
        df = pl.read_csv(path)

    required = {"utility", "lbmp_zone_name"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Zone mapping CSV missing columns: {sorted(missing)}")
    return df


def get_utility_zone_mapping(mapping_df: pl.DataFrame) -> dict[str, list[str]]:
    """Extract utility -> list of NYISO zone names from the zone mapping.

    Uses lbmp_zone_name (canonical NYISO names like WEST, GENESE, etc.).
    Deduplicates zones per utility (e.g. ConEd has multiple rows for the
    same zones with different capacity weights).
    """
    result: dict[str, list[str]] = {}
    for row in (
        mapping_df.select("utility", "lbmp_zone_name").unique().iter_rows(named=True)
    ):
        utility = str(row["utility"])
        zone = str(row["lbmp_zone_name"])
        result.setdefault(utility, []).append(zone)
    return {u: sorted(set(z)) for u, z in result.items()}


def load_zone_data(
    zone_base: str,
    year: int,
    zones: list[str],
) -> pl.DataFrame:
    """Load zone load data from local parquet for specified zones and year.

    Validates that all 12 months are present for each zone.
    """
    print("\n" + "=" * 60)
    print("LOADING ZONE DATA")
    print("=" * 60)

    collected = (
        pl.scan_parquet(zone_base)
        .filter(pl.col("zone").is_in(zones))
        .filter(pl.col("year") == year)
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from zone data collect()")
    df: pl.DataFrame = collected

    if df.is_empty():
        raise ValueError(
            f"No zone data found for year={year}, zones={zones}. "
            f"Run fetch-zone-data first, then ensure {zone_base} "
            f"contains zone={{NAME}}/year={year}/month={{M}}/ partitions."
        )

    if "month" not in df.columns:
        raise ValueError("Expected 'month' partition column missing from zone data")

    missing_data = []
    for zone in zones:
        zone_months = (
            df.filter(pl.col("zone") == zone)
            .select(pl.col("month").cast(pl.Int32).unique().sort())
            .to_series()
            .to_list()
        )
        for month in sorted(set(range(1, 13)) - set(zone_months)):
            missing_data.append(f"Zone {zone}: month {year}-{month:02d} missing")

    if missing_data:
        for item in missing_data:
            print(f"  {item}")
        raise ValueError(
            f"Incomplete data: {len(missing_data)} missing partition(s). "
            f"All 12 months required for {year}."
        )

    print(f"  Loaded zone data for year={year}, {len(zones)} zones")
    return df


def aggregate_utility_load(
    zone_df: pl.DataFrame, utility_name: str, zones: list[str]
) -> pl.DataFrame:
    """Sum zone loads for a single utility by timestamp."""
    utility_data = zone_df.filter(pl.col("zone").is_in(zones))
    if len(utility_data) == 0:
        raise ValueError(f"No data found for utility {utility_name} zones {zones}")

    return (
        utility_data.group_by("timestamp")
        .agg(pl.col("load_mw").sum().alias("load_mw"))
        .with_columns(pl.lit(utility_name).alias("utility"))
        .select(["timestamp", "utility", "load_mw"])
        .sort("timestamp")
    )


def write_utility_loads_local(
    utility_df: pl.DataFrame,
    utility_base: str,
    utility_name: str,
) -> None:
    """Write utility load parquet to local dir (partitioned for S3 sync)."""
    output_df = utility_df.with_columns(
        [
            pl.col("timestamp").dt.year().alias("year"),
            pl.col("timestamp").dt.month().alias("month"),
        ]
    )
    Path(utility_base).mkdir(parents=True, exist_ok=True)
    output_df.write_parquet(
        utility_base,
        compression="zstd",
        partition_by=["utility", "year", "month"],
    )
    print(f"  Wrote utility={utility_name} partitions under {utility_base}")


def expected_hours_for_year(year: int) -> int:
    """Expected hourly records for a year in Eastern timezone.

    Accounts for DST: spring forward loses 1 hour, fall back gains 1, net = 0.
    So non-leap years have 8760, leap years have 8784.
    """
    is_leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    return 8784 if is_leap else 8760


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate NYISO zone loads to utility-level profiles."
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Calendar year to process.",
    )
    parser.add_argument(
        "--utility",
        type=str,
        default="all",
        help="Specific utility name, or 'all' (default: all).",
    )
    parser.add_argument(
        "--path-zone-mapping-csv",
        dest="path_zone_mapping_csv",
        type=str,
        required=True,
        help="Path to ny_utility_zone_mapping.csv (local or S3).",
    )
    parser.add_argument(
        "--path-local-zones",
        dest="path_local_zones",
        type=str,
        required=True,
        help="Local directory with zone parquet inputs.",
    )
    parser.add_argument(
        "--path-local-utilities",
        dest="path_local_utilities",
        type=str,
        required=True,
        help="Local directory for utility parquet output.",
    )

    args = parser.parse_args()
    year = args.year

    mapping_df = load_zone_mapping(args.path_zone_mapping_csv)
    utility_zone_map = get_utility_zone_mapping(mapping_df)

    selected = args.utility.lower()
    if selected != "all" and selected not in utility_zone_map:
        valid = ", ".join(sorted(utility_zone_map.keys()))
        parser.error(f"Invalid --utility '{args.utility}'. Valid: all, {valid}")

    utilities_to_process = (
        sorted(utility_zone_map.keys()) if selected == "all" else [selected]
    )

    all_zones_needed = set()
    for u in utilities_to_process:
        all_zones_needed.update(utility_zone_map[u])

    print("=" * 60)
    print("NYISO UTILITY LOAD AGGREGATION")
    print("=" * 60)
    print(f"Year: {year}")
    print(f"Zone input: {args.path_local_zones}")
    print(f"Utility output: {args.path_local_utilities}")
    print(f"Utilities: {', '.join(utilities_to_process)}")
    print(f"Zones needed: {sorted(all_zones_needed)}")
    print("=" * 60)

    zone_df = load_zone_data(args.path_local_zones, year, sorted(all_zones_needed))
    print(f"  Total zone rows: {len(zone_df):,}")

    expected = expected_hours_for_year(year)

    for utility_name in utilities_to_process:
        zones = utility_zone_map[utility_name]
        print(f"\n{'=' * 60}")
        print(f"UTILITY: {utility_name}")
        print(f"{'=' * 60}")
        print(f"  Zones: {zones}")

        utility_df = aggregate_utility_load(zone_df, utility_name, zones)
        n = len(utility_df)
        print(f"  Aggregated to {n:,} hourly records")

        if n != expected:
            print(f"  WARNING: Expected {expected} hours, got {n}")
        else:
            print(f"  Hour count: {expected}")

        print("\n  Load statistics (MW):")
        print(f"    Min:  {utility_df['load_mw'].min():.2f}")
        print(f"    Max:  {utility_df['load_mw'].max():.2f}")
        print(f"    Mean: {utility_df['load_mw'].mean():.2f}")

        write_utility_loads_local(
            utility_df,
            args.path_local_utilities,
            utility_name,
        )

    print(f"\n{'=' * 60}")
    print("All utilities processed")
    print("  Run Justfile upload recipe to sync to S3")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
