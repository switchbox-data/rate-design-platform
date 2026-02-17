"""Aggregate state-level zone loads to utility-level profiles.

Reads zone parquet from a local dir (partitioned by region/zone/year/month),
applies zone-to-utility mapping, and writes utility-level aggregated load to local parquet.
Upload to S3 via data/eia/hourly_loads Justfile upload recipe.

Operates on complete calendar years only - validates that all 12 months are available.

Input:  local zone parquet (same layout as S3)
Output: local utility parquet (same layout as S3 for sync)
"""

import argparse

import polars as pl
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import (
    get_state_config,
    get_utility_zone_mapping_for_state,
)


def load_zone_data(
    zone_base: str,
    iso_region: str,
    year: int,
    zones: list[str],
) -> pl.DataFrame:
    """Load zone load data from local parquet dir for specified zones and year.

    Reads from Hive-style partitioned structure:
    region=<iso_region>/zone=X/year=YYYY/month=M/data.parquet
    Validates that all 12 months are present for each zone before loading.

    Args:
        zone_base: Local directory with partitioned zone parquet (same layout as S3)
        iso_region: ISO region partition key (e.g., nyiso, isone)
        year: Calendar year (must have all 12 months)
        zones: List of zone identifiers (e.g., ["A", "B", "C"])

    Returns:
        Combined DataFrame with all zone data

    Raises:
        ValueError: If any zone is missing months or if data validation fails
    """
    print("\n" + "=" * 60)
    print("LOADING DATA")
    print("=" * 60)

    collected = (
        pl.scan_parquet(zone_base)
        .filter(pl.col("region") == iso_region)
        .filter(pl.col("zone").is_in(zones))
        .filter(pl.col("year") == year)
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from zone data collect()")
    combined = collected

    if combined.is_empty():
        raise ValueError(
            f"No zone data found for region={iso_region}, year={year}, zones={zones}. "
            "Fetch zone data first (data/eia/hourly_loads Justfile fetch-zone-data), "
            f"then ensure {zone_base} contains region={iso_region}/zone=X/year={year}/month=M/."
        )

    if "month" not in combined.columns:
        raise ValueError("Expected 'month' partition column is missing from zone data")

    missing_data = []
    for zone in zones:
        zone_months = (
            combined.filter(pl.col("zone") == zone)
            .select(pl.col("month").cast(pl.Int32).unique().sort())
            .to_series()
            .to_list()
        )
        missing_months = sorted(set(range(1, 13)) - set(zone_months))
        for month in missing_months:
            missing_data.append(f"Zone {zone}: month {year}-{month:02d} missing")

    if missing_data:
        print("\n❌ INCOMPLETE DATA - Missing the following:")
        for item in missing_data:
            print(f"  • {item}")
        raise ValueError(
            f"Cannot proceed with incomplete data. Missing {len(missing_data)} partition(s). "
            f"All 12 months required for calendar year {year}. Re-run fetch-zone-data to backfill."
        )

    print(f"✓ Loaded complete zone data for region={iso_region}, year={year}")
    return combined


def aggregate_utility_load(
    zone_df: pl.DataFrame, utility_name: str, zones: list[str]
) -> pl.DataFrame:
    """Aggregate zone loads for a single utility.

    Args:
        zone_df: DataFrame with zone load data
        utility_name: Name of the utility
        zones: List of zones served by this utility

    Returns:
        DataFrame with aggregated utility load
        Schema: timestamp, utility, load_mw
    """
    # Filter to zones served by this utility
    utility_data = zone_df.filter(pl.col("zone").is_in(zones))

    if len(utility_data) == 0:
        raise ValueError(f"No data found for utility {utility_name} zones {zones}")

    # Aggregate by summing loads across zones for each timestamp
    aggregated = (
        utility_data.group_by("timestamp")
        .agg(
            [
                pl.col("load_mw").sum().alias("load_mw"),
            ]
        )
        .with_columns(
            [
                pl.lit(utility_name).alias("utility"),
            ]
        )
        .select(["timestamp", "utility", "load_mw"])
        .sort("timestamp")
    )

    return aggregated


def expected_hours_for_year(year: int) -> int:
    """Expected hourly records for Eastern timezone year (accounts for spring DST gap)."""
    is_leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    return (8784 if is_leap else 8760) - 1


def write_utility_loads_local(
    utility_df: pl.DataFrame,
    utility_base: str,
    iso_region: str,
    utility_name: str,
) -> None:
    """Write utility load parquet to local dir (same layout as S3 for later sync)."""
    output_df = utility_df.with_columns(
        [
            pl.lit(iso_region).alias("region"),
            pl.col("timestamp").dt.year().alias("year"),
            pl.col("timestamp").dt.month().alias("month"),
        ]
    )
    output_df.write_parquet(
        utility_base,
        compression="zstd",
        partition_by=["region", "utility", "year", "month"],
    )
    print(
        "\n✓ Wrote utility partitioned data under "
        f"{utility_base}/region={iso_region}/utility={utility_name}/"
    )


def process_all_utilities(
    zone_base: str,
    utility_base: str,
    iso_region: str,
    year: int,
    utility_zone_mapping: dict[str, list[str]],
):
    """Process all utilities and write aggregated load profiles to local parquet.

    Args:
        zone_base: Local path to zone parquet (partitioned)
        utility_base: Local path for utility parquet output (partitioned)
        iso_region: ISO region partition key (nyiso/isone)
        year: Calendar year to process
        utility_zone_mapping: Utility to zones mapping for selected state
    """
    # Collect all unique zones needed
    all_zones = set()
    for zones in utility_zone_mapping.values():
        all_zones.update(zones)

    print(f"\nZones needed: {sorted(all_zones)}")
    print(f"Calendar year: {year}")

    # Load all zone data once (validates all 12 months present)
    zone_df = load_zone_data(zone_base, iso_region, year, sorted(all_zones))

    print(f"\n{'=' * 60}")
    print(f"Total zone data loaded: {len(zone_df):,} rows")
    print(f"Date range: {zone_df['timestamp'].min()} to {zone_df['timestamp'].max()}")
    print(f"{'=' * 60}")

    expected_hours = expected_hours_for_year(year)

    # Process each utility
    for utility_name, zones in utility_zone_mapping.items():
        print(f"\n{'=' * 60}")
        print(f"UTILITY: {utility_name}")
        print(f"{'=' * 60}")
        print(f"Zones: {zones}")

        # Aggregate utility load
        utility_df = aggregate_utility_load(zone_df, utility_name, zones)
        print(f"\nAggregated to {len(utility_df):,} hourly records")

        if len(utility_df) != expected_hours:
            print(f"⚠️  Expected {expected_hours} hours, got {len(utility_df)}")
        else:
            print(f"✓ Hour count matches expected: {expected_hours}")

        # Show data summary
        print("\nLoad statistics (MW):")
        print(f"  Min:  {utility_df['load_mw'].min():.2f}")
        print(f"  Max:  {utility_df['load_mw'].max():.2f}")
        print(f"  Mean: {utility_df['load_mw'].mean():.2f}")

        print("\nSample data (first 5 rows):")
        print(utility_df.head(5))

        print("\nSample data (last 5 rows):")
        print(utility_df.tail(5))

        write_utility_loads_local(
            utility_df,
            utility_base,
            iso_region,
            utility_name,
        )

    print(f"\n{'=' * 60}")
    print("✓ All utilities processed")
    print(
        f"✓ Output: {utility_base}/region={iso_region}/utility=<UTILITY>/year={year}/month=<M>/data.parquet"
    )
    print("  (run Justfile upload recipe to sync to S3)")
    print(f"{'=' * 60}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Aggregate state zone loads to utility-level profiles (calendar year)"
    )
    parser.add_argument(
        "--state",
        type=str,
        required=True,
        choices=["NY", "RI"],
        help="State to process (supported: NY, RI)",
    )
    parser.add_argument(
        "--utility",
        type=str,
        default="all",
        help="Specific utility short code for the selected state, or 'all' (default: all)",
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Calendar year to process (e.g., 2024). Must have all 12 months available.",
    )
    parser.add_argument(
        "--path-local-zone-parquet",
        dest="path_local_zone_parquet",
        type=str,
        required=True,
        help="Local directory with zone parquet inputs (partitioned)",
    )
    parser.add_argument(
        "--path-local-utility-parquet",
        dest="path_local_utility_parquet",
        type=str,
        required=True,
        help="Local directory for utility parquet output (partitioned; sync to S3 via Justfile upload)",
    )

    args = parser.parse_args()
    load_dotenv()
    config = get_state_config(args.state)
    utility_zone_mapping = get_utility_zone_mapping_for_state(args.state)
    if not utility_zone_mapping:
        parser.error(f"No utility mapping found for state {args.state}")

    selected_utility = args.utility.lower()
    if selected_utility != "all" and selected_utility not in utility_zone_mapping:
        valid = ", ".join(sorted(utility_zone_mapping.keys()))
        parser.error(
            f"Invalid --utility '{args.utility}' for state {args.state}. "
            f"Valid values: all, {valid}"
        )

    zone_base = args.path_local_zone_parquet
    utility_base = args.path_local_utility_parquet

    utility_list = sorted(utility_zone_mapping.keys())
    print("=" * 60)
    print(f"{config.label} UTILITY LOAD AGGREGATION")
    print("=" * 60)
    print(f"State: {config.state}")
    print(f"Calendar year: {args.year}")
    print(f"ISO region partition: {config.iso_region}")
    print(f"Zone input: {zone_base}")
    print(f"Utility output: {utility_base}")
    print(
        "Utilities: "
        f"{selected_utility if selected_utility != 'all' else 'All (' + ', '.join(utility_list) + ')'}"
    )
    print("=" * 60)

    if selected_utility == "all":
        process_all_utilities(
            zone_base,
            utility_base,
            config.iso_region,
            args.year,
            utility_zone_mapping,
        )
    else:
        # Process single utility
        zones = utility_zone_mapping[selected_utility]
        print(f"\nProcessing single utility: {selected_utility}")
        print(f"Zones: {zones}")

        zone_df = load_zone_data(zone_base, config.iso_region, args.year, zones)
        utility_df = aggregate_utility_load(zone_df, selected_utility, zones)

        print(f"\n{'=' * 60}")
        print(f"Aggregated to {len(utility_df):,} hourly records")

        expected_hours = expected_hours_for_year(args.year)

        if len(utility_df) != expected_hours:
            print(f"⚠️  Expected {expected_hours} hours, got {len(utility_df)}")
        else:
            print(f"✓ Hour count matches expected: {expected_hours}")

        print("\nLoad statistics (MW):")
        print(f"  Min:  {utility_df['load_mw'].min():.2f}")
        print(f"  Max:  {utility_df['load_mw'].max():.2f}")
        print(f"  Mean: {utility_df['load_mw'].mean():.2f}")

        print("\nSample data (first 10 rows):")
        print(utility_df.head(10))

        write_utility_loads_local(
            utility_df,
            utility_base,
            config.iso_region,
            selected_utility,
        )


if __name__ == "__main__":
    main()
