"""Aggregate state-level zone loads to utility-level profiles.

This script reads zone load data from S3 (partitioned by region/zone/year/month),
applies zone-to-utility mapping, and creates utility-level aggregated load profiles.

Operates on complete calendar years only - validates that all 12 months are available.

Input:
    - s3://data.sb/eia/hourly_demand/zones/region=<iso_region>/zone=X/year=YYYY/month=M/data.parquet
Output:
    - s3://data.sb/eia/hourly_demand/utilities/region=<iso_region>/utility=X/year=YYYY/month=M/data.parquet

Usage:
    # Process single year (inspection only, no upload)
    python aggregate_eia_utility_loads.py --state NY --year 2024

    # Process and upload to S3
    python aggregate_eia_utility_loads.py --state RI --year 2024 --upload

    # Process single utility
    python aggregate_eia_utility_loads.py --state NY --year 2024 --utility nyseg
"""

import argparse

import polars as pl
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import (
    get_aws_storage_options,
    get_state_config,
    get_utility_zone_mapping_for_state,
)


def load_zone_data(
    s3_base: str,
    iso_region: str,
    year: int,
    zones: list[str],
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load zone load data from S3 for specified zones and year.

    Reads from Hive-style partitioned structure:
    region=<iso_region>/zone=X/year=YYYY/month=M/data.parquet
    Validates that all 12 months are present for each zone before loading.

    Args:
        s3_base: Base S3 path (e.g., s3://data.sb/eia/hourly_demand/zones)
        iso_region: ISO region partition key (e.g., nyiso, isone)
        year: Calendar year (must have all 12 months)
        zones: List of zone identifiers (e.g., ["A", "B", "C"])
        storage_options: Polars S3 storage options with AWS bucket region

    Returns:
        Combined DataFrame with all zone data

    Raises:
        ValueError: If any zone is missing months or if data validation fails
    """
    print("\n" + "=" * 60)
    print("LOADING DATA")
    print("=" * 60)

    collected = (
        pl.scan_parquet(
            s3_base,
            storage_options=storage_options,
        )
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
            "Fetch zone data first, for example:\n"
            "  uv run python data/eia/hourly_loads/fetch_eia_zone_loads.py "
            "--state <NY|RI> "
            f"--start-month {year}-01 --end-month {year}-12 "
            f"--s3-base {s3_base}"
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
            f"All 12 months required for calendar year {year}. "
            "Re-run zone fetch to backfill missing partitions:\n"
            "  uv run python data/eia/hourly_loads/fetch_eia_zone_loads.py "
            "--state <NY|RI> "
            f"--start-month {year}-01 --end-month {year}-12 "
            f"--s3-base {s3_base}"
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


def upload_utility_loads_to_s3(
    utility_df: pl.DataFrame,
    utility_s3_base: str,
    iso_region: str,
    utility_name: str,
    storage_options: dict[str, str],
) -> None:
    """Add partition columns and write utility load parquet to S3."""
    output_df = utility_df.with_columns(
        [
            pl.lit(iso_region).alias("region"),
            pl.col("timestamp").dt.year().alias("year"),
            pl.col("timestamp").dt.month().alias("month"),
        ]
    )
    output_df.write_parquet(
        utility_s3_base,
        compression="zstd",
        partition_by=["region", "utility", "year", "month"],
        storage_options=storage_options,
    )
    print(
        "\n✓ Uploaded utility partitioned data under "
        f"{utility_s3_base}/region={iso_region}/utility={utility_name}/"
    )


def process_all_utilities(
    zone_s3_base: str,
    utility_s3_base: str,
    iso_region: str,
    year: int,
    utility_zone_mapping: dict[str, list[str]],
    storage_options: dict[str, str],
    upload_to_s3: bool = False,
):
    """Process all utilities and create aggregated load profiles for a calendar year.

    Args:
        zone_s3_base: Base S3 path for zonal loads
        utility_s3_base: Base S3 path for utility outputs
        iso_region: ISO region partition key (nyiso/isone)
        year: Calendar year to process
        utility_zone_mapping: Utility to zones mapping for selected state
        storage_options: Polars S3 storage options with AWS bucket region
        upload_to_s3: If True, upload results to S3. Default False for inspection.
    """
    # Collect all unique zones needed
    all_zones = set()
    for zones in utility_zone_mapping.values():
        all_zones.update(zones)

    print(f"\nZones needed: {sorted(all_zones)}")
    print(f"Calendar year: {year}")

    # Load all zone data once (validates all 12 months present)
    zone_df = load_zone_data(
        zone_s3_base,
        iso_region,
        year,
        sorted(all_zones),
        storage_options,
    )

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

        if upload_to_s3:
            upload_utility_loads_to_s3(
                utility_df,
                utility_s3_base,
                iso_region,
                utility_name,
                storage_options,
            )
        else:
            print("\n⚠️  S3 upload disabled (use --upload to enable)")

    if upload_to_s3:
        print(f"\n{'=' * 60}")
        print("✓ All utilities processed and uploaded")
        print(
            "✓ Output structure: "
            f"{utility_s3_base}/region={iso_region}/utility=<UTILITY>/year={year}/month=<M>/data.parquet"
        )
        print(f"{'=' * 60}")
    else:
        print(f"\n{'=' * 60}")
        print("✓ All utilities processed (data inspection complete)")
        print("⚠️  No data uploaded to S3 (use --upload flag to enable)")
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
        "--zone-s3-base",
        type=str,
        required=True,
        help="Base S3 path for zonal load inputs (e.g., s3://data.sb/eia/hourly_demand/zones/)",
    )
    parser.add_argument(
        "--utility-s3-base",
        type=str,
        required=True,
        help="Base S3 path for utility load outputs (e.g., s3://data.sb/eia/hourly_demand/utilities/)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload results to S3 (default: False, for data inspection only)",
    )

    args = parser.parse_args()
    load_dotenv()
    config = get_state_config(args.state)
    utility_zone_mapping = get_utility_zone_mapping_for_state(args.state)
    storage_options = get_aws_storage_options()
    if not utility_zone_mapping:
        parser.error(f"No utility mapping found for state {args.state}")

    selected_utility = args.utility.lower()
    if selected_utility != "all" and selected_utility not in utility_zone_mapping:
        valid = ", ".join(sorted(utility_zone_mapping.keys()))
        parser.error(
            f"Invalid --utility '{args.utility}' for state {args.state}. "
            f"Valid values: all, {valid}"
        )

    resolved_zone_s3_base = args.zone_s3_base
    resolved_utility_s3_base = args.utility_s3_base

    utility_list = sorted(utility_zone_mapping.keys())
    print("=" * 60)
    print(f"{config.label} UTILITY LOAD AGGREGATION")
    print("=" * 60)
    print(f"State: {config.state}")
    print(f"Calendar year: {args.year}")
    print(f"ISO region partition: {config.iso_region}")
    print(f"AWS bucket region: {storage_options.get('region')}")
    print(f"Zone base path: {resolved_zone_s3_base}")
    print(f"Utility output base path: {resolved_utility_s3_base}")
    print(f"Upload to S3: {'Yes' if args.upload else 'No (inspection only)'}")
    print(
        "Utilities: "
        f"{selected_utility if selected_utility != 'all' else 'All (' + ', '.join(utility_list) + ')'}"
    )
    print("=" * 60)

    if selected_utility == "all":
        process_all_utilities(
            resolved_zone_s3_base,
            resolved_utility_s3_base,
            config.iso_region,
            args.year,
            utility_zone_mapping,
            storage_options,
            args.upload,
        )
    else:
        # Process single utility
        zones = utility_zone_mapping[selected_utility]
        print(f"\nProcessing single utility: {selected_utility}")
        print(f"Zones: {zones}")

        zone_df = load_zone_data(
            resolved_zone_s3_base,
            config.iso_region,
            args.year,
            zones,
            storage_options,
        )
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

        if args.upload:
            upload_utility_loads_to_s3(
                utility_df,
                resolved_utility_s3_base,
                config.iso_region,
                selected_utility,
                storage_options,
            )
        else:
            print("\n⚠️  S3 upload disabled (use --upload to enable)")


if __name__ == "__main__":
    main()
