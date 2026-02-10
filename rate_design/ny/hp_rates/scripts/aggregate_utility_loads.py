"""Aggregate NYISO zone loads to utility-level 8760 profiles.

This script reads zone load data from S3 (partitioned by zone/year/month),
applies zone-to-utility mapping, and creates utility-level aggregated load profiles.

Operates on complete calendar years only - validates that all 12 months are available.

Input: s3://data.sb/nyiso/loads/zone=X/year=YYYY/month=MM/data.parquet
Output: s3://data.sb/nyiso/loads/utility=X/year=YYYY/data.parquet

Usage:
    # Process single year (inspection only, no upload)
    python aggregate_utility_loads.py --year 2024
    
    # Process and upload to S3
    python aggregate_utility_loads.py --year 2024 --upload
    
    # Process single utility
    python aggregate_utility_loads.py --year 2024 --utility nyseg
"""

import argparse
import io

import polars as pl
from cloudpathlib import S3Path

# Utility to NYISO zones mapping
# Based on NYISO zone definitions and utility service territories
UTILITY_ZONE_MAPPING = {
    "nyseg": ["A", "C", "D", "E", "F", "G", "H"],  #https://www.nyseg.com/w/iso-maps#:~:text=Zones%20A%2C%20B%2C%20C%2C,Lower%20Hudson%20Valley%20pricing%20zone.
    "rge": ["B"],  # https://www.rge.com/w/iso-maps
    "cenhud": ["G"],  # Hudson Valley
    "nimo": ["A", "B", "C", "D", "E", "F"],  # https://www.nationalgridus.com/media/pdfs/billing-payments/electric-rates/upstate-ny/rates_load_zones.pdf
    # ConEd, O&R, NiMo excluded for now (different methodology)
}


def load_zone_data(s3_base: str, year: int, zones: list[str]) -> pl.DataFrame:
    """Load zone load data from S3 for specified zones and year.
    
    Reads from Hive-style partitioned structure: zone=X/year=YYYY/month=MM/data.parquet
    Validates that all 12 months areE present for each zone before loading.
    
    Args:
        s3_base: Base S3 path (e.g., s3://data.sb/nyiso/loads)
        year: Calendar year (must have all 12 months)
        zones: List of zone identifiers (e.g., ["A", "B", "C"])
        
    Returns:
        Combined DataFrame with all zone data
        
    Raises:
        ValueError: If any zone is missing months or if data validation fails
    """
    base_path = S3Path(s3_base)
    
    print("\n" + "="*60)
    print("VALIDATING DATA AVAILABILITY")
    print("="*60)
    
    # First pass: check all months exist for all zones
    missing_data = []
    for zone in zones:
        zone_path = base_path / f"zone={zone}"
        
        if not zone_path.exists():
            missing_data.append(f"Zone {zone}: entire zone path missing")
            continue
        
        year_path = zone_path / f"year={year}"
        if not year_path.exists():
            missing_data.append(f"Zone {zone}: year {year} path missing")
            continue
        
        # Check all 12 months
        for month in range(1, 13):
            month_path = year_path / f"month={month:02d}" / "data.parquet"
            if not month_path.exists():
                missing_data.append(f"Zone {zone}: month {year}-{month:02d} missing")
    
    if missing_data:
        print("\n❌ INCOMPLETE DATA - Missing the following:")
        for item in missing_data:
            print(f"  • {item}")
        raise ValueError(
            f"Cannot proceed with incomplete data. Missing {len(missing_data)} partition(s). "
            f"All 12 months required for calendar year {year}."
        )
    
    print(f"✓ All zones have complete data for year {year} (12 months each)")
    
    # Second pass: load all data
    print("\n" + "="*60)
    print("LOADING DATA")
    print("="*60)
    
    all_zone_dfs = []
    
    for zone in zones:
        print(f"\nLoading zone {zone}...")
        zone_dfs = []
        
        zone_path = base_path / f"zone={zone}"
        year_path = zone_path / f"year={year}"
        
        # Load all 12 months
        for month in range(1, 13):
            month_path = year_path / f"month={month:02d}" / "data.parquet"
            
            # Read from S3 using BytesIO buffer
            parquet_bytes = month_path.read_bytes()
            month_df = pl.read_parquet(io.BytesIO(parquet_bytes))
            zone_dfs.append(month_df)
            print(f"  ✓ Loaded {year}-{month:02d}: {len(month_df):,} rows")
        
        zone_combined = pl.concat(zone_dfs)
        all_zone_dfs.append(zone_combined)
        print(f"  ✓ Total for zone {zone}: {len(zone_combined):,} rows")
    
    combined = pl.concat(all_zone_dfs)
    return combined


def aggregate_utility_load(
    zone_df: pl.DataFrame, 
    utility_name: str, 
    zones: list[str]
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
        utility_data
        .group_by("timestamp")
        .agg([
            pl.col("load_mw").sum().alias("load_mw"),
        ])
        .with_columns([
            pl.lit(utility_name).alias("utility"),
        ])
        .select(["timestamp", "utility", "load_mw"])
        .sort("timestamp")
    )
    
    return aggregated


def process_all_utilities(s3_base: str, year: int, upload_to_s3: bool = False):
    """Process all utilities and create aggregated load profiles for a calendar year.
    
    Args:
        s3_base: Base S3 path (e.g., s3://data.sb/nyiso/loads)
        year: Calendar year to process
        upload_to_s3: If True, upload results to S3. Default False for inspection.
    """
    # Collect all unique zones needed
    all_zones = set()
    for zones in UTILITY_ZONE_MAPPING.values():
        all_zones.update(zones)
    
    print(f"\nZones needed: {sorted(all_zones)}")
    print(f"Calendar year: {year}")
    
    # Load all zone data once (validates all 12 months present)
    zone_df = load_zone_data(s3_base, year, sorted(all_zones))
    
    print(f"\n{'='*60}")
    print(f"Total zone data loaded: {len(zone_df):,} rows")
    print(f"Date range: {zone_df['timestamp'].min()} to {zone_df['timestamp'].max()}")
    print(f"{'='*60}")
    
    # Calculate expected hours for this year (accounting for DST)
    is_leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    expected_hours = 8784 if is_leap else 8760
    # Subtract 1 for DST spring forward (2 AM doesn't exist)
    expected_hours -= 1
    
    # Process each utility
    for utility_name, zones in UTILITY_ZONE_MAPPING.items():
        print(f"\n{'='*60}")
        print(f"UTILITY: {utility_name}")
        print(f"{'='*60}")
        print(f"Zones: {zones}")
        
        # Aggregate utility load
        utility_df = aggregate_utility_load(zone_df, utility_name, zones)
        print(f"\nAggregated to {len(utility_df):,} hourly records")
        
        if len(utility_df) != expected_hours:
            print(f"⚠️  Expected {expected_hours} hours, got {len(utility_df)}")
        else:
            print(f"✓ Hour count matches expected: {expected_hours}")
        
        # Show data summary
        print(f"\nLoad statistics (MW):")
        print(f"  Min:  {utility_df['load_mw'].min():.2f}")
        print(f"  Max:  {utility_df['load_mw'].max():.2f}")
        print(f"  Mean: {utility_df['load_mw'].mean():.2f}")
        
        print(f"\nSample data (first 5 rows):")
        print(utility_df.head(5))
        
        print(f"\nSample data (last 5 rows):")
        print(utility_df.tail(5))
        
        if upload_to_s3:
            # Write to buffer
            buf = io.BytesIO()
            utility_df.write_parquet(buf)
            
            # Upload to S3 with Hive-style partitioning: utility=X/year=YYYY/data.parquet
            base_path = S3Path(s3_base)
            output_path = base_path / f"utility={utility_name}" / f"year={year}" / "data.parquet"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(buf.getvalue())
            
            print(f"\n✓ Uploaded to {output_path}")
        else:
            print(f"\n⚠️  S3 upload disabled (use --upload to enable)")
    
    if upload_to_s3:
        print(f"\n{'='*60}")
        print(f"✓ All utilities processed and uploaded")
        print(f"✓ Output structure: {s3_base}/utility=<UTILITY>/year={year}/data.parquet")
        print(f"{'='*60}")
    else:
        print(f"\n{'='*60}")
        print(f"✓ All utilities processed (data inspection complete)")
        print(f"⚠️  No data uploaded to S3 (use --upload flag to enable)")
        print(f"{'='*60}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Aggregate NYISO zone loads to utility-level profiles (calendar year)"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Calendar year to process (e.g., 2024). Must have all 12 months available.",
    )
    parser.add_argument(
        "--s3-base",
        type=str,
        default="s3://data.sb/nyiso/loads",
        help="Base S3 path (default: s3://data.sb/nyiso/loads)",
    )
    parser.add_argument(
        "--utility",
        type=str,
        choices=list(UTILITY_ZONE_MAPPING.keys()) + ["all"],
        default="all",
        help="Specific utility to process (lowercase short code: nyseg, rge, cenhud, nationalgrid), or 'all' for all utilities (default: all)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload results to S3 (default: False, for data inspection only)",
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("NYISO UTILITY LOAD AGGREGATION")
    print("="*60)
    print(f"Calendar year: {args.year}")
    print(f"S3 base path: {args.s3_base}")
    print(f"Upload to S3: {'Yes' if args.upload else 'No (inspection only)'}")
    print(f"Utilities: {args.utility if args.utility != 'all' else 'All (' + ', '.join(UTILITY_ZONE_MAPPING.keys()) + ')'}")
    print("="*60)
    
    if args.utility == "all":
        process_all_utilities(args.s3_base, args.year, args.upload)
    else:
        # Process single utility
        zones = UTILITY_ZONE_MAPPING[args.utility]
        print(f"\nProcessing single utility: {args.utility}")
        print(f"Zones: {zones}")
        
        zone_df = load_zone_data(args.s3_base, args.year, zones)
        utility_df = aggregate_utility_load(zone_df, args.utility, zones)
        
        print(f"\n{'='*60}")
        print(f"Aggregated to {len(utility_df):,} hourly records")
        
        # Calculate expected hours
        is_leap = args.year % 4 == 0 and (args.year % 100 != 0 or args.year % 400 == 0)
        expected_hours = (8784 if is_leap else 8760) - 1  # -1 for DST spring forward
        
        if len(utility_df) != expected_hours:
            print(f"⚠️  Expected {expected_hours} hours, got {len(utility_df)}")
        else:
            print(f"✓ Hour count matches expected: {expected_hours}")
        
        print(f"\nLoad statistics (MW):")
        print(f"  Min:  {utility_df['load_mw'].min():.2f}")
        print(f"  Max:  {utility_df['load_mw'].max():.2f}")
        print(f"  Mean: {utility_df['load_mw'].mean():.2f}")
        
        print(f"\nSample data (first 10 rows):")
        print(utility_df.head(10))
        
        if args.upload:
            # Write to buffer
            buf = io.BytesIO()
            utility_df.write_parquet(buf)
            
            # Upload to S3 with Hive-style partitioning: utility=X/year=YYYY/data.parquet
            base_path = S3Path(args.s3_base)
            output_path = base_path / f"utility={args.utility}" / f"year={args.year}" / "data.parquet"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(buf.getvalue())
            
            print(f"\n✓ Uploaded {args.utility} to {output_path}")
        else:
            print(f"\n⚠️  S3 upload disabled (use --upload to enable)")


if __name__ == "__main__":
    main()
