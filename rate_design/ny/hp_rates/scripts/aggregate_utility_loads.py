"""Aggregate NYISO zone loads to utility-level 8760 profiles.

This script reads zone load data from S3 (partitioned by zone/year/month),
applies zone-to-utility mapping, and creates utility-level aggregated load profiles.

Input: s3://data.sb/nyiso/loads/zone=X/year=YYYY/month=MM/data.parquet
Output: s3://data.sb/nyiso/loads/year=YYYY/utility/{utility_name}.parquet

Usage:
    # Process single year
    python aggregate_utility_loads.py --start-year 2024 --end-year 2024
    
    # Process multiple years
    python aggregate_utility_loads.py --start-year 2024 --end-year 2025
    
    # Process single utility
    python aggregate_utility_loads.py --start-year 2024 --end-year 2024 --utility NYSEG
"""

import argparse
import io

import polars as pl
from cloudpathlib import S3Path

# Utility to NYISO zones mapping
# Based on NYISO zone definitions and utility service territories
UTILITY_ZONE_MAPPING = {
    "NYSEG": ["A", "C", "D", "E", "F", "G", "H"],  #https://www.nyseg.com/w/iso-maps#:~:text=Zones%20A%2C%20B%2C%20C%2C,Lower%20Hudson%20Valley%20pricing%20zone.
    "RG&E": ["B"],  # https://www.rge.com/w/iso-maps
    "Central Hudson": ["G"],  # Hudson Valley
    "National Grid": ["A", "B", "C", "D", "E", "F"],  # https://www.nationalgridus.com/media/pdfs/billing-payments/electric-rates/upstate-ny/rates_load_zones.pdf
    # ConEd, O&R, NiMo excluded for now (different methodology)
}


def load_zone_data(s3_base: str, start_year: int, end_year: int, zones: list[str]) -> pl.DataFrame:
    """Load zone load data from S3 for specified zones and year range.
    
    Reads from Hive-style partitioned structure: zone=X/year=YYYY/month=MM/data.parquet
    
    Args:
        s3_base: Base S3 path (e.g., s3://data.sb/nyiso/loads)
        start_year: Start year (inclusive)
        end_year: End year (inclusive)
        zones: List of zone identifiers (e.g., ["A", "B", "C"])
        
    Returns:
        Combined DataFrame with all zone data
    """
    base_path = S3Path(s3_base)
    
    all_zone_dfs = []
    
    for zone in zones:
        zone_path = base_path / f"zone={zone}"
        
        if not zone_path.exists():
            print(f"⚠️  Warning: Zone path {zone_path} not found, skipping zone {zone}")
            continue
        
        print(f"\nLoading zone {zone}...")
        zone_dfs = []
        
        # Load all years for this zone
        for year in range(start_year, end_year + 1):
            year_path = zone_path / f"year={year}"
            
            if not year_path.exists():
                print(f"  ⚠️  Year {year} not found for zone {zone}, skipping")
                continue
            
            # Load all months for this year
            for month in range(1, 13):
                month_path = year_path / f"month={month:02d}" / "data.parquet"
                
                if not month_path.exists():
                    print(f"  ⚠️  Month {year}-{month:02d} not found for zone {zone}, skipping")
                    continue
                
                # Read from S3 using BytesIO buffer
                parquet_bytes = month_path.read_bytes()
                month_df = pl.read_parquet(io.BytesIO(parquet_bytes))
                zone_dfs.append(month_df)
                print(f"  ✓ Loaded {year}-{month:02d}: {len(month_df):,} rows")
        
        if zone_dfs:
            zone_combined = pl.concat(zone_dfs)
            all_zone_dfs.append(zone_combined)
            print(f"  ✓ Total for zone {zone}: {len(zone_combined):,} rows")
        else:
            print(f"  ⚠️  No data found for zone {zone}")
    
    if not all_zone_dfs:
        raise ValueError(f"No zone data found for zones {zones} in years {start_year}-{end_year}")
    
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


def process_all_utilities(s3_base: str, start_year: int, end_year: int, upload_to_s3: bool = False):
    """Process all utilities and create aggregated load profiles.
    
    Args:
        s3_base: Base S3 path (e.g., s3://data.sb/nyiso/loads)
        start_year: Start year (inclusive)
        end_year: End year (inclusive)
        upload_to_s3: If True, upload results to S3. Default False for inspection.
    """
    # Collect all unique zones needed
    all_zones = set()
    for zones in UTILITY_ZONE_MAPPING.values():
        all_zones.update(zones)
    
    print(f"Loading data for zones: {sorted(all_zones)}")
    print(f"Year range: {start_year} to {end_year}")
    
    # Load all zone data once
    zone_df = load_zone_data(s3_base, start_year, end_year, sorted(all_zones))
    print(f"\n{'='*60}")
    print(f"Total zone data loaded: {len(zone_df):,} rows")
    print(f"Date range: {zone_df['timestamp'].min()} to {zone_df['timestamp'].max()}")
    print(f"{'='*60}")
    
    # Process each utility
    for utility_name, zones in UTILITY_ZONE_MAPPING.items():
        print(f"\n{'='*60}")
        print(f"Processing utility: {utility_name}")
        print(f"{'='*60}")
        print(f"Zones: {zones}")
        
        # Aggregate utility load
        utility_df = aggregate_utility_load(zone_df, utility_name, zones)
        print(f"\nAggregated to {len(utility_df):,} hourly records")
        
        # Calculate expected hours accounting for DST
        total_expected = 0
        for year in range(start_year, end_year + 1):
            # Leap year check
            is_leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
            # Account for DST: spring forward (-1 hour), fall back (+1 hour) = net 0 change
            # But in 2024 we're missing the spring forward 2AM hour, so: 8784 - 1 = 8783 for leap years
            year_hours = 8784 if is_leap else 8760
            # Subtract 1 for each DST spring forward (2 AM doesn't exist)
            year_hours -= 1  # One spring forward per year
            total_expected += year_hours
        
        if len(utility_df) != total_expected:
            print(f"⚠️  Expected ~{total_expected} hours, got {len(utility_df)} (difference may be due to DST)")
        else:
            print(f"✓ Hour count matches expected: {total_expected}")
        
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
            
            # Upload to S3
            base_path = S3Path(s3_base)
            utility_path = base_path / "utility" / f"year={start_year}-{end_year}"
            output_path = utility_path / f"{utility_name}.parquet"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(buf.getvalue())
            
            print(f"\n✓ Uploaded to {output_path}")
        else:
            print(f"\n⚠️  S3 upload disabled (use --upload to enable)")
    
    if upload_to_s3:
        print(f"\n{'='*60}")
        print(f"✓ All utilities processed and uploaded")
        print(f"{'='*60}")
    else:
        print(f"\n{'='*60}")
        print(f"✓ All utilities processed (data inspection complete)")
        print(f"⚠️  No data uploaded to S3 (use --upload flag to enable)")
        print(f"{'='*60}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Aggregate NYISO zone loads to utility-level profiles"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        required=True,
        help="Start year for data aggregation (e.g., 2024)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        required=True,
        help="End year for data aggregation (e.g., 2025)",
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
        help="Specific utility to process, or 'all' for all utilities (default: all)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload results to S3 (default: False, for data inspection only)",
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("NYISO Utility Load Aggregation")
    print("="*60)
    print(f"Year range: {args.start_year} to {args.end_year}")
    print(f"S3 base path: {args.s3_base}")
    print(f"Upload to S3: {'Yes' if args.upload else 'No (inspection only)'}")
    print("="*60)
    
    if args.utility == "all":
        process_all_utilities(args.s3_base, args.start_year, args.end_year, args.upload)
    else:
        # Process single utility
        zones = UTILITY_ZONE_MAPPING[args.utility]
        print(f"\nProcessing single utility: {args.utility}")
        print(f"Zones: {zones}")
        
        zone_df = load_zone_data(args.s3_base, args.start_year, args.end_year, zones)
        utility_df = aggregate_utility_load(zone_df, args.utility, zones)
        
        print(f"\nAggregated to {len(utility_df):,} hourly records")
        print(f"\nSample data:")
        print(utility_df.head(10))
        
        if args.upload:
            base_path = S3Path(args.s3_base)
            utility_path = base_path / "utility" / f"year={args.start_year}-{args.end_year}"
            output_path = utility_path / f"{args.utility}.parquet"
            
            buf = io.BytesIO()
            utility_df.write_parquet(buf)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(buf.getvalue())
            
            print(f"\n✓ Uploaded {args.utility} to {output_path}")
        else:
            print(f"\n⚠️  S3 upload disabled (use --upload to enable)")


if __name__ == "__main__":
    main()
