"""Aggregate NYISO zone loads to utility-level 8760 profiles.

This script reads zone load data from S3, applies zone-to-utility mapping,
and creates utility-level aggregated load profiles.

Input: s3://data.sb/nyiso/loads/year=YYYY/zone/zone_X.parquet
Output: s3://data.sb/nyiso/loads/year=YYYY/utility/{utility_name}.parquet

Usage:
    python aggregate_utility_loads.py --year 2024 --s3-base s3://data.sb/nyiso/loads
"""

import argparse
import io

import polars as pl
from cloudpathlib import S3Path

# Utility to NYISO zones mapping
# Based on NYISO zone definitions and utility service territories
UTILITY_ZONE_MAPPING = {
    "NYSEG": ["A", "C", "D", "E", "F"],  # West, Central, North, Mohawk Valley, Capital
    "RG&E": ["A", "B"],  # West, Genesee
    "Central Hudson": ["G"],  # Hudson Valley
    "National Grid": ["A", "B", "C", "D", "E", "F"],  # Multiple zones
    # ConEd, O&R, NiMo excluded for now (different methodology)
}


def load_zone_data(s3_base: str, year: int, zones: list[str]) -> pl.DataFrame:
    """Load zone load data from S3 for specified zones.
    
    Args:
        s3_base: Base S3 path (e.g., s3://data.sb/nyiso/loads)
        year: Target year
        zones: List of zone identifiers (e.g., ["A", "B", "C"])
        
    Returns:
        Combined DataFrame with all zone data
    """
    base_path = S3Path(s3_base)
    zone_path = base_path / f"year={year}" / "zone"
    
    zone_dfs = []
    for zone in zones:
        zone_file = zone_path / f"zone_{zone}.parquet"
        
        if not zone_file.exists():
            print(f"Warning: Zone file {zone_file} not found, skipping zone {zone}")
            continue
        
        # Read from S3
        zone_bytes = zone_file.read_bytes()
        zone_df = pl.read_parquet(io.BytesIO(zone_bytes))
        zone_dfs.append(zone_df)
        print(f"Loaded {len(zone_df):,} rows for zone {zone}")
    
    if not zone_dfs:
        raise ValueError(f"No zone data found for zones {zones}")
    
    combined = pl.concat(zone_dfs)
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


def process_all_utilities(s3_base: str, year: int):
    """Process all utilities and create aggregated load profiles.
    
    Args:
        s3_base: Base S3 path (e.g., s3://data.sb/nyiso/loads)
        year: Target year
    """
    # Collect all unique zones needed
    all_zones = set()
    for zones in UTILITY_ZONE_MAPPING.values():
        all_zones.update(zones)
    
    print(f"Loading data for zones: {sorted(all_zones)}")
    
    # Load all zone data once
    zone_df = load_zone_data(s3_base, year, sorted(all_zones))
    print(f"Total zone data loaded: {len(zone_df):,} rows")
    
    # Process each utility
    base_path = S3Path(s3_base)
    utility_path = base_path / f"year={year}" / "utility"
    
    for utility_name, zones in UTILITY_ZONE_MAPPING.items():
        print(f"\nProcessing utility: {utility_name}")
        print(f"  Zones: {zones}")
        
        # Aggregate utility load
        utility_df = aggregate_utility_load(zone_df, utility_name, zones)
        print(f"  Aggregated to {len(utility_df):,} hourly records")
        
        # Validate we have 8760 hours (or 8784 for leap year)
        expected_hours = 8784 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 8760
        if len(utility_df) != expected_hours:
            print(f"  WARNING: Expected {expected_hours} hours, got {len(utility_df)}")
        
        # Write to buffer
        buf = io.BytesIO()
        utility_df.write_parquet(buf)
        
        # Upload to S3
        output_path = utility_path / f"{utility_name}.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(buf.getvalue())
        
        print(f"  ✓ Uploaded to {output_path}")
    
    print(f"\n✓ All utilities processed and uploaded to {utility_path}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Aggregate NYISO zone loads to utility-level profiles"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Target year for data aggregation (default: 2024)",
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
    
    args = parser.parse_args()
    
    print(f"Starting utility load aggregation for year {args.year}")
    print(f"S3 base path: {args.s3_base}")
    
    if args.utility == "all":
        process_all_utilities(args.s3_base, args.year)
    else:
        # Process single utility
        zones = UTILITY_ZONE_MAPPING[args.utility]
        print(f"Processing single utility: {args.utility}")
        print(f"Zones: {zones}")
        
        zone_df = load_zone_data(args.s3_base, args.year, zones)
        utility_df = aggregate_utility_load(zone_df, args.utility, zones)
        
        base_path = S3Path(args.s3_base)
        utility_path = base_path / f"year={args.year}" / "utility"
        output_path = utility_path / f"{args.utility}.parquet"
        
        buf = io.BytesIO()
        utility_df.write_parquet(buf)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(buf.getvalue())
        
        print(f"✓ Uploaded {args.utility} to {output_path}")


if __name__ == "__main__":
    main()
