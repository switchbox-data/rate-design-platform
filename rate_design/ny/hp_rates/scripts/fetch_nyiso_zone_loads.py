"""Fetch NYISO zone load data from EIA API v2 and upload to S3.

This script downloads hourly load data for NYISO zones A-K using the EIA API v2.
You must register for a free API key at https://www.eia.gov/opendata/

Setup:
    1. Register at https://www.eia.gov/opendata/
    2. Create .env file in project root: EIA_API_KEY=your_key_here

Output structure: s3://data.sb/nyiso/loads/zone=X/year=YYYY/month=MM/data.parquet
Schema: timestamp (timezone-aware, America/New_York), zone, load_mw, filled (bool)

Timezone handling:
    - EIA API returns UTC timestamps (frequency='hourly')
    - Script converts UTC to Eastern Time (America/New_York)
    - Output timestamps are timezone-aware and handle EST/EDT transitions
    - Spring forward: 23 hours, Fall back: 25 hours

Usage:
    # Fetch full year (API key from .env)
    python fetch_nyiso_zone_loads.py --start-month 2024-01 --end-month 2024-12
    
    # Fetch specific months
    python fetch_nyiso_zone_loads.py --start-month 2024-06 --end-month 2024-08
    
    # Override API key
    python fetch_nyiso_zone_loads.py --start-month 2024-01 --end-month 2024-12 --eia-api-key YOUR_KEY
    
Note:
    Script automatically expands month ranges to full month dates (first to last day)
    and skips months that already exist in S3.
"""

import argparse
import calendar
import getpass
import io
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import requests
from cloudpathlib import S3Path
from dotenv import load_dotenv

# EIA API Configuration
EIA_API_BASE = "https://api.eia.gov/v2/"
EIA_NYISO_ENDPOINT = "electricity/rto/region-sub-ba-data/data/"

# NYISO zone definitions
NYISO_ZONES = [
    "A",  # West
    "B",  # Genesee
    "C",  # Central
    "D",  # North
    "E",  # Mohawk Valley
    "F",  # Capital
    "G",  # Hudson Valley
    "H",  # Millwood
    "I",  # Dunwoodie
    "J",  # NYC
    "K",  # Long Island
]

# EIA API zone mapping (EIA uses ZONA, ZONB, etc.)
EIA_ZONE_MAPPING = {
    "ZONA": "A",    # West
    "ZONB": "B",    # Genesee
    "ZONC": "C",    # Central
    "ZOND": "D",    # North
    "ZONE": "E",    # Mohawk Valley
    "ZONF": "F",    # Capital
    "ZONG": "G",    # Hudson Valley
    "ZONH": "H",    # Millwood
    "ZONI": "I",    # Dunwoodie
    "ZONJ": "J",    # NYC
    "ZONK": "K",    # Long Island
}


def find_project_root() -> Path:
    """Find the project root directory (contains .git or pyproject.toml).
    
    Returns:
        Path to project root
    """
    # Start from current file and search upwards
    current = Path(__file__).resolve()
    
    for parent in [current] + list(current.parents):
        # Check for .git directory or pyproject.toml
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
            return parent
    
    # Fallback: assume we're in rate_design/ny/hp_rates/scripts/
    # and go up to project root
    return Path(__file__).resolve().parent.parent.parent.parent.parent


def load_api_key(cli_key: str | None = None, interactive: bool = True) -> str:
    """Load EIA API key from .env file, CLI argument, or interactive prompt.
    
    Priority:
    1. CLI argument (--eia-api-key)
    2. .env file (EIA_API_KEY)
    3. Interactive prompt (if interactive=True)
    
    Args:
        cli_key: API key from command line argument
        interactive: Allow interactive prompt if key not found
        
    Returns:
        API key string
        
    Raises:
        ValueError: If no API key found and not interactive
    """
    # 1. Check CLI argument first
    if cli_key:
        return cli_key
    
    # 2. Try loading from .env file at project root
    project_root = find_project_root()
    env_path = project_root / ".env"
    
    # Load .env file if it exists
    load_dotenv(dotenv_path=env_path)
    api_key = os.getenv("EIA_API_KEY")
    
    if api_key:
        return api_key
    
    # 3. Interactive prompt (if enabled)
    if interactive:
        print("\n" + "="*60)
        print("EIA API Key Not Found")
        print("="*60)
        print("To get a free API key:")
        print("  1. Visit: https://www.eia.gov/opendata/")
        print("  2. Register for an API key")
        print("  3. Check your email for the key")
        print("\nYou can also save it to .env file:")
        print(f"  echo 'EIA_API_KEY=your_key_here' > {env_path}")
        print("="*60)
        
        api_key = getpass.getpass("\nEnter your EIA API key (input hidden): ").strip()
        
        if api_key:
            # Ask if user wants to save it
            save = input("Save this key to .env file? [y/N]: ").strip().lower()
            if save in ['y', 'yes']:
                env_path.parent.mkdir(parents=True, exist_ok=True)
                with open(env_path, 'w') as f:
                    f.write(f"EIA_API_KEY={api_key}\n")
                print(f"✓ Saved API key to {env_path}")
            return api_key
    
    # 4. If we get here, no key was found
    raise ValueError(
        "EIA API key not found. Either:\n"
        "  1. Add to .env file: EIA_API_KEY=your_key\n"
        "  2. Use --eia-api-key argument\n"
        "  3. Run interactively to be prompted\n"
        "Register at https://www.eia.gov/opendata/"
    )


def fetch_all_zones_from_eia(
    api_key: str,
    start_date: str,     # e.g., "2024-01-01"
    end_date: str        # e.g., "2024-12-31"
) -> list[dict]:
    """Fetch hourly data for ALL NYISO zones from EIA API with pagination.
    
    More efficient than fetching zones individually - gets all 11 zones in one query.
    
    Note:
        Uses 'hourly' frequency which returns UTC timestamps. These are converted
        to Eastern Time in the transform function.
    
    Args:
        api_key: EIA API key
        start_date: Start date in Eastern Time (YYYY-MM-DD format)
        end_date: End date in Eastern Time (YYYY-MM-DD format)
        
    Returns:
        List of dictionaries with EIA API response data (all zones combined)
        
    Raises:
        requests.HTTPError: For API errors
        ValueError: For invalid responses
    """
    from zoneinfo import ZoneInfo
    
    url = f"{EIA_API_BASE}{EIA_NYISO_ENDPOINT}"
    
    # EIA API returns midnight-to-midnight UTC data
    # We need to request UTC days that fully encompass our ET range, then filter after
    # 
    # For Jan 2024 in ET:
    # - Jan 1 00:00 EST = Jan 1 05:00 UTC
    # - Jan 31 23:00 EST = Feb 1 04:00 UTC
    #
    # To capture all needed hours, request:
    # - Start: Midnight of the UTC day containing our ET start (Jan 1 00:00 UTC)
    # - End: Through the full UTC day containing our ET end (Feb 2 23:00 UTC)
    
    start_dt_et = datetime.strptime(start_date, "%Y-%m-%d").replace(
        hour=0, minute=0, second=0, tzinfo=ZoneInfo("America/New_York")
    )
    end_dt_et = datetime.strptime(end_date, "%Y-%m-%d").replace(
        hour=23, minute=0, second=0, tzinfo=ZoneInfo("America/New_York")
    )
    
    # Convert to UTC
    start_dt_utc = start_dt_et.astimezone(ZoneInfo("UTC"))
    end_dt_utc = end_dt_et.astimezone(ZoneInfo("UTC"))
    
    # Request full UTC days: start at midnight, end at midnight of day AFTER end_utc
    start_api = start_dt_utc.strftime("%Y-%m-%dT00")
    
    # Add 1 day to end_utc to ensure we get all hours
    end_utc_plus_one = end_dt_utc + timedelta(days=1)
    end_api = end_utc_plus_one.strftime("%Y-%m-%dT23")
    
    print(f"  API query (UTC): start={start_api}, end={end_api}")
    print(f"  Target ET range: {start_dt_et.strftime('%Y-%m-%d %H:%M')} to {end_dt_et.strftime('%Y-%m-%d %H:%M')}")
    
    all_data = []
    offset = 0
    
    while True:
        params = {
            'api_key': api_key,
            'frequency': 'hourly',  # Returns UTC timestamps
            'data[]': 'value',
            'facets[parent][]': 'NYIS',
            # Note: Omitting 'subba' facet fetches ALL zones at once (more efficient)
            'start': start_api,
            'end': end_api,
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'offset': offset,
            'length': 5000
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            if not response.ok:
                print(f"API Error Response: {response.text}")
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise ValueError("Invalid EIA API key. Register at https://www.eia.gov/opendata/")
            elif e.response.status_code == 429:
                raise ValueError("EIA API rate limit exceeded. Wait and retry.")
            else:
                raise
        except requests.exceptions.Timeout:
            raise ValueError(f"Request timeout for NYISO zones")
        
        data = response.json()
        
        if 'response' not in data or 'data' not in data['response']:
            raise ValueError(f"Unexpected API response format")
        
        rows = data['response']['data']
        
        if not rows:
            break
        
        # Debug: Print first few rows from first API call to verify format
        if offset == 0 and len(rows) > 0:
            print(f"  First 3 raw API rows:")
            for row in rows[:3]:
                print(f"    period={row.get('period')}, subba={row.get('subba')}, value={row.get('value')}")
        
        all_data.extend(rows)
        
        # Check if we got all data
        total = int(data['response'].get('total', 0))
        print(f"  Fetched {len(all_data):,} / {total:,} rows...")
        
        if len(all_data) >= total:
            break
        
        offset += 5000
        time.sleep(0.1)  # Rate limiting courtesy
    
    return all_data


def transform_eia_data(eia_data: list[dict]) -> pl.DataFrame:
    """Transform EIA API data to required schema with timezone conversion.
    
    EIA format (with frequency='hourly'):
        period: "2024-01-01T05" (UTC timezone - 5 hours ahead of EST)
        subba: "ZONA" (EIA zone code)
        value: "1234.5" (string, in megawatthours)
        
    Target format:
        timestamp: timezone-aware datetime in America/New_York (Eastern Time)
        zone: "A" (simple zone identifier)
        load_mw: float
        forecast_mw: float (set to null since EIA doesn't provide forecast)
    
    Note:
        EIA API with frequency='hourly' returns UTC timestamps. This function
        parses them as UTC and converts to Eastern Time (America/New_York).
    
    Args:
        eia_data: List of dictionaries from EIA API (all zones)
        
    Returns:
        DataFrame with target schema, timestamps in America/New_York timezone
    """
    if not eia_data:
        return pl.DataFrame({
            "timestamp": [],
            "zone": [],
            "load_mw": [],
            "forecast_mw": [],
        })
    
    df = pl.DataFrame(eia_data)
    
    # Map EIA zone codes (ZONA, ZONB, etc.) to simple codes (A, B, etc.)
    df = df.with_columns([
        pl.col("subba").replace(EIA_ZONE_MAPPING).alias("zone")
    ])
    
    # Parse UTC timestamp and convert to Eastern Time
    # EIA format: "2024-01-01T05" (UTC)
    # 1. Append ":00" for Polars parsing (requires minutes)
    # 2. Parse as UTC datetime
    # 3. Convert to America/New_York (Eastern Time)
    df = df.with_columns([
        (pl.col("period") + ":00")
            .str.strptime(pl.Datetime("us", "UTC"), "%Y-%m-%dT%H:%M")
            .dt.convert_time_zone("America/New_York")
            .alias("timestamp"),
        pl.col("value").cast(pl.Float64).alias("load_mw"),
        pl.lit(None).cast(pl.Float64).alias("forecast_mw"),  # EIA doesn't provide forecast
    ])
    
    # Select and order columns
    df = df.select(["timestamp", "zone", "load_mw", "forecast_mw"])
    
    return df


def fetch_nyiso_zone_data(start_date: str, end_date: str, api_key: str) -> pl.DataFrame:
    """Fetch NYISO zone load data from EIA API for a date range.
    
    Fetches all 11 NYISO zones in a single API query (more efficient than per-zone).
    
    Args:
        start_date: Start date (YYYY-MM-DD format)
        end_date: End date (YYYY-MM-DD format)
        api_key: EIA API key
        
    Returns:
        DataFrame with columns: timestamp, zone, load_mw, forecast_mw
    """
    print(f"Fetching NYISO zone load data from EIA API ({start_date} to {end_date})...")
    print(f"  Fetching all zones in single query...")
    
    try:
        # Fetch raw data from EIA (gets ALL zones at once)
        eia_data = fetch_all_zones_from_eia(api_key, start_date, end_date)
        
        if not eia_data:
            raise ValueError("No data returned from EIA API")
        
        print(f"  ✓ Fetched {len(eia_data):,} total rows")
        
        # Transform to target schema
        df = transform_eia_data(eia_data)
        
        # Count zones returned
        zones_returned = df["zone"].unique().sort().to_list()
        print(f"  ✓ Zones returned: {', '.join(zones_returned)}")
        
        return df
        
    except Exception as e:
        print(f"  ✗ Error fetching data: {e}")
        raise


def fill_missing_hours(df: pl.DataFrame, start_date: str, end_date: str) -> pl.DataFrame:
    """Fill missing hours with linear interpolation.
    
    Safety constraint: Will only interpolate if there are no more than 2 consecutive
    missing hours at any zone. If 3+ consecutive hours are missing, raises an error.
    
    Args:
        df: DataFrame with zone load data (timezone-aware timestamps in Eastern Time)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with missing hours filled via interpolation, including 'filled' column
        to track which rows were interpolated (True) vs. original (False)
        
    Raises:
        ValueError: If any zone has 3+ consecutive missing hours
    """
    from zoneinfo import ZoneInfo
    
    print("\nChecking for missing hours and gaps...")
    
    # Add 'filled' column to original data (all False initially)
    df = df.with_columns(pl.lit(False).alias("filled"))
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo("America/New_York"))
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo("America/New_York"))
    
    # Generate complete hourly range
    all_timestamps = []
    current = start_dt
    while current <= end_dt:
        for hour in range(24):
            try:
                ts = current.replace(hour=hour, minute=0, second=0, microsecond=0)
                if ts <= end_dt.replace(hour=23, minute=0, second=0):
                    all_timestamps.append(ts)
            except:
                pass  # Skip invalid hours (DST spring forward)
        current += timedelta(days=1)
    
    all_zones_data = []
    
    for zone in NYISO_ZONES:
        zone_df = df.filter(pl.col("zone") == zone).sort("timestamp")
        actual_timestamps = set(zone_df['timestamp'].to_list())
        expected_set = set(all_timestamps)
        missing = sorted(expected_set - actual_timestamps)
        
        if not missing:
            print(f"  ✓ Zone {zone}: Complete (no missing hours)")
            all_zones_data.append(zone_df)
            continue
        
        # Check for consecutive gaps
        consecutive_gaps = []
        current_gap = [missing[0]]
        
        for i in range(1, len(missing)):
            if missing[i] - missing[i-1] == timedelta(hours=1):
                current_gap.append(missing[i])
            else:
                consecutive_gaps.append(current_gap)
                current_gap = [missing[i]]
        consecutive_gaps.append(current_gap)
        
        max_gap = max(len(gap) for gap in consecutive_gaps)
        
        if max_gap > 2:
            raise ValueError(
                f"Zone {zone} has {max_gap} consecutive missing hours. "
                f"First gap: {consecutive_gaps[0][0]}. "
                f"Cannot safely interpolate gaps > 2 hours."
            )
        
        print(f"  ⚠️  Zone {zone}: {len(missing)} missing hour(s), max consecutive: {max_gap}")
        print(f"      Missing: {[ts.strftime('%m-%d %H:%M') for ts in missing]}")
        
        # Fill missing hours with interpolation
        # Create complete time series with nulls for missing hours
        complete_df = pl.DataFrame({
            "timestamp": all_timestamps,
            "zone": [zone] * len(all_timestamps)
        })
        
        # Left join to get existing data (brings 'filled' column along)
        filled_df = complete_df.join(zone_df, on=["timestamp", "zone"], how="left")
        
        # Mark rows that need interpolation (where load_mw is null) before interpolating
        filled_df = filled_df.with_columns([
            pl.when(pl.col("load_mw").is_null())
            .then(pl.lit(True))
            .otherwise(pl.col("filled"))
            .alias("filled")
        ])
        
        # Interpolate load_mw (linear interpolation between neighbors)
        filled_df = filled_df.with_columns([
            pl.col("load_mw").interpolate(method="linear").alias("load_mw")
        ])
        
        all_zones_data.append(filled_df)
        print(f"      ✓ Filled via linear interpolation")
    
    # Combine all zones
    result_df = pl.concat(all_zones_data)
    
    return result_df


def validate_zone_data(df: pl.DataFrame, start_date: str, end_date: str) -> None:
    """Validate that fetched data is complete and reasonable.
    
    Note: Validation accounts for DST transitions which can result in 23 or 25 hours
    in a day during spring forward or fall back.
    
    Args:
        df: DataFrame with zone load data (timezone-aware timestamps in Eastern Time)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    """
    # Calculate expected number of hours, accounting for DST transitions
    from zoneinfo import ZoneInfo
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo("America/New_York"))
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo("America/New_York"))
    
    # Count actual hours accounting for DST
    expected_hours = 0
    current = start_dt
    while current <= end_dt:
        for hour in range(24):
            try:
                ts = current.replace(hour=hour, minute=0, second=0, microsecond=0)
                if ts <= end_dt.replace(hour=23, minute=0, second=0):
                    expected_hours += 1
            except:
                # Skip invalid hours (spring forward)
                pass
        current += timedelta(days=1)
    
    print("\nValidating fetched data (Eastern Time with DST handling)...")
    
    for zone in NYISO_ZONES:
        zone_data = df.filter(pl.col("zone") == zone)
        n_hours = len(zone_data)
        
        if n_hours == 0:
            print(f"  ⚠️  Zone {zone}: No data found")
            continue
        
        if n_hours != expected_hours:
            print(f"  ⚠️  Zone {zone}: Expected {expected_hours} hours, got {n_hours}")
        else:
            print(f"  ✓ Zone {zone}: {n_hours} hours")
        
        # Check for nulls
        null_count = zone_data["load_mw"].null_count()
        if null_count > 0:
            print(f"  ⚠️  Zone {zone}: {null_count} null load values")
        
        # Check for negative loads
        negative_count = (zone_data["load_mw"] < 0).sum()
        if negative_count > 0:
            print(f"  ⚠️  Zone {zone}: {negative_count} negative load values")
        
        # Check for unreasonably high loads (> 50,000 MW)
        high_count = (zone_data["load_mw"] > 50000).sum()
        if high_count > 0:
            print(f"  ⚠️  Zone {zone}: {high_count} suspiciously high load values (> 50,000 MW)")


def upload_zone_data_to_s3(df: pl.DataFrame, s3_base: str, skip_existing: bool = True):
    """Upload zone load data to S3 with zone/year/month partitioning.
    
    Args:
        df: DataFrame with columns timestamp, zone, load_mw, forecast_mw
        s3_base: Base S3 path (e.g., s3://data.sb/nyiso/loads)
        skip_existing: If True, skip uploading partitions that already exist
        
    Output structure:
        s3://data.sb/nyiso/loads/zone=A/year=2024/month=01/data.parquet
        
    Note:
        Uses Polars partition_by to automatically create Hive-style partitioning.
        Checks for existing partitions and skips them if skip_existing=True.
    """
    base_path = S3Path(s3_base)
    
    # Add year and month columns for partitioning
    df = df.with_columns([
        pl.col("timestamp").dt.year().alias("year"),
        pl.col("timestamp").dt.month().alias("month")
    ])
    
    # Get unique zone-year-month combinations
    partitions = df.select(["zone", "year", "month"]).unique().sort(["zone", "year", "month"])
    
    print(f"\nChecking {len(partitions)} partitions...")
    
    # Check which partitions already exist
    partitions_to_write = []
    for row in partitions.iter_rows(named=True):
        zone, year, month = row["zone"], row["year"], row["month"]
        partition_path = base_path / f"zone={zone}" / f"year={year}" / f"month={month:02d}"
        
        if skip_existing and partition_path.exists():
            print(f"  ⏭️  Skipping zone={zone}/year={year}/month={month:02d} (already exists)")
        else:
            partitions_to_write.append((zone, year, month))
            print(f"  ✓ Will write zone={zone}/year={year}/month={month:02d}")
    
    if not partitions_to_write:
        print("\n⚠️  All partitions already exist. Nothing to upload.")
        return
    
    # Filter to only partitions we want to write
    filter_expr = pl.lit(False)
    for zone, year, month in partitions_to_write:
        filter_expr = filter_expr | (
            (pl.col("zone") == zone) & 
            (pl.col("year") == year) & 
            (pl.col("month") == month)
        )
    
    df_to_write = df.filter(filter_expr)
    
    print(f"\nWriting {len(df_to_write):,} rows across {len(partitions_to_write)} partitions...")
    
    # Write with Polars partitioning (automatically creates directory structure)
    df_to_write.write_parquet(
        str(base_path),
        partition_by=["zone", "year", "month"],
        use_pyarrow=False,
        compression="zstd",
    )
    
    print(f"✓ Successfully uploaded to {base_path}")


def parse_month_to_date_range(start_month: str, end_month: str) -> tuple[str, str]:
    """Parse month-year strings to full date range.
    
    Args:
        start_month: Start month in YYYY-MM format (e.g., "2024-01")
        end_month: End month in YYYY-MM format (e.g., "2024-12")
        
    Returns:
        Tuple of (start_date, end_date):
            - start_date: First day of start month (YYYY-MM-DD) in Eastern Time
            - end_date: Last day of end month (YYYY-MM-DD) in Eastern Time
    
    Note:
        Dates are interpreted as Eastern Time. The fetch function will handle
        conversion to UTC for the EIA API request.
    """
    # Parse start month
    try:
        start_year, start_mon = map(int, start_month.split("-"))
        start_date = f"{start_year:04d}-{start_mon:02d}-01"
    except (ValueError, AttributeError):
        raise ValueError(f"Invalid start month format: {start_month}. Use YYYY-MM.")
    
    # Parse end month and get last day
    try:
        end_year, end_mon = map(int, end_month.split("-"))
        last_day = calendar.monthrange(end_year, end_mon)[1]
        end_date = f"{end_year:04d}-{end_mon:02d}-{last_day:02d}"
    except (ValueError, AttributeError):
        raise ValueError(f"Invalid end month format: {end_month}. Use YYYY-MM.")
    
    # Validate order
    if datetime.strptime(start_date, "%Y-%m-%d") > datetime.strptime(end_date, "%Y-%m-%d"):
        raise ValueError("Start month must be before or equal to end month")
    
    return start_date, end_date


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Fetch NYISO zone load data from EIA API and upload to S3"
    )
    parser.add_argument(
        "--start-month",
        type=str,
        required=True,
        help="Start month (YYYY-MM format, e.g., 2024-01)",
    )
    parser.add_argument(
        "--end-month",
        type=str,
        required=True,
        help="End month (YYYY-MM format, e.g., 2024-12)",
    )
    parser.add_argument(
        "--eia-api-key",
        type=str,
        help="EIA API key (optional if set in .env file)",
    )
    parser.add_argument(
        "--s3-base",
        type=str,
        default="s3://data.sb/nyiso/loads",
        help="Base S3 path for uploads (default: s3://data.sb/nyiso/loads)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip uploading partitions that already exist in S3 (default: True)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force upload even if partitions exist (overrides --skip-existing)",
    )
    
    args = parser.parse_args()
    
    # Parse month ranges to dates
    try:
        start_date, end_date = parse_month_to_date_range(
            args.start_month, args.end_month
        )
    except ValueError as e:
        parser.error(str(e))
    
    skip_existing = args.skip_existing and not args.force
    
    print("="*60)
    print("NYISO Zone Load Data Fetch (via EIA API)")
    print("="*60)
    print(f"Months requested: {args.start_month} to {args.end_month}")
    print(f"Date range: {start_date} to {end_date} (Eastern Time, inclusive)")
    print(f"Target S3: {args.s3_base}/zone=X/year=YYYY/month=MM/")
    print(f"Skip existing: {skip_existing}")
    print("="*60)
    
    # Load API key
    api_key = load_api_key(args.eia_api_key)
    
    # Fetch data from EIA API
    # Note: API request is converted from ET to UTC, then responses are converted back to ET
    df = fetch_nyiso_zone_data(start_date, end_date, api_key)
    
    # Filter to exact ET date range
    # EIA API doesn't respect exact UTC hour boundaries, so we filter after conversion
    from zoneinfo import ZoneInfo
    start_dt_et = datetime.strptime(start_date, "%Y-%m-%d").replace(
        hour=0, minute=0, second=0, tzinfo=ZoneInfo("America/New_York")
    )
    end_dt_et = datetime.strptime(end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=ZoneInfo("America/New_York")
    )
    
    df = df.filter(
        (pl.col("timestamp") >= start_dt_et) & (pl.col("timestamp") <= end_dt_et)
    )
    
    print(f"\nFiltered to exact date range: {start_date} 00:00 to {end_date} 23:00 (Eastern Time)")
    print(f"Total rows after filtering: {len(df):,}")
    
    # Fill missing hours with interpolation (raises error if gaps > 2 consecutive hours)
    df = fill_missing_hours(df, start_date, end_date)
    print(f"Total rows after gap filling: {len(df):,}")
    filled_count = df.filter(pl.col("filled") == True).shape[0]
    print(f"Interpolated rows: {filled_count} ({filled_count/len(df)*100:.2f}%)")
    
    # Debug: Check for missing hours in one zone (with timezone-aware timestamps)
    debug_zone = df.filter(pl.col("zone") == "A").sort("timestamp")
    if len(debug_zone) > 0:
        print(f"\nDebug - Zone A timestamps (Eastern Time):")
        print(f"  First: {debug_zone['timestamp'].min()}")
        print(f"  Last:  {debug_zone['timestamp'].max()}")
        print(f"  Count: {len(debug_zone)}")
        
        # Check for gaps by generating expected hourly range (in Eastern Time)
        from zoneinfo import ZoneInfo
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo("America/New_York"))
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo("America/New_York"))
        
        # Generate all expected hourly timestamps (handles DST automatically)
        expected_timestamps = []
        current = start_dt
        while current <= end_dt:
            for hour in range(24):
                try:
                    ts = current.replace(hour=hour, minute=0, second=0, microsecond=0)
                    if ts <= end_dt.replace(hour=23, minute=0, second=0):
                        expected_timestamps.append(ts)
                except:
                    # Skip invalid hours (e.g., 2:00 AM during spring forward)
                    pass
            current += timedelta(days=1)
        
        print(f"  Expected: {len(expected_timestamps)} hours (accounting for DST)")
        
        # Find missing timestamps
        actual_timestamps = set(debug_zone['timestamp'].to_list())
        expected_set = set(expected_timestamps)
        missing = expected_set - actual_timestamps
        
        if missing:
            print(f"  Missing {len(missing)} hour(s):")
            for ts in sorted(missing)[:5]:  # Show first 5
                print(f"    - {ts}")
            if len(missing) > 5:
                print(f"    ... and {len(missing) - 5} more")
            
            # Show hours around the first missing hour
            if missing:
                first_missing = min(missing)
                print(f"\n  Hours around missing {first_missing}:")
                nearby = debug_zone.filter(
                    (pl.col("timestamp") >= first_missing - timedelta(hours=3)) &
                    (pl.col("timestamp") <= first_missing + timedelta(hours=3))
                ).sort("timestamp")
                for row in nearby.iter_rows(named=True):
                    ts_str = row['timestamp'].strftime("%Y-%m-%d %H:%M:%S %Z")
                    print(f"    {ts_str}: {row['load_mw']:.1f} MW")
        else:
            print(f"  ✓ No missing hours")
    
    # Validate data
    validate_zone_data(df, start_date, end_date)
    
    # Display data summary for inspection
    print("\n" + "="*60)
    print("DATA SUMMARY")
    print("="*60)
    print(f"Total rows: {len(df):,}")
    print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Zones: {df['zone'].unique().sort().to_list()}")
    print(f"\nLoad statistics (MW):")
    print(f"  Min:  {df['load_mw'].min():.2f}")
    print(f"  Max:  {df['load_mw'].max():.2f}")
    print(f"  Mean: {df['load_mw'].mean():.2f}")
    print(f"\nSample data (first 10 rows):")
    print(df.head(10))
    print(f"\nSample data (last 10 rows):")
    print(df.tail(10))
    
    # Upload to S3
    print("\n" + "="*60)
    print("UPLOADING TO S3")
    print("="*60)
    
    # Add year/month columns to show partition structure
    df_with_partitions = df.with_columns([
        pl.col("timestamp").dt.year().alias("year"),
        pl.col("timestamp").dt.month().alias("month")
    ])
    
    # Show partition summary
    partitions = df_with_partitions.select(["zone", "year", "month"]).unique().sort(["zone", "year", "month"])
    print(f"\nPreparing to upload {len(partitions)} partitions:")
    for row in partitions.iter_rows(named=True):
        zone, year, month = row["zone"], row["year"], row["month"]
        count = len(df_with_partitions.filter(
            (pl.col("zone") == zone) & 
            (pl.col("year") == year) & 
            (pl.col("month") == month)
        ))
        print(f"  zone={zone}/year={year}/month={month:02d}/ - {count:,} rows")
    
    # Upload to S3
    upload_zone_data_to_s3(df, args.s3_base, skip_existing=skip_existing)
    
    print("\n" + "="*60)
    print("✓ NYISO zone load data fetch and upload completed successfully")
    print("="*60)


if __name__ == "__main__":
    main()
