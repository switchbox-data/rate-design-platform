"""Fetch CPIAUCSL from FRED and write annual-average CPI to S3.

Series: CPIAUCSL (Consumer Price Index for All Urban Consumers: All Items).
Output path: s3://data.sb/fred/cpi/cpiaucsl_<start_year>_<end_year>_<YYYYMMDD>.parquet
Columns: year (int), cpi_value (float, annual average for that year).

Requires FRED_API_KEY in environment (e.g. from .env via python-dotenv).
"""

from __future__ import annotations

import argparse
import os
from datetime import date
from urllib.parse import urlencode

import polars as pl
import requests
from cloudpathlib import S3Path
from dotenv import load_dotenv

from utils.eia_region_config import get_aws_storage_options

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
SERIES_ID = "CPIAUCSL"
DEFAULT_S3_BASE = "s3://data.sb"


def fetch_observations(
    api_key: str,
    start_year: int,
    end_year: int,
) -> list[dict]:
    """Request CPIAUCSL observations from FRED for the given year range."""
    params = {
        "series_id": SERIES_ID,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": f"{start_year}-01-01",
        "observation_end": f"{end_year}-12-31",
    }
    url = f"{FRED_BASE}?{urlencode(params)}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "observations" not in data:
        raise ValueError("FRED response missing 'observations'")
    return data["observations"]


def observations_to_annual_df(observations: list[dict]) -> pl.DataFrame:
    """Convert monthly observations to annual-average CPI (year, cpi_value)."""
    rows: list[dict] = []
    for ob in observations:
        val_str = ob.get("value", ".")
        if val_str == ".":
            continue
        try:
            val = float(val_str)
        except ValueError:
            continue
        obs_date = ob.get("date", "")
        if len(obs_date) >= 4:
            rows.append({"year": int(obs_date[:4]), "value": val})
    if not rows:
        raise ValueError("No valid observations to aggregate")
    df = pl.DataFrame(rows)
    annual = (
        df.group_by("year").agg(pl.col("value").mean().alias("cpi_value")).sort("year")
    )
    return annual


def build_s3_path(
    s3_base: str, start_year: int, end_year: int, download_date: date
) -> str:
    """Return S3 key path: fred/cpi/cpiaucsl_<start>_<end>_<YYYYMMDD>.parquet."""
    base = s3_base.rstrip("/")
    yyyymmdd = download_date.strftime("%Y%m%d")
    return f"{base}/fred/cpi/cpiaucsl_{start_year}_{end_year}_{yyyymmdd}.parquet"


def upload_cpi_to_s3(annual_df: pl.DataFrame, s3_path_str: str) -> None:
    """Upload CPI parquet to S3 at the given path."""
    storage_options = get_aws_storage_options()
    s3_path = S3Path(s3_path_str)
    if not s3_path.parent.exists():
        s3_path.parent.mkdir(parents=True)
    annual_df.write_parquet(str(s3_path), storage_options=storage_options)
    print("Uploaded.")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Fetch CPIAUCSL from FRED and write annual CPI to S3."
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2020,
        help="First calendar year to fetch (default: 2020)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2025,
        help="Last calendar year to fetch (default: 2025)",
    )
    parser.add_argument(
        "--s3-base",
        default=DEFAULT_S3_BASE,
        help=f"S3 base URL for output (default: {DEFAULT_S3_BASE})",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload parquet to S3; otherwise only print path and row count",
    )
    args = parser.parse_args()

    if args.start_year > args.end_year:
        parser.error("--start-year must be <= --end-year")

    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise SystemExit("FRED_API_KEY environment variable is not set")

    observations = fetch_observations(api_key, args.start_year, args.end_year)
    annual_df = observations_to_annual_df(observations)
    download_date = date.today()
    s3_path_str = build_s3_path(
        args.s3_base, args.start_year, args.end_year, download_date
    )

    print(
        f"Fetched {len(observations)} monthly observations -> {len(annual_df)} annual rows"
    )
    print(f"Output path: {s3_path_str}")

    if args.upload:
        upload_cpi_to_s3(annual_df, s3_path_str)


if __name__ == "__main__":
    main()
