"""Fetch a FRED series and write annual-average values to local parquet (and optionally S3).

Supports any FRED series with monthly observations (e.g. CPIAUCSL for CPI).
With --output <dir>: writes <dir>/<series_lower>_<start>_<end>_<YYYYMMDD>.parquet.
Columns: year (int), value (float, annual average for that year).
Upload via Justfile upload recipe (aws s3 sync).

Requires FRED_API_KEY in environment (e.g. from .env via python-dotenv).
"""

from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

import polars as pl
import requests
from dotenv import load_dotenv

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
DEFAULT_SERIES = "CPIAUCSL"


def fetch_observations(
    api_key: str,
    series_id: str,
    start_year: int,
    end_year: int,
) -> list[dict]:
    """Request FRED series observations for the given year range."""
    params = {
        "series_id": series_id,
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
    """Convert monthly observations to annual average (year, value)."""
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
    annual = df.group_by("year").agg(pl.col("value").mean().alias("value")).sort("year")
    return annual


def build_filename(
    series_id: str, start_year: int, end_year: int, download_date: date
) -> str:
    """Return filename: <series_lower>_<start>_<end>_<YYYYMMDD>.parquet."""
    series_lower = series_id.lower()
    yyyymmdd = download_date.strftime("%Y%m%d")
    return f"{series_lower}_{start_year}_{end_year}_{yyyymmdd}.parquet"


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Fetch a FRED series and write annual-average values to local parquet."
    )
    parser.add_argument(
        "--series",
        default=DEFAULT_SERIES,
        help=f"FRED series ID (default: {DEFAULT_SERIES})",
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
        "--output",
        type=Path,
        default=None,
        metavar="DIR",
        help="Write parquet to DIR/<series>_<start>_<end>_<date>.parquet",
    )
    args = parser.parse_args()

    if args.start_year > args.end_year:
        parser.error("--start-year must be <= --end-year")

    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise SystemExit("FRED_API_KEY environment variable is not set")

    observations = fetch_observations(
        api_key, args.series, args.start_year, args.end_year
    )
    annual_df = observations_to_annual_df(observations)
    download_date = date.today()
    filename = build_filename(
        args.series, args.start_year, args.end_year, download_date
    )

    print(
        f"Fetched {len(observations)} monthly observations -> {len(annual_df)} annual rows"
    )
    print(f"Series: {args.series}")

    if args.output is not None:
        out_dir = args.output.resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / filename
        annual_df.write_parquet(out_path)
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
