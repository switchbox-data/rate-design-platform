"""Fetch residential and wholesale heating fuel prices from EIA API v2 (EIA-877).

Downloads weekly state-level propane and heating oil prices from the
petroleum/pri/wfr endpoint, aggregates to monthly averages, and writes
Hive-partitioned parquet locally.

Setup:
    1. Register at https://www.eia.gov/opendata/
    2. Create .env file in project root: EIA_API_KEY=your_key_here

Output structure:
    <path_local_parquet>/product=<heating_oil|propane>/year=YYYY/month=M/data.parquet

Schema (within each partition):
    state            Utf8     2-char state abbreviation
    price_type       Utf8     "residential" or "wholesale"
    price_per_gallon Float64  Monthly avg price, $/gallon excl. taxes

Coverage:
    - ~39 states (22 for heating oil, 38 for propane)
    - Weekly during heating season (Oct-Mar), monthly off-season (Apr-Sep, 2024+)
    - Pre-2024: Oct-Mar only (no summer data)
    - Source survey: EIA-877 (Winter Heating Fuels Telephone Survey)

Usage:
    # Fetch all available data (1990-present)
    python fetch_heating_fuel_prices_parquet.py --path-local-parquet ./parquet

    # Fetch a specific date range
    python fetch_heating_fuel_prices_parquet.py --path-local-parquet ./parquet \\
        --start 1990-10 --end 2026-02

    # Override API key
    python fetch_heating_fuel_prices_parquet.py --path-local-parquet ./parquet \\
        --eia-api-key YOUR_KEY
"""

import argparse
import json
import os
import time
import urllib.request
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

EIA_API_BASE = "https://api.eia.gov/v2/"
EIA_WFR_ENDPOINT = "petroleum/pri/wfr/data/"
PAGE_SIZE = 5000

PRODUCT_MAP = {
    "EPD2F": "heating_oil",
    "EPLLPA": "propane",
}

PROCESS_MAP = {
    "PRS": "residential",
    "PWR": "wholesale",
}


def find_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in [current, *list(current.parents)]:
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError(
        f"Could not locate project root from '{current}'. "
        "No '.git' directory or 'pyproject.toml' found."
    )


def load_api_key(cli_key: str | None = None) -> str:
    if cli_key:
        return cli_key
    project_root = find_project_root()
    load_dotenv(dotenv_path=project_root / ".env")
    api_key = os.getenv("EIA_API_KEY")
    if api_key:
        return api_key
    raise ValueError(
        "EIA API key not found. Either:\n"
        "  1. Add to .env file: EIA_API_KEY=your_key\n"
        "  2. Use --eia-api-key argument\n"
        "Register at https://www.eia.gov/opendata/"
    )


def fetch_weekly_prices(
    api_key: str,
    start: str | None = None,
    end: str | None = None,
) -> list[dict]:
    """Fetch all weekly state-level heating fuel prices from EIA API.

    Returns raw rows for both products (heating oil, propane) and both
    price types (residential, wholesale), filtered to state-level duoareas.
    """
    all_rows: list[dict] = []
    offset = 0

    while True:
        params = (
            f"api_key={api_key}"
            f"&frequency=weekly"
            f"&data[0]=value"
            f"&length={PAGE_SIZE}"
            f"&offset={offset}"
            f"&sort[0][column]=period"
            f"&sort[0][direction]=asc"
        )
        if start:
            params += f"&start={start}-01"
        if end:
            params += f"&end={end}-28"

        url = f"{EIA_API_BASE}{EIA_WFR_ENDPOINT}?{params}"
        resp = json.loads(urllib.request.urlopen(url, timeout=30).read())

        if "response" not in resp or "data" not in resp["response"]:
            raise ValueError(f"Unexpected API response: {list(resp.keys())}")

        total = int(resp["response"]["total"])
        rows = resp["response"]["data"]

        state_rows = [
            r
            for r in rows
            if r["duoarea"].startswith("S")
            and r["product"] in PRODUCT_MAP
            and r["process"] in PROCESS_MAP
        ]
        all_rows.extend(state_rows)

        offset += PAGE_SIZE
        print(f"  Fetched {min(offset, total):,} / {total:,} API rows...")

        if offset >= total:
            break

        time.sleep(0.1)

    print(f"  Kept {len(all_rows):,} state-level rows after filtering")
    return all_rows


def to_monthly_parquet(rows: list[dict]) -> pl.DataFrame:
    """Convert raw API rows to a monthly-aggregated DataFrame.

    Normalizes EIA codes to clean names, groups weekly observations
    into monthly averages, and sorts for efficient downstream queries.
    """
    df = pl.DataFrame(rows)

    df = df.select(
        pl.col("period").str.slice(0, 7).alias("year_month"),
        pl.col("duoarea").str.slice(1).alias("state"),
        pl.col("product").replace_strict(PRODUCT_MAP).alias("product"),
        pl.col("process").replace_strict(PROCESS_MAP).alias("price_type"),
        pl.col("value").cast(pl.Float64).alias("price_per_gallon"),
    )

    df = df.filter(pl.col("price_per_gallon").is_not_null())

    monthly = df.group_by(["year_month", "state", "product", "price_type"]).agg(
        pl.col("price_per_gallon").mean()
    )

    monthly = monthly.with_columns(
        pl.col("year_month").str.slice(0, 4).cast(pl.Int32).alias("year"),
        pl.col("year_month").str.slice(5, 2).cast(pl.Int8).alias("month"),
    ).drop("year_month")

    monthly = monthly.sort(["product", "year", "month", "state", "price_type"])

    return monthly


def write_partitioned_parquet(df: pl.DataFrame, path_local_parquet: str) -> None:
    """Write monthly data as Hive-partitioned parquet: product/year/month.

    Writes each partition as data.parquet (not the Polars default 00000000.parquet)
    for consistency with other pipelines in this repo.
    """
    out = Path(path_local_parquet)
    partitions = (
        df.select(["product", "year", "month"])
        .unique()
        .sort(["product", "year", "month"])
    )
    n_partitions = len(partitions)
    print(f"\nWriting {n_partitions:,} partitions ({len(df):,} rows) to {out}")

    data_cols = ["state", "price_type", "price_per_gallon"]

    for row in partitions.iter_rows(named=True):
        product, year, month = row["product"], row["year"], row["month"]
        part_dir = out / f"product={product}" / f"year={year}" / f"month={month}"
        part_dir.mkdir(parents=True, exist_ok=True)

        part_df = df.filter(
            (pl.col("product") == product)
            & (pl.col("year") == year)
            & (pl.col("month") == month)
        ).select(data_cols)

        part_df.write_parquet(part_dir / "data.parquet", compression="zstd")

    print(f"Done — {n_partitions} partitions at {out}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch EIA-877 heating fuel prices and write monthly parquet"
    )
    parser.add_argument(
        "--path-local-parquet",
        required=True,
        help="Local directory for partitioned parquet output",
    )
    parser.add_argument(
        "--start",
        default=None,
        help="Start month (YYYY-MM, default: earliest available ~1990-10)",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="End month (YYYY-MM, default: latest available)",
    )
    parser.add_argument(
        "--eia-api-key",
        default=None,
        help="EIA API key (optional if set in .env)",
    )
    args = parser.parse_args()

    path_local = args.path_local_parquet
    if "{{" in path_local or "}}" in path_local:
        raise ValueError(
            f"Output path looks like an uninterpolated Just variable: {path_local}"
        )

    api_key = load_api_key(args.eia_api_key)

    print("=" * 60)
    print("EIA-877 Heating Fuel Price Fetch")
    print("=" * 60)
    print(f"Date range: {args.start or '(earliest)'} to {args.end or '(latest)'}")
    print(f"Output: {path_local}/product=X/year=YYYY/month=M/data.parquet")
    print("=" * 60)

    print("\nFetching weekly prices from EIA API...")
    rows = fetch_weekly_prices(api_key, start=args.start, end=args.end)

    if not rows:
        print("No data returned from API.")
        return

    print("\nAggregating to monthly averages...")
    df = to_monthly_parquet(rows)

    print(f"\n{'=' * 60}")
    print("DATA SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total monthly observations: {len(df):,}")
    print(
        f"Date range: {df['year'].min()}-{df['month'].min():02d} to "
        f"{df['year'].max()}-{df['month'].max():02d}"
    )
    print(f"States: {df['state'].n_unique()}")
    print(f"Products: {df['product'].unique().sort().to_list()}")
    print(f"Price types: {df['price_type'].unique().sort().to_list()}")
    print("\nPrice statistics ($/gallon):")
    print(f"  Min:  {df['price_per_gallon'].min():.3f}")
    print(f"  Max:  {df['price_per_gallon'].max():.3f}")
    print(f"  Mean: {df['price_per_gallon'].mean():.3f}")
    print("\nSample rows:")
    print(df.head(10))

    write_partitioned_parquet(df, path_local)

    print(f"\n{'=' * 60}")
    print("Fetch complete (run upload recipe to sync to S3)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
