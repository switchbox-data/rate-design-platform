#!/usr/bin/env python3
"""Fetch NYISO ICAP monthly clearing prices via gridstatus → Hive-partitioned parquet.

Downloads ICAP Market Report Excel files from nyiso.com for each requested year
and writes tidy long-format parquet to:
    <output_dir>/year={YYYY}/month={M}/data.parquet

Schema:
    year             Int16       (redundant with partition key)
    month            UInt8       (redundant with partition key)
    locality         Categorical (NYCA, GHIJ, NYC, LI)
    auction_type     Categorical (Spot, Monthly, Strip)
    price_per_kw_month Float64   ($/kW-month clearing price)

Known limitation (as of gridstatus 0.34.0):
    Years 2014–2018 fail with "Year not currently supported" due to a bug in
    gridstatus's year_code if/elif chain (uses bare `if` for 2017–2019, so earlier
    years fall through to the `else` raise). The data for those years *does* exist
    in the rolling window of later reports (e.g. the Dec 2019 report contains rows
    back to May 2014), so a workaround would be to extract earlier years from a
    later report's raw DataFrame. Not implemented yet since 2019–2025 covers our
    immediate needs.

Usage:
    uv run python data/nyiso/icap/fetch_icap_clearing_prices.py \
        --start 2019 --end 2025 --path-local-parquet data/nyiso/icap/parquet
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from gridstatus import NYISO

ALL_LOCALITIES = ["NYCA", "GHIJ", "NYC", "LI"]
ALL_AUCTION_TYPES = ["Spot", "Monthly", "Strip"]

SCHEMA = {
    "year": pl.Int16,
    "month": pl.UInt8,
    "locality": pl.Categorical,
    "auction_type": pl.Categorical,
    "price_per_kw_month": pl.Float64,
}


def _reject_just_placeholders(val: str) -> None:
    if "{{" in val or "}}" in val:
        print(
            f"ERROR: looks like an uninterpolated Just variable: {val}", file=sys.stderr
        )
        sys.exit(1)


def _fetch_year_raw(
    iso: NYISO, year: int, *, max_month: int = 12
) -> pd.DataFrame | None:
    """Try the latest available month's report for the year, walking backwards."""
    for m in range(max_month, 0, -1):
        date = pd.Timestamp(year=year, month=m, day=1)
        try:
            df = iso.get_capacity_prices(date=date, verbose=False)
            print(f"  {date.strftime('%B %Y')}: {len(df)} rows")
            return df
        except Exception as e:
            err = str(e)
            short = err[:120] + "..." if len(err) > 120 else err
            print(f"  {date.strftime('%B %Y')}: {short}")
    return None


def _reshape_to_polars(raw: pd.DataFrame, year: int) -> pl.DataFrame:
    """Reshape gridstatus multi-index (locality, auction_type) into tidy Polars DataFrame."""
    records: list[dict[str, object]] = []
    for ts in raw.index:
        if ts.year != year:
            continue
        for locality, auction_type in raw.columns:
            price = raw.loc[ts, (locality, auction_type)]
            if pd.isna(price):
                continue
            records.append(
                {
                    "year": year,
                    "month": ts.month,
                    "locality": locality,
                    "auction_type": auction_type,
                    "price_per_kw_month": float(price),
                }
            )
    return pl.DataFrame(records, schema=SCHEMA).sort(
        "month", "locality", "auction_type"
    )


def _write_partitioned(df: pl.DataFrame, output_dir: Path) -> int:
    """Write one data.parquet per (year, month) partition. Returns files written."""
    files_written = 0
    for (year_val, month_val), group in df.group_by(
        ["year", "month"], maintain_order=True
    ):
        part_dir = output_dir / f"year={year_val}" / f"month={month_val}"
        part_dir.mkdir(parents=True, exist_ok=True)
        path = part_dir / "data.parquet"
        group.write_parquet(path, compression="snappy")
        files_written += 1
    return files_written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch NYISO ICAP clearing prices via gridstatus → parquet",
    )
    parser.add_argument(
        "--start", type=int, required=True, help="First year to fetch (e.g. 2014)"
    )
    parser.add_argument(
        "--end", type=int, required=True, help="Last year to fetch (e.g. 2025)"
    )
    parser.add_argument(
        "--path-local-parquet",
        type=str,
        required=True,
        help="Output directory for Hive-partitioned parquet",
    )
    args = parser.parse_args()

    _reject_just_placeholders(args.path_local_parquet)
    output_dir = Path(args.path_local_parquet)
    output_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    iso = NYISO()
    total_files = 0

    for year in range(args.start, args.end + 1):
        print(f"\n{'=' * 60}")
        print(f"Fetching ICAP clearing prices for {year}")
        print(f"{'=' * 60}")

        max_month = now.month - 1 if year == now.year else 12
        if max_month < 1:
            print(f"  No data available yet for {year}")
            continue

        raw = _fetch_year_raw(iso, year, max_month=max_month)
        if raw is None:
            print(f"  No data found for {year}")
            continue

        df = _reshape_to_polars(raw, year)
        n_months = df["month"].n_unique()
        n_files = _write_partitioned(df, output_dir)
        total_files += n_files
        print(f"  Wrote {n_files} partition files ({len(df)} rows, {n_months} months)")

    print(f"\nDone. {total_files} partition files in {output_dir}")


if __name__ == "__main__":
    main()
