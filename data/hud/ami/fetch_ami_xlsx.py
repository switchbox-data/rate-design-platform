#!/usr/bin/env python3
"""
Download HUD Section 8 Income Limits Excel files by fiscal year.

Downloads Section8-FY{yyyy}.xlsx from HUD for each year in [start_year, end_year]
into the given output directory. Use with the Justfile or run directly:

    uv run python fetch_ami_xlsx.py --start-year 2016 --end-year 2025 --output xlsx/
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from tqdm import tqdm

FY_MIN = 2016
FY_MAX = 2025


def build_url(year: int) -> str:
    """Build HUD Section 8 Excel URL for a fiscal year.

    HUD uses 2-digit year in both path and filename, e.g. il24/Section8-FY24.xlsx.
    """
    yy = year % 100
    return f"https://www.huduser.gov/portal/datasets/il/il{yy}/Section8-FY{yy}.xlsx"


def download_year(year: int, output_dir: Path) -> bool:
    """Download Section 8 Excel for one fiscal year. Return True on success.

    Saves as Section8-FY{yyyy}.xlsx locally so convert script can infer year from filename.
    """
    url = build_url(year)
    out_path = output_dir / f"Section8-FY{year}.xlsx"
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(resp.content)
        return True
    except Exception as e:
        print(f"Error downloading FY{year}: {e}", file=sys.stderr)
        return False


def main() -> int:
    args = _parse_args()
    start_year = args.start_year
    end_year = args.end_year
    if start_year > end_year:
        print("Error: start-year must be <= end-year", file=sys.stderr)
        return 1
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    years = list(range(start_year, end_year + 1))
    workers = args.workers
    failed = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_year = {
            executor.submit(download_year, y, output_dir): y for y in years
        }
        with tqdm(total=len(years), desc="Downloading", unit="file") as pbar:
            for future in as_completed(future_to_year):
                year = future_to_year[future]
                if not future.result():
                    failed.append(year)
                pbar.update(1)

    if failed:
        print(f"Failed years: {failed}", file=sys.stderr)
        return 1
    print(f"Downloaded {len(years)} file(s) to {output_dir.absolute()}")
    return 0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download HUD Section 8 Income Limits Excel files by fiscal year"
    )
    p.add_argument(
        "--start-year",
        type=int,
        default=FY_MIN,
        metavar="YEAR",
        help=f"First fiscal year (default: {FY_MIN})",
    )
    p.add_argument(
        "--end-year",
        type=int,
        default=FY_MAX,
        metavar="YEAR",
        help=f"Last fiscal year (default: {FY_MAX})",
    )
    p.add_argument(
        "--output",
        "-o",
        metavar="DIR",
        default="xlsx",
        help="Output directory for downloaded xlsx files (default: xlsx)",
    )
    p.add_argument(
        "--workers",
        "-j",
        type=int,
        default=5,
        metavar="N",
        help="Number of parallel downloads (default: 5)",
    )
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(main())
