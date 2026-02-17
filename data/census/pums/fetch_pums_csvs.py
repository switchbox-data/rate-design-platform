#!/usr/bin/env python3
"""Fetch Census ACS PUMS zip files (no extraction).

Downloads .zip files from Census FTP to a zips directory. A separate unzip
recipe (in the Justfile) extracts them into the canonical CSV tree. Supports
acs1 (1-year) and acs5 (5-year) for configurable end-year and states.

Usage:
    # Fetch acs1 2023 for DC only (fast smoke test)
    uv run python data/census/pums/fetch_pums_csvs.py --survey acs1 --end-year 2023 --state DC --output-dir zips

    # Fetch acs5 2022 for NY and RI, both record types
    uv run python data/census/pums/fetch_pums_csvs.py --survey acs5 --end-year 2022 --state NY RI --record-type both --output-dir zips

    # Fetch all states for one survey/year (writes to zips/acs1/2023/)
    uv run python data/census/pums/fetch_pums_csvs.py --survey acs1 --end-year 2023 --output-dir zips
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests

from data.census.pums.convert_pums_csv_to_parquet import PUMS_STATE_CODES

CENSUS_PUMS_BASE = "https://www2.census.gov/programs-surveys/acs/data/pums"

# End-year bounds (inclusive); Census may lag for latest years.
ACS1_YEAR_MIN = 2013
ACS1_YEAR_MAX = 2024
ACS5_YEAR_MIN = 2009
ACS5_YEAR_MAX = 2023


def _survey_to_subdir(survey: str) -> str:
    if survey == "acs1":
        return "1-Year"
    if survey == "acs5":
        return "5-Year"
    raise ValueError(f"survey must be acs1 or acs5, got {survey!r}")


def _validate_year(survey: str, end_year: int) -> None:
    if survey == "acs1":
        if not ACS1_YEAR_MIN <= end_year <= ACS1_YEAR_MAX:
            raise ValueError(
                f"acs1 end-year must be {ACS1_YEAR_MIN}-{ACS1_YEAR_MAX}, got {end_year}"
            )
    else:
        if not ACS5_YEAR_MIN <= end_year <= ACS5_YEAR_MAX:
            raise ValueError(
                f"acs5 end-year must be {ACS5_YEAR_MIN}-{ACS5_YEAR_MAX}, got {end_year}"
            )


def _url_for(survey: str, end_year: int, record_type: str, state_lower: str) -> str:
    subdir = _survey_to_subdir(survey)
    prefix = "csv_p" if record_type == "person" else "csv_h"
    filename = f"{prefix}{state_lower}.zip"
    return f"{CENSUS_PUMS_BASE}/{end_year}/{subdir}/{filename}"


def fetch_pums_zips(
    survey: str,
    end_year: int,
    states: list[str],
    record_types: list[str],
    output_dir: Path,
) -> None:
    """Download PUMS zip files to output_dir/{survey}/{end_year}/."""
    _validate_year(survey, end_year)
    out = output_dir / survey / str(end_year)
    out.mkdir(parents=True, exist_ok=True)

    for record_type in record_types:
        for state in states:
            state_lower = state.lower()
            if state_lower not in PUMS_STATE_CODES:
                raise ValueError(f"Invalid state code: {state!r}")
            url = _url_for(survey, end_year, record_type, state_lower)
            path = out / url.split("/")[-1]
            if path.exists():
                print(f"Skip (exists): {path.relative_to(output_dir)}")
                continue
            print(f"Downloading: {path.name}")
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            path.write_bytes(resp.content)
    print(f"Wrote zips to {out}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download Census ACS PUMS zip files (no extraction).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--survey",
        required=True,
        choices=["acs1", "acs5"],
        help="Survey: acs1 (1-year) or acs5 (5-year).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        required=True,
        help="End year (e.g. 2023).",
    )
    parser.add_argument(
        "--state",
        nargs="*",
        default=None,
        help="State(s) as 2-letter codes (e.g. DC RI), or 'all' for all 51. Default: all.",
    )
    parser.add_argument(
        "--record-type",
        choices=["person", "housing", "both"],
        default="both",
        help="Record type to fetch (default: both).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Root directory for zips (e.g. zips); files go to {output_dir}/{survey}/{end_year}/.",
    )
    args = parser.parse_args()

    if args.state and not (
        len(args.state) == 1 and args.state[0].strip().lower() == "all"
    ):
        states = [s.strip().upper() for s in args.state]
        for s in states:
            if s.lower() not in PUMS_STATE_CODES:
                parser.error(f"Invalid state code: {s!r}")
    else:
        states = [s.upper() for s in sorted(PUMS_STATE_CODES)]

    record_types = (
        ["person", "housing"] if args.record_type == "both" else [args.record_type]
    )

    try:
        fetch_pums_zips(
            survey=args.survey,
            end_year=args.end_year,
            states=states,
            record_types=record_types,
            output_dir=args.output_dir.resolve(),
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except requests.RequestException as e:
        print(f"Download error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
