"""Fetch HHS Federal Poverty Guidelines from the ASPE API and write fpl_guidelines.yaml.

API docs: https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines/poverty-guidelines-api
URL pattern: https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines/api/{YEAR}/{STATE}/{HOUSEHOLD_SIZE}

For each year the script fetches household sizes 1 and 2 to derive base and increment
(base = size-1 threshold; increment = size-2 threshold − base), then verifies the
increment against size 3.  Output is written to utils/post/data/fpl_guidelines.yaml
when --output is passed (e.g. via Justfile).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import requests
import yaml

ASPE_API_BASE = (
    "https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines/api"
)
DEFAULT_STATE = "us"
VERIFY_SIZE = 3


def fetch_threshold(year: int, state: str, household_size: int) -> int:
    """Fetch a single FPL threshold from the ASPE API."""
    url = f"{ASPE_API_BASE}/{year}/{state}/{household_size}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    inner = data.get("data", {})
    # API has returned both "income" and "poverty_threshold" across versions
    value = inner.get("income") or inner.get("poverty_threshold")
    if value is None:
        raise ValueError(
            f"No threshold in ASPE response for {year}/{state}/{household_size}: {data}"
        )
    return int(value)


def fetch_guidelines_for_year(year: int, state: str) -> dict[str, int]:
    """Derive base and increment for a single year, verified against size 3."""
    size1 = fetch_threshold(year, state, 1)
    size2 = fetch_threshold(year, state, 2)
    size3 = fetch_threshold(year, state, VERIFY_SIZE)

    base = size1
    increment = size2 - size1
    expected_size3 = base + (VERIFY_SIZE - 1) * increment
    if size3 != expected_size3:
        raise ValueError(
            f"Increment verification failed for {year}: "
            f"size-1={size1}, size-2={size2}, size-3={size3}, "
            f"expected size-3={expected_size3}"
        )
    return {"base": base, "increment": increment}


def build_yaml(guidelines: dict[int, dict[str, int]]) -> str:
    """Render guidelines dict as YAML with a header comment."""
    lines = [
        "# HHS Federal Poverty Guidelines (annual).",
        "# Source: https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines",
        "# Fetched via ASPE Poverty Guidelines API.",
        "# Formula: FPL_threshold = base + (occupants - 1) * increment",
        "# Values are for the 48 contiguous states and DC.",
        "",
    ]
    for year in sorted(guidelines):
        entry = guidelines[year]
        lines.append(yaml.dump({year: entry}, default_flow_style=False).strip())
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch HHS FPL guidelines from the ASPE API and write fpl_guidelines.yaml."
    )
    parser.add_argument(
        "--start-year",
        type=int,
        required=True,
        help="First guideline year to fetch",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        required=True,
        help="Last guideline year to fetch",
    )
    parser.add_argument(
        "--state",
        default=DEFAULT_STATE,
        choices=["us", "hi", "ak"],
        help=f"State code (default: {DEFAULT_STATE})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to write fpl_guidelines.yaml; omit to print to stdout only",
    )
    args = parser.parse_args()

    if args.start_year > args.end_year:
        parser.error("--start-year must be <= --end-year")

    guidelines: dict[int, dict[str, int]] = {}
    for year in range(args.start_year, args.end_year + 1):
        print(f"Fetching {year} ({args.state}) ... ", end="", flush=True)
        entry = fetch_guidelines_for_year(year, args.state)
        guidelines[year] = entry
        print(f"base={entry['base']}, increment={entry['increment']}")

    output = build_yaml(guidelines)
    print()
    print(output)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output)
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
