#!/usr/bin/env python3
"""Fetch PJM NITS rate PDFs from the PJM website.

Downloads NITS rate PDFs for specified years to a local cache directory. PDFs are
not committed to git; they're fetched on-demand for extraction or verification.

Usage:
    uv run python data/pjm/bulk_tx/nits/fetch_nits_pdfs.py --years 2021 2022 2023 2024 2025
    uv run python data/pjm/bulk_tx/nits/fetch_nits_pdfs.py --years 2021-2025
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests

# Known URL patterns for NITS rate PDFs (PJM has changed patterns over time)
URL_PATTERNS = [
    "https://www.pjm.com/-/media/DotCom/markets-ops/settlements/network-integration-trans-service-jan-{year}.pdf",
    "https://www.pjm.com/-/media/DotCom/markets-ops/settlements/network-integration-trans-service-june-{year}.pdf",
    "https://www.pjm.com/-/media/DotCom/markets-ops/settlements/network-integration-trans-service-{year}.pdf",
]


def fetch_pdf(url: str, output_path: Path, timeout: int = 30) -> bool:
    """Fetch a PDF from URL and save to output_path. Return True if successful."""
    try:
        print(f"  Fetching {url}...")
        response = requests.get(url, timeout=timeout, allow_redirects=True)

        # Check if response is actually a PDF (not a 404 HTML page)
        content_type = response.headers.get("content-type", "").lower()
        if "application/pdf" not in content_type and response.content[:4] != b"%PDF":
            print(f"    ✗ Not a PDF (got {content_type})")
            return False

        if response.status_code == 200:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(response.content)
            size_kb = len(response.content) / 1024
            print(f"    ✓ Saved ({size_kb:.1f} KB)")
            return True
        else:
            print(f"    ✗ HTTP {response.status_code}")
            return False

    except requests.exceptions.Timeout:
        print(f"    ✗ Timeout after {timeout}s")
        return False
    except requests.exceptions.RequestException as e:
        print(f"    ✗ Error: {e}")
        return False


def fetch_year(year: int, output_dir: Path) -> dict[str, bool]:
    """Fetch Jan and Jun PDFs for a given year. Return dict of {period: success}."""
    results = {}

    # Try Jan PDF
    jan_path = output_dir / f"nits_jan_{year}.pdf"
    if jan_path.exists():
        print(f"  Jan {year}: already exists, skipping")
        results["jan"] = True
    else:
        jan_url = URL_PATTERNS[0].format(year=year)
        results["jan"] = fetch_pdf(jan_url, jan_path)

    # Try Jun PDF
    jun_path = output_dir / f"nits_jun_{year}.pdf"
    if jun_path.exists():
        print(f"  Jun {year}: already exists, skipping")
        results["jun"] = True
    else:
        jun_url = URL_PATTERNS[1].format(year=year)
        results["jun"] = fetch_pdf(jun_url, jun_path)

    # If both failed, try the combined annual PDF
    if not results["jan"] and not results["jun"]:
        annual_path = output_dir / f"nits_{year}.pdf"
        if not annual_path.exists():
            annual_url = URL_PATTERNS[2].format(year=year)
            if fetch_pdf(annual_url, annual_path):
                print(f"    → Combined annual PDF saved as nits_{year}.pdf")
                results["annual"] = True

    return results


def parse_year_range(year_spec: str) -> list[int]:
    """Parse a year spec like '2021-2025' or '2021' into a list of years."""
    if "-" in year_spec:
        start, end = year_spec.split("-")
        return list(range(int(start), int(end) + 1))
    return [int(year_spec)]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PJM NITS rate PDFs for specified years."
    )
    parser.add_argument(
        "--years",
        nargs="+",
        required=True,
        help="Years to fetch (e.g., 2021 2022 or 2021-2025)",
    )
    parser.add_argument(
        "--output-dir",
        default="data/pjm/bulk_tx/nits/_local_pdfs",
        help="Output directory for PDFs (default: data/pjm/bulk_tx/nits/_local_pdfs)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds (default: 30)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse year specs
    years = []
    for spec in args.years:
        years.extend(parse_year_range(spec))
    years = sorted(set(years))

    print(f"Fetching NITS rate PDFs for years: {', '.join(map(str, years))}")
    print(f"Output directory: {output_dir}\n")

    all_results = {}
    for year in years:
        print(f"Year {year}:")
        results = fetch_year(year, output_dir)
        all_results[year] = results

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for year, results in all_results.items():
        status = []
        if results.get("jan"):
            status.append("Jan ✓")
        else:
            status.append("Jan ✗")
        if results.get("jun"):
            status.append("Jun ✓")
        else:
            status.append("Jun ✗")
        if results.get("annual"):
            status.append("Annual ✓")

        print(f"  {year}: {', '.join(status)}")

    failed_count = sum(
        1
        for results in all_results.values()
        if not results.get("jan")
        and not results.get("jun")
        and not results.get("annual")
    )

    if failed_count > 0:
        print(f"\n⚠ {failed_count} year(s) failed to download completely.")
        print(
            "  Older PDFs may no longer be available on PJM's website."
            "\n  Use alternative sources (ETCC table, CAPS Handbook) for missing years."
        )
        sys.exit(1)
    else:
        print("\n✓ All PDFs fetched successfully")


if __name__ == "__main__":
    main()
