"""
Fetch UGI Utilities residential "price to compare" (commodity rate) history.

Scrapes the table at https://www.pennsylvaniaenergy.com/pricing-history/, which
publishes a rolling ~24-month history of UGI's price-to-compare (Rider B PGC +
Rider D merchant function + Rider E gas procurement). This is the full commodity
pass-through — the rate that UGI customers buying supply from UGI pay per ccf.

The site is a static HTML page (no JS rendering required). Data is available from
roughly 24 months before today. Requesting dates outside that window raises an error.

Output CSV columns:
    month                    YYYY-MM (e.g. 2025-01)
    price_to_compare_per_ccf $/ccf (e.g. 0.5813)
    source                   URL

The output file is written fresh for the requested date range; any rows outside the
range that already exist in the file are preserved (merge behaviour). If --start is
before the rolling window, a warning is emitted and the fetch is clipped to the
earliest available month — existing rows before that are kept by the merge.

Usage:
    uv run python utils/data_prep/tariffs/fetch_ugi_pgc.py \\
        --start 2025-01 --end 2025-12 \\
        --path-output rate_design/hp_rates/md/config/tariffs/gas/ugi_central_penn_pgc.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
from datetime import date
from html.parser import HTMLParser
from pathlib import Path

import requests

log = logging.getLogger(__name__)

_SOURCE_URL = "https://www.pennsylvaniaenergy.com/pricing-history/"
_MONTH_ABBR = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}
_PRICE_RE = re.compile(r"\$([\d.]+)/[Cc][Cc][Ff]")
_MONTH_RE = re.compile(r"^([A-Za-z]{3})\s+(\d{4})$")


class _TableParser(HTMLParser):
    """Extract (month_str, ugi_price_str) pairs from the pricing-history table."""

    def __init__(self) -> None:
        super().__init__()
        self._in_td = False
        self._cell_text = ""
        self._current_row: list[str] = []
        self.rows: list[tuple[str, str]] = []  # (month_str, ugi_price_str)
        self._in_table = False
        self._table_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._in_table = True
            self._table_depth += 1
        elif tag == "tr" and self._in_table:
            self._current_row = []
        elif tag == "td" and self._in_table:
            self._in_td = True
            self._cell_text = ""

    def handle_endtag(self, tag: str) -> None:
        if tag == "table":
            self._table_depth -= 1
            if self._table_depth == 0:
                self._in_table = False
        elif tag == "tr" and self._in_table:
            if len(self._current_row) >= 3:
                month_str = self._current_row[0].strip()
                ugi_str = self._current_row[2].strip()
                if _MONTH_RE.match(month_str) and _PRICE_RE.search(ugi_str):
                    self.rows.append((month_str, ugi_str))
        elif tag == "td" and self._in_table:
            self._in_td = False
            self._current_row.append(self._cell_text.strip())

    def handle_data(self, data: str) -> None:
        if self._in_td:
            self._cell_text += data


def _parse_month(month_str: str) -> str:
    """Convert 'Jan 2025' -> '2025-01'."""
    m = _MONTH_RE.match(month_str.strip())
    if not m:
        raise ValueError(f"Unrecognised month string: {month_str!r}")
    abbr, year = m.group(1).capitalize(), int(m.group(2))
    month_num = _MONTH_ABBR[abbr]
    return f"{year}-{month_num:02d}"


def _parse_price(price_str: str) -> float:
    """Extract float from '$0.5813/Ccf'."""
    m = _PRICE_RE.search(price_str)
    if not m:
        raise ValueError(f"Unrecognised price string: {price_str!r}")
    return float(m.group(1))


def _ym_to_date(ym: str) -> date:
    """'2025-01' -> date(2025, 1, 1)."""
    year, month = map(int, ym.split("-"))
    return date(year, month, 1)


def fetch_ugi_pgc(
    start: str,
    end: str,
    path_output: Path,
) -> None:
    """
    Fetch UGI price-to-compare for YYYY-MM months in [start, end] (inclusive).

    Raises ValueError if the requested range extends beyond the available data.
    """
    start_date = _ym_to_date(start)
    end_date = _ym_to_date(end)
    if start_date > end_date:
        raise ValueError(f"--start {start} is after --end {end}")

    log.info("Fetching UGI pricing history from %s", _SOURCE_URL)
    resp = requests.get(_SOURCE_URL, timeout=30)
    resp.raise_for_status()

    parser = _TableParser()
    parser.feed(resp.text)

    if not parser.rows:
        raise RuntimeError(
            f"No pricing rows found in {_SOURCE_URL}. "
            "The page structure may have changed."
        )

    # Build dict: YYYY-MM -> price_per_ccf
    scraped: dict[str, float] = {}
    for month_str, price_str in parser.rows:
        ym = _parse_month(month_str)
        scraped[ym] = _parse_price(price_str)

    available_months = sorted(scraped)
    available_start = _ym_to_date(available_months[0])
    available_end = _ym_to_date(available_months[-1])

    log.info(
        "Scraped %d months of data: %s – %s",
        len(available_months),
        available_months[0],
        available_months[-1],
    )

    # Validate / clip the requested range
    if start_date < available_start:
        log.warning(
            "Requested start %s is before the earliest available month (%s). "
            "The site only keeps ~24 months of rolling history. "
            "Clipping to %s; existing rows before that are preserved by merge. "
            "For older data, check PAPUC quarterly filings or the MD PSC "
            "eDocket (Case No. 9516).",
            start,
            available_months[0],
            available_months[0],
        )
        start_date = available_start
    if end_date > available_end:
        log.warning(
            "Requested end %s is after the latest available month (%s). "
            "Clipping to %s; the current month may not yet be published.",
            end,
            available_months[-1],
            available_months[-1],
        )
        end_date = available_end

    # Collect months in range
    new_rows: dict[str, float] = {
        ym: price
        for ym, price in scraped.items()
        if start_date <= _ym_to_date(ym) <= end_date
    }
    log.info(
        "Fetched %d months in available range %s – %s",
        len(new_rows),
        available_months[0],
        end,
    )

    # Merge with existing file (preserve rows outside requested range)
    existing: dict[str, dict[str, str]] = {}
    if path_output.exists():
        with path_output.open(newline="") as f:
            for row in csv.DictReader(f):
                existing[row["month"]] = row

    # Build merged output: existing rows outside range + new rows in range
    merged: dict[str, dict[str, str]] = {}
    for ym, row in existing.items():
        if not (start_date <= _ym_to_date(ym) <= end_date):
            merged[ym] = row
    for ym, price in new_rows.items():
        merged[ym] = {
            "month": ym,
            "price_to_compare_per_ccf": str(price),
            "source": _SOURCE_URL,
        }

    path_output.parent.mkdir(parents=True, exist_ok=True)
    with path_output.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["month", "price_to_compare_per_ccf", "source"],
        )
        writer.writeheader()
        for ym in sorted(merged):
            writer.writerow(merged[ym])

    log.info("Wrote %d rows to %s", len(merged), path_output)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(
        description=(
            "Fetch UGI Utilities residential price-to-compare (commodity rate) history "
            "from pennsylvaniaenergy.com and write to CSV."
        )
    )
    parser.add_argument(
        "--start",
        required=True,
        metavar="YYYY-MM",
        help="First month to fetch (inclusive). Clipped to earliest available if before the ~24-month rolling window.",
    )
    parser.add_argument(
        "--end",
        required=True,
        metavar="YYYY-MM",
        help="Last month to fetch (inclusive). Clipped to latest available if the current month has not yet been published.",
    )
    parser.add_argument(
        "--path-output",
        required=True,
        type=Path,
        metavar="PATH",
        help="Output CSV file path (merged with existing rows outside the requested range).",
    )
    args = parser.parse_args()

    fetch_ugi_pgc(
        start=args.start,
        end=args.end,
        path_output=args.path_output,
    )


if __name__ == "__main__":
    main()
