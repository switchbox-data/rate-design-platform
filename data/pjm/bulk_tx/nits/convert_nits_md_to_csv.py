#!/usr/bin/env python3
"""Convert committed NITS source intermediates (sources/nits_{year}.md) to the CSV.

PJM publishes NITS rates twice per year (Jan 1 and Jun 1) as PDF documents
whose URLs and table layouts drift across years. To keep the dataset reproducible
without re-parsing a zoo of PDFs, the PDF->table step is done once and captured
as a committed, reviewable markdown intermediate per calendar year under
``sources/`` (one per year). This script is the deterministic, repeatable half:
it parses those intermediates into ``nits_rates.csv``.

Each ``sources/nits_{year}.md`` carries:
  - ``**Calendar year:** YYYY`` header field
  - ``**Jan YYYY source:** <URL>`` header field (URL may have a trailing comment)
  - ``**Jun YYYY source:** <URL>`` header field
  - ``## Jan YYYY rates (effective YYYY-01-01 ...)`` section with a zone table
  - ``## Jun YYYY rates (effective YYYY-06-01 ...)`` section with a zone table

Each table row has columns: zone | transmission_owner | atrr_millions | nits_rate_mw_yr.
The ``nits_rate_kw_yr`` column is derived as ``round(nits_rate_mw_yr / 1000, 2)``.

Workflow: edit/add an intermediate -> ``just convert`` -> review ``git diff`` of
the CSV -> ``just validate`` -> commit.

Usage:
    uv run python data/pjm/bulk_tx/nits/convert_nits_md_to_csv.py \\
        --path-sources data/pjm/bulk_tx/nits/sources \\
        --path-csv data/pjm/bulk_tx/nits/nits_rates.csv
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from dataclasses import dataclass
from pathlib import Path

CSV_HEADER = """\
# PJM NITS rates for MD-relevant transmission zones: BGE, DPL, PEPCO, APS.
# One row per (year, effective_date, zone).
#
# GENERATED FILE - do not edit by hand. Regenerate with:
#   just -f data/pjm/bulk_tx/nits/Justfile convert
# The source of record is the per-year markdown intermediates under
# sources/nits_{year}.md. To add a year, create the intermediate from the
# PJM PDF and re-run convert; review the git diff before committing.
#
# Schema:
#   year             – calendar year this rate applies to
#   effective_date   – date this rate took effect (YYYY-MM-DD)
#   zone             – PJM transmission zone label (BGE, DPL, PEPCO, APS)
#   nits_rate_mw_yr  – Network Integration Transmission Service rate ($/MW-year)
#   nits_rate_kw_yr  – same rate in $/kW-year (= nits_rate_mw_yr / 1000)
#   source_url       – URL of the PJM NITS PDF this row was transcribed from
#
# Blending for calendar-year BAT runs:
#   PJM bills NITS daily (Manual 27 §5.2.2). The correct calendar-year
#   effective rate is day-weighted:
#     blended = (days_jan_rate × jan_rate + days_jun_rate × jun_rate) / days_in_year
#   For a non-leap year: 151 days at Jan rate + 214 days at Jun rate.
#   For a leap year:     152 days at Jan rate + 214 days at Jun rate.
"""

COLUMNS = [
    "year",
    "effective_date",
    "zone",
    "nits_rate_mw_yr",
    "nits_rate_kw_yr",
    "source_url",
]

# Matches "**Jan YYYY source:** https://... (optional comment)"
# or "**Jun YYYY source:** https://..."
_SOURCE_RE = re.compile(
    r"^\*\*(?:Jan|Jun)\s+\d{4}\s+source:\*\*\s*(https?://\S+)",
    re.MULTILINE,
)

_YEAR_RE = re.compile(r"^\*\*Calendar year:\*\*\s*(\d{4})\s*$", re.MULTILINE)

# Matches section headings: "## Jan 2025 rates" or "## Jun 2024 rates"
_SECTION_RE = re.compile(
    r"^##\s+(Jan|Jun)\s+\d{4}\s+rates", re.MULTILINE | re.IGNORECASE
)


@dataclass
class Row:
    year: int
    effective_date: str
    zone: str
    nits_rate_mw_yr: float
    nits_rate_kw_yr: float
    source_url: str


def _split_table_row(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def _parse_table_after_heading(lines: list[str], start: int) -> list[list[str]]:
    """Parse a markdown table starting at line index ``start`` (after the heading)."""
    rows: list[list[str]] = []
    seen_header = False
    for line in lines[start:]:
        stripped = line.strip()
        if stripped.startswith("##") or stripped.startswith("#"):
            break
        if not stripped.startswith("|"):
            if rows or seen_header:
                break
            continue
        cells = _split_table_row(line)
        if not seen_header:
            seen_header = True
            continue
        if all(set(c) <= {"-", ":"} and c for c in cells):
            continue
        rows.append(cells)
    return rows


def parse_intermediate(path: Path) -> list[Row]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    year_m = _YEAR_RE.search(text)
    if not year_m:
        raise ValueError(f"{path.name}: missing '**Calendar year:**' header")
    year = int(year_m.group(1))

    source_urls = _SOURCE_RE.findall(text)
    if len(source_urls) != 2:
        raise ValueError(
            f"{path.name}: expected exactly 2 source URLs (Jan + Jun), "
            f"found {len(source_urls)}: {source_urls}"
        )
    jan_source_url, jun_source_url = source_urls

    # Locate the two section headings in line order
    sections: list[tuple[str, int]] = []
    for i, line in enumerate(lines):
        m = _SECTION_RE.match(line.strip())
        if m:
            sections.append((m.group(1).lower(), i + 1))  # +1: start after heading

    if len(sections) != 2:
        raise ValueError(
            f"{path.name}: expected 2 rate sections (## Jan ... rates, ## Jun ... rates), "
            f"found {len(sections)}"
        )

    all_rows: list[Row] = []
    for period, table_start in sections:
        effective_date = f"{year}-01-01" if period == "jan" else f"{year}-06-01"
        source_url = jan_source_url if period == "jan" else jun_source_url

        table = _parse_table_after_heading(lines, table_start)
        for cells in table:
            if len(cells) < 4:
                raise ValueError(
                    f"{path.name} [{period}]: expected ≥4 columns "
                    f"(zone|transmission_owner|atrr_millions|nits_rate_mw_yr), "
                    f"got {len(cells)}: {cells}"
                )
            zone = cells[0]
            nits_rate_mw_yr = float(cells[3])
            nits_rate_kw_yr = round(nits_rate_mw_yr / 1000, 2)
            all_rows.append(
                Row(
                    year=year,
                    effective_date=effective_date,
                    zone=zone,
                    nits_rate_mw_yr=nits_rate_mw_yr,
                    nits_rate_kw_yr=nits_rate_kw_yr,
                    source_url=source_url,
                )
            )

    return all_rows


def write_csv(rows: list[Row], path: Path) -> None:
    rows = sorted(rows, key=lambda r: (r.year, r.effective_date, r.zone))
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(COLUMNS)
    for r in rows:
        writer.writerow(
            [
                r.year,
                r.effective_date,
                r.zone,
                f"{r.nits_rate_mw_yr:.2f}",
                f"{r.nits_rate_kw_yr:.2f}",
                r.source_url,
            ]
        )
    path.write_text(CSV_HEADER.rstrip("\n") + "\n" + buf.getvalue(), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert NITS markdown intermediates to nits_rates.csv."
    )
    parser.add_argument("--path-sources", type=Path, required=True)
    parser.add_argument("--path-csv", type=Path, required=True)
    args = parser.parse_args()

    sources_dir = args.path_sources.resolve()
    if not sources_dir.is_dir():
        print(f"Sources dir not found: {sources_dir}", file=sys.stderr)
        return 1

    md_files = sorted(sources_dir.glob("nits_*.md"))
    if not md_files:
        print(f"No nits_*.md intermediates under {sources_dir}", file=sys.stderr)
        return 1

    all_rows: list[Row] = []
    for md in md_files:
        rows = parse_intermediate(md)
        all_rows.extend(rows)
        print(f"  parsed {md.name}: {len(rows)} rows")

    write_csv(all_rows, args.path_csv.resolve())
    years = sorted({r.year for r in all_rows})
    print(f"Wrote {len(all_rows)} rows for years {years} -> {args.path_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
