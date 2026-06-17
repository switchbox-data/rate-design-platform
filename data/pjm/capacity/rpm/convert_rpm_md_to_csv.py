#!/usr/bin/env python3
"""Convert committed RPM source intermediates (sources/rpm_YYYY_YY.md) to the CSV.

PJM publishes RPM auction results as per-delivery-year Excel files whose names,
sheet layouts, and paths drift across years, and BRA prices and Final Zonal
prices live in two different files. To keep the dataset reproducible without
re-parsing a zoo of spreadsheets, the Excel->table step is done once and
captured as a committed, reviewable markdown intermediate per delivery year
under ``sources/`` (one per DY). This script is the deterministic, repeatable
half: it parses those intermediates into ``rpm_capacity_prices.csv``.

Each ``sources/rpm_YYYY_YY.md`` carries its own citation header (the Final Zonal
file URL, the BRA results file URL, the as-of date) plus one table of per-zone
``lda / bra_price / final_zonal_price / notes``. The LDA assignment per zone is
the one editorial judgment and lives, reviewable, in that table. Both source
URLs are written onto every CSV row so provenance survives into the parquet.

Workflow: edit/add an intermediate -> ``just convert`` -> review ``git diff`` of
the CSV -> ``just validate`` -> commit -> ``just upload``.

Usage:
    uv run python data/pjm/capacity/rpm/convert_rpm_md_to_csv.py \
        --path-sources data/pjm/capacity/rpm/sources \
        --path-csv data/pjm/capacity/rpm/rpm_capacity_prices.csv
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
# PJM RPM capacity prices: BRA Resource Clearing Prices + Final Zonal Capacity
# Prices, one row per (delivery_year, zone).
#
# GENERATED FILE - do not edit by hand. Regenerate with:
#   just -f data/pjm/capacity/rpm/Justfile convert
# The source of record is the per-DY markdown intermediates under
# sources/rpm_YYYY_YY.md (one citation header + a per-zone price table each).
# To add or revise a DY, edit the intermediate and re-run convert; review the
# git diff of this CSV before committing.
#
# Sources (recorded per DY in each intermediate header and copied to the
# source_url / bra_source_url columns below):
#   - source_url: "<DY> Final Zonal UCAP Obligations, Capacity Prices and CTR
#     Credit Rates" XLS (for DY 2023/24+ the FZSF-FZCP sheet ships inside the
#     "<DY> 3IA Results" XLSX) -> final_zonal_capacity_price_per_mw_day.
#   - bra_source_url: "<DY> Base Residual Auction Results" XLS(X)
#     -> bra_price_per_mw_day (per LDA; copied to each member zone).
#
# As-of: the BRA clears once, so the BRA price's as-of is the fixed BRA posting
# date implied by the DY and is not tracked in its own column; only the Final
# Zonal price (which drifts as IAs settle) carries a final_price_as_of column.
#
# Zones are canonical labels (see data/pjm/README.md crosswalk). UGI is not
# separately reported (inside the PPL LDA); OVEC is excluded (no retail LSE
# load). lda is the most-specific LDA that cleared separately for the zone that
# DY (a per-row attribute; zones at the system price get lda=RTO).
#
# IA true-ups: when an Incremental Auction changes a DY's Final Zonal price,
# update the intermediate, bump final_price_as_of, re-run convert. Git history
# is the revision trail.
"""

COLUMNS = [
    "delivery_year",
    "dy_start",
    "dy_end",
    "zone",
    "lda",
    "bra_price_per_mw_day",
    "final_zonal_capacity_price_per_mw_day",
    "source_url",
    "bra_source_url",
    "final_price_as_of",
    "notes",
]

_FIELD_RE = {
    "delivery_year": re.compile(r"^\*\*Delivery year:\*\*\s*(.+?)\s*$", re.MULTILINE),
    "dy_start": re.compile(r"^\*\*DY start:\*\*\s*(.+?)\s*$", re.MULTILINE),
    "dy_end": re.compile(r"^\*\*DY end:\*\*\s*(.+?)\s*$", re.MULTILINE),
    "source_url": re.compile(r"^\*\*Final zonal source:\*\*\s*(.+?)\s*$", re.MULTILINE),
    "bra_source_url": re.compile(r"^\*\*BRA source:\*\*\s*(.+?)\s*$", re.MULTILINE),
    "final_price_as_of": re.compile(
        r"^\*\*Final price as of:\*\*\s*(.+?)\s*$", re.MULTILINE
    ),
}


@dataclass
class Row:
    delivery_year: str
    dy_start: str
    dy_end: str
    zone: str
    lda: str
    bra_price_per_mw_day: str
    final_zonal_capacity_price_per_mw_day: str
    source_url: str
    bra_source_url: str
    final_price_as_of: str
    notes: str


def _split_table_row(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def _parse_section_rows(text: str, heading: str) -> list[list[str]]:
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.strip().lower().startswith(heading.lower()):
            start = i + 1
            break
    if start is None:
        raise ValueError(f"section {heading!r} not found")

    rows: list[list[str]] = []
    seen_header = False
    for line in lines[start:]:
        stripped = line.strip()
        if stripped.startswith("#"):
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
    fields: dict[str, str] = {}
    for key, rx in _FIELD_RE.items():
        fm = rx.search(text)
        if not fm:
            raise ValueError(f"{path.name}: missing '{key}' header field")
        fields[key] = fm.group(1)

    table = _parse_section_rows(text, "## Zonal prices")
    rows: list[Row] = []
    for cells in table:
        if len(cells) != 5:
            raise ValueError(
                f"{path.name}: expected 5 columns (zone|lda|bra|final|notes), "
                f"got {len(cells)}: {cells}"
            )
        zone, lda, bra, final, notes = cells
        rows.append(
            Row(
                delivery_year=fields["delivery_year"],
                dy_start=fields["dy_start"],
                dy_end=fields["dy_end"],
                zone=zone,
                lda=lda,
                bra_price_per_mw_day=bra,
                final_zonal_capacity_price_per_mw_day=final,
                source_url=fields["source_url"],
                bra_source_url=fields["bra_source_url"],
                final_price_as_of=fields["final_price_as_of"],
                notes=notes,
            )
        )
    return rows


def write_csv(rows: list[Row], path: Path) -> None:
    rows = sorted(rows, key=lambda r: (r.delivery_year, r.zone))
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(COLUMNS)
    for r in rows:
        writer.writerow(
            [
                r.delivery_year,
                r.dy_start,
                r.dy_end,
                r.zone,
                r.lda,
                r.bra_price_per_mw_day,
                r.final_zonal_capacity_price_per_mw_day,
                r.source_url,
                r.bra_source_url,
                r.final_price_as_of,
                r.notes,
            ]
        )
    path.write_text(CSV_HEADER.rstrip("\n") + "\n" + buf.getvalue(), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert RPM markdown intermediates to rpm_capacity_prices.csv."
    )
    parser.add_argument("--path-sources", type=Path, required=True)
    parser.add_argument("--path-csv", type=Path, required=True)
    args = parser.parse_args()

    sources_dir = args.path_sources.resolve()
    if not sources_dir.is_dir():
        print(f"Sources dir not found: {sources_dir}", file=sys.stderr)
        return 1

    md_files = sorted(sources_dir.glob("rpm_*.md"))
    if not md_files:
        print(f"No rpm_*.md intermediates under {sources_dir}", file=sys.stderr)
        return 1

    all_rows: list[Row] = []
    for md in md_files:
        rows = parse_intermediate(md)
        all_rows.extend(rows)
        print(f"  parsed {md.name}: {len(rows)} rows")

    write_csv(all_rows, args.path_csv.resolve())
    dys = sorted({r.delivery_year for r in all_rows})
    print(f"Wrote {len(all_rows)} rows for DYs {dys} -> {args.path_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
