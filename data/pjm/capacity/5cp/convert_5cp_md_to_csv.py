#!/usr/bin/env python3
"""Convert committed 5CP source intermediates (sources/5cp_YYYY.md) to the CSV.

PJM publishes summer 5CP peaks only as PDFs, and old PDFs get pulled from
pjm.com. To keep the dataset reproducible without parsing live PDFs, the fragile
PDF->text step is done once and captured as a committed, reviewable markdown
intermediate per summer under ``sources/`` (see the repo's
``extract-pdf-to-markdown`` command). This script is the deterministic,
repeatable half: it parses those intermediates into ``fivecp_peaks.csv``.

Each ``sources/5cp_YYYY.md`` carries its own citation header (Source URL, As of,
Notes) plus two tables: the RTO coincident peaks (rank -> date, hour-ending, MW)
and the by-zone unrestricted MW (zone -> rank1..rank5). The citation header is
written onto every CSV row so provenance survives into the parquet on S3.

Note: ``notes`` here is a single file-level value (copied onto every row of the
summer, and may be empty), unlike the RPM dataset where ``notes`` is per-row.

Workflow: edit/add an intermediate -> ``just convert`` -> review ``git diff`` of
the CSV -> ``just validate`` -> commit -> ``just upload``.

Usage:
    uv run python data/pjm/capacity/5cp/convert_5cp_md_to_csv.py \
        --path-sources data/pjm/capacity/5cp/sources \
        --path-csv data/pjm/capacity/5cp/fivecp_peaks.csv
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from dataclasses import dataclass
from pathlib import Path

RANKS = (1, 2, 3, 4, 5)

CSV_HEADER = """\
# PJM summer 5CP peaks: RTO 5 coincident peak hours + zonal unrestricted MW.
#
# GENERATED FILE - do not edit by hand. Regenerate with:
#   just -f data/pjm/capacity/5cp/Justfile convert
# The source of record is the per-summer markdown intermediates under
# sources/5cp_YYYY.md (one citation header + an RTO peaks table + a by-zone MW
# table each). To add or revise a summer, edit the intermediate and re-run
# convert; review the git diff of this CSV before committing.
#
# Source: annual "Summer YYYY Peaks and 5CPs" PDF, PJM Planning > Resource
#   Adequacy > Load Forecast. Each summer's PDF URL + posting/revision date are
#   recorded in the matching sources/5cp_YYYY.md header and copied into the
#   source_url / source_as_of columns below.
#
# MW are unrestricted (metered + load-drop add-backs). Zone labels are
# normalized to canonical (AE->AECO, DAYTON->DAY, DLCo->DUQ, PENLC->PENELEC,
# PPL-EU->PPL, PS->PSEG; see data/pjm/README.md crosswalk). Sub-zone rows
# (EASTON, SMECO, Vineland) and OVEC are excluded.
#
# November revisions: PJM reposts the PDF with restated load-drop add-backs
# ("Revised MM/DD/YYYY"). Timestamps rarely change; MW values do. Update the
# intermediate, bump its As of, re-run convert. Git history is the revision
# trail.
"""

# Canonical column order in the CSV.
COLUMNS = [
    "summer_year",
    "rank",
    "peak_date",
    "hour_ending_ept",
    "zone",
    "mw_unrestricted",
    "source_url",
    "source_as_of",
    "notes",
]

_SUMMER_RE = re.compile(r"5cp_(\d{4})\.md$")
# source_url / source_as_of are required (non-empty); notes is a file-level
# field copied onto every row of the summer and may be left empty.
_FIELD_RE = {
    "source_url": re.compile(r"^\*\*Source:\*\*\s*(.+?)\s*$", re.MULTILINE),
    "source_as_of": re.compile(r"^\*\*As of:\*\*\s*(.+?)\s*$", re.MULTILINE),
    "notes": re.compile(r"^\*\*Notes:\*\*\s*(.*?)\s*$", re.MULTILINE),
}


@dataclass
class Row:
    summer_year: int
    rank: int
    peak_date: str
    hour_ending_ept: str
    zone: str
    mw_unrestricted: str
    source_url: str
    source_as_of: str
    notes: str


def _split_table_row(line: str) -> list[str]:
    """Split a markdown table row into stripped cell strings."""
    return [c.strip() for c in line.strip().strip("|").split("|")]


def _parse_section_rows(text: str, heading: str) -> list[list[str]]:
    """Return data rows (as cell lists) of the markdown table under a heading."""
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
        # Skip the markdown header row and the |---|---| separator row.
        if not seen_header:
            seen_header = True
            continue
        if all(set(c) <= {"-", ":"} and c for c in cells):
            continue
        rows.append(cells)
    return rows


def parse_intermediate(path: Path) -> list[Row]:
    m = _SUMMER_RE.search(path.name)
    if not m:
        raise ValueError(f"cannot parse summer year from filename {path.name!r}")
    summer = int(m.group(1))

    text = path.read_text(encoding="utf-8")
    fields: dict[str, str] = {}
    for key, rx in _FIELD_RE.items():
        fm = rx.search(text)
        if not fm:
            raise ValueError(f"{path.name}: missing '{key}' header field")
        fields[key] = fm.group(1)

    rto_rows = _parse_section_rows(text, "## RTO coincident peaks")
    zone_rows = _parse_section_rows(text, "## Coincident peaks by zone")

    # rank -> (peak_date, hour_ending, rto_mw)
    rto_by_rank: dict[int, tuple[str, str, str]] = {}
    for cells in rto_rows:
        rank, peak_date, he, mw = cells[0], cells[1], cells[2], cells[3]
        rto_by_rank[int(rank)] = (peak_date, he, mw)
    if set(rto_by_rank) != set(RANKS):
        raise ValueError(f"{path.name}: RTO table ranks {sorted(rto_by_rank)} != 1..5")

    # zone -> [mw rank1..rank5]
    zone_mw: dict[str, list[str]] = {}
    for cells in zone_rows:
        zone, mws = cells[0], cells[1:6]
        if len(mws) != 5:
            raise ValueError(f"{path.name}: zone {zone} has {len(mws)} MW values")
        zone_mw[zone] = mws

    rows: list[Row] = []
    for rank in RANKS:
        peak_date, he, rto_mw = rto_by_rank[rank]
        rows.append(Row(summer, rank, peak_date, he, "RTO", rto_mw, **fields))
        for zone in sorted(zone_mw):
            rows.append(
                Row(
                    summer, rank, peak_date, he, zone, zone_mw[zone][rank - 1], **fields
                )
            )
    return rows


def write_csv(rows: list[Row], path: Path) -> None:
    rows = sorted(rows, key=lambda r: (r.summer_year, r.rank, r.zone != "RTO", r.zone))
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(COLUMNS)
    for r in rows:
        writer.writerow(
            [
                str(r.summer_year),
                str(r.rank),
                r.peak_date,
                r.hour_ending_ept,
                r.zone,
                r.mw_unrestricted,
                r.source_url,
                r.source_as_of,
                r.notes,
            ]
        )
    path.write_text(CSV_HEADER.rstrip("\n") + "\n" + buf.getvalue(), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert 5CP markdown intermediates to fivecp_peaks.csv."
    )
    parser.add_argument("--path-sources", type=Path, required=True)
    parser.add_argument("--path-csv", type=Path, required=True)
    args = parser.parse_args()

    sources_dir = args.path_sources.resolve()
    if not sources_dir.is_dir():
        print(f"Sources dir not found: {sources_dir}", file=sys.stderr)
        return 1

    md_files = sorted(sources_dir.glob("5cp_*.md"))
    if not md_files:
        print(f"No 5cp_*.md intermediates under {sources_dir}", file=sys.stderr)
        return 1

    all_rows: list[Row] = []
    for md in md_files:
        rows = parse_intermediate(md)
        all_rows.extend(rows)
        print(f"  parsed {md.name}: {len(rows)} rows")

    write_csv(all_rows, args.path_csv.resolve())
    summers = sorted({r.summer_year for r in all_rows})
    print(f"Wrote {len(all_rows)} rows for summers {summers} -> {args.path_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
