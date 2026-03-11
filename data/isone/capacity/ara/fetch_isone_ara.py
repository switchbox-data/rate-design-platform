#!/usr/bin/env python3
"""Fetch ISO-NE ARA (Annual Reconfiguration Auction) clearing prices → Hive-partitioned parquet.

Downloads ARA results CSVs from ISO Express for each commitment period (CP) and
ARA number (1, 2, 3), parses the multi-section CSV format, and writes to:
    <output_dir>/cp={cp}/ara_number={ara_number}/data.parquet

The ISO Express CSV uses row-type markers: "C" (comment), "H" (header), "D" (data),
"T" (total/count). The CSV contains multiple sections separated by H/T markers:
  1. Zone section   — capacity zones (ROP, Export, Import types)
  2. Interface section — external interconnections
  3+ Summary sections — ISO totals, CSO, aggregate supply/demand (skipped)

Only zone and interface "D" rows are parsed.

Usage:
    uv run python data/isone/capacity/ara/fetch_isone_ara.py \
        --start-cp 2019-20 --end-cp 2032-33 \
        --path-local-parquet data/isone/capacity/ara/parquet
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl
import requests

BASE_URL = "https://www.iso-ne.com/transform/csv/fcmara"
ARA_NUMBERS = [1, 2, 3]

SCHEMA = pl.Schema(
    {
        "cp": pl.String,
        "ara_number": pl.Int8,
        "entity_type": pl.String,
        "capacity_zone_id": pl.Int32,
        "capacity_zone_name": pl.String,
        "entity_name": pl.String,
        "supply_submitted_mw": pl.Float64,
        "demand_submitted_mw": pl.Float64,
        "supply_cleared_mw": pl.Float64,
        "demand_cleared_mw": pl.Float64,
        "net_capacity_cleared_mw": pl.Float64,
        "clearing_price_per_kw_month": pl.Float64,
        "iso_supply_mw": pl.Float64,
        "iso_demand_mw": pl.Float64,
    }
)


def _reject_just_placeholders(val: str) -> None:
    if "{{" in val or "}}" in val:
        print(
            f"ERROR: looks like an uninterpolated Just variable: {val}", file=sys.stderr
        )
        sys.exit(1)


def _cp_range(start: str, end: str) -> list[str]:
    """Generate commitment period strings from start to end inclusive.

    E.g. '2019-20' to '2022-23' → ['2019-20', '2020-21', '2021-22', '2022-23']
    """
    start_year = int(start.split("-")[0])
    end_year = int(end.split("-")[0])
    result = []
    for y in range(start_year, end_year + 1):
        suffix = str(y + 1)[-2:]
        result.append(f"{y}-{suffix}")
    return result


def _fetch_csv(cp: str, ara_number: int) -> str | None:
    """Fetch ARA CSV from ISO Express. Returns text or None if no data rows or on error."""
    url = f"{BASE_URL}?cp={cp}&ara=ARA{ara_number}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException:
        return None
    text = resp.text
    for line in text.splitlines():
        if line.startswith('"D"'):
            return text
    return None


def _parse_float_or_none(val: str) -> float | None:
    """Parse a numeric string, returning None for empty strings."""
    val = val.strip()
    if val == "":
        return None
    return float(val)


def _parse_csv(text: str, cp: str, ara_number: int) -> list[dict[str, object]]:
    """Parse the multi-section ARA CSV into a list of record dicts.

    Tracks sections by header rows: the zone section header starts with
    "Capacity Zone Type" and the interface section starts with "External Interface Name".
    Only parses D rows from those two sections.
    """
    records: list[dict[str, object]] = []
    section: str | None = None  # "zone", "interface", or None (summary sections)

    reader = csv.reader(io.StringIO(text))
    for row in reader:
        if not row:
            continue

        marker = row[0]

        if marker == "H":
            if len(row) > 1 and row[1] == "Capacity Zone Type":
                section = "zone"
            elif len(row) > 1 and row[1] == "External Interface Name":
                section = "interface"
            elif len(row) > 1 and row[1] in ("String", "Number", "MW", "$/kW-month"):
                pass  # unit-description header row, keep current section
            else:
                section = None
            continue

        if marker == "T":
            section = None
            continue

        if marker != "D" or section is None:
            continue

        if section == "zone":
            # Columns: D, Zone Type, Zone ID, Zone Name, Supply Sub, Demand Sub,
            #          Supply Clr, Demand Clr, Net Cap Clr, ISO Supply, ISO Demand,
            #          ISO Supply Clr, ISO Demand Clr, Clearing Price
            records.append(
                {
                    "cp": cp,
                    "ara_number": ara_number,
                    "entity_type": "zone",
                    "capacity_zone_id": int(row[2]),
                    "capacity_zone_name": row[3],
                    "entity_name": row[3],
                    "supply_submitted_mw": float(row[4]),
                    "demand_submitted_mw": float(row[5]),
                    "supply_cleared_mw": float(row[6]),
                    "demand_cleared_mw": float(row[7]),
                    "net_capacity_cleared_mw": float(row[8]),
                    "clearing_price_per_kw_month": float(row[13]),
                    "iso_supply_mw": _parse_float_or_none(row[9]),
                    "iso_demand_mw": _parse_float_or_none(row[10]),
                }
            )

        elif section == "interface":
            # Columns: D, Interface Name, Zone ID, Zone Name, Supply Sub, Demand Sub,
            #          Supply Clr, Demand Clr, Net Cap Clr, ISO Supply, ISO Supply Clr,
            #          Clearing Price
            records.append(
                {
                    "cp": cp,
                    "ara_number": ara_number,
                    "entity_type": "external_interface",
                    "capacity_zone_id": int(row[2]),
                    "capacity_zone_name": row[3],
                    "entity_name": row[1],
                    "supply_submitted_mw": float(row[4]),
                    "demand_submitted_mw": float(row[5]),
                    "supply_cleared_mw": float(row[6]),
                    "demand_cleared_mw": float(row[7]),
                    "net_capacity_cleared_mw": float(row[8]),
                    "clearing_price_per_kw_month": float(row[11]),
                    "iso_supply_mw": _parse_float_or_none(row[9]),
                    "iso_demand_mw": None,
                }
            )

    return records


def _fetch_and_parse(
    cp: str, ara_number: int, output_dir: Path
) -> tuple[str, int, int]:
    """Fetch, parse, and write one CP/ARA partition. Returns (label, rows, skipped)."""
    label = f"CP {cp} ARA{ara_number}"
    part_dir = output_dir / f"cp={cp}" / f"ara_number={ara_number}"
    part_file = part_dir / "data.parquet"

    if part_file.exists():
        return label, 0, 1

    text = _fetch_csv(cp, ara_number)
    if text is None:
        return label, 0, 0

    records = _parse_csv(text, cp, ara_number)
    if not records:
        return label, 0, 0

    df = pl.DataFrame(records, schema=SCHEMA)
    part_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(part_file, compression="snappy")
    return label, len(records), 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ISO-NE ARA clearing prices → parquet",
    )
    parser.add_argument(
        "--start-cp",
        type=str,
        required=True,
        help='First commitment period, e.g. "2019-20"',
    )
    parser.add_argument(
        "--end-cp",
        type=str,
        required=True,
        help='Last commitment period, e.g. "2032-33"',
    )
    parser.add_argument(
        "--path-local-parquet",
        type=str,
        required=True,
        help="Output directory for Hive-partitioned parquet",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel fetch workers (default: 4)",
    )
    args = parser.parse_args()

    _reject_just_placeholders(args.path_local_parquet)
    output_dir = Path(args.path_local_parquet)
    output_dir.mkdir(parents=True, exist_ok=True)

    cps = _cp_range(args.start_cp, args.end_cp)
    tasks = [(cp, ara) for cp in cps for ara in ARA_NUMBERS]

    print(
        f"Fetching ARA data for {len(cps)} CPs × {len(ARA_NUMBERS)} ARAs = {len(tasks)} tasks"
    )
    print(f"Workers: {args.workers}")
    print(f"Output: {output_dir}\n")

    total_rows = 0
    total_written = 0
    total_skipped = 0
    total_empty = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(_fetch_and_parse, cp, ara, output_dir): (cp, ara)
            for cp, ara in tasks
        }
        for future in as_completed(futures):
            label, rows, skipped = future.result()
            if skipped:
                total_skipped += 1
            elif rows > 0:
                total_written += 1
                total_rows += rows
                print(f"  {label}: {rows} rows")
            else:
                total_empty += 1

    print(
        f"\nDone. {total_written} partitions written ({total_rows} rows), "
        f"{total_skipped} skipped (exist), {total_empty} empty (no data)"
    )


if __name__ == "__main__":
    main()
