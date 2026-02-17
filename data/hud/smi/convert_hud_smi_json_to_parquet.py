#!/usr/bin/env python3
"""
Convert HUD SMI JSON (from fetch_smi_json.py) to Parquet.

Reads json/states.json and json/fy={year}/*.json. Output schema is a subset of AMI:
same column names and data types as AMI where they overlap, but only columns SMI
has data for (no empty area-level columns). Columns: fy, state_fips, state_abbr,
state_name, median_income, l50_1..l50_8, eli_1..eli_8, l80_1..l80_8.

Writes parquet/fy={year}/data.parquet. Use pl.scan_parquet() for predicate pushdown.

    uv run python convert_hud_smi_json_to_parquet.py --input json/ --output parquet/
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# Subset of AMI columns that SMI has; same names and types as AMI for consistency
SMI_COLUMNS = [
    "fy",
    "state_fips",
    "state_abbr",
    "state_name",
    "median_income",
    *[f"l50_{i}" for i in range(1, 9)],
    *[f"eli_{i}" for i in range(1, 9)],
    *[f"l80_{i}" for i in range(1, 9)],
]


def _load_states(input_dir: Path) -> tuple[dict[str, str], dict[str, str]]:
    """Load states.json; return (state_code -> state_fips, state_code -> state_name)."""
    path = input_dir / "states.json"
    if not path.exists():
        return {}, {}
    with open(path) as f:
        states = json.load(f)
    fips_map: dict[str, str] = {}
    name_map: dict[str, str] = {}
    for s in states:
        code = s.get("state_code") or s.get("statecode") or ""
        if not code:
            continue
        name_map[code] = s.get("state_name") or ""
        num = s.get("state_num")
        if num is not None and str(num).strip():
            try:
                n = int(float(str(num)))
                fips_map[code] = str(n).zfill(2)
            except (ValueError, TypeError):
                fips_map[code] = ""
        else:
            fips_map[code] = ""
    return fips_map, name_map


def _one_row(
    state_code: str, state_fips: str, state_name: str, fy: int, data: dict
) -> dict:
    """Build one row in canonical shape; API data is il/statedata response."""
    year = data.get("year")
    if isinstance(year, str):
        year = int(year) if year.isdigit() else fy
    median = data.get("median_income")
    very = data.get("very_low") or {}
    extremely = data.get("extremely_low") or {}
    low = data.get("low") or {}

    row: dict[str, object] = {
        "fy": fy,
        "state_fips": state_fips or None,
        "state_abbr": state_code,
        "state_name": state_name or None,
        "median_income": float(median) if median is not None else None,
    }
    for i in range(1, 9):
        row[f"l50_{i}"] = _num(very.get(f"il50_p{i}"))
        row[f"eli_{i}"] = _num(extremely.get(f"il30_p{i}"))
        row[f"l80_{i}"] = _num(low.get(f"il80_p{i}"))
    return row


def _num(x: object) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float, str)):
        try:
            return float(x)
        except (TypeError, ValueError):
            return None
    return None


def convert_year(
    input_dir: Path,
    output_dir: Path,
    fy: int,
    state_code_to_fips: dict[str, str],
    state_code_to_name: dict[str, str],
) -> Path | None:
    """Read all json/fy={fy}/*.json, build one DataFrame, write parquet/fy={fy}/data.parquet."""
    year_dir = input_dir / f"fy={fy}"
    if not year_dir.is_dir():
        return None
    json_files = list(year_dir.glob("*.json"))
    if not json_files:
        return None

    rows: list[dict] = []
    for path in json_files:
        state_code = path.stem
        state_fips = state_code_to_fips.get(state_code, "")
        state_name = state_code_to_name.get(state_code, "")
        with open(path) as f:
            raw = json.load(f)
        data = raw.get("data") if isinstance(raw, dict) else raw
        if not isinstance(data, dict):
            continue
        rows.append(_one_row(state_code, state_fips, state_name, fy, data))

    if not rows:
        return None
    df = pl.DataFrame(rows)
    # Cast to match AMI types (same names/types as AMI for overlapping columns)
    df = df.with_columns(pl.col("fy").cast(pl.Int32))
    for c in ("state_fips", "state_abbr", "state_name"):
        if c in df.columns:
            df = df.with_columns(pl.col(c).cast(pl.Utf8))
    num_cols = (
        ["median_income"]
        + [f"l50_{i}" for i in range(1, 9)]
        + [f"eli_{i}" for i in range(1, 9)]
        + [f"l80_{i}" for i in range(1, 9)]
    )
    for c in num_cols:
        if c in df.columns:
            df = df.with_columns(pl.col(c).cast(pl.Float64))
    df = df.select(SMI_COLUMNS)

    out_partition = output_dir / f"fy={fy}"
    out_partition.mkdir(parents=True, exist_ok=True)
    out_file = out_partition / "data.parquet"
    table = df.to_arrow()
    fields = [pa.field("fy", pa.int32()) if f.name == "fy" else f for f in table.schema]
    table = table.cast(pa.schema(fields))
    pq.write_table(
        table,
        out_file,
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
    )
    return out_file


def main() -> int:
    args = _parse_args()
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    if not input_dir.exists():
        print(f"Error: Input directory not found: {input_dir}", file=sys.stderr)
        return 1

    state_code_to_fips, state_code_to_name = _load_states(input_dir)
    fy_dirs = sorted(
        d for d in input_dir.iterdir() if d.is_dir() and d.name.startswith("fy=")
    )
    if not fy_dirs:
        print("Error: No fy=<year> directories found under input.", file=sys.stderr)
        return 1

    written: list[Path] = []
    for d in tqdm(fy_dirs, desc="Converting", unit="year"):
        try:
            fy_str = d.name.replace("fy=", "")
            fy = int(fy_str)
        except ValueError:
            continue
        path = convert_year(
            input_dir, output_dir, fy, state_code_to_fips, state_code_to_name
        )
        if path:
            written.append(path)

    if not written:
        print("Error: No parquet files written.", file=sys.stderr)
        return 1
    print(f"Wrote {len(written)} partition(s) under {output_dir.absolute()}")
    return 0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Convert HUD SMI JSON to AMI-aligned Parquet"
    )
    p.add_argument(
        "--input",
        "-i",
        metavar="DIR",
        default="json",
        help="Input directory containing states.json and fy=<year>/ (default: json)",
    )
    p.add_argument(
        "--output",
        "-o",
        metavar="DIR",
        default="parquet",
        help="Output directory for parquet/fy=<year>/data.parquet (default: parquet)",
    )
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(main())
