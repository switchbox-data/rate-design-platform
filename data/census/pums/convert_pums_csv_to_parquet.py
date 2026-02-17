#!/usr/bin/env python3
"""Convert PUMS CSV tree to partitioned Parquet.

Walks the canonical CSV tree under --input-dir, infers survey/end_year/record_type/state
from each partition path, reads all *.csv in the partition, lowercases column names,
and writes one data.parquet per partition under --output-dir in the same layout.

Usage:
    uv run python data/census/pums/convert_pums_csv_to_parquet.py --input-dir csv --output-dir parquet
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import polars as pl

# 50 states + DC, lowercase (matches Census FTP filenames and EIA861 convention).
PUMS_STATE_CODES: frozenset[str] = frozenset(
    {
        "al",
        "ak",
        "az",
        "ar",
        "ca",
        "co",
        "ct",
        "de",
        "fl",
        "ga",
        "hi",
        "id",
        "il",
        "in",
        "ia",
        "ks",
        "ky",
        "la",
        "me",
        "md",
        "ma",
        "mi",
        "mn",
        "ms",
        "mo",
        "mt",
        "ne",
        "nv",
        "nh",
        "nj",
        "nm",
        "ny",
        "nc",
        "nd",
        "oh",
        "ok",
        "or",
        "pa",
        "ri",
        "sc",
        "sd",
        "tn",
        "tx",
        "ut",
        "vt",
        "va",
        "wa",
        "wv",
        "wi",
        "wy",
        "dc",
    }
)

# Path segment pattern: survey / 4-digit year / record_type / state=XX
_PARTITION_PATTERN = re.compile(
    r"^(acs1|acs5)/(\d{4})/(person|housing)/state=([A-Za-z]{2})$"
)


def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Lowercase all column names (for Parquet output convention)."""
    return df.rename({c: c.lower() for c in df.columns})


def parse_pums_partition_path(path: Path | str) -> tuple[str, int, str, str] | None:
    """Parse a canonical PUMS partition path into (survey, end_year, record_type, state).

    Accepts paths that end with {survey}/{end_year}/{record_type}/state={XX}
    (e.g. .../acs1/2020/person/state=RI). Returns None if the path does not match.

    Returns:
        (survey, end_year, record_type, state) with state uppercase, or None.
    """
    path = Path(path)
    parts = path.parts
    for i in range(len(parts) - 4, -1, -1):
        segment = "/".join(parts[i : i + 4])
        m = _PARTITION_PATTERN.fullmatch(segment)
        if m:
            survey, year_str, record_type, state = m.groups()
            return (survey, int(year_str), record_type, state.upper())
    return None


def run_convert(input_dir: Path, output_dir: Path) -> None:
    """Walk input_dir for canonical partition dirs; read CSVs, lowercase columns, write Parquet."""
    input_dir = input_dir.resolve()
    output_dir = output_dir.resolve()
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    for part_dir in input_dir.rglob("*"):
        if not part_dir.is_dir():
            continue
        parsed = parse_pums_partition_path(part_dir)
        if parsed is None:
            continue
        survey, end_year, record_type, state = parsed
        csv_files = list(part_dir.glob("*.csv"))
        if not csv_files:
            continue
        # Census PUMS has identifier columns (e.g. SERIALNO) that are string; avoid inferring as int.
        # scan_csv with glob reads all matching files and concatenates; Polars parallelizes automatically.
        csv_glob = str(part_dir / "*.csv")
        lf = pl.scan_csv(csv_glob, infer_schema_length=0)
        lf = lf.rename({c: c.lower() for c in lf.collect_schema().names()})
        out_part = output_dir / survey / str(end_year) / record_type / f"state={state}"
        out_part.mkdir(parents=True, exist_ok=True)
        out_file = out_part / "data.parquet"
        lf.sink_parquet(out_file)
        print(f"Wrote {out_file.relative_to(output_dir)}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert PUMS CSV tree to partitioned Parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Root of the canonical CSV tree (output of unzip).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Root for partitioned Parquet output (same layout as input).",
    )
    args = parser.parse_args()

    try:
        run_convert(args.input_dir.resolve(), args.output_dir.resolve())
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
