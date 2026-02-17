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
import csv
import re
import sys
from pathlib import Path
from typing import cast

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


def _data_dict_period(survey: str, end_year: int) -> str:
    """Map (survey, end_year) to Census data dict period string."""
    if survey == "acs1":
        return str(end_year)
    if survey == "acs5":
        return f"{end_year - 4}-{end_year}"
    raise ValueError(f"survey must be acs1 or acs5, got {survey!r}")


def _parse_data_dict_csv(path: Path) -> dict[str, tuple[str, int]]:
    """Parse data dict CSV; return {var_name.upper(): (type, len)} for NAME rows only."""
    result: dict[str, tuple[str, int]] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            if len(row) < 4:
                continue
            if row[0] != "NAME":
                continue
            var = row[1].strip().upper()
            dtype = row[2].strip().upper()
            if dtype not in ("C", "N"):
                continue
            try:
                length = int(row[3])
            except ValueError:
                continue
            result[var] = (dtype, length)
    return result


def _numeric_dtype_for_length(length: int) -> type[pl.DataType]:
    """Choose smallest signed int type that fits Census length (digit width).

    Census N/A codes like -666666666 are 9 digits; Int32 handles length ≤ 9.
    """
    if length <= 4:
        return pl.Int16  # max 9999 < 32767
    if length <= 9:
        return pl.Int32  # max 999999999, N/A codes fit
    return pl.Int64


def _build_schema_overrides(
    var_schema: dict[str, tuple[str, int]], columns: list[str]
) -> dict[str, pl.DataType]:
    """Build Polars schema_overrides from var_schema.

    C->Utf8. N->Int16/Int32/Int64 by length (digit width).
    """
    overrides: dict[str, pl.DataType] = {}
    for col in columns:
        key = col.upper()
        if key not in var_schema:
            raise ValueError(f"PUMS column {col!r} not in data dictionary")
        dtype, length = var_schema[key]
        if dtype == "C":
            polars_dtype = pl.Utf8
        else:
            polars_dtype = _numeric_dtype_for_length(length)
        overrides[col] = cast(pl.DataType, polars_dtype)
    return overrides


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


def run_convert(
    input_dir: Path, output_dir: Path, data_dict_cache_dir: Path | None = None
) -> None:
    """Walk input_dir for canonical partition dirs; read CSVs with schema from data dict, write Parquet."""
    input_dir = input_dir.resolve()
    output_dir = output_dir.resolve()
    cache_dir = (
        data_dict_cache_dir.resolve()
        if data_dict_cache_dir
        else input_dir.parent / "data_dict_cache"
    )
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
        csv_glob = str(part_dir / "*.csv")
        period = _data_dict_period(survey, end_year)
        csv_path = (
            cache_dir / survey / str(end_year) / f"PUMS_Data_Dictionary_{period}.csv"
        )
        if csv_path.exists():
            first_schema = pl.scan_csv(csv_files[0], n_rows=0).collect_schema()
            columns = list(first_schema.names())
            var_schema = _parse_data_dict_csv(csv_path)
            schema_overrides = _build_schema_overrides(var_schema, columns)
            print(
                f"data dict found, using it to define parquet schema "
                f"({survey} {end_year} {record_type} state={state})"
            )
            lf = pl.scan_csv(csv_glob, schema_overrides=schema_overrides)
        else:
            print(
                f"data dict not found, inferring parquet schema from csv values "
                f"({survey} {end_year} {record_type} state={state})"
            )
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
    parser.add_argument(
        "--data-dict-cache-dir",
        type=Path,
        default=None,
        help="Data dictionary cache (default: input-dir parent / data_dict_cache).",
    )
    args = parser.parse_args()

    try:
        run_convert(
            args.input_dir.resolve(),
            args.output_dir.resolve(),
            data_dict_cache_dir=args.data_dict_cache_dir,
        )
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
