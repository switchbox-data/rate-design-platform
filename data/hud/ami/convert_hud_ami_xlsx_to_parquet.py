#!/usr/bin/env python3
"""
Convert HUD Section 8 Income Limits Excel files to canonical Parquet.

Reads Section8-FY*.xlsx from the input directory (each file first sheet only),
maps columns to a single canonical schema (handling year-to-year name differences),
and writes one Parquet file per fiscal year under output/fy={year}/data.parquet.

Use pl.scan_parquet() when reading the output elsewhere for predicate pushdown.

    uv run python convert_hud_ami_xlsx_to_parquet.py --input xlsx/ --output parquet/
"""

import argparse
import re
import sys
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

CANONICAL_COLUMNS = [
    "fy",
    "state_fips",
    "state_abbr",
    "state_name",
    "county_fips",
    "county_name",
    "fips",
    "hud_area_code",
    "hud_area_name",
    "msa",
    "county_town_name",
    "metro",
    "median_income",
    *[f"l50_{i}" for i in range(1, 9)],
    *[f"eli_{i}" for i in range(1, 9)],
    *[f"l80_{i}" for i in range(1, 9)],
]

# Possible source column names per canonical name (lowercase); first match wins.
COLUMN_ALIASES: dict[str, list[str]] = {
    "state_fips": ["state", "state_fips"],
    "state_abbr": ["stusps", "state_alpha"],
    "state_name": ["state_name"],
    "county_fips": ["county", "county_fips"],
    "county_name": ["county_name"],
    "fips": ["fips", "fips2010"],
    "hud_area_code": ["hud_area_code"],
    "hud_area_name": ["hud_area_name"],
    "msa": ["msa", "cbsasub"],
    "county_town_name": ["county_town_name"],
    "metro": ["metro", "metro_area_name"],
    "median_income": [],  # filled per year: median{year}
    **{f"l50_{i}": [f"l50_{i}"] for i in range(1, 9)},
    **{f"eli_{i}": [f"eli_{i}", f"eli_{i}".upper()] for i in range(1, 9)},
    **{f"l80_{i}": [f"l80_{i}"] for i in range(1, 9)},
}


def _infer_fy_from_path(path: Path) -> int | None:
    """Extract fiscal year from filename like Section8-FY2023.xlsx."""
    m = re.search(r"Section8-FY(\d{4})\.xlsx", path.name, re.IGNORECASE)
    return int(m.group(1)) if m else None


def _normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Lowercase column names for consistent matching."""
    return df.rename({c: c.strip().lower() for c in df.columns})


def _map_to_canonical(df: pl.DataFrame, fy: int) -> pl.DataFrame:
    """Map source columns to canonical schema; add fy; fill missing with null."""
    df = _normalize_columns(df)
    aliases = {k: list(v) for k, v in COLUMN_ALIASES.items()}
    aliases["median_income"] = [f"median{fy}"]

    rename_map: dict[str, str] = {}
    for canon in CANONICAL_COLUMNS:
        if canon == "fy":
            continue
        for candidate in aliases.get(canon, []):
            if candidate in df.columns:
                rename_map[candidate] = canon
                break

    df = df.rename(rename_map)
    # Add fy
    df = df.with_columns(pl.lit(fy).cast(pl.Int32).alias("fy"))
    # Select canonical order; missing cols become null
    existing = [c for c in CANONICAL_COLUMNS if c in df.columns]
    missing = [c for c in CANONICAL_COLUMNS if c not in df.columns]
    out = df.select(existing)
    for c in missing:
        out = out.with_columns(pl.lit(None).alias(c))
    out = out.select(CANONICAL_COLUMNS)
    # Cast to consistent types so schema matches across years (for scan_parquet)
    str_cols = [
        "state_fips",
        "state_abbr",
        "state_name",
        "county_fips",
        "county_name",
        "fips",
        "hud_area_code",
        "hud_area_name",
        "msa",
        "county_town_name",
        "metro",
    ]
    for col in str_cols:
        if col in out.columns:
            out = out.with_columns(pl.col(col).cast(pl.Utf8))
    num_cols = (
        ["median_income"]
        + [f"l50_{i}" for i in range(1, 9)]
        + [f"eli_{i}" for i in range(1, 9)]
        + [f"l80_{i}" for i in range(1, 9)]
    )
    for col in num_cols:
        if col in out.columns:
            out = out.with_columns(pl.col(col).cast(pl.Float64))
    return out


def convert_file(xlsx_path: Path, output_base: Path) -> Path | None:
    """Read one xlsx, map to canonical schema, write parquet/fy={year}/data.parquet."""
    fy = _infer_fy_from_path(xlsx_path)
    if fy is None:
        return None
    df = pl.read_excel(xlsx_path)
    df = _map_to_canonical(df, fy)
    table = df.to_arrow()
    # Ensure fy is plain int32 (not dictionary) for consistent scan_parquet across years
    fields = [pa.field("fy", pa.int32()) if f.name == "fy" else f for f in table.schema]
    table = table.cast(pa.schema(fields))
    partition_dir = output_base / f"fy={fy}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    out_file = partition_dir / "data.parquet"
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
        sys.exit(1)

    xlsx_files = sorted(input_dir.glob("Section8-FY*.xlsx"))
    if not xlsx_files:
        xlsx_files = sorted(input_dir.glob("*.xlsx"))
    if not xlsx_files:
        print(f"Error: No xlsx files found in {input_dir}", file=sys.stderr)
        sys.exit(1)

    print("=" * 80)
    print("HUD INCOME LIMITS XLSX TO PARQUET CONVERTER")
    print("=" * 80)
    print(f"\nInput directory:  {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Total xlsx files: {len(xlsx_files)}")
    print("\nPartitioning: fy=<year>/data.parquet")
    print("Compression: Snappy")
    print()

    failed: list[tuple[str, str]] = []
    success_paths: list[Path] = []

    for path in tqdm(xlsx_files, desc="Converting", unit="file"):
        try:
            out_path = convert_file(path, output_dir)
            if out_path is not None:
                success_paths.append(out_path)
            else:
                failed.append((path.name, "Could not infer FY from filename"))
        except Exception as e:
            failed.append((path.name, str(e)))

    print()
    print("=" * 80)
    print("CONVERSION COMPLETE")
    print("=" * 80)
    successful = len(success_paths)
    print(f"\nSuccessful: {successful}/{len(xlsx_files)} files")
    if success_paths:
        for p in sorted(success_paths):
            nrows = pq.ParquetFile(str(p)).metadata.num_rows
            print(f"  {p.relative_to(output_dir)}  ({nrows} rows)")
    if failed:
        print(f"\nFailed files ({len(failed)}):")
        for name, err in failed[:10]:
            print(f"  - {name}: {err}")
        if len(failed) > 10:
            print(f"  ... and {len(failed) - 10} more")
        return 1
    total_size = sum(f.stat().st_size for f in output_dir.rglob("*.parquet"))
    print(f"\nTotal Parquet size: {total_size / (1024**2):.2f} MB")
    print(f"\n✓ Parquet files written to: {output_dir}")
    return 0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Convert HUD Section 8 Income Limits xlsx to partitioned Parquet"
    )
    p.add_argument(
        "--input",
        "-i",
        metavar="DIR",
        default="xlsx",
        help="Directory containing Section8-FY*.xlsx files (default: xlsx)",
    )
    p.add_argument(
        "--output",
        "-o",
        metavar="DIR",
        default="parquet",
        help="Output directory for partitioned Parquet (default: parquet)",
    )
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(main())
