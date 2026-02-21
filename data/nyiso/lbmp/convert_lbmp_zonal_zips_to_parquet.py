#!/usr/bin/env python3
"""Convert NYISO zonal LBMP ZIPs to partitioned Parquet (zone → year → month).

Reads ZIPs from path_local_zip (subdirs day_ahead/ and real_time/), normalizes
CSV headers, assigns canonical column names and types, groups by zone, and
writes one data.parquet per (zone, year, month) under path_local_parquet.

Usage:
    uv run python data/nyiso/lbmp/convert_lbmp_zonal_zips_to_parquet.py \\
        --path-local-zip /path/to/zips --path-local-parquet /path/to/parquet
    uv run python ... --start 2024-07 --end 2024-12  # only convert those months
"""

from __future__ import annotations

import argparse
import io
import zipfile
from pathlib import Path

import polars as pl

# Canonical output schema (lowercase)
CANONICAL_COLUMNS = [
    "interval_start_est",
    "zone",
    "ptid",
    "lbmp_usd_per_mwh",
    "marginal_cost_losses_usd_per_mwh",
    "marginal_cost_congestion_usd_per_mwh",
]

# Raw header (after normalization) -> canonical name
RAW_TO_CANONICAL = {
    "Time Stamp": "interval_start_est",
    "Name": "zone",
    "PTID": "ptid",
    "LBMP ($/MWHr)": "lbmp_usd_per_mwh",
    "Marginal Cost Losses ($/MWHr)": "marginal_cost_losses_usd_per_mwh",
    "Marginal Cost Congestion ($/MWHr)": "marginal_cost_congestion_usd_per_mwh",
}

SERIES_DIRS = ("day_ahead", "real_time")

# Historical zone name misspellings in NYISO CSVs -> canonical name (for partition and zone column)
ZONE_NAME_NORMALIZE: dict[str, str] = {
    "CENTRL": "CENTRAL",
}


def normalize_header(raw: list[str]) -> list[str]:
    """Strip whitespace, \\r, trailing quote; fix typo $/MWH\" -> $/MWHr."""
    out = [c.strip().rstrip("\r").strip('"') for c in raw]
    if (
        out
        and "Marginal Cost Congestion ($/MWH" in out[-1]
        and not out[-1].endswith("r")
    ):
        out[-1] = "Marginal Cost Congestion ($/MWHr)"
    return out


def read_csv_from_bytes(data: bytes) -> pl.DataFrame:
    """Parse one CSV (from a daily file inside a zip) with normalized headers."""
    lines = data.decode("utf-8", errors="replace").splitlines()
    if not lines or len(lines) < 2:
        return pl.DataFrame()
    raw_header = lines[0].split(",")
    header = normalize_header(raw_header)
    rename = {h: RAW_TO_CANONICAL[h] for h in header if h in RAW_TO_CANONICAL}
    if len(rename) < len(CANONICAL_COLUMNS):
        return pl.DataFrame()
    body = "\n".join(lines[1:])
    df = pl.read_csv(
        io.BytesIO(body.encode("utf-8")),
        has_header=False,
        new_columns=list(header),
        infer_schema_length=0,
    )
    df = df.rename(rename)
    keep = [c for c in CANONICAL_COLUMNS if c in df.columns]
    df = df.select(keep)
    return df


def parse_timestamp_and_types(df: pl.DataFrame) -> pl.DataFrame:
    """Parse interval_start_est and set dtypes for parquet."""
    if df.is_empty():
        return df
    if "interval_start_est" in df.columns:
        # NYISO CSVs vary: some have %H:%M:%S, older ones have %H:%M. Use earliest for DST-ambiguous times.
        ts = pl.col("interval_start_est")
        with_sec = ts.str.to_datetime(
            format="%m/%d/%Y %H:%M:%S",
            time_zone="America/New_York",
            strict=False,
            ambiguous="earliest",
        )
        without_sec = ts.str.to_datetime(
            format="%m/%d/%Y %H:%M",
            time_zone="America/New_York",
            strict=False,
            ambiguous="earliest",
        )
        df = df.with_columns(
            pl.coalesce(with_sec, without_sec).alias("interval_start_est")
        )
    if "ptid" in df.columns:
        df = df.with_columns(pl.col("ptid").cast(pl.Int32))
    for c in (
        "lbmp_usd_per_mwh",
        "marginal_cost_losses_usd_per_mwh",
        "marginal_cost_congestion_usd_per_mwh",
    ):
        if c in df.columns:
            df = df.with_columns(pl.col(c).cast(pl.Float64))
    return df


def month_key_from_zip_path(zip_path: Path) -> str | None:
    """Extract YYYYMM from zip filename like 20240101damlbmp_zone_csv.zip."""
    name = zip_path.stem
    # 20240101damlbmp_zone_csv -> 202401
    if len(name) >= 6 and name[:6].isdigit():
        return name[:6]
    return None


def convert_one_zip(zip_path: Path, series: str) -> pl.DataFrame | None:
    """Read one monthly zip, concatenate all daily CSVs, return one DataFrame."""
    dfs: list[pl.DataFrame] = []
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            for name in sorted(z.namelist()):
                if not name.endswith(".csv"):
                    continue
                with z.open(name) as f:
                    data = f.read()
                df = read_csv_from_bytes(data)
                if not df.is_empty():
                    dfs.append(df)
    except (zipfile.BadZipFile, KeyError) as e:
        print(f"Warning: {zip_path}: {e}")
        return None
    if not dfs:
        return None
    out = pl.concat(dfs)
    out = parse_timestamp_and_types(out)
    # Normalize zone names: fix misspellings (CENTRL -> CENTRAL), then spaces -> _ for partition paths
    if "zone" in out.columns:
        if ZONE_NAME_NORMALIZE:
            out = out.with_columns(
                pl.col("zone").replace(ZONE_NAME_NORMALIZE).alias("zone")
            )
        out = out.with_columns(pl.col("zone").str.replace_all(" ", "_").alias("zone"))
    return out


def convert_month(
    path_local_zip: Path,
    path_local_parquet: Path,
    series: str,
    yyyy_mm: str,
) -> None:
    """Convert one (series, month) and write zone-partitioned parquet."""
    yyyy, mm = yyyy_mm[:4], yyyy_mm[4:6]
    zip_dir = path_local_zip / series
    suffix = (
        "damlbmp_zone_csv.zip" if series == "day_ahead" else "realtime_zone_csv.zip"
    )
    # Match fetch naming: YYYYMM01_suffix (e.g. 20000101_damlbmp_zone_csv.zip)
    zip_name = f"{yyyy}{mm}01_{suffix}"
    zip_path = zip_dir / zip_name
    if not zip_path.exists():
        return
    df = convert_one_zip(zip_path, series)
    if df is None or df.is_empty():
        return
    # Group by zone, write one parquet per zone (no "zones" subdir; upload syncs to s3 .../zones/)
    for zone_name in df["zone"].unique().to_list():
        zone_str = str(zone_name).strip()
        if not zone_str:
            continue
        sub = df.filter(pl.col("zone") == zone_name)
        out_dir = (
            path_local_parquet
            / series
            / f"zone={zone_str}"
            / f"year={yyyy}"
            / f"month={mm}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"
        sub.write_parquet(out_path)


def list_months_in_zips(path_local_zip: Path, series: str) -> list[str]:
    """List YYYYMM for which we have a zip in path_local_zip/series/."""
    months: list[str] = []
    zip_dir = path_local_zip / series
    if not zip_dir.is_dir():
        return months
    suffix = (
        "damlbmp_zone_csv.zip" if series == "day_ahead" else "realtime_zone_csv.zip"
    )
    for p in zip_dir.glob(f"*{suffix}"):
        key = month_key_from_zip_path(p)
        if key:
            months.append(key)
    return sorted(set(months))


def filter_months(
    months: list[str], start_yyyy_mm: str | None, end_yyyy_mm: str | None
) -> list[str]:
    """Restrict to [start, end] inclusive (YYYYMM format)."""
    if start_yyyy_mm:
        start = start_yyyy_mm.replace("-", "")  # 2024-07 -> 202407
        months = [m for m in months if m >= start]
    if end_yyyy_mm:
        end = end_yyyy_mm.replace("-", "")
        months = [m for m in months if m <= end]
    return months


def _reject_just_placeholders(val: str) -> None:
    if "{{" in val or "}}" in val:
        raise ValueError(
            f"Path looks like uninterpolated Just: {val!r}. Pass a resolved path."
        )


def convert(
    path_local_zip: Path,
    path_local_parquet: Path,
    start_yyyy_mm: str | None = None,
    end_yyyy_mm: str | None = None,
) -> None:
    path_local_zip = path_local_zip.resolve()
    path_local_parquet = path_local_parquet.resolve()
    _reject_just_placeholders(str(path_local_zip))
    _reject_just_placeholders(str(path_local_parquet))
    path_local_parquet.mkdir(parents=True, exist_ok=True)

    all_months: set[str] = set()
    for series in SERIES_DIRS:
        all_months.update(list_months_in_zips(path_local_zip, series))
    months = sorted(all_months)
    months = filter_months(months, start_yyyy_mm, end_yyyy_mm)

    for series in SERIES_DIRS:
        series_months = list_months_in_zips(path_local_zip, series)
        series_months = filter_months(series_months, start_yyyy_mm, end_yyyy_mm)
        for yyyy_mm in series_months:
            convert_month(path_local_zip, path_local_parquet, series, yyyy_mm)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert NYISO zonal LBMP ZIPs to zone/year/month partitioned Parquet."
    )
    parser.add_argument(
        "--path-local-zip",
        type=Path,
        required=True,
        help="Local directory containing day_ahead/ and real_time/ ZIPs.",
    )
    parser.add_argument(
        "--path-local-parquet",
        type=Path,
        required=True,
        help="Local directory for parquet output (series/zones/zone=Z/year=Y/month=M/).",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        metavar="YYYY-MM",
        help="Start month (inclusive). Default: convert all months present in zips.",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        metavar="YYYY-MM",
        help="End month (inclusive). Default: convert all months present in zips.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    convert(
        path_local_zip=args.path_local_zip,
        path_local_parquet=args.path_local_parquet,
        start_yyyy_mm=args.start,
        end_yyyy_mm=args.end,
    )


if __name__ == "__main__":
    main()
