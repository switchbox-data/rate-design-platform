#!/usr/bin/env python3
"""Validate NYISO ancillary price parquet: schema, keys, prices, coarse row counts.

Scans local Hive-partitioned parquet under ``path_local_parquet`` (``year=*/month=*/data.parquet``).
Accepts any subset of ``market`` values (``dam`` and/or ``rt``); default fetches are
``rt``-only (5-minute), so partitions may contain only ``market=rt``.

Usage:
    uv run python data/nyiso/ancillary/validate_nyiso_as_prices_parquet.py \\
        --path-local-parquet data/nyiso/ancillary/parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

EXPECTED_MARKETS = {"dam", "rt"}

# Pre-2017 MIS hub-wide rows use these zone labels with null PTID.
_LEGACY_HUB_ZONES = frozenset({"EAST", "WEST", "SENY"})

# RT ``NYCA Regulation Movement`` first appears in MIS archives in 2012-11.
_MOVEMENT_REQUIRED_FROM: tuple[int, int] = (2012, 11)

# Parquet dtypes; categoricals may round-trip as String in some readers — accept both.
_SCHEMA_CORE_REQUIRED = {
    "year": (pl.Int16,),
    "month": (pl.UInt8,),
    "market": (pl.Categorical, pl.String),
    "time_et": (pl.Datetime("us", "America/New_York"),),
    "interval_start_et": (pl.Datetime("us", "America/New_York"),),
    "interval_end_et": (pl.Datetime("us", "America/New_York"),),
    "time_zone": (pl.String,),
    "zone": (pl.String,),
    "ptid": (pl.Int64, pl.Int32),
}

# Standard NYISO AS price columns (present on typical MIS files); optional if ISO adds layout changes.
_SCHEMA_KNOWN_FLOAT_OPTIONAL = {
    "spin_10min_usd_per_mwhr": (pl.Float64,),
    "non_sync_10min_usd_per_mwhr": (pl.Float64,),
    "operating_30min_usd_per_mwhr": (pl.Float64,),
    "nyca_regulation_capacity_usd_per_mwhr": (pl.Float64,),
    "nyca_regulation_movement_usd_per_mw": (pl.Float64,),
}

_ALLOWED_EXTRA_DTYPES = (
    pl.Float64,
    pl.Float32,
    pl.Int64,
    pl.Int32,
    pl.Int16,
    pl.UInt8,
    pl.UInt32,
    pl.String,
    pl.Utf8,
    pl.Boolean,
)

_NON_NULL_CORE = [
    "year",
    "month",
    "market",
    "time_et",
    "interval_start_et",
    "interval_end_et",
    "time_zone",
    "zone",
]

# When these columns exist, require no nulls (DAM may omit movement; fetch backfills as null-only).
_NON_NULL_FLOAT_IF_PRESENT = [
    "spin_10min_usd_per_mwhr",
    "non_sync_10min_usd_per_mwhr",
    "operating_30min_usd_per_mwhr",
    "nyca_regulation_capacity_usd_per_mwhr",
]

_FINITE_FLOAT_IF_PRESENT = [
    "spin_10min_usd_per_mwhr",
    "non_sync_10min_usd_per_mwhr",
    "operating_30min_usd_per_mwhr",
    "nyca_regulation_capacity_usd_per_mwhr",
]


def _collect_parquet_files(root: Path) -> list[Path]:
    return sorted(root.glob("year=*/month=*/data.parquet"))


def _check_file(path: Path) -> list[str]:
    errors: list[str] = []
    df = pl.read_parquet(path)
    for col, allowed in _SCHEMA_CORE_REQUIRED.items():
        if col not in df.columns:
            errors.append(f"{path}: missing column {col}")
            continue
        if df.schema[col] not in allowed:
            errors.append(
                f"{path}: {col} expected one of {allowed}, got {df.schema[col]}",
            )

    for col, allowed in _SCHEMA_KNOWN_FLOAT_OPTIONAL.items():
        if col not in df.columns:
            continue
        if df.schema[col] not in allowed:
            errors.append(
                f"{path}: {col} expected one of {allowed}, got {df.schema[col]}",
            )

    known_and_core = set(_SCHEMA_CORE_REQUIRED) | set(_SCHEMA_KNOWN_FLOAT_OPTIONAL)
    for col in df.columns:
        if col in known_and_core:
            continue
        if df.schema[col] not in _ALLOWED_EXTRA_DTYPES:
            errors.append(
                f"{path}: column {col!r} has unexpected dtype {df.schema[col]} "
                f"(allowed extras: {_ALLOWED_EXTRA_DTYPES})",
            )

    if errors:
        return errors

    for col in _NON_NULL_CORE:
        if df[col].null_count() > 0:
            errors.append(f"{path}: nulls in required column {col}")

    if "ptid" in df.columns:
        bad_ptid = df.filter(
            pl.col("ptid").is_null()
            & ~pl.col("zone").cast(pl.String).is_in(list(_LEGACY_HUB_ZONES))
        )
        if bad_ptid.height > 0:
            errors.append(
                f"{path}: {bad_ptid.height} rows with null ptid outside legacy hubs "
                f"{sorted(_LEGACY_HUB_ZONES)}",
            )

    for col in _NON_NULL_FLOAT_IF_PRESENT:
        if col in df.columns and df[col].null_count() > 0:
            errors.append(f"{path}: nulls in column {col}")

    keys = ["interval_start_et", "interval_end_et", "zone", "market"]
    n_dup = df.height - df.select(keys).unique().height
    if n_dup:
        errors.append(f"{path}: {n_dup} duplicate key rows")

    for col in _FINITE_FLOAT_IF_PRESENT:
        if col not in df.columns:
            continue
        n_bad = df.filter(pl.col(col).is_nan() | pl.col(col).is_infinite()).height
        if n_bad:
            errors.append(f"{path}: {n_bad} NaN/Inf in {col}")

    mk = set(df["market"].cast(pl.String).unique().to_list())
    if not mk <= EXPECTED_MARKETS:
        errors.append(f"{path}: unexpected markets {mk - EXPECTED_MARKETS}")

    for mkt in mk:
        sub = df.filter(pl.col("market").cast(pl.String) == mkt)
        n = sub.height
        if mkt == "dam" and n < 24 * 10:
            errors.append(f"{path}: dam rows suspiciously low ({n})")
        if mkt == "rt" and n < 24 * 10 * 10:
            errors.append(f"{path}: rt rows suspiciously low ({n})")

    # DAM has no movement; RT MIS adds regulation movement from 2012-11 onward.
    y = int(df["year"][0])
    mo = int(df["month"][0])
    rt = df.filter(pl.col("market").cast(pl.String) == "rt")
    if rt.height > 0 and (y, mo) >= _MOVEMENT_REQUIRED_FROM:
        if rt["nyca_regulation_movement_usd_per_mw"].null_count() == rt.height:
            errors.append(f"{path}: all RT regulation movement values are null")

    return errors


def main() -> int:
    import argparse

    p = argparse.ArgumentParser(description="Validate NYISO AS price parquet")
    p.add_argument(
        "--path-local-parquet",
        type=str,
        required=True,
        help="Hive-partitioned parquet root",
    )
    args = p.parse_args()
    root = Path(args.path_local_parquet)
    if not root.is_dir():
        print(f"ERROR: not a directory: {root}", file=sys.stderr)
        return 1

    files = _collect_parquet_files(root)
    if not files:
        print(f"ERROR: no year=*/month=*/data.parquet under {root}", file=sys.stderr)
        return 1

    all_errors: list[str] = []
    for f in files:
        all_errors.extend(_check_file(f))

    if all_errors:
        for e in all_errors:
            print(e, file=sys.stderr)
        return 1

    print(f"OK: {len(files)} partition file(s) under {root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
