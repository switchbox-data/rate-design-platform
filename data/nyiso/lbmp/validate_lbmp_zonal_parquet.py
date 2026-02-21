#!/usr/bin/env python3
"""Validate NYISO zonal LBMP parquet: schema, partition layout, nulls, row counts, zones, ranges.

Runs a full QA battery on local parquet under path_local_parquet (day_ahead and real_time).
Use after convert and before upload.

Usage:
    uv run python data/nyiso/lbmp/validate_lbmp_zonal_parquet.py --path-local-parquet /path/to/parquet
"""

from __future__ import annotations

import argparse
import calendar
import re
import sys
from pathlib import Path

import polars as pl

CANONICAL_COLUMNS = [
    "interval_start_est",
    "zone",
    "ptid",
    "lbmp_usd_per_mwh",
    "marginal_cost_losses_usd_per_mwh",
    "marginal_cost_congestion_usd_per_mwh",
]

# 11 canonical NYISO zonal LBMP zones (underscore in data/partitions, e.g. MHK_VL, HUD_VL).
# Other zones (e.g. H_Q, NPX, O_H, PJM) may appear in source; we allow them and log.
CANONICAL_NYISO_ZONES = frozenset(
    {
        "CAPITL",
        "WEST",
        "GENESE",
        "CENTRAL",
        "NORTH",
        "MHK_VL",
        "HUD_VL",
        "MILLWD",
        "DUNWOD",
        "N.Y.C.",
        "LONGIL",
    }
)

NYISO_PTIDS = frozenset(
    {61752, 61753, 61754, 61755, 61756, 61757, 61758, 61759, 61760, 61761, 61762}
)

# Plausible bounds for prices ($/MWh) — flag outliers
LBMP_LOW, LBMP_HIGH = -500.0, 2000.0

PARTITION_RE = re.compile(r"zone=([^/]+)/year=(\d{4})/month=(\d{2})")


def expected_rows_day_ahead(year: int, month: int) -> int:
    """Day-ahead: one row per hour in the month (single zone partition)."""
    _, ndays = calendar.monthrange(year, month)
    return ndays * 24


def expected_rows_real_time(year: int, month: int) -> int:
    """Real-time: one row per 5-min interval in the month (single zone partition)."""
    _, ndays = calendar.monthrange(year, month)
    return ndays * 24 * (60 // 5)


def check_schema(df: pl.DataFrame) -> list[str]:
    errs: list[str] = []
    if set(df.columns) != set(CANONICAL_COLUMNS):
        errs.append(f"Schema: expected columns {CANONICAL_COLUMNS}, got {df.columns}")
    if "interval_start_est" in df.columns:
        dt = df.schema["interval_start_est"]
        if not isinstance(dt, pl.Datetime):
            errs.append("interval_start_est should be Datetime")
    if df.schema.get("ptid") != pl.Int32:
        errs.append("ptid should be Int32")
    for c in (
        "lbmp_usd_per_mwh",
        "marginal_cost_losses_usd_per_mwh",
        "marginal_cost_congestion_usd_per_mwh",
    ):
        if df.schema.get(c) != pl.Float64:
            errs.append(f"{c} should be Float64")
    return errs


def check_nulls(df: pl.DataFrame) -> list[str]:
    errs: list[str] = []
    key_cols = ["interval_start_est", "zone", "ptid", "lbmp_usd_per_mwh"]
    for c in key_cols:
        if c in df.columns and df[c].null_count() > 0:
            errs.append(f"Nulls in key column {c}: {df[c].null_count()}")
    return errs


def check_missing_values(df: pl.DataFrame) -> list[str]:
    errs: list[str] = []
    if "zone" in df.columns:
        empty_zone = df.filter(pl.col("zone").str.strip_chars() == "").height
        if empty_zone:
            errs.append(f"Empty/whitespace zone: {empty_zone} rows")
    for c in (
        "lbmp_usd_per_mwh",
        "marginal_cost_losses_usd_per_mwh",
        "marginal_cost_congestion_usd_per_mwh",
    ):
        if c not in df.columns:
            continue
        inf_count = df.filter(pl.col(c).is_infinite()).height
        nan_count = df[c].null_count()
        if inf_count:
            errs.append(f"Inf in {c}: {inf_count}")
        # Sentinel-like values
        if (df[c] < -999).any() or (df[c] > 999999).any():
            errs.append(f"Plausible range: {c} has values outside [-999, 999999]")
    return errs


def check_zones(df: pl.DataFrame) -> list[str]:
    """Per-partition: exactly one zone. Log (do not fail) if zone is not in canonical 11."""
    errs: list[str] = []
    zones = set(df["zone"].unique().to_list())
    if not zones:
        return errs
    if len(zones) != 1:
        errs.append(f"Partition should have exactly one zone, got {len(zones)}")
        return errs
    zone = next(iter(zones))
    if zone not in CANONICAL_NYISO_ZONES:
        # Allow non-canonical zones (e.g. H_Q, NPX, O_H, PJM); just log
        print(
            f"Note: partition has non-canonical zone {zone!r} (allowed, not filtered)",
            file=sys.stderr,
        )
    return errs


def check_value_ranges(df: pl.DataFrame) -> list[str]:
    errs: list[str] = []
    if "lbmp_usd_per_mwh" in df.columns:
        low, high = df["lbmp_usd_per_mwh"].min(), df["lbmp_usd_per_mwh"].max()
        if isinstance(low, (int, float)) and low < LBMP_LOW:
            errs.append(f"lbmp_usd_per_mwh min {low} below {LBMP_LOW}")
        if isinstance(high, (int, float)) and high > LBMP_HIGH:
            errs.append(f"lbmp_usd_per_mwh max {high} above {LBMP_HIGH}")
    return errs


def check_uniqueness(df: pl.DataFrame) -> list[str]:
    errs: list[str] = []
    n = df.height
    if "interval_start_est" in df.columns:
        u = df["interval_start_est"].n_unique()
        if u != n and n > 0:
            errs.append(f"interval_start_est not unique: {u} unique vs {n} rows")
    return errs


def check_row_count(df: pl.DataFrame, series: str, year: int, month: int) -> list[str]:
    errs: list[str] = []
    if series == "day_ahead":
        expected = expected_rows_day_ahead(year, month)
    else:
        expected = expected_rows_real_time(year, month)
    actual = df.height
    if actual == 0:
        errs.append("Partition has 0 rows")
    elif actual < expected * 0.5:
        errs.append(f"Row count {actual} < 50% of expected {expected}")
    elif actual != expected:
        errs.append(f"Row count {actual} vs expected {expected}")
    return errs


def validate_partition(
    path: Path,
    series: str,
    zone_val: str,
    year: int,
    month: int,
) -> list[str]:
    df = pl.read_parquet(path)
    errs: list[str] = []
    errs.extend(check_schema(df))
    errs.extend(check_nulls(df))
    errs.extend(check_missing_values(df))
    errs.extend(check_zones(df))
    errs.extend(check_value_ranges(df))
    errs.extend(check_uniqueness(df))
    errs.extend(check_row_count(df, series, year, month))
    return errs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate NYISO zonal LBMP parquet tree."
    )
    parser.add_argument(
        "--path-local-parquet",
        type=Path,
        required=True,
        help="Local parquet root (day_ahead/zone=* and real_time/zone=*).",
    )
    args = parser.parse_args()
    root = args.path_local_parquet.resolve()

    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    total_errors: list[str] = []
    partitions_checked = 0
    for series in ("day_ahead", "real_time"):
        series_dir = root / series
        if not series_dir.is_dir():
            continue
        for zone_dir in series_dir.iterdir():
            if not zone_dir.is_dir() or not zone_dir.name.startswith("zone="):
                continue
            zone_val = zone_dir.name.replace("zone=", "", 1)
            for year_dir in zone_dir.iterdir():
                if not year_dir.is_dir() or not year_dir.name.startswith("year="):
                    continue
                year = int(year_dir.name.replace("year=", "", 1))
                for month_dir in year_dir.iterdir():
                    if not month_dir.is_dir() or not month_dir.name.startswith(
                        "month="
                    ):
                        continue
                    month = int(month_dir.name.replace("month=", "", 1))
                    data_path = month_dir / "data.parquet"
                    if not data_path.is_file():
                        total_errors.append(f"Missing {data_path}")
                        continue
                    errs = validate_partition(data_path, series, zone_val, year, month)
                    for e in errs:
                        total_errors.append(f"{data_path}: {e}")
                    partitions_checked += 1

    if partitions_checked == 0:
        print(
            "Error: no parquet partitions found under path_local_parquet (day_ahead/zone=* or real_time/zone=*). Run convert first.",
            file=sys.stderr,
        )
        return 1
    if total_errors:
        for e in total_errors:
            print(e, file=sys.stderr)
        return 1
    print(f"✓ All {partitions_checked} partitions passed validation.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
