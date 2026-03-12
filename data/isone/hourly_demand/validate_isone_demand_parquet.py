#!/usr/bin/env python3
"""Validate ISO-NE hourly demand parquet: schema, completeness, ranges, gaps.

Runs a QA battery on local Hive-partitioned parquet under path_local_zones.
Use after fetch and before upload.

Usage:
    uv run python data/isone/hourly_demand/validate_isone_demand_parquet.py \\
        --path-local-zones data/isone/hourly_demand/zones
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

EXPECTED_ZONES = {"ME", "NH", "VT", "CT", "RI", "SEMA", "WCMA", "NEMA"}

EXPECTED_SCHEMA = {
    "interval_start_et": pl.Datetime("us", "America/New_York"),
    "zone": pl.String,
    "location_id": pl.Int32,
    "load_mw": pl.Float64,
}

LOAD_FLOOR = 0.0
LOAD_CEILING = 10_000.0

ET = ZoneInfo("America/New_York")


class ValidationResult:
    def __init__(self) -> None:
        self.checks: list[tuple[str, str, str]] = []

    def passed(self, name: str, detail: str = "") -> None:
        self.checks.append((name, "PASS", detail))

    def error(self, name: str, detail: str) -> None:
        self.checks.append((name, "FAIL", detail))

    def warn(self, name: str, detail: str) -> None:
        self.checks.append((name, "WARN", detail))

    @property
    def ok(self) -> bool:
        return all(s != "FAIL" for _, s, _ in self.checks)


def _expected_hours_in_month(year: int, month: int) -> int:
    """DST-aware count of distinct wall-clock hours in a month."""
    utc = ZoneInfo("UTC")
    start = datetime(year, month, 1, tzinfo=ET).astimezone(utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=ET).astimezone(utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=ET).astimezone(utc)
    return int((end - start).total_seconds() // 3600)


def check_schema(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col not in df.columns:
            errors.append(f"missing column {col}")
        elif df.schema[col] != expected_type:
            errors.append(f"{col}: expected {expected_type}, got {df.schema[col]}")
    extra = set(df.columns) - set(EXPECTED_SCHEMA) - {"year", "month"}
    if extra:
        errors.append(f"unexpected columns: {extra}")
    if errors:
        for e in errors:
            result.error("Schema", e)
    else:
        cols = ", ".join(f"{c} ({t})" for c, t in EXPECTED_SCHEMA.items())
        result.passed("Schema", cols)


def check_no_nulls(df: pl.DataFrame, result: ValidationResult) -> None:
    null_cols = [
        col for col in EXPECTED_SCHEMA if col in df.columns and df[col].null_count() > 0
    ]
    if null_cols:
        for col in null_cols:
            result.error("Nulls", f"{col}: {df[col].null_count()} nulls")
    else:
        result.passed("Nulls", "zero nulls across all columns")


def check_all_zones(df: pl.DataFrame, result: ValidationResult) -> None:
    zones = set(df["zone"].unique().to_list())
    missing = EXPECTED_ZONES - zones
    extra = zones - EXPECTED_ZONES
    if missing:
        result.error("Zones", f"missing: {sorted(missing)}")
    if extra:
        result.warn("Zones", f"unexpected: {sorted(extra)}")
    if not missing and not extra:
        result.passed("Zones", f"all {len(EXPECTED_ZONES)} zones present")


def check_uniqueness(df: pl.DataFrame, result: ValidationResult) -> None:
    """interval_start_et must be unique within each zone."""
    dupes = (
        df.group_by("zone", "interval_start_et")
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)
    )
    if dupes.height > 0:
        n_dupes = dupes.height
        sample = dupes.head(3)
        result.error(
            "Uniqueness",
            f"{n_dupes} duplicate (zone, timestamp) pairs; sample: "
            + str(sample.to_dicts()),
        )
    else:
        result.passed("Uniqueness", "interval_start_et unique within each zone")


def check_hours_per_month(df: pl.DataFrame, result: ValidationResult) -> None:
    """Check each zone-month has the DST-aware expected number of hours."""
    df_with_ym = df.with_columns(
        pl.col("interval_start_et").dt.year().alias("_year"),
        pl.col("interval_start_et").dt.month().alias("_month"),
    )
    counts = (
        df_with_ym.group_by("zone", "_year", "_month")
        .agg(pl.len().alias("n"))
        .sort("zone", "_year", "_month")
    )

    mismatches: list[str] = []
    for row in counts.iter_rows(named=True):
        expected = _expected_hours_in_month(row["_year"], row["_month"])
        if row["n"] != expected:
            mismatches.append(
                f"{row['zone']} {row['_year']}-{row['_month']:02d}: "
                f"got {row['n']}, expected {expected}"
            )

    if mismatches:
        for m in mismatches[:10]:
            result.warn("Hours per month", m)
        if len(mismatches) > 10:
            result.warn("Hours per month", f"… and {len(mismatches) - 10} more")
    else:
        n_zone_months = counts.height
        result.passed(
            "Hours per month",
            f"all {n_zone_months} zone-months have DST-correct hour counts",
        )


def check_load_ranges(df: pl.DataFrame, result: ValidationResult) -> None:
    loads = df["load_mw"]
    n_nan = loads.is_nan().sum()
    n_inf = loads.is_infinite().sum()
    lmin = float(loads.min())  # type: ignore[arg-type]
    lmax = float(loads.max())  # type: ignore[arg-type]
    lmean = float(loads.mean())  # type: ignore[arg-type]

    if n_nan > 0:
        result.error("Load integrity", f"{n_nan} NaN values")
    if n_inf > 0:
        result.error("Load integrity", f"{n_inf} infinite values")
    if n_nan == 0 and n_inf == 0:
        result.passed("Load integrity", "no NaN or Inf values")

    if lmin < LOAD_FLOOR:
        result.warn(
            "Load range",
            f"min {lmin:.1f} MW is negative — verify",
        )
    if lmax > LOAD_CEILING:
        result.warn(
            "Load range",
            f"max {lmax:.1f} MW exceeds {LOAD_CEILING:.0f} MW ceiling — verify",
        )
    if lmin >= LOAD_FLOOR and lmax <= LOAD_CEILING:
        result.passed("Load range", f"{lmin:.1f}–{lmax:.1f} MW (mean {lmean:.1f})")

    summary = (
        df.group_by("zone")
        .agg(
            pl.col("load_mw").min().alias("min"),
            pl.col("load_mw").mean().alias("mean"),
            pl.col("load_mw").max().alias("max"),
        )
        .sort("zone")
    )
    lines = []
    for row in summary.iter_rows(named=True):
        lines.append(
            f"{row['zone']}: {row['min']:.1f}–{row['max']:.1f} MW "
            f"(mean {row['mean']:.1f})"
        )
    result.passed("Load summary by zone", "; ".join(lines))


def check_gap_detection(df: pl.DataFrame, result: ValidationResult) -> None:
    """Identify missing hours within each zone's time series."""
    gaps_found: list[str] = []
    for zone in sorted(EXPECTED_ZONES):
        zone_df = df.filter(pl.col("zone") == zone).sort("interval_start_et")
        if zone_df.height < 2:
            continue
        ts = zone_df["interval_start_et"]
        diffs = ts.diff().drop_nulls()
        large_gaps = diffs.filter(diffs > timedelta(hours=1))
        if large_gaps.len() > 0:
            gaps_found.append(f"{zone}: {large_gaps.len()} gap(s)")

    if gaps_found:
        for g in gaps_found:
            result.warn("Gap detection", g)
    else:
        result.passed("Gap detection", "no gaps > 1 hour in any zone")


def check_coverage_summary(df: pl.DataFrame, result: ValidationResult) -> None:
    """Year/month coverage summary."""
    df_with_ym = df.with_columns(
        pl.col("interval_start_et").dt.year().alias("_year"),
        pl.col("interval_start_et").dt.month().alias("_month"),
    )
    ym = df_with_ym.select("_year", "_month").unique().sort("_year", "_month")
    years = sorted(ym["_year"].unique().to_list())
    n_months = ym.height

    gaps: list[str] = []
    for y in years:
        months_present = set(ym.filter(pl.col("_year") == y)["_month"].to_list())
        missing = set(range(1, 13)) - months_present
        if missing and y == years[-1]:
            pass  # partial final year is fine
        elif missing:
            gaps.append(f"{y} missing months {sorted(missing)}")

    if gaps:
        for g in gaps:
            result.warn("Coverage", g)
    else:
        result.passed(
            "Coverage",
            f"{n_months} months across {len(years)} years ({years[0]}–{years[-1]})",
        )


def print_report(result: ValidationResult) -> None:
    max_name = max(len(name) for name, _, _ in result.checks)
    for name, status, detail in result.checks:
        if status == "PASS":
            icon = "  ok"
        elif status == "WARN":
            icon = "WARN"
        else:
            icon = "FAIL"
        line = f"  {icon}  {name:<{max_name}}  {detail}"
        print(line)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate ISO-NE hourly demand parquet."
    )
    parser.add_argument(
        "--path-local-zones",
        type=Path,
        required=True,
        help="Local parquet root (zone=*/year=*/month=*/data.parquet).",
    )
    args = parser.parse_args()
    root = args.path_local_zones.resolve()

    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    parquet_files = list(root.glob("zone=*/year=*/month=*/data.parquet"))
    if not parquet_files:
        print(f"No parquet partitions found under {root}", file=sys.stderr)
        return 1

    df = pl.read_parquet(
        root / "**/*.parquet",
        hive_partitioning=True,
    )

    print("ISO-NE Hourly Demand Validation Report")
    print("=" * 60)
    print(f"Source:     {root}")
    print(f"Partitions: {len(parquet_files)}")
    print(f"Rows:       {df.height:,}")
    print("=" * 60 + "\n")

    result = ValidationResult()

    check_schema(df, result)
    check_no_nulls(df, result)
    check_all_zones(df, result)
    check_uniqueness(df, result)
    check_hours_per_month(df, result)
    check_load_ranges(df, result)
    check_gap_detection(df, result)
    check_coverage_summary(df, result)

    print_report(result)

    n_pass = sum(1 for _, s, _ in result.checks if s == "PASS")
    n_warn = sum(1 for _, s, _ in result.checks if s == "WARN")
    n_fail = sum(1 for _, s, _ in result.checks if s == "FAIL")
    print(f"\n{'=' * 60}")
    print(f"Result: {n_pass} passed, {n_warn} warnings, {n_fail} failures")

    if not result.ok:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
