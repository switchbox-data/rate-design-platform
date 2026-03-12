#!/usr/bin/env python3
"""Validate ISO-NE zonal LMP parquet: schema, nulls, row counts, zones, ranges, decomposition.

Runs a QA battery on local Hive-partitioned parquet under path_local_parquet
(day_ahead/ and real_time/ subdirs).  Use after fetch and before upload.

Usage:
    uv run python data/isone/lmp/validate_isone_lmp_parquet.py \
        --path-local-parquet data/isone/lmp/parquet
"""

from __future__ import annotations

import calendar
import sys
from pathlib import Path

import polars as pl

CANONICAL_COLUMNS = {
    "interval_start_et": pl.Datetime("us", "America/New_York"),
    "zone": pl.String,
    "location_id": pl.Int32,
    "lmp_usd_per_mwh": pl.Float64,
    "lmp_energy_usd_per_mwh": pl.Float64,
    "marginal_cost_congestion_usd_per_mwh": pl.Float64,
    "marginal_cost_losses_usd_per_mwh": pl.Float64,
}

CANONICAL_ZONES = frozenset({"ME", "NH", "VT", "CT", "RI", "SEMA", "WCMA", "NEMA"})

ZONE_LOC_IDS: dict[str, int] = {
    "ME": 4001,
    "NH": 4002,
    "VT": 4003,
    "CT": 4004,
    "RI": 4005,
    "SEMA": 4006,
    "WCMA": 4007,
    "NEMA": 4008,
}

LMP_LOW, LMP_HIGH = -500.0, 2000.0
DECOMP_TOLERANCE = 0.01

# DST: spring-forward in March, fall-back in November
DST_SPRING_HOURS = 23
DST_FALL_HOURS = 25
NORMAL_HOURS_PER_DAY = 24


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


def _expected_hours(year: int, month: int) -> tuple[int, int, int]:
    """Return (expected, min_with_dst, max_with_dst) hours for a month.

    March (spring-forward): ndays*24 - 1 = 743 for 31-day March
    November (fall-back): ndays*24 + 1 = 721 for 30-day November
    """
    _, ndays = calendar.monthrange(year, month)
    normal = ndays * NORMAL_HOURS_PER_DAY
    if month == 3:
        return normal - 1, normal - 1, normal
    if month == 11:
        return normal + 1, normal, normal + 1
    return normal, normal, normal


def check_schema(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for col, expected_type in CANONICAL_COLUMNS.items():
        if col not in df.columns:
            errors.append(f"missing column {col}")
        elif df.schema[col] != expected_type:
            errors.append(f"{col}: expected {expected_type}, got {df.schema[col]}")
    extra = set(df.columns) - set(CANONICAL_COLUMNS)
    if extra:
        errors.append(f"unexpected columns: {extra}")

    if errors:
        for e in errors:
            result.error("Schema", e)
    else:
        cols = ", ".join(f"{c} ({t})" for c, t in CANONICAL_COLUMNS.items())
        result.passed("Schema", cols)


def check_no_nulls(df: pl.DataFrame, result: ValidationResult) -> None:
    null_cols = [col for col in df.columns if df[col].null_count() > 0]
    if null_cols:
        for col in null_cols:
            result.error("Nulls", f"{col}: {df[col].null_count()} nulls")
    else:
        result.passed("Nulls", "zero nulls across all columns")


def check_zones(df: pl.DataFrame, result: ValidationResult) -> None:
    zones = set(df["zone"].unique().to_list())
    missing = CANONICAL_ZONES - zones
    extra = zones - CANONICAL_ZONES
    if missing:
        result.error("Zones", f"missing zones: {sorted(missing)}")
    if extra:
        result.warn("Zones", f"unexpected zones: {sorted(extra)}")
    if not missing and not extra:
        result.passed("Zones", f"all 8 canonical zones present: {sorted(zones)}")


def check_location_ids(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for zone, expected_id in ZONE_LOC_IDS.items():
        zone_df = df.filter(pl.col("zone") == zone)
        if zone_df.height == 0:
            continue
        ids = set(zone_df["location_id"].unique().to_list())
        if ids != {expected_id}:
            errors.append(f"{zone}: expected location_id {expected_id}, got {ids}")
    if errors:
        for e in errors:
            result.error("Location IDs", e)
    else:
        result.passed("Location IDs", "all zones map to correct location_id")


def check_uniqueness(df: pl.DataFrame, result: ValidationResult) -> None:
    """interval_start_et should be unique within each zone partition."""
    dupes = (
        df.group_by("zone", "interval_start_et")
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)
    )
    if dupes.height > 0:
        result.error(
            "Uniqueness",
            f"{dupes.height} duplicate (zone, interval_start_et) pairs",
        )
    else:
        result.passed("Uniqueness", "interval_start_et unique per zone")


def check_row_counts(df: pl.DataFrame, result: ValidationResult) -> None:
    """Check per-zone-month row counts against DST-aware expectations."""
    counts = (
        df.with_columns(
            pl.col("interval_start_et").dt.year().alias("year"),
            pl.col("interval_start_et").dt.month().alias("month"),
        )
        .group_by("zone", "year", "month")
        .agg(pl.len().alias("n"))
    )
    bad: list[str] = []
    for row in counts.iter_rows(named=True):
        _, min_h, max_h = _expected_hours(row["year"], row["month"])
        if not (min_h <= row["n"] <= max_h):
            bad.append(
                f"{row['zone']} {row['year']}-{row['month']:02d}: "
                f"{row['n']} rows (expected {min_h}–{max_h})"
            )
    if bad:
        for b in bad:
            result.warn("Row counts", b)
    else:
        n_months = counts.height
        result.passed(
            "Row counts",
            f"all {n_months} zone-months have expected hour counts (DST-aware)",
        )


def check_value_ranges(df: pl.DataFrame, result: ValidationResult) -> None:
    lmp = df["lmp_usd_per_mwh"]
    pmin = float(lmp.min())  # type: ignore[arg-type]
    pmax = float(lmp.max())  # type: ignore[arg-type]
    pmean = float(lmp.mean())  # type: ignore[arg-type]

    n_inf = lmp.is_infinite().sum()
    if n_inf > 0:
        result.error("Price integrity", f"{n_inf} infinite values")

    if pmin < LMP_LOW:
        result.error("Price range", f"min ${pmin:.2f} below ${LMP_LOW:.0f}")
    elif pmax > LMP_HIGH:
        result.warn("Price range", f"max ${pmax:.2f} above ${LMP_HIGH:.0f}")
    else:
        result.passed(
            "Price range",
            f"${pmin:.2f}–${pmax:.2f}/MWh (mean ${pmean:.2f})",
        )


def check_decomposition(df: pl.DataFrame, result: ValidationResult) -> None:
    """Verify |LMP - (energy + congestion + losses)| < tolerance."""
    diff = (
        df["lmp_usd_per_mwh"]
        - df["lmp_energy_usd_per_mwh"]
        - df["marginal_cost_congestion_usd_per_mwh"]
        - df["marginal_cost_losses_usd_per_mwh"]
    ).abs()
    max_diff = float(diff.max())  # type: ignore[arg-type]
    n_bad = diff.filter(diff > DECOMP_TOLERANCE).len()
    if n_bad > 0:
        result.error(
            "LMP decomposition",
            f"{n_bad} rows where |LMP - (energy+congestion+losses)| > {DECOMP_TOLERANCE} "
            f"(max diff: {max_diff:.6f})",
        )
    else:
        result.passed(
            "LMP decomposition",
            f"all rows satisfy |LMP - components| < {DECOMP_TOLERANCE} "
            f"(max diff: {max_diff:.6f})",
        )


def check_cross_year_continuity(df: pl.DataFrame, result: ValidationResult) -> None:
    """Flag large year-over-year jumps in annual average LMP per zone."""
    annual = (
        df.with_columns(pl.col("interval_start_et").dt.year().alias("year"))
        .group_by("zone", "year")
        .agg(pl.col("lmp_usd_per_mwh").mean().alias("avg_lmp"))
        .sort("zone", "year")
    )
    jumps: list[str] = []
    for zone in sorted(annual["zone"].unique().to_list()):
        z_data = annual.filter(pl.col("zone") == zone).sort("year")
        prices = z_data["avg_lmp"].to_list()
        years = z_data["year"].to_list()
        for i in range(1, len(prices)):
            if prices[i - 1] is None or prices[i - 1] == 0:
                continue
            ratio = prices[i] / prices[i - 1]
            if ratio > 5.0 or ratio < 0.2:
                jumps.append(
                    f"{zone}: {years[i - 1]}→{years[i]} = {ratio:.1f}x "
                    f"(${prices[i - 1]:.2f}→${prices[i]:.2f})"
                )
    if jumps:
        for j in jumps:
            result.warn("Year-over-year continuity", j)
    else:
        n_transitions = sum(
            annual.filter(pl.col("zone") == z).height - 1
            for z in annual["zone"].unique().to_list()
        )
        result.passed(
            "Year-over-year continuity",
            f"no jumps >5x across {n_transitions} year transitions",
        )


def check_price_summary_by_zone(df: pl.DataFrame, result: ValidationResult) -> None:
    summary = (
        df.group_by("zone")
        .agg(
            pl.col("lmp_usd_per_mwh").min().alias("min"),
            pl.col("lmp_usd_per_mwh").mean().alias("mean"),
            pl.col("lmp_usd_per_mwh").max().alias("max"),
        )
        .sort("zone")
    )
    lines = []
    for row in summary.iter_rows(named=True):
        lines.append(
            f"{row['zone']}: ${row['min']:.2f}–${row['max']:.2f} "
            f"(mean ${row['mean']:.2f})"
        )
    result.passed("Price summary by zone", "; ".join(lines))


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


def validate_series(root: Path, series: str, result: ValidationResult) -> int:
    """Validate one series (day_ahead or real_time). Returns row count."""
    series_dir = root / series
    if not series_dir.is_dir():
        result.warn(f"[{series}] Directory", f"{series_dir} not found")
        return 0

    parquet_files = list(series_dir.glob("zone=*/year=*/month=*/data.parquet"))
    if not parquet_files:
        result.error(f"[{series}] Partitions", "no parquet files found")
        return 0

    df = pl.read_parquet(
        series_dir / "**/*.parquet",
        hive_partitioning=False,
    )

    result.passed(
        f"[{series}] Partitions",
        f"{len(parquet_files)} files, {df.height} rows",
    )

    check_schema(df, result)
    check_no_nulls(df, result)
    check_zones(df, result)
    check_location_ids(df, result)
    check_uniqueness(df, result)
    check_row_counts(df, result)
    check_value_ranges(df, result)
    check_decomposition(df, result)
    check_cross_year_continuity(df, result)
    check_price_summary_by_zone(df, result)

    return df.height


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate ISO-NE zonal LMP parquet tree."
    )
    parser.add_argument(
        "--path-local-parquet",
        type=Path,
        required=True,
        help="Local parquet root (day_ahead/ and real_time/ subdirs).",
    )
    args = parser.parse_args()
    root = args.path_local_parquet.resolve()

    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    print("ISO-NE Zonal LMP Validation Report")
    print(f"{'=' * 60}")
    print(f"Source: {root}")
    print(f"{'=' * 60}\n")

    total_rows = 0
    result = ValidationResult()

    for series in ("day_ahead", "real_time"):
        print(f"--- {series} ---")
        rows = validate_series(root, series, result)
        total_rows += rows
        print()

    print_report(result)

    n_pass = sum(1 for _, s, _ in result.checks if s == "PASS")
    n_warn = sum(1 for _, s, _ in result.checks if s == "WARN")
    n_fail = sum(1 for _, s, _ in result.checks if s == "FAIL")
    print(f"\n{'=' * 60}")
    print(f"Total rows: {total_rows}")
    print(f"Result: {n_pass} passed, {n_warn} warnings, {n_fail} failures")

    if not result.ok:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
