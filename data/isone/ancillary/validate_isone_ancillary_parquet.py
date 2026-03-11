#!/usr/bin/env python3
"""Validate ISO-NE ancillary data parquet: schema, completeness, ranges, uniqueness.

Runs a QA battery on local Hive-partitioned parquet under path_local_parquet.
Use after fetch and before upload.

Usage:
    uv run python data/isone/ancillary/validate_isone_ancillary_parquet.py \
        --path-local-parquet data/isone/ancillary/parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

EXPECTED_SCHEMA = {
    "interval_start_et": pl.Datetime("us", "America/New_York"),
    "reg_service_price_usd_per_mwh": pl.Float64,
    "reg_capacity_price_usd_per_mwh": pl.Float64,
    "system_load_mw": pl.Float64,
    "native_load_mw": pl.Float64,
    "ard_demand_mw": pl.Float64,
}

REG_PRICE_FLOOR = 0.0
REG_PRICE_CEILING = 500.0
LOAD_FLOOR = 5_000.0
LOAD_CEILING = 35_000.0

# DST: spring-forward loses 1 hour, fall-back gains 1 hour (Eastern Time)
SPRING_FORWARD_MONTHS = {3}
FALL_BACK_MONTHS = {11}


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


def _expected_hours(year: int, month: int) -> int:
    """Expected number of hourly intervals for a month, accounting for DST."""
    import calendar

    n_days = calendar.monthrange(year, month)[1]
    hours = n_days * 24
    if month in SPRING_FORWARD_MONTHS:
        hours -= 1
    elif month in FALL_BACK_MONTHS:
        hours += 1
    return hours


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
    data_cols = [c for c in df.columns if c not in ("year", "month")]
    null_cols = [col for col in data_cols if df[col].null_count() > 0]
    if null_cols:
        for col in null_cols:
            result.error("Nulls", f"{col}: {df[col].null_count()} nulls")
    else:
        result.passed("Nulls", "zero nulls across all data columns")


def check_uniqueness(df: pl.DataFrame, result: ValidationResult) -> None:
    n_total = df.height
    n_unique = df.select("interval_start_et").unique().height
    if n_unique != n_total:
        result.error(
            "Uniqueness",
            f"{n_total - n_unique} duplicate interval_start_et values",
        )
    else:
        result.passed(
            "Uniqueness",
            f"all {n_total} rows have unique interval_start_et",
        )


def check_hourly_completeness(df: pl.DataFrame, result: ValidationResult) -> None:
    """Each (year, month) should have the expected number of hourly intervals."""
    bad: list[str] = []
    for (year, month), group in df.group_by("year", "month", maintain_order=True):
        expected = _expected_hours(int(year), int(month))
        actual = group.height
        if actual != expected:
            bad.append(
                f"year={year} month={month}: {actual} hours (expected {expected})"
            )

    if bad:
        for b in bad:
            result.warn("Hourly completeness", b)
    else:
        result.passed("Hourly completeness", "all months have expected hour count")


def check_reg_price_ranges(df: pl.DataFrame, result: ValidationResult) -> None:
    for col_label, col_name in [
        ("Reg service price", "reg_service_price_usd_per_mwh"),
        ("Reg capacity price", "reg_capacity_price_usd_per_mwh"),
    ]:
        prices = df[col_name]
        n_nan = prices.is_nan().sum()
        n_inf = prices.is_infinite().sum()
        pmin = float(prices.min())  # type: ignore[arg-type]
        pmax = float(prices.max())  # type: ignore[arg-type]
        pmean = float(prices.mean())  # type: ignore[arg-type]

        if n_nan > 0:
            result.error(f"{col_label} integrity", f"{n_nan} NaN values")
        if n_inf > 0:
            result.error(f"{col_label} integrity", f"{n_inf} infinite values")
        if n_nan == 0 and n_inf == 0:
            result.passed(f"{col_label} integrity", "no NaN or Inf")

        if pmin < REG_PRICE_FLOOR:
            result.warn(
                f"{col_label} range",
                f"min ${pmin:.2f}/MWh is negative",
            )
        elif pmax > REG_PRICE_CEILING:
            result.warn(
                f"{col_label} range",
                f"max ${pmax:.2f}/MWh exceeds ${REG_PRICE_CEILING:.0f} ceiling",
            )
        else:
            result.passed(
                f"{col_label} range",
                f"${pmin:.2f}–${pmax:.2f}/MWh (mean ${pmean:.2f})",
            )


def check_load_ranges(df: pl.DataFrame, result: ValidationResult) -> None:
    for col_label, col_name in [
        ("System load", "system_load_mw"),
        ("Native load", "native_load_mw"),
        ("ARD demand", "ard_demand_mw"),
    ]:
        vals = df[col_name]
        vmin = float(vals.min())  # type: ignore[arg-type]
        vmax = float(vals.max())  # type: ignore[arg-type]
        vmean = float(vals.mean())  # type: ignore[arg-type]

        if col_name == "system_load_mw":
            if vmin < LOAD_FLOOR:
                result.warn(
                    f"{col_label} range",
                    f"min {vmin:,.0f} MW below {LOAD_FLOOR:,.0f} floor",
                )
            elif vmax > LOAD_CEILING:
                result.warn(
                    f"{col_label} range",
                    f"max {vmax:,.0f} MW exceeds {LOAD_CEILING:,.0f} ceiling",
                )
            else:
                result.passed(
                    f"{col_label} range",
                    f"{vmin:,.0f}–{vmax:,.0f} MW (mean {vmean:,.0f})",
                )
        else:
            result.passed(
                f"{col_label} range",
                f"{vmin:,.0f}–{vmax:,.0f} MW (mean {vmean:,.0f})",
            )


def check_year_month_coverage(df: pl.DataFrame, result: ValidationResult) -> None:
    """Report year/month coverage."""
    ym = df.select("year", "month").unique().sort("year", "month")
    years = sorted(ym["year"].unique().to_list())
    n_months = ym.height

    gaps: list[str] = []
    for y in years:
        months = set(ym.filter(pl.col("year") == y)["month"].to_list())
        missing = set(range(1, 13)) - months
        if missing and y != max(years):
            gaps.append(f"{y} missing months {sorted(missing)}")

    if gaps:
        for g in gaps:
            result.warn("Year/month coverage", g)
    else:
        result.passed(
            "Year/month coverage",
            f"{n_months} months across {len(years)} years ({min(years)}–{max(years)})",
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
        description="Validate ISO-NE ancillary data parquet."
    )
    parser.add_argument(
        "--path-local-parquet",
        type=Path,
        required=True,
        help="Local parquet root (year=*/month=*/data.parquet).",
    )
    args = parser.parse_args()
    root = args.path_local_parquet.resolve()

    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    parquet_files = list(root.glob("year=*/month=*/data.parquet"))
    if not parquet_files:
        print(f"No parquet partitions found under {root}", file=sys.stderr)
        return 1

    df = pl.read_parquet(
        root / "**/*.parquet",
        hive_partitioning=True,
    )

    print("ISO-NE Ancillary Data Validation Report")
    print(f"{'=' * 60}")
    print(f"Source: {root}")
    print(f"Partitions: {len(parquet_files)}")
    print(f"Rows: {df.height}")
    print(
        f"Date range: {df['interval_start_et'].min()} — {df['interval_start_et'].max()}"
    )
    print(f"{'=' * 60}\n")

    result = ValidationResult()

    check_schema(df, result)
    check_no_nulls(df, result)
    check_uniqueness(df, result)
    check_hourly_completeness(df, result)
    check_reg_price_ranges(df, result)
    check_load_ranges(df, result)
    check_year_month_coverage(df, result)

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
