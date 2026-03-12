#!/usr/bin/env python3
"""Validate ISO-NE MRA clearing price parquet: schema, completeness, ranges, domain invariants.

Runs a QA battery on local Hive-partitioned parquet under path_local_parquet.
Use after fetch and before upload.

Usage:
    uv run python data/isone/capacity/mra/validate_isone_mra.py \
        --path-local-parquet data/isone/capacity/mra/parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

EXPECTED_SCHEMA = {
    "year": pl.Int16,
    "month": pl.Int8,
    "cp": pl.String,
    "entity_type": pl.String,
    "capacity_zone_id": pl.Int32,
    "capacity_zone_name": pl.String,
    "entity_name": pl.String,
    "supply_submitted_mw": pl.Float64,
    "demand_submitted_mw": pl.Float64,
    "supply_cleared_mw": pl.Float64,
    "demand_cleared_mw": pl.Float64,
    "net_capacity_cleared_mw": pl.Float64,
    "clearing_price_per_kw_month": pl.Float64,
}

REQUIRED_NO_NULL_COLS = [
    "year",
    "month",
    "cp",
    "entity_type",
    "capacity_zone_id",
    "capacity_zone_name",
    "entity_name",
    "clearing_price_per_kw_month",
]

# Zone IDs observed historically.  The active set changes across CPs.
KNOWN_ZONE_IDS = {8500, 8501, 8502, 8503, 8504, 8505, 8506}

PRICE_FLOOR = 0.0
PRICE_CEILING = 50.0


class ValidationResult:
    def __init__(self) -> None:
        self.checks: list[tuple[str, str, str]] = []  # (name, status, detail)

    def passed(self, name: str, detail: str = "") -> None:
        self.checks.append((name, "PASS", detail))

    def error(self, name: str, detail: str) -> None:
        self.checks.append((name, "FAIL", detail))

    def warn(self, name: str, detail: str) -> None:
        self.checks.append((name, "WARN", detail))

    @property
    def ok(self) -> bool:
        return all(s != "FAIL" for _, s, _ in self.checks)


def check_schema(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col not in df.columns:
            errors.append(f"missing column {col}")
        elif df.schema[col] != expected_type:
            errors.append(f"{col}: expected {expected_type}, got {df.schema[col]}")
    extra = set(df.columns) - set(EXPECTED_SCHEMA)
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
        col
        for col in REQUIRED_NO_NULL_COLS
        if col in df.columns and df[col].null_count() > 0
    ]
    if null_cols:
        for col in null_cols:
            result.error("Nulls", f"{col}: {df[col].null_count()} nulls")
    else:
        result.passed(
            "Nulls", f"zero nulls in {len(REQUIRED_NO_NULL_COLS)} required columns"
        )


def check_no_duplicates(df: pl.DataFrame, result: ValidationResult) -> None:
    keys = ["year", "month", "entity_type", "entity_name"]
    n_total = df.height
    n_unique = df.select(keys).unique().height
    if n_unique != n_total:
        result.error("Uniqueness", f"{n_total - n_unique} duplicate rows on {keys}")
    else:
        result.passed("Uniqueness", f"all {n_total} rows unique on {keys}")


def check_price_ranges(df: pl.DataFrame, result: ValidationResult) -> None:
    prices = df["clearing_price_per_kw_month"]
    n_nan = prices.is_nan().sum()
    n_inf = prices.is_infinite().sum()
    pmin = float(prices.min())  # type: ignore[arg-type]
    pmax = float(prices.max())  # type: ignore[arg-type]
    pmean = float(prices.mean())  # type: ignore[arg-type]

    if n_nan > 0:
        result.error("Price integrity", f"{n_nan} NaN values")
    if n_inf > 0:
        result.error("Price integrity", f"{n_inf} infinite values")
    if n_nan == 0 and n_inf == 0:
        result.passed("Price integrity", "no NaN or Inf values")

    if pmin < PRICE_FLOOR:
        result.error(
            "Price range",
            f"min ${pmin:.4f} is negative (MRA prices should be >= 0)",
        )
    elif pmax > PRICE_CEILING:
        result.warn(
            "Price range",
            f"max ${pmax:.4f} exceeds ${PRICE_CEILING:.0f}/kW-mo ceiling — verify",
        )
    else:
        result.passed(
            "Price range", f"${pmin:.4f}–${pmax:.4f}/kW-mo (mean ${pmean:.4f})"
        )


def check_zone_ids(df: pl.DataFrame, result: ValidationResult) -> None:
    observed = set(df["capacity_zone_id"].unique().to_list())
    unknown = observed - KNOWN_ZONE_IDS
    if unknown:
        result.warn("Zone IDs", f"unknown zone IDs: {unknown}")
    else:
        result.passed("Zone IDs", f"all IDs in known set: {sorted(observed)}")


def check_price_summary_by_zone(df: pl.DataFrame, result: ValidationResult) -> None:
    zones = df.filter(pl.col("entity_type") == "zone")
    if zones.height == 0:
        result.warn("Price summary by zone", "no zone rows found")
        return

    summary = (
        zones.group_by("capacity_zone_name")
        .agg(
            pl.col("clearing_price_per_kw_month").min().alias("min"),
            pl.col("clearing_price_per_kw_month").mean().alias("mean"),
            pl.col("clearing_price_per_kw_month").max().alias("max"),
            pl.len().alias("n"),
        )
        .sort("capacity_zone_name")
    )
    lines = []
    for row in summary.iter_rows(named=True):
        lines.append(
            f"{row['capacity_zone_name']}: "
            f"${row['min']:.4f}–${row['max']:.4f} "
            f"(mean ${row['mean']:.4f}, n={row['n']})"
        )
    result.passed("Price summary by zone", "; ".join(lines))


def check_year_month_coverage(df: pl.DataFrame, result: ValidationResult) -> None:
    coverage = df.select("year", "month").unique().sort("year", "month")
    n_months = coverage.height
    years = sorted(coverage["year"].unique().to_list())
    first = coverage.row(0)
    last = coverage.row(-1)
    result.passed(
        "Year/month coverage",
        f"{n_months} months from {first[0]}-{first[1]:02d} to {last[0]}-{last[1]:02d} "
        f"across {len(years)} years",
    )

    # Check for gaps within the covered range
    all_months: set[tuple[int, int]] = set()
    min_year, min_month = int(first[0]), int(first[1])
    max_year, max_month = int(last[0]), int(last[1])
    y, m = min_year, min_month
    while (y, m) <= (max_year, max_month):
        all_months.add((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    actual = {(int(r[0]), int(r[1])) for r in coverage.iter_rows()}
    missing = sorted(all_months - actual)
    if missing:
        gap_strs = [f"{y}-{m:02d}" for y, m in missing[:10]]
        suffix = f" (and {len(missing) - 10} more)" if len(missing) > 10 else ""
        result.warn(
            "Coverage gaps", f"{len(missing)} missing months: {gap_strs}{suffix}"
        )
    else:
        result.passed("Coverage gaps", "no gaps in covered range")


def check_entity_types(df: pl.DataFrame, result: ValidationResult) -> None:
    types = set(df["entity_type"].unique().to_list())
    expected = {"zone", "external_interface"}
    if types == expected:
        result.passed("Entity types", f"{sorted(types)}")
    elif types - expected:
        result.error("Entity types", f"unexpected types: {types - expected}")
    else:
        missing = expected - types
        result.warn("Entity types", f"missing types: {missing}")


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

    parser = argparse.ArgumentParser(description="Validate ISO-NE MRA parquet.")
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

    print("ISO-NE MRA Validation Report")
    print(f"{'=' * 60}")
    print(f"Source: {root}")
    print(f"Partitions: {len(parquet_files)}")
    print(f"Rows: {df.height}")
    print(f"{'=' * 60}\n")

    result = ValidationResult()

    check_schema(df, result)
    check_no_nulls(df, result)
    check_no_duplicates(df, result)
    check_entity_types(df, result)
    check_zone_ids(df, result)
    check_price_ranges(df, result)
    check_price_summary_by_zone(df, result)
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
