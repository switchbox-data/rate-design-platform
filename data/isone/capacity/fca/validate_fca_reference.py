#!/usr/bin/env python3
"""Validate ISO-NE FCA clearing price CSV: schema, completeness, ranges, domain invariants.

Runs a QA battery on the curated fca_clearing_prices.csv reference dataset.

Usage:
    uv run python data/isone/capacity/fca/validate_fca_reference.py \
        --path-csv data/isone/capacity/fca/fca_clearing_prices.csv
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

EXPECTED_COLUMNS = [
    "fca_number",
    "ccp_start",
    "ccp_end",
    "capacity_zone_id",
    "capacity_zone_name",
    "resource_status",
    "clearing_price_per_kw_month",
    "notes",
]

EXPECTED_TYPES = {
    "fca_number": pl.Int64,
    "ccp_start": pl.Date,
    "ccp_end": pl.Date,
    "capacity_zone_id": pl.Int64,
    "capacity_zone_name": pl.String,
    "resource_status": pl.String,
    "clearing_price_per_kw_month": pl.Float64,
    "notes": pl.String,
}

CANONICAL_ZONE_IDS = {8500, 8503, 8504, 8505, 8506}
VALID_RESOURCE_STATUSES = {"all", "existing", "new"}

PRICE_FLOOR = 0.0
PRICE_CEILING = 20.0

CROSS_CHECK_VALUES = {
    (10, 8500, "all"): 7.030,
    (11, 8500, "all"): 5.297,
    (15, 8506, "all"): 3.980,
    (16, 8506, "all"): 2.639,
    (1, 8500, "all"): 4.500,
    (18, 8500, "all"): 3.580,
}


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


def check_schema(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            errors.append(f"missing column {col}")
    extra = set(df.columns) - set(EXPECTED_COLUMNS)
    if extra:
        errors.append(f"unexpected columns: {extra}")

    for col, expected_type in EXPECTED_TYPES.items():
        if col in df.columns and df.schema[col] != expected_type:
            errors.append(f"{col}: expected {expected_type}, got {df.schema[col]}")

    if errors:
        for e in errors:
            result.error("Schema", e)
    else:
        cols = ", ".join(f"{c} ({t})" for c, t in EXPECTED_TYPES.items())
        result.passed("Schema", cols)


def check_no_nulls(df: pl.DataFrame, result: ValidationResult) -> None:
    required = [c for c in EXPECTED_COLUMNS if c != "notes"]
    null_cols = [col for col in required if df[col].null_count() > 0]
    if null_cols:
        for col in null_cols:
            result.error("Nulls", f"{col}: {df[col].null_count()} nulls")
    else:
        result.passed("Nulls", "zero nulls in required columns")


def check_no_duplicates(df: pl.DataFrame, result: ValidationResult) -> None:
    keys = ["fca_number", "capacity_zone_id", "resource_status"]
    n_total = df.height
    n_unique = df.select(keys).unique().height
    if n_unique != n_total:
        result.error(
            "Uniqueness", f"{n_total - n_unique} duplicate (fca, zone, status) rows"
        )
    else:
        result.passed(
            "Uniqueness",
            f"all {n_total} rows have unique (fca_number, capacity_zone_id, resource_status)",
        )


def check_all_fcas_present(df: pl.DataFrame, result: ValidationResult) -> None:
    fca_numbers = set(df["fca_number"].unique().to_list())
    expected = set(range(1, 19))
    missing = expected - fca_numbers
    extra = fca_numbers - expected
    if missing:
        result.error("FCA coverage", f"missing FCAs: {sorted(missing)}")
    if extra:
        result.warn("FCA coverage", f"unexpected FCAs: {sorted(extra)}")
    if not missing and not extra:
        result.passed("FCA coverage", "all 18 FCAs present (1-18)")


def check_resource_status(df: pl.DataFrame, result: ValidationResult) -> None:
    statuses = set(df["resource_status"].unique().to_list())
    invalid = statuses - VALID_RESOURCE_STATUSES
    if invalid:
        result.error("Resource status", f"invalid values: {invalid}")
    else:
        result.passed("Resource status", f"all values in {VALID_RESOURCE_STATUSES}")


def check_existing_new_pairs(df: pl.DataFrame, result: ValidationResult) -> None:
    """Existing and new must always appear as pairs for the same (fca_number, zone_id)."""
    admin = df.filter(pl.col("resource_status").is_in(["existing", "new"]))
    if admin.height == 0:
        result.passed("Existing/new pairs", "no administrative pricing rows")
        return

    errors: list[str] = []
    for (fca, zone), group in admin.group_by("fca_number", "capacity_zone_id"):
        statuses = set(group["resource_status"].to_list())
        if statuses != {"existing", "new"}:
            errors.append(
                f"FCA {fca} zone {zone}: has {statuses}, expected both existing and new"
            )

    has_all_overlap = df.filter(pl.col("resource_status") == "all")
    for (fca, zone), _ in admin.group_by("fca_number", "capacity_zone_id"):
        overlap = has_all_overlap.filter(
            (pl.col("fca_number") == fca) & (pl.col("capacity_zone_id") == zone)
        )
        if overlap.height > 0:
            errors.append(
                f"FCA {fca} zone {zone}: has both 'all' and existing/new rows"
            )

    if errors:
        for e in errors:
            result.error("Existing/new pairs", e)
    else:
        pairs = admin.select("fca_number", "capacity_zone_id").unique().height
        result.passed(
            "Existing/new pairs", f"{pairs} admin-priced (fca, zone) pairs, all valid"
        )


def check_price_ranges(df: pl.DataFrame, result: ValidationResult) -> None:
    prices = df["clearing_price_per_kw_month"]
    pmin = float(prices.min())  # type: ignore[arg-type]
    pmax = float(prices.max())  # type: ignore[arg-type]
    pmean = float(prices.mean())  # type: ignore[arg-type]

    if pmin < PRICE_FLOOR:
        result.error("Price range", f"min ${pmin:.3f} is negative")
    elif pmax > PRICE_CEILING:
        result.warn(
            "Price range", f"max ${pmax:.3f} exceeds ${PRICE_CEILING:.0f}/kW-mo ceiling"
        )
    else:
        result.passed(
            "Price range", f"${pmin:.3f}–${pmax:.3f}/kW-mo (mean ${pmean:.3f})"
        )

    summary = (
        df.group_by("capacity_zone_name")
        .agg(
            pl.col("clearing_price_per_kw_month").min().alias("min"),
            pl.col("clearing_price_per_kw_month").mean().alias("mean"),
            pl.col("clearing_price_per_kw_month").max().alias("max"),
        )
        .sort("capacity_zone_name")
    )
    lines = []
    for row in summary.iter_rows(named=True):
        lines.append(
            f"{row['capacity_zone_name']}: ${row['min']:.3f}–${row['max']:.3f} (mean ${row['mean']:.3f})"
        )
    result.passed("Price summary by zone", "; ".join(lines))


def check_zone_ids(df: pl.DataFrame, result: ValidationResult) -> None:
    zone_ids = set(df["capacity_zone_id"].unique().to_list())
    invalid = zone_ids - CANONICAL_ZONE_IDS
    if invalid:
        result.error("Zone IDs", f"non-canonical zone IDs: {invalid}")
    else:
        result.passed("Zone IDs", f"all zone IDs in {sorted(zone_ids)}")


def check_cross_check_values(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for (fca, zone, status), expected_price in CROSS_CHECK_VALUES.items():
        row = df.filter(
            (pl.col("fca_number") == fca)
            & (pl.col("capacity_zone_id") == zone)
            & (pl.col("resource_status") == status)
        )
        if row.height == 0:
            errors.append(f"FCA {fca} zone {zone} status={status}: row not found")
            continue
        actual = float(row["clearing_price_per_kw_month"][0])
        if abs(actual - expected_price) > 0.001:
            errors.append(
                f"FCA {fca} zone {zone}: expected ${expected_price:.3f}, got ${actual:.3f}"
            )
    if errors:
        for e in errors:
            result.error("Cross-check", e)
    else:
        result.passed(
            "Cross-check",
            f"{len(CROSS_CHECK_VALUES)} known values verified",
        )


def check_ccp_dates(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []

    for row in df.iter_rows(named=True):
        fca = row["fca_number"]
        start = row["ccp_start"]
        end = row["ccp_end"]

        expected_start = date(2009 + fca, 6, 1)
        expected_end = date(2010 + fca, 5, 31)

        if start != expected_start:
            errors.append(f"FCA {fca}: ccp_start={start}, expected {expected_start}")
        if end != expected_end:
            errors.append(f"FCA {fca}: ccp_end={end}, expected {expected_end}")

    unique_errors = list(dict.fromkeys(errors))
    if unique_errors:
        for e in unique_errors:
            result.error("CCP dates", e)
    else:
        fca_min = df["fca_number"].min()
        fca_max = df["fca_number"].max()
        result.passed(
            "CCP dates",
            f"all dates valid and sequential (FCA {fca_min}–{fca_max})",
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
        description="Validate ISO-NE FCA clearing price CSV."
    )
    parser.add_argument(
        "--path-csv",
        type=Path,
        required=True,
        help="Path to fca_clearing_prices.csv.",
    )
    args = parser.parse_args()
    csv_path = args.path_csv.resolve()

    if not csv_path.is_file():
        print(f"File not found: {csv_path}", file=sys.stderr)
        return 1

    df = pl.read_csv(
        csv_path,
        comment_prefix="#",
        try_parse_dates=True,
    )

    print("ISO-NE FCA Clearing Price Validation Report")
    print(f"{'=' * 60}")
    print(f"Source: {csv_path}")
    print(f"Rows: {df.height}")
    print(f"FCAs: {sorted(df['fca_number'].unique().to_list())}")
    print(f"{'=' * 60}\n")

    result = ValidationResult()

    check_schema(df, result)
    check_no_nulls(df, result)
    check_no_duplicates(df, result)
    check_all_fcas_present(df, result)
    check_resource_status(df, result)
    check_existing_new_pairs(df, result)
    check_price_ranges(df, result)
    check_zone_ids(df, result)
    check_cross_check_values(df, result)
    check_ccp_dates(df, result)

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
