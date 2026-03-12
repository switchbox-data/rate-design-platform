#!/usr/bin/env python3
"""Validate ISO-NE ARA clearing price parquet: schema, completeness, ranges, domain invariants.

Runs QA checks on local Hive-partitioned parquet under path_local_parquet.
Use after fetch and before upload.

Usage:
    uv run python data/isone/capacity/ara/validate_isone_ara.py \
        --path-local-parquet data/isone/capacity/ara/parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

REQUIRED_COLUMNS = {
    "cp": pl.String,
    "ara_number": pl.Int8,
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

NULLABLE_COLUMNS = {
    "iso_supply_mw": pl.Float64,
    "iso_demand_mw": pl.Float64,
}

ALL_COLUMNS = {**REQUIRED_COLUMNS, **NULLABLE_COLUMNS}

CANONICAL_ZONE_IDS = {8500, 8503, 8505, 8506}
VALID_ARA_NUMBERS = {1, 2, 3}
VALID_ENTITY_TYPES = {"zone", "external_interface"}

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
    for col, expected_type in ALL_COLUMNS.items():
        if col not in df.columns:
            errors.append(f"missing column {col}")
        elif df.schema[col] != expected_type:
            errors.append(f"{col}: expected {expected_type}, got {df.schema[col]}")
    extra = set(df.columns) - set(ALL_COLUMNS)
    if extra:
        errors.append(f"unexpected columns: {extra}")

    if errors:
        for e in errors:
            result.error("Schema", e)
    else:
        cols = ", ".join(f"{c} ({t})" for c, t in ALL_COLUMNS.items())
        result.passed("Schema", cols)


def check_no_nulls_required(df: pl.DataFrame, result: ValidationResult) -> None:
    null_cols = [
        col
        for col in REQUIRED_COLUMNS
        if col in df.columns and df[col].null_count() > 0
    ]
    if null_cols:
        for col in null_cols:
            result.error("Nulls (required)", f"{col}: {df[col].null_count()} nulls")
    else:
        result.passed("Nulls (required)", "zero nulls in required columns")


def check_no_duplicates(df: pl.DataFrame, result: ValidationResult) -> None:
    keys = ["cp", "ara_number", "entity_type", "entity_name"]
    n_total = df.height
    n_unique = df.select(keys).unique().height
    if n_unique != n_total:
        result.error("Uniqueness", f"{n_total - n_unique} duplicate rows on {keys}")
    else:
        result.passed(
            "Uniqueness",
            f"all {n_total} rows unique on (cp, ara_number, entity_type, entity_name)",
        )


def check_price_ranges(df: pl.DataFrame, result: ValidationResult) -> None:
    prices = df["clearing_price_per_kw_month"]
    pmin = float(prices.min())  # type: ignore[arg-type]
    pmax = float(prices.max())  # type: ignore[arg-type]
    pmean = float(prices.mean())  # type: ignore[arg-type]

    n_nan = prices.is_nan().sum()
    n_inf = prices.is_infinite().sum()
    if n_nan > 0:
        result.error("Price integrity", f"{n_nan} NaN values")
    if n_inf > 0:
        result.error("Price integrity", f"{n_inf} infinite values")
    if n_nan == 0 and n_inf == 0:
        result.passed("Price integrity", "no NaN or Inf values")

    if pmin < PRICE_FLOOR:
        result.error(
            "Price range",
            f"min ${pmin:.4f} is negative (prices should be >= 0)",
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
    actual_ids = set(df["capacity_zone_id"].unique().to_list())
    unexpected = actual_ids - CANONICAL_ZONE_IDS
    if unexpected:
        result.warn("Zone IDs", f"unexpected zone IDs: {unexpected}")
    else:
        result.passed(
            "Zone IDs", f"all IDs in canonical set {sorted(CANONICAL_ZONE_IDS)}"
        )


def check_ara_numbers(df: pl.DataFrame, result: ValidationResult) -> None:
    actual = set(df["ara_number"].unique().to_list())
    unexpected = actual - VALID_ARA_NUMBERS
    if unexpected:
        result.error("ARA numbers", f"unexpected values: {unexpected}")
    else:
        result.passed("ARA numbers", f"all values in {sorted(VALID_ARA_NUMBERS)}")


def check_entity_types(df: pl.DataFrame, result: ValidationResult) -> None:
    actual = set(df["entity_type"].unique().to_list())
    unexpected = actual - VALID_ENTITY_TYPES
    if unexpected:
        result.error("Entity types", f"unexpected values: {unexpected}")
    else:
        result.passed("Entity types", f"values: {sorted(actual)}")


def summarize_cp_ara(df: pl.DataFrame, result: ValidationResult) -> None:
    """Summarize which CPs have which ARAs."""
    cp_ara = df.select("cp", "ara_number").unique().sort("cp", "ara_number")

    cps = sorted(df["cp"].unique().to_list())
    lines: list[str] = []
    for cp in cps:
        aras = sorted(cp_ara.filter(pl.col("cp") == cp)["ara_number"].to_list())
        lines.append(f"{cp}: ARA {','.join(str(a) for a in aras)}")

    result.passed(
        "CP/ARA coverage",
        f"{len(cps)} CPs — " + "; ".join(lines),
    )


def summarize_zones_and_interfaces(df: pl.DataFrame, result: ValidationResult) -> None:
    zones = sorted(
        df.filter(pl.col("entity_type") == "zone")["entity_name"].unique().to_list()
    )
    interfaces = sorted(
        df.filter(pl.col("entity_type") == "external_interface")["entity_name"]
        .unique()
        .to_list()
    )
    result.passed("Zones", ", ".join(zones))
    result.passed("External interfaces", ", ".join(interfaces))

    by_zone = (
        df.filter(pl.col("entity_type") == "zone")
        .group_by("entity_name")
        .agg(
            pl.col("clearing_price_per_kw_month").min().alias("min_price"),
            pl.col("clearing_price_per_kw_month").mean().alias("mean_price"),
            pl.col("clearing_price_per_kw_month").max().alias("max_price"),
        )
        .sort("entity_name")
    )
    for row in by_zone.iter_rows(named=True):
        result.passed(
            f"Price: {row['entity_name']}",
            f"${row['min_price']:.4f}–${row['max_price']:.4f} (mean ${row['mean_price']:.4f})",
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

    parser = argparse.ArgumentParser(description="Validate ISO-NE ARA parquet.")
    parser.add_argument(
        "--path-local-parquet",
        type=Path,
        required=True,
        help="Local parquet root (cp=*/ara=*/data.parquet).",
    )
    args = parser.parse_args()
    root = args.path_local_parquet.resolve()

    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    parquet_files = list(root.glob("cp=*/ara_number=*/data.parquet"))
    if not parquet_files:
        print(f"No parquet partitions found under {root}", file=sys.stderr)
        return 1

    df = pl.read_parquet(
        root / "**/*.parquet",
        hive_partitioning=True,
    )

    print("ISO-NE ARA Validation Report")
    print(f"{'=' * 60}")
    print(f"Source: {root}")
    print(f"Partitions: {len(parquet_files)}")
    print(f"Rows: {df.height}")
    print(f"{'=' * 60}\n")

    result = ValidationResult()

    check_schema(df, result)
    check_no_nulls_required(df, result)
    check_no_duplicates(df, result)
    check_price_ranges(df, result)
    check_zone_ids(df, result)
    check_ara_numbers(df, result)
    check_entity_types(df, result)
    summarize_cp_ara(df, result)
    summarize_zones_and_interfaces(df, result)

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
