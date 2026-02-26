#!/usr/bin/env python3
"""Validate EIA-877 heating fuel price parquet: schema, completeness, ranges, continuity.

Runs a QA battery on local Hive-partitioned parquet under path_local_parquet.
Use after fetch and before upload.

Usage:
    uv run python data/eia/heating_fuel_prices/validate_heating_fuel_prices_parquet.py \
        --path-local-parquet data/eia/heating_fuel_prices/parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

EXPECTED_PRODUCTS = {"heating_oil", "propane"}
EXPECTED_PRICE_TYPES = {"residential", "wholesale"}
EXPECTED_DATA_COLS = {
    "state": pl.String,
    "price_type": pl.String,
    "price_per_gallon": pl.Float64,
}
EXPECTED_PARTITION_COLS = {
    "product": pl.String,
    "year": pl.Int64,
    "month": pl.Int64,
}

HEATING_SEASON_MONTHS = {10, 11, 12, 1, 2, 3}
OFF_SEASON_MONTHS = {4, 5, 6, 7, 8, 9}

# Historical residential retail range: roughly $0.50–$6/gallon.
# Wholesale can dip lower.
PRICE_FLOOR = 0.0
PRICE_CEILING = 8.0

# States we expect to have data for both products
STATES_BOTH_PRODUCTS = {
    "CT",
    "DE",
    "IA",
    "IN",
    "KY",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "NC",
    "NE",
    "NH",
    "NJ",
    "NY",
    "OH",
    "PA",
    "RI",
    "VA",
    "VT",
    "WI",
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
    expected = {**EXPECTED_DATA_COLS, **EXPECTED_PARTITION_COLS}
    errors: list[str] = []
    for col, expected_type in expected.items():
        if col not in df.columns:
            errors.append(f"missing column {col}")
        elif df.schema[col] != expected_type:
            errors.append(f"{col}: expected {expected_type}, got {df.schema[col]}")
    extra = set(df.columns) - set(expected)
    if extra:
        errors.append(f"unexpected columns: {extra}")

    if errors:
        for e in errors:
            result.error("Schema", e)
    else:
        cols = ", ".join(f"{c} ({t})" for c, t in expected.items())
        result.passed("Schema", cols)


def check_no_nulls(df: pl.DataFrame, result: ValidationResult) -> None:
    null_cols = [col for col in df.columns if df[col].null_count() > 0]
    if null_cols:
        for col in null_cols:
            result.error("Nulls", f"{col}: {df[col].null_count()} nulls")
    else:
        result.passed("Nulls", "zero nulls across all columns")


def check_no_duplicates(df: pl.DataFrame, result: ValidationResult) -> None:
    keys = ["product", "year", "month", "state", "price_type"]
    n_total = df.height
    n_unique = df.select(keys).unique().height
    if n_unique != n_total:
        result.error("Uniqueness", f"{n_total - n_unique} duplicate rows on {keys}")
    else:
        result.passed("Uniqueness", f"all {n_total} rows unique on {keys}")


def check_dimensions(df: pl.DataFrame, result: ValidationResult) -> None:
    products = set(df["product"].unique().to_list())
    price_types = set(df["price_type"].unique().to_list())
    states = sorted(df["state"].unique().to_list())

    missing_prod = EXPECTED_PRODUCTS - products
    missing_pt = EXPECTED_PRICE_TYPES - price_types

    if missing_prod:
        result.error("Dimensions", f"missing products: {missing_prod}")
    if missing_pt:
        result.error("Dimensions", f"missing price types: {missing_pt}")
    if not missing_prod and not missing_pt:
        result.passed(
            "Dimensions",
            f"products: {sorted(products)}, price_types: {sorted(price_types)}, "
            f"{len(states)} states: {states}",
        )


def check_state_product_coverage(df: pl.DataFrame, result: ValidationResult) -> None:
    """Check that key states have data for both products."""
    state_products = (
        df.select(["state", "product"])
        .unique()
        .group_by("state")
        .agg(pl.col("product").sort())
    )
    sp_map = {
        row["state"]: set(row["product"])
        for row in state_products.iter_rows(named=True)
    }

    missing: list[str] = []
    for st in sorted(STATES_BOTH_PRODUCTS):
        if st not in sp_map:
            missing.append(f"{st}: no data at all")
        elif sp_map[st] != EXPECTED_PRODUCTS:
            missing.append(f"{st}: only {sp_map[st]}")

    if missing:
        for m in missing:
            result.warn("State-product coverage", m)
    else:
        result.passed(
            "State-product coverage",
            f"all {len(STATES_BOTH_PRODUCTS)} key states have both products",
        )


def check_seasonal_completeness(df: pl.DataFrame, result: ValidationResult) -> None:
    """Pre-2024: expect Oct-Mar only. 2024+: expect year-round with possible gaps."""
    years = sorted(df["year"].unique().to_list())
    min_year, max_year = years[0], years[-1]

    pre_2024_off_season = df.filter(
        (pl.col("year") < 2024) & (pl.col("month").is_in(list(OFF_SEASON_MONTHS)))
    )
    if pre_2024_off_season.height > 0:
        result.warn(
            "Seasonal completeness",
            f"unexpected off-season data pre-2024: {pre_2024_off_season.height} rows",
        )
    else:
        result.passed(
            "Seasonal completeness (pre-2024)",
            f"{min_year}-2023: data only in heating season (Oct-Mar) as expected",
        )

    post_2024 = df.filter(pl.col("year") >= 2024)
    if post_2024.height > 0:
        months_present = set(post_2024["month"].unique().to_list())
        off_season_present = months_present & OFF_SEASON_MONTHS
        if off_season_present:
            result.passed(
                "Seasonal completeness (2024+)",
                f"off-season months present: {sorted(off_season_present)}",
            )
        else:
            result.warn(
                "Seasonal completeness (2024+)",
                "no off-season months found in 2024+ data",
            )

    # Check heating season completeness for a few recent years
    gaps: list[str] = []
    for y in range(max(min_year, 2020), max_year + 1):
        year_months = set(df.filter(pl.col("year") == y)["month"].unique().to_list())
        expected_hs = HEATING_SEASON_MONTHS - year_months
        if expected_hs:
            gaps.append(f"{y}: missing heating-season months {sorted(expected_hs)}")

    if gaps:
        for g in gaps:
            result.warn("Heating season gaps (2020+)", g)
    else:
        result.passed(
            "Heating season gaps (2020+)",
            "all heating-season months present for 2020+",
        )


def check_price_ranges(df: pl.DataFrame, result: ValidationResult) -> None:
    prices = df["price_per_gallon"]
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
        result.error("Price range", f"min ${pmin:.3f} is negative")
    elif pmax > PRICE_CEILING:
        result.warn(
            "Price range",
            f"max ${pmax:.2f} exceeds ${PRICE_CEILING:.0f}/gal ceiling — verify",
        )
    else:
        result.passed("Price range", f"${pmin:.3f}–${pmax:.3f}/gal (mean ${pmean:.3f})")

    summary = (
        df.group_by("product", "price_type")
        .agg(
            pl.col("price_per_gallon").min().alias("min"),
            pl.col("price_per_gallon").mean().alias("mean"),
            pl.col("price_per_gallon").max().alias("max"),
            pl.len().alias("n"),
        )
        .sort("product", "price_type")
    )
    for row in summary.iter_rows(named=True):
        result.passed(
            f"  {row['product']} / {row['price_type']}",
            f"${row['min']:.3f}–${row['max']:.3f} (mean ${row['mean']:.3f}, n={row['n']})",
        )


def check_wholesale_below_residential(
    df: pl.DataFrame, result: ValidationResult
) -> None:
    """Wholesale prices should generally be below residential."""
    joined = (
        df.filter(pl.col("price_type") == "residential")
        .select(["product", "year", "month", "state", "price_per_gallon"])
        .rename({"price_per_gallon": "residential"})
        .join(
            df.filter(pl.col("price_type") == "wholesale")
            .select(["product", "year", "month", "state", "price_per_gallon"])
            .rename({"price_per_gallon": "wholesale"}),
            on=["product", "year", "month", "state"],
            how="inner",
        )
    )

    if joined.height == 0:
        result.warn("Wholesale < residential", "no paired observations to compare")
        return

    violations = joined.filter(pl.col("wholesale") > pl.col("residential"))
    pct = violations.height / joined.height * 100

    if pct > 10:
        result.warn(
            "Wholesale < residential",
            f"{violations.height}/{joined.height} pairs ({pct:.1f}%) have wholesale > residential",
        )
    else:
        result.passed(
            "Wholesale < residential",
            f"holds in {joined.height - violations.height}/{joined.height} pairs "
            f"({100 - pct:.1f}%)",
        )


def check_cross_year_continuity(df: pl.DataFrame, result: ValidationResult) -> None:
    """Flag large year-over-year jumps in annual mean residential prices."""
    residential = df.filter(pl.col("price_type") == "residential")
    annual = (
        residential.group_by("product", "state", "year")
        .agg(pl.col("price_per_gallon").mean().alias("avg_price"))
        .sort("product", "state", "year")
    )

    jumps: list[str] = []
    for (product, state), group in annual.group_by(
        "product", "state", maintain_order=True
    ):
        prices = group["avg_price"].to_list()
        years = group["year"].to_list()
        for i in range(1, len(prices)):
            if prices[i - 1] is None or prices[i - 1] == 0:
                continue
            ratio = prices[i] / prices[i - 1]
            if ratio > 3.0 or ratio < 0.33:
                jumps.append(
                    f"{state} {product}: {years[i - 1]}->{years[i]} = {ratio:.1f}x "
                    f"(${prices[i - 1]:.2f}->${prices[i]:.2f})"
                )

    if jumps:
        for j in jumps:
            result.warn("Year-over-year continuity", j)
    else:
        n_transitions = sum(
            annual.filter((pl.col("product") == p) & (pl.col("state") == s)).height - 1
            for p in annual["product"].unique().to_list()
            for s in annual.filter(pl.col("product") == p)["state"].unique().to_list()
        )
        result.passed(
            "Year-over-year continuity",
            f"no jumps >3x across {n_transitions} year transitions",
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
        print(f"  {icon}  {name:<{max_name}}  {detail}")


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate EIA-877 heating fuel price parquet."
    )
    parser.add_argument(
        "--path-local-parquet",
        type=Path,
        required=True,
        help="Local parquet root (product=*/year=*/month=*/data.parquet).",
    )
    args = parser.parse_args()
    root = args.path_local_parquet.resolve()

    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    parquet_files = list(root.glob("product=*/year=*/month=*/data.parquet"))
    if not parquet_files:
        print(f"No parquet partitions found under {root}", file=sys.stderr)
        return 1

    df = pl.read_parquet(root / "**/*.parquet", hive_partitioning=True)

    print("EIA-877 Heating Fuel Price Validation Report")
    print("=" * 60)
    print(f"Source: {root}")
    print(f"Partitions: {len(parquet_files)}")
    print(f"Rows: {df.height:,}")
    print("=" * 60)
    print()

    result = ValidationResult()

    check_schema(df, result)
    check_no_nulls(df, result)
    check_no_duplicates(df, result)
    check_dimensions(df, result)
    check_state_product_coverage(df, result)
    check_seasonal_completeness(df, result)
    check_price_ranges(df, result)
    check_wholesale_below_residential(df, result)
    check_cross_year_continuity(df, result)

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
