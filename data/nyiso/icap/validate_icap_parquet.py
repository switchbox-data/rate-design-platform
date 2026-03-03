#!/usr/bin/env python3
"""Validate NYISO ICAP clearing price parquet: schema, completeness, ranges, domain invariants.

Runs a QA battery on local Hive-partitioned parquet under path_local_parquet.
Use after fetch and before upload.

Usage:
    uv run python data/nyiso/icap/validate_icap_parquet.py \
        --path-local-parquet data/nyiso/icap/parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

EXPECTED_LOCALITIES = {"NYCA", "GHIJ", "NYC", "LI"}
EXPECTED_AUCTION_TYPES = {"Spot", "Monthly", "Strip"}
EXPECTED_SCHEMA = {
    "year": pl.Int16,
    "month": pl.UInt8,
    "locality": pl.Categorical,
    "auction_type": pl.Categorical,
    "price_per_kw_month": pl.Float64,
}

# NYISO capability periods: summer = May–Oct, winter = Nov–Apr
SUMMER_MONTHS = {5, 6, 7, 8, 9, 10}
WINTER_MONTHS = {1, 2, 3, 4, 11, 12}

# ICAP prices are $/kW-month. Historical range is roughly $0–$25.
# Prices can be very low (near zero) in winter for non-NYC zones but should
# never be negative.
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
    null_cols = [col for col in df.columns if df[col].null_count() > 0]
    if null_cols:
        for col in null_cols:
            result.error("Nulls", f"{col}: {df[col].null_count()} nulls")
    else:
        result.passed("Nulls", "zero nulls across all columns")


def check_no_duplicates(df: pl.DataFrame, result: ValidationResult) -> None:
    keys = ["year", "month", "locality", "auction_type"]
    n_total = df.height
    n_unique = df.select(keys).unique().height
    if n_unique != n_total:
        result.error("Uniqueness", f"{n_total - n_unique} duplicate rows")
    else:
        result.passed(
            "Uniqueness",
            f"all {n_total} rows have unique (year, month, locality, auction_type)",
        )


def check_completeness(df: pl.DataFrame, result: ValidationResult) -> None:
    """Every (year, month) should have exactly 12 rows: 4 localities x 3 auction types."""
    by_ym = df.group_by("year", "month").agg(pl.len().alias("n"))
    bad = by_ym.filter(pl.col("n") != 12)
    if bad.height > 0:
        for row in bad.iter_rows(named=True):
            result.error(
                "Completeness",
                f"year={row['year']} month={row['month']}: {row['n']} rows (expected 12)",
            )
    else:
        result.passed(
            "Completeness",
            "every (year, month) has 12 rows (4 localities x 3 auctions)",
        )

    localities = set(df["locality"].unique().to_list())
    auction_types = set(df["auction_type"].unique().to_list())
    missing_loc = EXPECTED_LOCALITIES - localities
    missing_at = EXPECTED_AUCTION_TYPES - auction_types
    if missing_loc:
        result.error("Completeness", f"missing localities: {missing_loc}")
    if missing_at:
        result.error("Completeness", f"missing auction types: {missing_at}")
    if not missing_loc and not missing_at:
        result.passed(
            "Dimensions",
            f"localities: {sorted(localities)}, auctions: {sorted(auction_types)}",
        )


def check_full_year_coverage(df: pl.DataFrame, result: ValidationResult) -> None:
    """Each year should have all 12 months."""
    gaps: list[str] = []
    for y in sorted(df["year"].unique().to_list()):
        months = set(df.filter(pl.col("year") == y)["month"].unique().to_list())
        missing = set(range(1, 13)) - months
        if missing:
            gaps.append(f"{y} missing months {sorted(missing)}")
    if gaps:
        for g in gaps:
            result.error("Year coverage", g)
    else:
        years = sorted(df["year"].unique().to_list())
        result.passed(
            "Year coverage",
            f"{len(years)} years ({years[0]}–{years[-1]}), all with 12 months",
        )


def check_price_ranges(df: pl.DataFrame, result: ValidationResult) -> None:
    prices = df["price_per_kw_month"]
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
            "Price range", f"min ${pmin:.2f} is negative (ICAP prices should be >= 0)"
        )
    elif pmax > PRICE_CEILING:
        result.warn(
            "Price range",
            f"max ${pmax:.2f} exceeds ${PRICE_CEILING:.0f}/kW-mo ceiling — verify",
        )
    else:
        result.passed(
            "Price range", f"${pmin:.2f}–${pmax:.2f}/kW-mo (mean ${pmean:.2f})"
        )

    # Per-locality price summary
    summary = (
        df.group_by("locality")
        .agg(
            pl.col("price_per_kw_month").min().alias("min"),
            pl.col("price_per_kw_month").mean().alias("mean"),
            pl.col("price_per_kw_month").max().alias("max"),
        )
        .sort("locality")
    )
    lines = []
    for row in summary.iter_rows(named=True):
        lines.append(
            f"{row['locality']}: ${row['min']:.2f}–${row['max']:.2f} (mean ${row['mean']:.2f})"
        )
    result.passed("Price summary by locality", "; ".join(lines))


def check_locality_ordering(df: pl.DataFrame, result: ValidationResult) -> None:
    """NYC Spot >= GHIJ Spot >= NYCA Spot (nested constrained localities)."""
    spot = df.filter(pl.col("auction_type") == "Spot")
    violations_nyc_ghij = 0
    violations_ghij_nyca = 0
    n_months = 0

    for (_, _), group in spot.group_by("year", "month", maintain_order=True):
        prices = {
            row["locality"]: row["price_per_kw_month"]
            for row in group.iter_rows(named=True)
        }
        if not {"NYC", "GHIJ", "NYCA"}.issubset(prices):
            continue
        n_months += 1
        if prices["NYC"] < prices["GHIJ"]:
            violations_nyc_ghij += 1
        if prices["GHIJ"] < prices["NYCA"]:
            violations_ghij_nyca += 1

    if n_months == 0:
        return

    threshold = max(1, int(n_months * 0.10))
    ok_nyc = violations_nyc_ghij <= threshold
    ok_ghij = violations_ghij_nyca <= threshold

    if ok_nyc and ok_ghij:
        result.passed(
            "Locality ordering (Spot)",
            f"NYC >= GHIJ >= NYCA holds in {n_months - violations_nyc_ghij}/{n_months} "
            f"and {n_months - violations_ghij_nyca}/{n_months} months respectively",
        )
    if not ok_nyc:
        result.warn(
            "Locality ordering",
            f"NYC Spot < GHIJ Spot in {violations_nyc_ghij}/{n_months} months",
        )
    if not ok_ghij:
        result.warn(
            "Locality ordering",
            f"GHIJ Spot < NYCA Spot in {violations_ghij_nyca}/{n_months} months",
        )


def check_seasonal_pattern(df: pl.DataFrame, result: ValidationResult) -> None:
    """Summer Strip prices should generally exceed winter Strip for NYC."""
    nyc_strip = df.filter(
        (pl.col("locality") == "NYC") & (pl.col("auction_type") == "Strip")
    )
    if nyc_strip.height == 0:
        return

    anomalies: list[str] = []
    normal: list[str] = []
    for y in sorted(nyc_strip["year"].unique().to_list()):
        year_data = nyc_strip.filter(pl.col("year") == y)
        summer = year_data.filter(pl.col("month").is_in(list(SUMMER_MONTHS)))
        winter = year_data.filter(pl.col("month").is_in(list(WINTER_MONTHS)))
        if summer.height == 0 or winter.height == 0:
            continue
        summer_mean_raw = summer["price_per_kw_month"].mean()
        winter_mean_raw = winter["price_per_kw_month"].mean()
        if summer_mean_raw is None or winter_mean_raw is None:
            continue
        summer_mean = float(summer_mean_raw)  # type: ignore[arg-type]
        winter_mean = float(winter_mean_raw)  # type: ignore[arg-type]
        if summer_mean < winter_mean:
            anomalies.append(
                f"{y}: summer ${summer_mean:.2f} < winter ${winter_mean:.2f}"
            )
        else:
            normal.append(f"{y}: summer ${summer_mean:.2f} > winter ${winter_mean:.2f}")

    if anomalies:
        for a in anomalies:
            result.warn("Seasonal pattern (NYC Strip)", a)
    years_checked = len(anomalies) + len(normal)
    result.passed(
        "Seasonal pattern (NYC Strip)",
        f"summer > winter in {len(normal)}/{years_checked} years",
    )


def check_cross_year_continuity(df: pl.DataFrame, result: ValidationResult) -> None:
    """Flag large year-over-year jumps in annual average Spot prices per locality."""
    spot = df.filter(pl.col("auction_type") == "Spot")
    annual = (
        spot.group_by("year", "locality")
        .agg(pl.col("price_per_kw_month").mean().alias("avg_price"))
        .sort("locality", "year")
    )

    jumps: list[str] = []
    for loc in sorted(annual["locality"].unique().to_list()):
        loc_data = annual.filter(pl.col("locality") == loc).sort("year")
        prices = loc_data["avg_price"].to_list()
        years = loc_data["year"].to_list()
        for i in range(1, len(prices)):
            if prices[i - 1] == 0 or prices[i - 1] is None:
                continue
            ratio = prices[i] / prices[i - 1]
            if ratio > 5.0 or ratio < 0.2:
                jumps.append(
                    f"{loc}: {years[i - 1]}→{years[i]} = {ratio:.1f}x "
                    f"(${prices[i - 1]:.2f}→${prices[i]:.2f})"
                )

    if jumps:
        for j in jumps:
            result.warn("Year-over-year continuity", j)
    else:
        n_transitions = sum(
            annual.filter(pl.col("locality") == loc).height - 1
            for loc in annual["locality"].unique().to_list()
        )
        result.passed(
            "Year-over-year continuity",
            f"no jumps >5x across {n_transitions} year transitions",
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

    parser = argparse.ArgumentParser(description="Validate NYISO ICAP parquet.")
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

    print("NYISO ICAP Validation Report")
    print(f"{'=' * 60}")
    print(f"Source: {root}")
    print(f"Partitions: {len(parquet_files)}")
    print(f"Rows: {df.height}")
    print(f"{'=' * 60}\n")

    result = ValidationResult()

    check_schema(df, result)
    check_no_nulls(df, result)
    check_no_duplicates(df, result)
    check_completeness(df, result)
    check_full_year_coverage(df, result)
    check_price_ranges(df, result)
    check_locality_ordering(df, result)
    check_seasonal_pattern(df, result)
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
