#!/usr/bin/env python3
"""Validate ISO-NE CELT CSO parquet: schema, completeness, ranges, subtotal consistency.

Runs a QA battery on local Hive-partitioned parquet under path_local_parquet.
Use after fetch and before upload.

Usage:
    uv run python data/isone/capacity/cso/validate_isone_celt_cso.py \
        --path-local-parquet data/isone/capacity/cso/parquet
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import polars as pl

EXPECTED_SCHEMA = {
    "celt_year": pl.Int16,
    "fca_number": pl.Int16,
    "ccp": pl.String,
    "state": pl.String,
    "resource_type": pl.String,
    "summer_cso_mw": pl.Float64,
    "winter_cso_mw": pl.Float64,
}

EXPECTED_ZONES = {"CT", "ME", "NEMA", "NH", "RI", "SEMA", "VT", "WCMA"}

EXPECTED_RESOURCE_TYPES = {
    "Active DCR",
    "Passive DCR",
    "Gen Intermittent",
    "Gen Non-Intermittent",
    "DCR Total",
    "Gen Total",
    "Total",
}

# Subtotal relations: child1 + child2 = parent
SUBTOTAL_RELATIONS = [
    ("Active DCR", "Passive DCR", "DCR Total"),
    ("Gen Intermittent", "Gen Non-Intermittent", "Gen Total"),
    ("DCR Total", "Gen Total", "Total"),
]

# Cross-check values: (celt_year, fca_number, state, column, expected_value)
KNOWN_VALUES = [
    (2025, 15, "RI", "summer_cso_mw", 2158.971),
    (2025, 15, "RI", "winter_cso_mw", 2194.808),
    (2025, 16, "RI", "winter_cso_mw", 2166.722),
]

FLOAT_TOL = 0.1  # MW tolerance for subtotal checks


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


def check_completeness(df: pl.DataFrame, result: ValidationResult) -> None:
    """Every (celt_year, fca_number) should have all 8 load zones."""
    errors: list[str] = []
    for (cy, fca), group in df.group_by("celt_year", "fca_number", maintain_order=True):
        zones = set(group["state"].unique().to_list())
        missing = EXPECTED_ZONES - zones
        if missing:
            errors.append(f"celt_year={cy} FCA {fca}: missing zones {sorted(missing)}")
    if errors:
        for e in errors:
            result.error("Zone completeness", e)
    else:
        n_groups = df.select("celt_year", "fca_number").unique().height
        result.passed(
            "Zone completeness",
            f"all 8 zones present in {n_groups} (celt_year, FCA) groups",
        )

    resource_types = set(df["resource_type"].unique().to_list())
    missing_rt = EXPECTED_RESOURCE_TYPES - resource_types
    extra_rt = resource_types - EXPECTED_RESOURCE_TYPES
    if missing_rt:
        result.error("Resource types", f"missing: {sorted(missing_rt)}")
    if extra_rt:
        result.error("Resource types", f"unexpected: {sorted(extra_rt)}")
    if not missing_rt and not extra_rt:
        result.passed(
            "Resource types",
            f"all {len(EXPECTED_RESOURCE_TYPES)} expected types present",
        )


def check_positive_csos(df: pl.DataFrame, result: ValidationResult) -> None:
    """CSO values should be >= 0 (some subtypes like SEASONAL PEAK can be 0)."""
    totals = df.filter(pl.col("resource_type") == "Total")
    neg_summer = totals.filter(pl.col("summer_cso_mw") < 0).height
    neg_winter = totals.filter(pl.col("winter_cso_mw") < 0).height
    zero_summer = totals.filter(pl.col("summer_cso_mw") == 0).height
    zero_winter = totals.filter(pl.col("winter_cso_mw") == 0).height

    if neg_summer > 0 or neg_winter > 0:
        result.error(
            "Positive CSOs",
            f"negative Total CSOs: {neg_summer} summer, {neg_winter} winter",
        )
    elif zero_summer > 0 or zero_winter > 0:
        result.warn(
            "Positive CSOs",
            f"zero Total CSOs: {zero_summer} summer, {zero_winter} winter",
        )
    else:
        smin = float(totals["summer_cso_mw"].min())  # type: ignore[arg-type]
        smax = float(totals["summer_cso_mw"].max())  # type: ignore[arg-type]
        wmin = float(totals["winter_cso_mw"].min())  # type: ignore[arg-type]
        wmax = float(totals["winter_cso_mw"].max())  # type: ignore[arg-type]
        result.passed(
            "Positive CSOs",
            f"Total summer: {smin:.1f}–{smax:.1f} MW, winter: {wmin:.1f}–{wmax:.1f} MW",
        )


def check_subtotals(df: pl.DataFrame, result: ValidationResult) -> None:
    """Verify additive subtotal relationships within each (celt_year, fca_number, state)."""
    errors: list[str] = []
    n_checked = 0

    for (cy, fca, state), group in df.group_by(
        "celt_year", "fca_number", "state", maintain_order=True
    ):
        rt_map: dict[str, dict[str, float]] = {}
        for row in group.iter_rows(named=True):
            rt_map[row["resource_type"]] = {
                "summer": row["summer_cso_mw"],
                "winter": row["winter_cso_mw"],
            }

        for child1, child2, parent in SUBTOTAL_RELATIONS:
            if child1 not in rt_map or child2 not in rt_map or parent not in rt_map:
                continue
            n_checked += 1
            for season in ("summer", "winter"):
                expected = rt_map[child1][season] + rt_map[child2][season]
                actual = rt_map[parent][season]
                if not math.isclose(actual, expected, abs_tol=FLOAT_TOL):
                    errors.append(
                        f"celt={cy} FCA {fca} {state} {season}: "
                        f"{child1} ({rt_map[child1][season]:.3f}) + "
                        f"{child2} ({rt_map[child2][season]:.3f}) = {expected:.3f} "
                        f"!= {parent} ({actual:.3f})"
                    )

    if errors:
        for e in errors[:10]:
            result.error("Subtotals", e)
        if len(errors) > 10:
            result.error("Subtotals", f"... and {len(errors) - 10} more")
    else:
        result.passed(
            "Subtotals", f"all {n_checked} subtotal checks pass (tol={FLOAT_TOL} MW)"
        )


def check_known_values(df: pl.DataFrame, result: ValidationResult) -> None:
    """Cross-check against known values from manual inspection."""
    for celt_year, fca, state, col, expected in KNOWN_VALUES:
        match = df.filter(
            (pl.col("celt_year") == celt_year)
            & (pl.col("fca_number") == fca)
            & (pl.col("state") == state)
            & (pl.col("resource_type") == "Total")
        )
        if match.height == 0:
            result.error(
                "Known values",
                f"celt={celt_year} FCA {fca} {state}: no matching row",
            )
            continue
        actual = float(match[col][0])
        if math.isclose(actual, expected, abs_tol=0.01):
            result.passed(
                "Known values",
                f"celt={celt_year} FCA {fca} {state} {col}: {actual:.3f} == {expected:.3f}",
            )
        else:
            result.error(
                "Known values",
                f"celt={celt_year} FCA {fca} {state} {col}: {actual:.3f} != {expected:.3f}",
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
    parser = argparse.ArgumentParser(description="Validate ISO-NE CELT CSO parquet.")
    parser.add_argument(
        "--path-local-parquet",
        type=Path,
        required=True,
        help="Local parquet root (celt_year=*/data.parquet).",
    )
    args = parser.parse_args()
    root = args.path_local_parquet.resolve()

    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    parquet_files = list(root.glob("celt_year=*/data.parquet"))
    if not parquet_files:
        print(f"No parquet partitions found under {root}", file=sys.stderr)
        return 1

    df = pl.read_parquet(root / "**/*.parquet", hive_partitioning=True).cast(
        {"celt_year": pl.Int16}
    )

    print("ISO-NE CELT CSO Validation Report")
    print(f"{'=' * 60}")
    print(f"Source: {root}")
    print(f"Partitions: {len(parquet_files)}")
    print(f"Rows: {df.height}")

    celt_years = sorted(df["celt_year"].unique().to_list())
    print(f"CELT years: {celt_years}")

    for cy in celt_years:
        cy_df = df.filter(pl.col("celt_year") == cy)
        fcas = sorted(cy_df["fca_number"].unique().to_list())
        zones = sorted(cy_df["state"].unique().to_list())
        print(f"  {cy}: {cy_df.height} rows, FCAs {fcas}, zones {zones}")

    print(f"{'=' * 60}\n")

    result = ValidationResult()
    check_schema(df, result)
    check_no_nulls(df, result)
    check_completeness(df, result)
    check_positive_csos(df, result)
    check_subtotals(df, result)
    check_known_values(df, result)

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
