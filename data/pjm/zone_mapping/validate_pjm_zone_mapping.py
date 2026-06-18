#!/usr/bin/env python3
"""Validate the PJM utility → zone crosswalk mapping CSV.

Runs a QA battery on csv/pjm_utility_zone_mapping.csv: schema, slugs, weights,
zone-code vocabulary, internal crosswalk consistency, and (when the sibling
curated datasets exist) cross-dataset checks that every mapped zone actually
appears in rpm_capacity_prices.csv and fivecp_peaks.csv.

This script is the single enforcement point of zone-code integrity between the
three data/pjm/ datasets.

Usage:
    uv run python data/pjm/zone_mapping/validate_pjm_zone_mapping.py \
        --path-csv data/pjm/zone_mapping/csv/pjm_utility_zone_mapping.csv \
        --path-rpm-csv data/pjm/capacity/rpm/rpm_capacity_prices.csv \
        --path-fivecp-csv data/pjm/capacity/5cp/fivecp_peaks.csv
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

EXPECTED_COLUMNS = [
    "utility",
    "state",
    "dataminer_zone",
    "fivecp_zone_label",
    "price_zone",
    "capacity_weight",
    "pnode_id",
]

EXPECTED_TYPES = {
    "utility": pl.String,
    "state": pl.String,
    "dataminer_zone": pl.String,
    "fivecp_zone_label": pl.String,
    "price_zone": pl.String,
    "capacity_weight": pl.Float64,
    "pnode_id": pl.Int64,
}

KNOWN_STATES = {"md"}

# Canonical zone label → Data Miner legacy transmission-zone code.
# Same source of truth as the crosswalk table in data/pjm/README.md.
CANONICAL_CROSSWALK: dict[str, str] = {
    "AECO": "AE",
    "AEP": "AEP",
    "APS": "AP",
    "ATSI": "ATSI",
    "BGE": "BC",
    "COMED": "CE",
    "DAY": "DAY",
    "DEOK": "DEOK",
    "DOM": "DOM",
    "DPL": "DPL",
    "DUQ": "DUQ",
    "EKPC": "EKPC",
    "JCPL": "JC",
    "METED": "ME",
    "PECO": "PE",
    "PENELEC": "PN",
    "PEPCO": "PEP",
    "PPL": "PL",
    "PSEG": "PS",
    "RECO": "RECO",
    "UGI": "UGI",
}

VALID_DATAMINER_ZONES = frozenset(CANONICAL_CROSSWALK.values())

UTILITY_SLUG_RE = re.compile(r"^[a-z0-9-]+$")


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
    null_cols = [col for col in EXPECTED_COLUMNS if df[col].null_count() > 0]
    if null_cols:
        for col in null_cols:
            result.error("Nulls", f"{col}: {df[col].null_count()} nulls")
    else:
        result.passed("Nulls", "zero nulls in all columns")


def check_utility_slugs(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for utility in df["utility"].unique().to_list():
        if not UTILITY_SLUG_RE.match(utility):
            errors.append(f"utility '{utility}' is not a lowercase [a-z0-9-]+ slug")
    bad_states = set(df["state"].unique().to_list()) - KNOWN_STATES
    if bad_states:
        errors.append(f"unknown states: {sorted(bad_states)} (known: {KNOWN_STATES})")
    if errors:
        for e in errors:
            result.error("Utility slugs", e)
    else:
        utilities = sorted(df["utility"].unique().to_list())
        result.passed("Utility slugs", f"{len(utilities)} utilities: {utilities}")


def check_weights_sum(df: pl.DataFrame, result: ValidationResult) -> None:
    sums = df.group_by("utility").agg(pl.col("capacity_weight").sum().alias("total"))
    bad = sums.filter((pl.col("total") - 1.0).abs() > 1e-9)
    if bad.height > 0:
        for row in bad.iter_rows(named=True):
            result.error(
                "Weights sum", f"{row['utility']}: weights sum to {row['total']}"
            )
    else:
        result.passed(
            "Weights sum",
            f"capacity_weight sums to 1.0 for all {sums.height} utilities",
        )


def check_dataminer_codes(df: pl.DataFrame, result: ValidationResult) -> None:
    codes = set(df["dataminer_zone"].unique().to_list())
    invalid = codes - VALID_DATAMINER_ZONES
    if invalid:
        result.error("Data Miner codes", f"invalid codes: {sorted(invalid)}")
    else:
        result.passed("Data Miner codes", f"all codes in vocabulary: {sorted(codes)}")


def check_internal_consistency(df: pl.DataFrame, result: ValidationResult) -> None:
    """(dataminer_zone, fivecp_zone_label, price_zone) must agree with the crosswalk."""
    errors: list[str] = []
    for row in df.iter_rows(named=True):
        canonical = row["price_zone"]
        if canonical not in CANONICAL_CROSSWALK:
            errors.append(
                f"{row['utility']}: price_zone '{canonical}' not a canonical zone"
            )
            continue
        if row["fivecp_zone_label"] != canonical:
            errors.append(
                f"{row['utility']}: fivecp_zone_label '{row['fivecp_zone_label']}' "
                f"!= price_zone '{canonical}' (both store canonical labels)"
            )
        expected_dm = CANONICAL_CROSSWALK[canonical]
        if row["dataminer_zone"] != expected_dm:
            errors.append(
                f"{row['utility']}: dataminer_zone '{row['dataminer_zone']}' != "
                f"'{expected_dm}' expected for canonical zone {canonical}"
            )
    if errors:
        for e in errors:
            result.error("Internal consistency", e)
    else:
        result.passed(
            "Internal consistency",
            f"all {df.height} rows agree with the canonical crosswalk",
        )


def check_pnode_ids(df: pl.DataFrame, result: ValidationResult) -> None:
    """pnode_id must be a positive integer; rows sharing a fivecp_zone_label must share it."""
    errors: list[str] = []

    # All values must be positive.
    non_positive = df.filter(pl.col("pnode_id") <= 0)
    for row in non_positive.iter_rows(named=True):
        errors.append(f"{row['utility']}: pnode_id {row['pnode_id']} is not positive")

    # Within each fivecp_zone_label the pnode_id must be unique (same zone → same node).
    zone_ids = (
        df.group_by("fivecp_zone_label")
        .agg(pl.col("pnode_id").n_unique().alias("n_unique"))
        .filter(pl.col("n_unique") > 1)
    )
    for row in zone_ids.iter_rows(named=True):
        ids = (
            df.filter(pl.col("fivecp_zone_label") == row["fivecp_zone_label"])[
                "pnode_id"
            ]
            .unique()
            .to_list()
        )
        errors.append(
            f"fivecp_zone_label '{row['fivecp_zone_label']}' has {row['n_unique']} "
            f"different pnode_ids: {sorted(ids)} (all rows for a zone must share one id)"
        )

    if errors:
        for e in errors:
            result.error("Pnode IDs", e)
    else:
        unique_ids = sorted(
            df.select(["fivecp_zone_label", "pnode_id"]).unique().rows()
        )
        result.passed(
            "Pnode IDs",
            f"{len(unique_ids)} zone→pnode mappings: "
            + ", ".join(f"{z}={n}" for z, n in unique_ids),
        )


def check_price_zone_crosswalk(
    df: pl.DataFrame, path_rpm_csv: Path, result: ValidationResult
) -> None:
    """Every price_zone must exist in rpm_capacity_prices.csv for every DY."""
    if not path_rpm_csv.is_file():
        result.warn(
            "Price zone crosswalk",
            f"{path_rpm_csv} not found — skipping (run again once rpm CSV exists)",
        )
        return
    rpm = pl.read_csv(path_rpm_csv, comment_prefix="#", try_parse_dates=True)
    errors: list[str] = []
    for dy in rpm["delivery_year"].unique().to_list():
        dy_zones = set(rpm.filter(pl.col("delivery_year") == dy)["zone"].to_list())
        missing = set(df["price_zone"].to_list()) - dy_zones
        if missing:
            errors.append(
                f"DY {dy}: price zones missing from rpm CSV: {sorted(missing)}"
            )
    if errors:
        for e in errors:
            result.error("Price zone crosswalk", e)
    else:
        n_dy = rpm["delivery_year"].n_unique()
        result.passed(
            "Price zone crosswalk",
            f"all price zones present in rpm CSV for all {n_dy} delivery years",
        )


def check_fivecp_crosswalk(
    df: pl.DataFrame, path_fivecp_csv: Path, result: ValidationResult
) -> None:
    """Every fivecp_zone_label must exist in fivecp_peaks.csv for every summer."""
    if not path_fivecp_csv.is_file():
        result.warn(
            "5CP zone crosswalk",
            f"{path_fivecp_csv} not found — skipping (run again once 5cp CSV exists)",
        )
        return
    fivecp = pl.read_csv(path_fivecp_csv, comment_prefix="#", try_parse_dates=True)
    errors: list[str] = []
    for summer in fivecp["summer_year"].unique().to_list():
        summer_zones = set(
            fivecp.filter(pl.col("summer_year") == summer)["zone"].to_list()
        )
        missing = set(df["fivecp_zone_label"].to_list()) - summer_zones
        if missing:
            errors.append(
                f"summer {summer}: 5CP zones missing from fivecp CSV: {sorted(missing)}"
            )
    if errors:
        for e in errors:
            result.error("5CP zone crosswalk", e)
    else:
        n_summers = fivecp["summer_year"].n_unique()
        result.passed(
            "5CP zone crosswalk",
            f"all 5CP zone labels present in fivecp CSV for all {n_summers} summers",
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
        description="Validate the PJM utility zone mapping CSV."
    )
    parser.add_argument(
        "--path-csv",
        type=Path,
        required=True,
        help="Path to pjm_utility_zone_mapping.csv.",
    )
    parser.add_argument(
        "--path-rpm-csv",
        type=Path,
        required=True,
        help="Path to rpm_capacity_prices.csv (cross-dataset check; WARN if absent).",
    )
    parser.add_argument(
        "--path-fivecp-csv",
        type=Path,
        required=True,
        help="Path to fivecp_peaks.csv (cross-dataset check; WARN if absent).",
    )
    args = parser.parse_args()
    csv_path = args.path_csv.resolve()

    if not csv_path.is_file():
        print(f"File not found: {csv_path}", file=sys.stderr)
        return 1

    df = pl.read_csv(csv_path, comment_prefix="#")

    print("PJM Utility Zone Mapping Validation Report")
    print(f"{'=' * 60}")
    print(f"Source: {csv_path}")
    print(f"Rows: {df.height}")
    print(f"{'=' * 60}\n")

    result = ValidationResult()

    check_schema(df, result)
    check_no_nulls(df, result)
    check_utility_slugs(df, result)
    check_weights_sum(df, result)
    check_dataminer_codes(df, result)
    check_internal_consistency(df, result)
    check_pnode_ids(df, result)
    check_price_zone_crosswalk(df, args.path_rpm_csv.resolve(), result)
    check_fivecp_crosswalk(df, args.path_fivecp_csv.resolve(), result)

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
