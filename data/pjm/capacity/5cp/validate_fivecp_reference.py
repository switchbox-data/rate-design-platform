#!/usr/bin/env python3
"""Validate PJM summer 5CP peaks CSV: schema, coverage, calendar rules, MW invariants.

Runs a QA battery on the curated fivecp_peaks.csv reference dataset (RTO 5
coincident peak hours per summer + zonal unrestricted MW coincident with each
RTO peak, per PJM's "Summer YYYY Peaks and 5CPs" PDFs).

Usage:
    uv run python data/pjm/capacity/5cp/validate_fivecp_reference.py \
        --path-csv data/pjm/capacity/5cp/fivecp_peaks.csv
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl

EXPECTED_COLUMNS = [
    "summer_year",
    "rank",
    "peak_date",
    "hour_ending_ept",
    "zone",
    "mw_unrestricted",
    "source_url",
    "source_as_of",
    "notes",
]

EXPECTED_TYPES = {
    "summer_year": pl.Int64,
    "rank": pl.Int64,
    "peak_date": pl.Date,
    "hour_ending_ept": pl.Int64,
    "zone": pl.String,
    "mw_unrestricted": pl.Float64,
    "source_url": pl.String,
    "source_as_of": pl.Date,
    "notes": pl.String,
}

# Canonical PJM transmission-zone labels (see data/pjm/README.md crosswalk).
CANONICAL_ZONES = frozenset(
    {
        "AECO",
        "AEP",
        "APS",
        "ATSI",
        "BGE",
        "COMED",
        "DAY",
        "DEOK",
        "DOM",
        "DPL",
        "DUQ",
        "EKPC",
        "JCPL",
        "METED",
        "PECO",
        "PENELEC",
        "PEPCO",
        "PPL",
        "PSEG",
        "RECO",
        "UGI",
    }
)

EXPECTED_SUMMERS = set(range(2021, 2026))
EXPECTED_RANKS = {1, 2, 3, 4, 5}

# Plausible afternoon band for summer RTO peaks (hour-ending EPT).
HOUR_PLAUSIBLE_MIN = 14
HOUR_PLAUSIBLE_MAX = 20

# Zonal MW must sum to the RTO MW within this tolerance (rounding + excluded
# sub-zone rows like EASTON/SMECO/Vineland and the tiny OVEC zone).
ZONAL_SUM_TOLERANCE = 0.02

# Pinned values transcribed independently from the source PDFs.
# (summer_year, rank, zone) -> mw_unrestricted
CROSS_CHECK_VALUES: dict[tuple[int, int, str], float] = {
    (2021, 1, "RTO"): 148425.2,
    (2022, 3, "RTO"): 144245.9,
    (2023, 1, "RTO"): 146843.2,
    (2024, 1, "RTO"): 152307.4,
    (2025, 5, "RTO"): 151524.7,
    (2023, 2, "DPL"): 3720.8,
    (2021, 4, "APS"): 8461.7,
    (2022, 3, "PEPCO"): 5487.6,
    (2024, 1, "BGE"): 6765.9,
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


def check_source_urls(df: pl.DataFrame, result: ValidationResult) -> None:
    """Every row must carry a well-formed pjm.com citation."""
    bad = df.filter(
        ~pl.col("source_url").str.starts_with("https://www.pjm.com/")
        | pl.col("source_url").is_null()
    )
    if bad.height:
        sample = bad.select("summer_year", "zone", "source_url").head(3).to_dicts()
        result.error(
            "Source URLs", f"{bad.height} non-pjm.com/empty source_url, e.g. {sample}"
        )
    else:
        result.passed("Source URLs", "all rows cite a pjm.com source_url")


def check_uniqueness(df: pl.DataFrame, result: ValidationResult) -> None:
    keys = ["summer_year", "rank", "zone"]
    n_total = df.height
    n_unique = df.select(keys).unique().height
    if n_unique != n_total:
        result.error(
            "Uniqueness", f"{n_total - n_unique} duplicate (summer, rank, zone) rows"
        )
    else:
        result.passed(
            "Uniqueness",
            f"all {n_total} rows have unique (summer_year, rank, zone)",
        )


def check_summer_coverage(df: pl.DataFrame, result: ValidationResult) -> None:
    summers = set(df["summer_year"].unique().to_list())
    missing = EXPECTED_SUMMERS - summers
    extra = summers - EXPECTED_SUMMERS
    if missing:
        result.error("Summer coverage", f"missing summers: {sorted(missing)}")
    if extra:
        result.warn("Summer coverage", f"unexpected summers: {sorted(extra)}")
    if not missing and not extra:
        result.passed(
            "Summer coverage",
            f"all {len(EXPECTED_SUMMERS)} summers present "
            f"({min(EXPECTED_SUMMERS)}-{max(EXPECTED_SUMMERS)})",
        )


def check_five_ranks(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for (summer, zone), group in df.group_by("summer_year", "zone"):
        ranks = set(group["rank"].to_list())
        if ranks != EXPECTED_RANKS:
            errors.append(f"summer {summer} zone {zone}: ranks {sorted(ranks)}")
    if errors:
        for e in errors:
            result.error("Five ranks", e)
    else:
        result.passed("Five ranks", "exactly ranks {1..5} per (summer, zone)")


def check_rto_present(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for summer in sorted(df["summer_year"].unique().to_list()):
        rto = df.filter((pl.col("summer_year") == summer) & (pl.col("zone") == "RTO"))
        if set(rto["rank"].to_list()) != EXPECTED_RANKS:
            errors.append(f"summer {summer}: RTO series incomplete")
    if errors:
        for e in errors:
            result.error("RTO present", e)
    else:
        result.passed("RTO present", "full 5-rank RTO series in every summer")


def check_zone_consistency(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    invalid = set(df["zone"].unique().to_list()) - CANONICAL_ZONES - {"RTO"}
    if invalid:
        errors.append(f"non-canonical zone labels: {sorted(invalid)}")
    for (summer,), group in df.group_by("summer_year"):
        zone_sets = [frozenset(g["zone"].to_list()) for _, g in group.group_by("rank")]
        if len(set(zone_sets)) > 1:
            errors.append(f"summer {summer}: zone set differs across ranks")
    if errors:
        for e in errors:
            result.error("Zone consistency", e)
    else:
        n_zones = df["zone"].n_unique()
        result.passed(
            "Zone consistency",
            f"{n_zones} zone labels, all canonical; same zone set across ranks",
        )


def check_window(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for row in df.select("summer_year", "peak_date").unique().iter_rows(named=True):
        summer = row["summer_year"]
        d = row["peak_date"]
        if not (date(summer, 6, 1) <= d <= date(summer, 9, 30)):
            errors.append(f"{d} outside Jun 1 - Sep 30 of {summer}")
    if errors:
        for e in errors:
            result.error("5CP window", e)
    else:
        result.passed("5CP window", "all peak dates within Jun 1 - Sep 30")


def _labor_day(year: int) -> date:
    """First Monday of September."""
    d = date(year, 9, 1)
    return d + timedelta(days=(7 - d.weekday()) % 7)


def check_weekday_non_holiday(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for row in df.select("summer_year", "peak_date").unique().iter_rows(named=True):
        summer = row["summer_year"]
        d = row["peak_date"]
        if d.weekday() >= 5:
            errors.append(f"{d} is a weekend day")
        if d == date(summer, 7, 4):
            errors.append(f"{d} is July 4")
        if d == _labor_day(summer):
            errors.append(f"{d} is Labor Day")
    if errors:
        for e in errors:
            result.error("Weekday non-holiday", e)
    else:
        result.passed("Weekday non-holiday", "all peak dates are non-holiday weekdays")


def check_distinct_days(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    rto = df.filter(pl.col("zone") == "RTO")
    for (summer,), group in rto.group_by("summer_year"):
        dates = group["peak_date"].to_list()
        if len(set(dates)) != 5:
            errors.append(f"summer {summer}: only {len(set(dates))} distinct days")
    if errors:
        for e in errors:
            result.error("Distinct days", e)
    else:
        result.passed("Distinct days", "5 distinct peak days per summer")


def check_hour_ending(df: pl.DataFrame, result: ValidationResult) -> None:
    hours = df["hour_ending_ept"]
    hmin = int(hours.min())  # type: ignore[arg-type]
    hmax = int(hours.max())  # type: ignore[arg-type]
    if hmin < 1 or hmax > 24:
        result.error("Hour ending", f"hours outside 1-24: min {hmin}, max {hmax}")
    elif hmin < HOUR_PLAUSIBLE_MIN or hmax > HOUR_PLAUSIBLE_MAX:
        result.warn(
            "Hour ending",
            f"hours outside plausible {HOUR_PLAUSIBLE_MIN}-{HOUR_PLAUSIBLE_MAX} "
            f"afternoon band: min {hmin}, max {hmax}",
        )
    else:
        result.passed("Hour ending", f"all hours within HE {hmin}-{hmax} EPT")


def check_rto_descending(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    rto = df.filter(pl.col("zone") == "RTO").sort("summer_year", "rank")
    for (summer,), group in rto.group_by("summer_year", maintain_order=True):
        mws = group.sort("rank")["mw_unrestricted"].to_list()
        if any(a <= b for a, b in zip(mws, mws[1:])):
            errors.append(f"summer {summer}: RTO MW not strictly descending: {mws}")
    if errors:
        for e in errors:
            result.error("RTO descending", e)
    else:
        result.passed(
            "RTO descending", "RTO MW strictly descending by rank in every summer"
        )


def check_zonal_sum(df: pl.DataFrame, result: ValidationResult) -> None:
    zonal = df.filter(pl.col("zone") != "RTO")
    if zonal.height == 0:
        result.warn("Zonal sum", "no zonal rows yet (RTO-only dataset)")
        return
    warns: list[str] = []
    for (summer, rank), group in zonal.group_by("summer_year", "rank"):
        zone_sum = float(group["mw_unrestricted"].sum())
        rto_row = df.filter(
            (pl.col("summer_year") == summer)
            & (pl.col("rank") == rank)
            & (pl.col("zone") == "RTO")
        )
        rto_mw = float(rto_row["mw_unrestricted"][0])
        rel_err = abs(zone_sum - rto_mw) / rto_mw
        if rel_err > ZONAL_SUM_TOLERANCE:
            warns.append(
                f"summer {summer} rank {rank}: zones sum {zone_sum:,.1f} vs "
                f"RTO {rto_mw:,.1f} ({rel_err:.2%})"
            )
    if warns:
        for w in warns:
            result.warn("Zonal sum", w)
    else:
        n = zonal.select("summer_year", "rank").unique().height
        result.passed(
            "Zonal sum",
            f"zonal MW sums within ±{ZONAL_SUM_TOLERANCE:.0%} of RTO for all {n} peaks",
        )


def check_cross_check_values(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for (summer, rank, zone), expected_mw in CROSS_CHECK_VALUES.items():
        row = df.filter(
            (pl.col("summer_year") == summer)
            & (pl.col("rank") == rank)
            & (pl.col("zone") == zone)
        )
        if row.height == 0:
            errors.append(f"summer {summer} rank {rank} zone {zone}: row not found")
            continue
        actual = float(row["mw_unrestricted"][0])
        if abs(actual - expected_mw) > 0.05:
            errors.append(
                f"summer {summer} rank {rank} zone {zone}: "
                f"expected {expected_mw:,.1f}, got {actual:,.1f}"
            )
    if errors:
        for e in errors:
            result.error("Cross-check", e)
    else:
        result.passed("Cross-check", f"{len(CROSS_CHECK_VALUES)} known values verified")


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

    parser = argparse.ArgumentParser(description="Validate PJM summer 5CP peaks CSV.")
    parser.add_argument(
        "--path-csv",
        type=Path,
        required=True,
        help="Path to fivecp_peaks.csv.",
    )
    args = parser.parse_args()
    csv_path = args.path_csv.resolve()

    if not csv_path.is_file():
        print(f"File not found: {csv_path}", file=sys.stderr)
        return 1

    df = pl.read_csv(csv_path, comment_prefix="#", try_parse_dates=True)

    print("PJM Summer 5CP Peaks Validation Report")
    print(f"{'=' * 60}")
    print(f"Source: {csv_path}")
    print(f"Rows: {df.height}")
    print(f"Summers: {sorted(df['summer_year'].unique().to_list())}")
    print(f"{'=' * 60}\n")

    result = ValidationResult()

    check_schema(df, result)
    check_no_nulls(df, result)
    check_source_urls(df, result)
    check_uniqueness(df, result)
    check_summer_coverage(df, result)
    check_five_ranks(df, result)
    check_rto_present(df, result)
    check_zone_consistency(df, result)
    check_window(df, result)
    check_weekday_non_holiday(df, result)
    check_distinct_days(df, result)
    check_hour_ending(df, result)
    check_rto_descending(df, result)
    check_zonal_sum(df, result)
    check_cross_check_values(df, result)

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
