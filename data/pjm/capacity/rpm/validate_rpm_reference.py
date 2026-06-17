#!/usr/bin/env python3
"""Validate PJM RPM capacity price CSV: schema, coverage, LDA invariants, ranges.

Runs a QA battery on the curated rpm_capacity_prices.csv reference dataset
(BRA Resource Clearing Prices + Final Zonal Capacity Prices, one row per
(delivery_year, zone)).

Usage:
    uv run python data/pjm/capacity/rpm/validate_rpm_reference.py \
        --path-csv data/pjm/capacity/rpm/rpm_capacity_prices.csv
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

EXPECTED_COLUMNS = [
    "delivery_year",
    "dy_start",
    "dy_end",
    "zone",
    "lda",
    "bra_price_per_mw_day",
    "final_zonal_capacity_price_per_mw_day",
    "source_url",
    "bra_source_url",
    "final_price_as_of",
    "notes",
]

EXPECTED_TYPES = {
    "delivery_year": pl.String,
    "dy_start": pl.Date,
    "dy_end": pl.Date,
    "zone": pl.String,
    "lda": pl.String,
    "bra_price_per_mw_day": pl.Float64,
    "final_zonal_capacity_price_per_mw_day": pl.Float64,
    "source_url": pl.String,
    "bra_source_url": pl.String,
    "final_price_as_of": pl.Date,
    "notes": pl.String,
}

# Citation columns: the final-zonal file and the BRA-results file URLs.
URL_COLUMNS = ("source_url", "bra_source_url")

# Canonical PJM transmission-zone labels (see data/pjm/README.md crosswalk).
# UGI is part of the canonical vocabulary but is not separately reported in the
# RPM zonal price files (inside the PPL LDA), so it is allowed to be absent.
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

# Modeled RPM LDAs (most-specific LDA assigned per zone row).
CANONICAL_LDAS = frozenset(
    {
        "RTO",
        "MAAC",
        "EMAAC",
        "SWMAAC",
        "BGE",
        "PEPCO",
        "COMED",
        "ATSI",
        "ATSI-C",
        "PSEG",
        "PS-NORTH",
        "DPL-S",
        "DAY",
        "DEOK",
        "DOM",
        "JCPL",
    }
)

# LDA nesting: child -> parent. RPM LDAs nest; constrained children clear at or
# above their parent. Used by the BRA price monotonicity check.
LDA_NESTING: dict[str, str] = {
    "MAAC": "RTO",
    "EMAAC": "MAAC",
    "SWMAAC": "MAAC",
    "BGE": "SWMAAC",
    "PEPCO": "SWMAAC",
    "PSEG": "EMAAC",
    "PS-NORTH": "PSEG",
    "DPL-S": "EMAAC",
    "JCPL": "EMAAC",
    "COMED": "RTO",
    "ATSI": "RTO",
    "ATSI-C": "ATSI",
    "DAY": "RTO",
    "DEOK": "RTO",
    "DOM": "RTO",
}

EXPECTED_DYS = [
    "2018/19",
    "2019/20",
    "2020/21",
    "2021/22",
    "2022/23",
    "2023/24",
    "2024/25",
    "2025/26",
    "2026/27",
]

PRICE_FLOOR = 0.0
PRICE_CEILING = 700.0

# Final zonal prices drift from the BRA price as IAs settle; flag only large
# divergences (e.g. sub-LDA blends like DPL-S in 2024/25).
FINAL_VS_BRA_WARN_RATIO = 0.5

# Pinned values transcribed independently from the source reports.
# (delivery_year, zone) -> (bra_price, final_zonal_price or None)
CROSS_CHECK_VALUES: dict[tuple[str, str], tuple[float, float | None]] = {
    ("2025/26", "BGE"): (466.35, 471.328782),
    ("2025/26", "AEP"): (269.92, 270.432933),
    ("2018/19", "BGE"): (164.77, 158.203169),
    ("2022/23", "BGE"): (126.50, 125.120983),
    ("2019/20", "COMED"): (202.77, 195.994360),
    ("2026/27", "PEPCO"): (329.17, 329.077063),
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
    """Every row must carry a well-formed pjm.com citation for both prices."""
    errors: list[str] = []
    for col in URL_COLUMNS:
        if col not in df.columns:
            continue
        bad = df.filter(
            ~pl.col(col).str.starts_with("https://www.pjm.com/") | pl.col(col).is_null()
        )
        if bad.height:
            sample = bad.select("delivery_year", "zone", col).head(3).to_dicts()
            errors.append(f"{col}: {bad.height} non-pjm.com/empty URLs, e.g. {sample}")
    if errors:
        for e in errors:
            result.error("Source URLs", e)
    else:
        result.passed(
            "Source URLs",
            f"all rows cite pjm.com for {', '.join(URL_COLUMNS)}",
        )


def check_uniqueness(df: pl.DataFrame, result: ValidationResult) -> None:
    keys = ["delivery_year", "zone"]
    n_total = df.height
    n_unique = df.select(keys).unique().height
    if n_unique != n_total:
        result.error(
            "Uniqueness", f"{n_total - n_unique} duplicate (delivery_year, zone) rows"
        )
    else:
        result.passed(
            "Uniqueness", f"all {n_total} rows have unique (delivery_year, zone)"
        )


def check_dy_coverage(df: pl.DataFrame, result: ValidationResult) -> None:
    dys = set(df["delivery_year"].unique().to_list())
    missing = set(EXPECTED_DYS) - dys
    extra = dys - set(EXPECTED_DYS)
    if missing:
        result.error("DY coverage", f"missing DYs: {sorted(missing)}")
    if extra:
        result.warn("DY coverage", f"unexpected DYs: {sorted(extra)}")
    if not missing and not extra:
        result.passed(
            "DY coverage",
            f"all {len(EXPECTED_DYS)} DYs present ({EXPECTED_DYS[0]}-{EXPECTED_DYS[-1]})",
        )


def check_dy_dates(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for row in (
        df.select("delivery_year", "dy_start", "dy_end").unique().iter_rows(named=True)
    ):
        label = row["delivery_year"]
        y0 = int(label[:4])
        expected_start = date(y0, 6, 1)
        expected_end = date(y0 + 1, 5, 31)
        if row["dy_start"] != expected_start:
            errors.append(
                f"DY {label}: dy_start={row['dy_start']}, expected {expected_start}"
            )
        if row["dy_end"] != expected_end:
            errors.append(
                f"DY {label}: dy_end={row['dy_end']}, expected {expected_end}"
            )
    if errors:
        for e in errors:
            result.error("DY dates", e)
    else:
        result.passed(
            "DY dates", "dy_start/dy_end consistent with labels (Jun 1 - May 31)"
        )


def check_zone_vocabulary(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    bad_zones = set(df["zone"].unique().to_list()) - CANONICAL_ZONES
    if bad_zones:
        errors.append(f"non-canonical zones: {sorted(bad_zones)}")
    bad_ldas = set(df["lda"].unique().to_list()) - CANONICAL_LDAS
    if bad_ldas:
        errors.append(f"non-canonical LDAs: {sorted(bad_ldas)}")
    if errors:
        for e in errors:
            result.error("Zone vocabulary", e)
    else:
        result.passed(
            "Zone vocabulary",
            f"{df['zone'].n_unique()} zones, {df['lda'].n_unique()} LDAs, all canonical",
        )


def check_zone_completeness(df: pl.DataFrame, result: ValidationResult) -> None:
    zone_sets = {
        dy: frozenset(df.filter(pl.col("delivery_year") == dy)["zone"].to_list())
        for dy in df["delivery_year"].unique().to_list()
    }
    if len(set(zone_sets.values())) > 1:
        base: frozenset[str] = frozenset()
        for zones in zone_sets.values():
            if len(zones) > len(base):
                base = zones
        for dy, zones in sorted(zone_sets.items()):
            if zones != base:
                diff = sorted(base ^ zones)
                result.warn("Zone completeness", f"DY {dy}: zone set differs ({diff})")
    else:
        n = len(next(iter(zone_sets.values())))
        result.passed("Zone completeness", f"same {n}-zone set in every DY")


def check_rto_lda_present(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for dy in df["delivery_year"].unique().to_list():
        sub = df.filter(pl.col("delivery_year") == dy)
        if sub.filter(pl.col("lda") == "RTO").height == 0:
            errors.append(f"DY {dy}: no row with lda=RTO")
        for (lda,), group in sub.group_by("lda"):
            if group["bra_price_per_mw_day"].n_unique() > 1:
                errors.append(
                    f"DY {dy} lda {lda}: inconsistent bra_price values "
                    f"{sorted(group['bra_price_per_mw_day'].unique().to_list())}"
                )
    if errors:
        for e in errors:
            result.error("RTO LDA present", e)
    else:
        result.passed(
            "RTO LDA present",
            "every DY has lda=RTO rows; BRA price consistent within each (DY, lda)",
        )


def check_price_ranges(df: pl.DataFrame, result: ValidationResult) -> None:
    for col in ("bra_price_per_mw_day", "final_zonal_capacity_price_per_mw_day"):
        prices = df[col]
        pmin = float(prices.min())  # type: ignore[arg-type]
        pmax = float(prices.max())  # type: ignore[arg-type]
        pmean = float(prices.mean())  # type: ignore[arg-type]
        if pmin < PRICE_FLOOR:
            result.error("Price range", f"{col}: min ${pmin:.2f} is negative")
        elif pmax > PRICE_CEILING:
            result.warn(
                "Price range",
                f"{col}: max ${pmax:.2f} exceeds ${PRICE_CEILING:.0f}/MW-day ceiling",
            )
        else:
            result.passed(
                "Price range",
                f"{col}: ${pmin:.2f}-${pmax:.2f}/MW-day (mean ${pmean:.2f})",
            )


def check_lda_nesting(df: pl.DataFrame, result: ValidationResult) -> None:
    """BRA prices must be monotone along the LDA nesting chain within each DY.

    Applies to BRA prices only — final zonal prices can drop below the system
    price after IA true-downs (e.g. PPL in 2018/19), so they are not checked.
    """
    errors: list[str] = []
    for dy in df["delivery_year"].unique().to_list():
        sub = df.filter(pl.col("delivery_year") == dy)
        lda_price: dict[str, float] = {
            row["lda"]: row["bra_price_per_mw_day"]
            for row in sub.select("lda", "bra_price_per_mw_day")
            .unique()
            .iter_rows(named=True)
        }
        rto_price = lda_price.get("RTO")
        for lda, price in lda_price.items():
            if rto_price is not None and price < rto_price:
                errors.append(
                    f"DY {dy}: lda {lda} BRA ${price:.2f} < RTO ${rto_price:.2f}"
                )
            # Walk up the nesting chain to the nearest priced ancestor.
            parent = LDA_NESTING.get(lda)
            while parent is not None and parent not in lda_price:
                parent = LDA_NESTING.get(parent)
            if parent is not None and price < lda_price[parent]:
                errors.append(
                    f"DY {dy}: lda {lda} BRA ${price:.2f} < parent {parent} "
                    f"${lda_price[parent]:.2f}"
                )
    if errors:
        for e in errors:
            result.error("LDA nesting", e)
    else:
        result.passed(
            "LDA nesting", "BRA prices monotone along the LDA nesting chain in every DY"
        )


def check_final_vs_bra(df: pl.DataFrame, result: ValidationResult) -> None:
    """Final zonal price should stay in the BRA price's neighborhood (IA drift)."""
    warns: list[str] = []
    for row in df.iter_rows(named=True):
        bra = row["bra_price_per_mw_day"]
        final = row["final_zonal_capacity_price_per_mw_day"]
        if bra <= 0:
            continue
        if abs(final - bra) / bra > FINAL_VS_BRA_WARN_RATIO:
            warns.append(
                f"DY {row['delivery_year']} zone {row['zone']}: final ${final:.2f} "
                f"deviates >{FINAL_VS_BRA_WARN_RATIO:.0%} from BRA ${bra:.2f}"
                + (f" ({row['notes']})" if row["notes"] else "")
            )
    if warns:
        for w in warns:
            result.warn("Final vs BRA", w)
    else:
        result.passed(
            "Final vs BRA",
            f"all final zonal prices within ±{FINAL_VS_BRA_WARN_RATIO:.0%} of BRA price",
        )


def check_cross_check_values(df: pl.DataFrame, result: ValidationResult) -> None:
    errors: list[str] = []
    for (dy, zone), (bra_expected, final_expected) in CROSS_CHECK_VALUES.items():
        row = df.filter((pl.col("delivery_year") == dy) & (pl.col("zone") == zone))
        if row.height == 0:
            errors.append(f"DY {dy} zone {zone}: row not found")
            continue
        bra_actual = float(row["bra_price_per_mw_day"][0])
        if abs(bra_actual - bra_expected) > 0.005:
            errors.append(
                f"DY {dy} zone {zone}: BRA expected ${bra_expected:.2f}, "
                f"got ${bra_actual:.2f}"
            )
        if final_expected is not None:
            final_actual = float(row["final_zonal_capacity_price_per_mw_day"][0])
            if abs(final_actual - final_expected) > 0.005:
                errors.append(
                    f"DY {dy} zone {zone}: final expected ${final_expected:.2f}, "
                    f"got ${final_actual:.2f}"
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

    parser = argparse.ArgumentParser(description="Validate PJM RPM capacity price CSV.")
    parser.add_argument(
        "--path-csv",
        type=Path,
        required=True,
        help="Path to rpm_capacity_prices.csv.",
    )
    args = parser.parse_args()
    csv_path = args.path_csv.resolve()

    if not csv_path.is_file():
        print(f"File not found: {csv_path}", file=sys.stderr)
        return 1

    df = pl.read_csv(csv_path, comment_prefix="#", try_parse_dates=True)

    print("PJM RPM Capacity Price Validation Report")
    print(f"{'=' * 60}")
    print(f"Source: {csv_path}")
    print(f"Rows: {df.height}")
    print(f"DYs: {sorted(df['delivery_year'].unique().to_list())}")
    print(f"{'=' * 60}\n")

    result = ValidationResult()

    check_schema(df, result)
    check_no_nulls(df, result)
    check_source_urls(df, result)
    check_uniqueness(df, result)
    check_dy_coverage(df, result)
    check_dy_dates(df, result)
    check_zone_vocabulary(df, result)
    check_zone_completeness(df, result)
    check_rto_lda_present(df, result)
    check_price_ranges(df, result)
    check_lda_nesting(df, result)
    check_final_vs_bra(df, result)
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
