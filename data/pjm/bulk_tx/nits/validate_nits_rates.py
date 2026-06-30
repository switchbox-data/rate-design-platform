"""Validate the PJM NITS rates reference CSV.

Checks:
- Required columns and dtypes
- Zone coverage (all 4 MD-relevant zones present per year)
- Each year has exactly 2 effective dates (Jan and Jun)
- NITS rates are positive and $/kW-yr = $/MW-yr / 1000
- Source URLs are non-empty

Usage:
    uv run python data/pjm/bulk_tx/nits/validate_nits_rates.py \
        --path-csv data/pjm/bulk_tx/nits/nits_rates.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

EXPECTED_COLUMNS = [
    "year",
    "effective_date",
    "zone",
    "nits_rate_mw_yr",
    "nits_rate_kw_yr",
    "source_url",
]

MD_ZONES = {"APS", "BGE", "DPL", "PEPCO"}


def validate(path_csv: str) -> tuple[bool, list[str]]:
    """Run all validations; return (ok, messages)."""
    msgs: list[str] = []
    ok = True

    df = pl.read_csv(path_csv, comment_prefix="#")

    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        msgs.append(f"FAIL: missing columns: {sorted(missing)}")
        return False, msgs
    msgs.append(f"OK: all expected columns present ({len(df.columns)} cols)")

    for year in sorted(df["year"].unique().to_list()):
        year_df = df.filter(pl.col("year") == year)
        zones = set(year_df["zone"].to_list())
        missing_zones = MD_ZONES - zones
        if missing_zones:
            msgs.append(f"FAIL: year {year} missing zones: {sorted(missing_zones)}")
            ok = False
        else:
            msgs.append(f"OK: year {year} has all 4 MD zones")

        dates = sorted(year_df["effective_date"].unique().to_list())
        if len(dates) != 2:
            msgs.append(
                f"WARN: year {year} has {len(dates)} effective date(s), expected 2 (Jan+Jun): {dates}"
            )

        for row in year_df.iter_rows(named=True):
            rate_mw = float(row["nits_rate_mw_yr"])
            rate_kw = float(row["nits_rate_kw_yr"])
            zone = row["zone"]
            eff = row["effective_date"]

            if rate_mw <= 0:
                msgs.append(f"FAIL: {zone} {eff} has non-positive MW rate: {rate_mw}")
                ok = False

            expected_kw = round(rate_mw / 1000, 2)
            if abs(rate_kw - expected_kw) > 0.01:
                msgs.append(
                    f"FAIL: {zone} {eff} kW rate mismatch: "
                    f"got {rate_kw}, expected {expected_kw} (= {rate_mw}/1000)"
                )
                ok = False

            if not row["source_url"]:
                msgs.append(f"WARN: {zone} {eff} has empty source_url")

    msgs.append(f"\nTotal: {len(df)} rows across {df['year'].n_unique()} year(s)")
    return ok, msgs


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate PJM NITS rates CSV.")
    parser.add_argument("--path-csv", required=True, help="Path to nits_rates.csv")
    args = parser.parse_args()

    if not Path(args.path_csv).exists():
        print(f"ERROR: file not found: {args.path_csv}")
        sys.exit(1)

    ok, msgs = validate(args.path_csv)
    for m in msgs:
        print(f"  {m}")

    if ok:
        print("\n✓ NITS rates validation PASSED")
    else:
        print("\n✗ NITS rates validation FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
