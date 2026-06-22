"""Validate fetched PJM LMP data for hourly completeness.

Checks that every hour in the requested date range has a valid row for each
zone.  Reports missing hours, duplicate hours, and null price values.

Validation uses the **EPT** (Eastern Prevailing Time) timestamp column because
the PJM API filters by EPT — the ``datetime_beginning_ept`` column is the
authoritative indicator of which hours the API was asked to return.

Can be used as a library (``validate_zone_lmp``) or as a CLI::

    uv run python data/pjm/validate_pjm_lmp.py \\
        --zone BGE --start-date 2023-01-01 --end-date 2023-12-31 \\
        path/to/data.parquet
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl


def _expected_ept_hours(start: date, end: date) -> pl.Series:
    """Generate the complete sequence of hourly EPT timestamps from start to end.

    Returns one timestamp per hour, from ``start 00:00`` through
    ``end 23:00`` inclusive.  These are timezone-naive datetimes representing
    Eastern Prevailing Time (EPT).

    Note: DST transitions mean some calendar days have 23 or 25 hours.
    This function generates a flat 24-per-day sequence, so DST-transition
    hours may show as ±1 "missing" — that's expected and the validator
    accounts for it.
    """
    start_dt = datetime(start.year, start.month, start.day)
    end_dt = datetime(end.year, end.month, end.day, 23)

    hours: list[datetime] = []
    cur = start_dt
    while cur <= end_dt:
        hours.append(cur)
        cur += timedelta(hours=1)

    return pl.Series("datetime_beginning_ept", hours).cast(pl.Datetime("us"))


# DST transitions cause 1 missing or 1 extra hour per transition (2 per year).
# We tolerate a small number so DST gaps don't cause false failures.
_DST_TOLERANCE_HOURS = 10


def validate_zone_lmp(
    df: pl.DataFrame,
    zone: str,
    start: date,
    end: date,
) -> tuple[bool, list[str]]:
    """Validate that *df* has one valid row per hour for *zone*.

    Parameters
    ----------
    df:
        LMP DataFrame with at least ``datetime_beginning_ept``, ``zone``,
        and ``total_lmp_rt`` columns.
    zone:
        PJM zone code to validate (e.g. ``"BGE"``).
    start, end:
        Inclusive date range (in EPT) the fetch was supposed to cover.

    Returns
    -------
    (ok, messages):
        ``ok`` is True when all checks pass.  ``messages`` contains human-
        readable descriptions of every check result (pass or fail).
    """
    msgs: list[str] = []
    ok = True

    zone_df = df.filter(pl.col("zone") == zone)

    if zone_df.is_empty():
        return False, [f"FAIL  No rows found for zone={zone}"]

    ts_col = "datetime_beginning_ept"
    if ts_col not in zone_df.columns:
        return False, [
            f"FAIL  Column '{ts_col}' not found. Available columns: {zone_df.columns}"
        ]

    # Parse if still string.
    if zone_df.schema[ts_col] == pl.String:
        zone_df = zone_df.with_columns(
            pl.col(ts_col).str.to_datetime(strict=False).alias(ts_col)
        )

    # Strip timezone if present so naive-vs-aware doesn't break joins.
    col_dtype = zone_df.schema[ts_col]
    if hasattr(col_dtype, "time_zone") and col_dtype.time_zone is not None:
        zone_df = zone_df.with_columns(
            pl.col(ts_col).dt.replace_time_zone(None).alias(ts_col)
        )

    zone_df = zone_df.with_columns(pl.col(ts_col).cast(pl.Datetime("us")).alias(ts_col))

    # Check for all-null timestamps.
    if zone_df[ts_col].null_count() == zone_df.height:
        return False, [
            f"FAIL  All {zone_df.height:,} timestamps in zone={zone} are null. "
            "Timestamp parsing likely failed — check the datetime format."
        ]

    expected = _expected_ept_hours(start, end)
    n_expected = expected.len()
    n_actual = zone_df.height

    # --- Duplicate hours ---
    # Fall-back DST transitions produce 1 duplicate per year (the repeated
    # 1:00 AM hour).  Only fail if duplicates exceed DST tolerance.
    dup_count = n_actual - zone_df[ts_col].n_unique()
    if dup_count > _DST_TOLERANCE_HOURS:
        ok = False
        msgs.append(f"FAIL  {dup_count:,} duplicate hour(s) in zone={zone}")
    elif dup_count > 0:
        msgs.append(
            f"  ok  {dup_count} duplicate hour(s) (expected from DST fall-back)"
        )
    else:
        msgs.append("  ok  No duplicate hours")

    # --- Missing hours ---
    expected_frame = expected.to_frame()
    actual_frame = zone_df.select(ts_col).unique()
    missing = expected_frame.join(actual_frame, on=ts_col, how="anti")
    n_missing = missing.height

    if n_missing > _DST_TOLERANCE_HOURS:
        ok = False
        pct = n_missing / n_expected * 100
        msgs.append(
            f"FAIL  {n_missing:,} of {n_expected:,} expected hours missing "
            f"({pct:.2f}%) in zone={zone}"
        )
    elif n_missing > 0:
        msgs.append(
            f"  ok  {n_missing} hour(s) missing (within DST tolerance "
            f"of {_DST_TOLERANCE_HOURS})"
        )
    else:
        msgs.append(f"  ok  All {n_expected:,} expected hours present")

    # Always print missing hours for visibility.
    if n_missing > 0:
        sorted_missing = missing.sort(ts_col)
        sample = sorted_missing.head(20)[ts_col].to_list()
        sample_str = "\n              ".join(str(t) for t in sample)
        suffix = (
            f"\n              ... and {n_missing - 20} more" if n_missing > 20 else ""
        )
        msgs.append(f"      missing: {sample_str}{suffix}")

    # --- Null prices ---
    price_col = "total_lmp_rt"
    if price_col in zone_df.columns:
        n_null = zone_df[price_col].null_count()
        if n_null > 0:
            ok = False
            msgs.append(f"FAIL  {n_null:,} null {price_col} values in zone={zone}")
        else:
            msgs.append(f"  ok  No null {price_col} values")

    # --- Date range coverage ---
    actual_min: datetime | None = zone_df[ts_col].cast(pl.Datetime("us")).min()  # type: ignore[assignment]
    actual_max: datetime | None = zone_df[ts_col].cast(pl.Datetime("us")).max()  # type: ignore[assignment]
    range_start = datetime(start.year, start.month, start.day)
    range_end = datetime(end.year, end.month, end.day, 23)
    msgs.append(f"      EPT range: {actual_min} → {actual_max}")

    if actual_min is not None and actual_min < range_start:
        msgs.append(f"WARN  Data starts before requested range: {actual_min}")
    if actual_max is not None and actual_max > range_end:
        msgs.append(f"WARN  Data extends past requested range: {actual_max}")

    # --- Summary line (inserted at top) ---
    msgs.insert(
        0,
        f"zone={zone}: {n_actual:,} rows, "
        f"{n_expected:,} expected hours, "
        f"{n_missing:,} missing, "
        f"{dup_count:,} duplicates",
    )

    return ok, msgs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate PJM LMP parquet data for hourly completeness."
    )
    parser.add_argument(
        "path_parquet",
        type=Path,
        help="Path to the LMP parquet file or Hive-partitioned directory.",
    )
    parser.add_argument(
        "--zone",
        required=True,
        help="PJM zone code(s), comma-separated (e.g. BGE or BGE,PEPCO,DPL,APS).",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Expected start date (inclusive), ISO format: YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="Expected end date (inclusive), ISO format: YYYY-MM-DD.",
    )

    args = parser.parse_args()
    pq_path = args.path_parquet.resolve()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    zones = [z.strip() for z in args.zone.split(",")]

    if not pq_path.exists():
        print(f"File not found: {pq_path}", file=sys.stderr)
        return 1

    df = pl.read_parquet(pq_path)

    print("PJM LMP Hourly Completeness Report")
    print(f"{'=' * 60}")
    print(f"Source: {pq_path}")
    print(f"Range:  {start} → {end} (EPT)")
    print(f"{'=' * 60}\n")

    all_ok = True
    for zone in zones:
        zone_ok, msgs = validate_zone_lmp(df, zone, start, end)
        if not zone_ok:
            all_ok = False
        for m in msgs:
            print(f"  {m}")
        print()

    if all_ok:
        print("Result: ALL CHECKS PASSED")
    else:
        print("Result: SOME CHECKS FAILED")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
