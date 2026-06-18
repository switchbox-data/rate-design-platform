#!/usr/bin/env python3
"""Validate PJM zonal hourly demand: schema, completeness, ranges, gaps.

Two entry points:

- :func:`validate_zone_loads` — in-memory check used by the fetch script before
  it writes anything. Returns ``(ok, messages)``; ``ok=False`` blocks the write.
- :func:`main` — CLI QA battery over local Hive-partitioned parquet
  (``zone=*/year=*/data.parquet``), run after fetch/aggregate and before upload.

Completeness is checked against the **Eastern calendar year**: a full year is
8760 hours (8784 in a leap year), counted DST-aware off UTC so the spring-forward
and fall-back days are handled correctly (no missing/extra wall-clock hours).

Usage:
    uv run python -m data.pjm.hourly_demand.validate_pjm_demand_parquet \\
        --path-local-zones data/pjm/hourly_demand/zones
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

EXPECTED_SCHEMA = {
    "timestamp": pl.Datetime("us", "America/New_York"),
    "zone": pl.String,
    "load_mw": pl.Float64,
}

LOAD_FLOOR = 0.0
LOAD_CEILING = 50_000.0  # PJM zones range widely; this is a sanity ceiling

# Soft peak expectations (MW) for spot-check warnings, by Data Miner zone code.
PEAK_SANITY: dict[str, tuple[float, float]] = {
    "BC": (4_000.0, 8_000.0),  # BGE summer peak ~6,100 MW
}


def expected_hours_in_year(year: int) -> int:
    """DST-aware count of distinct wall-clock hours in an Eastern calendar year."""
    start = datetime(year, 1, 1, tzinfo=ET).astimezone(UTC)
    end = datetime(year + 1, 1, 1, tzinfo=ET).astimezone(UTC)
    return int((end - start).total_seconds() // 3600)


def validate_zone_loads(
    df: pl.DataFrame, zones: list[str], year: int
) -> tuple[bool, list[str]]:
    """In-memory validation of a (timestamp, zone, load_mw) frame for one year.

    Hard-fails (``ok=False``) on missing zones, wrong hour counts, nulls,
    negatives, duplicates, or internal gaps. Peak-sanity issues are warnings.
    """
    msgs: list[str] = []
    ok = True
    expected = expected_hours_in_year(year)

    present = set(df["zone"].unique().to_list())
    missing = set(zones) - present
    if missing:
        ok = False
        msgs.append(f"FAIL zones: missing {sorted(missing)}")

    for zone in sorted(zones):
        zdf = df.filter(pl.col("zone") == zone).sort("timestamp")
        n = zdf.height
        if n == 0:
            continue

        if n != expected:
            ok = False
            msgs.append(f"FAIL {zone}: {n} hours, expected {expected} for {year}")

        nulls = zdf["load_mw"].null_count()
        if nulls:
            ok = False
            msgs.append(f"FAIL {zone}: {nulls} null load_mw")

        n_neg = zdf.filter(pl.col("load_mw") < 0).height
        if n_neg:
            ok = False
            msgs.append(f"FAIL {zone}: {n_neg} negative load_mw")

        n_unique = zdf["timestamp"].n_unique()
        if n_unique != n:
            ok = False
            msgs.append(f"FAIL {zone}: {n - n_unique} duplicate timestamps")

        diffs = zdf["timestamp"].diff().drop_nulls()
        n_gaps = diffs.filter(diffs > timedelta(hours=1)).len()
        if n_gaps:
            ok = False
            msgs.append(f"FAIL {zone}: {n_gaps} gap(s) > 1 hour")

        peak = float(zdf["load_mw"].max())  # type: ignore[arg-type]
        sanity = PEAK_SANITY.get(zone)
        if sanity is not None and not (sanity[0] <= peak <= sanity[1]):
            msgs.append(
                f"WARN {zone}: peak {peak:.0f} MW outside expected "
                f"[{sanity[0]:.0f}, {sanity[1]:.0f}]"
            )
        else:
            msgs.append(f"ok   {zone}: {n} hours, peak {peak:.0f} MW")

    return ok, msgs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate PJM zonal hourly demand parquet."
    )
    parser.add_argument(
        "--path-local-zones",
        type=Path,
        required=True,
        help="Local parquet root (zone=*/year=*/data.parquet).",
    )
    args = parser.parse_args()
    root = args.path_local_zones.resolve()

    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    parquet_files = list(root.glob("zone=*/year=*/data.parquet"))
    if not parquet_files:
        print(f"No parquet partitions found under {root}", file=sys.stderr)
        return 1

    df = pl.read_parquet(root / "**/*.parquet", hive_partitioning=True)

    # Hive partition columns come back as strings; cast year for grouping.
    df = df.with_columns(pl.col("year").cast(pl.Int64))

    print("PJM Hourly Demand Validation Report")
    print("=" * 60)
    print(f"Source:     {root}")
    print(f"Partitions: {len(parquet_files)}")
    print(f"Rows:       {df.height:,}")
    print("=" * 60)

    # Schema check (ignore the year/zone partition columns).
    schema_errors: list[str] = []
    for col, expected_type in EXPECTED_SCHEMA.items():
        if col not in df.columns:
            schema_errors.append(f"missing column {col}")
        elif df.schema[col] != expected_type:
            schema_errors.append(
                f"{col}: expected {expected_type}, got {df.schema[col]}"
            )
    if schema_errors:
        for e in schema_errors:
            print(f"  FAIL  Schema  {e}")
        return 1
    print("  ok    Schema  " + ", ".join(EXPECTED_SCHEMA))

    overall_ok = True
    for (zone, year), zdf in df.group_by(["zone", "year"], maintain_order=True):
        ok, msgs = validate_zone_loads(zdf, [str(zone)], int(year))
        overall_ok = overall_ok and ok
        for m in msgs:
            print(f"  {m}  ({zone} {year})")

    print("=" * 60)
    print("Result: PASS" if overall_ok else "Result: FAIL")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
