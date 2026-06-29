#!/usr/bin/env python3
"""Aggregate ISO-NE zone loads to utility-level profiles.

Reads local zone parquet (``zone={ZONE}/year=YYYY/month=MM/data.parquet``),
maps each utility to its ISO-NE zone(s) via
``data/isone/zone_mapping/csv/isone_utility_zone_mapping.csv``, sums zone
loads by timestamp, and writes utility-level parquet locally.

RI maps to a single zone (``RI`` → location 4005), so the aggregation is a
1:1 zone-to-utility relabel with no summing.  To add future ISONE utilities,
add rows to ``data/isone/zone_mapping/generate_zone_mapping_csv.py`` and
regenerate the CSV.

Zone parquet schema (from ``fetch_isone_zone_loads.py``):
    interval_start_et  Datetime[us, America/New_York]
    zone               String   (ME, NH, VT, CT, RI, SEMA, WCMA, NEMA)
    location_id        Int32
    load_mw            Float64

Output utility parquet schema:
    timestamp          Datetime[us, America/New_York]  (renamed from interval_start_et)
    utility            String
    load_mw            Float64

Upload to S3 via the Justfile ``upload`` recipe.

Usage:
    uv run python -m data.isone.hourly_demand.aggregate_isone_utility_loads \\
        --year 2025 --utility all \\
        --path-zone-mapping-csv data/isone/zone_mapping/csv/isone_utility_zone_mapping.csv \\
        --path-local-zones data/isone/hourly_demand/zones/ \\
        --path-local-utilities data/isone/hourly_demand/utilities/
"""

from __future__ import annotations

import argparse
import io
from pathlib import Path

import polars as pl
from cloudpathlib import S3Path


def load_zone_mapping(path: str) -> pl.DataFrame:
    """Load the ISO-NE utility-zone crosswalk CSV (local or S3).

    Required columns: ``utility``, ``iso_zone``.
    """
    if path.startswith("s3://"):
        df = pl.read_csv(io.BytesIO(S3Path(path).read_bytes()))
    else:
        df = pl.read_csv(path)
    required = {"utility", "iso_zone"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Zone mapping CSV missing columns: {sorted(missing)}")
    return df


def get_utility_zone_mapping(mapping_df: pl.DataFrame) -> dict[str, list[str]]:
    """Extract utility -> sorted unique list of ISO-NE zone names."""
    result: dict[str, list[str]] = {}
    for row in mapping_df.select("utility", "iso_zone").unique().iter_rows(named=True):
        result.setdefault(str(row["utility"]), []).append(str(row["iso_zone"]))
    return {u: sorted(set(z)) for u, z in result.items()}


def load_zone_data(zone_base: str, year: int, zones: list[str]) -> pl.DataFrame:
    """Load local zone parquet for the given zones and year.

    Validates that all 12 months are present for each zone before returning.
    Renames ``interval_start_et`` to ``timestamp`` for consistency with the
    NYISO and PJM utility output schemas.

    Args:
        zone_base: Local root with ``zone=*/year=*/month=*/data.parquet``.
        year: Calendar year to load.
        zones: ISO-NE zone names to include (e.g. ``["RI"]``).

    Returns:
        DataFrame with columns: timestamp, zone, load_mw (plus hive partition
        columns year, month if present).

    Raises:
        ValueError: If no data found or any zone is missing months.
    """
    collected = (
        pl.scan_parquet(zone_base, hive_partitioning=True)
        .filter(pl.col("zone").is_in(zones))
        .filter(pl.col("year") == year)
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from zone data collect()")
    if collected.is_empty():
        raise ValueError(
            f"No zone data for year={year}, zones={zones}. Run fetch-zone-data "
            f"first so {zone_base} has zone={{ZONE}}/year={year}/ partitions."
        )

    if "month" not in collected.columns:
        raise ValueError("Expected 'month' partition column missing from zone data")

    missing: list[str] = []
    for zone in zones:
        zone_months = (
            collected.filter(pl.col("zone") == zone)
            .select(pl.col("month").cast(pl.Int32).unique().sort())
            .to_series()
            .to_list()
        )
        for m in sorted(set(range(1, 13)) - set(zone_months)):
            missing.append(f"Zone {zone}: month {year}-{m:02d} missing")
    if missing:
        for item in missing:
            print(f"  {item}")
        raise ValueError(
            f"Incomplete data: {len(missing)} missing partition(s). "
            f"All 12 months required for {year}."
        )

    return collected.rename({"interval_start_et": "timestamp"})


def aggregate_utility_load(
    zone_df: pl.DataFrame, utility_name: str, zones: list[str]
) -> pl.DataFrame:
    """Sum zone loads for a single utility by timestamp.

    For single-zone utilities (e.g. rie → RI) this is a 1:1 relabel.
    For multi-zone utilities, hourly loads are summed across zones.

    Args:
        zone_df: Zone DataFrame with columns: timestamp, zone, load_mw.
        utility_name: Utility slug to assign (e.g. ``"rie"``).
        zones: ISO-NE zone names belonging to this utility.

    Returns:
        DataFrame with columns: timestamp, utility, load_mw, sorted by timestamp.
    """
    utility_data = zone_df.filter(pl.col("zone").is_in(zones))
    if utility_data.is_empty():
        raise ValueError(f"No data found for utility {utility_name} zones {zones}")
    return (
        utility_data.group_by("timestamp")
        .agg(pl.col("load_mw").sum().alias("load_mw"))
        .with_columns(pl.lit(utility_name).alias("utility"))
        .select(["timestamp", "utility", "load_mw"])
        .sort("timestamp")
    )


def write_utility_loads_local(
    utility_df: pl.DataFrame, utility_base: str, utility_name: str
) -> None:
    """Write utility load Hive parquet locally.

    Layout: ``utility={slug}/year=YYYY/month=MM/data.parquet`` (Eastern
    wall-clock year/month), matching the NYISO and PJM convention.

    Writes one ``data.parquet`` per partition rather than relying on Polars'
    auto-named partition files.  Partition columns are encoded in the path
    only, not stored in the file.

    Args:
        utility_df: DataFrame with columns: timestamp, utility, load_mw.
        utility_base: Local root directory for utility parquet output.
        utility_name: Utility slug (used only for the log message).
    """
    output_df = utility_df.with_columns(
        pl.col("timestamp").dt.year().alias("year"),
        pl.col("timestamp").dt.month().alias("month"),
    )
    base = Path(utility_base)
    for (utility, year, month), part_df in output_df.partition_by(
        ["utility", "year", "month"], as_dict=True
    ).items():
        part_dir = base / f"utility={utility}" / f"year={year}" / f"month={month:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_df.select("timestamp", "load_mw").write_parquet(
            part_dir / "data.parquet", compression="zstd"
        )
    print(f"  Wrote utility={utility_name} partitions under {base}")


def expected_hours_in_year(year: int) -> int:
    """DST-aware expected hour count for Eastern Time.

    Spring-forward and fall-back cancel out; only the leap-year extra day
    matters (8784 vs 8760).
    """
    is_leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    return 8784 if is_leap else 8760


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate ISO-NE zone loads to utility-level profiles."
    )
    parser.add_argument("--year", type=int, required=True, help="Calendar year.")
    parser.add_argument(
        "--utility",
        type=str,
        default="all",
        help="Specific utility slug, or 'all' (default: all).",
    )
    parser.add_argument(
        "--path-zone-mapping-csv",
        dest="path_zone_mapping_csv",
        type=str,
        required=True,
        help="Path to isone_utility_zone_mapping.csv (local or S3).",
    )
    parser.add_argument(
        "--path-local-zones",
        dest="path_local_zones",
        type=str,
        required=True,
        help="Local directory with zone parquet inputs.",
    )
    parser.add_argument(
        "--path-local-utilities",
        dest="path_local_utilities",
        type=str,
        required=True,
        help="Local directory for utility parquet output.",
    )

    args = parser.parse_args()
    year = args.year

    mapping_df = load_zone_mapping(args.path_zone_mapping_csv)
    utility_zone_map = get_utility_zone_mapping(mapping_df)

    selected = args.utility.lower()
    if selected != "all" and selected not in utility_zone_map:
        valid = ", ".join(sorted(utility_zone_map.keys()))
        parser.error(f"Invalid --utility '{args.utility}'. Valid: all, {valid}")

    utilities_to_process = (
        sorted(utility_zone_map.keys()) if selected == "all" else [selected]
    )

    all_zones_needed: set[str] = set()
    for u in utilities_to_process:
        all_zones_needed.update(utility_zone_map[u])

    print("=" * 60)
    print("ISO-NE UTILITY LOAD AGGREGATION")
    print("=" * 60)
    print(f"Year: {year}")
    print(f"Zone mapping: {args.path_zone_mapping_csv}")
    print(f"Zone input: {args.path_local_zones}")
    print(f"Utility output: {args.path_local_utilities}")
    print(f"Utilities: {', '.join(utilities_to_process)}")
    print(f"Zones needed: {sorted(all_zones_needed)}")
    print("=" * 60)

    zone_df = load_zone_data(args.path_local_zones, year, sorted(all_zones_needed))
    print(f"  Total zone rows: {len(zone_df):,}")

    expected = expected_hours_in_year(year)

    for utility_name in utilities_to_process:
        zones = utility_zone_map[utility_name]
        print(f"\n{'=' * 60}")
        print(f"UTILITY: {utility_name}  (zones: {zones})")
        print("=" * 60)

        utility_df = aggregate_utility_load(zone_df, utility_name, zones)
        n = len(utility_df)
        if n != expected:
            print(f"  WARNING: expected {expected} hours, got {n}")
        else:
            print(f"  Hour count: {expected}")

        print("  Load statistics (MW):")
        print(f"    Min:  {utility_df['load_mw'].min():.2f}")
        print(f"    Max:  {utility_df['load_mw'].max():.2f}")
        print(f"    Mean: {utility_df['load_mw'].mean():.2f}")

        write_utility_loads_local(utility_df, args.path_local_utilities, utility_name)

    print(f"\n{'=' * 60}")
    print("All utilities processed")
    print("  Run Justfile upload recipe to sync to S3")
    print("=" * 60)


if __name__ == "__main__":
    main()
