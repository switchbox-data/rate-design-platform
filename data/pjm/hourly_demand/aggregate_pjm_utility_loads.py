#!/usr/bin/env python3
"""Aggregate PJM zone loads to utility-level profiles.

Reads local zone parquet (``zone={CODE}/year=YYYY/data.parquet``, Data Miner
zone codes), maps each utility to its zone(s) via the PJM utility-zone crosswalk
(`data/pjm/zone_mapping/csv/pjm_utility_zone_mapping.csv`), sums zone loads by
timestamp, and writes utility-level parquet locally.

Each Maryland utility maps to exactly one Data Miner zone (bge->BC, pepco->PEP,
dpl->DPL, potomac-edison->AP), so a utility series equals its zone series. The
``capacity_weight`` column is for capacity-cost allocation and is intentionally
NOT applied here — load is assigned in full to the mapped utility.

Flagged zone-hours (``value_flag=True`` in the raw zone data) are linearly
interpolated before aggregation; the utility output carries an ``interpolated``
boolean marking any utility-hour built from an interpolated zone-hour.

Input:  zone parquet:    zone={CODE}/year=YYYY/data.parquet
        (timestamp, load_mw, value_flag)
Output: utility parquet: utility={slug}/year=YYYY/data.parquet
        (timestamp, utility, load_mw, interpolated)

Upload to S3 via the Justfile `upload` recipe.

Usage:
    uv run python -m data.pjm.hourly_demand.aggregate_pjm_utility_loads \\
        --year 2025 --utility all \\
        --path-zone-mapping-csv data/pjm/zone_mapping/csv/pjm_utility_zone_mapping.csv \\
        --path-local-zones data/pjm/hourly_demand/zones/ \\
        --path-local-utilities data/pjm/hourly_demand/utilities/
"""

from __future__ import annotations

import argparse
import io
from pathlib import Path

import polars as pl
from cloudpathlib import S3Path

from data.pjm.hourly_demand.validate_pjm_demand_parquet import expected_hours_in_year


def load_zone_mapping(path: str) -> pl.DataFrame:
    """Load the PJM utility-zone crosswalk CSV (local or S3)."""
    if path.startswith("s3://"):
        df = pl.read_csv(io.BytesIO(S3Path(path).read_bytes()))
    else:
        df = pl.read_csv(path)

    required = {"utility", "dataminer_zone"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Zone mapping CSV missing columns: {sorted(missing)}")
    return df


def get_utility_zone_mapping(mapping_df: pl.DataFrame) -> dict[str, list[str]]:
    """Extract utility -> sorted unique list of Data Miner zone codes."""
    result: dict[str, list[str]] = {}
    for row in (
        mapping_df.select("utility", "dataminer_zone").unique().iter_rows(named=True)
    ):
        result.setdefault(str(row["utility"]), []).append(str(row["dataminer_zone"]))
    return {u: sorted(set(z)) for u, z in result.items()}


def load_zone_data(zone_base: str, year: int, zones: list[str]) -> pl.DataFrame:
    """Load local zone parquet for the given zones and year."""
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
            f"first so {zone_base} has zone={{CODE}}/year={year}/ partitions."
        )
    return collected


def interpolate_flagged_zone_hours(zone_df: pl.DataFrame) -> pl.DataFrame:
    """Linearly interpolate zone-hours marked bad in the raw zone data.

    Zones are stored as a faithful mirror of PJM with a ``value_flag`` column
    marking hours where a constituent load area reported a bad value. Here, for
    the curated utility product, we null those hours' ``load_mw`` and interpolate
    per zone in timestamp order (the hourly grid is uniform in instant order, so
    position-based interpolation is exact), then forward/backward fill any
    flagged endpoints.
    """
    if "value_flag" not in zone_df.columns:
        return zone_df

    flagged = zone_df.filter(pl.col("value_flag"))
    if flagged.height:
        print(f"  Interpolating {flagged.height} flagged zone-hour(s):")
        for r in flagged.select("zone", "timestamp", "load_mw").iter_rows(named=True):
            print(
                f"    {r['zone']} {r['timestamp']}: raw {r['load_mw']:.1f} MW -> interpolated"
            )

    return (
        zone_df.sort("zone", "timestamp")
        .with_columns(
            pl.when(pl.col("value_flag"))
            .then(None)
            .otherwise(pl.col("load_mw"))
            .alias("load_mw")
        )
        .with_columns(pl.col("load_mw").interpolate().over("zone"))
        .with_columns(pl.col("load_mw").fill_null(strategy="forward").over("zone"))
        .with_columns(pl.col("load_mw").fill_null(strategy="backward").over("zone"))
    )


def aggregate_utility_load(
    zone_df: pl.DataFrame, utility_name: str, zones: list[str]
) -> pl.DataFrame:
    """Sum zone loads for a single utility by timestamp.

    Carries an ``interpolated`` flag forward: a utility-hour is marked
    interpolated when any constituent zone-hour was ``value_flag=True`` (and so
    had its ``load_mw`` interpolated by :func:`interpolate_flagged_zone_hours`).
    """
    utility_data = zone_df.filter(pl.col("zone").is_in(zones))
    if utility_data.is_empty():
        raise ValueError(f"No data found for utility {utility_name} zones {zones}")

    interpolated_flag = (
        pl.col("value_flag").any()
        if "value_flag" in utility_data.columns
        else pl.lit(False)  # noqa: FBT003
    )
    return (
        utility_data.group_by("timestamp")
        .agg(
            pl.col("load_mw").sum().alias("load_mw"),
            interpolated_flag.alias("interpolated"),
        )
        .with_columns(pl.lit(utility_name).alias("utility"))
        .select(["timestamp", "utility", "load_mw", "interpolated"])
        .sort("timestamp")
    )


def write_utility_loads_local(
    utility_df: pl.DataFrame, utility_base: str, utility_name: str
) -> None:
    """Write utility load Hive parquet (utility={slug}/year=YYYY/data.parquet).

    Writes one ``data.parquet`` per partition (matching the documented S3 layout)
    rather than relying on Polars' auto-named partition files. Partition columns
    are encoded in the path, not the file.
    """
    output_df = utility_df.with_columns(pl.col("timestamp").dt.year().alias("year"))
    base = Path(utility_base)
    for (utility, year), part_df in output_df.partition_by(
        ["utility", "year"], as_dict=True
    ).items():
        part_dir = base / f"utility={utility}" / f"year={year}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_df.select("timestamp", "load_mw", "interpolated").write_parquet(
            part_dir / "data.parquet", compression="zstd"
        )
    print(f"  Wrote utility={utility_name} partition under {base}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate PJM zone loads to utility-level profiles."
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
        help="Path to pjm_utility_zone_mapping.csv (local or S3).",
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
    print("PJM UTILITY LOAD AGGREGATION")
    print("=" * 60)
    print(f"Year: {year}")
    print(f"Zone input: {args.path_local_zones}")
    print(f"Utility output: {args.path_local_utilities}")
    print(f"Utilities: {', '.join(utilities_to_process)}")
    print(f"Zones needed: {sorted(all_zones_needed)}")
    print("=" * 60)

    zone_df = load_zone_data(args.path_local_zones, year, sorted(all_zones_needed))
    print(f"  Total zone rows: {len(zone_df):,}")

    zone_df = interpolate_flagged_zone_hours(zone_df)

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
