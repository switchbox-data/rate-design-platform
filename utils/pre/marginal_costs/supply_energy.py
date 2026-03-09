"""Energy (LBMP) marginal cost computation for NY utility supply MCs."""

from __future__ import annotations

import polars as pl

from utils.pre.marginal_costs.supply_utils import (
    load_zone_loads,
    remap_year_if_needed,
    strip_tz_if_needed,
)


def aggregate_lbmp_to_hourly(lbmp_df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate 5-minute real-time LBMP intervals to hourly averages."""
    aggregated = (
        lbmp_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("timestamp"))
        .group_by("timestamp", "zone")
        .agg(pl.col("lbmp_usd_per_mwh").mean().alias("lbmp_usd_per_mwh"))
        .sort("timestamp", "zone")
    )
    print(
        f"  Aggregated {lbmp_df.height:,} 5-minute intervals -> {aggregated.height:,} hourly averages"
    )
    return aggregated


def load_lbmp_for_zones(
    lbmp_s3_base: str,
    zone_names: list[str],
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load real-time LBMP data for the given zones and year."""
    base = lbmp_s3_base.rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(pl.col("zone").is_in(zone_names), pl.col("year") == year)
        .select("interval_start_est", "zone", "lbmp_usd_per_mwh")
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from LBMP collect()")
    if collected.is_empty():
        raise FileNotFoundError(
            f"No LBMP data found for zones={zone_names}, year={year} under {base}"
        )

    collected = strip_tz_if_needed(collected, "interval_start_est").rename(
        {"interval_start_est": "timestamp"}
    )
    zones_found = sorted(collected["zone"].unique().to_list())
    print(
        f"Loaded LBMP data: {len(collected):,} rows for zones {zones_found}, year {year}"
    )
    return collected


def compute_supply_energy_mc(
    utility_mapping: pl.DataFrame,
    lbmp_s3_base: str,
    zone_loads_s3_base: str,
    year: int,
    storage_options: dict[str, str],
    zone_load_year: int | None = None,
) -> pl.DataFrame:
    """Compute hourly utility-level supply energy MC from LBMP."""
    zone_load_year = year if zone_load_year is None else zone_load_year
    zone_names = sorted(utility_mapping["lbmp_zone_name"].unique().to_list())

    lbmp_df = aggregate_lbmp_to_hourly(
        load_lbmp_for_zones(lbmp_s3_base, zone_names, year, storage_options)
    )

    if len(zone_names) == 1:
        print(f"  Single-zone utility -> using {zone_names[0]} LBMP directly")
        result = lbmp_df.select(
            "timestamp", pl.col("lbmp_usd_per_mwh").alias("energy_cost_enduse")
        ).sort("timestamp")
    else:
        print(f"  Multi-zone utility -> load-weighting across {zone_names}")
        zone_loads = load_zone_loads(
            zone_loads_s3_base, zone_names, zone_load_year, storage_options
        )
        if zone_load_year != year:
            print(f"  Remapping zone load timestamps: {zone_load_year} -> {year}")
            zone_loads = remap_year_if_needed(
                zone_loads, "timestamp", zone_load_year, year
            )

        joined = lbmp_df.join(
            zone_loads.select("timestamp", "zone", "load_mw"),
            on=["timestamp", "zone"],
            how="inner",
        )
        if joined.is_empty():
            raise ValueError(
                "No matching timestamps between LBMP and zone loads. "
                "Check that both datasets cover the same year."
            )

        result = (
            joined.group_by("timestamp")
            .agg(
                (
                    (pl.col("lbmp_usd_per_mwh") * pl.col("load_mw")).sum()
                    / pl.col("load_mw").sum()
                ).alias("energy_cost_enduse")
            )
            .sort("timestamp")
        )

    avg_lbmp = result["energy_cost_enduse"].mean()
    print(f"  Energy MC: {result.height} hours, avg LBMP = ${avg_lbmp:.2f}/MWh")
    return result
