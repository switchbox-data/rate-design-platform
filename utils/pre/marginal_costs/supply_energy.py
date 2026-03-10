"""Energy marginal cost computation for supply MCs (NYISO LBMP & ISO-NE LMP)."""

from __future__ import annotations

import polars as pl

from utils.pre.marginal_costs.supply_utils import (
    DEFAULT_ISONE_LMP_S3_BASE,
    load_zone_loads,
    remap_year_if_needed,
    strip_tz_if_needed,
)

# ---------------------------------------------------------------------------
# NYISO LBMP helpers
# ---------------------------------------------------------------------------


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
    """Load NYISO real-time LBMP data for the given zones and year."""
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


# ---------------------------------------------------------------------------
# ISO-NE LMP helpers
# ---------------------------------------------------------------------------


def load_lmp_for_zone(
    zone: str,
    year: int,
    storage_options: dict[str, str],
    lmp_s3_base: str = DEFAULT_ISONE_LMP_S3_BASE,
) -> pl.DataFrame:
    """Load ISO-NE hourly real-time LMP for a single zone and year.

    ISO-NE LMP data is already hourly (unlike NYISO's 5-min LBMP), so no
    sub-hourly aggregation is needed.  Reads from the Hive-partitioned
    ``s3://data.sb/isone/lmp/real_time/zones/`` tree, filters by *zone* and
    *year*, and returns a DataFrame with columns ``timestamp`` and
    ``lmp_usd_per_mwh``.
    """
    base = lmp_s3_base.rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(pl.col("zone") == zone, pl.col("year") == year)
        .select("interval_start_et", "zone", "lmp_usd_per_mwh")
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from ISO-NE LMP collect()")
    if collected.is_empty():
        raise FileNotFoundError(
            f"No ISO-NE LMP data found for zone={zone!r}, year={year} under {base}"
        )

    collected = strip_tz_if_needed(collected, "interval_start_et").rename(
        {"interval_start_et": "timestamp"}
    )

    # Collapse duplicate timestamps (e.g., from DST fallback hour) by taking mean LMP.
    # This ensures we have at most one value per unique timestamp before further processing.
    # The final alignment to exactly 8760 hours happens in prepare_component_output.
    n_before = collected.height
    n_unique_before = collected.select(pl.col("timestamp").n_unique()).item()
    collected = (
        collected.group_by("timestamp")
        .agg(pl.col("lmp_usd_per_mwh").mean().alias("lmp_usd_per_mwh"))
        .sort("timestamp")
    )
    n_after = collected.height
    if n_before != n_after or n_unique_before != n_after:
        print(
            f"  Collapsed duplicate timestamps: {n_before} rows "
            f"({n_unique_before} unique) → {n_after} rows"
        )

    print(
        f"Loaded ISO-NE LMP data: {n_after:,} hourly rows "
        f"for zone {zone!r}, year {year}"
    )
    return collected


def compute_isone_supply_energy_mc(
    zone: str,
    year: int,
    storage_options: dict[str, str],
    lmp_s3_base: str = DEFAULT_ISONE_LMP_S3_BASE,
) -> pl.DataFrame:
    """Compute hourly supply energy MC from ISO-NE real-time LMP.

    ISO-NE utilities are single-zone, so no load-weighting is needed.
    The LMP is used directly as ``energy_cost_enduse`` ($/MWh).
    """
    lmp_df = load_lmp_for_zone(zone, year, storage_options, lmp_s3_base)

    result = lmp_df.select(
        "timestamp",
        pl.col("lmp_usd_per_mwh").alias("energy_cost_enduse"),
    ).sort("timestamp")

    # Final check: ensure no duplicate timestamps before returning
    n_rows = result.height
    n_unique = result.select(pl.col("timestamp").n_unique()).item()
    if n_rows != n_unique:
        raise ValueError(
            f"Energy MC DataFrame has {n_rows} rows but only {n_unique} unique timestamps. "
            f"Duplicate timestamps detected before prepare_component_output."
        )

    avg_lmp = result["energy_cost_enduse"].mean()
    print(f"  Energy MC (ISO-NE): {result.height} hours, avg LMP = ${avg_lmp:.2f}/MWh")
    return result


# ---------------------------------------------------------------------------
# NYISO LBMP computation (multi-zone load-weighted)
# ---------------------------------------------------------------------------


def compute_supply_energy_mc(
    utility_mapping: pl.DataFrame,
    lbmp_s3_base: str,
    zone_loads_s3_base: str,
    year: int,
    storage_options: dict[str, str],
    zone_load_year: int | None = None,
) -> pl.DataFrame:
    """Compute hourly utility-level supply energy MC from NYISO LBMP."""
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
