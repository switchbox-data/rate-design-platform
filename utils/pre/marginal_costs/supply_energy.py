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
        # Select only the columns we need. Don't select "zone" to avoid conflicts
        # between partition zone and data zone columns. We've already filtered by zone.
        .select("interval_start_et", "lmp_usd_per_mwh")
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from ISO-NE LMP collect()")
    if collected.is_empty():
        raise FileNotFoundError(
            f"No ISO-NE LMP data found for zone={zone!r}, year={year} under {base}"
        )

    # Handle DST properly: convert to UTC first to preserve both DST fallback hours
    # as distinct timestamps. When DST falls back, we get two 1:00 AM hours in local
    # time, but they are distinct UTC hours. We preserve both by keeping UTC timestamps
    # until conversion to local, then handle the duplicate local time explicitly.
    ts_col = "interval_start_et"
    ts_dtype = collected.schema[ts_col]
    if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
        # Convert to UTC to preserve DST fallback hours as distinct timestamps
        # This gives us 8761 hours for a year with DST fallback (instead of losing one)
        collected = collected.with_columns(
            pl.col(ts_col).dt.convert_time_zone("UTC").alias("timestamp_utc")
        )
        # Convert UTC to local naive time (America/New_York)
        # DST fallback hours will map to the same local time, but we preserve both
        # by keeping them as separate rows with the same timestamp
        collected = collected.with_columns(
            pl.col("timestamp_utc")
            .dt.convert_time_zone("America/New_York")
            .dt.replace_time_zone(None)
            .alias("timestamp")
        )
        collected = collected.drop("timestamp_utc")
    else:
        # Already naive, just rename
        collected = collected.rename({ts_col: "timestamp"})

    # Check for duplicates. DST fallback creates duplicate local timestamps (same
    # wall-clock time, different UTC times). We preserve both by keeping them as
    # separate rows. True duplicates (same UTC time) should be collapsed.
    n_before = collected.height
    n_unique_before = collected.select(pl.col("timestamp").n_unique()).item()

    # Count how many rows we have vs unique timestamps
    # For DST fallback: we expect 8760 rows with 8759 unique timestamps (one duplicate)
    # For true duplicates (partition overlap): we collapse by taking mean
    if n_before != n_unique_before:
        # We have duplicate timestamps. For DST fallback, we want to preserve both
        # values, not average them. We'll keep both rows and let prepare_component_output
        # handle the alignment to 8760 (it will take the mean when joining to ref_8760).
        # However, if we have more than 1 duplicate, it might be partition overlap.
        n_duplicates = n_before - n_unique_before
        if n_duplicates == 1:
            # Likely DST fallback - preserve both rows (prepare_component_output will handle)
            print(
                f"  DST fallback detected: {n_before} rows with {n_unique_before} unique timestamps. "
                f"Preserving both DST fallback hours (will be handled in alignment to 8760)."
            )
            collected = collected.select("timestamp", "lmp_usd_per_mwh").sort(
                "timestamp"
            )
            n_after = collected.height
        else:
            # Multiple duplicates - likely partition overlap, collapse by mean
            print(
                f"  Warning: Found {n_duplicates} duplicate timestamps "
                f"({n_before} total rows, {n_unique_before} unique timestamps). "
                f"Collapsing by taking mean LMP."
            )
            collected = (
                collected.group_by("timestamp")
                .agg(pl.col("lmp_usd_per_mwh").mean().alias("lmp_usd_per_mwh"))
                .sort("timestamp")
            )
            n_after = collected.height
            print(
                f"  Collapsed duplicate timestamps: {n_before} rows "
                f"({n_unique_before} unique) → {n_after} rows"
            )
    else:
        # No duplicates - all timestamps are unique
        collected = collected.select("timestamp", "lmp_usd_per_mwh").sort("timestamp")
        n_after = collected.height

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

    # Note: We may have duplicate timestamps from DST fallback (preserved from load_lmp_for_zone).
    # prepare_component_output will handle these by grouping and taking the mean when aligning to 8760.

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
