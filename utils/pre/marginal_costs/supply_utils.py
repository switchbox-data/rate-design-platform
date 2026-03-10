"""Shared utility helpers for supply marginal cost pipelines (NYISO & ISO-NE)."""

from __future__ import annotations

import calendar
import io
from datetime import datetime, timedelta

import polars as pl
from cloudpathlib import S3Path

# ---------------------------------------------------------------------------
# NYISO defaults
# ---------------------------------------------------------------------------
DEFAULT_NYISO_LBMP_S3_BASE = "s3://data.sb/nyiso/lbmp/real_time/zones/"
DEFAULT_NYISO_ICAP_S3_BASE = "s3://data.sb/nyiso/icap/"
DEFAULT_NYISO_ZONE_LOADS_S3_BASE = "s3://data.sb/nyiso/hourly_demand/zones/"
DEFAULT_NYISO_ZONE_MAPPING_PATH = (
    "s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv"
)
DEFAULT_NYISO_OUTPUT_S3_BASE = "s3://data.sb/switchbox/marginal_costs/ny/supply/"

VALID_NYISO_UTILITIES = frozenset(
    {"cenhud", "coned", "nimo", "nyseg", "or", "rge", "psegli"}
)

# Backward-compatible aliases (used by generate scripts and other importers)
DEFAULT_LBMP_S3_BASE = DEFAULT_NYISO_LBMP_S3_BASE
DEFAULT_ICAP_S3_BASE = DEFAULT_NYISO_ICAP_S3_BASE
DEFAULT_ZONE_LOADS_S3_BASE = DEFAULT_NYISO_ZONE_LOADS_S3_BASE
DEFAULT_ZONE_MAPPING_PATH = DEFAULT_NYISO_ZONE_MAPPING_PATH
DEFAULT_OUTPUT_S3_BASE = DEFAULT_NYISO_OUTPUT_S3_BASE
VALID_UTILITIES = VALID_NYISO_UTILITIES

# ---------------------------------------------------------------------------
# ISO-NE defaults
# ---------------------------------------------------------------------------
DEFAULT_ISONE_LMP_S3_BASE = "s3://data.sb/isone/lmp/real_time/zones/"
DEFAULT_ISONE_ANCILLARY_S3_BASE = "s3://data.sb/isone/ancillary/"
DEFAULT_ISONE_ZONE_LOADS_S3_BASE = "s3://data.sb/isone/hourly_demand/zones/"
DEFAULT_ISONE_FCA_S3_PATH = "s3://data.sb/isone/capacity/fca/data.parquet"
DEFAULT_ISONE_OUTPUT_S3_BASE = "s3://data.sb/switchbox/marginal_costs/ri/supply/"

VALID_ISONE_UTILITIES = frozenset({"rie"})

# Maps each ISO-NE utility to its single load zone. ISO-NE utilities are
# single-zone, so no load-weighting is needed (unlike NYISO multi-zone).
ISONE_UTILITY_ZONES: dict[str, str] = {"rie": "RI"}

# ISO-NE FCA (Forward Capacity Auction) capacity zone mappings.
# Maps each ISO-NE utility to its capacity zone ID for FCA price resolution.
ISONE_UTILITY_CAPACITY_ZONES: dict[str, int] = {"rie": 8506}  # SENE

# Fallback capacity zone ID when utility-specific zone price is absent.
ISONE_CAPACITY_ZONE_FALLBACK: int = 8500  # System / Rest-of-Pool

# Maps each ISO-NE utility to its load zones for aggregate peak identification.
# For SENE (Southeast New England), aggregate RI + SEMA zones.
ISONE_CAPACITY_ZONE_LOAD_ZONES: dict[str, list[str]] = {
    "rie": ["RI", "SEMA"]
}  # SENE aggregate


def load_zone_mapping(path: str, storage_options: dict[str, str]) -> pl.DataFrame:
    """Load utility zone mapping CSV from local path or S3."""
    if path.startswith("s3://"):
        csv_bytes = S3Path(path).read_bytes()
        df = pl.read_csv(io.BytesIO(csv_bytes))
    else:
        df = pl.read_csv(path)

    required = {
        "utility",
        "load_zone_letter",
        "lbmp_zone_name",
        "icap_locality",
        "gen_capacity_zone",
        "capacity_weight",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Zone mapping CSV missing columns: {sorted(missing)}")

    print(f"Loaded zone mapping with {len(df)} rows from {path}")
    return df


def get_utility_mapping(mapping_df: pl.DataFrame, utility: str) -> pl.DataFrame:
    """Filter zone mapping to a single utility."""
    rows = mapping_df.filter(pl.col("utility") == utility)
    if rows.is_empty():
        available = sorted(mapping_df["utility"].unique().to_list())
        raise ValueError(
            f"Utility '{utility}' not found in zone mapping. Available: {available}"
        )
    return rows


def strip_tz_if_needed(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Strip timezone from datetime column when present."""
    ts_dtype = df.schema[col]
    if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
        return df.with_columns(pl.col(col).dt.replace_time_zone(None).alias(col))
    return df


def load_zone_loads(
    zone_loads_s3_base: str,
    zone_names: list[str],
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load NYISO zone-level hourly loads for selected zones and year."""
    base = zone_loads_s3_base.rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(pl.col("zone").is_in(zone_names), pl.col("year") == year)
        .select("timestamp", "zone", "load_mw")
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from zone loads collect()")
    if collected.is_empty():
        raise FileNotFoundError(
            f"No zone load data found for zones={zone_names}, year={year} under {base}"
        )

    collected = strip_tz_if_needed(collected, "timestamp")
    zones_found = sorted(collected["zone"].unique().to_list())
    print(
        f"Loaded zone loads: {len(collected):,} rows for zones {zones_found}, year {year}"
    )
    return collected


def remap_year_if_needed(
    df: pl.DataFrame, timestamp_col: str, from_year: int, to_year: int
) -> pl.DataFrame:
    """Offset timestamps by integer years when years differ."""
    if from_year == to_year:
        return df
    offset = f"{to_year - from_year}y"
    return df.with_columns(
        pl.col(timestamp_col).dt.offset_by(offset).alias(timestamp_col)
    )


def build_cairo_8760_timestamps(year: int) -> pl.DataFrame:
    """Build Cairo-compatible 8760 naive timestamps (drops Dec 31 in leap years)."""
    start = datetime(year, 1, 1, 0, 0, 0)
    end = datetime(year, 12, 31, 23, 0, 0)
    timestamps: list[datetime] = []
    cur = start
    while cur <= end:
        timestamps.append(cur)
        cur += timedelta(hours=1)

    if calendar.isleap(year):
        timestamps = [t for t in timestamps if not (t.month == 12 and t.day == 31)]

    if len(timestamps) != 8760:
        raise ValueError(
            f"Expected 8760 timestamps, got {len(timestamps)} for year {year}"
        )
    return pl.DataFrame({"timestamp": timestamps})


def prepare_component_output(
    df: pl.DataFrame,
    year: int,
    input_col: str,
    output_col: str,
    scale: float,
) -> pl.DataFrame:
    """Prepare a single hourly component as Cairo-compatible 8760 output.

    This function ensures exactly 8760 unique timestamps corresponding to 8760 hours
    in the year, properly handling DST transitions (fallback hour creates duplicates).

    Steps:
    1. Truncate timestamps to hour precision
    2. Collapse any duplicate timestamps (e.g., from DST fallback) by taking mean
    3. Join with Cairo's reference 8760 timestamps (handles leap years by dropping Dec 31)
    4. Validate data completeness - raise error if any months are missing
    5. Apply scaling and validate final output has exactly 8760 unique timestamps

    Raises:
        ValueError: If source data is incomplete (missing months or hours).
    """
    ref_8760 = build_cairo_8760_timestamps(year)

    # Step 1-2: Truncate to hour and collapse duplicates (e.g., DST fallback hour)
    # This ensures we have at most one row per unique timestamp before joining
    n_before = df.height
    n_unique_before = df.select(pl.col("timestamp").n_unique()).item()
    hourly = (
        df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("timestamp"))
        .group_by("timestamp")
        .agg(pl.col(input_col).mean().alias(input_col))
        .sort("timestamp")
    )
    n_after = hourly.height
    if n_before != n_after or n_unique_before != n_after:
        print(
            f"  Collapsed duplicate timestamps in {input_col}: {n_before} rows "
            f"({n_unique_before} unique) → {n_after} rows"
        )

    # Step 3: Join with reference 8760 timestamps (Cairo-compatible, handles leap years)
    output = ref_8760.join(hourly, on="timestamp", how="left").select(
        "timestamp", input_col
    )

    # Step 4: Validate data completeness - check for missing months (allow DST transitions)
    missing_hours = output.filter(pl.col(input_col).is_null())
    if missing_hours.height > 0:
        # Check which months are present in source data
        months_present = (
            hourly.with_columns(pl.col("timestamp").dt.month().alias("month"))
            .select("month")
            .unique()
            .sort("month")
            .to_series()
            .to_list()
        )
        expected_months = list(range(1, 13))
        missing_months = sorted(set(expected_months) - set(months_present))

        # If we have missing months, that's an error (can't interpolate entire months)
        if missing_months:
            raise ValueError(
                f"{output_col}: Source data is incomplete for year {year}. "
                f"Missing months: {missing_months}. Present months: {months_present}. "
                f"Backfill missing months in source data before generating marginal costs."
            )

        # Missing individual hours (e.g., DST spring forward, data gaps) are acceptable
        # We'll interpolate them from surrounding hours to preserve data patterns
        if missing_hours.height > 0:
            # Identify DST spring forward hour if present
            missing_hours_with_month = missing_hours.with_columns(
                pl.col("timestamp").dt.month().alias("month"),
                pl.col("timestamp").dt.hour().alias("hour"),
            )
            dst_spring_forward = missing_hours_with_month.filter(
                (pl.col("month") == 3) & (pl.col("hour") == 2)
            )
            n_dst = dst_spring_forward.height
            n_other = missing_hours.height - n_dst

            if n_dst > 0:
                print(
                    f"  Note: Interpolating {n_dst} DST spring forward hour(s) "
                    f"from surrounding hours (expected behavior for Eastern timezone)"
                )
            if n_other > 0:
                print(
                    f"  Note: Interpolating {n_other} additional missing hour(s) "
                    f"from surrounding hours (data gaps in source)"
                )

    # Step 5: Interpolate DST spring forward hour, then apply scaling
    # Use forward fill then backward fill to handle DST gaps (interpolate between surrounding hours)
    output = output.with_columns(
        (
            pl.col(input_col)
            .interpolate()  # Linear interpolation for gaps
            .forward_fill()  # Forward fill any remaining nulls at start
            .backward_fill()  # Backward fill any remaining nulls at end
            * scale
        ).alias(output_col)
    ).select("timestamp", output_col)

    if output.height != 8760:
        raise ValueError(
            f"{output_col} output has {output.height} rows, expected 8760. "
            f"Check DST handling and timestamp alignment."
        )
    n_unique = output.select(pl.col("timestamp").n_unique()).item()
    if n_unique != 8760:
        raise ValueError(
            f"{output_col} output has {n_unique} unique timestamps, expected 8760. "
            f"Duplicate timestamps detected after join."
        )
    if output.filter(pl.col(output_col).is_null()).height > 0:
        raise ValueError(f"{output_col} output has nulls after processing")
    return output


def assemble_output(
    energy_df: pl.DataFrame,
    capacity_df: pl.DataFrame,
    year: int,
) -> pl.DataFrame:
    """Assemble final combined output matching Cambium-compatible schema."""
    ref_8760 = build_cairo_8760_timestamps(year)
    energy_hourly = (
        energy_df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("timestamp"))
        .group_by("timestamp")
        .agg(pl.col("energy_cost_enduse").mean())
        .sort("timestamp")
    )
    capacity_hourly = (
        capacity_df.with_columns(
            pl.col("timestamp").dt.truncate("1h").alias("timestamp")
        )
        .group_by("timestamp")
        .agg(pl.col("capacity_cost_per_kw").mean())
        .sort("timestamp")
    )

    output = (
        ref_8760.join(energy_hourly, on="timestamp", how="left")
        .join(capacity_hourly, on="timestamp", how="left")
        .with_columns(
            pl.col("energy_cost_enduse").fill_null(0.0).alias("energy_cost_enduse"),
            (pl.col("capacity_cost_per_kw").fill_null(0.0) * 1000.0).alias(
                "capacity_cost_enduse"
            ),
        )
        .select("timestamp", "energy_cost_enduse", "capacity_cost_enduse")
    )

    if output.height != 8760:
        raise ValueError(f"Output has {output.height} rows, expected 8760")

    energy_nulls = output.filter(pl.col("energy_cost_enduse").is_null()).height
    capacity_nulls = output.filter(pl.col("capacity_cost_enduse").is_null()).height
    if energy_nulls > 0 or capacity_nulls > 0:
        raise ValueError(
            f"Output has nulls: energy={energy_nulls}, capacity={capacity_nulls}"
        )

    avg_energy = output["energy_cost_enduse"].mean()
    avg_capacity = output["capacity_cost_enduse"].mean()
    max_capacity = output["capacity_cost_enduse"].max()
    nonzero_cap = output.filter(pl.col("capacity_cost_enduse") > 0).height
    print("\nOutput summary (8760 rows):")
    print(f"  Energy:   avg=${avg_energy:.2f}/MWh")
    print(f"  Capacity: avg=${avg_capacity:.2f}/MWh, max=${max_capacity:.2f}/MWh")
    print(f"  Capacity: {nonzero_cap} non-zero hours out of 8760")
    return output


def generate_zero_energy_mc(year: int) -> pl.DataFrame:
    """Generate a zero-filled energy MC DataFrame with 8760 hours.

    Creates a placeholder energy marginal cost file with all zeros.
    Used for delivery-only runs where supply energy MC is not needed.

    Args:
        year: Target year for timestamp generation.

    Returns:
        DataFrame with columns: `timestamp` (datetime), `energy_cost_enduse` ($/MWh, all zeros).
        Exactly 8760 rows matching Cairo's expected format.
    """
    timestamps_df = build_cairo_8760_timestamps(year)
    energy_df = timestamps_df.with_columns(pl.lit(0.0).alias("energy_cost_enduse"))
    return energy_df


def generate_zero_capacity_mc(year: int) -> pl.DataFrame:
    """Generate a zero-filled capacity MC DataFrame with 8760 hours.

    Creates a placeholder capacity marginal cost file with all zeros.
    Used for ISO-NE utilities where capacity MC (FCM) integration is not yet implemented.

    Args:
        year: Target year for timestamp generation.

    Returns:
        DataFrame with columns: `timestamp` (datetime), `capacity_cost_enduse` ($/MWh, all zeros).
        Exactly 8760 rows matching Cairo's expected format.
    """
    timestamps_df = build_cairo_8760_timestamps(year)
    capacity_df = timestamps_df.with_columns(pl.lit(0.0).alias("capacity_cost_enduse"))
    return capacity_df


def save_zero_energy_mc(
    energy_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    """Write zero-filled energy MC parquet to S3 with custom filename zero.parquet.

    Writes directly to a specific path with filename zero.parquet (instead of using
    Hive partitioning which would create data.parquet).

    Args:
        energy_df: DataFrame with timestamp and energy_cost_enduse columns.
        utility: Utility short name.
        year: Target year.
        output_s3_base: S3 base path for output.
        storage_options: AWS storage options for S3 access.
    """
    base = output_s3_base.rstrip("/") + "/energy/"
    output_path = f"{base}utility={utility}/year={year}/zero.parquet"

    # Write directly to the specific path (not using partitioning)
    energy_df.write_parquet(
        output_path,
        storage_options=storage_options,
    )

    print(f"\n✓ Saved zero-filled energy MC to {output_path}")
    print(f"  Rows: {len(energy_df):,}")
    print(f"  Columns: {', '.join(energy_df.columns)}")


def save_zero_capacity_mc(
    capacity_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    """Write zero-filled capacity MC parquet to S3 with custom filename zero.parquet.

    Writes directly to a specific path with filename zero.parquet (instead of using
    Hive partitioning which would create data.parquet).

    Args:
        capacity_df: DataFrame with timestamp and capacity_cost_enduse columns.
        utility: Utility short name.
        year: Target year.
        output_s3_base: S3 base path for output.
        storage_options: AWS storage options for S3 access.
    """
    base = output_s3_base.rstrip("/") + "/capacity/"
    output_path = f"{base}utility={utility}/year={year}/zero.parquet"

    # Write directly to the specific path (not using partitioning)
    capacity_df.write_parquet(
        output_path,
        storage_options=storage_options,
    )

    print(f"\n✓ Saved zero-filled capacity MC to {output_path}")
    print(f"  Rows: {len(capacity_df):,}")
    print(f"  Columns: {', '.join(capacity_df.columns)}")


def save_component_output(
    component_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
    component: str,
) -> None:
    """Write a single component parquet to S3 with Hive-style partitioning."""
    # Final validation: must have exactly 8760 rows with unique timestamps
    if component_df.height != 8760:
        raise ValueError(
            f"Expected 8760 rows before writing {component} MC, got {component_df.height} rows. "
            f"Check prepare_component_output logic."
        )
    n_unique = component_df.select(pl.col("timestamp").n_unique()).item()
    if n_unique != 8760:
        raise ValueError(
            f"Expected 8760 unique timestamps before writing {component} MC, got {n_unique}. "
            f"Data contains duplicate timestamps."
        )

    base = output_s3_base.rstrip("/") + f"/{component}/"
    # Write directly to data.parquet path (not using partition_by which creates 00000000.parquet)
    output_path = f"{base}utility={utility}/year={year}/data.parquet"
    component_df.write_parquet(output_path, storage_options=storage_options)

    print(f"\n✓ Saved {component} MC to {output_path}")
    print(f"  Rows: {len(component_df):,}")
    print(f"  Columns: {', '.join(component_df.columns)}")
