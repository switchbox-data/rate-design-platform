"""Shared utility helpers for supply marginal cost pipelines (NYISO & ISO-NE)."""

from __future__ import annotations

import calendar
import io
from datetime import datetime, timedelta
from pathlib import Path

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

# All eight ISO-NE load zones, used for New England system-wide peak identification
# (e.g. RNS / bulk TX allocation is driven by NE coincident peak).
ISONE_ALL_LOAD_ZONES: list[str] = [
    "CT",
    "ME",
    "NEMA",
    "NH",
    "RI",
    "SEMA",
    "VT",
    "WCMA",
]

DEFAULT_ISONE_BULK_TX_OUTPUT_S3_BASE = (
    "s3://data.sb/switchbox/marginal_costs/ri/bulk_tx/"
)
DEFAULT_NYISO_BULK_TX_OUTPUT_S3_BASE = (
    "s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/"
)

MC_COMPONENT_RELATIVE_PATHS: dict[str, str] = {
    "supply_energy": "supply/energy",
    "supply_capacity": "supply/capacity",
    "supply_ancillary": "supply/ancillary",
    "bulk_tx": "bulk_tx",
    "dist_sub_tx": "dist_and_sub_tx",
}


def build_mc_partition_path(
    state: str,
    component: str,
    utility: str,
    year: int,
    mc_base_path: str = "s3://data.sb/switchbox/marginal_costs",
) -> str:
    """Build canonical MC partition path for one state/component/utility/year."""
    component_relpath = MC_COMPONENT_RELATIVE_PATHS.get(component)
    if component_relpath is None:
        raise ValueError(
            f"Unsupported marginal cost component '{component}'. "
            f"Expected one of {sorted(MC_COMPONENT_RELATIVE_PATHS)}."
        )
    base = mc_base_path.rstrip("/")
    return f"{base}/{state.lower()}/{component_relpath}/utility={utility}/year={year}"


def build_mc_partition_parquet_path(
    state: str,
    component: str,
    utility: str,
    year: int,
    filename: str = "data.parquet",
    mc_base_path: str = "s3://data.sb/switchbox/marginal_costs",
) -> str:
    """Build canonical MC parquet object path for one partition."""
    return (
        f"{build_mc_partition_path(state, component, utility, year, mc_base_path)}/"
        f"{filename}"
    )


def list_partition_parquet_paths(path_partition: str) -> list[str]:
    """List parquet object paths in one partition directory."""
    if path_partition.startswith("s3://"):
        partition = S3Path(path_partition)
        if not partition.exists():
            return []
        return sorted(str(p) for p in partition.glob("*.parquet"))

    partition_local = Path(path_partition)
    if not partition_local.exists():
        return []
    return sorted(str(p) for p in partition_local.glob("*.parquet"))


def warn_if_multiple_partition_parquets(
    path_partition: str,
    expected_filename: str,
    context: str,
) -> None:
    """Emit a non-destructive warning when a partition has multiple parquet files."""
    parquet_paths = list_partition_parquet_paths(path_partition)
    if len(parquet_paths) <= 1:
        return

    filenames = [Path(path).name for path in parquet_paths]
    if expected_filename not in filenames:
        print(
            f"⚠️  WARNING [{context}]: partition has {len(parquet_paths)} parquet files, "
            f"but expected canonical '{expected_filename}' is missing."
        )
    else:
        print(
            f"⚠️  WARNING [{context}]: partition has {len(parquet_paths)} parquet files; "
            f"canonical file is '{expected_filename}'."
        )
    print(f"    Partition: {path_partition}")
    print(f"    Files: {', '.join(filenames)}")


def allocate_annual_exceedance_to_hours(
    load_df: pl.DataFrame,
    annual_cost_kw_year: float,
    n_peak_hours: int = 100,
    cost_col: str = "cost_per_kw",
) -> pl.DataFrame:
    """Allocate an annual $/kW-year cost to hours using top-N exceedance weighting.

    Identifies the top-N hours by load, computes a threshold as the maximum load
    strictly below the Nth-highest hour, and distributes the annual cost
    proportionally to each hour's exceedance above that threshold.

    This is a generic building block used by both supply capacity (FCA) and bulk
    transmission marginal cost pipelines.

    Args:
        load_df: DataFrame with columns ``timestamp`` (datetime) and ``load_mw`` (float).
        annual_cost_kw_year: Annual cost in $/kW-year to allocate.
        n_peak_hours: Number of top-load hours over which to spread the cost.
        cost_col: Name of the output cost column.

    Returns:
        DataFrame with columns ``timestamp`` and *cost_col*, containing only the
        *n_peak_hours* rows with non-zero cost.  Sorted by timestamp.

    Raises:
        ValueError: If load_df has fewer rows than *n_peak_hours*, total exceedance
            is non-positive, or weights fail to sum to 1.
    """
    if load_df.height < n_peak_hours:
        raise ValueError(
            f"Load profile has only {load_df.height} hours, "
            f"need at least {n_peak_hours} for exceedance allocation"
        )

    sorted_load = load_df.sort("load_mw", descending=True)
    top_n = sorted_load.head(n_peak_hours)
    load_nth = float(top_n["load_mw"][-1])

    # Threshold = max load strictly below the Nth-highest (tie-safe)
    below = load_df.filter(pl.col("load_mw") < load_nth)["load_mw"]
    threshold = float(below.max()) if not below.is_empty() else 0.0  # type: ignore[arg-type]

    result = top_n.with_columns((pl.col("load_mw") - threshold).alias("exceedance"))
    total_exceedance = float(result["exceedance"].sum())
    if total_exceedance <= 0:
        raise ValueError(
            f"Total exceedance is zero or negative. "
            f"Threshold={threshold:.2f} MW, "
            f"max load={float(sorted_load['load_mw'][0]):.2f} MW"
        )

    result = result.with_columns(
        (pl.col("exceedance") / total_exceedance * annual_cost_kw_year).alias(cost_col)
    )

    weight_sum = float((result["exceedance"] / total_exceedance).sum())
    if abs(weight_sum - 1.0) > 1e-6:
        raise ValueError(f"Exceedance weights sum to {weight_sum:.6f}, expected 1.0")

    n_nonzero = result.filter(pl.col(cost_col) > 0).height
    print(
        f"  Annual exceedance allocation: ${annual_cost_kw_year:.4f}/kW-yr, "
        f"threshold={threshold:,.1f} MW, "
        f"{n_nonzero} peak hours (of {n_peak_hours} requested)"
    )

    return result.select("timestamp", cost_col).sort("timestamp")


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
    path_partition = f"{base}utility={utility}/year={year}"
    output_path = f"{path_partition}/zero.parquet"

    warn_if_multiple_partition_parquets(
        path_partition=path_partition,
        expected_filename="zero.parquet",
        context="pre-write zero energy MC",
    )

    # Write directly to the specific path (not using partitioning)
    energy_df.write_parquet(
        output_path,
        storage_options=storage_options,
    )

    warn_if_multiple_partition_parquets(
        path_partition=path_partition,
        expected_filename="zero.parquet",
        context="post-write zero energy MC",
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
    path_partition = f"{base}utility={utility}/year={year}"
    output_path = f"{path_partition}/zero.parquet"

    warn_if_multiple_partition_parquets(
        path_partition=path_partition,
        expected_filename="zero.parquet",
        context="pre-write zero capacity MC",
    )

    # Write directly to the specific path (not using partitioning)
    capacity_df.write_parquet(
        output_path,
        storage_options=storage_options,
    )

    warn_if_multiple_partition_parquets(
        path_partition=path_partition,
        expected_filename="zero.parquet",
        context="post-write zero capacity MC",
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
    path_partition = f"{base}utility={utility}/year={year}"
    output_path = f"{path_partition}/data.parquet"

    warn_if_multiple_partition_parquets(
        path_partition=path_partition,
        expected_filename="data.parquet",
        context=f"pre-write {component} MC",
    )
    component_df.write_parquet(output_path, storage_options=storage_options)

    warn_if_multiple_partition_parquets(
        path_partition=path_partition,
        expected_filename="data.parquet",
        context=f"post-write {component} MC",
    )

    print(f"\n✓ Saved {component} MC to {output_path}")
    print(f"  Rows: {len(component_df):,}")
    print(f"  Columns: {', '.join(component_df.columns)}")
