"""Generate utility-level supply marginal costs (energy LBMP + capacity ICAP MCOS).

This script replaces Cambium-based supply marginal costs with real NYISO data:
- **Energy**: Day-ahead LBMP prices, load-weighted across the utility's LBMP zones.
- **Capacity**: ICAP Spot prices allocated to hours using threshold-exceedance
  weights (8 peak hours per month, analogous to Cambium § 6.8).

Input data:
    - LBMP day-ahead zonal prices:
        s3://data.sb/nyiso/lbmp/day_ahead/zones/zone={NAME}/year={YYYY}/month={MM}/data.parquet
    - ICAP spot prices (4 localities):
        s3://data.sb/nyiso/icap/year={YYYY}/month={M}/data.parquet
    - EIA zone-level hourly loads (for LBMP load-weighting):
        s3://data.sb/eia/hourly_demand/zones/region=nyiso/zone={letter}/year={YYYY}/month={M}/data.parquet
    - EIA utility-level hourly loads (for ICAP peak identification):
        s3://data.sb/eia/hourly_demand/utilities/region=nyiso/utility={name}/year={YYYY}/month={M}/data.parquet
    - Utility zone mapping CSV (utility → LBMP zone, ICAP locality, capacity_weight):
        s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv

Output:
    s3://data.sb/switchbox/marginal_costs/ny/supply/utility={utility}/year={YYYY}/data.parquet
    Schema: timestamp (datetime), energy_cost_enduse ($/MWh), capacity_cost_enduse ($/MWh)
    This matches the Cambium parquet schema consumed by _load_cambium_marginal_costs().

Usage:
    # Inspect results (no upload)
    uv run python utils/pre/generate_utility_supply_mc.py \\
        --utility nyseg --year 2025

    # Upload to S3
    uv run python utils/pre/generate_utility_supply_mc.py \\
        --utility nyseg --year 2025 --upload

    # Override zone mapping path
    uv run python utils/pre/generate_utility_supply_mc.py \\
        --utility coned --year 2025 \\
        --zone-mapping-path data/nyiso/zone_mapping/csv/ny_utility_zone_mapping.csv
"""

from __future__ import annotations

import argparse
import io
from datetime import datetime, timedelta

import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.pre.generate_utility_tx_dx_mc import (
    load_utility_load_profile,
    normalize_load_to_cairo_8760,
)

# ── Default S3 paths ─────────────────────────────────────────────────────────

DEFAULT_LBMP_S3_BASE = "s3://data.sb/nyiso/lbmp/day_ahead/zones/"
DEFAULT_ICAP_S3_BASE = "s3://data.sb/nyiso/icap/"
DEFAULT_ZONE_LOADS_S3_BASE = "s3://data.sb/eia/hourly_demand/zones/"
DEFAULT_UTILITY_LOADS_S3_BASE = "s3://data.sb/eia/hourly_demand/utilities/"
DEFAULT_ZONE_MAPPING_PATH = (
    "s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv"
)
DEFAULT_OUTPUT_S3_BASE = "s3://data.sb/switchbox/marginal_costs/ny/supply/"

# Number of peak hours per month for ICAP MCOS allocation
N_PEAK_HOURS_PER_MONTH = 8

# Valid NY utilities
VALID_UTILITIES = frozenset({"cenhud", "coned", "nimo", "nyseg", "or", "rge", "psegli"})


# ── Zone mapping ─────────────────────────────────────────────────────────────


def load_zone_mapping(
    path: str,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load the utility zone mapping CSV.

    Args:
        path: Local or S3 path to ny_utility_zone_mapping.csv.
        storage_options: AWS storage options for S3 reads.

    Returns:
        DataFrame with columns: utility, load_zone_letter, lbmp_zone_name,
        icap_locality, gen_capacity_zone, capacity_weight.
    """
    if path.startswith("s3://"):
        s3_path = S3Path(path)
        csv_bytes = s3_path.read_bytes()
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


# ── Energy MC (LBMP) ─────────────────────────────────────────────────────────


def load_lbmp_for_zones(
    lbmp_s3_base: str,
    zone_names: list[str],
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load day-ahead LBMP data for the given zones and year.

    Args:
        lbmp_s3_base: S3 base path for LBMP data (e.g. s3://data.sb/nyiso/lbmp/day_ahead/zones/).
        zone_names: List of LBMP zone names (e.g. ["WEST", "GENESE"]).
        year: Target year.
        storage_options: AWS storage options.

    Returns:
        DataFrame with columns: timestamp (naive), zone, lbmp_usd_per_mwh.
    """
    lbmp_s3_base = lbmp_s3_base.rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            lbmp_s3_base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(
            pl.col("zone").is_in(zone_names),
            pl.col("year") == year,
        )
        .select("interval_start_est", "zone", "lbmp_usd_per_mwh")
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from LBMP collect()")
    if collected.is_empty():
        raise FileNotFoundError(
            f"No LBMP data found for zones={zone_names}, year={year} under {lbmp_s3_base}"
        )

    # Strip timezone to get naive EST timestamps for consistency
    ts_dtype = collected.schema["interval_start_est"]
    if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
        collected = collected.with_columns(
            pl.col("interval_start_est")
            .dt.replace_time_zone(None)
            .alias("interval_start_est")
        )

    collected = collected.rename({"interval_start_est": "timestamp"})

    zones_found = sorted(collected["zone"].unique().to_list())
    print(
        f"Loaded LBMP data: {len(collected):,} rows for zones {zones_found}, year {year}"
    )
    return collected


def load_zone_loads(
    zone_loads_s3_base: str,
    zone_letters: list[str],
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load EIA zone-level hourly loads for LBMP weighting.

    Args:
        zone_loads_s3_base: S3 base path for zone loads.
        zone_letters: Zone letters (e.g. ["A", "C", "D"]).
        year: Target year.
        storage_options: AWS storage options.

    Returns:
        DataFrame with columns: timestamp (naive), zone, load_mw.
    """
    zone_loads_s3_base = zone_loads_s3_base.rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            zone_loads_s3_base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(
            pl.col("region") == "nyiso",
            pl.col("zone").is_in(zone_letters),
            pl.col("year") == year,
        )
        .select("timestamp", "zone", "load_mw")
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from zone loads collect()")
    if collected.is_empty():
        raise FileNotFoundError(
            f"No zone load data found for zones={zone_letters}, year={year} "
            f"under {zone_loads_s3_base}"
        )

    # Strip timezone
    ts_dtype = collected.schema["timestamp"]
    if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
        collected = collected.with_columns(
            pl.col("timestamp").dt.replace_time_zone(None).alias("timestamp")
        )

    zones_found = sorted(collected["zone"].unique().to_list())
    print(
        f"Loaded zone loads: {len(collected):,} rows for zones {zones_found}, year {year}"
    )
    return collected


def compute_energy_mc(
    utility_mapping: pl.DataFrame,
    lbmp_s3_base: str,
    zone_loads_s3_base: str,
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Compute hourly energy marginal costs from LBMP data.

    For single-zone utilities, returns the zone's LBMP directly.
    For multi-zone utilities, computes load-weighted average across zones.

    Args:
        utility_mapping: Zone mapping rows for this utility.
        lbmp_s3_base: S3 base for LBMP data.
        zone_loads_s3_base: S3 base for EIA zone loads.
        year: Target year.
        storage_options: AWS storage options.

    Returns:
        DataFrame with columns: timestamp, energy_cost_enduse ($/MWh).
    """
    # Get unique zone names and letters for this utility
    zone_info = utility_mapping.select("lbmp_zone_name", "load_zone_letter").unique()
    zone_names = sorted(zone_info["lbmp_zone_name"].unique().to_list())
    zone_letters = sorted(zone_info["load_zone_letter"].unique().to_list())

    # Map zone letters to LBMP zone names
    letter_to_name: dict[str, str] = dict(
        zip(
            zone_info["load_zone_letter"].to_list(),
            zone_info["lbmp_zone_name"].to_list(),
            strict=False,
        )
    )

    # Load LBMP data for all relevant zones
    lbmp_df = load_lbmp_for_zones(lbmp_s3_base, zone_names, year, storage_options)

    if len(zone_names) == 1:
        # Single-zone utility: use LBMP directly
        print(f"  Single-zone utility → using {zone_names[0]} LBMP directly")
        result = lbmp_df.select(
            "timestamp",
            pl.col("lbmp_usd_per_mwh").alias("energy_cost_enduse"),
        ).sort("timestamp")
    else:
        # Multi-zone utility: load-weighted average
        print(f"  Multi-zone utility → load-weighting across {zone_names}")
        zone_loads = load_zone_loads(
            zone_loads_s3_base, zone_letters, year, storage_options
        )

        # Map zone load letters to LBMP zone names for joining
        zone_loads = zone_loads.with_columns(
            pl.col("zone")
            .replace_strict(letter_to_name, default=pl.col("zone"))
            .alias("lbmp_zone_name")
        )

        # Join LBMP prices with zone loads on (timestamp, zone)
        joined = lbmp_df.join(
            zone_loads.select("timestamp", "lbmp_zone_name", "load_mw"),
            left_on=["timestamp", "zone"],
            right_on=["timestamp", "lbmp_zone_name"],
            how="inner",
        )

        if joined.is_empty():
            raise ValueError(
                "No matching timestamps between LBMP and zone loads. "
                "Check that both datasets cover the same year."
            )

        # Compute load-weighted average LBMP per hour
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

    n_hours = result.height
    avg_lbmp = result["energy_cost_enduse"].mean()
    print(f"  Energy MC: {n_hours} hours, avg LBMP = ${avg_lbmp:.2f}/MWh")
    return result


# ── Capacity MC (ICAP MCOS) ──────────────────────────────────────────────────


def load_icap_spot_prices(
    icap_s3_base: str,
    localities: list[str],
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load ICAP Spot prices for the given localities and year.

    Args:
        icap_s3_base: S3 base path for ICAP data.
        localities: List of ICAP localities (e.g. ["NYCA"], ["NYC", "GHIJ"]).
        year: Target year.
        storage_options: AWS storage options.

    Returns:
        DataFrame with columns: month (int), locality, price_per_kw_month (float).
        Filtered to Spot auction type only.
    """
    icap_s3_base = icap_s3_base.rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            icap_s3_base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(
            pl.col("year") == year,
            pl.col("auction_type") == "Spot",
            pl.col("locality").is_in(localities),
        )
        .select("month", "locality", "price_per_kw_month")
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from ICAP collect()")
    # Cast month to Int32 for consistency (source is UInt8),
    # and locality to String (source is Categorical) for reliable joins.
    collected = collected.with_columns(
        pl.col("month").cast(pl.Int32),
        pl.col("locality").cast(pl.Utf8),
    )
    if collected.is_empty():
        raise FileNotFoundError(
            f"No ICAP Spot data found for localities={localities}, year={year} "
            f"under {icap_s3_base}"
        )

    locs_found = sorted(collected["locality"].unique().to_list())
    months_found = sorted(collected["month"].unique().to_list())
    print(
        f"Loaded ICAP Spot prices: {len(collected)} rows, "
        f"localities={locs_found}, months={months_found}"
    )
    return collected


def compute_weighted_icap_prices(
    icap_df: pl.DataFrame,
    utility_mapping: pl.DataFrame,
) -> pl.DataFrame:
    """Compute weighted ICAP price per month for a utility.

    For utilities with a single ICAP locality (capacity_weight=1.0), returns
    prices directly. For ConEd (split 87% NYC, 13% GHIJ), returns the
    weighted blend.

    Args:
        icap_df: ICAP Spot prices DataFrame (month, locality, price_per_kw_month).
        utility_mapping: Zone mapping rows for this utility.

    Returns:
        DataFrame with columns: month (int), icap_price_per_kw_month (float).
        One row per month.
    """
    # Get unique (icap_locality, capacity_weight) pairs
    locality_weights = utility_mapping.select(
        "icap_locality", "capacity_weight"
    ).unique()

    if len(locality_weights) == 1:
        # Single locality: use prices directly
        locality = locality_weights["icap_locality"][0]
        print(f"  Single ICAP locality: {locality}")
        result = icap_df.filter(pl.col("locality") == locality).select(
            "month",
            pl.col("price_per_kw_month").alias("icap_price_per_kw_month"),
        )
    else:
        # Multiple localities with weights (ConEd case)
        print(f"  Blending ICAP localities: {locality_weights}")
        # Join weights onto ICAP prices
        joined = icap_df.join(
            locality_weights,
            left_on="locality",
            right_on="icap_locality",
            how="inner",
        )
        # Weighted sum per month
        result = joined.group_by("month").agg(
            (pl.col("price_per_kw_month") * pl.col("capacity_weight"))
            .sum()
            .alias("icap_price_per_kw_month")
        )

    result = result.sort("month")

    if result.height != 12:
        months_found = sorted(result["month"].to_list())
        raise ValueError(
            f"Expected 12 months of ICAP prices, got {result.height}: {months_found}"
        )

    total_annual = result["icap_price_per_kw_month"].sum()
    print(f"  Annual ICAP total: ${total_annual:.2f}/kW-yr (sum of 12 monthly prices)")
    return result


def allocate_icap_to_hours(
    utility_load_df: pl.DataFrame,
    icap_prices: pl.DataFrame,
    n_peak_hours: int = N_PEAK_HOURS_PER_MONTH,
) -> pl.DataFrame:
    """Allocate monthly ICAP $/kW-month to hourly $/kW using threshold-exceedance.

    For each month:
    1. Sort hours by load (descending).
    2. Threshold = load of the (n_peak_hours+1)-th highest hour.
    3. Exceedance = max(load_h - threshold, 0).
    4. Weight_h = exceedance_h / sum(exceedance in month).
    5. capacity_cost_h = weight_h * icap_price_m.

    Args:
        utility_load_df: Utility load profile with columns: timestamp, load_mw.
        icap_prices: Monthly ICAP prices with columns: month, icap_price_per_kw_month.
        n_peak_hours: Number of peak hours per month for allocation (default: 8).

    Returns:
        DataFrame with columns: timestamp, capacity_cost_per_kw ($/kW per hour).
    """
    # Add month column
    load_with_month = utility_load_df.with_columns(
        pl.col("timestamp").dt.month().cast(pl.Int32).alias("month")
    )

    all_months: list[pl.DataFrame] = []

    for month_num in range(1, 13):
        # Filter to this month
        month_load = load_with_month.filter(pl.col("month") == month_num)
        if month_load.is_empty():
            raise ValueError(f"No load data for month {month_num}")

        # Get ICAP price for this month
        price_row = icap_prices.filter(pl.col("month") == month_num)
        if price_row.is_empty():
            raise ValueError(f"No ICAP price for month {month_num}")
        icap_price = float(price_row["icap_price_per_kw_month"][0])

        # Sort by load descending to find threshold
        sorted_load = month_load.sort("load_mw", descending=True)
        n_hours = sorted_load.height

        if n_hours <= n_peak_hours:
            raise ValueError(
                f"Month {month_num} has only {n_hours} hours, "
                f"need at least {n_peak_hours + 1} for threshold computation"
            )

        # Threshold = load of the (n_peak_hours+1)-th highest hour (0-indexed: n_peak_hours)
        threshold = float(sorted_load["load_mw"][n_peak_hours])

        # Compute exceedance for all hours
        month_result = month_load.with_columns(
            pl.when(pl.col("load_mw") > threshold)
            .then(pl.col("load_mw") - threshold)
            .otherwise(0.0)
            .alias("exceedance")
        )

        total_exceedance = month_result["exceedance"].sum()
        if total_exceedance <= 0:
            raise ValueError(
                f"Month {month_num}: total exceedance is zero or negative. "
                f"Threshold={threshold:.2f}, max load={sorted_load['load_mw'][0]:.2f}"
            )

        # Compute weights and allocate
        month_result = month_result.with_columns(
            (pl.col("exceedance") / total_exceedance * icap_price).alias(
                "capacity_cost_per_kw"
            )
        )

        # Verify weights sum to 1 within this month
        weight_sum = (month_result["exceedance"] / total_exceedance).sum()
        if abs(weight_sum - 1.0) > 1e-6:
            raise ValueError(
                f"Month {month_num}: weights sum to {weight_sum:.6f}, expected 1.0"
            )

        n_nonzero = month_result.filter(pl.col("capacity_cost_per_kw") > 0).height
        print(
            f"  Month {month_num:2d}: ICAP=${icap_price:6.2f}/kW-mo, "
            f"threshold={threshold:,.1f} MW, "
            f"{n_nonzero} peak hours, "
            f"total exceedance={total_exceedance:,.1f} MW"
        )

        all_months.append(month_result.select("timestamp", "capacity_cost_per_kw"))

    result = pl.concat(all_months).sort("timestamp")
    return result


def validate_capacity_allocation(
    capacity_df: pl.DataFrame,
    icap_prices: pl.DataFrame,
) -> None:
    """Validate that 1 kW constant load recovers the sum of 12 monthly ICAP prices.

    Args:
        capacity_df: Hourly capacity costs (timestamp, capacity_cost_per_kw).
        icap_prices: Monthly ICAP prices (month, icap_price_per_kw_month).

    Raises:
        ValueError: If the total diverges from expected by more than 0.01%.
    """
    expected_annual = float(icap_prices["icap_price_per_kw_month"].sum())
    actual_annual = float(capacity_df["capacity_cost_per_kw"].sum())

    error = abs(actual_annual - expected_annual)
    error_pct = (error / expected_annual * 100) if expected_annual > 0 else 0.0

    print("\n" + "=" * 60)
    print("VALIDATION: 1 kW Constant Load → ICAP Recovery")
    print("=" * 60)
    print(f"  Expected (sum of 12 ICAP Spot prices): ${expected_annual:.4f}/kW-yr")
    print(f"  Actual (sum of hourly allocations):     ${actual_annual:.4f}/kW-yr")
    print(f"  Error: ${error:.6f} ({error_pct:.6f}%)")

    tolerance = 0.01  # 0.01%
    if error_pct > tolerance:
        print("  ✗ Validation FAILED")
        print("=" * 60)
        raise ValueError(
            f"ICAP validation failed: error {error_pct:.6f}% exceeds {tolerance}%. "
            f"Expected ${expected_annual:.4f}/kW-yr, got ${actual_annual:.4f}/kW-yr."
        )
    print("  ✓ Validation PASSED")
    print("=" * 60)


# ── Output assembly ──────────────────────────────────────────────────────────


def build_cairo_8760_timestamps(year: int) -> pl.DataFrame:
    """Build 8760 naive timestamps for a given year (Cairo-compatible).

    For leap years, drops Dec 31 to maintain 8760 rows (Cairo convention).

    Returns:
        DataFrame with a single column: timestamp (datetime, 8760 rows).
    """
    import calendar

    is_leap = calendar.isleap(year)
    start = datetime(year, 1, 1, 0, 0, 0)
    end = datetime(year, 12, 31, 23, 0, 0)
    timestamps: list[datetime] = []
    cur = start
    while cur <= end:
        timestamps.append(cur)
        cur += timedelta(hours=1)

    if is_leap:
        # Cairo convention: drop Dec 31 for leap years
        timestamps = [t for t in timestamps if not (t.month == 12 and t.day == 31)]

    if len(timestamps) != 8760:
        raise ValueError(
            f"Expected 8760 timestamps, got {len(timestamps)} for year {year}"
        )

    return pl.DataFrame({"timestamp": timestamps})


def assemble_output(
    energy_df: pl.DataFrame,
    capacity_df: pl.DataFrame,
    year: int,
) -> pl.DataFrame:
    """Assemble final output matching Cambium parquet schema.

    Joins energy and capacity MCs on timestamp, fills missing hours with 0,
    and ensures exactly 8760 rows.

    Output columns:
        - timestamp (datetime, naive EST)
        - energy_cost_enduse ($/MWh)
        - capacity_cost_enduse ($/MWh): capacity_cost_per_kw * 1000

    Args:
        energy_df: Energy MC (timestamp, energy_cost_enduse in $/MWh).
        capacity_df: Capacity MC (timestamp, capacity_cost_per_kw in $/kW).
        year: Target year.

    Returns:
        DataFrame with 8760 rows, Cambium-compatible schema.
    """
    # Build reference 8760 index
    ref_8760 = build_cairo_8760_timestamps(year)

    # Truncate both datasets' timestamps to hour to handle any sub-hour artifacts
    energy_hourly = energy_df.with_columns(
        pl.col("timestamp").dt.truncate("1h").alias("timestamp")
    )
    capacity_hourly = capacity_df.with_columns(
        pl.col("timestamp").dt.truncate("1h").alias("timestamp")
    )

    # Deduplicate energy (in case of DST overlaps) → take mean
    energy_hourly = (
        energy_hourly.group_by("timestamp")
        .agg(pl.col("energy_cost_enduse").mean())
        .sort("timestamp")
    )

    # Deduplicate capacity similarly
    capacity_hourly = (
        capacity_hourly.group_by("timestamp")
        .agg(pl.col("capacity_cost_per_kw").mean())
        .sort("timestamp")
    )

    # Join onto reference index
    output = ref_8760.join(energy_hourly, on="timestamp", how="left")
    output = output.join(capacity_hourly, on="timestamp", how="left")

    # Fill missing with 0 and convert capacity to $/MWh
    output = output.with_columns(
        pl.col("energy_cost_enduse").fill_null(0.0).alias("energy_cost_enduse"),
        (pl.col("capacity_cost_per_kw").fill_null(0.0) * 1000.0).alias(
            "capacity_cost_enduse"
        ),
    )

    output = output.select("timestamp", "energy_cost_enduse", "capacity_cost_enduse")

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


def save_output(
    output_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    """Write output parquet to S3 with Hive-style partitioning.

    Path: {output_s3_base}/utility={utility}/year={year}/data.parquet

    Args:
        output_df: Final 8760-row DataFrame.
        utility: Utility name.
        year: Target year.
        output_s3_base: S3 base path.
        storage_options: AWS storage options.
    """
    output_s3_base = output_s3_base.rstrip("/") + "/"

    # Add partition columns for write
    partitioned = output_df.with_columns(
        pl.lit(utility).alias("utility"),
        pl.lit(year).alias("year"),
    )

    partitioned.write_parquet(
        output_s3_base,
        partition_by=["utility", "year"],
        storage_options=storage_options,
    )

    output_path = f"{output_s3_base}utility={utility}/year={year}/data.parquet"
    print(f"\n✓ Saved to {output_path}")
    print(f"  Rows: {len(output_df):,}")
    print(f"  Columns: {', '.join(output_df.columns)}")


# ── CLI ───────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate utility-level supply marginal costs "
            "(energy LBMP + capacity ICAP MCOS)."
        )
    )
    parser.add_argument(
        "--utility",
        type=str,
        required=True,
        choices=sorted(VALID_UTILITIES),
        help="Utility short name (e.g. nyseg, coned, rge).",
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Target year for supply MC generation (e.g. 2025).",
    )
    parser.add_argument(
        "--load-year",
        type=int,
        default=None,
        help=(
            "Year of load profile to use for weighting/peak identification "
            "(defaults to --year). Use to apply one year's prices to another "
            "year's load shape."
        ),
    )
    parser.add_argument(
        "--zone-mapping-path",
        type=str,
        default=DEFAULT_ZONE_MAPPING_PATH,
        help=f"Path to zone mapping CSV (default: {DEFAULT_ZONE_MAPPING_PATH}).",
    )
    parser.add_argument(
        "--lbmp-s3-base",
        type=str,
        default=DEFAULT_LBMP_S3_BASE,
        help=f"S3 base for LBMP data (default: {DEFAULT_LBMP_S3_BASE}).",
    )
    parser.add_argument(
        "--icap-s3-base",
        type=str,
        default=DEFAULT_ICAP_S3_BASE,
        help=f"S3 base for ICAP data (default: {DEFAULT_ICAP_S3_BASE}).",
    )
    parser.add_argument(
        "--zone-loads-s3-base",
        type=str,
        default=DEFAULT_ZONE_LOADS_S3_BASE,
        help=f"S3 base for EIA zone loads (default: {DEFAULT_ZONE_LOADS_S3_BASE}).",
    )
    parser.add_argument(
        "--utility-loads-s3-base",
        type=str,
        default=DEFAULT_UTILITY_LOADS_S3_BASE,
        help=f"S3 base for EIA utility loads (default: {DEFAULT_UTILITY_LOADS_S3_BASE}).",
    )
    parser.add_argument(
        "--output-s3-base",
        type=str,
        default=DEFAULT_OUTPUT_S3_BASE,
        help=f"S3 base for output (default: {DEFAULT_OUTPUT_S3_BASE}).",
    )
    parser.add_argument(
        "--peak-hours",
        type=int,
        default=N_PEAK_HOURS_PER_MONTH,
        help=f"Peak hours per month for ICAP allocation (default: {N_PEAK_HOURS_PER_MONTH}).",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload results to S3 (default: inspect only).",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = _parse_args()
    load_dotenv()
    storage_options = get_aws_storage_options()

    utility = args.utility
    price_year = args.year
    load_year = args.load_year if args.load_year else price_year

    print("=" * 60)
    print("SUPPLY MARGINAL COST GENERATION")
    print("=" * 60)
    print(f"  Utility:       {utility}")
    print(f"  Price year:    {price_year} (LBMP + ICAP)")
    print(f"  Load year:     {load_year} (for weighting / peak ID)")
    print(f"  Peak hours:    {args.peak_hours}/month")
    print(f"  Upload to S3:  {'Yes' if args.upload else 'No (inspect only)'}")
    print("=" * 60)

    # ── 1. Load zone mapping ─────────────────────────────────────────────
    print("\n── Zone Mapping ──")
    mapping_df = load_zone_mapping(args.zone_mapping_path, storage_options)
    utility_mapping = get_utility_mapping(mapping_df, utility)
    print(utility_mapping)

    # ── 2. Energy MC (LBMP) ──────────────────────────────────────────────
    print("\n── Energy MC (LBMP) ──")
    energy_df = compute_energy_mc(
        utility_mapping,
        args.lbmp_s3_base,
        args.zone_loads_s3_base,
        price_year if price_year == load_year else load_year,
        storage_options,
    )

    # If price_year != load_year, we use load_year's zone loads for weighting
    # but price_year's LBMP prices. Reload LBMP for price_year.
    if price_year != load_year:
        print(f"\n  Note: Using {load_year} load shapes with {price_year} LBMP prices.")
        energy_df = compute_energy_mc(
            utility_mapping,
            args.lbmp_s3_base,
            args.zone_loads_s3_base,
            price_year,
            storage_options,
        )
        # For the output timestamps, we want load_year's timestamps
        # but price_year's LBMP prices. For now, use price_year as the
        # primary (LBMP and ICAP are both for price_year).

    # ── 3. Capacity MC (ICAP MCOS) ──────────────────────────────────────
    print("\n── Capacity MC (ICAP MCOS) ──")

    # Get ICAP localities and weights
    localities = sorted(utility_mapping["icap_locality"].unique().to_list())
    print(f"  ICAP localities: {localities}")

    # Load ICAP Spot prices
    icap_df = load_icap_spot_prices(
        args.icap_s3_base, localities, price_year, storage_options
    )

    # Compute weighted monthly prices
    icap_prices = compute_weighted_icap_prices(icap_df, utility_mapping)
    print(icap_prices)

    # Load utility load profile for peak identification
    print(f"\n  Loading utility load profile for {utility}, year {load_year}...")
    utility_load_df = load_utility_load_profile(
        args.utility_loads_s3_base,
        "nyiso",
        load_year,
        utility,
        storage_options,
    )
    utility_load_df = normalize_load_to_cairo_8760(utility_load_df, utility, load_year)

    # Allocate ICAP to hours
    print("\n  Allocating ICAP to hours (threshold-exceedance):")
    capacity_df = allocate_icap_to_hours(utility_load_df, icap_prices, args.peak_hours)

    # Validate capacity allocation
    validate_capacity_allocation(capacity_df, icap_prices)

    # ── 4. Assemble output ───────────────────────────────────────────────
    print("\n── Output Assembly ──")
    output = assemble_output(energy_df, capacity_df, price_year)

    # Display sample
    print("\n" + "=" * 60)
    print("SAMPLE: Top 10 hours by capacity cost")
    print("=" * 60)
    sample = output.sort("capacity_cost_enduse", descending=True).head(10)
    print(sample)

    print("\nSAMPLE: Top 10 hours by energy cost")
    print("=" * 60)
    sample = output.sort("energy_cost_enduse", descending=True).head(10)
    print(sample)

    # ── 5. Save ──────────────────────────────────────────────────────────
    if args.upload:
        save_output(output, utility, price_year, args.output_s3_base, storage_options)
        print("\n" + "=" * 60)
        print("✓ Supply marginal cost generation completed and uploaded")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("✓ Supply marginal cost generation completed (inspect only)")
        print("⚠️  No data uploaded to S3 (use --upload flag to enable)")
        print("=" * 60)


if __name__ == "__main__":
    main()
