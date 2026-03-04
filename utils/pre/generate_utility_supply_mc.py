"""Generate utility-level supply marginal costs (energy LBMP + capacity ICAP MCOS).

This script replaces Cambium-based supply marginal costs with real NYISO data:
- **Energy**: Real-time LBMP prices (5-minute intervals aggregated to hourly),
  load-weighted across the utility's LBMP zones.
- **Capacity**: ICAP Spot prices allocated to hours using threshold-exceedance
  weights (8 peak hours per month, analogous to Cambium § 6.8).

Input data:
    - LBMP real-time zonal prices (5-minute intervals):
        s3://data.sb/nyiso/lbmp/real_time/zones/zone={NAME}/year={YYYY}/month={MM}/data.parquet
    - ICAP spot prices (4 localities):
        s3://data.sb/nyiso/icap/year={YYYY}/month={M}/data.parquet
    - NYISO zone-level hourly loads (for LBMP load-weighting and ICAP peak identification):
        s3://data.sb/nyiso/hourly_demand/zones/zone={NAME}/year={YYYY}/month={M}/data.parquet
    - Utility zone mapping CSV (utility → LBMP zone, ICAP locality, capacity_weight):
        s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv

Output (two separate files):
    Energy: s3://data.sb/switchbox/marginal_costs/ny/supply/energy/utility={utility}/year={YYYY}/data.parquet
        Schema: timestamp (datetime), energy_cost_enduse ($/MWh), 8760 rows
    Capacity: s3://data.sb/switchbox/marginal_costs/ny/supply/capacity/utility={utility}/year={YYYY}/data.parquet
        Schema: timestamp (datetime), capacity_cost_enduse ($/MWh), 8760 rows

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
from utils.pre.generate_utility_tx_dx_mc import normalize_load_to_cairo_8760

# ── Default S3 paths ─────────────────────────────────────────────────────────

DEFAULT_LBMP_S3_BASE = "s3://data.sb/nyiso/lbmp/real_time/zones/"
DEFAULT_ICAP_S3_BASE = "s3://data.sb/nyiso/icap/"
DEFAULT_ZONE_LOADS_S3_BASE = "s3://data.sb/nyiso/hourly_demand/zones/"
DEFAULT_ZONE_MAPPING_PATH = (
    "s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv"
)
DEFAULT_OUTPUT_S3_BASE = "s3://data.sb/switchbox/marginal_costs/ny/supply/"

# Number of peak hours per month for ICAP MCOS allocation
N_PEAK_HOURS_PER_MONTH = 8

# Valid NY utilities
VALID_UTILITIES = frozenset({"cenhud", "coned", "nimo", "nyseg", "or", "rge", "psegli"})

# Nested locality footprints used for capacity-peak load profiles.
# These are overlapping by design (NYCA ⊃ LHV ⊃ NYC, and LI=K).
# Keyed by canonical NYISO zone names (matching s3://data.sb/nyiso/hourly_demand/zones/).
NESTED_LOCALITY_ZONES: dict[str, list[str]] = {
    "NYCA": [
        "WEST",
        "GENESE",
        "CENTRAL",
        "NORTH",
        "MHK_VL",
        "CAPITL",
        "HUD_VL",
        "MILLWD",
        "DUNWOD",
        "N.Y.C.",
        "LONGIL",
    ],
    "LHV": ["HUD_VL", "MILLWD", "DUNWOD", "N.Y.C."],
    "NYC": ["N.Y.C."],
    "LI": ["LONGIL"],
}

# Raw ICAP locality names in the source dataset mapped to internal nested names.
ICAP_RAW_TO_NESTED_LOCALITY = {
    "NYCA": "NYCA",
    "GHIJ": "LHV",
    "NYC": "NYC",
    "LI": "LI",
}
NESTED_LOCALITY_TO_ICAP_RAW = {
    nested: raw for raw, nested in ICAP_RAW_TO_NESTED_LOCALITY.items()
}

# Partitioned (non-overlapping) localities used when applying utility splits to prices.
GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY = {
    "ROS": "NYCA",  # A-F
    "LHV": "LHV",  # G-I
    "NYC": "NYC",  # J
    "LI": "LI",  # K
}

VALID_NESTED_LOCALITIES = frozenset(NESTED_LOCALITY_ZONES)
VALID_PARTITIONED_LOCALITIES = frozenset(NESTED_LOCALITY_TO_ICAP_RAW)


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


def aggregate_lbmp_to_hourly(lbmp_df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate 5-minute real-time LBMP intervals to hourly averages.

    Real-time LBMP data contains 5-minute intervals. This function aggregates
    them to hourly by taking the mean of all 5-minute intervals within each hour.

    Args:
        lbmp_df: DataFrame with 5-minute LBMP data (timestamp, zone, lbmp_usd_per_mwh).

    Returns:
        DataFrame with hourly LBMP data (timestamp truncated to hour, zone, lbmp_usd_per_mwh).
    """
    # Truncate timestamps to hour boundaries
    lbmp_hourly = lbmp_df.with_columns(
        pl.col("timestamp").dt.truncate("1h").alias("timestamp")
    )

    # Group by (hourly timestamp, zone) and take mean of LBMP prices
    aggregated = (
        lbmp_hourly.group_by("timestamp", "zone")
        .agg(pl.col("lbmp_usd_per_mwh").mean().alias("lbmp_usd_per_mwh"))
        .sort("timestamp", "zone")
    )

    n_5min = lbmp_df.height
    n_hourly = aggregated.height
    print(f"  Aggregated {n_5min:,} 5-minute intervals → {n_hourly:,} hourly averages")
    return aggregated


def load_lbmp_for_zones(
    lbmp_s3_base: str,
    zone_names: list[str],
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load real-time LBMP data for the given zones and year.

    Real-time LBMP data contains 5-minute intervals. This function loads the raw
    5-minute data; aggregation to hourly is handled separately.

    Args:
        lbmp_s3_base: S3 base path for LBMP data (e.g. s3://data.sb/nyiso/lbmp/real_time/zones/).
        zone_names: List of LBMP zone names (e.g. ["WEST", "GENESE"]).
        year: Target year.
        storage_options: AWS storage options.

    Returns:
        DataFrame with columns: timestamp (naive), zone, lbmp_usd_per_mwh.
        Timestamps are at 5-minute intervals.
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
    zone_names: list[str],
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load NYISO zone-level hourly loads.

    Args:
        zone_loads_s3_base: S3 base path for zone loads
            (e.g. s3://data.sb/nyiso/hourly_demand/zones/).
        zone_names: Canonical NYISO zone names (e.g. ["WEST", "GENESE"]).
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
            pl.col("zone").is_in(zone_names),
            pl.col("year") == year,
        )
        .select("timestamp", "zone", "load_mw")
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from zone loads collect()")
    if collected.is_empty():
        raise FileNotFoundError(
            f"No zone load data found for zones={zone_names}, year={year} "
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
    zone_load_year: int | None = None,
) -> pl.DataFrame:
    """Compute hourly energy marginal costs from real-time LBMP data.

    Real-time LBMP data (5-minute intervals) is aggregated to hourly averages,
    then used to compute energy marginal costs.

    For single-zone utilities, returns the zone's hourly LBMP directly.
    For multi-zone utilities, computes load-weighted average across zones.

    Args:
        utility_mapping: Zone mapping rows for this utility.
        lbmp_s3_base: S3 base for real-time LBMP data.
        zone_loads_s3_base: S3 base for zone loads.
        year: LBMP price year (also determines output timestamps).
        storage_options: AWS storage options.
        zone_load_year: Year to use for zone loads (for multi-zone weighting).
            Defaults to ``year``. When different from ``year``, load timestamps
            are remapped to ``year`` before joining with LBMP.

    Returns:
        DataFrame with columns: timestamp, energy_cost_enduse ($/MWh).
        Timestamps are hourly.
    """
    if zone_load_year is None:
        zone_load_year = year

    # Get unique LBMP zone names for this utility
    zone_names = sorted(utility_mapping["lbmp_zone_name"].unique().to_list())

    # Load LBMP data for all relevant zones (5-minute intervals)
    lbmp_df = load_lbmp_for_zones(lbmp_s3_base, zone_names, year, storage_options)

    # Aggregate 5-minute intervals to hourly averages
    lbmp_df = aggregate_lbmp_to_hourly(lbmp_df)

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
            zone_loads_s3_base, zone_names, zone_load_year, storage_options
        )

        if zone_load_year != year:
            offset = f"{year - zone_load_year}y"
            print(f"  Remapping zone load timestamps: {zone_load_year} → {year}")
            zone_loads = zone_loads.with_columns(
                pl.col("timestamp").dt.offset_by(offset)
            )

        # Join LBMP prices with zone loads on (timestamp, zone name)
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
    """Load ICAP Spot prices for the given partitioned localities and year.

    Args:
        icap_s3_base: S3 base path for ICAP data.
        localities: Partitioned localities (subset of NYCA, LHV, NYC, LI).
        year: Target year.
        storage_options: AWS storage options.

    Returns:
        DataFrame with columns: month (int), locality, price_per_kw_month (float),
        where locality is in partitioned names (NYCA, LHV, NYC, LI).
    """
    raw_localities = sorted({NESTED_LOCALITY_TO_ICAP_RAW[loc] for loc in localities})
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
            pl.col("locality").is_in(raw_localities),
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
        pl.col("locality")
        .cast(pl.Utf8)
        .replace_strict(ICAP_RAW_TO_NESTED_LOCALITY)
        .alias("locality"),
    )
    if collected.is_empty():
        raise FileNotFoundError(
            f"No ICAP Spot data found for localities={raw_localities}, year={year} "
            f"under {icap_s3_base}"
        )

    locs_found = sorted(collected["locality"].unique().to_list())
    months_found = sorted(collected["month"].unique().to_list())
    print(
        f"Loaded ICAP Spot prices: {len(collected)} rows, "
        f"localities={locs_found}, months={months_found}"
    )
    return collected


def _resolve_locality_weights(
    utility_mapping: pl.DataFrame,
    source_col: str,
    source_to_locality: dict[str, str] | None,
    valid_localities: frozenset[str],
    purpose: str,
) -> pl.DataFrame:
    """Resolve and validate utility-level locality weights."""
    locality_expr = pl.col(source_col).cast(pl.Utf8)
    if source_to_locality:
        locality_expr = locality_expr.replace_strict(source_to_locality)

    locality_weights = (
        utility_mapping.select(source_col, "capacity_weight")
        .unique()
        .with_columns(locality_expr.alias("locality"))
        .group_by("locality")
        .agg(pl.col("capacity_weight").sum().alias("capacity_weight"))
        .sort("locality")
    )
    if locality_weights.is_empty():
        raise ValueError(f"No locality weights found for {purpose}")

    invalid = sorted(
        set(locality_weights["locality"].to_list()) - set(valid_localities)
    )
    if invalid:
        raise ValueError(
            f"Unknown localities for {purpose}: {invalid}. "
            f"Expected {sorted(valid_localities)}."
        )
    return locality_weights


def get_partitioned_price_locality_weights(
    utility_mapping: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve partitioned locality weights for ICAP price blending.

    This maps utility `gen_capacity_zone` (ROS/LHV/NYC/LI) to partitioned
    ICAP price localities (NYCA/LHV/NYC/LI).
    """
    return _resolve_locality_weights(
        utility_mapping=utility_mapping,
        source_col="gen_capacity_zone",
        source_to_locality=GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY,
        valid_localities=VALID_PARTITIONED_LOCALITIES,
        purpose="partitioned ICAP pricing",
    )


def compute_weighted_icap_prices(
    icap_df: pl.DataFrame,
    locality_weights: pl.DataFrame,
) -> pl.DataFrame:
    """Compute weighted ICAP price per month for a utility.

    Locality weights should already be in partitioned terms (NYCA/LHV/NYC/LI)
    with split weights applied.

    Args:
        icap_df: ICAP Spot prices DataFrame (month, locality, price_per_kw_month),
            with locality in partitioned terms.
        locality_weights: DataFrame with columns locality, capacity_weight.

    Returns:
        DataFrame with columns: month (int), icap_price_per_kw_month (float).
        One row per month.
    """
    print(f"  ICAP locality weights: {locality_weights.to_dicts()}")
    joined = icap_df.join(
        locality_weights,
        on="locality",
        how="inner",
    )
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


def build_locality_load_profiles(
    icap_locality_names: list[str],
    zone_loads_df: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Build raw load profile per nested locality from NYISO zone loads.

    Each locality's profile is the unweighted sum of its constituent zone loads.
    No ``capacity_weight`` is applied here; weights are only used later when
    scaling ICAP prices in ``compute_capacity_mc_components``.

    Args:
        icap_locality_names: Raw ICAP locality names from the zone mapping
            (e.g. ``["GHIJ", "NYC"]``).  Duplicates are silently de-duplicated.
        zone_loads_df: Zone hourly loads with columns: timestamp, zone, load_mw.

    Returns:
        Dictionary keyed by nested locality name (e.g. ``"LHV"``, ``"NYC"``) to
        a DataFrame with columns: timestamp, load_mw (raw unweighted MW sum).
    """
    profiles: dict[str, pl.DataFrame] = {}
    for raw_name in icap_locality_names:
        if raw_name not in ICAP_RAW_TO_NESTED_LOCALITY:
            raise ValueError(
                f"Unknown ICAP locality name: {raw_name!r}. "
                f"Expected one of {sorted(ICAP_RAW_TO_NESTED_LOCALITY)}."
            )
        nested = ICAP_RAW_TO_NESTED_LOCALITY[raw_name]
        if nested in profiles:
            # Already computed (two rows mapping to the same nested locality).
            continue
        zone_names = NESTED_LOCALITY_ZONES[nested]
        profile = (
            zone_loads_df.filter(pl.col("zone").is_in(zone_names))
            .group_by("timestamp")
            .agg(pl.col("load_mw").sum().alias("load_mw"))
            .sort("timestamp")
        )
        if profile.is_empty():
            raise ValueError(
                f"No zone load rows found for nested locality={nested!r} "
                f"(zones={zone_names}). Raw ICAP name: {raw_name!r}."
            )
        profiles[nested] = profile
    return profiles


def compute_capacity_mc_components(
    utility_icap_rows: pl.DataFrame,
    icap_df: pl.DataFrame,
    locality_profiles: dict[str, pl.DataFrame],
    n_peak_hours: int = N_PEAK_HOURS_PER_MONTH,
) -> pl.DataFrame:
    """Compute capacity MC by summing per-locality components.

    For each ``(icap_locality, gen_capacity_zone, capacity_weight)`` row:

    1. Map ``icap_locality`` (raw ICAP name) → nested locality → load profile
       for independent peak identification.
    2. Map ``gen_capacity_zone`` → partitioned locality → filter ``icap_df``
       → scale ``price_per_kw_month`` by ``capacity_weight``.
    3. Allocate scaled ICAP price to the locality's own peak hours using
       threshold-exceedance weights (via :func:`allocate_icap_to_hours`).
    4. Sum all per-locality cost contributions.

    This is analogous to ``compute_utility_bulk_tx_signal`` in
    ``generate_bulk_tx_mc.py``:  each NYISO ICAP locality identifies its own
    peak hours independently; the ``capacity_weight`` only scales the *cost*,
    not the load used for peak identification.

    Args:
        utility_icap_rows: DataFrame with columns: icap_locality (raw ICAP
            name), gen_capacity_zone (partitioned locality), capacity_weight.
            These are the zone-mapping rows filtered to a single utility.
        icap_df: ICAP Spot prices DataFrame with columns: month (int),
            locality (nested/partitioned name: NYCA/LHV/NYC/LI),
            price_per_kw_month (float).
        locality_profiles: Dictionary mapping nested locality name to a
            normalized load profile DataFrame (timestamp, load_mw).
        n_peak_hours: Number of peak hours per month for allocation (default: 8).

    Returns:
        DataFrame with columns: timestamp, capacity_cost_per_kw ($/kW per hour).
        Non-zero hours equal the union of all localities' peak hours (up to
        ``n_peak_hours × 12 × n_localities`` distinct hours).
    """
    component_frames: list[pl.DataFrame] = []

    for row in utility_icap_rows.iter_rows(named=True):
        icap_locality_raw = str(row["icap_locality"])
        gen_capacity_zone = str(row["gen_capacity_zone"])
        capacity_weight = float(row["capacity_weight"])

        # Map raw ICAP locality → nested locality (for load profile / peak ID)
        if icap_locality_raw not in ICAP_RAW_TO_NESTED_LOCALITY:
            raise ValueError(
                f"Unknown icap_locality {icap_locality_raw!r}. "
                f"Expected one of {sorted(ICAP_RAW_TO_NESTED_LOCALITY)}."
            )
        nested_locality = ICAP_RAW_TO_NESTED_LOCALITY[icap_locality_raw]

        # Map gen_capacity_zone → partitioned locality (for ICAP price lookup)
        if gen_capacity_zone not in GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY:
            raise ValueError(
                f"Unknown gen_capacity_zone {gen_capacity_zone!r}. "
                f"Expected one of {sorted(GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY)}."
            )
        partitioned_locality = GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY[
            gen_capacity_zone
        ]

        # Get load profile for this nested locality
        if nested_locality not in locality_profiles:
            raise ValueError(
                f"No load profile for nested locality {nested_locality!r}. "
                f"Available: {sorted(locality_profiles)}"
            )
        load_profile = locality_profiles[nested_locality]

        # Filter ICAP prices to this component's partitioned locality and scale by weight
        component_icap = icap_df.filter(pl.col("locality") == partitioned_locality)
        if component_icap.is_empty():
            raise ValueError(
                f"No ICAP prices found for partitioned locality {partitioned_locality!r}. "
                f"Available: {sorted(icap_df['locality'].unique().to_list())}"
            )
        component_prices = component_icap.select(
            pl.col("month"),
            (pl.col("price_per_kw_month") * capacity_weight).alias(
                "icap_price_per_kw_month"
            ),
        )

        print(
            f"  Component: icap_locality={icap_locality_raw!r} → "
            f"nested={nested_locality!r}, "
            f"gen_capacity_zone={gen_capacity_zone!r} → "
            f"partitioned={partitioned_locality!r}, "
            f"weight={capacity_weight:.4f}"
        )

        # Allocate this component's scaled ICAP prices to the locality's peak hours
        component_hourly = allocate_icap_to_hours(
            load_profile, component_prices, n_peak_hours
        )
        component_frames.append(component_hourly)

    if not component_frames:
        raise ValueError("No ICAP locality components found for utility")

    # Sum all component costs; non-zero hours = union of all localities' peak hours
    utility_hourly = (
        pl.concat(component_frames)
        .group_by("timestamp")
        .agg(pl.col("capacity_cost_per_kw").sum().alias("capacity_cost_per_kw"))
        .sort("timestamp")
    )
    return utility_hourly


def _zone_names_for_localities(
    localities: list[str], locality_zone_map: dict[str, list[str]]
) -> list[str]:
    """Return sorted unique zone names covered by the given localities."""
    return sorted({z for loc in localities for z in locality_zone_map[loc]})


def allocate_icap_to_hours(
    utility_load_df: pl.DataFrame,
    icap_prices: pl.DataFrame,
    n_peak_hours: int = N_PEAK_HOURS_PER_MONTH,
) -> pl.DataFrame:
    """Allocate monthly ICAP $/kW-month to hourly $/kW using threshold-exceedance.

    For each month:
    1. Sort hours by load (descending); take the top N.
    2. Threshold = max(load_mw where load_mw < load of the Nth hour).
       This avoids ties: when the Nth and (N+1)th hours have equal load,
       the threshold drops to the next genuinely lower value so the Nth
       hour always gets positive exceedance.
    3. Exceedance_h = load_h - threshold (for top-N hours only).
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

        if n_hours < n_peak_hours:
            raise ValueError(
                f"Month {month_num} has only {n_hours} hours, "
                f"need at least {n_peak_hours} for threshold computation"
            )

        top_n = sorted_load.head(n_peak_hours)
        load_nth = float(top_n["load_mw"][-1])

        # Threshold = max load strictly below the Nth highest hour.
        # When the Nth and (N+1)th hours tie, this drops to the next genuinely
        # lower value so the Nth hour always gets positive exceedance.
        below = month_load.filter(pl.col("load_mw") < load_nth)["load_mw"]
        threshold = float(below.max()) if not below.is_empty() else 0.0  # type: ignore[arg-type]

        month_result = top_n.with_columns(
            (pl.col("load_mw") - threshold).alias("exceedance")
        )

        total_exceedance = float(month_result["exceedance"].sum())
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


def prepare_energy_output(
    energy_df: pl.DataFrame,
    year: int,
) -> pl.DataFrame:
    """Prepare energy MC DataFrame for saving (8760 rows, timestamp + energy_cost_enduse).

    Args:
        energy_df: Energy MC (timestamp, energy_cost_enduse in $/MWh).
        year: Target year.

    Returns:
        DataFrame with 8760 rows: timestamp, energy_cost_enduse ($/MWh).
    """
    # Build reference 8760 index
    ref_8760 = build_cairo_8760_timestamps(year)

    # Truncate timestamps to hour
    energy_hourly = energy_df.with_columns(
        pl.col("timestamp").dt.truncate("1h").alias("timestamp")
    )

    # Deduplicate (in case of DST overlaps) → take mean
    energy_hourly = (
        energy_hourly.group_by("timestamp")
        .agg(pl.col("energy_cost_enduse").mean())
        .sort("timestamp")
    )

    # Join onto reference index
    output = ref_8760.join(energy_hourly, on="timestamp", how="left")

    # Fill missing with 0
    output = output.with_columns(
        pl.col("energy_cost_enduse").fill_null(0.0).alias("energy_cost_enduse"),
    )

    output = output.select("timestamp", "energy_cost_enduse")

    if output.height != 8760:
        raise ValueError(f"Energy output has {output.height} rows, expected 8760")

    if output.filter(pl.col("energy_cost_enduse").is_null()).height > 0:
        raise ValueError("Energy output has nulls")

    return output


def prepare_capacity_output(
    capacity_df: pl.DataFrame,
    year: int,
) -> pl.DataFrame:
    """Prepare capacity MC DataFrame for saving (8760 rows, timestamp + capacity_cost_enduse).

    Args:
        capacity_df: Capacity MC (timestamp, capacity_cost_per_kw in $/kW).
        year: Target year.

    Returns:
        DataFrame with 8760 rows: timestamp, capacity_cost_enduse ($/MWh).
    """
    # Build reference 8760 index
    ref_8760 = build_cairo_8760_timestamps(year)

    # Truncate timestamps to hour
    capacity_hourly = capacity_df.with_columns(
        pl.col("timestamp").dt.truncate("1h").alias("timestamp")
    )

    # Deduplicate (in case of DST overlaps) → take mean
    capacity_hourly = (
        capacity_hourly.group_by("timestamp")
        .agg(pl.col("capacity_cost_per_kw").mean())
        .sort("timestamp")
    )

    # Join onto reference index
    output = ref_8760.join(capacity_hourly, on="timestamp", how="left")

    # Fill missing with 0 and convert to $/MWh
    output = output.with_columns(
        (pl.col("capacity_cost_per_kw").fill_null(0.0) * 1000.0).alias(
            "capacity_cost_enduse"
        ),
    )

    output = output.select("timestamp", "capacity_cost_enduse")

    if output.height != 8760:
        raise ValueError(f"Capacity output has {output.height} rows, expected 8760")

    if output.filter(pl.col("capacity_cost_enduse").is_null()).height > 0:
        raise ValueError("Capacity output has nulls")

    return output


def save_energy_output(
    energy_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    """Write energy MC parquet to S3 with Hive-style partitioning.

    Path: {output_s3_base}/energy/utility={utility}/year={year}/data.parquet

    Args:
        energy_df: Energy MC DataFrame (timestamp, energy_cost_enduse).
        utility: Utility name.
        year: Target year.
        output_s3_base: S3 base path.
        storage_options: AWS storage options.
    """
    output_s3_base = output_s3_base.rstrip("/") + "/"
    energy_base = output_s3_base + "energy/"

    # Add partition columns for write
    partitioned = energy_df.with_columns(
        pl.lit(utility).alias("utility"),
        pl.lit(year).alias("year"),
    )

    partitioned.write_parquet(
        energy_base,
        partition_by=["utility", "year"],
        storage_options=storage_options,
    )

    output_path = f"{energy_base}utility={utility}/year={year}/data.parquet"
    print(f"\n✓ Saved energy MC to {output_path}")
    print(f"  Rows: {len(energy_df):,}")
    print(f"  Columns: {', '.join(energy_df.columns)}")


def save_capacity_output(
    capacity_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    """Write capacity MC parquet to S3 with Hive-style partitioning.

    Path: {output_s3_base}/capacity/utility={utility}/year={year}/data.parquet

    Args:
        capacity_df: Capacity MC DataFrame (timestamp, capacity_cost_enduse).
        utility: Utility name.
        year: Target year.
        output_s3_base: S3 base path.
        storage_options: AWS storage options.
    """
    output_s3_base = output_s3_base.rstrip("/") + "/"
    capacity_base = output_s3_base + "capacity/"

    # Add partition columns for write
    partitioned = capacity_df.with_columns(
        pl.lit(utility).alias("utility"),
        pl.lit(year).alias("year"),
    )

    partitioned.write_parquet(
        capacity_base,
        partition_by=["utility", "year"],
        storage_options=storage_options,
    )

    output_path = f"{capacity_base}utility={utility}/year={year}/data.parquet"
    print(f"\n✓ Saved capacity MC to {output_path}")
    print(f"  Rows: {len(capacity_df):,}")
    print(f"  Columns: {', '.join(capacity_df.columns)}")


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
        help=f"S3 base for NYISO zone loads (default: {DEFAULT_ZONE_LOADS_S3_BASE}).",
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
        price_year,
        storage_options,
        zone_load_year=load_year if load_year != price_year else None,
    )

    # ── 3. Capacity MC (ICAP MCOS) ──────────────────────────────────────
    print("\n── Capacity MC (ICAP MCOS) ──")

    # Get unique (icap_locality, gen_capacity_zone, capacity_weight) rows for this utility
    utility_icap_rows = utility_mapping.select(
        "icap_locality", "gen_capacity_zone", "capacity_weight"
    ).unique()
    print(f"  Utility ICAP rows:\n{utility_icap_rows}")

    # Determine all partitioned localities needed (for ICAP price loading)
    partitioned_localities = sorted(
        {
            GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY[z]
            for z in utility_icap_rows["gen_capacity_zone"].to_list()
        }
    )
    print(f"  Loading ICAP prices for partitioned localities: {partitioned_localities}")

    # Load ICAP Spot prices (unblended, one row per locality per month)
    icap_df = load_icap_spot_prices(
        args.icap_s3_base, partitioned_localities, price_year, storage_options
    )

    # Determine all nested localities needed (for zone load loading)
    icap_locality_names = utility_icap_rows["icap_locality"].to_list()
    nested_localities = sorted(
        {ICAP_RAW_TO_NESTED_LOCALITY[raw] for raw in icap_locality_names}
    )
    zones_needed = _zone_names_for_localities(nested_localities, NESTED_LOCALITY_ZONES)
    print(
        f"\n  Building locality load profiles for year {load_year}..."
        f"\n  Nested localities: {nested_localities}"
        f"\n  Zones needed: {zones_needed}"
    )

    # Load zone loads and build raw per-locality profiles (unweighted MW sums)
    zone_loads_df = load_zone_loads(
        args.zone_loads_s3_base, zones_needed, load_year, storage_options
    )
    raw_profiles = build_locality_load_profiles(icap_locality_names, zone_loads_df)

    # Normalize each locality profile to Cairo-compatible 8760 hours
    locality_profiles: dict[str, pl.DataFrame] = {}
    for loc, profile in raw_profiles.items():
        locality_profiles[loc] = normalize_load_to_cairo_8760(
            profile, utility, load_year
        )

    # Compute capacity MC component-by-component (each locality picks its own peaks)
    print("\n  Computing capacity MC (component-by-component):")
    capacity_df = compute_capacity_mc_components(
        utility_icap_rows, icap_df, locality_profiles, args.peak_hours
    )

    # Validate: expected annual total = Σ_locality(weight × Σ_month(price))
    price_locality_weights = get_partitioned_price_locality_weights(utility_mapping)
    icap_prices_for_validation = compute_weighted_icap_prices(
        icap_df, price_locality_weights
    )
    validate_capacity_allocation(capacity_df, icap_prices_for_validation)

    # Remap capacity timestamps from load_year → price_year when they differ.
    # offset_by("Ny") shifts by N calendar years on naive datetimes; safe because
    # Cairo 8760 normalization already handled leap years and DST.
    if load_year != price_year:
        print(f"\n  Remapping capacity timestamps: {load_year} → {price_year}")
        capacity_df = capacity_df.with_columns(
            pl.col("timestamp").dt.offset_by(f"{price_year - load_year}y")
        )

    # ── 4. Prepare separate outputs ──────────────────────────────────────
    print("\n── Output Preparation ──")
    energy_output = prepare_energy_output(energy_df, price_year)
    capacity_output = prepare_capacity_output(capacity_df, price_year)

    # Display samples
    print("\n" + "=" * 60)
    print("SAMPLE: Top 10 hours by capacity cost")
    print("=" * 60)
    sample = capacity_output.sort("capacity_cost_enduse", descending=True).head(10)
    print(sample)

    print("\nSAMPLE: Top 10 hours by energy cost")
    print("=" * 60)
    sample = energy_output.sort("energy_cost_enduse", descending=True).head(10)
    print(sample)

    # ── 5. Save ──────────────────────────────────────────────────────────
    if args.upload:
        save_energy_output(
            energy_output, utility, price_year, args.output_s3_base, storage_options
        )
        save_capacity_output(
            capacity_output, utility, price_year, args.output_s3_base, storage_options
        )
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
