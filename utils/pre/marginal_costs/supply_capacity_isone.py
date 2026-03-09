"""Capacity (FCA) marginal cost computation for ISO-NE (RI) utility supply MCs.

Methodology
-----------
Convert FCA clearing price ($/kW-month) into an hourly capacity cost adder
($/kW per hour) using annual threshold-exceedance weighting on the top-N system
peak-load hours, then scale to $/MWh via prepare_component_output(scale=1000).

Key design decisions:
- No CSO multiplication — this is a per-kW marginal cost allocation, not total
  supplier revenue.
- SENE aggregate load (RI + SEMA zones summed) for peak-hour identification,
  matching the capacity zone where FCA prices clear.
- Annual exceedance allocation (not monthly like NYISO ICAP) — the FCA locks in
  a single price per CCP, so we allocate the full annualized cost across top-N
  calendar-year hours.
- Two-CCP blending for calendar years that span the June–May boundary: sum
  FCA_price * months_in_calendar_year across both overlapping CCPs to get
  capacity_cost_kw_year.

Unit chain:
  $/kW-month
  --[sum 12 months split across two CCPs]--> $/kW-year
  --[exceedance allocation]--> $/kW per peak hour
  --[x1000 via prepare_component_output]--> $/MWh = capacity_cost_enduse
"""

from __future__ import annotations

from datetime import date

import polars as pl

from utils.pre.marginal_costs.supply_utils import (
    DEFAULT_ISONE_FCA_S3_PATH,
    DEFAULT_ISONE_ZONE_LOADS_S3_BASE,
    ISONE_CAPACITY_ZONE_FALLBACK,
    ISONE_CAPACITY_ZONE_LOAD_ZONES,
    ISONE_UTILITY_CAPACITY_ZONES,
    build_cairo_8760_timestamps,
    strip_tz_if_needed,
)

# Number of top system-peak hours in a calendar year over which FCA cost is spread.
# ISO-NE allocates capacity to ~100 highest-demand hours (annual, not monthly).
N_PEAK_HOURS_ANNUAL = 100


# ---------------------------------------------------------------------------
# FCA data loading
# ---------------------------------------------------------------------------


def load_fca_prices(
    fca_s3_path: str,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load ISO-NE FCA clearing prices from parquet, filtered to resource_status='all'.

    The parquet schema mirrors the curated fca_clearing_prices.csv:
        fca_number              Int64
        ccp_start               Date
        ccp_end                 Date
        capacity_zone_id        Int64
        capacity_zone_name      String
        resource_status         String  ('all' | 'existing' | 'new')
        clearing_price_per_kw_month  Float64
        notes                   String

    For FCAs with administrative pricing (Insufficient Competition or Inadequate
    Supply), zones have 'existing' and 'new' rows instead of 'all'. Filtering to
    'all' drops those special cases; callers must handle them separately if needed.

    Args:
        fca_s3_path: S3 path to the FCA parquet file.
        storage_options: AWS storage options for S3 access.

    Returns:
        DataFrame with resource_status == 'all' rows only.
    """
    df = pl.read_parquet(fca_s3_path, storage_options=storage_options)

    required = {
        "fca_number",
        "ccp_start",
        "ccp_end",
        "capacity_zone_id",
        "clearing_price_per_kw_month",
        "resource_status",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"FCA parquet missing columns: {sorted(missing)}")

    filtered = df.filter(pl.col("resource_status") == "all")
    n_all = filtered.height
    n_total = df.height
    print(
        f"Loaded FCA prices: {n_all} 'all' rows (of {n_total} total) from {fca_s3_path}"
    )
    return filtered


# ---------------------------------------------------------------------------
# FCA price resolution for a calendar year
# ---------------------------------------------------------------------------


def _get_fca_price_for_ccp(
    fca_df: pl.DataFrame,
    ccp_start_year: int,
    primary_zone_id: int,
    fallback_zone_id: int,
) -> tuple[float, int]:
    """Return (price_per_kw_month, zone_id_used) for the CCP starting in ccp_start_year.

    Tries primary_zone_id first; falls back to fallback_zone_id if SENE-specific
    price is absent (e.g. FCA 17–18 where all zones cleared at the same RoP price).

    Args:
        fca_df: DataFrame already filtered to resource_status='all'.
        ccp_start_year: Year of June 1 start of the CCP.
        primary_zone_id: Preferred capacity zone ID (e.g. 8506 for SENE).
        fallback_zone_id: Fallback zone ID (e.g. 8500 for System/Rest-of-Pool).

    Returns:
        Tuple of (clearing_price_per_kw_month, actual_zone_id_used).

    Raises:
        ValueError: If neither primary nor fallback zone is found for the CCP.
    """
    ccp_start = date(ccp_start_year, 6, 1)

    def _lookup(zone_id: int) -> pl.DataFrame:
        return fca_df.filter(
            (pl.col("ccp_start") == ccp_start) & (pl.col("capacity_zone_id") == zone_id)
        )

    rows = _lookup(primary_zone_id)
    if not rows.is_empty():
        price = float(rows["clearing_price_per_kw_month"][0])
        return price, primary_zone_id

    # Primary zone absent — try fallback
    rows = _lookup(fallback_zone_id)
    if not rows.is_empty():
        price = float(rows["clearing_price_per_kw_month"][0])
        return price, fallback_zone_id

    raise ValueError(
        f"No FCA price found for CCP starting {ccp_start} with zone_id={primary_zone_id} "
        f"or fallback zone_id={fallback_zone_id}. "
        f"Available (ccp_start, zone_id) pairs: "
        f"{sorted(set(zip(fca_df['ccp_start'].to_list(), fca_df['capacity_zone_id'].to_list())))}"
    )


def resolve_fca_price_for_calendar_year(
    fca_df: pl.DataFrame,
    capacity_zone_id: int,
    calendar_year: int,
    fallback_zone_id: int = ISONE_CAPACITY_ZONE_FALLBACK,
) -> float:
    """Compute capacity_cost_kw_year ($/kW-year) by blending two overlapping FCAs.

    A calendar year spans two ISO-NE Capacity Commitment Periods (CCPs):
      - CCP1: Jun (year-1) to May (year) — covers Jan–May of calendar_year (5 months)
      - CCP2: Jun (year)   to May (year+1) — covers Jun–Dec of calendar_year (7 months)

    Applies zone fallback: if capacity_zone_id (e.g. SENE=8506) has no entry for a
    given CCP, the fallback zone (e.g. System/RoP=8500) is used instead.

    Args:
        fca_df: DataFrame already filtered to resource_status='all'.
        capacity_zone_id: Primary FCA capacity zone ID (e.g. 8506 for SENE).
        calendar_year: Calendar year to compute cost for.
        fallback_zone_id: Zone ID to use when primary zone has no price for a CCP.

    Returns:
        capacity_cost_kw_year: Total annual capacity cost in $/kW-year.

    Example:
        Calendar year 2025:
          CCP1 (Jun 2024–May 2025): FCA 15 SENE $3.980/kW-mo × 5 = $19.900
          CCP2 (Jun 2025–May 2026): FCA 16 SENE $2.639/kW-mo × 7 = $18.473
          Total: $38.373/kW-year
    """
    # CCP1: the commitment period that runs Jun (year-1) to May (year)
    # covers Jan–May of calendar_year → 5 months
    months1 = 5
    price1, zone1 = _get_fca_price_for_ccp(
        fca_df, calendar_year - 1, capacity_zone_id, fallback_zone_id
    )

    # CCP2: the commitment period that runs Jun (year) to May (year+1)
    # covers Jun–Dec of calendar_year → 7 months
    months2 = 7
    price2, zone2 = _get_fca_price_for_ccp(
        fca_df, calendar_year, capacity_zone_id, fallback_zone_id
    )

    capacity_cost_kw_year = price1 * months1 + price2 * months2

    zone1_label = f"zone {zone1}" + (" [fallback]" if zone1 != capacity_zone_id else "")
    zone2_label = f"zone {zone2}" + (" [fallback]" if zone2 != capacity_zone_id else "")
    print(
        f"  FCA price resolution for calendar year {calendar_year}:"
        f"\n    CCP1 (Jun {calendar_year - 1}–May {calendar_year}): "
        f"${price1:.3f}/kW-mo × {months1} mo = ${price1 * months1:.3f}/kW ({zone1_label})"
        f"\n    CCP2 (Jun {calendar_year}–May {calendar_year + 1}): "
        f"${price2:.3f}/kW-mo × {months2} mo = ${price2 * months2:.3f}/kW ({zone2_label})"
        f"\n    Total: ${capacity_cost_kw_year:.4f}/kW-year"
    )
    return capacity_cost_kw_year


# ---------------------------------------------------------------------------
# ISO-NE zone load loading
# ---------------------------------------------------------------------------


def load_isone_zone_loads(
    zone_loads_s3_base: str,
    zone_names: list[str],
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load ISO-NE zone-level hourly loads for selected zones and year, summed to aggregate.

    Reads from the Hive-partitioned dataset at zone_loads_s3_base (partitioned by
    zone/year/month). Handles the timezone-aware ``interval_start_et`` column by
    stripping the timezone and renaming to ``timestamp``. Sums load_mw across all
    requested zones to produce a single aggregate load profile (e.g. SENE = RI + SEMA).

    Args:
        zone_loads_s3_base: S3 base path for ISO-NE zone hourly demand data.
        zone_names: Zone abbreviations to include in the aggregate (e.g. ["RI", "SEMA"]).
        year: Calendar year to load.
        storage_options: AWS storage options for S3 access.

    Returns:
        DataFrame with columns ``timestamp`` (naive datetime) and ``load_mw``
        (aggregate load in MW), with one row per hour (8760 or 8784 rows).
    """
    base = zone_loads_s3_base.rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(pl.col("zone").is_in(zone_names), pl.col("year") == year)
        .select("interval_start_et", "zone", "load_mw")
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from ISO-NE zone loads collect()")
    if collected.is_empty():
        raise FileNotFoundError(
            f"No ISO-NE zone load data found for zones={zone_names}, year={year} under {base}"
        )

    # Strip timezone from interval_start_et and rename to timestamp
    collected = strip_tz_if_needed(collected, "interval_start_et").rename(
        {"interval_start_et": "timestamp"}
    )

    zones_found = sorted(collected["zone"].unique().to_list())
    missing_zones = sorted(set(zone_names) - set(zones_found))
    if missing_zones:
        raise ValueError(
            f"Zone load data missing for zones: {missing_zones}. Found: {zones_found}"
        )

    # Sum across zones for SENE aggregate (or any multi-zone request)
    aggregate = (
        collected.group_by("timestamp")
        .agg(pl.col("load_mw").sum().alias("load_mw"))
        .sort("timestamp")
    )

    print(
        f"Loaded ISO-NE zone loads: {collected.height:,} zone-hour rows for zones "
        f"{zones_found}, year {year} → {aggregate.height:,} aggregate hours"
    )
    return aggregate


# ---------------------------------------------------------------------------
# Annual FCA exceedance allocation
# ---------------------------------------------------------------------------


def allocate_fca_to_hours(
    load_df: pl.DataFrame,
    capacity_cost_kw_year: float,
    n_peak_hours: int = N_PEAK_HOURS_ANNUAL,
) -> pl.DataFrame:
    """Allocate annual FCA $/kW-year to hourly $/kW via threshold exceedance.

    Identifies the top-N hours by system load (SENE aggregate), computes a load
    threshold as the maximum load strictly below the Nth-highest load (to handle
    ties exactly), and allocates capacity_cost_kw_year proportionally to the
    exceedance above that threshold.

    This is the annual equivalent of NYISO's allocate_icap_to_hours (which operates
    per-month). ISO-NE's FCA locks in a single price per CCP so the full annualized
    cost is allocated over the full calendar year at once.

    Args:
        load_df: DataFrame with columns ``timestamp`` (datetime) and ``load_mw`` (float).
            Typically the SENE aggregate load for the calendar year (8760 rows).
        capacity_cost_kw_year: Annual capacity cost per kW in $/kW-year.
        n_peak_hours: Number of top-system-peak hours to allocate cost over.

    Returns:
        DataFrame with columns ``timestamp`` and ``capacity_cost_per_kw``, containing
        exactly n_peak_hours nonzero rows (the rest are absent — non-peak hours carry
        no capacity cost). Sorted by timestamp.

    Raises:
        ValueError: If load_df has fewer rows than n_peak_hours, total exceedance is
            non-positive, or weights fail to sum to 1.
    """
    if load_df.height < n_peak_hours:
        raise ValueError(
            f"Load profile has only {load_df.height} hours, "
            f"need at least {n_peak_hours} for exceedance allocation"
        )

    sorted_load = load_df.sort("load_mw", descending=True)
    top_n = sorted_load.head(n_peak_hours)
    load_nth = float(top_n["load_mw"][-1])

    # Threshold = max load strictly below the Nth-highest (tiebreaker: exactly N hours)
    below = load_df.filter(pl.col("load_mw") < load_nth)["load_mw"]
    threshold = float(below.max()) if not below.is_empty() else 0.0  # type: ignore[arg-type]

    result = top_n.with_columns((pl.col("load_mw") - threshold).alias("exceedance"))
    total_exceedance = float(result["exceedance"].sum())
    if total_exceedance <= 0:
        raise ValueError(
            f"Total exceedance is zero or negative. "
            f"Threshold={threshold:.2f} MW, max load={float(sorted_load['load_mw'][0]):.2f} MW"
        )

    result = result.with_columns(
        (pl.col("exceedance") / total_exceedance * capacity_cost_kw_year).alias(
            "capacity_cost_per_kw"
        )
    )

    weight_sum = float((result["exceedance"] / total_exceedance).sum())
    if abs(weight_sum - 1.0) > 1e-6:
        raise ValueError(f"Exceedance weights sum to {weight_sum:.6f}, expected 1.0")

    n_nonzero = result.filter(pl.col("capacity_cost_per_kw") > 0).height
    print(
        f"  FCA annual allocation: ${capacity_cost_kw_year:.4f}/kW-yr, "
        f"threshold={threshold:,.1f} MW, "
        f"{n_nonzero} peak hours (of {n_peak_hours} requested)"
    )

    return result.select("timestamp", "capacity_cost_per_kw").sort("timestamp")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_fca_allocation(
    capacity_df: pl.DataFrame,
    capacity_cost_kw_year: float,
) -> None:
    """Validate that a 1 kW constant load recovers the annual FCA cost.

    Sums all hourly capacity_cost_per_kw values and compares to capacity_cost_kw_year.
    A constant 1 kW load present in every peak hour would be allocated its proportional
    share, so the sum equals the annual cost exactly.

    Args:
        capacity_df: DataFrame with ``capacity_cost_per_kw`` column (output of
            allocate_fca_to_hours).
        capacity_cost_kw_year: Expected annual cost in $/kW-year.

    Raises:
        ValueError: If the percentage error exceeds 0.01%.
    """
    actual_annual = float(capacity_df["capacity_cost_per_kw"].sum())
    error = abs(actual_annual - capacity_cost_kw_year)
    error_pct = (
        (error / capacity_cost_kw_year * 100) if capacity_cost_kw_year > 0 else 0.0
    )

    print("\n" + "=" * 60)
    print("VALIDATION: 1 kW Constant Load -> FCA Recovery")
    print("=" * 60)
    print(
        f"  Expected (annual FCA cost):             ${capacity_cost_kw_year:.4f}/kW-yr"
    )
    print(f"  Actual (sum of hourly allocations):     ${actual_annual:.4f}/kW-yr")
    print(f"  Error: ${error:.6f} ({error_pct:.6f}%)")

    tolerance = 0.01
    if error_pct > tolerance:
        print("  Validation FAILED")
        print("=" * 60)
        raise ValueError(
            f"FCA validation failed: error {error_pct:.6f}% exceeds {tolerance}%. "
            f"Expected ${capacity_cost_kw_year:.4f}/kW-yr, got ${actual_annual:.4f}/kW-yr."
        )
    print("  Validation PASSED")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


def compute_isone_supply_capacity_mc(
    utility: str,
    year: int,
    storage_options: dict[str, str],
    fca_s3_path: str = DEFAULT_ISONE_FCA_S3_PATH,
    zone_loads_s3_base: str = DEFAULT_ISONE_ZONE_LOADS_S3_BASE,
    capacity_zone_id: int | None = None,
    capacity_load_zones: list[str] | None = None,
    n_peak_hours: int = N_PEAK_HOURS_ANNUAL,
    fallback_zone_id: int = ISONE_CAPACITY_ZONE_FALLBACK,
    capacity_load_year: int | None = None,
) -> pl.DataFrame:
    """Compute hourly utility-level ISO-NE supply capacity MC from FCA data.

    Args:
        utility: Utility short name (e.g. 'rie'). Used to look up default
            capacity_zone_id and capacity_load_zones from supply_utils constants.
        year: Calendar year for which to generate the capacity MC.
        storage_options: AWS storage options for S3 access.
        fca_s3_path: S3 path to FCA clearing price parquet.
        zone_loads_s3_base: S3 base for ISO-NE zone hourly demand data.
        capacity_zone_id: FCA capacity zone ID (e.g. 8506 for SENE). Defaults to
            ISONE_UTILITY_CAPACITY_ZONES[utility].
        capacity_load_zones: Zone names to sum for aggregate peak-load identification
            (e.g. ["RI", "SEMA"] for SENE). Defaults to
            ISONE_CAPACITY_ZONE_LOAD_ZONES[utility].
        n_peak_hours: Number of annual peak hours to allocate cost over.
        fallback_zone_id: Zone ID to use when capacity_zone_id has no FCA price.
        capacity_load_year: Year of zone load profile for peak identification.
            Defaults to year.

    Returns:
        DataFrame with columns ``timestamp`` (naive datetime) and
        ``capacity_cost_per_kw`` ($/kW per hour, nonzero only at peak hours).
        The timestamp column covers only the n_peak_hours peak hours; non-peak
        hours are absent (prepare_component_output fills nulls with 0).
    """
    capacity_load_year = capacity_load_year if capacity_load_year is not None else year

    # Resolve defaults from supply_utils constants
    if capacity_zone_id is None:
        if utility not in ISONE_UTILITY_CAPACITY_ZONES:
            raise ValueError(
                f"No default capacity_zone_id for utility {utility!r}. "
                f"Provide capacity_zone_id explicitly or add to ISONE_UTILITY_CAPACITY_ZONES."
            )
        capacity_zone_id = ISONE_UTILITY_CAPACITY_ZONES[utility]

    if capacity_load_zones is None:
        if utility not in ISONE_CAPACITY_ZONE_LOAD_ZONES:
            raise ValueError(
                f"No default capacity_load_zones for utility {utility!r}. "
                f"Provide capacity_load_zones explicitly or add to ISONE_CAPACITY_ZONE_LOAD_ZONES."
            )
        capacity_load_zones = ISONE_CAPACITY_ZONE_LOAD_ZONES[utility]

    print(f"  Utility:             {utility}")
    print(f"  Calendar year:       {year}")
    print(f"  Capacity zone ID:    {capacity_zone_id}")
    print(f"  Load zones (SENE):   {capacity_load_zones}")
    print(f"  Load year:           {capacity_load_year}")
    print(f"  Peak hours (annual): {n_peak_hours}")

    # 1. Load FCA prices (resource_status='all')
    print("\n── FCA Prices ──")
    fca_df = load_fca_prices(fca_s3_path, storage_options)

    # 2. Resolve blended annual capacity cost for the calendar year
    print("\n── FCA Price Resolution ──")
    capacity_cost_kw_year = resolve_fca_price_for_calendar_year(
        fca_df=fca_df,
        capacity_zone_id=capacity_zone_id,
        calendar_year=year,
        fallback_zone_id=fallback_zone_id,
    )

    # 3. Load ISO-NE zone loads for SENE aggregate (RI + SEMA)
    print(f"\n── Zone Loads (year={capacity_load_year}) ──")
    raw_load_df = load_isone_zone_loads(
        zone_loads_s3_base=zone_loads_s3_base,
        zone_names=capacity_load_zones,
        year=capacity_load_year,
        storage_options=storage_options,
    )

    # 4. Normalize to Cairo-compatible 8760 timestamps
    #    (drop Dec 31 in leap years; strip tz already done in load_isone_zone_loads)
    print("\n── Normalizing load to Cairo 8760 ──")
    ref_8760 = build_cairo_8760_timestamps(capacity_load_year)
    load_df = ref_8760.join(
        raw_load_df.with_columns(
            pl.col("timestamp").dt.truncate("1h").alias("timestamp")
        )
        .group_by("timestamp")
        .agg(pl.col("load_mw").mean().alias("load_mw")),
        on="timestamp",
        how="left",
    ).with_columns(pl.col("load_mw").fill_null(0.0))
    n_nulls = load_df.filter(pl.col("load_mw") == 0).height
    if n_nulls > 0:
        print(f"  Warning: {n_nulls} hours filled with 0 MW load after normalization")
    print(f"  Normalized load: {load_df.height} rows")

    # Remap timestamps to price_year if using a different load year
    if capacity_load_year != year:
        print(f"\n  Remapping load timestamps: {capacity_load_year} → {year}")
        offset = f"{year - capacity_load_year}y"
        load_df = load_df.with_columns(
            pl.col("timestamp").dt.offset_by(offset).alias("timestamp")
        )

    # 5. Allocate FCA cost to top-N annual peak hours
    print("\n── FCA Exceedance Allocation ──")
    capacity_df = allocate_fca_to_hours(
        load_df=load_df,
        capacity_cost_kw_year=capacity_cost_kw_year,
        n_peak_hours=n_peak_hours,
    )

    # 6. Validate 1-kW constant load recovery
    validate_fca_allocation(capacity_df, capacity_cost_kw_year)

    return capacity_df
