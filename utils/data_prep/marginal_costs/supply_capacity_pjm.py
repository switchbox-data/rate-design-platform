"""Capacity (RPM) marginal cost computation for PJM (Maryland) utility supply MCs.

Methodology
-----------
Translate PJM's annual generation-adequacy cost (the Reliability Pricing Model
Final Zonal Capacity Price) into an hourly capacity cost adder ($/kW per hour)
concentrated on the five summer coincident-peak (5CP) hours that drive a
customer's capacity obligation (PLC), then scale to $/MWh via
prepare_component_output(scale=1000).

See context/domain/marginal_costs/pjm_supply_capacity_marginal_cost.md for the
full decision record. v1 defaults (BGE):

- Price (A1): Final Zonal Capacity Price for the utility's price zone. The
  curated RPM dataset already bakes in the locational (LDA) adder, so we select
  the utility's own zone row (e.g. BGE) — which is a *constrained* LDA in most
  delivery years — rather than assuming the RTO system price.
- Annualization (B1) via an exact calendar-year day-count blend of the two
  overlapping delivery years (PJM DY runs Jun 1 – May 31):
    DY1 (Jun year-1 – May year) covers Jan 1 – May 31 of the calendar year.
    DY2 (Jun year   – May year+1) covers Jun 1 – Dec 31 of the calendar year.
  Because the RPM price is natively $/MW-day, we weight each DY by its actual
  number of calendar-year days (not a 5/7 month approximation).
- Peak hours (C1 + D1): the calendar year's own published RTO 5CP hours
  (Jun 1 – Sep 30).
- Weights (F1): equal 1/5. PJM defines PLC as the simple *average* of load over
  the five hours, i.e. each hour carries weight 1/5 (PJM Manual 19 §4.3).

Unit chain:
  $/MW-day
  --[day-count blend across two DYs, /1000]--> $/kW-year
  --[equal split across 5 hours]--> $/kW per peak hour
  --[x1000 via prepare_component_output]--> $/MWh = capacity_cost_enduse
"""

from __future__ import annotations

from datetime import date

import polars as pl

from utils.data_prep.marginal_costs.supply_utils import (
    DEFAULT_PJM_5CP_S3_PATH,
    DEFAULT_PJM_RPM_S3_PATH,
    PJM_UTILITY_ZONES,
    build_cairo_8760_timestamps,
)

# Number of summer coincident-peak hours PJM publishes per delivery year.
N_5CP_HOURS = 5

# ---------------------------------------------------------------------------
# RPM price loading
# ---------------------------------------------------------------------------


def load_rpm_prices(
    rpm_s3_path: str,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load curated PJM RPM capacity prices from parquet.

    Schema (see data/pjm/capacity/rpm/):
        delivery_year                         String  ('2024/25')
        dy_start, dy_end                      Date
        zone                                  String  ('BGE', 'PEPCO', ...)
        lda                                   String
        bra_price_per_mw_day                  Float64
        final_zonal_capacity_price_per_mw_day Float64
        ...

    Returns the full DataFrame (one row per delivery_year x zone).
    """
    df = pl.read_parquet(rpm_s3_path, storage_options=storage_options)

    required = {
        "delivery_year",
        "dy_start",
        "zone",
        "final_zonal_capacity_price_per_mw_day",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"RPM parquet missing columns: {sorted(missing)}")

    print(f"Loaded RPM prices: {df.height} rows from {rpm_s3_path}")
    return df


def _get_rpm_price_per_mw_day(
    rpm_df: pl.DataFrame,
    dy_start_year: int,
    price_zone: str,
) -> tuple[float, str]:
    """Return (final_zonal_capacity_price_per_mw_day, delivery_year) for one DY.

    Selects the row whose delivery year *starts* in ``dy_start_year`` (June 1)
    for ``price_zone``.

    Raises:
        ValueError: If no matching (delivery_year, zone) row is found.
    """
    rows = rpm_df.filter(
        (pl.col("dy_start").dt.year() == dy_start_year) & (pl.col("zone") == price_zone)
    )
    if rows.is_empty():
        available = sorted(
            set(
                zip(
                    rpm_df["dy_start"].dt.year().to_list(),
                    rpm_df["zone"].to_list(),
                )
            )
        )
        raise ValueError(
            f"No RPM price found for DY starting {dy_start_year} and zone "
            f"{price_zone!r}. Available (dy_start_year, zone) pairs: {available}"
        )
    price = float(rows["final_zonal_capacity_price_per_mw_day"][0])
    delivery_year = str(rows["delivery_year"][0])
    return price, delivery_year


def resolve_rpm_price_for_calendar_year(
    rpm_df: pl.DataFrame,
    price_zone: str,
    calendar_year: int,
) -> float:
    """Compute capacity_cost_kw_year ($/kW-year) for a calendar year.

    A calendar year spans two PJM delivery years (DY = Jun 1 – May 31):
      - DY1 (Jun year-1 – May year): covers Jan 1 – May 31 of calendar_year
      - DY2 (Jun year   – May year+1): covers Jun 1 – Dec 31 of calendar_year

    Because the RPM Final Zonal Capacity Price is natively $/MW-day, each DY is
    weighted by its actual number of calendar-year days (exact day-count blend),
    then converted MW -> kW:

        capacity_cost_kw_year =
            (P_DY1 * days(Jan 1..May 31) + P_DY2 * days(Jun 1..Dec 31)) / 1000

    Args:
        rpm_df: RPM prices DataFrame (output of load_rpm_prices).
        price_zone: RPM zone label for the utility (e.g. 'BGE').
        calendar_year: Calendar year to compute cost for.

    Returns:
        capacity_cost_kw_year: Total annual capacity cost in $/kW-year.
    """
    days_jan_may = (date(calendar_year, 6, 1) - date(calendar_year, 1, 1)).days
    days_jun_dec = (date(calendar_year + 1, 1, 1) - date(calendar_year, 6, 1)).days

    price_dy1, dy1_label = _get_rpm_price_per_mw_day(
        rpm_df, calendar_year - 1, price_zone
    )
    price_dy2, dy2_label = _get_rpm_price_per_mw_day(rpm_df, calendar_year, price_zone)

    cost_dy1 = price_dy1 * days_jan_may / 1000.0
    cost_dy2 = price_dy2 * days_jun_dec / 1000.0
    capacity_cost_kw_year = cost_dy1 + cost_dy2

    print(
        f"  RPM price resolution for calendar year {calendar_year} "
        f"(zone {price_zone}):"
        f"\n    DY1 {dy1_label} (Jan 1–May 31, {days_jan_may} d): "
        f"${price_dy1:.4f}/MW-day → ${cost_dy1:.4f}/kW"
        f"\n    DY2 {dy2_label} (Jun 1–Dec 31, {days_jun_dec} d): "
        f"${price_dy2:.4f}/MW-day → ${cost_dy2:.4f}/kW"
        f"\n    Total: ${capacity_cost_kw_year:.4f}/kW-year"
    )
    return capacity_cost_kw_year


# ---------------------------------------------------------------------------
# 5CP peak-hour selection
# ---------------------------------------------------------------------------


def select_5cp_hours(
    fivecp_df: pl.DataFrame,
    summer_year: int,
) -> pl.DataFrame:
    """Return the five RTO coincident-peak hours for a summer as hour-beginning ts.

    The 5CP dataset stores hour-*ending* clock hours (``hour_ending_ept``, e.g.
    18 means the hour ending 18:00, i.e. 17:00–18:00). CAIRO's 8760 grid uses
    hour-*beginning* naive Eastern timestamps, so the peak hour-beginning is
    ``peak_date`` at ``hour_ending_ept - 1`` o'clock (HE18 -> 17:00).

    Args:
        fivecp_df: 5CP peaks DataFrame (see data/pjm/capacity/5cp/).
        summer_year: Summer (Jun–Sep) whose five RTO peaks to select.

    Returns:
        DataFrame with columns ``timestamp`` (naive datetime, hour-beginning),
        ``rank`` (1–5), and ``mw_unrestricted`` (RTO MW), sorted by timestamp.

    Raises:
        ValueError: If the summer does not have exactly five distinct RTO hours.
    """
    required = {"summer_year", "rank", "peak_date", "hour_ending_ept", "zone"}
    missing = required - set(fivecp_df.columns)
    if missing:
        raise ValueError(f"5CP parquet missing columns: {sorted(missing)}")

    peaks = (
        fivecp_df.filter(
            (pl.col("zone") == "RTO") & (pl.col("summer_year") == summer_year)
        )
        .with_columns(
            (
                pl.col("peak_date").cast(pl.Datetime)
                + pl.duration(hours=pl.col("hour_ending_ept") - 1)
            ).alias("timestamp")
        )
        .select("timestamp", "rank", "mw_unrestricted")
        .sort("timestamp")
    )

    if peaks.height != N_5CP_HOURS:
        raise ValueError(
            f"Expected {N_5CP_HOURS} RTO 5CP hours for summer {summer_year}, "
            f"found {peaks.height}. Backfill data/pjm/capacity/5cp/ for that summer."
        )
    n_unique = peaks.select(pl.col("timestamp").n_unique()).item()
    if n_unique != N_5CP_HOURS:
        raise ValueError(
            f"5CP hours for summer {summer_year} contain duplicate timestamps "
            f"({n_unique} unique of {peaks.height})."
        )

    months = peaks.select(pl.col("timestamp").dt.month()).to_series().to_list()
    if any(m < 6 or m > 9 for m in months):
        raise ValueError(
            f"5CP hours for summer {summer_year} fall outside Jun–Sep: months={months}"
        )

    print(f"  Selected {peaks.height} RTO 5CP hours for summer {summer_year}:")
    for ts, rank in zip(
        peaks["timestamp"].to_list(), peaks["rank"].to_list(), strict=True
    ):
        print(f"    rank {rank}: {ts}")
    return peaks


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_pjm_allocation(
    capacity_df: pl.DataFrame,
    capacity_cost_kw_year: float,
) -> None:
    """Validate that a 1 kW constant load recovers the annual RPM cost.

    Sums all hourly capacity_cost_per_kw values and compares to the annual
    capacity_cost_kw_year. A constant 1 kW load present in every peak hour is
    allocated its proportional share, so the sum equals the annual cost exactly.

    Raises:
        ValueError: If the percentage error exceeds 0.01%.
    """
    actual_annual = float(capacity_df["capacity_cost_per_kw"].sum())
    error = abs(actual_annual - capacity_cost_kw_year)
    error_pct = (
        (error / capacity_cost_kw_year * 100) if capacity_cost_kw_year > 0 else 0.0
    )

    print("\n" + "=" * 60)
    print("VALIDATION: 1 kW Constant Load -> RPM Recovery")
    print("=" * 60)
    print(
        f"  Expected (annual RPM cost):             ${capacity_cost_kw_year:.4f}/kW-yr"
    )
    print(f"  Actual (sum of hourly allocations):     ${actual_annual:.4f}/kW-yr")
    print(f"  Error: ${error:.6f} ({error_pct:.6f}%)")

    tolerance = 0.01
    if error_pct > tolerance:
        print("  Validation FAILED")
        print("=" * 60)
        raise ValueError(
            f"PJM validation failed: error {error_pct:.6f}% exceeds {tolerance}%. "
            f"Expected ${capacity_cost_kw_year:.4f}/kW-yr, got ${actual_annual:.4f}/kW-yr."
        )
    print("  Validation PASSED")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


def compute_supply_capacity_mc_pjm(
    utility: str,
    year: int,
    storage_options: dict[str, str],
    rpm_s3_path: str = DEFAULT_PJM_RPM_S3_PATH,
    fivecp_s3_path: str = DEFAULT_PJM_5CP_S3_PATH,
    price_zone: str | None = None,
    n_peak_hours: int = N_5CP_HOURS,
) -> pl.DataFrame:
    """Compute hourly utility-level PJM supply capacity MC from RPM + 5CP data.

    Args:
        utility: Utility short name (e.g. 'bge'). Used to look up the default
            price_zone from PJM_UTILITY_ZONES.
        year: Calendar year for which to generate the capacity MC.
        storage_options: AWS storage options for S3 access.
        rpm_s3_path: S3 path to the curated RPM prices parquet.
        fivecp_s3_path: S3 path to the curated 5CP peaks parquet.
        price_zone: RPM zone label (e.g. 'BGE'). Defaults to
            PJM_UTILITY_ZONES[utility].
        n_peak_hours: Number of 5CP hours to spread cost over (default 5).

    Returns:
        DataFrame with columns ``timestamp`` (naive datetime) and
        ``capacity_cost_per_kw`` ($/kW per hour, nonzero only at the 5CP hours),
        containing all 8760 hours for the calendar year (non-peak hours = 0.0).
    """
    if price_zone is None:
        if utility not in PJM_UTILITY_ZONES:
            raise ValueError(
                f"No default price_zone for utility {utility!r}. "
                f"Provide price_zone explicitly or add to PJM_UTILITY_ZONES."
            )
        price_zone = PJM_UTILITY_ZONES[utility]

    print(f"  Utility:             {utility}")
    print(f"  Calendar year:       {year}")
    print(f"  Price zone:          {price_zone}")
    print(f"  Peak hours (5CP):    {n_peak_hours}")

    # 1. Resolve blended annual capacity cost for the calendar year.
    print("\n── RPM Prices ──")
    rpm_df = load_rpm_prices(rpm_s3_path, storage_options)
    print("\n── RPM Price Resolution ──")
    capacity_cost_kw_year = resolve_rpm_price_for_calendar_year(
        rpm_df=rpm_df,
        price_zone=price_zone,
        calendar_year=year,
    )

    # 2. Select the calendar year's own summer 5CP hours.
    print(f"\n── 5CP Hour Selection (summer {year}) ──")
    fivecp_df = pl.read_parquet(fivecp_s3_path, storage_options=storage_options)
    peaks = select_5cp_hours(fivecp_df, summer_year=year)

    # 3. Equal 1/K allocation (PLC = average over the five hours).
    per_hour_cost = capacity_cost_kw_year / n_peak_hours
    peak_hours_df = peaks.select(
        "timestamp", pl.lit(per_hour_cost).alias("capacity_cost_per_kw")
    )
    print(
        f"\n  Equal-weight allocation: ${capacity_cost_kw_year:.4f}/kW-yr / "
        f"{n_peak_hours} = ${per_hour_cost:.4f}/kW on each 5CP hour"
    )

    # 4. Validate 1-kW constant-load recovery.
    validate_pjm_allocation(peak_hours_df, capacity_cost_kw_year)

    # 5. Expand to the full 8760-hour grid, filling non-peak hours with 0.0.
    print("\n── Expanding to Full 8760 Hours ──")
    ref_8760 = build_cairo_8760_timestamps(year)
    capacity_df = (
        ref_8760.join(peak_hours_df, on="timestamp", how="left")
        .with_columns(pl.col("capacity_cost_per_kw").fill_null(0.0))
        .sort("timestamp")
    )
    n_peaks_placed = capacity_df.filter(pl.col("capacity_cost_per_kw") > 0).height
    if n_peaks_placed != n_peak_hours:
        raise ValueError(
            f"Placed {n_peaks_placed} nonzero peak hours on the 8760 grid, "
            f"expected {n_peak_hours}. A 5CP timestamp may not align to the "
            f"Cairo grid (check DST / leap-year handling)."
        )
    print(f"  Placed {n_peaks_placed} peak hours on {capacity_df.height} total hours")

    n_rows = capacity_df.height
    n_unique = capacity_df.select(pl.col("timestamp").n_unique()).item()
    if n_rows != 8760:
        raise ValueError(f"Capacity MC DataFrame has {n_rows} rows, expected 8760.")
    if n_rows != n_unique:
        raise ValueError(
            f"Capacity MC DataFrame has {n_rows} rows but {n_unique} unique "
            f"timestamps. Duplicate timestamps detected."
        )

    return capacity_df
