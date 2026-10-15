"""ISO-NE bulk transmission marginal cost logic using AESC avoided PTF values.

Methodology
-----------
Regional Network Service (RNS) costs are allocated to load-serving entities
based on their share of New England coincident peak load:

    RNS_Share_i = L_{i,t*} / L_{NE,t*}

where t* is the New England system peak hour, L_{NE,t*} is total NE load at
that hour, and L_{i,t*} is the entity's (or zone's) load at that hour.

Rhode Island's share is therefore driven by RI's load at the NE system peak.
The *magnitude* of the marginal cost comes from the **AESC 2024** Avoided
Energy Supply Components study (Synapse Energy Economics), which publishes
an avoided PTF (Pool Transmission Facility) cost in $/kW-year.  For
AESC 2024 this value is **$69/kW-year** across all six New England states.

Two load-based hourly allocations live here, both on the aggregate New
England system load (all 8 ISO-NE load zones summed) by default:

- ``isone_aesc_ptf``: the **top-100-hour exceedance** method on one year of
  load.  This is consistent with how RNS costs are peak-driven and with the
  sub-TX/dist PoP methodology used elsewhere in the platform.
- ``pop_kde``: a month x day-type x hour **probability of peak** from several
  years of load (truncated Gaussian KDE, ``probability_of_peak.py``), spread
  as ``$/kW-yr x PoP / num_days`` on the CAIRO 8760.  The default history is
  2022-2025, the same four years Eversource used for its own PoP.  The CT
  Justfile passes this method; the CLI default remains ``isone_aesc_ptf``
  so RI is unchanged.

A third method, ``month_hour``, uses OCC-863's published probability of peak
instead of a load curve; it lives in ``generate_bulk_tx_mc.py``.

Unit chain
----------
$/kW-year (AESC avoided PTF)
  --[top-100-hour exceedance on NE system load]--> $/kW per peak hour
  --[or: PoP on 2022-2025 NE system load / days per cell]--> $/kWh per hour type
  --[output as bulk_tx_cost_enduse]--> hourly signal for CAIRO BAT

For NYISO bulk transmission, see ``bulk_tx_nyiso.py``.
For the CLI entrypoint, see ``generate_bulk_tx_mc.py``.
"""

from __future__ import annotations

from typing import cast

import polars as pl

from utils.data_prep.marginal_costs.probability_of_peak import (
    WEEKEND_HOLIDAY,
    ProbabilityOfPeak,
    allocate_annual_cost_by_pop,
    probability_of_peak,
)
from utils.data_prep.marginal_costs.supply.supply_capacity_isone import (
    load_isone_zone_loads,
)
from utils.data_prep.marginal_costs.supply_utils import (
    allocate_annual_exceedance_to_hours,
    build_cairo_8760_timestamps,
)

# ── AESC 2024 default ────────────────────────────────────────────────────────
#
# Regional Transmission (PTF) avoided cost from AESC 2024 (Synapse Energy
# Economics), Table / Slide deck.  The value is the same across all six NE
# states because PTF is a regional cost allocated by 12-CP.
#
# Source: https://www.synapse-energy.com/aesc-2024-materials
AESC_2024_AVOIDED_PTF_KW_YEAR: float = 69.0

# AESC 2024 states its avoided costs in 2024 dollars.  The month_hour and
# pop_kde methods inflate the PTF value to the target dollar year with CPI;
# isone_aesc_ptf (exceedance) uses it as given.
AESC_2024_DOLLAR_YEAR: int = 2024

# Default number of top system-load hours for exceedance allocation.
# Matches the top-100 convention used for sub-TX/dist PoP and FCA capacity
# allocation.  Bulk TX may justify a narrower window (e.g. 50), but 100 is
# consistent and the difference is unlikely to be material for the BAT.
DEFAULT_N_PEAK_HOURS: int = 100

# History for the pop_kde method: the four complete years Eversource used for
# the OCC-863 probability of peak, so the bulk-tx and distribution signals
# rest on the same years.
DEFAULT_POP_KDE_HISTORY_YEARS: tuple[int, ...] = (2022, 2023, 2024, 2025)


# ── Core logic ────────────────────────────────────────────────────────────────


def compute_isone_bulk_tx_signal(
    load_df: pl.DataFrame,
    aesc_ptf_kw_year: float,
    n_peak_hours: int = DEFAULT_N_PEAK_HOURS,
    utility_zone_load_df: pl.DataFrame | None = None,
    utility_label: str = "Utility zone",
) -> pl.DataFrame:
    """Compute ISO-NE bulk transmission hourly signal via exceedance allocation.

    Args:
        load_df: Allocation load (8760 rows, columns: timestamp, load_mw).
            This is the load used to identify peak hours and drive exceedance
            allocation.  Typically either the NE system aggregate (all 8 zones)
            or the utility's own zone load, depending on the ``--allocation-load``
            CLI flag.
        aesc_ptf_kw_year: AESC avoided PTF cost in $/kW-year.
        n_peak_hours: Number of top load hours for exceedance allocation.
        utility_zone_load_df: Optional utility zone load for informational load-share
            display at the allocation peak.  Not used in the cost calculation.
            Only meaningful when ``load_df`` is system-wide (otherwise the utility
            zone IS the allocation load and its share is trivially 100%).
        utility_label: Human-readable label for the utility zone in the display
            (e.g. ``"RI"`` or ``"CT"``).

    Returns:
        DataFrame with columns ``timestamp`` and ``bulk_tx_cost_enduse``
        ($/kW per hour).  Contains *n_peak_hours* non-zero rows.
    """
    # Informational: show utility's share of the allocation load peak
    if utility_zone_load_df is not None:
        peak_hour = load_df.sort("load_mw", descending=True).head(1)
        peak_ts = peak_hour["timestamp"][0]
        peak_mw = float(peak_hour["load_mw"][0])

        util_at_peak = utility_zone_load_df.filter(pl.col("timestamp") == peak_ts)
        if not util_at_peak.is_empty():
            util_peak_mw = float(util_at_peak["load_mw"][0])
            load_share = util_peak_mw / peak_mw
            print("\n── Load Share at Allocation Peak ──")
            print(f"  Peak hour:               {peak_ts}")
            print(f"  Allocation load at peak: {peak_mw:,.1f} MW")
            print(f"  {utility_label} load at peak:   {util_peak_mw:,.1f} MW")
            print(
                f"  Load share:              {load_share:.4f} ({load_share * 100:.2f}%)"
            )

    # Allocate AESC PTF value to top-N peak hours
    print("\n── Bulk TX Exceedance Allocation ──")
    peak_hours_df = allocate_annual_exceedance_to_hours(
        load_df=load_df,
        annual_cost_kw_year=aesc_ptf_kw_year,
        n_peak_hours=n_peak_hours,
        cost_col="bulk_tx_cost_enduse",
    )

    return peak_hours_df


def parse_history_years(text: str) -> tuple[int, ...]:
    """Parse ``2022-2025`` or ``2022,2023,2024,2025`` into sorted years."""
    text = text.strip()
    if "-" in text and "," not in text:
        first, last = (int(part) for part in text.split("-", 1))
        if last < first:
            raise ValueError(f"history years {text!r}: end before start")
        return tuple(range(first, last + 1))
    years = tuple(sorted({int(part) for part in text.split(",") if part.strip()}))
    if not years:
        raise ValueError("history years must name at least one year")
    return years


def load_isone_zone_load_history(
    zone_loads_s3_base: str,
    zone_names: list[str],
    years: tuple[int, ...],
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Several calendar years of summed ISO-NE zone load, one row per hour.

    Each year goes through :func:`load_isone_zone_loads`, so the time zone is
    handled the same way as the exceedance method: ``interval_start_et`` is
    hour-beginning Eastern prevailing time, kept as naive wall-clock time, with
    the fall-back duplicate 1 AM averaged and the spring-forward 2 AM
    interpolated onto the CAIRO 8760 (which also drops December 31 in a leap
    year).  Returns ``timestamp`` and ``load_mw`` sorted by time.
    """
    frames = []
    for year in years:
        print(f"\n── Allocation load history (year={year}) ──")
        frames.append(
            load_isone_zone_loads(
                zone_loads_s3_base=zone_loads_s3_base,
                zone_names=zone_names,
                year=year,
                storage_options=storage_options,
            )
        )
    return pl.concat(frames).sort("timestamp")


def compute_isone_bulk_tx_pop_kde(
    load_history: pl.DataFrame,
    aesc_ptf_kw_year: float,
    year: int,
    *,
    calendar_year: int | None = None,
) -> tuple[pl.DataFrame, ProbabilityOfPeak]:
    """Spread the PTF cost over the 8760 by a KDE probability of peak.

    Args:
        load_history: Hourly allocation load over one or more complete
            calendar years (columns ``timestamp``, ``load_mw``), e.g. from
            :func:`load_isone_zone_load_history`.
        aesc_ptf_kw_year: Annual PTF cost to recover, in the dollars of the
            analysis year.
        year: Year stamped on the output timestamps.
        calendar_year: Calendar that classifies weekdays, weekends, and
            holidays and sets the day counts (default ``year``).

    Returns:
        The 8760 (``timestamp``, ``bulk_tx_cost_enduse``) and the
        :class:`ProbabilityOfPeak` behind it.
    """
    pop = probability_of_peak(load_history)
    hourly = allocate_annual_cost_by_pop(
        pop.cells,
        aesc_ptf_kw_year,
        year,
        calendar_year=calendar_year,
        value_col="bulk_tx_cost_enduse",
    )
    return hourly.select("timestamp", "bulk_tx_cost_enduse"), pop


def print_pop_summary(pop: ProbabilityOfPeak) -> None:
    """Print the delimiter, per-year normalization, and where the PoP lands."""
    print("\n── Probability of Peak (KDE) ──")
    print(f"  Delimiter: {pop.delimiter:.4f} x year mean load")
    print(f"  Delimiter / normalized peak: {pop.delimiter_share_of_peak:.4f}")
    print(pop.years)
    by_month = (
        pop.cells.group_by("month")
        .agg(pl.col("probability_of_peak").sum().alias("pop"))
        .sort("month")
        .filter(pl.col("pop") > 0)
    )
    weekend = pop.cells.filter(pl.col("daytype") == WEEKEND_HOLIDAY)[
        "probability_of_peak"
    ].sum()
    nonzero = pop.cells.filter(pl.col("probability_of_peak") > 0).height
    print(f"  Weekend-and-holiday share: {float(weekend):.4f}")
    print(f"  Nonzero cells: {nonzero} of {pop.cells.height}")
    print("  PoP by month (nonzero):")
    print(by_month)
    print("  Top 5 cells:")
    print(
        pop.cells.sort("probability_of_peak", descending=True)
        .head(5)
        .select("month", "daytype", "hour", "n_obs", "probability_of_peak")
    )


def prepare_output(allocated_df: pl.DataFrame, year: int) -> pl.DataFrame:
    """Expand allocated hours to full CAIRO-compatible 8760 output.

    Args:
        allocated_df: DataFrame with ``timestamp`` and ``bulk_tx_cost_enduse``.
        year: Target year for the 8760 timestamp grid.

    Returns:
        DataFrame with exactly 8760 rows (non-peak hours filled with 0.0).
    """
    ref_8760 = build_cairo_8760_timestamps(year)

    output = (
        ref_8760.join(allocated_df, on="timestamp", how="left")
        .with_columns(
            pl.col("bulk_tx_cost_enduse").fill_null(0.0).alias("bulk_tx_cost_enduse")
        )
        .sort("timestamp")
    )

    if output.height != 8760:
        raise ValueError(f"Output has {output.height} rows, expected 8760")
    if output.filter(pl.col("bulk_tx_cost_enduse").is_null()).height > 0:
        raise ValueError("Output has null bulk_tx_cost_enduse values")

    return output.select("timestamp", "bulk_tx_cost_enduse")


def validate_allocation(
    output_df: pl.DataFrame,
    aesc_ptf_kw_year: float,
) -> None:
    """Validate that a 1 kW constant load recovers the annual PTF cost.

    Args:
        output_df: Full 8760-row output with ``bulk_tx_cost_enduse``.
        aesc_ptf_kw_year: Expected annual cost in $/kW-year.

    Raises:
        ValueError: If percentage error exceeds 0.01%.
    """
    actual_annual = float(output_df["bulk_tx_cost_enduse"].sum())
    error = abs(actual_annual - aesc_ptf_kw_year)
    error_pct = (error / aesc_ptf_kw_year * 100) if aesc_ptf_kw_year > 0 else 0.0

    print("\n" + "=" * 60)
    print("VALIDATION: 1 kW Constant Load → PTF Recovery")
    print("=" * 60)
    print(f"  Expected (AESC PTF):                    ${aesc_ptf_kw_year:.4f}/kW-yr")
    print(f"  Actual (sum of hourly allocations):      ${actual_annual:.4f}/kW-yr")
    print(f"  Error: ${error:.6f} ({error_pct:.6f}%)")

    tolerance = 0.01
    if error_pct > tolerance:
        print("  ✗ Validation FAILED")
        print("=" * 60)
        raise ValueError(
            f"PTF validation failed: error {error_pct:.6f}% exceeds {tolerance}%. "
            f"Expected ${aesc_ptf_kw_year:.4f}/kW-yr, got ${actual_annual:.4f}/kW-yr."
        )
    print("  ✓ Validation PASSED")
    print("=" * 60)


def save_output(
    output_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    """Write bulk Tx MC parquet to S3 (hive-style utility/year partition path)."""
    output_s3_base = output_s3_base.rstrip("/") + "/"
    output_path = f"{output_s3_base}utility={utility}/year={year}/data.parquet"
    output_df.write_parquet(output_path, storage_options=storage_options)

    print(f"\n✓ Saved bulk Tx MC to {output_path}")
    print(f"  Rows: {len(output_df):,}")


def print_summary(output_df: pl.DataFrame) -> None:
    """Print top-10 hours and summary statistics for a bulk TX output DataFrame."""
    print("\n" + "=" * 60)
    print("SAMPLE: Top 10 hours by bulk Tx cost")
    print("=" * 60)
    print(output_df.sort("bulk_tx_cost_enduse", descending=True).head(10))

    avg_cost = cast(float, output_df["bulk_tx_cost_enduse"].mean())
    max_cost = cast(float, output_df["bulk_tx_cost_enduse"].max())
    n_nonzero = output_df.filter(pl.col("bulk_tx_cost_enduse") > 0).height
    print("\nOutput summary:")
    print(f"  avg = ${avg_cost:.6f}/kWh, max = ${max_cost:.6f}/kWh")
    print(f"  {n_nonzero} non-zero hours out of 8760")
