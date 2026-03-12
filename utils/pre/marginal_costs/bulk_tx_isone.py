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

The hourly allocation uses the **top-100-hour exceedance** method on the
aggregate New England system load (all 8 ISO-NE load zones summed).  This
is consistent with how RNS costs are peak-driven and with the sub-TX/dist
PoP methodology used elsewhere in the platform.

Unit chain
----------
$/kW-year (AESC avoided PTF)
  --[top-100-hour exceedance on NE system load]--> $/kW per peak hour
  --[output as bulk_tx_cost_enduse]--> hourly signal for CAIRO BAT

For NYISO bulk transmission, see ``bulk_tx_nyiso.py``.
For the CLI entrypoint, see ``generate_bulk_tx_mc.py``.
"""

from __future__ import annotations

from typing import cast

import polars as pl

from utils.pre.marginal_costs.supply_utils import (
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

# Default number of top system-load hours for exceedance allocation.
# Matches the top-100 convention used for sub-TX/dist PoP and FCA capacity
# allocation.  Bulk TX may justify a narrower window (e.g. 50), but 100 is
# consistent and the difference is unlikely to be material for the BAT.
DEFAULT_N_PEAK_HOURS: int = 100


# ── Core logic ────────────────────────────────────────────────────────────────


def compute_isone_bulk_tx_signal(
    ne_load_df: pl.DataFrame,
    aesc_ptf_kw_year: float,
    n_peak_hours: int = DEFAULT_N_PEAK_HOURS,
    ri_zone_load_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Compute ISO-NE bulk transmission hourly signal from NE system load.

    Args:
        ne_load_df: NE-wide aggregate load (8760 rows, columns: timestamp, load_mw).
        aesc_ptf_kw_year: AESC avoided PTF cost in $/kW-year.
        n_peak_hours: Number of top NE system-load hours for exceedance allocation.
        ri_zone_load_df: Optional RI zone load for informational RNS share display.
            Not used in the cost calculation.

    Returns:
        DataFrame with columns ``timestamp`` and ``bulk_tx_cost_enduse``
        ($/kW per hour).  Contains *n_peak_hours* non-zero rows.
    """
    # Informational: show RI's share of NE peak for context
    if ri_zone_load_df is not None:
        ne_peak_hour = ne_load_df.sort("load_mw", descending=True).head(1)
        ne_peak_ts = ne_peak_hour["timestamp"][0]
        ne_peak_mw = float(ne_peak_hour["load_mw"][0])

        ri_at_peak = ri_zone_load_df.filter(pl.col("timestamp") == ne_peak_ts)
        if not ri_at_peak.is_empty():
            ri_peak_mw = float(ri_at_peak["load_mw"][0])
            rns_share = ri_peak_mw / ne_peak_mw
            print("\n── RNS Share at NE System Peak ──")
            print(f"  NE peak hour:   {ne_peak_ts}")
            print(f"  NE peak load:   {ne_peak_mw:,.1f} MW")
            print(f"  RI load at peak: {ri_peak_mw:,.1f} MW")
            print(f"  RI RNS share:   {rns_share:.4f} ({rns_share * 100:.2f}%)")

    # Allocate AESC PTF value to top-N NE system peak hours
    print("\n── Bulk TX Exceedance Allocation ──")
    peak_hours_df = allocate_annual_exceedance_to_hours(
        load_df=ne_load_df,
        annual_cost_kw_year=aesc_ptf_kw_year,
        n_peak_hours=n_peak_hours,
        cost_col="bulk_tx_cost_enduse",
    )

    return peak_hours_df


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
