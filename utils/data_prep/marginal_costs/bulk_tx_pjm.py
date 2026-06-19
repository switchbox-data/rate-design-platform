"""PJM bulk transmission marginal cost logic using NITS rates and PCAF allocation.

Methodology
-----------
PJM Network Integration Transmission Service (NITS) rates represent the bulk
transmission cost per MW-year for each transmission zone. Following the E3 2025
Illinois ICC-VDER methodology (Appendix C), we allocate the annual $/kW-yr cost
to the top-K hours using Peak Capacity Allocation Factor (PCAF) load-share
weighting across the full year — no seasonal filter.

Key design choices (see context/methods/marginal_costs/md_bulk_transmission.md):

1. **Annual value**: Day-weighted blend of Jan and Jun NITS rates (derived from
   PJM Manual 27 §5.2.2 daily billing formula).
2. **No seasonal filter**: Following E3's ICC-VDER methodology, which uses
   full-year top-K hours with no seasonal restriction. This is a deliberate
   departure from PJM's K=1 NSPL billing mechanism, chosen for rate design
   tractability (see methodology doc for full rationale).
3. **Allocation method**: PCAF load-share (not exceedance). Each of the top-K
   hours receives a share proportional to its load divided by total load across
   all K peak hours.
4. **K = 150**: Following E3's ICC-VDER Appendix C which uses top-150 hours for
   transmission and distribution capacity allocation.

Result: exactly K = 150 hours have non-zero cost; all others are zero.

Unit chain
----------
$/kW-year (blended NITS)
  --[PCAF load-share on top-150 full-year hours]--> $/kW per peak hour
  --[output as bulk_tx_cost_enduse]--> hourly signal for CAIRO BAT

Sources
-------
- E3 ICC-VDER Report, Illinois (Jan 2025), Section 3.2 Table 8 and Appendix C.
  E3 uses a single NTS rate snapshot (ComEd: $39.80/kW-yr = PJM NITS rate of
  $39,796/MW-yr from the June 2023 formula rate filing, in effect through
  early 2024). Source: ETCC PJM Transmission Rates History 2015-2024.
- PJM Manual 27, §5.2.2 (daily NITS billing formula).
"""

from __future__ import annotations

import calendar
from pathlib import Path
from typing import cast

import polars as pl

from utils.data_prep.marginal_costs.supply_utils import build_cairo_8760_timestamps

# ── Constants ─────────────────────────────────────────────────────────────────

DEFAULT_K_PEAK_HOURS: int = 150
_VALIDATION_TOLERANCE_PCT: float = 0.01

# Jun 1 – Dec 31 is always 214 days regardless of leap year
# (30+31+31+30+31+30+31 = Jun through Dec)
_DAYS_JUN_TO_DEC: int = 214

PJM_HOURLY_DEMAND_S3_BASE = "s3://data.sb/pjm/hourly_demand/utilities"
DEFAULT_PJM_BULK_TX_OUTPUT_S3_BASE = "s3://data.sb/switchbox/marginal_costs/md/bulk_tx"
NITS_CSV_PATH = (
    Path(__file__).resolve().parents[3] / "data/pjm/bulk_tx/nits/nits_rates.csv"
)

# Map utility name to NITS zone (used for looking up rates in CSV).
# VALID_PJM_UTILITIES is derived from this mapping — add entries here only.
UTILITY_TO_NITS_ZONE: dict[str, str] = {
    "bge": "BGE",
    "dpl": "DPL",
    "pepco": "PEPCO",
    "potomac-edison": "APS",
}
VALID_PJM_UTILITIES: frozenset[str] = frozenset(UTILITY_TO_NITS_ZONE)


# ── NITS rate loading ─────────────────────────────────────────────────────────


def load_nits_rates(nits_csv_path: Path | str = NITS_CSV_PATH) -> pl.DataFrame:
    """Load the NITS rates CSV (skipping comment lines)."""
    return pl.read_csv(str(nits_csv_path), comment_prefix="#")


def compute_blended_nits_rate(nits_df: pl.DataFrame, zone: str, year: int) -> float:
    """Compute the day-weighted blended NITS rate ($/kW-yr) for a zone and year.

    Formula (PJM Manual 27 §5.2.2):
        blended = (days_jan × jan_rate + days_jun × jun_rate) / days_in_year

    Non-leap: (151 × jan + 214 × jun) / 365
    Leap:     (152 × jan + 214 × jun) / 366
    """
    year_rows = nits_df.filter((pl.col("year") == year) & (pl.col("zone") == zone))

    if year_rows.height == 0:
        available_years = sorted(nits_df["year"].unique().to_list())
        raise ValueError(
            f"No NITS data for zone={zone}, year={year}. "
            f"Available years: {available_years}"
        )

    jan_rows = year_rows.filter(pl.col("effective_date").str.contains("-01-01"))
    jun_rows = year_rows.filter(pl.col("effective_date").str.contains("-06-01"))

    if jan_rows.height == 0 or jun_rows.height == 0:
        raise ValueError(
            f"Missing Jan or Jun rate for zone={zone}, year={year}. "
            f"Found effective_dates: {year_rows['effective_date'].to_list()}"
        )

    jan_rate_kw = float(jan_rows["nits_rate_kw_yr"][0])
    jun_rate_kw = float(jun_rows["nits_rate_kw_yr"][0])

    is_leap = calendar.isleap(year)
    days_jan = 152 if is_leap else 151  # Jan 1 – May 31
    days_year = 366 if is_leap else 365

    blended = (days_jan * jan_rate_kw + _DAYS_JUN_TO_DEC * jun_rate_kw) / days_year
    return blended


# ── Hourly demand loading ─────────────────────────────────────────────────────


def load_hourly_demand(
    utility: str,
    year: int,
    s3_base: str = PJM_HOURLY_DEMAND_S3_BASE,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Load PJM hourly demand for a utility and year from S3.

    Returns DataFrame with columns: timestamp (tz-aware Eastern), load_mw.
    """
    path = f"{s3_base.rstrip('/')}/utility={utility}/year={year}/data.parquet"
    kwargs: dict = {}
    if storage_options:
        kwargs["storage_options"] = storage_options

    df = pl.read_parquet(path, **kwargs)

    if "timestamp" not in df.columns or "load_mw" not in df.columns:
        raise ValueError(
            f"Expected columns 'timestamp' and 'load_mw' in {path}, got: {df.columns}"
        )

    return df.select("timestamp", "load_mw")


# ── PCAF allocation ───────────────────────────────────────────────────────────


def allocate_pcaf(
    load_df: pl.DataFrame,
    annual_cost_kw_year: float,
    k_peak_hours: int = DEFAULT_K_PEAK_HOURS,
) -> pl.DataFrame:
    """Allocate annual $/kW-yr to top-K full-year hours using PCAF load-share.

    Implements E3's ICC-VDER Appendix C methodology (no seasonal filter):
        AF_h = load_h / sum(load across top-K hours)
        cost_h = AF_h * annual_cost_kw_year

    Args:
        load_df: DataFrame with 'timestamp' (tz-aware or naive) and 'load_mw'.
        annual_cost_kw_year: Blended annual NITS rate in $/kW-year.
        k_peak_hours: Number of top hours for allocation (default 150).

    Returns:
        DataFrame with 'timestamp' and 'bulk_tx_cost_enduse' for the K peak hours.
        Exactly k_peak_hours rows with non-zero cost.
    """
    if load_df.height < k_peak_hours:
        raise ValueError(
            f"Load data has only {load_df.height} hours, "
            f"need at least {k_peak_hours} for PCAF allocation."
        )

    # Select top-K hours by load across the full year
    top_k = load_df.sort("load_mw", descending=True).head(k_peak_hours)

    # Compute PCAF load-share weights
    total_peak_load = float(top_k["load_mw"].sum())
    if total_peak_load <= 0:
        raise ValueError(
            f"Total load across top-{k_peak_hours} hours is zero or negative."
        )

    result = top_k.with_columns(
        (pl.col("load_mw") / total_peak_load * annual_cost_kw_year).alias(
            "bulk_tx_cost_enduse"
        )
    )

    return result.select("timestamp", "bulk_tx_cost_enduse").sort("timestamp")


# ── Full pipeline ─────────────────────────────────────────────────────────────


def compute_pjm_bulk_tx_mc(
    utility: str,
    year: int,
    nits_csv_path: Path | str = NITS_CSV_PATH,
    k_peak_hours: int = DEFAULT_K_PEAK_HOURS,
    s3_base: str = PJM_HOURLY_DEMAND_S3_BASE,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """End-to-end PJM bulk TX MC computation for a single utility and year.

    Steps:
        1. Load NITS rates and compute blended annual $/kW-yr.
        2. Load hourly demand for the utility.
        3. Run PCAF load-share allocation on top-K full-year hours.
        4. Expand to full 8760 output.
        5. Validate sum equals blended rate.

    Returns:
        8760-row DataFrame with columns: timestamp, bulk_tx_cost_enduse
    """
    if utility not in VALID_PJM_UTILITIES:
        raise ValueError(
            f"Invalid utility '{utility}'. Valid: {sorted(VALID_PJM_UTILITIES)}"
        )

    # Step 1: Blended NITS rate
    nits_zone = UTILITY_TO_NITS_ZONE[utility]
    nits_df = load_nits_rates(nits_csv_path)
    blended_rate = compute_blended_nits_rate(nits_df, nits_zone, year)

    print("\n── PJM Bulk TX MC Configuration ──")
    print(f"  Utility:        {utility}")
    print(f"  NITS zone:      {nits_zone}")
    print(f"  Year:           {year}")
    print(f"  Blended rate:   ${blended_rate:.4f}/kW-yr")
    print(f"  K peak hours:   {k_peak_hours}")
    print("  Method:         PCAF load-share, full-year (E3 ICC-VDER)")

    # Step 2: Load hourly demand
    print("\n── Loading hourly demand ──")
    load_df = load_hourly_demand(utility, year, s3_base, storage_options)
    print(f"  Loaded {load_df.height} hours")

    # Strip timezone for joining with naive CAIRO timestamps
    ts_dtype = load_df.schema["timestamp"]
    if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
        load_df = load_df.with_columns(
            pl.col("timestamp").dt.replace_time_zone(None).alias("timestamp")
        )

    # Step 3: PCAF allocation on full-year load
    print("\n── PCAF Allocation ──")
    peak_hours_df = allocate_pcaf(
        load_df=load_df,
        annual_cost_kw_year=blended_rate,
        k_peak_hours=k_peak_hours,
    )
    print(f"  Allocated to {peak_hours_df.height} hours")

    # Step 4: Expand to full 8760
    ref_8760 = build_cairo_8760_timestamps(year)
    output = (
        ref_8760.join(peak_hours_df, on="timestamp", how="left")
        .with_columns(
            pl.col("bulk_tx_cost_enduse").fill_null(0.0).alias("bulk_tx_cost_enduse")
        )
        .sort("timestamp")
    )

    if output.height != 8760:
        raise ValueError(f"Output has {output.height} rows, expected 8760")

    # Step 5: Validate
    actual_sum = float(output["bulk_tx_cost_enduse"].sum())
    error = abs(actual_sum - blended_rate)
    error_pct = (error / blended_rate * 100) if blended_rate > 0 else 0.0

    print("\n── Validation ──")
    print(f"  Expected annual sum: ${blended_rate:.6f}/kW-yr")
    print(f"  Actual annual sum:   ${actual_sum:.6f}/kW-yr")
    print(f"  Error:               ${error:.8f} ({error_pct:.6f}%)")

    n_nonzero = output.filter(pl.col("bulk_tx_cost_enduse") > 0).height
    print(f"  Non-zero hours:      {n_nonzero} (expected {k_peak_hours})")

    if error_pct > _VALIDATION_TOLERANCE_PCT:
        raise ValueError(
            f"PCAF validation failed: error {error_pct:.6f}% exceeds {_VALIDATION_TOLERANCE_PCT}%. "
            f"Expected ${blended_rate:.6f}/kW-yr, got ${actual_sum:.6f}/kW-yr."
        )
    if n_nonzero != k_peak_hours:
        raise ValueError(
            f"Expected exactly {k_peak_hours} non-zero hours, got {n_nonzero}"
        )

    print("  ✓ Validation PASSED")

    return output.select("timestamp", "bulk_tx_cost_enduse")


# ── Output helpers ────────────────────────────────────────────────────────────


def print_summary(output_df: pl.DataFrame) -> None:
    """Print top-10 hours and summary statistics."""
    print("\n" + "=" * 60)
    print("SAMPLE: Top 10 hours by bulk Tx cost")
    print("=" * 60)
    print(output_df.sort("bulk_tx_cost_enduse", descending=True).head(10))

    avg_cost = cast(float, output_df["bulk_tx_cost_enduse"].mean())
    max_cost = cast(float, output_df["bulk_tx_cost_enduse"].max())
    n_nonzero = output_df.filter(pl.col("bulk_tx_cost_enduse") > 0).height
    total_cost = float(output_df["bulk_tx_cost_enduse"].sum())
    print("\nOutput summary:")
    print(f"  Total annual: ${total_cost:.4f}/kW-yr")
    print(f"  Avg (all hrs): ${avg_cost:.6f}/kWh")
    print(f"  Max (peak hr): ${max_cost:.6f}/kWh")
    print(f"  Non-zero hours: {n_nonzero} out of 8760")


def save_output(
    output_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    """Write bulk TX MC parquet to S3 (hive-style utility/year partition)."""
    output_path = (
        f"{output_s3_base.rstrip('/')}/utility={utility}/year={year}/data.parquet"
    )
    output_df.write_parquet(output_path, storage_options=storage_options)
    print(f"\n✓ Saved bulk TX MC to {output_path}")
    print(f"  Rows: {len(output_df):,}")
