"""Generate utility-level bulk transmission marginal costs using SCR allocation.

This script allocates bulk transmission marginal costs (v_z, $/kW-yr) derived from
NYISO AC Transmission and LI Export studies to hourly price signals using SCR
(Seasonal Coincident Reserve) top-40-per-season peak hours.

Bulk transmission is treated as a delivery charge (combined with distribution MC in
`rate_design/hp_rates/run_scenario.py`) and active in delivery-only CAIRO runs.

The allocation method (two-level):
    Level 1 — between seasons (peak surplus above SCR threshold):
        τ_min = min(τ_summer, τ_winter); φ_s = (peak_s − τ_min) / ((peak_s − τ_min) + (peak_w − τ_min))
        φ_w = 1 − φ_s
    Level 2 — within each season (exceedance above the (N+1)th hour):
        threshold_season = load of the (N+1)th highest hour in that season
        exc_h = max(load_h − threshold_season, 0)  for h in top-N SCR hours
        w_h = exc_h / Σ exc_h  (sums to 1 within season)
    Hourly allocation:
        pi_h = v_z × φ_season × w_h   [$/kW-yr]
        bulk_tx_cost_enduse_h = pi_h × 1000   [$/MWh]
    Global weights sum to 1: Σ (φ_s × Σw_s + φ_w × Σw_w) = φ_s + φ_w = 1.

Input data:
    - Derived v_z table:
        s3://data.sb/nyiso/bulk_tx/ny_bulk_tx_values.csv
        Schema: gen_capacity_zone, v_low_kw_yr, v_mid_kw_yr, v_high_kw_yr, v_isotonic_kw_yr
    - Utility zone mapping CSV (utility → gen_capacity_zone, capacity_weight):
        s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv
    - EIA utility-level hourly loads (for SCR peak identification):
        s3://data.sb/eia/hourly_demand/utilities/region=nyiso/utility={name}/year={YYYY}/month={M}/data.parquet

Output:
    s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/utility={utility}/year={YYYY}/data.parquet
    Schema: timestamp (datetime), bulk_tx_cost_enduse ($/MWh), 8760 rows

Usage:
    # Inspect results (no upload)
    uv run python utils/pre/generate_bulk_tx_mc.py \\
        --utility nyseg --year 2025

    # Upload to S3
    uv run python utils/pre/generate_bulk_tx_mc.py \\
        --utility nyseg --year 2025 --upload

    # Use P25 quantile instead of default P50
    uv run python utils/pre/generate_bulk_tx_mc.py \\
        --utility nyseg --year 2025 --v-z-quantile low

    # Override default paths
    uv run python utils/pre/generate_bulk_tx_mc.py \\
        --utility coned --year 2025 \\
        --zone-mapping-path data/nyiso/zone_mapping/csv/ny_utility_zone_mapping.csv \\
        --v-z-table-path data/nyiso/bulk_tx/csv/ny_bulk_tx_values.csv
"""

from __future__ import annotations

import argparse
import io
from pathlib import Path

import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.pre.generate_utility_supply_mc import (
    build_cairo_8760_timestamps,
    load_zone_mapping,
)
from utils.pre.generate_utility_tx_dx_mc import (
    load_utility_load_profile,
    normalize_load_to_cairo_8760,
)
from utils.pre.season_config import (
    DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
    derive_summer_months,
    get_utility_periods_yaml_path,
    load_winter_months_from_periods,
    parse_months_arg,
    resolve_winter_summer_months,
)

# ── Default S3 paths ─────────────────────────────────────────────────────────

DEFAULT_VZ_TABLE_PATH = "s3://data.sb/nyiso/bulk_tx/ny_bulk_tx_values.csv"
DEFAULT_ZONE_MAPPING_PATH = (
    "s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv"
)
DEFAULT_UTILITY_LOADS_S3_BASE = "s3://data.sb/eia/hourly_demand/utilities/"
DEFAULT_OUTPUT_S3_BASE = "s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/"

# SCR allocation parameters
N_SCR_HOURS_PER_SEASON = 40

# Default season: matches all NY utility periods YAMLs (Oct–Mar winter, Apr–Sep summer).
# Override via --winter-months or --periods-yaml.
#
# Note: NYISO capability periods use May–Oct / Nov–Apr, but rate design seasons
# drive what the tariff charges — SCR peak identification should match the rate
# seasons so cost signals and rate structure are coherent.
DEFAULT_SCR_WINTER_MONTHS: tuple[int, ...] = DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS

# Valid NY utilities
VALID_UTILITIES = frozenset({"cenhud", "coned", "nimo", "nyseg", "or", "rge", "psegli"})

# Valid v_z quantile columns
VZ_QUANTILE_MAP: dict[str, str] = {
    "low": "v_low_kw_yr",
    "mid": "v_mid_kw_yr",
    "high": "v_high_kw_yr",
    "isotonic": "v_isotonic_kw_yr",
}


# ── v_z lookup ────────────────────────────────────────────────────────────────


def load_vz_table(
    path: str,
) -> pl.DataFrame:
    """Load the derived v_z table CSV.

    Args:
        path: Local or S3 path to ny_bulk_tx_values.csv.

    Returns:
        DataFrame with columns: gen_capacity_zone, v_low_kw_yr, v_mid_kw_yr,
        v_high_kw_yr, v_isotonic_kw_yr.
    """
    if path.startswith("s3://"):
        s3_path = S3Path(path)
        csv_bytes = s3_path.read_bytes()
        df = pl.read_csv(io.BytesIO(csv_bytes))
    else:
        df = pl.read_csv(path)

    required = {"gen_capacity_zone"} | set(VZ_QUANTILE_MAP.values())
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"v_z table CSV missing columns: {sorted(missing)}")

    print(f"Loaded v_z table with {len(df)} rows from {path}")
    print(df)
    return df


def resolve_utility_vz(
    mapping_df: pl.DataFrame,
    vz_df: pl.DataFrame,
    utility: str,
    quantile: str,
) -> float:
    """Resolve the weighted v_z value ($/kW-yr) for a utility.

    For single-zone utilities, returns the v_z directly.
    For multi-zone utilities (e.g. ConEd: 87% NYC + 13% LHV), returns the
    capacity-weighted blend.

    Args:
        mapping_df: Full zone mapping DataFrame.
        vz_df: v_z table DataFrame.
        utility: Utility name.
        quantile: Quantile to select (low/mid/high/isotonic).

    Returns:
        Weighted v_z in $/kW-yr.
    """
    vz_col = VZ_QUANTILE_MAP[quantile]

    # Get unique (gen_capacity_zone, capacity_weight) pairs for this utility.
    # gen_capacity_zone is the same four-zone grouping (ROS, LHV, NYC, LI) used
    # by the generation capacity MC — bulk TX uses the same locality mapping.
    utility_rows = mapping_df.filter(pl.col("utility") == utility)
    if utility_rows.is_empty():
        available = sorted(mapping_df["utility"].unique().to_list())
        raise ValueError(
            f"Utility '{utility}' not found in zone mapping. Available: {available}"
        )

    zone_weights = utility_rows.select("gen_capacity_zone", "capacity_weight").unique()

    # Join with v_z table on gen_capacity_zone
    joined = zone_weights.join(
        vz_df.select("gen_capacity_zone", vz_col),
        on="gen_capacity_zone",
        how="left",
    )

    # Check for missing v_z values
    missing = joined.filter(pl.col(vz_col).is_null())
    if not missing.is_empty():
        missing_zones = missing["gen_capacity_zone"].to_list()
        raise ValueError(
            f"No v_z value found for gen_capacity_zone(s): {missing_zones}. "
            f"Check that ny_bulk_tx_values.csv covers all gen_capacity_zones."
        )

    # Compute weighted v_z
    v_z = float((joined[vz_col] * joined["capacity_weight"]).sum())

    if v_z <= 0:
        raise ValueError(
            f"Resolved v_z for {utility} is non-positive: {v_z:.4f} $/kW-yr. "
            f"All utilities should have positive bulk Tx costs."
        )

    print(f"\nv_z resolution for {utility} (quantile={quantile}):")
    for row in joined.iter_rows(named=True):
        print(
            f"  gen_capacity_zone={row['gen_capacity_zone']}, "
            f"weight={row['capacity_weight']:.2f}, "
            f"v_z={row[vz_col]:.2f} $/kW-yr"
        )
    print(f"  → Weighted v_z = {v_z:.4f} $/kW-yr")

    return v_z


# ── SCR allocation ────────────────────────────────────────────────────────────


def identify_scr_hours(
    load_df: pl.DataFrame,
    n_hours_per_season: int = N_SCR_HOURS_PER_SEASON,
    winter_months: list[int] | None = None,
) -> pl.DataFrame:
    """Identify SCR (top-N-per-season) hours from a utility load profile.

    Season boundaries are determined by ``winter_months`` (the complement
    becomes summer).  Pass the same ``winter_months`` used in the utility's
    rate design (from its periods YAML) so that SCR peak identification is
    coherent with the seasonal rate structure.

    Args:
        load_df: Load profile with columns: timestamp, load_mw.
        n_hours_per_season: Number of peak hours per season (default: 40).
        winter_months: 1-indexed month numbers for winter.  Defaults to
            ``DEFAULT_SCR_WINTER_MONTHS`` (Oct–Mar, matching all current NY
            utility periods YAMLs).

    Returns:
        DataFrame with columns: timestamp, load_mw, season, is_scr.
        Exactly 8760 rows.
    """
    if winter_months is None:
        winter_months = list(DEFAULT_SCR_WINTER_MONTHS)
    summer_months = derive_summer_months(winter_months)

    # Add season column
    result = load_df.with_columns(
        pl.when(pl.col("timestamp").dt.month().is_in(summer_months))
        .then(pl.lit("summer"))
        .otherwise(pl.lit("winter"))
        .alias("season")
    )

    # For each season, identify top N hours by load
    summer = result.filter(pl.col("season") == "summer").sort(
        "load_mw", descending=True
    )
    winter = result.filter(pl.col("season") == "winter").sort(
        "load_mw", descending=True
    )

    n_summer = summer.height
    n_winter = winter.height
    if n_summer < n_hours_per_season:
        raise ValueError(
            f"Summer has only {n_summer} hours, need at least {n_hours_per_season}"
        )
    if n_winter < n_hours_per_season:
        raise ValueError(
            f"Winter has only {n_winter} hours, need at least {n_hours_per_season}"
        )

    # Get top-N timestamps per season
    summer_scr_ts = summer.head(n_hours_per_season)["timestamp"].to_list()
    winter_scr_ts = winter.head(n_hours_per_season)["timestamp"].to_list()

    # Mark SCR hours
    result = result.with_columns(
        (
            pl.col("timestamp").is_in(summer_scr_ts)
            | pl.col("timestamp").is_in(winter_scr_ts)
        ).alias("is_scr")
    )

    # Validate counts
    n_scr_summer = result.filter(
        pl.col("is_scr") & (pl.col("season") == "summer")
    ).height
    n_scr_winter = result.filter(
        pl.col("is_scr") & (pl.col("season") == "winter")
    ).height
    n_scr_total = result.filter(pl.col("is_scr")).height

    if n_scr_summer != n_hours_per_season:
        raise ValueError(
            f"Expected {n_hours_per_season} summer SCR hours, got {n_scr_summer}"
        )
    if n_scr_winter != n_hours_per_season:
        raise ValueError(
            f"Expected {n_hours_per_season} winter SCR hours, got {n_scr_winter}"
        )
    if n_scr_total != 2 * n_hours_per_season:
        raise ValueError(
            f"Expected {2 * n_hours_per_season} total SCR hours, got {n_scr_total}"
        )

    # Print SCR hour distribution by month
    scr_months = (
        result.filter(pl.col("is_scr"))
        .with_columns(pl.col("timestamp").dt.month().alias("month"))
        .group_by("season", "month")
        .len()
        .sort("season", "month")
    )
    print(
        f"\nSCR hour distribution ({n_hours_per_season} per season):"
        f"\n  winter months: {sorted(winter_months)}"
        f"\n  summer months: {sorted(summer_months)}"
    )
    for row in scr_months.iter_rows(named=True):
        print(f"  {row['season']:>8s} month {row['month']:2d}: {row['len']:3d} hours")

    # Print top-5 SCR hours per season
    for season_name in ["summer", "winter"]:
        top5 = (
            result.filter(pl.col("is_scr") & (pl.col("season") == season_name))
            .sort("load_mw", descending=True)
            .head(5)
        )
        print(f"\n  Top 5 {season_name} SCR hours:")
        for row in top5.iter_rows(named=True):
            print(f"    {row['timestamp']}  load={row['load_mw']:,.1f} MW")

    return result


def allocate_bulk_tx_to_hours(
    load_with_scr: pl.DataFrame,
    v_z: float,
    n_hours_per_season: int = N_SCR_HOURS_PER_SEASON,
) -> pl.DataFrame:
    """Allocate v_z ($/kW-yr) using two-level seasonal PoP allocation.

    Level 1 — between seasons (peak surplus above SCR threshold):
        τ_min = min(τ_summer, τ_winter)   [lower of the two SCR thresholds]
        φ_s = (peak_s − τ_min) / ((peak_s − τ_min) + (peak_w − τ_min))
        φ_w = 1 − φ_s
        Intuition: τ_min is the capacity-triggering floor; only load above it is
        "capacity-driving". Summer peaks far exceed τ_min; winter barely clears it.

    Level 2 — within each season (exceedance above the (N+1)th hour):
        threshold = load of the (n_hours_per_season + 1)th highest hour in season
        exc_h = max(load_h − threshold, 0)  for h in top-N SCR hours
        w_h = exc_h / Σ exc_h  (sums to 1 within season)

    Hourly allocation:
        pi_h = v_z × φ_season × w_h   [$/kW-yr]
        bulk_tx_cost_enduse_h = pi_h × 1000   [$/MWh]

    Global weights sum to 1 by construction:
        Σ_all w_h = φ_s × Σ_summer w_h + φ_w × Σ_winter w_h = φ_s + φ_w = 1.

    Args:
        load_with_scr: DataFrame with columns: timestamp, load_mw, season, is_scr.
            (Output of identify_scr_hours.)
        v_z: Bulk transmission marginal cost in $/kW-yr.
        n_hours_per_season: SCR window size; the (n+1)th hour sets the exceedance
            threshold for each season (default: N_SCR_HOURS_PER_SEASON).

    Returns:
        DataFrame with columns: timestamp, bulk_tx_cost_enduse ($/MWh).
    """
    # One group_by pass: seasonal peak and SCR threshold (the (N+1)th highest load).
    season_stats = load_with_scr.group_by("season").agg(
        pl.col("load_mw").max().alias("peak_mw"),
        pl.col("load_mw").top_k(n_hours_per_season + 1).min().alias("threshold_mw"),
    )
    peaks = dict(zip(season_stats["season"], season_stats["peak_mw"]))
    thresholds = dict(zip(season_stats["season"], season_stats["threshold_mw"]))

    # φ: peak surplus above the minimum SCR threshold (the capacity-triggering floor).
    # τ_min is almost always the winter threshold; summer peaks far exceed it, winter barely clears it.
    tau_min = min(thresholds.values())
    summer_surplus = peaks["summer"] - tau_min
    winter_surplus = peaks["winter"] - tau_min
    if summer_surplus <= 0 or winter_surplus <= 0:
        raise ValueError(
            f"A seasonal peak does not exceed τ_min={tau_min:.1f} MW: "
            f"summer surplus={summer_surplus:.1f}, winter surplus={winter_surplus:.1f}"
        )
    total_surplus = summer_surplus + winter_surplus
    phi_s = summer_surplus / total_surplus
    phi_w = 1.0 - phi_s

    # Attach φ and threshold per row so the weight expression needs no Python branches.
    season_phi = season_stats.with_columns(
        pl.when(pl.col("season") == "summer").then(phi_s).otherwise(phi_w).alias("phi")
    ).select("season", "threshold_mw", "phi")

    # Exceedance: max(load − threshold, 0) for SCR hours, 0 elsewhere.
    with_exc = load_with_scr.join(season_phi, on="season").with_columns(
        pl.when(pl.col("is_scr"))
        .then((pl.col("load_mw") - pl.col("threshold_mw")).clip(lower_bound=0.0))
        .otherwise(0.0)
        .alias("exceedance")
    )

    # Within-season sums for normalisation; validate before joining back.
    exc_sums = with_exc.group_by("season").agg(
        pl.col("exceedance").sum().alias("exc_sum")
    )
    for season, exc_sum in zip(exc_sums["season"], exc_sums["exc_sum"]):
        if exc_sum <= 0:
            raise ValueError(
                f"All {season} SCR loads equal the threshold "
                f"({thresholds[season]:.1f} MW); cannot compute exceedance weights."
            )

    # w_h = (exc_h / Σ_season exc) × φ_season  →  global weights sum to 1.
    result = (
        with_exc.join(exc_sums, on="season")
        .with_columns(
            (pl.col("exceedance") / pl.col("exc_sum") * pl.col("phi")).alias("weight")
        )
        .with_columns((pl.col("weight") * v_z * 1000.0).alias("bulk_tx_cost_enduse"))
    )

    weight_sum = float(result["weight"].sum())
    if abs(weight_sum - 1.0) > 1e-4:
        raise ValueError(
            f"Global weights sum to {weight_sum:.6f}, expected 1.0 (tolerance 0.01%)"
        )

    n_nonzero = result.filter(pl.col("bulk_tx_cost_enduse") > 0).height
    avg_nonzero = float(
        result.filter(pl.col("bulk_tx_cost_enduse") > 0)["bulk_tx_cost_enduse"].mean()
    )  # type: ignore[arg-type]
    max_cost = float(result["bulk_tx_cost_enduse"].max())  # type: ignore[arg-type]
    print(f"\nSeasonal φ via peak surplus above τ_min={tau_min:,.1f} MW (SCR floor):")
    print(
        f"  Summer: peak={peaks['summer']:,.1f} MW  surplus={summer_surplus:,.1f} MW  →  φ_s = {phi_s:.4f}"
    )
    print(
        f"  Winter: peak={peaks['winter']:,.1f} MW  surplus={winter_surplus:,.1f} MW  →  φ_w = {phi_w:.4f}"
    )
    print(
        f"\nAllocation:  v_z={v_z:.4f} $/kW-yr  |  non-zero={n_nonzero} hrs  |  weight_sum={weight_sum:.6f}"
    )
    print(f"  avg={avg_nonzero:.2f} $/MWh  max={max_cost:.2f} $/MWh")

    return result.select("timestamp", "bulk_tx_cost_enduse")


# ── Output assembly ──────────────────────────────────────────────────────────


def prepare_output(
    allocated_df: pl.DataFrame,
    year: int,
) -> pl.DataFrame:
    """Prepare bulk Tx MC DataFrame for saving (8760 rows).

    Joins allocated costs onto a reference 8760 timestamp index, filling
    non-SCR hours with 0.

    Args:
        allocated_df: Allocated costs (timestamp, bulk_tx_cost_enduse in $/MWh).
        year: Target year.

    Returns:
        DataFrame with 8760 rows: timestamp, bulk_tx_cost_enduse ($/MWh).
    """
    ref_8760 = build_cairo_8760_timestamps(year)

    # Truncate to hour
    hourly = allocated_df.with_columns(
        pl.col("timestamp").dt.truncate("1h").alias("timestamp")
    )

    # Deduplicate (e.g. DST)
    hourly = (
        hourly.group_by("timestamp")
        .agg(pl.col("bulk_tx_cost_enduse").mean())
        .sort("timestamp")
    )

    # Join onto reference index
    output = ref_8760.join(hourly, on="timestamp", how="left")

    # Fill missing with 0
    output = output.with_columns(
        pl.col("bulk_tx_cost_enduse").fill_null(0.0).alias("bulk_tx_cost_enduse"),
    )

    output = output.select("timestamp", "bulk_tx_cost_enduse")

    if output.height != 8760:
        raise ValueError(f"Output has {output.height} rows, expected 8760")

    if output.filter(pl.col("bulk_tx_cost_enduse").is_null()).height > 0:
        raise ValueError("Output has null values in bulk_tx_cost_enduse")

    return output


def save_output(
    output_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    """Write bulk Tx MC parquet to S3 with Hive-style partitioning.

    Path: {output_s3_base}/utility={utility}/year={year}/data.parquet

    Args:
        output_df: Bulk Tx MC DataFrame (timestamp, bulk_tx_cost_enduse).
        utility: Utility name.
        year: Target year.
        output_s3_base: S3 base path.
        storage_options: AWS storage options.
    """
    output_s3_base = output_s3_base.rstrip("/") + "/"

    # Write directly to the partition path with data.parquet filename
    # (matching the pattern used by generate_utility_tx_dx_mc.py)
    output_path = f"{output_s3_base}utility={utility}/year={year}/data.parquet"
    output_df.write_parquet(
        output_path,
        storage_options=storage_options,
    )

    print(f"\n✓ Saved bulk Tx MC to {output_path}")
    print(f"  Rows: {len(output_df):,}")
    print(f"  Columns: {', '.join(output_df.columns)}")


# ── CLI ───────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate utility-level bulk transmission marginal costs "
            "using SCR (top-40-per-season) allocation."
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
        help="Target year for bulk Tx MC generation (e.g. 2025).",
    )
    parser.add_argument(
        "--load-year",
        type=int,
        default=None,
        help=(
            "Year of load profile to use for SCR peak identification "
            "(defaults to --year). Use to apply one year's v_z to another "
            "year's load shape."
        ),
    )
    parser.add_argument(
        "--v-z-table-path",
        type=str,
        default=DEFAULT_VZ_TABLE_PATH,
        help=f"Path to v_z table CSV (default: {DEFAULT_VZ_TABLE_PATH}).",
    )
    parser.add_argument(
        "--zone-mapping-path",
        type=str,
        default=DEFAULT_ZONE_MAPPING_PATH,
        help=f"Path to zone mapping CSV (default: {DEFAULT_ZONE_MAPPING_PATH}).",
    )
    parser.add_argument(
        "--v-z-quantile",
        type=str,
        default="mid",
        choices=sorted(VZ_QUANTILE_MAP.keys()),
        help="v_z quantile to use (default: mid = P50).",
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
        "--scr-hours-per-season",
        type=int,
        default=N_SCR_HOURS_PER_SEASON,
        help=f"SCR hours per season (default: {N_SCR_HOURS_PER_SEASON}).",
    )
    parser.add_argument(
        "--periods-yaml",
        type=str,
        default=None,
        help=(
            "Path to a periods YAML containing `winter_months`. "
            "When omitted, resolves rate_design/hp_rates/ny/config/periods/<utility>.yaml. "
            "Overridden by --winter-months if both are provided."
        ),
    )
    parser.add_argument(
        "--winter-months",
        type=str,
        default=None,
        help=(
            "Comma-separated 1-indexed winter month numbers (e.g. 10,11,12,1,2,3). "
            "Overrides --periods-yaml. "
            f"Default (from periods YAML or fallback): {sorted(DEFAULT_SCR_WINTER_MONTHS)}."
        ),
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
    year = args.year
    load_year = args.load_year if args.load_year else year
    n_scr = args.scr_hours_per_season

    # ── Resolve season boundaries ──────────────────────────────────────────
    # Priority: --winter-months > --periods-yaml > utility periods YAML > default
    project_root = Path(__file__).resolve().parents[2]
    periods_yaml_path = (
        Path(args.periods_yaml)
        if args.periods_yaml
        else get_utility_periods_yaml_path(
            project_root=project_root,
            state="ny",
            utility=utility,
        )
    )

    if periods_yaml_path.exists():
        yaml_winter = load_winter_months_from_periods(
            periods_yaml_path,
            default_winter_months=DEFAULT_SCR_WINTER_MONTHS,
        )
    else:
        yaml_winter = list(DEFAULT_SCR_WINTER_MONTHS)

    winter_months, summer_months = resolve_winter_summer_months(
        parse_months_arg(args.winter_months) if args.winter_months else None,
        default_winter_months=yaml_winter,
    )

    print("=" * 60)
    print("BULK TRANSMISSION MARGINAL COST GENERATION")
    print("=" * 60)
    print(f"  Utility:          {utility}")
    print(f"  Year:             {year}")
    print(f"  Load year:        {load_year} (for SCR peak ID)")
    print(f"  v_z quantile:     {args.v_z_quantile}")
    print(f"  SCR hours/season: {n_scr}")
    print(f"  Winter months:    {winter_months}")
    print(f"  Summer months:    {summer_months}")
    src = args.periods_yaml or (
        str(periods_yaml_path) if periods_yaml_path.exists() else "default"
    )
    print(f"  Season source:    {src}")
    print(f"  Upload to S3:     {'Yes' if args.upload else 'No (inspect only)'}")
    print("=" * 60)

    # ── 1. Load zone mapping and v_z table ────────────────────────────────
    print("\n── Zone Mapping & v_z Table ──")
    mapping_df = load_zone_mapping(args.zone_mapping_path, storage_options)
    vz_df = load_vz_table(args.v_z_table_path)

    # ── 2. Resolve v_z for this utility ───────────────────────────────────
    v_z = resolve_utility_vz(mapping_df, vz_df, utility, args.v_z_quantile)

    # ── 3. Load utility load profile ──────────────────────────────────────
    print("\n── Utility Load Profile ──")
    print(f"Loading utility load profile for {utility}, year {load_year}...")
    utility_load_df = load_utility_load_profile(
        args.utility_loads_s3_base,
        "nyiso",
        load_year,
        utility,
        storage_options,
    )
    utility_load_df = normalize_load_to_cairo_8760(utility_load_df, utility, load_year)

    # ── 4. Identify SCR hours ─────────────────────────────────────────────
    print("\n── SCR Hour Identification ──")
    load_with_scr = identify_scr_hours(utility_load_df, n_scr, winter_months)

    # ── 5. Allocate v_z to hours ──────────────────────────────────────────
    print("\n── Bulk Tx Allocation ──")
    allocated_df = allocate_bulk_tx_to_hours(load_with_scr, v_z, n_scr)

    # ── 6. Prepare output (8760 rows) ─────────────────────────────────────
    print("\n── Output Preparation ──")
    output_df = prepare_output(allocated_df, year)

    # ── 7. Display sample ─────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("SAMPLE: Top 10 hours by bulk Tx cost")
    print("=" * 60)
    sample = output_df.sort("bulk_tx_cost_enduse", descending=True).head(10)
    print(sample)

    avg_cost = float(output_df["bulk_tx_cost_enduse"].mean())  # type: ignore[arg-type]
    max_cost = float(output_df["bulk_tx_cost_enduse"].max())  # type: ignore[arg-type]
    n_nonzero = output_df.filter(pl.col("bulk_tx_cost_enduse") > 0).height
    print("\nOutput summary:")
    print(f"  avg = ${avg_cost:.2f}/MWh, max = ${max_cost:.2f}/MWh")
    print(f"  {n_nonzero} non-zero hours out of 8760")

    # ── 8. Save ───────────────────────────────────────────────────────────
    if args.upload:
        save_output(output_df, utility, year, args.output_s3_base, storage_options)
        print("\n" + "=" * 60)
        print("✓ Bulk transmission MC generation completed and uploaded")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("✓ Bulk transmission MC generation completed (inspect only)")
        print("⚠️  No data uploaded to S3 (use --upload flag to enable)")
        print("=" * 60)


if __name__ == "__main__":
    main()
