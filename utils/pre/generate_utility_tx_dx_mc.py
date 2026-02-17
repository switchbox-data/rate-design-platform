"""Allocate marginal transmission and distribution costs to hourly price signals.

This script implements the diluted marginal cost allocation methodology using
the Probability of Peak (PoP) method to allocate $/kW-yr costs to $/kWh hourly
price signals.

Input:
    - Utility hourly load profile: s3://data.sb/eia/hourly_demand/utilities/region=<iso_region>/utility=X/year=YYYY/month=M/data.parquet
    - Marginal cost table CSV ($/kW-yr)
    - Target year (2026-2035)

Output: s3://data.sb/switchbox/marginal_costs/ny/utility=X/year=YYYY/data.parquet
Output partitions written as:
    - NY default base: s3://data.sb/switchbox/marginal_costs/ny/
    - RI default base: s3://data.sb/switchbox/marginal_costs/ri/
    - Partition path: region=<iso_region>/utility=X/year=YYYY/data.parquet

Usage:
    # Inspect results (no upload) - uses 2026 MC with 2026 loads
    python generate_utility_tx_dx_MC.py --utility nyseg --mc-year 2026 --mc-table-path data/marginal_costs/ny_marginal_costs_2026_2035.csv

    # Apply 2026 MC to 2024 loads
    python generate_utility_tx_dx_MC.py --utility nyseg --mc-year 2026 --load-year 2024 --mc-table-path data/marginal_costs/ny_marginal_costs_2026_2035.csv

    # Upload to S3
    python generate_utility_tx_dx_MC.py --utility nyseg --mc-year 2026 --mc-table-path data/marginal_costs/ny_marginal_costs_2026_2035.csv --upload
"""

import argparse
import io
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import (
    get_aws_storage_options,
    get_state_config,
)


def load_utility_load_profile(
    s3_base: str,
    iso_region: str,
    year_load: int,
    utility: str,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load utility 8760 load profile from S3.

    Reads from Hive-style partitioned structure:
    region=<iso_region>/utility=X/year=YYYY/month=M/data.parquet

    Args:
        s3_base: Base S3 path for utility loads (e.g., s3://data.sb/eia/hourly_demand/utilities)
        iso_region: ISO region partition key (nyiso/isone)
        year_load: Year of the load profile to use
        utility: Utility name
        storage_options: Polars S3 storage options with AWS bucket region

    Returns:
        DataFrame with columns: timestamp, utility, load_mw
    """
    s3_base = s3_base.rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            s3_base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(pl.col("region") == iso_region)
        .filter(pl.col("utility") == utility)
        .filter(pl.col("year") == year_load)
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from utility load collect()")
    df = collected
    if df.is_empty():
        raise FileNotFoundError(
            "Utility load profile not found for "
            f"region={iso_region}, utility={utility}, year={year_load} under {s3_base}"
        )

    print(f"Loaded {len(df):,} hourly load records for {utility} (year {year_load})")
    return df


def normalize_load_to_cairo_8760(
    load_df: pl.DataFrame, utility: str, year_load: int
) -> pl.DataFrame:
    """Normalize utility load profile to Cairo-compatible 8760 hours.

    Cairo leap-year handling drops Dec 31 when Feb 29 is present to keep 8760 rows.
    This function mirrors that behavior and collapses duplicate wall-clock timestamps
    before interpolation.

    Args:
        load_df: Utility load DataFrame with at least timestamp/load_mw columns
        utility: Utility short code (e.g., nyseg)
        year_load: Target load year to normalize

    Returns:
        DataFrame with exactly 8760 rows and columns: timestamp, utility, load_mw
    """
    if "timestamp" not in load_df.columns or "load_mw" not in load_df.columns:
        raise ValueError("load_df must contain timestamp and load_mw columns")

    print("\nNormalizing load profile to Cairo-compatible 8760...")

    cols = ["timestamp", "load_mw"]
    if "utility" in load_df.columns:
        cols.append("utility")
    df = load_df.select(cols)

    # Convert timezone-aware timestamps to local wall-clock naive timestamps.
    ts_dtype = df.schema["timestamp"]
    if isinstance(ts_dtype, pl.Datetime) and ts_dtype.time_zone is not None:
        df = df.with_columns(
            pl.col("timestamp")
            .dt.convert_time_zone("America/New_York")
            .dt.replace_time_zone(None)
            .alias("timestamp")
        )

    # Keep only requested load year.
    df = df.filter(pl.col("timestamp").dt.year() == year_load)
    if df.is_empty():
        raise ValueError(f"No load rows found for load_year={year_load}")

    # Cairo leap-year rule: if Feb 29 exists, drop Dec 31.
    has_feb29 = df.select(
        ((pl.col("timestamp").dt.month() == 2) & (pl.col("timestamp").dt.day() == 29))
        .any()
        .alias("has_feb29")
    ).item()
    if has_feb29:
        print(
            "  Leap-year pattern detected (Feb 29 present); dropping Dec 31 to match Cairo."
        )
        df = df.filter(
            ~(
                (pl.col("timestamp").dt.month() == 12)
                & (pl.col("timestamp").dt.day() == 31)
            )
        )

    # Collapse duplicate wall-clock timestamps (e.g., DST fallback) using mean load.
    duplicate_count = df.height - df.select(pl.col("timestamp").n_unique()).item()
    if duplicate_count > 0:
        print(
            f"  Collapsing {duplicate_count} duplicate timestamp row(s) with mean load."
        )
    agg_exprs = [pl.col("load_mw").mean().alias("load_mw")]
    if "utility" in df.columns:
        agg_exprs.append(pl.col("utility").first().alias("utility"))
    df = df.group_by("timestamp").agg(agg_exprs).sort("timestamp")

    # Build expected 8760 index for this year.
    start = datetime(year_load, 1, 1, 0, 0, 0)
    end = datetime(year_load, 12, 31, 23, 0, 0)
    expected = []
    cur = start
    while cur <= end:
        expected.append(cur)
        cur += timedelta(hours=1)
    if has_feb29:
        expected = [t for t in expected if not (t.month == 12 and t.day == 31)]
    expected_df = pl.DataFrame({"timestamp": expected})

    # Reindex and fill any missing hours.
    df = expected_df.join(df, on="timestamp", how="left").sort("timestamp")
    missing_before_fill = df.filter(pl.col("load_mw").is_null()).height
    if missing_before_fill > 0:
        print(f"  Filling {missing_before_fill} missing hour(s) via interpolation.")
    df = df.with_columns(
        pl.col("load_mw").interpolate().forward_fill().backward_fill().alias("load_mw")
    )
    if "utility" in df.columns:
        df = df.with_columns(pl.col("utility").fill_null(utility).alias("utility"))
    else:
        df = df.with_columns(pl.lit(utility).alias("utility"))

    # Final hard checks.
    if df.height != 8760:
        raise ValueError(
            f"Cairo 8760 normalization failed: expected 8760 rows, got {df.height}"
        )
    if df.select(pl.col("timestamp").n_unique()).item() != 8760:
        raise ValueError("Cairo 8760 normalization failed: timestamps are not unique")
    if df.filter(pl.col("load_mw").is_null()).height > 0:
        raise ValueError("Cairo 8760 normalization failed: load_mw contains nulls")

    print("  ✓ Normalized to 8760 hourly rows (Cairo-compatible)")
    return df.select(["timestamp", "utility", "load_mw"])


def load_marginal_cost_table(mc_table_path: str) -> pl.DataFrame:
    """Load marginal cost table from CSV.

    Args:
        mc_table_path: Path to CSV file (local or S3)

    Returns:
        DataFrame with columns: Utility, Year, Upstream, Distribution Substation,
                                Primary Feeder, Total MC
    """
    if mc_table_path.startswith("s3://"):
        s3_path = S3Path(mc_table_path)
        csv_bytes = s3_path.read_bytes()
        df = pl.read_csv(io.BytesIO(csv_bytes))
    else:
        df = pl.read_csv(mc_table_path)

    print(f"Loaded marginal cost table with {len(df)} rows")
    return df


def validate_mc_table_path(mc_table_path: str) -> None:
    """Fail early if marginal cost table path does not exist."""
    if mc_table_path.startswith("s3://"):
        s3_path = S3Path(mc_table_path)
        if not s3_path.exists():
            raise FileNotFoundError(f"Marginal cost table not found: {mc_table_path}")
        return

    if not Path(mc_table_path).exists():
        raise FileNotFoundError(f"Marginal cost table not found: {mc_table_path}")


def get_marginal_costs_for_year(
    mc_df: pl.DataFrame, utility: str, year: int
) -> tuple[float, float]:
    """Extract upstream and distribution marginal costs for a utility and year.

    Args:
        mc_df: Marginal cost table DataFrame
        utility: Utility name
        year: Target year

    Returns:
        Tuple of (mc_upstream, mc_dist) in $/kW-yr
    """
    row = mc_df.filter((pl.col("utility") == utility) & (pl.col("year") == year))

    if len(row) == 0:
        raise ValueError(f"No marginal cost data found for {utility} in year {year}")

    if len(row) > 1:
        raise ValueError(
            f"Multiple marginal cost entries found for {utility} in year {year}"
        )

    mc_upstream = float(row["upstream"][0])
    mc_dist_substation = float(row["distribution_substation"][0])
    mc_dist_feeder = float(row["primary_feeder"][0])
    mc_dist = mc_dist_substation + mc_dist_feeder

    print(f"\nMarginal Costs for {utility} - {year}:")
    print(f"  Upstream (Tx/Sub-Tx): ${mc_upstream:.2f}/kW-yr")
    print(f"  Distribution Substation: ${mc_dist_substation:.2f}/kW-yr")
    print(f"  Primary Feeder: ${mc_dist_feeder:.2f}/kW-yr")
    print(f"  Total Distribution: ${mc_dist:.2f}/kW-yr")
    print(f"  Total MC: ${mc_upstream + mc_dist:.2f}/kW-yr")

    return mc_upstream, mc_dist


def calculate_pop_weights(
    load_df: pl.DataFrame, n_upstream_hours: int = 100, n_dist_hours: int = 100
) -> pl.DataFrame:
    """Calculate Probability of Peak (PoP) weights using load-weighted method.

    Args:
        load_df: DataFrame with timestamp and load_mw columns
        n_upstream_hours: Number of top hours for upstream allocation (default: 100)
        n_dist_hours: Number of top hours for distribution allocation (default: 50)

    Returns:
        DataFrame with added columns: w_upstream, w_dist, is_upstream_peak, is_dist_peak
    """
    # Sort by load to identify peak hours
    sorted_df = load_df.sort("load_mw", descending=True)

    # Identify top hours
    top_upstream_indices = sorted_df.head(n_upstream_hours)["timestamp"]
    top_dist_indices = sorted_df.head(n_dist_hours)["timestamp"]

    # Calculate sum of loads in peak windows
    sum_top_upstream = sorted_df.head(n_upstream_hours)["load_mw"].sum()
    sum_top_dist = sorted_df.head(n_dist_hours)["load_mw"].sum()

    print("\nPeak Hour Identification:")
    print(f"  Top {n_upstream_hours} hours sum: {sum_top_upstream:.2f} MW")
    print(f"  Top {n_dist_hours} hours sum: {sum_top_dist:.2f} MW")

    # Add peak flags and weights
    result_df = load_df.with_columns(
        [
            pl.col("timestamp")
            .is_in(top_upstream_indices.implode())
            .alias("is_upstream_peak"),
            pl.col("timestamp").is_in(top_dist_indices.implode()).alias("is_dist_peak"),
        ]
    )

    # Calculate load-weighted PoP weights
    result_df = result_df.with_columns(
        [
            pl.when(pl.col("is_upstream_peak"))
            .then(pl.col("load_mw") / sum_top_upstream)
            .otherwise(0.0)
            .alias("w_upstream"),
            pl.when(pl.col("is_dist_peak"))
            .then(pl.col("load_mw") / sum_top_dist)
            .otherwise(0.0)
            .alias("w_dist"),
        ]
    )

    # Verify weights sum to 1.0
    sum_w_upstream = result_df["w_upstream"].sum()
    sum_w_dist = result_df["w_dist"].sum()

    print("\nWeight Verification:")
    print(f"  Sum of w_upstream: {sum_w_upstream:.6f} (should be 1.0)")
    print(f"  Sum of w_dist: {sum_w_dist:.6f} (should be 1.0)")

    if abs(sum_w_upstream - 1.0) > 1e-6:
        raise ValueError(f"Upstream weights do not sum to 1.0: {sum_w_upstream}")

    if abs(sum_w_dist - 1.0) > 1e-6:
        raise ValueError(f"Distribution weights do not sum to 1.0: {sum_w_dist}")

    return result_df


def allocate_costs_to_hours(
    load_df: pl.DataFrame,
    mc_upstream: float,
    mc_dist: float,
) -> pl.DataFrame:
    """Allocate marginal costs to hourly price signals.

    Args:
        load_df: DataFrame with PoP weights (w_upstream, w_dist)
        mc_upstream: Upstream marginal cost ($/kW-yr)
        mc_dist: Distribution marginal cost ($/kW-yr)

    Returns:
        DataFrame with added columns: mc_upstream_per_kwh, mc_dist_per_kwh, mc_total_per_kwh
    """
    # Convert $/kW-yr to $/kWh using PoP weights
    # Formula: P_h = (MC * W_h) / 8760
    # But we want it in $/kWh, and the customer usage is in kWh
    # The proper formula is: P_h = MC * W_h (in $/kWh)
    # Because MC is in $/kW-yr and W_h is probability, the result needs no division by 8760

    result_df = load_df.with_columns(
        [
            (pl.lit(mc_upstream) * pl.col("w_upstream")).alias("mc_upstream_per_kwh"),
            (pl.lit(mc_dist) * pl.col("w_dist")).alias("mc_dist_per_kwh"),
        ]
    )

    result_df = result_df.with_columns(
        [
            (pl.col("mc_upstream_per_kwh") + pl.col("mc_dist_per_kwh")).alias(
                "mc_total_per_kwh"
            )
        ]
    )

    return result_df


def validate_allocation(
    df: pl.DataFrame,
    mc_upstream: float,
    mc_dist: float,
) -> dict:
    """Validate that a constant 1 kW load results in correct annual cost.

    The validation checks that:
    sum(1 kW * P_h * 1 hour) for all hours = MC_upstream + MC_dist ($/kW-yr)

    Args:
        df: DataFrame with hourly marginal costs
        mc_upstream: Expected upstream marginal cost ($/kW-yr)
        mc_dist: Expected distribution marginal cost ($/kW-yr)

    Returns:
        Dictionary with validation results
    """
    # Calculate total cost for 1 kW constant load
    # For each hour: cost = 1 kW * P_h ($/kWh) * 1 hour = P_h ($)
    # Annual cost = sum of all hourly costs

    total_upstream_cost = df["mc_upstream_per_kwh"].sum()
    total_dist_cost = df["mc_dist_per_kwh"].sum()
    total_cost = df["mc_total_per_kwh"].sum()

    expected_total = mc_upstream + mc_dist

    validation_results = {
        "expected_upstream": mc_upstream,
        "actual_upstream": total_upstream_cost,
        "upstream_error": abs(total_upstream_cost - mc_upstream),
        "upstream_error_pct": abs(total_upstream_cost - mc_upstream) / mc_upstream * 100
        if mc_upstream > 0
        else 0,
        "expected_dist": mc_dist,
        "actual_dist": total_dist_cost,
        "dist_error": abs(total_dist_cost - mc_dist),
        "dist_error_pct": abs(total_dist_cost - mc_dist) / mc_dist * 100
        if mc_dist > 0
        else 0,
        "expected_total": expected_total,
        "actual_total": total_cost,
        "total_error": abs(total_cost - expected_total),
        "total_error_pct": abs(total_cost - expected_total) / expected_total * 100
        if expected_total > 0
        else 0,
    }

    print("\n" + "=" * 60)
    print("VALIDATION: 1 kW Constant Load Test")
    print("=" * 60)
    print("Upstream:")
    print(f"  Expected: ${validation_results['expected_upstream']:.4f}/kW-yr")
    print(f"  Actual:   ${validation_results['actual_upstream']:.4f}/kW-yr")
    print(
        f"  Error:    ${validation_results['upstream_error']:.4f} ({validation_results['upstream_error_pct']:.4f}%)"
    )
    print("\nDistribution:")
    print(f"  Expected: ${validation_results['expected_dist']:.4f}/kW-yr")
    print(f"  Actual:   ${validation_results['actual_dist']:.4f}/kW-yr")
    print(
        f"  Error:    ${validation_results['dist_error']:.4f} ({validation_results['dist_error_pct']:.4f}%)"
    )
    print("\nTotal:")
    print(f"  Expected: ${validation_results['expected_total']:.4f}/kW-yr")
    print(f"  Actual:   ${validation_results['actual_total']:.4f}/kW-yr")
    print(
        f"  Error:    ${validation_results['total_error']:.4f} ({validation_results['total_error_pct']:.4f}%)"
    )

    # Check if validation passed (within 0.01% tolerance)
    tolerance = 0.01  # 0.01% error tolerance
    if validation_results["total_error_pct"] > tolerance:
        print("\n✗ Validation FAILED")
        print("=" * 60)
        raise ValueError(
            f"Validation failed: Total error {validation_results['total_error_pct']:.6f}% "
            f"exceeds tolerance of {tolerance}%. "
            f"Expected ${validation_results['expected_total']:.4f}/kW-yr, "
            f"got ${validation_results['actual_total']:.4f}/kW-yr. "
            f"Check allocation logic and input data."
        )
    else:
        print("\n✓ Validation PASSED")
    print("=" * 60)

    return validation_results


def save_allocated_costs(
    df: pl.DataFrame,
    iso_region: str,
    utility: str,
    year: int,
    s3_base: str,
    validation_results: dict,
    storage_options: dict[str, str],
):
    """Save allocated marginal costs to S3 with Hive-style partitioning.

    Output structure:
        region=<iso_region>/utility=X/year=YYYY/data.parquet

    Args:
        df: DataFrame with allocated costs
        iso_region: ISO region partition key (nyiso/isone)
        utility: Utility name
        year: Target year
        s3_base: Base S3 path for marginal costs
        validation_results: Validation results to include in metadata
        storage_options: Polars S3 storage options with AWS bucket region
    """
    # Select final columns
    output_df = df.select(
        [
            "timestamp",
            pl.lit(iso_region).alias("region"),
            pl.lit(utility).alias("utility"),
            pl.lit(year).alias("year"),
            "mc_total_per_kwh",
        ]
    )

    s3_base = s3_base.rstrip("/") + "/"
    output_df.write_parquet(
        s3_base,
        partition_by=["region", "utility", "year"],
        storage_options=storage_options,
    )

    print(
        "\n✓ Saved allocated costs to "
        f"{s3_base}/region={iso_region}/utility={utility}/year={year}/data.parquet"
    )
    print(f"  Rows: {len(output_df):,}")
    print(f"  Columns: {', '.join(output_df.columns)}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Allocate marginal costs to hourly price signals using PoP method"
    )
    parser.add_argument(
        "--state",
        type=str,
        required=True,
        choices=["NY", "RI"],
        help="State to process (supported: NY, RI)",
    )
    parser.add_argument(
        "--utility",
        type=str,
        required=True,
        help="Utility name (lowercase short code: nyseg, rge, cenhud, nationalgrid)",
    )
    parser.add_argument(
        "--mc-year",
        type=int,
        required=True,
        help="Marginal cost year (2026-2035) - which year's MC table to use",
    )
    parser.add_argument(
        "--load-year",
        type=int,
        help="Year of load profile to use (defaults to same as --mc-year). Use this to apply future MC to historical load shapes.",
    )
    parser.add_argument(
        "--mc-table-path",
        type=str,
        required=True,
        help="Path to marginal cost table CSV (local or s3://)",
    )
    parser.add_argument(
        "--utility-load-s3-base",
        "--nyiso-s3-base",
        dest="utility_load_s3_base",
        type=str,
        required=True,
        help=(
            "Base S3 path for utility loads "
            "(e.g. s3://data.sb/eia/hourly_demand/utilities/)"
        ),
    )
    parser.add_argument(
        "--output-s3-base",
        type=str,
        required=True,
        help="Base S3 path for output (e.g., s3://data.sb/switchbox/marginal_costs/ny/)",
    )
    parser.add_argument(
        "--upstream-hours",
        type=int,
        choices=range(0, 8761),
        default=100,
        help="Number of top load hours for upstream allocation (0-8760, default: 100)",
    )
    parser.add_argument(
        "--dist-hours",
        type=int,
        choices=range(0, 8761),
        default=50,
        help="Number of top load hours for distribution allocation (0-8760, default: 50)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload results to S3 (default: False, for data inspection only)",
    )

    args = parser.parse_args()
    validate_mc_table_path(args.mc_table_path)
    load_dotenv()
    config = get_state_config(args.state)
    storage_options = get_aws_storage_options()

    # Default load_year to mc_year if not specified
    load_year = args.load_year if args.load_year else args.mc_year

    print("=" * 60)
    print("MARGINAL COST ALLOCATION")
    print(f"State: {config.state}")
    print(f"ISO region partition: {config.iso_region}")
    print(f"AWS bucket region: {storage_options.get('region')}")
    print("=" * 60)
    print(f"Utility: {args.utility}")
    print(f"MC Year: {args.mc_year}")
    print(f"Load Year: {load_year}")
    print(f"Upstream allocation window: Top {args.upstream_hours} hours")
    print(f"Distribution allocation window: Top {args.dist_hours} hours")
    print(f"Upload to S3: {'Yes' if args.upload else 'No (inspection only)'}")
    print("=" * 60)

    utility_load_s3_base = args.utility_load_s3_base
    output_s3_base = args.output_s3_base

    # Load utility load profile
    load_df = load_utility_load_profile(
        utility_load_s3_base,
        config.iso_region,
        load_year,
        args.utility,
        storage_options,
    )
    load_df = normalize_load_to_cairo_8760(load_df, args.utility, load_year)

    # Load marginal cost table
    mc_df = load_marginal_cost_table(args.mc_table_path)

    # Get marginal costs for this utility and year
    mc_upstream, mc_dist = get_marginal_costs_for_year(
        mc_df, args.utility, args.mc_year
    )

    # Calculate PoP weights
    load_df = calculate_pop_weights(load_df, args.upstream_hours, args.dist_hours)

    # Allocate costs to hours
    load_df = allocate_costs_to_hours(load_df, mc_upstream, mc_dist)

    # Validate allocation
    validation_results = validate_allocation(load_df, mc_upstream, mc_dist)

    # Display sample results
    print("\n" + "=" * 60)
    print("SAMPLE RESULTS")
    print("=" * 60)
    print("\nTop 10 hours by total marginal cost:")
    sample_df = load_df.sort("mc_total_per_kwh", descending=True).head(10)
    print(
        sample_df.select(
            [
                "timestamp",
                "load_mw",
                "mc_upstream_per_kwh",
                "mc_dist_per_kwh",
                "mc_total_per_kwh",
                "is_upstream_peak",
                "is_dist_peak",
            ]
        )
    )

    # Save results if upload flag is set
    if args.upload:
        save_allocated_costs(
            load_df,
            config.iso_region,
            args.utility,
            args.mc_year,
            output_s3_base,
            validation_results,
            storage_options,
        )
        print("\n" + "=" * 60)
        print("✓ Marginal cost allocation completed and uploaded")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("✓ Marginal cost allocation completed (data inspection complete)")
        print("⚠️  No data uploaded to S3 (use --upload flag to enable)")
        print("=" * 60)


if __name__ == "__main__":
    main()
