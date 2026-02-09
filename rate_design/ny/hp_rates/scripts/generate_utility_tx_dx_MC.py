"""Allocate marginal transmission and distribution costs to hourly price signals.

This script implements the diluted marginal cost allocation methodology using
the Probability of Peak (PoP) method to allocate $/kW-yr costs to $/kWh hourly
price signals.

Input:
    - Utility 8760 load profile from S3
    - Marginal cost table CSV ($/kW-yr)
    - Target year (2026-2035)

Output: s3://data.sb/switchbox/marginal_costs/ny/{utility}/year={year}/mc_8760.parquet

Usage:
    python generate_utility_tx_dx_MC.py --utility NYSEG --year 2026 --mc-table-path data/marginal_costs/ny_marginal_costs_2026_2035.csv
"""

import argparse
import io
from pathlib import Path

import polars as pl
from cloudpathlib import S3Path


def load_utility_load_profile(s3_base: str, year_load: int, utility: str) -> pl.DataFrame:
    """Load utility 8760 load profile from S3.
    
    Args:
        s3_base: Base S3 path for NYISO loads
        year_load: Year of the load profile to use
        utility: Utility name
        
    Returns:
        DataFrame with columns: timestamp, utility, load_mw
    """
    load_path = S3Path(s3_base) / f"year={year_load}" / "utility" / f"{utility}.parquet"
    
    if not load_path.exists():
        raise FileNotFoundError(f"Utility load profile not found: {load_path}")
    
    load_bytes = load_path.read_bytes()
    df = pl.read_parquet(io.BytesIO(load_bytes))
    
    print(f"Loaded {len(df):,} hourly load records for {utility} (year {year_load})")
    return df


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


def get_marginal_costs_for_year(
    mc_df: pl.DataFrame, 
    utility: str, 
    year: int
) -> tuple[float, float]:
    """Extract upstream and distribution marginal costs for a utility and year.
    
    Args:
        mc_df: Marginal cost table DataFrame
        utility: Utility name
        year: Target year
        
    Returns:
        Tuple of (mc_upstream, mc_dist) in $/kW-yr
    """
    row = mc_df.filter(
        (pl.col("Utility") == utility) & (pl.col("Year") == year)
    )
    
    if len(row) == 0:
        raise ValueError(f"No marginal cost data found for {utility} in year {year}")
    
    if len(row) > 1:
        raise ValueError(f"Multiple marginal cost entries found for {utility} in year {year}")
    
    mc_upstream = float(row["Upstream"][0])
    mc_dist_substation = float(row["Distribution Substation"][0])
    mc_dist_feeder = float(row["Primary Feeder"][0])
    mc_dist = mc_dist_substation + mc_dist_feeder
    
    print(f"\nMarginal Costs for {utility} - {year}:")
    print(f"  Upstream (Tx/Sub-Tx): ${mc_upstream:.2f}/kW-yr")
    print(f"  Distribution Substation: ${mc_dist_substation:.2f}/kW-yr")
    print(f"  Primary Feeder: ${mc_dist_feeder:.2f}/kW-yr")
    print(f"  Total Distribution: ${mc_dist:.2f}/kW-yr")
    print(f"  Total MC: ${mc_upstream + mc_dist:.2f}/kW-yr")
    
    return mc_upstream, mc_dist


def calculate_pop_weights(
    load_df: pl.DataFrame, 
    n_upstream_hours: int = 100,
    n_dist_hours: int = 50
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
    
    print(f"\nPeak Hour Identification:")
    print(f"  Top {n_upstream_hours} hours sum: {sum_top_upstream:.2f} MW")
    print(f"  Top {n_dist_hours} hours sum: {sum_top_dist:.2f} MW")
    
    # Add peak flags and weights
    result_df = load_df.with_columns([
        pl.col("timestamp").is_in(top_upstream_indices).alias("is_upstream_peak"),
        pl.col("timestamp").is_in(top_dist_indices).alias("is_dist_peak"),
    ])
    
    # Calculate load-weighted PoP weights
    result_df = result_df.with_columns([
        pl.when(pl.col("is_upstream_peak"))
        .then(pl.col("load_mw") / sum_top_upstream)
        .otherwise(0.0)
        .alias("w_upstream"),
        
        pl.when(pl.col("is_dist_peak"))
        .then(pl.col("load_mw") / sum_top_dist)
        .otherwise(0.0)
        .alias("w_dist"),
    ])
    
    # Verify weights sum to 1.0
    sum_w_upstream = result_df["w_upstream"].sum()
    sum_w_dist = result_df["w_dist"].sum()
    
    print(f"\nWeight Verification:")
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
    
    result_df = load_df.with_columns([
        (pl.lit(mc_upstream) * pl.col("w_upstream")).alias("mc_upstream_per_kwh"),
        (pl.lit(mc_dist) * pl.col("w_dist")).alias("mc_dist_per_kwh"),
    ])
    
    result_df = result_df.with_columns([
        (pl.col("mc_upstream_per_kwh") + pl.col("mc_dist_per_kwh")).alias("mc_total_per_kwh")
    ])
    
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
        "upstream_error_pct": abs(total_upstream_cost - mc_upstream) / mc_upstream * 100 if mc_upstream > 0 else 0,
        "expected_dist": mc_dist,
        "actual_dist": total_dist_cost,
        "dist_error": abs(total_dist_cost - mc_dist),
        "dist_error_pct": abs(total_dist_cost - mc_dist) / mc_dist * 100 if mc_dist > 0 else 0,
        "expected_total": expected_total,
        "actual_total": total_cost,
        "total_error": abs(total_cost - expected_total),
        "total_error_pct": abs(total_cost - expected_total) / expected_total * 100 if expected_total > 0 else 0,
    }
    
    print("\n" + "="*60)
    print("VALIDATION: 1 kW Constant Load Test")
    print("="*60)
    print(f"Upstream:")
    print(f"  Expected: ${validation_results['expected_upstream']:.4f}/kW-yr")
    print(f"  Actual:   ${validation_results['actual_upstream']:.4f}/kW-yr")
    print(f"  Error:    ${validation_results['upstream_error']:.4f} ({validation_results['upstream_error_pct']:.4f}%)")
    print(f"\nDistribution:")
    print(f"  Expected: ${validation_results['expected_dist']:.4f}/kW-yr")
    print(f"  Actual:   ${validation_results['actual_dist']:.4f}/kW-yr")
    print(f"  Error:    ${validation_results['dist_error']:.4f} ({validation_results['dist_error_pct']:.4f}%)")
    print(f"\nTotal:")
    print(f"  Expected: ${validation_results['expected_total']:.4f}/kW-yr")
    print(f"  Actual:   ${validation_results['actual_total']:.4f}/kW-yr")
    print(f"  Error:    ${validation_results['total_error']:.4f} ({validation_results['total_error_pct']:.4f}%)")
    
    # Check if validation passed (within 0.01% tolerance)
    tolerance = 0.01  # 0.01% error tolerance
    if validation_results['total_error_pct'] > tolerance:
        print("\n✗ Validation FAILED")
        print("="*60)
        raise ValueError(
            f"Validation failed: Total error {validation_results['total_error_pct']:.6f}% "
            f"exceeds tolerance of {tolerance}%. "
            f"Expected ${validation_results['expected_total']:.4f}/kW-yr, "
            f"got ${validation_results['actual_total']:.4f}/kW-yr. "
            f"Check allocation logic and input data."
        )
    else:
        print("\n✓ Validation PASSED")
    print("="*60)
    
    return validation_results


def save_allocated_costs(
    df: pl.DataFrame,
    utility: str,
    year: int,
    s3_base: str,
    validation_results: dict,
):
    """Save allocated marginal costs to S3.
    
    Args:
        df: DataFrame with allocated costs
        utility: Utility name
        year: Target year
        s3_base: Base S3 path for marginal costs
        validation_results: Validation results to include in metadata
    """
    # Select final columns
    output_df = df.select([
        "timestamp",
        pl.lit(utility).alias("utility"),
        pl.lit(year).alias("year"),
        "load_mw",
        "mc_upstream_per_kwh",
        "mc_dist_per_kwh",
        "mc_total_per_kwh",
        "is_upstream_peak",
        "is_dist_peak",
        "w_upstream",
        "w_dist",
    ])
    
    # Write to buffer
    buf = io.BytesIO()
    output_df.write_parquet(buf)
    
    # Upload to S3
    base_path = S3Path(s3_base)
    output_path = base_path / utility / f"year={year}" / "mc_8760.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(buf.getvalue())
    
    print(f"\n✓ Saved allocated costs to {output_path}")
    print(f"  Rows: {len(output_df):,}")
    print(f"  Columns: {', '.join(output_df.columns)}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Allocate marginal costs to hourly price signals using PoP method"
    )
    parser.add_argument(
        "--utility",
        type=str,
        required=True,
        help="Utility name (e.g., NYSEG, RG&E, Central Hudson)",
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Target year for marginal cost allocation (2026-2035)",
    )
    parser.add_argument(
        "--year-load",
        type=int,
        help="Year of load profile to use (defaults to same as --year). Use this to apply future MC to historical load shapes.",
    )
    parser.add_argument(
        "--mc-table-path",
        type=str,
        required=True,
        help="Path to marginal cost table CSV (local or s3://)",
    )
    parser.add_argument(
        "--nyiso-s3-base",
        type=str,
        default="s3://data.sb/nyiso/loads",
        help="Base S3 path for NYISO loads (default: s3://data.sb/nyiso/loads)",
    )
    parser.add_argument(
        "--output-s3-base",
        type=str,
        default="s3://data.sb/switchbox/marginal_costs/ny",
        help="Base S3 path for output (default: s3://data.sb/switchbox/marginal_costs/ny)",
    )
    parser.add_argument(
        "--upstream-hours",
        type=int,
        default=100,
        help="Number of top load hours for upstream allocation (default: 100)",
    )
    parser.add_argument(
        "--dist-hours",
        type=int,
        default=50,
        help="Number of top load hours for distribution allocation (default: 50)",
    )
    
    args = parser.parse_args()
    
    # Default year_load to year if not specified
    year_load = args.year_load if args.year_load else args.year
    
    print("="*60)
    print("NY Marginal Cost Allocation")
    print("="*60)
    print(f"Utility: {args.utility}")
    print(f"MC Year: {args.year}")
    print(f"Load Year: {year_load}")
    print(f"Upstream allocation window: Top {args.upstream_hours} hours")
    print(f"Distribution allocation window: Top {args.dist_hours} hours")
    print("="*60)
    
    # Load utility load profile
    load_df = load_utility_load_profile(args.nyiso_s3_base, year_load, args.utility)
    
    # Load marginal cost table
    mc_df = load_marginal_cost_table(args.mc_table_path)
    
    # Get marginal costs for this utility and year
    mc_upstream, mc_dist = get_marginal_costs_for_year(mc_df, args.utility, args.year)
    
    # Calculate PoP weights
    load_df = calculate_pop_weights(load_df, args.upstream_hours, args.dist_hours)
    
    # Allocate costs to hours
    load_df = allocate_costs_to_hours(load_df, mc_upstream, mc_dist)
    
    # Validate allocation
    validation_results = validate_allocation(load_df, mc_upstream, mc_dist)
    
    # Save results
    save_allocated_costs(
        load_df, 
        args.utility, 
        args.year, 
        args.output_s3_base,
        validation_results
    )
    
    print("\n✓ Marginal cost allocation completed successfully")


if __name__ == "__main__":
    main()
