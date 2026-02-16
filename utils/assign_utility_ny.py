"""
Utility assignment for ResStock buildings (NY).

Equivalent to utils/utility_assignment_ny.R.
For create_hh_utilities: requires geopandas; building data from S3 or local parquet.
For assign_utility_ny: requires S3Path.
"""

import argparse
from typing import cast

import geopandas as gpd
import numpy as np
import pandas as pd
import polars as pl
from cloudpathlib import S3Path
from pygris import pumas as get_pumas

from utils import get_aws_region

STORAGE_OPTIONS = {"aws_region": get_aws_region()}
CONFIGS: dict = {
    "state_code": "NY",
    "state_fips": "36",
    "state_crs": 2260,  # New York state plane (meters)
    "utility_name_map": [
        {
            "state_name": "Bath Electric Gas and Water",
            "std_name": "bath",
        },  # TODO: Don't include in the mapping (i.e. )
        {"state_name": "Central Hudson Gas and Electric", "std_name": "cenhud"},
        {
            "state_name": "Chautauqua Utilities, Inc.",
            "std_name": "chautauqua",
        },  # TODO: Don't include in the mapping
        {"state_name": "Consolidated Edison", "std_name": "coned"},
        {
            "state_name": "Corning Natural Gas",
            "std_name": "corning",
        },  # TODO: Don't include in the mapping
        {
            "state_name": "Fillmore Gas Company",
            "std_name": "fillmore",
        },  # TODO: Don't include in the mapping
        {"state_name": "National Grid - NYC", "std_name": "kedny"},
        {"state_name": "National Grid - Long Island", "std_name": "kedli"},
        {"state_name": "National Grid", "std_name": "nimo"},
        {"state_name": "None", "std_name": "none"},
        {
            "state_name": "National Fuel Gas Distribution",
            "std_name": "nfg",
        },
        {"state_name": "NYS Electric and Gas", "std_name": "nyseg"},
        {"state_name": "Orange and Rockland Utilities", "std_name": "or"},
        {"state_name": "Long Island Power Authority", "std_name": "psegli"},
        {
            "state_name": "Reserve Gas Company",
            "std_name": "reserve",
        },  # TODO: Don't include in the mapping. Leave it blank/unassigned, then nearest neighbor to the nearest puma polygon, then assign based on that polygon's assignment probability
        {"state_name": "Rochester Gas and Electric", "std_name": "rge"},
        {
            "state_name": "St. Lawrence Gas",
            "std_name": "stlaw",
        },  # TODO: Don't include in the mapping
        {
            "state_name": "Valley Energy",
            "std_name": "valley",
        },  # TODO: Don't include in the mapping
        {
            "state_name": "Woodhull Municipal Gas Company",
            "std_name": "woodhull",
        },  # TODO: Don't include in the mapping
    ],
}


def _select_puma_and_heating_fuel_metadata(metadata: pl.LazyFrame) -> pl.LazyFrame:
    """
    Select puma and heating fuel metadata from a LazyFrame.

    Expects columns bldg_id, in.puma, in.heating_fuel. Returns LazyFrame with
    bldg_id, puma, heating_fuel.
    """

    result = metadata.select(
        pl.col("bldg_id"),
        pl.col("in.puma").str.slice(-5).alias("puma"),
        pl.col("in.heating_fuel").alias("heating_fuel"),
    )

    return result


########################################################
# GIS Utility Mapping
########################################################
def create_hh_utilities(
    puma_and_heating_fuel: pl.LazyFrame,
    electric_polygons: gpd.GeoDataFrame,
    gas_polygons: gpd.GeoDataFrame,
    pumas: gpd.GeoDataFrame,
    config: dict | None = None,
) -> pl.LazyFrame:
    """
    Create a LazyFrame of households with their associated utilities.

    Uses Census PUMAs and utility service territory polygons to compute
    overlap, then assigns electric/gas utilities to ResStock buildings by
    sampling from PUMA-level probabilities. Requires geopandas.

    Args:
        puma_and_heating_fuel: LazyFrame with bldg_id, puma, heating_fuel columns
        electric_polygons: GeoDataFrame of electric utility service territories
        gas_polygons: GeoDataFrame of gas utility service territories
        pumas: GeoDataFrame of Census PUMAs
        config: State config dictionary; defaults to CONFIGS

    Returns:
        LazyFrame with bldg_id, sb.electric_utility, sb.gas_utility.
    """

    config = config or CONFIGS

    utility_name_map = pl.DataFrame(
        config["utility_name_map"]
    ).lazy()  # Convert to LazyFrame for consistency

    # Calculate overlap between PUMAs and utilities
    puma_elec_overlap = _calculate_puma_utility_overlap(
        pumas, electric_polygons, config["state_crs"]
    )

    puma_gas_overlap = _calculate_puma_utility_overlap(
        pumas, gas_polygons, config["state_crs"]
    )

    # Electric utility probabilities
    # Force all buildings to have an electric utility (filter out "none")
    puma_elec_probs = _calculate_utility_probabilities(
        puma_elec_overlap,
        utility_name_map,
        handle_municipal=True,
        filter_none=True,
    )

    # Gas utility probabilities
    # Allow buildings without gas utility (keep "none" for non-gas buildings)
    puma_gas_probs = _calculate_utility_probabilities(
        puma_gas_overlap,
        utility_name_map,
        handle_municipal=False,
        filter_none=False,
    )

    # Calculate prior probability distributions
    elec_prior_weighted, gas_prior_weighted = _calculate_prior_distributions(
        puma_elec_probs, puma_gas_probs, puma_and_heating_fuel=puma_and_heating_fuel
    )

    # Assign electric and gas to bldg's
    building_elec = _sample_utility_per_building(
        puma_and_heating_fuel, puma_elec_probs, "sb.electric_utility"
    )

    building_gas = _sample_utility_per_building(
        puma_and_heating_fuel,
        puma_gas_probs,
        "sb.gas_utility",
        only_when_fuel="Natural Gas",
    )

    # Print comparison summary
    _print_comparison_summary(
        building_elec,
        building_gas,
        elec_prior_weighted,
        gas_prior_weighted,
        puma_and_heating_fuel=puma_and_heating_fuel,
    )

    building_utilities = building_elec.join(
        building_gas.select("bldg_id", "sb.gas_utility"), on="bldg_id", how="left"
    )

    return building_utilities


def read_csv_to_gdf_from_s3(
    s3_path: S3Path,
    utility_type: str,
    geometry_col: str = "the_geom",
    crs: str = "EPSG:4326",
) -> gpd.GeoDataFrame:
    """
    Read a CSV file from S3 with WKT geometry and convert to GeoDataFrame.

    Parameters
    ----------
    s3_path : S3Path
        S3Path to the CSV file (e.g., S3Path('s3://bucket/path/file.csv'))
    geometry_col : str, default 'the_geom'
        Name of the column containing WKT geometry
    crs : str, default 'EPSG:4326'
        Coordinate reference system for the geometries

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with geometry from the WKT column
    """
    # Read CSV directly from S3
    df = pd.read_csv(str(s3_path), low_memory=False)

    # Convert numeric columns that might have been read as strings
    # Skip geometry column and known string columns (like COMP_FULL/utility names)
    if utility_type == "electric":
        string_columns = {"comp_full", "utility", "state_name", "std_name"}
    elif utility_type == "gas":
        string_columns = {"COMP_FULL", "utility", "state_name", "std_name"}
    for col in df.columns:
        if col != geometry_col and col not in string_columns:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            except (ValueError, TypeError):
                pass  # Leave non-numeric columns as-is

    # Ensure string columns stay as strings (in case they were inferred as numeric)
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.GeoSeries.from_wkt(df[geometry_col]), crs=crs
    )

    gdf = gdf.to_crs(epsg=CONFIGS["state_crs"])
    if utility_type == "electric":
        gdf = cast(gpd.GeoDataFrame, gdf.rename(columns={"comp_full": "utility"}))
    elif utility_type == "gas":
        gdf = cast(gpd.GeoDataFrame, gdf.rename(columns={"COMP_FULL": "utility"}))

    return gdf


def _calculate_puma_utility_overlap(
    pumas: gpd.GeoDataFrame,
    utility_gdf: gpd.GeoDataFrame,
    state_crs: int,
) -> pl.LazyFrame:
    """Calculate overlap between PUMAs and utility service territories.

    Args:
        pumas: GeoDataFrame of PUMAs
        utility_gdf: GeoDataFrame of utility polygons
        state_crs: State-specific CRS (EPSG code) for accurate area calculations

    Returns:
        Polars LazyFrame with puma_id, pct_overlap, and all utility columns
    """
    # Transform PUMAs to state CRS and calculate area
    puma_overlap = pumas.to_crs(epsg=state_crs).copy()
    puma_overlap["puma_area"] = puma_overlap.geometry.area

    # Transform utility polygons to state CRS
    utility_polygons_transformed = utility_gdf.to_crs(epsg=state_crs)

    # Perform spatial intersection
    puma_overlap = gpd.overlay(
        puma_overlap, utility_polygons_transformed, how="intersection"
    )

    # Calculate overlap area and percentage
    puma_overlap["overlap_area"] = puma_overlap.geometry.area
    puma_overlap["pct_overlap"] = (
        puma_overlap["overlap_area"] / puma_overlap["puma_area"] * 100
    ).astype(float)

    # Drop geometry and select relevant columns
    puma_overlap = puma_overlap.drop(columns=["geometry"])
    utility_cols = [col for col in puma_overlap.columns if "utility" in col.lower()]
    puma_overlap = puma_overlap[["PUMACE10", "pct_overlap"] + utility_cols].rename(
        columns={"PUMACE10": "puma_id"}
    )

    # Convert to polars LazyFrame (utility column should already be string from source CSV)
    return pl.from_pandas(puma_overlap).lazy()


def _calculate_prior_distributions(
    puma_elec_probs: pl.LazyFrame,
    puma_gas_probs: pl.LazyFrame,
    puma_and_heating_fuel: pl.LazyFrame,
) -> tuple[dict[str, float], dict[str, float]]:
    """Calculate prior probability distributions for electric and gas utilities.

    Args:
        puma_elec_probs: Wide-format LazyFrame with puma_id and electric utility probability columns
        puma_gas_probs: Wide-format LazyFrame with puma_id and gas utility probability columns
        puma_and_heating_fuel: LazyFrame with bldg_id, puma, heating_fuel

    Returns:
        Tuple of (elec_prior_weighted dict, gas_prior_weighted dict) for later comparison
    """
    # Electric: Calculate weighted average probability across all PUMAs
    # (weighted by number of buildings in each PUMA)
    puma_counts = (
        puma_and_heating_fuel.group_by("puma").agg(pl.len().alias("count")).collect()
    )
    puma_elec_probs_df = cast(pl.DataFrame, puma_elec_probs.collect())
    elec_prior = cast(
        pl.DataFrame,
        (
            pl.LazyFrame(puma_elec_probs_df)
            .join(
                pl.LazyFrame(puma_counts),
                left_on="puma_id",
                right_on="puma",
                how="left",
            )
            .with_columns(pl.col("count").fill_null(0))
            .collect()
        ),
    )
    utility_cols_elec = [c for c in puma_elec_probs_df.columns if c != "puma_id"]
    elec_prior_weighted = {}
    total_bldgs = elec_prior["count"].sum()
    for util in utility_cols_elec:
        weighted_prob = (
            (elec_prior[util] * elec_prior["count"]).sum() / total_bldgs
            if total_bldgs > 0
            else 0
        )
        if weighted_prob > 0:
            elec_prior_weighted[util] = weighted_prob

    # Gas: Calculate weighted average probability (only for Natural Gas buildings)
    gas_bldgs = puma_and_heating_fuel.filter(pl.col("heating_fuel") == "Natural Gas")
    gas_puma_counts = gas_bldgs.group_by("puma").agg(pl.len().alias("count")).collect()
    puma_gas_probs_df = cast(pl.DataFrame, puma_gas_probs.collect())
    gas_prior = cast(
        pl.DataFrame,
        (
            pl.LazyFrame(puma_gas_probs_df)
            .join(
                pl.LazyFrame(gas_puma_counts),
                left_on="puma_id",
                right_on="puma",
                how="left",
            )
            .with_columns(pl.col("count").fill_null(0))
            .collect()
        ),
    )
    utility_cols_gas = [c for c in puma_gas_probs_df.columns if c != "puma_id"]
    gas_prior_weighted = {}
    total_gas_bldgs = gas_prior["count"].sum()
    for util in utility_cols_gas:
        weighted_prob = (
            (gas_prior[util] * gas_prior["count"]).sum() / total_gas_bldgs
            if total_gas_bldgs > 0
            else 0
        )
        if weighted_prob > 0:
            gas_prior_weighted[util] = weighted_prob

    return elec_prior_weighted, gas_prior_weighted


def _print_comparison_summary(
    building_elec: pl.LazyFrame,
    building_gas: pl.LazyFrame,
    elec_prior_weighted: dict[str, float],
    gas_prior_weighted: dict[str, float],
    puma_and_heating_fuel: pl.LazyFrame,
) -> None:
    """Print comparison summary showing prior vs posterior distributions in a formatted table.

    This summary compares the expected probability distribution (prior) with the actual
    assignment distribution (posterior) to verify that sampling matches the intended probabilities.

    Args:
        building_elec: LazyFrame with bldg_id, sb.electric_utility
        building_gas: LazyFrame with bldg_id, sb.gas_utility
        elec_prior_weighted: Dictionary mapping electric utility names to prior probabilities
        gas_prior_weighted: Dictionary mapping gas utility names to prior probabilities
        puma_and_heating_fuel: LazyFrame with bldg_id, puma, heating_fuel (for filtering by fuel type)
    """

    def _print_comparison_table(
        utility_type: str,
        building_df: pl.LazyFrame,
        utility_col: str,
        prior_weighted: dict[str, float],
        filter_fuel_type: str | None = None,
    ) -> None:
        """Print a formatted comparison table for one utility type.

        Args:
            filter_fuel_type: If provided, only compare buildings with this heating_fuel type
        """
        # Filter buildings by fuel type if specified (for gas utilities)
        if filter_fuel_type is not None:
            building_df_filtered = building_df.join(
                puma_and_heating_fuel.select("bldg_id", "heating_fuel"),
                on="bldg_id",
                how="left",
            ).filter(pl.col("heating_fuel") == filter_fuel_type)
        else:
            building_df_filtered = building_df

        # Collect once and reuse for posterior, counts, and assignment stats
        building_filtered_df = cast(pl.DataFrame, building_df_filtered.collect())
        posterior_df = (
            building_filtered_df.group_by(utility_col)
            .agg(pl.len().alias("count"))
            .with_columns((pl.col("count") / pl.col("count").sum()).alias("proportion"))
            .sort("proportion", descending=True)
        )

        # Build comparison data (include None/null utilities)
        all_utils = sorted(
            set(
                list(prior_weighted.keys())
                + [r[utility_col] for r in posterior_df.iter_rows(named=True)]
            ),
            key=lambda x: (x is None, str(x) if x is not None else ""),
        )

        total_buildings = building_filtered_df.height
        assigned_buildings = building_filtered_df.filter(
            pl.col(utility_col).is_not_null()
        ).height
        unassigned_buildings = building_filtered_df.filter(
            pl.col(utility_col).is_null()
        ).height

        if filter_fuel_type is not None:
            print(
                f"\nNote: Comparing only {filter_fuel_type} buildings "
                f"({assigned_buildings} assigned, {unassigned_buildings} unassigned out of {total_buildings} total)"
            )
        else:
            # For electric utilities, show assignment statistics
            print(
                f"\nAssignment Statistics: {assigned_buildings} assigned, "
                f"{unassigned_buildings} unassigned (null) out of {total_buildings} total buildings"
            )

        comparisons = []
        differences = []
        for util in all_utils:
            # Handle None/null utilities (they won't be in prior_weighted)
            prior_prob = prior_weighted.get(util, 0.0) if util is not None else 0.0
            prior_pct = prior_prob * 100
            expected_count = prior_prob * total_buildings

            # Find matching row (handle None comparison carefully)
            post_row = None
            for r in posterior_df.iter_rows(named=True):
                if util is None and r[utility_col] is None:
                    post_row = r
                    break
                elif util is not None and r[utility_col] == util:
                    post_row = r
                    break

            posterior_pct = post_row["proportion"] * 100 if post_row else 0.0
            actual_count = post_row["count"] if post_row else 0

            # Format utility name for display
            util_display = "None (unassigned)" if util is None else util
            diff_pct = posterior_pct - prior_pct
            diff_count = actual_count - expected_count
            differences.append(abs(diff_pct))
            comparisons.append(
                {
                    "utility": util_display,
                    "prior_pct": prior_pct,
                    "posterior_pct": posterior_pct,
                    "diff_pct": diff_pct,
                    "expected_count": expected_count,
                    "actual_count": actual_count,
                    "diff_count": diff_count,
                }
            )

        # Print percentage comparison table
        print(
            f"\n{utility_type} Utilities - Prior vs Posterior Comparison (Percentages):"
        )
        print("This table compares the expected probability distribution (prior) with")
        print(
            "the actual assignment distribution (posterior) to verify sampling accuracy.\n"
        )

        # Table header
        print(
            f"{'Utility':<30} {'Prior %':>12} {'Posterior %':>14} {'Difference %':>15}"
        )
        print("-" * 73)

        # Table rows
        for comp in comparisons:
            diff_str = f"{comp['diff_pct']:+.2f}"
            print(
                f"{comp['utility']:<30} "
                f"{comp['prior_pct']:>11.2f}% "
                f"{comp['posterior_pct']:>13.2f}% "
                f"{diff_str:>14}"
            )

        # Print building count comparison table
        print(f"\n{utility_type} Utilities - Expected vs Actual Building Counts:")
        print(f"Total buildings: {total_buildings}")
        print(f"{'Utility':<30} {'Expected':>12} {'Actual':>12} {'Difference':>15}")
        print("-" * 73)

        # Table rows
        for comp in comparisons:
            diff_str = f"{comp['diff_count']:+.0f}"
            print(
                f"{comp['utility']:<30} "
                f"{comp['expected_count']:>11.0f} "
                f"{comp['actual_count']:>11.0f} "
                f"{diff_str:>14}"
            )

        # Statistics
        if differences:
            max_diff = max(differences)
            avg_diff = sum(differences) / len(differences)
            # Calculate standard deviation
            mean_diff = avg_diff
            variance = sum((d - mean_diff) ** 2 for d in differences) / len(differences)
            std_diff = variance**0.5

            print("\nDifference Statistics (Percentages):")
            print(f"  Maximum absolute difference: {max_diff:.2f}%")
            print(f"  Average absolute difference: {avg_diff:.2f}%")
            print(f"  Standard deviation of differences: {std_diff:.2f}%")
            if max_diff > 5.0:
                print(
                    "\n  WARNING: Maximum absolute difference exceeds 5%. "
                    "Prior and posterior distributions may not match well."
                )

    print("\n" + "=" * 80)
    print("PRIOR vs POSTERIOR COMPARISON SUMMARY")
    print("=" * 80)

    _print_comparison_table(
        "Electric", building_elec, "sb.electric_utility", elec_prior_weighted
    )

    _print_comparison_table(
        "Gas",
        building_gas,
        "sb.gas_utility",
        gas_prior_weighted,
        filter_fuel_type="Natural Gas",
    )

    print("\n" + "=" * 80 + "\n")


def _calculate_utility_probabilities(
    puma_overlap: pl.LazyFrame,
    utility_name_map: pl.LazyFrame,
    handle_municipal: bool = True,
    filter_none: bool = False,
    include_municipal: bool = False,
) -> pl.LazyFrame:
    """Calculate utility probabilities for each PUMA based on overlap percentages.

    Args:
        puma_overlap: LazyFrame with puma_id, utility, pct_overlap columns
        utility_name_map: LazyFrame mapping from state_name to std_name for utilities
        handle_municipal: Whether to transform "Municipal Utility:" names to "muni-" format
        filter_none: Whether to filter out utilities named "none"
        include_municipal: Whether to include municipal utilities in the utility assignment

    Returns:
        Wide-format LazyFrame with puma_id and probability columns for each utility
    """
    # Join with utility name map
    probs = puma_overlap.join(
        utility_name_map, left_on="utility", right_on="state_name", how="left"
    )
    probs = probs.with_columns(
        pl.col("std_name").fill_null(pl.col("utility")).alias("utility")
    ).drop("std_name")

    # Handle municipal utilities if requested
    if handle_municipal:
        probs = probs.with_columns(
            pl.when(pl.col("utility").str.starts_with("Municipal Utility:"))
            .then(
                pl.concat_str(
                    [
                        pl.lit("muni-"),
                        pl.col("utility")
                        .str.replace("Municipal Utility:", "")
                        .str.strip_chars()
                        .str.to_lowercase(),
                    ]
                )
            )
            .otherwise(pl.col("utility"))
            .alias("utility")
        )

    # Aggregate overlap for duplicate (puma_id, utility) pairs
    # (can happen when overlay produces multiple intersection fragments for non-contiguous territories)
    probs = probs.group_by(["puma_id", "utility"]).agg(pl.col("pct_overlap").sum())

    # Calculate probability as percentage of overlap within each PUMA
    probs = probs.with_columns(
        (pl.col("pct_overlap") / pl.col("pct_overlap").sum().over("puma_id")).alias(
            "probability"
        )
    )

    # Select relevant columns
    probs = probs.select(["puma_id", "utility", "probability"])

    # Exclude municipal (muni-*) utilities from probabilities if requested
    if not include_municipal:
        probs = probs.filter(~pl.col("utility").str.contains("muni-"))

    # Filter out "none" utilities if requested
    if filter_none:
        probs = probs.filter(pl.col("utility") != "none")

    # Pivot to wide format (pivot must be done on collected DataFrame)
    probs_collected = cast(pl.DataFrame, probs.collect())
    probs_pivoted = probs_collected.pivot(
        index="puma_id", on="utility", values="probability", aggregate_function=None
    )
    probs = probs_pivoted.fill_null(0).lazy()

    return probs


def _sample_utility_per_building(
    bldgs: pl.LazyFrame,
    puma_probs: pl.LazyFrame,
    utility_col_name: str,
    only_when_fuel: str | None = None,
    seed: int = 42,
) -> pl.LazyFrame:
    """For each building, sample one utility from its PUMA's probability distribution.

    Args:
        bldgs: LazyFrame with bldg_id, puma, heating_fuel
        puma_probs: Wide-format LazyFrame with puma_id and probability columns for each utility
        utility_col_name: Name for the output utility column
        only_when_fuel: If provided, only sample when heating_fuel matches this value

    Returns:
        LazyFrame with bldg_id and the sampled utility column
    """
    # Join buildings with their PUMA probabilities
    bldgs_joined = bldgs.join(
        puma_probs, left_on="puma", right_on="puma_id", how="left"
    )

    # Get utility column names (all columns except puma_id from puma_probs)
    # Sort for deterministic ordering
    puma_probs_df = cast(pl.DataFrame, puma_probs.collect())
    utility_cols = sorted([c for c in puma_probs_df.columns if c != "puma_id"])

    # Convert to pandas for row-wise sampling with numpy (need to collect for pandas)
    bldgs_joined_df = cast(pl.DataFrame, bldgs_joined.collect())
    bldgs_pd = bldgs_joined_df.to_pandas()

    # Sort by bldg_id for deterministic ordering
    bldgs_pd = bldgs_pd.sort_values("bldg_id").reset_index(drop=True)

    # Set random seed for deterministic sampling
    np.random.seed(seed)

    def sample_utility(row) -> str | None:
        """Sample one utility based on probability distribution."""
        if only_when_fuel is not None:
            # For gas: only sample if heating_fuel matches
            if row["heating_fuel"] != only_when_fuel:
                return None

        # Get probabilities for this row and convert to numeric (float)
        probs = pd.to_numeric(row[utility_cols].values, errors="coerce").astype(float)

        # Handle cases where all probabilities are 0 or NaN
        if np.all(np.isnan(probs)) or np.sum(probs) == 0:
            return None

        # Replace NaN with 0 and normalize probabilities
        probs = np.nan_to_num(probs, nan=0.0)
        probs = probs / np.sum(probs)

        # Sample one utility based on probabilities (replace=False is explicit but doesn't matter for size=1)
        sampled_utility = np.random.choice(
            utility_cols, size=1, replace=False, p=probs
        )[0]
        return sampled_utility

    # Apply sampling to each row
    bldgs_pd[utility_col_name] = bldgs_pd.apply(sample_utility, axis=1)

    # Convert back to polars LazyFrame and select only needed columns
    result = pl.from_pandas(bldgs_pd[["bldg_id", utility_col_name]]).lazy()

    return result


def assign_utility_ny(
    input_metadata: pl.LazyFrame,
    electric_polygons: gpd.GeoDataFrame,
    gas_polygons: gpd.GeoDataFrame,
    pumas: gpd.GeoDataFrame,
    config: dict | None = None,
) -> pl.LazyFrame:
    """
    Assign electric and gas utilities to ResStock buildings and add columns to metadata.

    Creates utility assignments based on PUMA-level probabilities and joins them
    to the input metadata. Drops existing utility columns if present before joining.

    Args:
        input_metadata: input metadata LazyFrame
        electric_polygons: GeoDataFrame of electric utility service territories
        gas_polygons: GeoDataFrame of gas utility service territories
        pumas: GeoDataFrame of Census PUMAs
        config: Optional state configuration dictionary; defaults to CONFIGS

    Returns:
        LazyFrame with all original columns plus sb.electric_utility and sb.gas_utility
    """

    config = config or CONFIGS

    puma_and_heating_fuel = _select_puma_and_heating_fuel_metadata(input_metadata)
    building_utilities = create_hh_utilities(
        puma_and_heating_fuel=puma_and_heating_fuel,
        config=config,
        electric_polygons=electric_polygons,
        gas_polygons=gas_polygons,
        pumas=pumas,
    )

    # Drop existing utility columns if they exist (equivalent to ADD COLUMN IF NOT EXISTS)
    input_metadata = input_metadata.drop(
        ["sb.electric_utility", "sb.gas_utility"], strict=False
    )

    # Runtime check: ensure both LazyFrames have the same number of rows (one collect)
    counts_df = cast(
        pl.DataFrame,
        input_metadata.select(pl.lit(1).sum().alias("input_count"))
        .join(
            building_utilities.select(pl.lit(1).sum().alias("building_count")),
            how="cross",
        )
        .collect(),
    )
    input_count = cast(int, counts_df["input_count"][0])
    building_utilities_count = cast(int, counts_df["building_count"][0])
    if input_count != building_utilities_count:
        raise ValueError(
            f"Row count mismatch: input_metadata has {input_count} rows, "
            f"but building_utilities has {building_utilities_count} rows"
        )

    # Join utilities to metadata (equivalent to UPDATE ... FROM)
    metadata_with_utility_assignment = input_metadata.join(
        building_utilities.select(["bldg_id", "sb.electric_utility", "sb.gas_utility"]),
        on="bldg_id",
        how="left",
    )
    return metadata_with_utility_assignment


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Assign electric and gas utilities to ResStock buildings in NY"
    )
    parser.add_argument(
        "--input_metadata_dir",
        type=str,
        required=True,
        help="Input directory to metadata parquet file (S3Path format)",
    )
    parser.add_argument(
        "--input_metadata_filename",
        type=str,
        required=True,
        help="Input filename to metadata parquet file (e.g. metadata-sb.parquet)",
    )
    parser.add_argument(
        "--output_metadata_dir",
        type=str,
        required=True,
        help="Output directory for parquet file with utility assignment (S3Path format)",
    )
    parser.add_argument(
        "--output_utility_assignment_filename",
        type=str,
        required=True,
        help="Output filename for parquet file with utility assignment (e.g. utility_assignment.parquet)",
    )
    parser.add_argument(
        "--electric_poly_dir",
        type=str,
        required=True,
        help="Directory containing electric utility polygons (S3Path format)",
    )
    parser.add_argument(
        "--gas_poly_dir",
        type=str,
        required=True,
        help="Directory containing gas utility polygons (S3Path format)",
    )
    parser.add_argument(
        "--electric_poly_filename",
        type=str,
        required=True,
        help="Filename of electric utility polygon (e.g. ny_electric_utilities_20260216.csv)",
    )
    parser.add_argument(
        "--gas_poly_filename",
        type=str,
        required=True,
        help="Filename of gas utility polygon (e.g. ny_gas_utilities_20260216.csv)",
    )

    args = parser.parse_args()

    input_dir_s3 = S3Path(args.input_metadata_dir)
    input_path_s3 = S3Path(input_dir_s3 / args.input_metadata_filename)
    input_metadata = pl.scan_parquet(
        str(input_path_s3), storage_options=STORAGE_OPTIONS
    )

    output_dir_s3 = S3Path(args.output_metadata_dir)
    output_path_s3 = S3Path(output_dir_s3 / args.output_utility_assignment_filename)

    electric_poly_dir = S3Path(args.electric_poly_dir)
    gas_poly_dir = S3Path(args.gas_poly_dir)
    electric_poly_path = S3Path(electric_poly_dir / args.electric_poly_filename)
    gas_poly_path = S3Path(gas_poly_dir / args.gas_poly_filename)

    # Load utility polygons (.csv files from S3) to GeoDataFrame
    electric_polygons = read_csv_to_gdf_from_s3(
        electric_poly_path,
        utility_type="electric",
        geometry_col="the_geom",
        crs="EPSG:4326",
    )
    gas_polygons = read_csv_to_gdf_from_s3(
        gas_poly_path, utility_type="gas", geometry_col="the_geom", crs="EPSG:4326"
    )

    # Load PUMAS using pygris
    pumas = get_pumas(
        state=CONFIGS["state_code"],
        year=2019,
        cb=True,  # Use cartographic boundaries (simplified)
    )
    pumas = pumas.to_crs(epsg=CONFIGS["state_crs"])
    pumas["puma_area"] = pumas.geometry.area
    pumas = cast(gpd.GeoDataFrame, pumas)

    metadata_with_utility_assignment = assign_utility_ny(
        input_metadata=input_metadata,
        electric_polygons=electric_polygons,
        gas_polygons=gas_polygons,
        pumas=pumas,
        config=CONFIGS,
    )

    # Write back to the same location using sink_parquet
    metadata_with_utility_assignment.sink_parquet(
        str(output_path_s3), compression="zstd", storage_options=STORAGE_OPTIONS
    )
