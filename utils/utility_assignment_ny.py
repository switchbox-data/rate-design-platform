"""
Utility assignment for ResStock buildings (NY and MA).

Equivalent to utils/utility_assignment_ny.R.
For create_hh_utilities: requires geopandas; building data from S3 or local parquet.
For write_utilities_to_s3: requires S3Path.
"""

import argparse
import io
from pathlib import Path

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
    "electric_poly_path": "s3://data.sb/utility_territories/ny/NYS_Electric_Utility_Service_Territories.csv",
    "gas_poly_path": "s3://data.sb/utility_territories/ny/NYS_Electric_Utility_Service_Territories.csv",
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
            "std_name": "nationalfuel",
        },
        {"state_name": "NYS Electric and Gas", "std_name": "nyseg"},
        {"state_name": "Orange and Rockland Utilities", "std_name": "or"},
        {"state_name": "Long Island Power Authority", "std_name": "pseg-li"},
        {
            "state_name": "Reserve Gas Company",
            "std_name": "reserve",
        },  # TODO: Don't include in the mapping. Leave it blank/unassigned, then nearest neighbor to the nearest puma polygon, then assign based on that polygon's assignment probability
        {"state_name": "Rochester Gas and Electric", "std_name": "rge"},
        {
            "state_name": "St. Lawrence Gas",
            "std_name": "stlawrence",
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


########################################################
# Making or updating utility crosswalks
########################################################
def make_empty_utility_crosswalk(path_to_rs2024_metadata: str | Path) -> pl.DataFrame:
    """
    Make an empty utility crosswalk CSV/feather file.

    Reads the ResStock metadata parquet, adds empty sb.electric_utility and sb.gas_utility
    columns, and writes rs2024_bldg_utility_crosswalk.feather and .csv to the same dir.

    Args:
        path_to_rs2024_metadata: Directory containing metadata.parquet (or path to metadata.parquet).

    Returns:
        DataFrame with bldg_id, in.state, in.heating_fuel, out.natural_gas..., sb.electric_utility, sb.gas_utility.
    """

    USE_THESE_COLUMNS = [
        "bldg_id",
        "in.state",
        "in.heating_fuel",
        "out.natural_gas.total.energy_consumption.kwh",
    ]

    path = Path(path_to_rs2024_metadata)
    if path.is_dir():
        parquet_path = path / "metadata.parquet"
    else:
        parquet_path = path

    bldg_utility_mapping = pl.read_parquet(parquet_path, columns=USE_THESE_COLUMNS)
    out_dir = parquet_path.parent if parquet_path.suffix else path

    bldg_utility_mapping = bldg_utility_mapping.with_columns(
        pl.lit(None).cast(pl.Utf8).alias("sb.electric_utility"),
        pl.lit(None).cast(pl.Utf8).alias("sb.gas_utility"),
    )

    feather_path = out_dir / "rs2024_bldg_utility_crosswalk.feather"
    csv_path = out_dir / "rs2024_bldg_utility_crosswalk.csv"
    bldg_utility_mapping.write_ipc(feather_path)
    bldg_utility_mapping.write_csv(csv_path)

    return bldg_utility_mapping


def _read_housing_parquet(s3_path: str | Path | S3Path) -> pl.DataFrame:
    """
    Read housing units from a parquet file (S3 or local).

    Expects columns bldg_id, in.puma, in.heating_fuel. Returns DataFrame with
    bldg_id, puma (last 5 chars of in.puma), heating_fuel.
    """
    path_str = str(s3_path)
    if path_str.startswith("s3://"):
        path = S3Path(path_str) if not isinstance(s3_path, S3Path) else s3_path
        raw = pl.read_parquet(io.BytesIO(path.read_bytes()))
    else:
        raw = pl.read_parquet(str(s3_path))

    result = raw.select(
        pl.col("bldg_id"),
        pl.col("in.puma").str.slice(-5).alias("puma"),
        pl.col("in.heating_fuel").alias("heating_fuel"),
    )

    return result


########################################################
# GIS Utility Mapping
########################################################
def create_hh_utilities(
    s3_path: str | Path | S3Path,
    config: dict | None = None,
    puma_year: int = 2019,
) -> pl.DataFrame:
    """
    Create a dataframe of households with their associated utilities.

    Uses Census PUMAs and utility service territory polygons to compute
    overlap, then assigns electric/gas utilities to ResStock buildings by
    sampling from PUMA-level probabilities. Requires geopandas and S3Path.

    Args:
        config: State config; defaults to CONFIGS (state is implied by config keys e.g. state_fips).
        puma_year: Year for PUMA boundaries.
        s3_path: S3 path or local path to a parquet file containing housing units
            with columns bldg_id, in.puma, in.heating_fuel.

    Returns:
        DataFrame with bldg_id, sb.electric_utility, sb.gas_utility.
    """

    config = config or CONFIGS

    utility_name_map = pl.DataFrame(
        config["utility_name_map"]
    )  # Need to update this path
    s3_path = s3_path

    # Load PUMAS using pygris
    pumas = get_pumas(
        state=config["state_code"],
        year=puma_year,
        cb=True,  # Use cartographic boundaries (simplified)
    )
    pumas = pumas.to_crs(epsg=config["state_crs"])
    pumas["puma_area"] = pumas.geometry.area

    # Load utility polygons (.csv files from S3)
    electric_gdf = read_csv_to_gdf_from_s3(
        config["electric_poly_path"], geometry_col="the_geom", crs="EPSG:4326"
    )
    electric_gdf = electric_gdf.to_crs(epsg=config["state_crs"])
    electric_gdf = electric_gdf.rename(columns={"COMP_FULL": "utility"})

    gas_gdf = read_csv_to_gdf_from_s3(
        config["gas_poly_path"], geometry_col="the_geom", crs="EPSG:4326"
    )
    gas_gdf = gas_gdf.to_crs(epsg=config["state_crs"])
    gas_gdf = gas_gdf.rename(columns={"COMP_FULL": "utility"})

    # Calculate overlap between PUMAs and utilities
    puma_elec_overlap = _calculate_puma_utility_overlap(
        pumas, electric_gdf, config["state_crs"]
    )

    puma_gas_overlap = _calculate_puma_utility_overlap(
        pumas, gas_gdf, config["state_crs"]
    )

    # Electric utility probabilities
    puma_elec_probs = _calculate_utility_probabilities(
        puma_elec_overlap,
        utility_name_map,
        handle_municipal=True,
        filter_none=False,
    )

    puma_gas_probs = _calculate_utility_probabilities(
        puma_gas_overlap,
        utility_name_map,
        handle_municipal=False,
        filter_none=True,
    )

    # ResStock buildings from parquet (S3 or local)
    bldg_ids_df = _read_housing_parquet(s3_path)

    # Calculate prior probability distributions
    elec_prior_weighted, gas_prior_weighted = _calculate_prior_distributions(
        puma_elec_probs, puma_gas_probs, bldg_ids_df
    )

    # Assign electric and gas to bldg's
    building_elec = _sample_utility_per_building(
        bldg_ids_df, puma_elec_probs, "sb.electric_utility"
    )

    building_gas = _sample_utility_per_building(
        bldg_ids_df, puma_gas_probs, "sb.gas_utility", only_when_fuel="Natural Gas"
    )

    # Print comparison summary
    _print_comparison_summary(
        building_elec,
        building_gas,
        elec_prior_weighted,
        gas_prior_weighted,
        bldg_ids_df,
    )

    building_utilities = building_elec.join(
        building_gas.select("bldg_id", "sb.gas_utility"), on="bldg_id", how="left"
    )

    return building_utilities


def read_csv_to_gdf_from_s3(s3_path, geometry_col="the_geom", crs="EPSG:4326"):
    """
    Read a CSV file from S3 with WKT geometry and convert to GeoDataFrame.

    Parameters
    ----------
    s3_path : str
        S3 path to the CSV file (e.g., 's3://bucket/path/file.csv')
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
    df = pd.read_csv(s3_path, low_memory=False)

    # Convert numeric columns that might have been read as strings
    # Skip geometry column and known string columns (like COMP_FULL/utility names)
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

    return gdf


def _calculate_puma_utility_overlap(
    pumas: gpd.GeoDataFrame,
    utility_gdf: gpd.GeoDataFrame,
    state_crs: int,
) -> pl.DataFrame:
    """Calculate overlap between PUMAs and utility service territories.

    Args:
        pumas: GeoDataFrame of PUMAs
        utility_gdf: GeoDataFrame of utility polygons
        state_crs: State-specific CRS (EPSG code) for accurate area calculations

    Returns:
        Polars DataFrame with puma_id, pct_overlap, and all utility columns
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

    # Convert to polars (utility column should already be string from source CSV)
    return pl.from_pandas(puma_overlap)


def _calculate_prior_distributions(
    puma_elec_probs: pl.DataFrame,
    puma_gas_probs: pl.DataFrame,
    bldg_ids_df: pl.DataFrame,
) -> tuple[dict[str, float], dict[str, float]]:
    """Calculate prior probability distributions for electric and gas utilities.

    Args:
        puma_elec_probs: Wide-format DataFrame with puma_id and electric utility probability columns
        puma_gas_probs: Wide-format DataFrame with puma_id and gas utility probability columns
        bldg_ids_df: DataFrame with bldg_id, puma, heating_fuel

    Returns:
        Tuple of (elec_prior_weighted dict, gas_prior_weighted dict) for later comparison
    """
    # Electric: Calculate weighted average probability across all PUMAs
    # (weighted by number of buildings in each PUMA)
    puma_counts = bldg_ids_df.group_by("puma").agg(pl.len().alias("count"))

    elec_prior = puma_elec_probs.join(
        puma_counts, left_on="puma_id", right_on="puma", how="left"
    ).with_columns(pl.col("count").fill_null(0))
    utility_cols_elec = [c for c in puma_elec_probs.columns if c != "puma_id"]
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
    gas_bldgs = bldg_ids_df.filter(pl.col("heating_fuel") == "Natural Gas")
    gas_puma_counts = gas_bldgs.group_by("puma").agg(pl.len().alias("count"))

    gas_prior = puma_gas_probs.join(
        gas_puma_counts, left_on="puma_id", right_on="puma", how="left"
    ).with_columns(pl.col("count").fill_null(0))
    utility_cols_gas = [c for c in puma_gas_probs.columns if c != "puma_id"]
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
    building_elec: pl.DataFrame,
    building_gas: pl.DataFrame,
    elec_prior_weighted: dict[str, float],
    gas_prior_weighted: dict[str, float],
    bldg_ids_df: pl.DataFrame,
) -> None:
    """Print comparison summary showing prior vs posterior distributions in a formatted table.

    This summary compares the expected probability distribution (prior) with the actual
    assignment distribution (posterior) to verify that sampling matches the intended probabilities.

    Args:
        building_elec: DataFrame with bldg_id, sb.electric_utility
        building_gas: DataFrame with bldg_id, sb.gas_utility
        elec_prior_weighted: Dictionary mapping electric utility names to prior probabilities
        gas_prior_weighted: Dictionary mapping gas utility names to prior probabilities
        bldg_ids_df: DataFrame with bldg_id, puma, heating_fuel (for filtering by fuel type)
    """

    def _print_comparison_table(
        utility_type: str,
        building_df: pl.DataFrame,
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
                bldg_ids_df.select("bldg_id", "heating_fuel"),
                on="bldg_id",
                how="left",
            ).filter(pl.col("heating_fuel") == filter_fuel_type)
        else:
            building_df_filtered = building_df

        # Calculate posterior distribution (only on assigned buildings, not NULL)
        posterior_df = (
            building_df_filtered.filter(pl.col(utility_col).is_not_null())
            .group_by(utility_col)
            .agg(pl.len().alias("count"))
            .with_columns((pl.col("count") / pl.col("count").sum()).alias("proportion"))
            .sort("proportion", descending=True)
        )

        # Build comparison data
        all_utils = sorted(
            set(
                list(prior_weighted.keys())
                + [
                    r[utility_col]
                    for r in posterior_df.iter_rows(named=True)
                    if r[utility_col] is not None
                ]
            )
        )

        # Get total number of buildings (filtered by fuel type if specified)
        total_buildings = len(building_df_filtered)

        # For gas, only count buildings that should get assigned (Natural Gas)
        if filter_fuel_type is not None:
            assigned_buildings = building_df_filtered.filter(
                pl.col(utility_col).is_not_null()
            ).height
            print(
                f"\nNote: Comparing only {filter_fuel_type} buildings "
                f"({assigned_buildings} assigned out of {total_buildings} total)"
            )

        comparisons = []
        differences = []
        for util in all_utils:
            prior_prob = prior_weighted.get(util, 0.0)
            prior_pct = prior_prob * 100
            expected_count = prior_prob * total_buildings

            post_row = next(
                (
                    r
                    for r in posterior_df.iter_rows(named=True)
                    if r[utility_col] == util
                ),
                None,
            )
            posterior_pct = post_row["proportion"] * 100 if post_row else 0.0
            actual_count = post_row["count"] if post_row else 0
            diff_pct = posterior_pct - prior_pct
            diff_count = actual_count - expected_count
            differences.append(abs(diff_pct))
            comparisons.append(
                {
                    "utility": util,
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


# TODO: Check the posterior distribution matches the prior probabilities of utilities per PUMA district
def _calculate_utility_probabilities(
    puma_overlap: pl.DataFrame,
    utility_name_map: pl.DataFrame,
    handle_municipal: bool = True,
    filter_none: bool = False,
) -> pl.DataFrame:
    """Calculate utility probabilities for each PUMA based on overlap percentages.

    Args:
        puma_overlap: DataFrame with puma_id, utility, pct_overlap columns
        utility_name_map: Mapping from state_name to std_name for utilities
        handle_municipal: Whether to transform "Municipal Utility:" names to "muni-" format
        filter_none: Whether to filter out utilities named "none"

    Returns:
        Wide-format DataFrame with puma_id and probability columns for each utility
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

    # Calculate probability as percentage of overlap within each PUMA
    probs = probs.with_columns(
        (pl.col("pct_overlap") / pl.col("pct_overlap").sum().over("puma_id")).alias(
            "probability"
        )
    )

    # Select relevant columns
    probs = probs.select(["puma_id", "utility", "probability"])

    # Filter out "none" utilities if requested
    if filter_none:
        probs = probs.filter(pl.col("utility") != "none")

    # Pivot to wide format
    probs = probs.pivot(index="puma_id", on="utility", values="probability").fill_null(
        0
    )

    return probs


def _sample_utility_per_building(
    bldgs: pl.DataFrame,
    puma_probs: pl.DataFrame,
    utility_col_name: str,
    only_when_fuel: str | None = None,
) -> pl.DataFrame:
    """For each building, sample one utility from its PUMA's probability distribution.

    Args:
        bldgs: DataFrame with bldg_id, puma, heating_fuel
        puma_probs: Wide-format DataFrame with puma_id and probability columns for each utility
        utility_col_name: Name for the output utility column
        only_when_fuel: If provided, only sample when heating_fuel matches this value

    Returns:
        DataFrame with bldg_id and the sampled utility column
    """
    # Join buildings with their PUMA probabilities
    bldgs_joined = bldgs.join(
        puma_probs, left_on="puma", right_on="puma_id", how="left"
    )

    # Get utility column names (all columns except puma_id from puma_probs)
    utility_cols = [c for c in puma_probs.columns if c != "puma_id"]

    # Convert to pandas for row-wise sampling with numpy
    bldgs_pd = bldgs_joined.to_pandas()

    def sample_utility(row):
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

    # Convert back to polars and select only needed columns
    result = pl.from_pandas(bldgs_pd[["bldg_id", utility_col_name]])

    return result


def write_utilities_to_s3(
    input_path_s3: str | Path | S3Path,
    output_path_s3: str | Path | S3Path,
    config: dict | None = None,
) -> None:
    """
    Create hh_utilities for the state and write sb.electric_utility, sb.gas_utility
    to the housing_units parquet file.

    Args:
        state_code: Two-letter state code
        s3_path: Path to housing_units parquet file (local Path or S3Path)
        config: Optional state configuration dictionary
    """

    config = config or CONFIGS

    building_utilities = create_hh_utilities(s3_path=input_path_s3, config=config)

    # Convert to S3Path if needed
    if isinstance(input_path_s3, str):
        input_path_s3 = (
            S3Path(input_path_s3)
            if input_path_s3.startswith("s3://")
            else Path(input_path_s3)
        )

    # Read existing housing_units
    housing_units = pl.read_parquet(str(input_path_s3), storage_options=STORAGE_OPTIONS)

    # Drop existing utility columns if they exist (equivalent to ADD COLUMN IF NOT EXISTS)
    if "sb.electric_utility" in housing_units.columns:
        housing_units = housing_units.drop("sb.electric_utility")
    if "sb.gas_utility" in housing_units.columns:
        housing_units = housing_units.drop("sb.gas_utility")

    # Join utilities to housing_units (equivalent to UPDATE ... FROM)
    housing_units = housing_units.join(
        building_utilities.select(["bldg_id", "sb.electric_utility", "sb.gas_utility"]),
        on="bldg_id",
        how="left",
    )

    # Write back to the same location
    housing_units.write_parquet(
        str(output_path_s3),
        storage_options=STORAGE_OPTIONS,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Assign electric and gas utilities to ResStock buildings in NY"
    )
    parser.add_argument(
        "--input_path",
        type=str,
        required=True,
        help="Input path to metadata parquet file (S3 or local)",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        required=True,
        help="Output path for parquet file with utilities (S3 or local)",
    )
    parser.add_argument(
        "--state",
        type=str,
        required=True,
        help="State code (e.g., 'NY')",
    )
    parser.add_argument(
        "--release",
        type=str,
        default="res_2024_amy2018_2",
        help="ResStock release name (default: res_2024_amy2018_2)",
    )
    parser.add_argument(
        "--upgrade_id",
        type=str,
        help="Upgrade ID (e.g., '00')",
    )

    args = parser.parse_args()

    write_utilities_to_s3(
        input_path_s3=args.input_path,
        output_path_s3=args.output_path,
        config=CONFIGS,
    )
