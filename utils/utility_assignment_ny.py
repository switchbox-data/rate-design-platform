"""
Utility assignment for ResStock buildings (NY and MA).

Equivalent to utils/utility_assignment_ny.R.
For create_hh_utilities: requires geopandas; building data from S3 or local parquet.
For write_utilities_to_s3: requires S3Path.
"""

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
    "bldg_id_utility_mapping_output_path": "data.sb/nrel/resstock/<release>/utilities/<state>/utility_lookup.parquet",  # This is the output path
    "resstock_path": "/workspaces/reports/data/ResStock/2022_resstock_amy2018_release_1.1/20230922.db",  # change to S3 metadata path
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
    print(bldg_utility_mapping.head())

    feather_path = out_dir / "rs2024_bldg_utility_crosswalk.feather"
    csv_path = out_dir / "rs2024_bldg_utility_crosswalk.csv"
    bldg_utility_mapping.write_ipc(feather_path)
    bldg_utility_mapping.write_csv(csv_path)

    return bldg_utility_mapping


def get_bldg_by_utility(
    state_code: str,
    utility_electric: str | list[str] | None = None,
    utility_gas: str | list[str] | None = None,
    config: dict | None = None,
) -> pl.DataFrame:
    """
    Get buildings by utility service area.

    Returns bldg_id, sb.electric_utility, sb.gas_utility for buildings matching the
    given electric and/or gas utilities. If neither filter is given, returns all
    buildings in the state with their utilities.

    Args:
        state_code: "NY" or "MA".
        utility_electric: Electric utility std_name(s) to filter (e.g. "nimo", "coned").
        utility_gas: Gas utility std_name(s) to filter.
        config: State config dict; defaults to STATE_CONFIGS.

    Returns:
        DataFrame with columns bldg_id, sb.electric_utility, sb.gas_utility.
    """
    state_config = config or CONFIGS

    hh_path = Path(state_config["bldg_id_utility_mapping_output_path"])
    if not hh_path.exists():
        print("Creating new hh_utilities file")
        hh_df = create_hh_utilities(config=config)
    else:
        print("Loading existing hh_utilities file")
        hh_df = pl.read_csv(hh_path)

    elec_filter = True
    if utility_electric is not None:
        elec_list = (
            [utility_electric]
            if isinstance(utility_electric, str)
            else utility_electric
        )
        elec_filter = pl.col("sb.electric_utility").is_in(elec_list)
    gas_filter = True
    if utility_gas is not None:
        gas_list = [utility_gas] if isinstance(utility_gas, str) else utility_gas
        gas_filter = pl.col("sb.gas_utility").is_in(gas_list)

    return hh_df.filter(elec_filter & gas_filter).select(
        "bldg_id", "sb.electric_utility", "sb.gas_utility"
    )


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

    return raw.select(
        pl.col("bldg_id"),
        pl.col("in.puma").str.slice(-5).alias("puma"),
        pl.col("in.heating_fuel").alias("heating_fuel"),
    )


########################################################
# GIS Utility Mapping
########################################################
def create_hh_utilities(
    config: dict | None = None,
    puma_year: int = 2019,
    s3_path: str | Path | S3Path | None = None,
) -> pl.DataFrame:
    """
    Create a dataframe of households with their associated utilities.

    Uses Census PUMAs and utility service territory polygons to compute
    overlap, then assigns electric/gas utilities to ResStock buildings by
    sampling from PUMA-level probabilities. Requires geopandas and S3Path.

    Args:
        config: State config; defaults to CONFIGS (state is implied by config keys e.g. state_fips).
        puma_year: Year for PUMA boundaries.
        save_file: Whether to save the result to state_config["bldg_id_utility_mapping_output_path"].
        s3_path: S3 path or local path to a parquet file containing housing units
            with columns bldg_id, in.puma, in.heating_fuel.

    Returns:
        DataFrame with bldg_id, sb.electric_utility, sb.gas_utility.
    """

    config = config or CONFIGS

    utility_name_map = pl.DataFrame(
        config["utility_name_map"]
    )  # Need to update this path
    s3_path = s3_path or config["resstock_path"]

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

    # Assign electric and gas to bldg's
    building_elec = _sample_utility_per_building(
        bldg_ids_df, puma_elec_probs, "sb.electric_utility"
    )

    building_gas = _sample_utility_per_building(
        bldg_ids_df, puma_gas_probs, "sb.gas_utility", only_when_fuel="Natural Gas"
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


def _split_multi_service_areas(puma_utility_overlap: pl.DataFrame) -> pl.DataFrame:
    """Split rows with two utilities into two rows with half overlap each.

    Args:
        puma_utility_overlap: DataFrame with puma_id, utility, pct_overlap, multi_utility,
                             and utility_1, utility_2 columns

    Returns:
        DataFrame with puma_id, utility, pct_overlap where multi-utility areas are split
    """
    # Multi-utility areas: split into two rows with half overlap each
    multi = puma_utility_overlap.filter(pl.col("multi_utility") == 1).drop(
        "utility", "multi_utility"
    )

    # Get all columns that aren't puma_id or pct_overlap (these are utility_1, utility_2, etc.)
    utility_cols = [c for c in multi.columns if c not in ("puma_id", "pct_overlap")]

    # Pivot longer: each utility_1, utility_2 becomes a separate row
    multi = (
        multi.unpivot(
            index=["puma_id", "pct_overlap"], on=utility_cols, value_name="utility"
        )
        .drop("variable")
        .filter(pl.col("utility").is_not_null())  # Remove null utilities
        .with_columns((pl.col("pct_overlap") / 2).alias("pct_overlap"))
    )

    # Single-utility areas: keep as-is
    single = puma_utility_overlap.filter(pl.col("multi_utility") == 0).select(
        "puma_id", "utility", "pct_overlap"
    )

    # Combine and aggregate (in case same puma_id + utility appears multiple times)
    combined = pl.concat([multi, single])

    return combined.group_by(["puma_id", "utility"]).agg(pl.col("pct_overlap").sum())


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
    housing_units.write_parquet(str(output_path_s3), storage_options=STORAGE_OPTIONS)


if __name__ == "__main__":
    path_s3 = S3Path("s3://data.sb/nrel/resstock/")
    state = "NY"
    upgrade_id = "00"
    release = "res_2024_amy2018_2"
    path_release = path_s3 / release
    input_metadata_path = (
        path_release
        / "metadata"
        / f"state={state}"
        / f"upgrade={upgrade_id}"
        / "metadata-sb.parquet"
    )

    output_metadata_path = S3Path(
        path_release
        / "metadata"
        / f"state={state}"
        / f"upgrade={upgrade_id}"
        / "metadata-sb-with-utilities.parquet"
    )

    write_utilities_to_s3(
        input_path_s3=input_metadata_path,
        output_path_s3=output_metadata_path,
        config=CONFIGS,
    )
