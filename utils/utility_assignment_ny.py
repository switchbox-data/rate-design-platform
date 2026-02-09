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


########################################################
# Making or updating utility crosswalks
########################################################
def make_empty_utility_crosswalk(path_to_rs2024_metadata: str | Path) -> pl.DataFrame:
    """
    Make an empty utility crosswalk CSV/feather file.

    Reads the ResStock metadata parquet, adds empty electric_utility and gas_utility
    columns, and writes rs2024_bldg_utility_crosswalk.feather and .csv to the same dir.

    Args:
        path_to_rs2024_metadata: Directory containing metadata.parquet (or path to metadata.parquet).

    Returns:
        DataFrame with bldg_id, in.state, in.heating_fuel, out.natural_gas..., electric_utility, gas_utility.
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
        pl.lit(None).cast(pl.Utf8).alias("electric_utility"),
        pl.lit(None).cast(pl.Utf8).alias("gas_utility"),
    )
    print(bldg_utility_mapping.head())

    feather_path = out_dir / "rs2024_bldg_utility_crosswalk.feather"
    csv_path = out_dir / "rs2024_bldg_utility_crosswalk.csv"
    bldg_utility_mapping.write_ipc(feather_path)
    bldg_utility_mapping.write_csv(csv_path)

    return bldg_utility_mapping


########################################################
# Forced Utility Mapping
########################################################
def forced_utility_crosswalk_ri(path_to_rs2024_metadata: str | Path) -> pl.DataFrame:
    """
    Apply forced utility mapping for RI: electric and gas set to rhode_island_energy
    where state is RI; gas only where natural gas consumption > 10.
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
    out_dir = parquet_path.parent if parquet_path.suffix else path

    bldg_utility_mapping = pl.read_parquet(
        parquet_path, columns=USE_THESE_COLUMNS
    ).with_columns(
        pl.lit(None).cast(pl.Utf8).alias("electric_utility"),
        pl.lit(None).cast(pl.Utf8).alias("gas_utility"),
    )

    ng_col = "out.natural_gas.total.energy_consumption.kwh"
    bldg_utility_mapping = bldg_utility_mapping.with_columns(
        pl.when(pl.col("in.state") == "RI")
        .then(pl.lit("rhode_island_energy"))
        .otherwise(pl.col("electric_utility"))
        .alias("electric_utility"),
        pl.when((pl.col("in.state") == "RI") & (pl.col(ng_col) > 10))
        .then(pl.lit("rhode_island_energy"))
        .otherwise(pl.col("gas_utility"))
        .alias("gas_utility"),
    )

    bldg_utility_mapping.write_ipc(out_dir / "rs2024_bldg_utility_crosswalk.feather")
    bldg_utility_mapping.write_csv(out_dir / "rs2024_bldg_utility_crosswalk.csv")
    return bldg_utility_mapping


########################################################
# GIS Utility Mapping
########################################################
STATE_CONFIGS: dict[str, dict] = {
    "NY": {
        "state_fips": "36",
        "state_crs": 2260,  # New York state plane (meters)
        "hh_utilities_path": "/workspaces/reports/data/ResStock/utility_lookups/NY_hh_utilities.csv",  # Need to update this path
        "resstock_path": "/workspaces/reports/data/ResStock/2022_resstock_amy2018_release_1.1/20230922.db",  # Need to update this path
        "electric_poly_path": "/workspaces/reports/data/buildings2/Utilities/NYS_Electric_Utility_Service_Territories.csv",  # Need to update this path
        "gas_poly_path": "/workspaces/reports/data/buildings2/Utilities/NYS_Gas_Utility_Service_Territories.csv",  # Need to update this path
        "utility_name_map": [
            {"state_name": "Bath Electric Gas and Water", "std_name": "bath"},
            {"state_name": "Central Hudson Gas and Electric", "std_name": "cenhud"},
            {"state_name": "Chautauqua Utilities, Inc.", "std_name": "chautauqua"},
            {"state_name": "Consolidated Edison", "std_name": "coned"},
            {"state_name": "Corning Natural Gas", "std_name": "corning"},
            {"state_name": "Fillmore Gas Company", "std_name": "fillmore"},
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
            {"state_name": "Reserve Gas Company", "std_name": "reserve"},
            {"state_name": "Rochester Gas and Electric", "std_name": "rge"},
            {"state_name": "St. Lawrence Gas", "std_name": "stlawrence"},
            {"state_name": "Valley Energy", "std_name": "valley"},
            {"state_name": "Woodhull Municipal Gas Company", "std_name": "woodhull"},
        ],
    },
    "MA": {
        "state_fips": "25",
        "state_crs": 26986,  # Massachusetts state plane (meters)
        "hh_utilities_path": "/workspaces/reports/data/ResStock/utility_lookups/MA_hh_utilities.csv",  # Need to update this path
        "resstock_path": "/workspaces/reports/data/ResStock/2022_resstock_amy2018_release_1.1/rs_20250326.db",  # Need to update this path
        "electric_poly_path": "/workspaces/reports/data/datamagov/MA_utility_territory_shapefiles_20250326/TOWNS_POLY_V_ELEC.shp",  # Need to update this path
        "gas_poly_path": "/workspaces/reports/data/datamagov/MA_utility_territory_shapefiles_20250326/TOWNS_POLY_V_GAS.shp",  # Need to update this path
        "utility_name_map": [
            {"state_name": "The Berkshire Gas Company", "std_name": "berkshire"},
            {"state_name": "Eversource Energy", "std_name": "eversource"},
            {
                "state_name": "NSTAR Electric d/b/a Eversource Energy",
                "std_name": "eversource",
            },
            {"state_name": "Liberty Utilities", "std_name": "liberty"},
            {"state_name": "Municipal", "std_name": "municipal"},
            {"state_name": "National Grid", "std_name": "nationalgrid"},
            {
                "state_name": "Massachusetts Electric d/b/a National Grid",
                "std_name": "nationalgrid",
            },
            {
                "state_name": "Nantucket Electric Company d/b/a National Grid",
                "std_name": "nationalgrid",
            },
            {"state_name": "No Natural Gas Service", "std_name": "none"},
            {"state_name": "Unitil", "std_name": "unitil"},
            {"state_name": "UNITIL", "std_name": "unitil"},
        ],
    },
}


def get_bldg_by_utility(
    state_code: str,
    utility_electric: str | list[str] | None = None,
    utility_gas: str | list[str] | None = None,
    config: dict | None = None,
) -> pl.DataFrame:
    """
    Get buildings by utility service area.

    Returns bldg_id, electric_utility, gas_utility for buildings matching the
    given electric and/or gas utilities. If neither filter is given, returns all
    buildings in the state with their utilities.

    Args:
        state_code: "NY" or "MA".
        utility_electric: Electric utility std_name(s) to filter (e.g. "nimo", "coned").
        utility_gas: Gas utility std_name(s) to filter.
        config: State config dict; defaults to STATE_CONFIGS.

    Returns:
        DataFrame with columns bldg_id, electric_utility, gas_utility.
    """
    config = config or STATE_CONFIGS
    state_config = config.get(state_code)
    if state_config is None:
        raise ValueError(f"No configuration available for state: {state_code}")

    hh_path = Path(state_config["hh_utilities_path"])
    if not hh_path.exists():
        print("Creating new hh_utilities file")
        hh_df = create_hh_utilities(state_code=state_code, config=config)
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
        elec_filter = pl.col("electric_utility").is_in(elec_list)
    gas_filter = True
    if utility_gas is not None:
        gas_list = [utility_gas] if isinstance(utility_gas, str) else utility_gas
        gas_filter = pl.col("gas_utility").is_in(gas_list)

    return hh_df.filter(elec_filter & gas_filter).select(
        "bldg_id", "electric_utility", "gas_utility"
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
        raw = pl.read_parquet(s3_path)

    return raw.select(
        pl.col("bldg_id"),
        pl.col("in.puma").str.slice(-5).alias("puma"),
        pl.col("in.heating_fuel").alias("heating_fuel"),
    )


def create_hh_utilities(
    state_code: str,
    config: dict | None = None,
    puma_year: int = 2019,
    save_file: bool = True,
    s3_path: str | Path | S3Path | None = None,
) -> pl.DataFrame:
    """
    Create a dataframe of households with their associated utilities.

    Uses Census PUMAs and utility service territory polygons to compute
    overlap, then assigns electric/gas utilities to ResStock buildings by
    sampling from PUMA-level probabilities. Requires geopandas and S3Path.

    Args:
        state_code: "NY" or "MA".
        config: State config; defaults to STATE_CONFIGS.
        puma_year: Year for PUMA boundaries.
        save_file: Whether to save the result to state_config["hh_utilities_path"].
        s3_path: S3 path or local path to a parquet file containing housing units
            with columns bldg_id, in.puma, in.heating_fuel.

    Returns:
        DataFrame with bldg_id, electric_utility, gas_utility.
    """

    config = config or STATE_CONFIGS
    state_config = config.get(state_code)
    if state_config is None:
        raise ValueError(f"No configuration available for state: {state_code}")

    utility_name_map = pl.DataFrame(
        state_config["utility_name_map"]
    )  # Need to update this path
    s3_path = s3_path or state_config["resstock_path"]

    # Load PUMAs (Census TIGER; tigris equivalent in Python via census-data or URL)
    # Using geopandas read_file with Census TIGER PUMAs URL or local path
    pumas = gpd.read_file(
        f"https://www2.census.gov/geo/tiger/TIGER2023/PUMA/tl_2023_{state_config['state_fips']}_puma10.zip"
    )
    pumas = pumas.to_crs(epsg=state_config["state_crs"])
    pumas["puma_area"] = pumas.geometry.area

    # Load utility polygons (NY: CSV with WKT the_geom; MA: shapefile merged by utility)
    if state_code == "MA":
        electric_gdf = _merge_ma_electric_polygons(state_config["electric_poly_path"])
        gas_gdf = _merge_ma_gas_polygons(state_config["gas_poly_path"])
    else:
        electric_df = pd.read_csv(state_config["electric_poly_path"])
        electric_gdf = gpd.GeoDataFrame(
            electric_df,
            geometry=gpd.GeoSeries.from_wkt(electric_df["the_geom"]),
            crs="EPSG:4326",
        ).to_crs(epsg=state_config["state_crs"])
        electric_gdf = gpd.GeoDataFrame(
            electric_gdf.rename(columns={"COMP_FULL": "utility"}),
            geometry=electric_gdf.geometry,
            crs=electric_gdf.crs,
        )

        gas_df = pd.read_csv(state_config["gas_poly_path"])
        gas_gdf = gpd.GeoDataFrame(
            gas_df,
            geometry=gpd.GeoSeries.from_wkt(gas_df["the_geom"]),
            crs="EPSG:4326",
        ).to_crs(epsg=state_config["state_crs"])
        gas_gdf = gpd.GeoDataFrame(
            gas_gdf.rename(columns={"COMP_FULL": "utility"}),
            geometry=gas_gdf.geometry,
            crs=gas_gdf.crs,
        )

    # Calculate overlap between PUMAs and utilities
    puma_elec_overlap = _calculate_puma_utility_overlap(
        pumas, electric_gdf, state_config["state_crs"]
    )

    puma_gas_overlap = _calculate_puma_utility_overlap(
        pumas, gas_gdf, state_config["state_crs"]
    )

    if state_code == "MA":
        puma_elec_overlap = _split_multi_service_areas(puma_elec_overlap)
        puma_gas_overlap = _split_multi_service_areas(puma_gas_overlap)

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
        bldg_ids_df, puma_elec_probs, "electric_utility"
    )

    building_gas = _sample_utility_per_building(
        bldg_ids_df, puma_gas_probs, "gas_utility", only_when_fuel="Natural Gas"
    )

    building_utilities = building_elec.join(
        building_gas.select("bldg_id", "gas_utility"), on="bldg_id", how="left"
    )

    if save_file:
        out_path = Path(state_config["hh_utilities_path"])  # Need to update this path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        building_utilities.write_csv(out_path)

    return building_utilities


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

    # Convert to polars
    return pl.from_pandas(puma_overlap)


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

        # Get probabilities for this row
        probs = row[utility_cols].values

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


def _merge_ma_electric_polygons(electric_poly_path: str | Path) -> "gpd.GeoDataFrame":
    """MA electric polygons: read shapefile, merge by utility label."""

    gdf = gpd.read_file(electric_poly_path)
    gdf["utility_1"] = (
        gdf["ELEC_LABEL"].str.extract(r"^([^,]+)", expand=False).str.strip()
    )
    gdf["utility_2"] = (
        gdf["ELEC_LABEL"].str.extract(r", (.+)", expand=False).str.strip()
    )
    gdf["multi_utility"] = gdf["utility_2"].notna().astype(int)
    gdf = gdf.rename(columns={"ELEC_LABEL": "utility"})

    # Group by utility and aggregate to match R's summarise behavior
    merged = gdf.dissolve(
        by="utility",
        aggfunc={"utility_1": "first", "utility_2": "first", "multi_utility": "first"},
    )

    # Add n_towns count
    merged["n_towns"] = gdf.groupby("utility").size()

    # Keep only the columns that R keeps (plus geometry which is automatic)
    merged = merged[["n_towns", "utility_1", "utility_2", "multi_utility"]]

    # Reset index to make 'utility' a column instead of the index
    merged = merged.reset_index()

    return merged


def _merge_ma_gas_polygons(gas_poly_path: str | Path) -> "gpd.GeoDataFrame":
    """MA gas polygons: read shapefile, merge by utility label."""

    gdf = gpd.read_file(gas_poly_path)
    gdf["utility_1"] = (
        gdf["GAS_LABEL"].str.extract(r"^([^,]+)", expand=False).str.strip()
    )
    gdf["utility_2"] = gdf["GAS_LABEL"].str.extract(r", (.+)", expand=False).str.strip()
    gdf["multi_utility"] = gdf["utility_2"].notna().astype(int)
    gdf = gdf.rename(columns={"GAS_LABEL": "utility"})

    # Group by utility and aggregate to match R's summarise behavior
    merged = gdf.dissolve(
        by="utility",
        aggfunc={"utility_1": "first", "utility_2": "first", "multi_utility": "first"},
    )

    # Add n_towns count
    merged["n_towns"] = gdf.groupby("utility").size()

    # Keep only the columns that R keeps (plus geometry which is automatic)
    merged = merged[["n_towns", "utility_1", "utility_2", "multi_utility"]]

    # Reset index to make 'utility' a column instead of the index
    merged = merged.reset_index()

    return merged


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
    state_code: str, s3_path: str | Path | S3Path, config: dict | None = None
) -> None:
    """
    Create hh_utilities for the state and write electric_utility, gas_utility
    to the housing_units parquet file.

    Args:
        state_code: Two-letter state code
        s3_path: Path to housing_units parquet file (local Path or S3Path)
        config: Optional state configuration dictionary
    """

    config = config or STATE_CONFIGS
    if state_code not in config:
        print(
            f"Cannot add utilities, state {state_code} not supported. "
            f"Only {' and '.join(config.keys())} are currently supported."
        )
        return None

    building_utilities = create_hh_utilities(state_code, s3_path=s3_path, config=config)

    # Convert to S3Path if needed
    if isinstance(s3_path, str):
        s3_path = S3Path(s3_path) if s3_path.startswith("s3://") else Path(s3_path)

    # Read existing housing_units
    housing_units = pl.read_parquet(str(s3_path))

    # Drop existing utility columns if they exist (equivalent to ADD COLUMN IF NOT EXISTS)
    if "electric_utility" in housing_units.columns:
        housing_units = housing_units.drop("electric_utility")
    if "gas_utility" in housing_units.columns:
        housing_units = housing_units.drop("gas_utility")

    # Join utilities to housing_units (equivalent to UPDATE ... FROM)
    housing_units = housing_units.join(
        building_utilities.select(["bldg_id", "electric_utility", "gas_utility"]),
        on="bldg_id",
        how="left",
    )

    # Write back to the same location
    housing_units.write_parquet(str(s3_path))
