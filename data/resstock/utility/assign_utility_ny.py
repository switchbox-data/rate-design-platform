"""Utility assignment for ResStock buildings (NY).

Thin wrapper around generic utility assignment functions in
``data.resstock.utility.utils``, providing NY-specific configuration
(utility name crosswalk, excluded gas utilities).
"""

import argparse
from typing import cast

import geopandas as gpd
import polars as pl
from cloudpathlib import S3Path
from pygris import pumas as get_pumas

from data.resstock.utils import (
    load_state_configs,
    select_puma_and_heating_fuel_metadata,
)
from data.resstock.utility.utils import (
    create_hh_utilities,
    read_csv_to_gdf_from_s3,
)
from utils import get_aws_region
from utils.utility_codes import get_ny_open_data_to_std_name

STORAGE_OPTIONS = {"aws_region": get_aws_region()}

EXCLUDED_GAS_UTILITIES: frozenset[str] = frozenset(
    load_state_configs()["NY"]["excluded_gas_utilities"]
)


def assign_utility_ny(
    input_metadata: pl.LazyFrame,
    electric_polygons: gpd.GeoDataFrame,
    gas_polygons: gpd.GeoDataFrame,
    pumas: gpd.GeoDataFrame,
    config: dict,
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
        config: State config dictionary (see data/resstock/state_configs.yaml).
            Must contain ``state_crs``.

    Returns:
        LazyFrame with all original columns plus sb.electric_utility and sb.gas_utility
    """
    puma_and_heating_fuel = select_puma_and_heating_fuel_metadata(input_metadata)

    utility_name_map = pl.DataFrame(
        [
            {"state_name": k, "std_name": v}
            for k, v in get_ny_open_data_to_std_name().items()
        ]
    ).lazy()

    building_utilities = create_hh_utilities(
        puma_and_heating_fuel=puma_and_heating_fuel,
        electric_polygons=electric_polygons,
        gas_polygons=gas_polygons,
        pumas=pumas,
        utility_name_map=utility_name_map,
        state_crs=config["state_crs"],
        excluded_gas_utilities=EXCLUDED_GAS_UTILITIES,
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

    ny_config = load_state_configs()["NY"]

    # Load utility polygons (.csv files from S3) to GeoDataFrame
    electric_polygons = read_csv_to_gdf_from_s3(
        electric_poly_path,
        utility_type="electric",
        state_crs=ny_config["state_crs"],
        geometry_col="the_geom",
        crs="EPSG:4326",
    )
    gas_polygons = read_csv_to_gdf_from_s3(
        gas_poly_path,
        utility_type="gas",
        state_crs=ny_config["state_crs"],
        geometry_col="the_geom",
        crs="EPSG:4326",
    )

    # Load PUMAS using pygris
    pumas = get_pumas(
        state="NY",
        year=ny_config["puma_year"],
        cb=True,  # Use cartographic boundaries (simplified)
    )
    pumas = pumas.to_crs(epsg=ny_config["state_crs"])
    pumas["puma_area"] = pumas.geometry.area
    pumas = cast(gpd.GeoDataFrame, pumas)

    metadata_with_utility_assignment = assign_utility_ny(
        input_metadata=input_metadata,
        electric_polygons=electric_polygons,
        gas_polygons=gas_polygons,
        pumas=pumas,
        config=ny_config,
    )

    # Write only the assignment columns — full metadata lives in metadata-sb.parquet.
    metadata_with_utility_assignment.select(
        "bldg_id", "sb.electric_utility", "sb.gas_utility"
    ).sink_parquet(
        str(output_path_s3), compression="zstd", storage_options=STORAGE_OPTIONS
    )
