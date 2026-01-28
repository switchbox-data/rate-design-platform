"""Utility functions for Cairo-related operations."""

import json
import pandas as pd
from pathlib import Path, PurePath

from cairo.rates_tool import config
from cairo.rates_tool.tariffs import (
    URDBv7_to_ElectricityRates,
    __load_tariff_maps__,
)


def build_bldg_id_to_load_filepath(path_resstock_loads: Path, path_resstock: Path) -> dict[int, Path]:
    """
    Build a dictionary mapping building IDs to their load file paths.
    
    Args:
        path_resstock_loads: Path to the directory containing parquet load files
        path_resstock: Base path for constructing full file paths
        
    Returns:
        Dictionary mapping building ID (int) to full file path (Path)
    """
    bldg_id_to_load_filepath = {}
    for parquet_file in path_resstock_loads.glob("*.parquet"):
        # Extract building ID from filename (e.g., "3837-0.parquet" -> "3837")
        bldg_id = int(parquet_file.stem.split("-")[0])
        # Construct path using path_resstock as base
        bldg_id_to_load_filepath[bldg_id] = path_resstock / "loads" / parquet_file.name
    
    return bldg_id_to_load_filepath


def __find_tariff_path_by_prototype__(
    prototype,
    tariff_strategy,
    #tariff_str_loc,
    tariffsdir,
    tarff_list_main=None,  # TODO: This variable is not found in cairo codebase - may need to be defined or passed
):
    """
    matches building stock with appropriate tariff for strategy and baseline
    Inputs
    ---------
    tariffs_stock_map_path
    prototype

    Outputs
    ---------
    Direct
        - None
    Implicit
        - tariff path strategy - SQL or local file path (latter only temporary) after strategy implemented
        - strategy path baseline - SQL or local file path (latter only temporary) before strategy implemented
    """

    tariffs_stock_map = __load_tariff_maps__(tariff_strategy)

    tariffs_stock_map = tariffs_stock_map.loc[(tariffs_stock_map.index == prototype)]
    tariffs_stock_map = tariffs_stock_map.to_dict(orient="index")

    # Note: tarff_list_main is not found in cairo codebase - this may need to be defined
    if tarff_list_main is None:
        raise ValueError("tarff_list_main must be provided")
    
    tariff_path = (
        tariffsdir
        / f"tariff_{tarff_list_main[tariffs_stock_map[prototype]['tariff']]}.json"
    )

    return tariff_path


def __initialize_tariff__(tariff_path):

    with open(tariff_path) as tariff_json_file:
        tariff_dict = json.load(tariff_json_file)
        tariff_dict = tariff_dict["items"][0]

    return tariff_dict


def get_default_tariff_structures(tariff_paths):
    
    """
    Sets up the initial default tariff and overwrites things as needed. Importantly it converts the
    structure of the tariffs to the ElectricityRates structure used elsewhere in code.
    """

    default_tariff_dict = {tariff_path: None for tariff_path in tariff_paths}
    # for each customer class being evaluated, process the default tariff to use as a
    # basic structure and then overwrite as necessary with user input in lookups.py
    for tariff_path in tariff_paths:

        # read default tariff structure, convert to 'local' format from URDB json format
        default_tariff_dict.update({tariff_path: __initialize_tariff__(tariff_path=tariff_path)})
        default_tariff_dict.update(
            {tariff_path: URDBv7_to_ElectricityRates(default_tariff_dict[tariff_path])}
        )

    return default_tariff_dict


def _initialize_tariffs(tariff_map, tariff_paths, building_stock_sample=None):

    """
    Once at the beginning of the run, establish the params_grid which will
    be used for calculating customer bills. It contains all information on the
    tariffs needed for bill calculation.
    """

    if isinstance(tariff_map, str):
        # if a string is passed, assume it is the name of the tariff map and based
        # in the config.TARIFFSSTOCKMAP directory
        tariff_map_fp = config.TARIFFSSTOCKMAP / f"tariff_map_{tariff_map}.csv"
        tariff_map_df = pd.read_csv(tariff_map_fp)
    elif isinstance(tariff_map, PurePath):
        # if a PurePath is passed, assume it is the full path to the tariff map
        tariff_map_fp = tariff_map        
        tariff_map_df = pd.read_csv(tariff_map_fp)
    elif isinstance(tariff_map, pd.DataFrame):
        tariff_map_fp = None
        tariff_map_df = tariff_map

    if isinstance(building_stock_sample, list):
        tariff_map_df = tariff_map_df.loc[
            tariff_map_df.bldg_id.isin(building_stock_sample)
        ]

    customer_class_iter = tariff_map_df["tariff_key"].unique()

    # Get Parameter Grid for Tech 
    # placed here so that postprocessing initialization not thrown off,
    # it relies on seeing self.params_grid which is defined here
    param_grid = get_default_tariff_structures(tariff_paths)

    return param_grid, tariff_map_df
