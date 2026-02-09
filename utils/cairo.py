"""Utility functions for Cairo-related operations."""

import json
import pandas as pd
from pathlib import Path, PurePath

from cairo.rates_tool import config
from cairo.rates_tool.tariffs import (
    URDBv7_to_ElectricityRates,
    __load_tariff_maps__,
)


def build_bldg_id_to_load_filepath(
    path_resstock_loads: Path,
    return_path_base: Path | None = None,
) -> dict[int, Path]:
    """
    Build a dictionary mapping building IDs to their load file paths.

    Args:
        path_resstock_loads: Directory containing parquet load files to scan
        return_path_base: Base directory for returned paths.
            If None, returns actual file paths from path_resstock_loads.
            If Path, returns paths as return_path_base / filename.

    Returns:
        Dictionary mapping building ID (int) to full file path (Path)

    Raises:
        FileNotFoundError: If path_resstock_loads does not exist
    """
    if not path_resstock_loads.exists():
        raise FileNotFoundError(f"Load directory not found: {path_resstock_loads}")

    bldg_id_to_load_filepath = {}
    for parquet_file in path_resstock_loads.glob("*.parquet"):
        try:
            bldg_id = int(parquet_file.stem.split("-")[0])
        except ValueError:
            continue  # Skip files that don't match expected pattern

        if return_path_base is None:
            filepath = parquet_file
        else:
            filepath = return_path_base / parquet_file.name

        bldg_id_to_load_filepath[bldg_id] = filepath

    return bldg_id_to_load_filepath


def __find_tariff_path_by_prototype__(
    prototype,
    tariff_strategy,
    # tariff_str_loc,
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
        default_tariff_dict.update(
            {tariff_path: __initialize_tariff__(tariff_path=tariff_path)}
        )
        default_tariff_dict.update(
            {tariff_path: URDBv7_to_ElectricityRates(default_tariff_dict[tariff_path])}
        )

    return default_tariff_dict


def _initialize_tariffs(
    tariff_map: str | Path | pd.DataFrame,
    tariff_paths: list[Path],
    building_stock_sample: list[int] | None = None,
    tariff_map_dir: Path | None = None,
) -> tuple[dict, pd.DataFrame]:
    """
    Initialize tariff parameters and mapping for bill calculation.

    Args:
        tariff_map: Tariff map identifier. Can be:
            - str: Name prefix (loads "tariff_map_{name}.csv" from tariff_map_dir)
            - Path: Direct path to CSV file
            - DataFrame: Pre-loaded tariff mapping
        tariff_paths: List of paths to tariff structure JSON files
        building_stock_sample: Optional list of building IDs to filter
        tariff_map_dir: Directory for string-based tariff map lookup.
            If None and tariff_map is str, falls back to config.TARIFFSSTOCKMAP.

    Returns:
        Tuple of (param_grid dict, filtered tariff_map DataFrame)

    Raises:
        FileNotFoundError: If tariff map file does not exist
        ValueError: If tariff_map type is unsupported
    """
    if isinstance(tariff_map, str):
        base_dir = tariff_map_dir if tariff_map_dir is not None else config.TARIFFSSTOCKMAP
        tariff_map_fp = base_dir / f"tariff_map_{tariff_map}.csv"
        if not tariff_map_fp.exists():
            raise FileNotFoundError(f"Tariff map not found: {tariff_map_fp}")
        tariff_map_df = pd.read_csv(tariff_map_fp)
    elif isinstance(tariff_map, PurePath):
        if not tariff_map.exists():
            raise FileNotFoundError(f"Tariff map not found: {tariff_map}")
        tariff_map_df = pd.read_csv(tariff_map)
    elif isinstance(tariff_map, pd.DataFrame):
        tariff_map_df = tariff_map
    else:
        raise ValueError(f"tariff_map must be str, Path, or DataFrame, got {type(tariff_map)}")

    if building_stock_sample is not None:
        tariff_map_df = tariff_map_df.loc[
            tariff_map_df.bldg_id.isin(building_stock_sample)
        ]

    param_grid = get_default_tariff_structures(tariff_paths)

    return param_grid, tariff_map_df
