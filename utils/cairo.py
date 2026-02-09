"""Utility functions for Cairo-related operations."""

import json
import pandas as pd
from pathlib import Path, PurePath

from cairo.rates_tool import config
from cairo.rates_tool.tariffs import URDBv7_to_ElectricityRates


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


def _load_tariff_json(tariff_path: Path) -> dict:
    """
    Load and extract tariff structure from URDB v7 JSON file.

    Args:
        tariff_path: Path to tariff JSON file

    Returns:
        Tariff dictionary (first item from URDB "items" array)

    Raises:
        FileNotFoundError: If tariff file does not exist
        KeyError: If JSON structure is invalid (missing "items" key)
    """
    if not tariff_path.exists():
        raise FileNotFoundError(f"Tariff file not found: {tariff_path}")

    with open(tariff_path) as f:
        tariff_data = json.load(f)

    if "items" not in tariff_data or not tariff_data["items"]:
        raise KeyError(f"Invalid tariff JSON structure in: {tariff_path}")

    return tariff_data["items"][0]


def get_default_tariff_structures(tariff_paths: list[Path]) -> dict[Path, object]:
    """
    Load tariff JSON files and convert to ElectricityRates objects.

    Args:
        tariff_paths: List of paths to tariff structure JSON files (URDB v7 format)

    Returns:
        Dictionary mapping tariff path to ElectricityRates object

    Raises:
        FileNotFoundError: If any tariff file does not exist
    """
    tariff_structures = {}
    for tariff_path in tariff_paths:
        tariff_dict = _load_tariff_json(tariff_path)
        tariff_structures[tariff_path] = URDBv7_to_ElectricityRates(tariff_dict)

    return tariff_structures


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
