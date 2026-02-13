"""Utility functions for Cairo-related operations."""

from pathlib import Path


def build_bldg_id_to_load_filepath(
    path_resstock_loads: Path,
    building_ids: list[int] | None = None,
    return_path_base: Path | None = None,
) -> dict[int, Path]:
    """
    Build a dictionary mapping building IDs to their load file paths.

    Args:
        path_resstock_loads: Directory containing parquet load files to scan
        building_ids: Optional list of building IDs to include. If None, includes all.
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

    building_ids_set = set(building_ids) if building_ids is not None else None

    bldg_id_to_load_filepath = {}
    for parquet_file in path_resstock_loads.glob("*.parquet"):
        try:
            bldg_id = int(parquet_file.stem.split("-")[0])
        except ValueError:
            continue  # Skip files that don't match expected pattern

        if building_ids_set is not None and bldg_id not in building_ids_set:
            continue

        if return_path_base is None:
            filepath = parquet_file
        else:
            filepath = return_path_base / parquet_file.name

        bldg_id_to_load_filepath[bldg_id] = filepath

    return bldg_id_to_load_filepath
