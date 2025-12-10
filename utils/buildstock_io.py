"""IO helpers for working with BuildStock artifacts."""

from pathlib import Path
from typing import Optional

import numpy as np
from buildstock_fetch.main import BuildingID, fetch_bldg_data, fetch_bldg_ids


def get_buildstock_release_dir(
    output_dir: Path,
    release_year: str = "2024",
    weather_file: str = "tmy3",
    release_version: str = "2",
) -> Path:
    """Return buildstock-fetch output directory for a release.

    Follows the naming convention:
      {output_dir}/res_{release_year}_{weather_file}_{release_version}/

    Example: tests/test_data/res_2024_tmy3_2/
    """
    release_name = f"res_{release_year}_{weather_file}_{release_version}"
    return output_dir / release_name


def get_metadata_path(
    output_dir: Path,
    release_year: str = "2024",
    weather_file: str = "tmy3",
    release_version: str = "2",
    upgrade_id: str = "0",
    state: str = "NY",
) -> Path:
    """Return path to metadata parquet for a specific upgrade.

    Structure: {release_dir}/metadata/state={state}/upgrade={upgrade_id}/metadata.parquet
    Upgrade ID is zero-padded to 2 digits (e.g., "0" -> "00", "1" -> "01")
    """
    release_dir = get_buildstock_release_dir(output_dir, release_year, weather_file, release_version)
    upgrade_padded = f"{int(upgrade_id):02d}"
    return release_dir / "metadata" / f"state={state}" / f"upgrade={upgrade_padded}" / "metadata.parquet"


def get_load_curve_dir(
    output_dir: Path,
    release_year: str = "2024",
    weather_file: str = "tmy3",
    release_version: str = "2",
    curve_subdir: str = "load_curve_hourly",
) -> Path:
    """Return directory containing load curves (hourly or 15-minute)."""
    release_dir = get_buildstock_release_dir(output_dir, release_year, weather_file, release_version)
    return release_dir / curve_subdir


def get_load_curve_path(
    load_curve_dir: Path,
    bldg_id: int,
    state: str = "NY",
    upgrade_id: str = "0",
) -> Path:
    """Return path to load curve parquet file for a specific building/upgrade.

    Structure: {load_curve_dir}/state={state}/upgrade={upgrade_id}/{bldg_id}-{upgrade_id_unpadded}.parquet
    Upgrade directory is zero-padded to 2 digits, but filename uses unpadded ID.
    Example: state=NY/upgrade=00/352381-0.parquet
    """
    upgrade_padded = f"{int(upgrade_id):02d}"
    filename = f"{bldg_id}-{upgrade_id}.parquet"
    return load_curve_dir / f"state={state}" / f"upgrade={upgrade_padded}" / filename


# ------------------------------------------------------------------------------
# BuildStock-fetch interface
# ------------------------------------------------------------------------------

def fetch_sample(
    *,
    upgrade_id: str = "0",
    release_year: str = "2024",
    weather_file: str = "tmy3",
    release_version: str = "2",
    state: str = "NY",
    output_dir: Path = Path("./rate_design/ny/hp_rates/data/buildstock_raw"),
    max_workers: int = 5,
    sample_size: Optional[int] = None,
    random_seed: Optional[int] = None,
    file_type: tuple[str, ...] = ("metadata", "load_curve_hourly"),
) -> tuple[list[Path], list[str]]:
    """Fetch a sample of N buildings for a given upgrade via buildstock-fetch.

    This is a generic interface for fetching any upgrade data. Optionally samples
    a random subset of buildings for faster iteration.

    Args:
        upgrade_id: Upgrade ID to fetch (e.g., "0" for baseline, "1" for upgrade)
        release_year: ResStock release year (e.g., "2024")
        weather_file: Weather file type (e.g., "tmy3")
        release_version: Release version number (e.g., "2")
        state: State abbreviation (e.g., "NY")
        output_dir: Directory to save downloaded files
        max_workers: Number of parallel download workers
        sample_size: Optional number of buildings to sample (None = all buildings)
        random_seed: Random seed for sampling reproducibility
        file_type: Tuple of file types to fetch (e.g., ("metadata", "load_curve_hourly"))

    Returns:
        Tuple of (downloaded_paths, failed_building_ids)
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Fetch all available building IDs for this upgrade
    bldg_ids = fetch_bldg_ids(
        product="resstock",
        release_year=release_year,
        weather_file=weather_file,
        release_version=release_version,
        state=state,
        upgrade_id=upgrade_id,
    )

    # Sample if requested
    if sample_size is not None and sample_size < len(bldg_ids):
        rng = np.random.default_rng(random_seed)
        all_bldg_ids = [bid.bldg_id for bid in bldg_ids]
        sampled_ids = set(rng.choice(all_bldg_ids, size=sample_size, replace=False))
        bldg_ids = [bid for bid in bldg_ids if bid.bldg_id in sampled_ids]

    # Fetch data
    paths, failed = fetch_bldg_data(
        bldg_ids=bldg_ids,
        file_type=file_type,
        output_dir=output_dir,
        max_workers=max_workers,
    )

    return paths, failed


def fetch_for_building_ids(
    *,
    building_ids: list[int],
    upgrade_id: str,
    release_year: str = "2024",
    weather_file: str = "tmy3",
    release_version: str = "2",
    state: str = "NY",
    output_dir: Path = Path("./rate_design/ny/hp_rates/data/buildstock_raw"),
    max_workers: int = 5,
    file_type: tuple[str, ...] = ("metadata", "load_curve_hourly"),
) -> tuple[list[Path], list[str]]:
    """Fetch data for specific building IDs from a given upgrade.

    This is useful when you already know which buildings you want to fetch
    (e.g., a subset selected for upgrade adoption).

    Args:
        building_ids: List of building IDs to fetch
        upgrade_id: Upgrade ID to fetch (e.g., "0" for baseline, "1" for upgrade)
        release_year: ResStock release year (e.g., "2024")
        weather_file: Weather file type (e.g., "tmy3")
        release_version: Release version number (e.g., "2")
        state: State abbreviation (e.g., "NY")
        output_dir: Directory to save downloaded files
        max_workers: Number of parallel download workers
        file_type: Tuple of file types to fetch (e.g., ("metadata", "load_curve_hourly"))

    Returns:
        Tuple of (downloaded_paths, failed_building_ids)
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert integer building IDs to BuildingID objects
    bldg_id_objects = [
        BuildingID(
            product="resstock",
            release_year=release_year,
            weather_file=weather_file,
            release_version=release_version,
            state=state,
            upgrade_id=upgrade_id,
            bldg_id=bid,
        )
        for bid in building_ids
    ]

    # Fetch data
    paths, failed = fetch_bldg_data(
        bldg_ids=bldg_id_objects,
        file_type=file_type,
        output_dir=output_dir,
        max_workers=max_workers,
    )

    return paths, failed
