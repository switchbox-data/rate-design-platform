"""Pipeline utilities for mixed baseline/upgrade adoption trajectories.

This module orchestrates mixed adoption scenarios where a fraction of buildings
adopt an upgrade (e.g., heat pumps, weatherization) while others remain at baseline.

Workflow:
1. Fetch baseline sample (N buildings) → establishes deterministic ordering
2. For each adoption fraction (10%, 20%, etc.):
   - Select first x% of building IDs from ordering
   - Fetch upgrade data ONLY for those specific buildings
   - Create mixed metadata (baseline + upgrade with adoption flag)
   - Create mixed load curves (baseline + upgrade concatenated)
"""

from pathlib import Path

import polars as pl

from utils.buildstock_io import (
    fetch_for_building_ids,
    fetch_sample,
    get_load_curve_path,
    get_metadata_path,
)


# ------------------------------------------------------------------------------
# Step 1: Fetch baseline sample
# ------------------------------------------------------------------------------

def fetch_baseline_sample(
    *,
    sample_size: int,
    random_seed: int,
    release_year: str = "2024",
    weather_file: str = "tmy3",
    release_version: str = "2",
    state: str = "NY",
    output_dir: Path,
    max_workers: int = 5,
) -> tuple[Path, list[int]]:
    """Fetch baseline sample and return metadata path + deterministic building ID ordering.

    This establishes the fixed ordering of buildings for all adoption fractions.
    The ordering is determined by the random seed and remains constant across
    all adoption scenarios.

    Args:
        sample_size: Number of buildings to sample
        random_seed: Seed for reproducible sampling
        release_year: ResStock release year (e.g., "2024")
        weather_file: Weather file type (e.g., "tmy3")
        release_version: Release version (e.g., "2")
        state: State abbreviation (e.g., "NY")
        output_dir: Directory to save downloaded files
        max_workers: Number of parallel download workers

    Returns:
        Tuple of (metadata_path, building_id_ordering)
    """
    # Fetch baseline data (upgrade 0)
    _, failed = fetch_sample(
        upgrade_id="0",
        release_year=release_year,
        weather_file=weather_file,
        release_version=release_version,
        state=state,
        output_dir=output_dir,
        max_workers=max_workers,
        sample_size=sample_size,
        random_seed=random_seed,
        file_type=("metadata", "load_curve_hourly"),
    )

    if failed:
        print(f"Warning: {len(failed)} baseline files failed to download")

    # Get metadata path and extract building ID ordering
    metadata_path = get_metadata_path(
        output_dir=output_dir,
        release_year=release_year,
        weather_file=weather_file,
        release_version=release_version,
        upgrade_id="0",
        state=state,
    )

    # Read metadata to get building IDs - this is our deterministic ordering
    metadata = pl.read_parquet(metadata_path)
    building_ids = metadata["bldg_id"].to_list()

    return metadata_path, building_ids


# ------------------------------------------------------------------------------
# Step 2: Build adoption trajectory
# ------------------------------------------------------------------------------

def build_adoption_trajectory(
    *,
    baseline_metadata_path: Path,
    baseline_building_ids: list[int],
    adoption_fractions: list[float],
    upgrade_id: str = "1",
    release_year: str = "2024",
    weather_file: str = "tmy3",
    release_version: str = "2",
    state: str = "NY",
    output_dir: Path,
    max_workers: int = 5,
    output_processed_dir: Path,
) -> dict[float, Path]:
    """Build mixed adoption scenarios for multiple adoption fractions.

    For each adoption fraction:
    1. Select first x% of building IDs from ordering
    2. Fetch upgrade data only for those buildings
    3. Create mixed metadata and load curves
    4. Save to parquet

    Args:
        baseline_metadata_path: Path to baseline metadata parquet
        baseline_building_ids: Ordered list of building IDs (from fetch_baseline_sample)
        adoption_fractions: List of adoption fractions (e.g., [0.1, 0.2, 0.5])
        upgrade_id: Upgrade ID to fetch (e.g., "1" for heat pump upgrade)
        release_year: ResStock release year
        weather_file: Weather file type
        release_version: Release version
        state: State abbreviation
        output_dir: Directory where buildstock data is stored
        max_workers: Number of parallel download workers
        output_processed_dir: Directory to save processed mixed scenarios

    Returns:
        Dictionary mapping adoption fraction to output parquet path
    """
    output_processed_dir = Path(output_processed_dir)
    output_processed_dir.mkdir(parents=True, exist_ok=True)

    # Load baseline metadata (will be reused for all fractions)
    baseline_metadata = pl.read_parquet(baseline_metadata_path)

    # Track which buildings have been fetched for upgrade
    fetched_upgrade_ids = set()
    scenario_paths = {}

    for fraction in sorted(adoption_fractions):
        print(f"\n--- Building {fraction*100:.0f}% adoption scenario ---")

        # Select first x% of buildings from ordering
        n_adopters = int(round(fraction * len(baseline_building_ids)))
        adopter_ids = baseline_building_ids[:n_adopters]

        # Fetch upgrade data only for new adopters (not already fetched)
        new_adopters = set(adopter_ids) - fetched_upgrade_ids
        if new_adopters:
            print(f"Fetching upgrade data for {len(new_adopters)} new adopters...")
            fetch_for_building_ids(
                building_ids=list(new_adopters),
                upgrade_id=upgrade_id,
                release_year=release_year,
                weather_file=weather_file,
                release_version=release_version,
                state=state,
                output_dir=output_dir,
                max_workers=max_workers,
                file_type=("metadata", "load_curve_hourly"),
            )
            fetched_upgrade_ids.update(new_adopters)

        # Create mixed metadata and loads
        output_path = output_processed_dir / f"mixed_{fraction:.2f}.parquet"

        mixed_data = create_mixed_scenario(
            baseline_metadata=baseline_metadata,
            baseline_building_ids=baseline_building_ids,
            adopter_ids=adopter_ids,
            upgrade_id=upgrade_id,
            state=state,
            output_dir=output_dir,
            release_year=release_year,
            weather_file=weather_file,
            release_version=release_version,
        )

        # Save mixed scenario
        mixed_data.write_parquet(output_path)
        print(f"Saved {fraction*100:.0f}% adoption scenario to {output_path}")

        scenario_paths[fraction] = output_path

    return scenario_paths


# ------------------------------------------------------------------------------
# Step 3: Create mixed scenario (metadata + loads)
# ------------------------------------------------------------------------------

def create_mixed_scenario(
    *,
    baseline_metadata: pl.DataFrame,
    baseline_building_ids: list[int],
    adopter_ids: list[int],
    upgrade_id: str,
    state: str,
    output_dir: Path,
    release_year: str,
    weather_file: str,
    release_version: str,
) -> pl.DataFrame:
    """Create a mixed scenario with metadata and load curves.

    Combines baseline and upgrade data, adding an adoption flag column.

    Args:
        baseline_metadata: Baseline metadata DataFrame
        baseline_building_ids: All building IDs in baseline
        adopter_ids: Building IDs that adopt the upgrade
        upgrade_id: Upgrade ID
        state: State abbreviation
        output_dir: BuildStock data directory
        release_year: ResStock release year
        weather_file: Weather file type
        release_version: Release version

    Returns:
        DataFrame with concatenated load curves and metadata with adoption flag
    """
    # Get upgrade metadata path
    upgrade_metadata_path = get_metadata_path(
        output_dir=output_dir,
        release_year=release_year,
        weather_file=weather_file,
        release_version=release_version,
        upgrade_id=upgrade_id,
        state=state,
    )

    # Create mixed metadata
    mixed_metadata = create_mixed_metadata(
        baseline_metadata=baseline_metadata,
        upgrade_metadata_path=upgrade_metadata_path,
        adopter_ids=adopter_ids,
    )

    # Create mixed load curves
    from utils.buildstock_io import get_load_curve_dir

    load_curve_dir = get_load_curve_dir(
        output_dir=output_dir,
        release_year=release_year,
        weather_file=weather_file,
        release_version=release_version,
        curve_subdir="load_curve_hourly",
    )

    mixed_loads = create_mixed_loads(
        building_ids=baseline_building_ids,
        adopter_ids=adopter_ids,
        load_curve_dir=load_curve_dir,
        state=state,
        upgrade_id=upgrade_id,
    )

    # Join loads with metadata
    result = mixed_loads.join(mixed_metadata, on="bldg_id", how="left")

    return result


def create_mixed_metadata(
    *,
    baseline_metadata: pl.DataFrame,
    upgrade_metadata_path: Path,
    adopter_ids: list[int],
) -> pl.DataFrame:
    """Create mixed metadata with adoption flag.

    Takes baseline metadata and replaces rows for adopters with upgrade metadata,
    adding an 'adopted' flag column (0 for baseline, 1 for upgrade).

    Args:
        baseline_metadata: Baseline metadata DataFrame
        upgrade_metadata_path: Path to upgrade metadata parquet
        adopter_ids: List of building IDs that adopted the upgrade

    Returns:
        Mixed metadata DataFrame with adoption flag
    """
    # Read upgrade metadata (only for adopters)
    if adopter_ids:
        upgrade_metadata = pl.read_parquet(upgrade_metadata_path)
        upgrade_metadata = upgrade_metadata.filter(pl.col("bldg_id").is_in(adopter_ids))
    else:
        upgrade_metadata = pl.DataFrame()

    # Start with baseline, add adoption flag (0 = not adopted)
    mixed = baseline_metadata.with_columns(pl.lit(0).alias("adopted"))

    # Replace adopter rows with upgrade metadata if any
    if not upgrade_metadata.is_empty():
        # Filter out adopters from baseline
        mixed = mixed.filter(~pl.col("bldg_id").is_in(adopter_ids))

        # Add upgrade metadata with adoption flag (1 = adopted)
        upgrade_with_flag = upgrade_metadata.with_columns(pl.lit(1).alias("adopted"))

        # Concatenate
        mixed = pl.concat([mixed, upgrade_with_flag])

    return mixed


def create_mixed_loads(
    *,
    building_ids: list[int],
    adopter_ids: list[int],
    load_curve_dir: Path,
    state: str,
    upgrade_id: str,
) -> pl.DataFrame:
    """Create mixed load curves (baseline + upgrade).

    Args:
        building_ids: All building IDs
        adopter_ids: Building IDs that adopted the upgrade
        load_curve_dir: Directory containing load curve parquets
        state: State abbreviation
        upgrade_id: Upgrade ID for adopters

    Returns:
        Concatenated load curves with bldg_id column
    """
    adopter_set = set(adopter_ids)
    load_dfs = []

    for bid in building_ids:
        # Determine which upgrade to use
        use_upgrade_id = upgrade_id if bid in adopter_set else "0"

        # Get load curve path
        load_path = get_load_curve_path(
            load_curve_dir=load_curve_dir,
            bldg_id=bid,
            state=state,
            upgrade_id=use_upgrade_id,
        )

        # Read and add building ID
        load_df = pl.read_parquet(load_path).with_columns(pl.lit(bid).alias("bldg_id"))
        load_dfs.append(load_df)

    return pl.concat(load_dfs)
