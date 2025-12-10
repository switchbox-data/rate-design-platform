"""Tests for utils/mixed_adoption_trajectory.py.

Each test function corresponds to one key function in mixed_adoption_trajectory.py.
Integration tests generate outputs to tests/test_outputs/mixed_adoption_trajectory/ (git-ignored).

Test creates a complete adoption trajectory with:
- Sample size: 10 buildings
- Adoption fractions: 10% (1 building), 20% (2 buildings)
"""

from pathlib import Path

import polars as pl
import pytest

from utils.mixed_adoption_trajectory import (
    build_adoption_trajectory,
    create_mixed_loads,
    create_mixed_metadata,
    fetch_baseline_sample,
)

# Test output directory (git-ignored)
TEST_OUTPUT_DIR = Path(__file__).parent.parent / "test_outputs" / "mixed_adoption_trajectory"


# ==============================================================================
# Integration tests - Require buildstock-fetch and generate test outputs
# ==============================================================================


@pytest.mark.integration
def test_fetch_baseline_sample():
    """Test fetch_baseline_sample with sample_size=10.

    Outputs saved to: tests/test_outputs/mixed_adoption_trajectory/baseline_10/
    """
    output_dir = TEST_OUTPUT_DIR / "baseline_10"
    output_dir.mkdir(parents=True, exist_ok=True)

    metadata_path, building_ids = fetch_baseline_sample(
        sample_size=10,
        random_seed=42,
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        state="NY",
        output_dir=output_dir,
        max_workers=5,
    )

    assert metadata_path.exists()
    assert len(building_ids) == 10
    assert all(isinstance(bid, int) for bid in building_ids)

    metadata = pl.read_parquet(metadata_path)
    assert len(metadata) == 10
    assert metadata["bldg_id"].to_list() == building_ids


@pytest.mark.integration
def test_create_mixed_metadata():
    """Test create_mixed_metadata with 2 adopters out of 10 buildings.

    Requires baseline data from test_fetch_baseline_sample.
    """
    baseline_dir = TEST_OUTPUT_DIR / "baseline_10"

    from utils.buildstock_io import get_metadata_path

    baseline_metadata_path = get_metadata_path(
        output_dir=baseline_dir,
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        upgrade_id="0",
        state="NY",
    )

    if not baseline_metadata_path.exists():
        pytest.skip("Baseline data not found - run test_fetch_baseline_sample first")

    baseline_metadata = pl.read_parquet(baseline_metadata_path)
    building_ids = baseline_metadata["bldg_id"].to_list()
    adopter_ids = building_ids[:2]

    mixed_metadata = create_mixed_metadata(
        baseline_metadata=baseline_metadata,
        upgrade_metadata_path=baseline_metadata_path,
        adopter_ids=adopter_ids,
    )

    assert len(mixed_metadata) == 10
    assert "adopted" in mixed_metadata.columns
    assert mixed_metadata["adopted"].sum() == 2

    for bid in adopter_ids:
        adopted_value = mixed_metadata.filter(pl.col("bldg_id") == bid)["adopted"][0]
        assert adopted_value == 1


@pytest.mark.integration
def test_create_mixed_loads():
    """Test create_mixed_loads with 2 adopters out of 10 buildings.

    Requires baseline data from test_fetch_baseline_sample.
    """
    baseline_dir = TEST_OUTPUT_DIR / "baseline_10"

    from utils.buildstock_io import get_load_curve_dir, get_metadata_path

    baseline_metadata_path = get_metadata_path(
        output_dir=baseline_dir,
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        upgrade_id="0",
        state="NY",
    )

    if not baseline_metadata_path.exists():
        pytest.skip("Baseline data not found - run test_fetch_baseline_sample first")

    baseline_metadata = pl.read_parquet(baseline_metadata_path)
    building_ids = baseline_metadata["bldg_id"].to_list()

    load_curve_dir = get_load_curve_dir(
        output_dir=baseline_dir,
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        curve_subdir="load_curve_hourly",
    )

    adopter_ids = building_ids[:2]

    mixed_loads = create_mixed_loads(
        building_ids=building_ids,
        adopter_ids=adopter_ids,
        load_curve_dir=load_curve_dir,
        state="NY",
        upgrade_id="0",
    )

    assert "bldg_id" in mixed_loads.columns
    unique_bldgs = mixed_loads["bldg_id"].unique().to_list()
    assert len(unique_bldgs) == 10
    assert set(unique_bldgs) == set(building_ids)


@pytest.mark.integration
def test_build_adoption_trajectory():
    """Test complete build_adoption_trajectory with 10% and 20% adoption.

    Full integration test:
    1. Fetches 10 baseline buildings
    2. Creates 10% adoption scenario (1 building)
    3. Creates 20% adoption scenario (2 buildings)
    4. Verifies cumulative property

    Outputs saved to: tests/test_outputs/mixed_adoption_trajectory/scenarios/
    """
    baseline_dir = TEST_OUTPUT_DIR / "full_test_baseline"
    baseline_dir.mkdir(parents=True, exist_ok=True)

    metadata_path, building_ids = fetch_baseline_sample(
        sample_size=10,
        random_seed=42,
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        state="NY",
        output_dir=baseline_dir,
        max_workers=5,
    )

    output_processed_dir = TEST_OUTPUT_DIR / "scenarios"
    output_processed_dir.mkdir(parents=True, exist_ok=True)

    scenario_paths = build_adoption_trajectory(
        baseline_metadata_path=metadata_path,
        baseline_building_ids=building_ids,
        adoption_fractions=[0.1, 0.2],
        upgrade_id="1",
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        state="NY",
        output_dir=baseline_dir,
        max_workers=5,
        output_processed_dir=output_processed_dir,
    )

    # Verify scenarios created
    assert 0.1 in scenario_paths
    assert 0.2 in scenario_paths
    assert scenario_paths[0.1].exists()
    assert scenario_paths[0.2].exists()

    # Load scenarios
    scenario_10 = pl.read_parquet(scenario_paths[0.1])
    scenario_20 = pl.read_parquet(scenario_paths[0.2])

    # Verify structure
    assert "bldg_id" in scenario_10.columns
    assert "adopted" in scenario_10.columns
    assert "bldg_id" in scenario_20.columns
    assert "adopted" in scenario_20.columns

    # Count adopters
    adopters_10 = scenario_10.groupby("bldg_id").agg(pl.col("adopted").first())
    adopters_20 = scenario_20.groupby("bldg_id").agg(pl.col("adopted").first())

    n_adopters_10 = adopters_10["adopted"].sum()
    n_adopters_20 = adopters_20["adopted"].sum()

    assert n_adopters_10 == 1
    assert n_adopters_20 == 2

    # Verify cumulative property
    adopter_ids_10 = set(adopters_10.filter(pl.col("adopted") == 1)["bldg_id"].to_list())
    adopter_ids_20 = set(adopters_20.filter(pl.col("adopted") == 1)["bldg_id"].to_list())

    assert adopter_ids_10.issubset(adopter_ids_20)
