"""Tests for utils/buildstock_io.py.

Each test function corresponds to one key function in buildstock_io.py.
Tests that interact with buildstock-fetch save outputs to tests/test_outputs/buildstock_io/ (git-ignored).
Path construction tests are self-contained and don't require fixtures or API calls.
"""

from pathlib import Path

import pytest

from utils.buildstock_io import (
    fetch_for_building_ids,
    fetch_sample,
    get_buildstock_release_dir,
    get_load_curve_dir,
    get_load_curve_path,
    get_metadata_path,
)

# Test output directory (git-ignored)
TEST_OUTPUT_DIR = Path(__file__).parent.parent / "test_outputs" / "buildstock_io"


# ==============================================================================
# Unit tests - No fixtures or API calls required
# ==============================================================================


def test_get_buildstock_release_dir():
    """Test get_buildstock_release_dir constructs correct directory path."""
    output_dir = Path("/data/resstock")
    release_dir = get_buildstock_release_dir(
        output_dir=output_dir,
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
    )
    assert release_dir == output_dir / "res_2024_tmy3_2"


def test_get_metadata_path():
    """Test get_metadata_path constructs correct metadata file path."""
    output_dir = Path("/data/resstock")
    metadata_path = get_metadata_path(
        output_dir=output_dir,
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        upgrade_id="0",
        state="NY",
    )
    expected = output_dir / "res_2024_tmy3_2" / "metadata" / "state=NY" / "upgrade=00" / "metadata.parquet"
    assert metadata_path == expected


def test_get_load_curve_dir():
    """Test get_load_curve_dir constructs correct load curve directory path."""
    output_dir = Path("/data/resstock")
    load_curve_dir = get_load_curve_dir(
        output_dir=output_dir,
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        curve_subdir="load_curve_hourly",
    )
    expected = output_dir / "res_2024_tmy3_2" / "load_curve_hourly"
    assert load_curve_dir == expected


def test_get_load_curve_path():
    """Test get_load_curve_path constructs correct load curve file path."""
    load_curve_dir = Path("/data/resstock/load_curve_hourly")
    path = get_load_curve_path(
        load_curve_dir=load_curve_dir,
        bldg_id=12345,
        state="NY",
        upgrade_id="0",
    )
    expected = load_curve_dir / "state=NY" / "upgrade=00" / "12345-0.parquet"
    assert path == expected


# ==============================================================================
# Integration tests - Require buildstock-fetch and generate test outputs
# ==============================================================================


@pytest.mark.integration
def test_fetch_sample():
    """Test fetch_sample with sample_size=1.

    Outputs saved to: tests/test_outputs/buildstock_io/sample_1/
    """
    output_dir = TEST_OUTPUT_DIR / "sample_1"
    output_dir.mkdir(parents=True, exist_ok=True)

    paths, failed = fetch_sample(
        upgrade_id="0",
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        state="NY",
        output_dir=output_dir,
        max_workers=1,
        sample_size=1,
        random_seed=42,
        file_type=("metadata", "load_curve_hourly"),
    )

    assert len(failed) == 0
    assert len(paths) > 0

    metadata_path = get_metadata_path(
        output_dir=output_dir,
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        upgrade_id="0",
        state="NY",
    )
    assert metadata_path.exists()

    import polars as pl

    metadata = pl.read_parquet(metadata_path)
    assert len(metadata) == 1


@pytest.mark.integration
def test_fetch_for_building_ids():
    """Test fetch_for_building_ids with 1 specific building.

    Outputs saved to: tests/test_outputs/buildstock_io/specific_building/
    """
    baseline_dir = TEST_OUTPUT_DIR / "baseline_for_upgrade_test"
    baseline_dir.mkdir(parents=True, exist_ok=True)

    _, _ = fetch_sample(
        upgrade_id="0",
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        state="NY",
        output_dir=baseline_dir,
        max_workers=1,
        sample_size=1,
        random_seed=42,
        file_type=("metadata",),
    )

    import polars as pl

    metadata_path = get_metadata_path(
        output_dir=baseline_dir,
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        upgrade_id="0",
        state="NY",
    )
    metadata = pl.read_parquet(metadata_path)
    building_id = metadata["bldg_id"][0]

    output_dir = TEST_OUTPUT_DIR / "specific_building"
    output_dir.mkdir(parents=True, exist_ok=True)

    paths, failed = fetch_for_building_ids(
        building_ids=[building_id],
        upgrade_id="1",
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        state="NY",
        output_dir=output_dir,
        max_workers=1,
        file_type=("metadata", "load_curve_hourly"),
    )

    assert len(failed) == 0
    assert len(paths) > 0

    upgrade_metadata_path = get_metadata_path(
        output_dir=output_dir,
        release_year="2024",
        weather_file="tmy3",
        release_version="2",
        upgrade_id="1",
        state="NY",
    )
    assert upgrade_metadata_path.exists()
