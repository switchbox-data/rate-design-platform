import shutil
from pathlib import Path

import pytest

from rate_design_platform.utils.buildstock import download_bldg_files


@pytest.fixture(scope="function")
def cleanup_downloads():
    # Setup - clean up any existing files before test
    data_dir = Path("data/res_2024_amy2018_2")
    if data_dir.exists():
        shutil.rmtree(data_dir)

    yield

    # Teardown - clean up downloaded files after test
    if data_dir.exists():
        shutil.rmtree(data_dir)


def test_download_bldg_files(cleanup_downloads):
    downloaded_paths, failed_downloads = download_bldg_files(
        product="resstock",
        release_year="2024",
        weather_file="amy2018",
        release_version="2",
        upgrade_id="0",
        state="NY",
        num_bldgs=10,
        file_type=("hpxml", "schedule", "metadata"),
        output_dir=Path("data"),
    )
    assert len(downloaded_paths) == 20
    assert len(failed_downloads) == 1
    assert Path("data/res_2024_amy2018_2/hpxml/NY/bldg0188258-up00.xml").exists()
    assert Path("data/res_2024_amy2018_2/hpxml/NY/bldg0213318-up00.xml").exists()
    assert Path("data/res_2024_amy2018_2/hpxml/NY/bldg0255710-up00.xml").exists()

    assert Path("data/res_2024_amy2018_2/schedule/NY/bldg0213318-up00_schedule.csv").exists()
    assert Path("data/res_2024_amy2018_2/schedule/NY/bldg0255710-up00_schedule.csv").exists()

    assert Path("data/res_2024_amy2018_2/metadata/NY/metadata.parquet").exists()
