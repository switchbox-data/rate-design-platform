import argparse
import io

import pytest
import polars as pl
from cloudpathlib import S3Path


@pytest.mark.parametrize(
    "data_path,release,state,upgrade_id",
    [
        ("s3://data.sb/nrel/resstock/", "res_2024_amy2018_2", "NY", "00"),
        ("s3://data.sb/nrel/resstock/", "res_2024_amy2018_2", "RI", "00"),
    ],
)
def test_resstock_download_to_s3(
    data_path: str, release: str, state: str, upgrade_id: str
):
    """Test that the resstock data is downloaded to the correct path."""
    base = S3Path(data_path)
    release_dir = base / release
    if not release_dir.exists():
        raise FileNotFoundError(f"Release directory {release_dir} does not exist")

    upgrade_ids = upgrade_id.split(" ")
    for upgrade_id in upgrade_ids:
        # Find unique bldg_ids in the metadata
        metadata_path = (
            release_dir
            / "metadata"
            / f"state={state}"
            / f"upgrade={upgrade_id}"
            / "metadata.parquet"
        )
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata file {metadata_path} does not exist")

        # Read via S3Path (boto3) so Polars doesn't need S3 credentials/region
        parquet_bytes = metadata_path.read_bytes()
        metadata_bldg_ids: pl.DataFrame = (
            pl.read_parquet(io.BytesIO(parquet_bytes)).select("bldg_id").unique()
        )

        # Get a count of files in the load_curve_hourly_directory
        load_curve_hourly_dir = (
            release_dir
            / "load_curve_hourly"
            / f"state={state}"
            / f"upgrade={upgrade_id}"
        )
        if not load_curve_hourly_dir.exists():
            raise FileNotFoundError(
                f"Load curve hourly directory {load_curve_hourly_dir} does not exist"
            )

        load_curve_hourly_files = list(load_curve_hourly_dir.glob("*.parquet"))
        num_load_curve_hourly_files = len(load_curve_hourly_files)

        assert num_load_curve_hourly_files == len(metadata_bldg_ids), (
            "Number of load curve hourly files does not match number of metadata bldg_ids"
        )

    print("Test passed")
    print("Release: ", release)
    print("State: ", state)
    print("Upgrade ids: ", upgrade_ids)
    print(f"Number of load curve hourly files: {num_load_curve_hourly_files}")
    print(f"Number of metadata bldg_ids: {len(metadata_bldg_ids)}")


def main():
    parser = argparse.ArgumentParser(description="Verify resstock data download.")
    parser.add_argument(
        "--data_path", required=True, help="Base path for resstock data"
    )
    parser.add_argument(
        "--release",
        required=True,
        help="Resstock release name (e.g. res_2024_amy2018_2)",
    )
    parser.add_argument("--state", required=True, help="State code (e.g. RI)")
    parser.add_argument("--upgrade_id", required=True, help="Upgrade id (e.g. 00)")
    args = parser.parse_args()
    test_resstock_download_to_s3(
        data_path=args.data_path,
        release=args.release,
        state=args.state,
        upgrade_id=args.upgrade_id,
    )


if __name__ == "__main__":
    main()
