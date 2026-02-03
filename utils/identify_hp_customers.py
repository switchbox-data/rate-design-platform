import argparse
import io

import polars as pl
from cloudpathlib import S3Path


def get_upgrade_ids(data_path: str, release: str, state: str) -> list[str]:
    """Test that the resstock data is downloaded to the correct path."""
    base = S3Path(data_path)
    release_dir = base / release
    if not release_dir.exists():
        raise FileNotFoundError(f"Release directory {release_dir} does not exist")

    metadata_dir = release_dir / "metadata" / f"state={state}"
    if not metadata_dir.exists():
        raise FileNotFoundError(f"Metadata directory {metadata_dir} does not exist")

    upgrade_ids = [
        path.name.split("=")[1]
        for path in metadata_dir.iterdir()
        if path.is_dir() and path.name.startswith("upgrade=")
    ]
    print(f"Upgrade ids: {upgrade_ids}")
    return upgrade_ids


def add_has_HP_column(data_path: str, release: str, state: str):
    upgrade_ids = get_upgrade_ids(data_path, release, state)
    base = S3Path(data_path)
    release_dir = base / release
    if not release_dir.exists():
        raise FileNotFoundError(f"Release directory {release_dir} does not exist")

    for upgrade_id in upgrade_ids:
        metadata_path = (
            release_dir
            / "metadata"
            / f"state={state}"
            / f"upgrade={upgrade_id}"
            / "metadata.parquet"
        )
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata file {metadata_path} does not exist")

        metadata_bytes = metadata_path.read_bytes()
        metadata_bldg_ids: pl.DataFrame = (
            pl.read_parquet(io.BytesIO(metadata_bytes)).select("bldg_id").unique()
        )
        print(f"Number of metadata bldg ids: {len(metadata_bldg_ids)}")


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
    args = parser.parse_args()
    add_has_HP_column(
        data_path=args.data_path,
        release=args.release,
        state=args.state,
    )


if __name__ == "__main__":
    add_has_HP_column(
        data_path="s3://data.sb/nrel/resstock/",
        release="res_2024_amy2018_2",
        state="NY",
    )
