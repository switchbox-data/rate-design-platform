import argparse
import io

import polars as pl
from cloudpathlib import S3Path

IN_HVAC_COLUMNS = ["in.hvac_cooling_type", "in.hvac_heating_type"]

UPGRADE_HVAC_COLUMNS = [
    "upgrade.hvac_cooling_efficiency",
    "upgrade.hvac_heating_efficiency",
]


def get_upgrade_ids(data_path: str, release: str, state: str) -> list[str]:
    """Get the upgrade ids for the given state, release, and data path."""
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


def identify_hp_customers(metadata_path: S3Path, upgrade_id: str):
    metadata_bytes = metadata_path.read_bytes()
    metadata_df = pl.read_parquet(io.BytesIO(metadata_bytes))

    if upgrade_id == "00":
        hvac_cooling_type = metadata_df[IN_HVAC_COLUMNS[0]]
        hvac_heating_type = metadata_df[IN_HVAC_COLUMNS[1]]
        hp_customers = (
            hvac_cooling_type.str.contains("Heat Pump", literal=True)
            & hvac_heating_type.str.contains("Heat Pump", literal=True)
        ).fill_null(False)
    else:
        in_hvac_cooling_type = metadata_df[IN_HVAC_COLUMNS[0]]
        in_hvac_heating_type = metadata_df[IN_HVAC_COLUMNS[1]]
        upgrade_hvac_cooling_type = metadata_df[UPGRADE_HVAC_COLUMNS[0]]
        upgrade_hvac_heating_type = metadata_df[UPGRADE_HVAC_COLUMNS[1]]
        # Row-wise OR: upgrade path is HP *or* in.path is HP
        upgrade_is_hp = upgrade_hvac_cooling_type.str.contains(
            "Heat Pump", literal=True
        ) & (
            upgrade_hvac_heating_type.str.contains("MSHP", literal=True)
            | upgrade_hvac_heating_type.str.contains("ASHP", literal=True)
            | upgrade_hvac_heating_type.str.contains("GSHP", literal=True)
        )
        in_is_hp = in_hvac_cooling_type.str.contains("Heat Pump", literal=True) & (
            in_hvac_heating_type.str.contains("Heat Pump", literal=True)
        )
        hp_customers = (upgrade_is_hp | in_is_hp).fill_null(False)
    metadata_df = metadata_df.with_columns(
        hp_customers.alias("postprocess_group.has_hp")
    )
    return metadata_df


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
        print(f"Processing upgrade id: {upgrade_id}")
        print(f"Number of bldg_ids in the metadata parquet: {len(metadata_bldg_ids)}")
        sb_metadata_df = identify_hp_customers(metadata_path, upgrade_id)
        output_path = (
            release_dir
            / "metadata"
            / f"state={state}"
            / f"upgrade={upgrade_id}"
            / "metadata-sb.parquet"
        )
        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True)
        # Polars doesn't accept S3Path; write to buffer then upload via cloudpathlib
        buf = io.BytesIO()
        sb_metadata_df.write_parquet(buf)
        output_path.write_bytes(buf.getvalue())
        print(f"Wrote metadata to {output_path}")
    print("All upgrades processed")


def main():
    parser = argparse.ArgumentParser(description="Add has_HP column to metadata.")
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
    add_has_HP_column(data_path=args.data_path, release=args.release, state=args.state)


if __name__ == "__main__":
    main()
