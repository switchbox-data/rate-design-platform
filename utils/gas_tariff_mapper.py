import argparse
from pathlib import Path
from typing import cast

import polars as pl
from cloudpathlib import S3Path

from utils import get_storage_options
from utils.types import electric_utility

STORAGE_OPTIONS = get_storage_options()


def map_gas_tariff(
    SB_metadata_df: pl.LazyFrame,
    electric_utility_name: electric_utility,
) -> pl.LazyFrame:
    utility_metadata_df = SB_metadata_df.filter(
        pl.col("sb.electric_utility") == electric_utility_name
    )

    # Check if there are any rows in the filtered dataframe
    test_sample = cast(pl.DataFrame, utility_metadata_df.head(1).collect())
    if test_sample.is_empty():
        raise ValueError(f"No rows found for electric utility {electric_utility_name}")

    gas_tariff_mapping_df = (
        utility_metadata_df.select(pl.col("bldg_id", "sb.gas_utility"))
        .with_columns(
            pl.when(pl.col("sb.gas_utility") == "National Grid")
            .then(pl.lit("national_grid"))
            .otherwise(pl.lit("nyseg"))
            .alias("tariff_key")
        )
        .drop("sb.gas_utility")
    )

    return gas_tariff_mapping_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Utility to help assign gas tariffs to utility customers."
    )
    parser.add_argument(
        "--metadata_path",
        required=True,
        help="Absolute or s3 path to ResStock metadata",
    )
    parser.add_argument("--state", required=True, help="State code (e.g. NY, RI)")
    parser.add_argument("--upgrade_id", required=True, help="Upgrade id (e.g. 00)")
    parser.add_argument(
        "--electric_utility", required=True, help="Electric utility (e.g. Coned)"
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Output directory for output CSV",
    )
    args = parser.parse_args()

    #########################################################
    # For now, we will manually add the electric and gas utility columns. Later the metadata parquet will include them.
    # Electric: first ~1/3 Coned, next ~1/3 National Grid, last ~1/3 NYSEG. Gas: half National Grid, half NYSEG.
    try:  # If the metadata path is an S3 path, use the S3Path class.
        base_path = S3Path(args.metadata_path)
        metadata_path = (
            base_path
            / f"state={args.state}"
            / f"upgrade={args.upgrade_id}"
            / "metadata-sb.parquet"
        )
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")
        SB_metadata_df = pl.scan_parquet(
            str(metadata_path), storage_options=STORAGE_OPTIONS
        )
    except ValueError:  # If the metadata path is a local path, use the Path class.
        base_path = Path(args.metadata_path)
        metadata_path = (
            base_path
            / f"state={args.state}"
            / f"upgrade={args.upgrade_id}"
            / "metadata-sb.parquet"
        )
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")
        SB_metadata_df = pl.scan_parquet(str(metadata_path))

    SB_metadata_df_with_utilities = SB_metadata_df.with_columns(
        pl.when(pl.col("bldg_id").hash() % 3 == 0)
        .then(pl.lit("Coned"))
        .when(pl.col("bldg_id").hash() % 3 == 1)
        .then(pl.lit("National Grid"))
        .otherwise(pl.lit("NYSEG"))
        .alias("sb.electric_utility"),
        pl.when((pl.col("bldg_id").hash() % 2) == 0)
        .then(pl.lit("National Grid"))
        .otherwise(pl.lit("NYSEG"))
        .alias("sb.gas_utility"),
    )
    #########################################################

    gas_tariff_mapping_df = map_gas_tariff(
        SB_metadata_df=SB_metadata_df_with_utilities,
        electric_utility_name=args.electric_utility,
    )

    output_filename = f"{args.electric_utility}_gas.csv"
    try:
        out_base = S3Path(args.output_dir)
        output_path = out_base / output_filename
        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True)
        gas_tariff_mapping_df.sink_csv(
            str(output_path), storage_options=STORAGE_OPTIONS
        )
    except ValueError:
        out_base = Path(args.output_dir)
        output_path = out_base / output_filename
        if not output_path.parent.exists():
            out_base.mkdir(parents=True, exist_ok=True)
        gas_tariff_mapping_df.sink_csv(str(output_path))
