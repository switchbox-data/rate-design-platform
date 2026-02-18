import argparse

import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region

STORAGE_OPTIONS = {"aws_region": get_aws_region()}


def assign_utility_ri(input_metadata: pl.LazyFrame) -> pl.LazyFrame:
    """
    Assign electric and gas utilities to ResStock buildings in RI.

    - sb.electric_utility: All rows get "rie"
    - sb.gas_utility: Only rows with has_natgas_connection=True get "rie", others get null
    """
    # Check that has_natgas_connection column exists
    schema_cols = input_metadata.collect_schema().names()
    if "has_natgas_connection" not in schema_cols:
        raise ValueError(
            "Missing required column 'has_natgas_connection'. "
            "Run identify_heating_type first to add this column."
        )

    # Drop existing utility columns if they exist (equivalent to ADD COLUMN IF NOT EXISTS)
    input_metadata = input_metadata.drop(
        ["sb.electric_utility", "sb.gas_utility"], strict=False
    )

    return input_metadata.with_columns(
        # All rows get "rie" for electric utility
        pl.lit("rie").alias("sb.electric_utility"),
        # Only rows with has_natgas_connection=True get "rie" for gas utility, others get null
        pl.when(pl.col("has_natgas_connection").eq(True))
        .then(pl.lit("rie"))
        .otherwise(None)
        .alias("sb.gas_utility"),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Assign electric and gas utilities to ResStock buildings in RI"
    )
    parser.add_argument(
        "--input_metadata_dir",
        type=str,
        required=True,
        help="Input directory to metadata parquet file (S3Path format)",
    )
    parser.add_argument(
        "--input_metadata_filename",
        type=str,
        required=True,
        help="Input filename to metadata parquet file (e.g. metadata-sb.parquet)",
    )
    parser.add_argument(
        "--output_metadata_dir",
        type=str,
        required=True,
        help="Output directory for parquet file with utility assignment (S3Path format)",
    )
    parser.add_argument(
        "--output_utility_assignment_filename",
        type=str,
        required=True,
        help="Output filename for parquet file with utility assignment (e.g. utility_assignment.parquet)",
    )

    args = parser.parse_args()

    input_dir_s3 = S3Path(args.input_metadata_dir)
    input_path_s3 = S3Path(input_dir_s3 / args.input_metadata_filename)
    input_metadata = pl.scan_parquet(
        str(input_path_s3), storage_options=STORAGE_OPTIONS
    )

    output_dir_s3 = S3Path(args.output_metadata_dir)
    output_path_s3 = S3Path(output_dir_s3 / args.output_utility_assignment_filename)

    metadata_with_utility_assignment = assign_utility_ri(
        input_metadata=input_metadata,
    )

    # Write back to the output path using sink_parquet
    metadata_with_utility_assignment.sink_parquet(
        str(output_path_s3), compression="zstd", storage_options=STORAGE_OPTIONS
    )
