import argparse
from typing import cast

import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region

NATGAS_CONSUMPTION_COLUMN = "out.natural_gas.total.energy_consumption.kwh"
STORAGE_OPTIONS = {"aws_region": get_aws_region()}


def identify_natgas_connection(
    metadata: pl.LazyFrame, load_curve_annual: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Identify if the building has a natural gas connection.

    - Reads heats_with_natgas (and optional has_natgas_connection) from metadata.
    - Derives has_natgas_connection from load_curve_annual where
      out.natural_gas.total.energy_consumption.kwh > 0 (aggregated by bldg_id).
    - Sanity check: every row with heats_with_natgas=True must have
      has_natgas_connection=True.
    """
    # Require metadata to have heats_with_natgas; drop has_natgas_connection if present
    metadata_cols = metadata.collect_schema().names()
    if "heats_with_natgas" not in metadata_cols:
        raise ValueError(
            "Metadata must contain column 'heats_with_natgas'. "
            "Run identify_heating_type before identify_natgas_connection."
        )
    metadata = metadata.drop("has_natgas_connection", strict=False)

    # Derive has_natgas_connection from load_curve_annual (natgas consumption > 0).
    # load_curve_annual has one row per bldg_id.
    natgas_from_load = load_curve_annual.select(
        "bldg_id",
        pl.col(NATGAS_CONSUMPTION_COLUMN).gt(0).alias("has_natgas_connection"),
    )

    # Inner join: every metadata bldg_id must appear in load_curve_annual.
    result = metadata.join(natgas_from_load, on="bldg_id", how="inner").with_columns(
        pl.col("has_natgas_connection").fill_null(False)
    )

    # Check row counts in one collect (ensures load_curve_annual has all metadata bldg_ids).
    counts = cast(
        pl.DataFrame,
        metadata.select(pl.len().alias("n_metadata"))
        .join(result.select(pl.len().alias("n_result")), how="cross")
        .collect(),
    )
    n_metadata, n_result = counts.row(0)
    if n_result != n_metadata:
        raise ValueError(
            f"Row count mismatch: metadata has {n_metadata} rows, "
            f"but result has {n_result} rows. "
            "load_curve_annual must contain every bldg_id in metadata."
        )

    # Sanity check: heats_with_natgas=True => must have positive natural gas consumption
    n_violations = cast(
        pl.DataFrame,
        result.filter(
            pl.col("heats_with_natgas") & ~pl.col("has_natgas_connection")
        )
        .select(pl.len())
        .collect(),
    ).item()
    if n_violations > 0:
        raise ValueError(
            "Sanity check failed: If a bldg id has heats_with_natgas=True, then it must have positive natural gas consumption"
        )

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add has_natgas_connection column to ResStock metadata."
    )
    parser.add_argument(
        "--input_metadata_dir",
        required=True,
        help="Full S3Path url pointing to the input metadata file directory",
    )
    parser.add_argument(
        "--input_load_curve_annual_dir",
        required=True,
        help="Full S3Path url pointing to the input load curve annual file directory",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Full S3Path url pointing to the output directory",
    )
    parser.add_argument(
        "--input_metadata_filename",
        required=True,
        help="Input filename (e.g. 'metadata.parquet')",
    )
    parser.add_argument(
        "--input_load_curve_annual_filename",
        required=True,
        help="Input filename (e.g. 'RI_upgrade00_metadata_and_annual_results.parquet')",
    )
    parser.add_argument(
        "--output_filename",
        required=True,
        help="Output filename (e.g. 'metadata-sb.parquet')",
    )
    args = parser.parse_args()

    # Read input metadata file
    input_metadata_path = S3Path(args.input_metadata_dir) / args.input_metadata_filename
    if not input_metadata_path.exists():
        raise FileNotFoundError(f"Metadata file {input_metadata_path} does not exist")
    input_metadata = pl.scan_parquet(
        str(input_metadata_path), storage_options=STORAGE_OPTIONS
    )
    # Read input load curve annual file
    input_load_curve_annual_path = (
        S3Path(args.input_load_curve_annual_dir) / args.input_load_curve_annual_filename
    )
    if not input_load_curve_annual_path.exists():
        raise FileNotFoundError(
            f"Load curve annual file {input_load_curve_annual_path} does not exist"
        )
    input_load_curve_annual = pl.scan_parquet(
        str(input_load_curve_annual_path), storage_options=STORAGE_OPTIONS
    )
    # Add `has_natgas_connection` column
    output_metadata = identify_natgas_connection(
        metadata=input_metadata, load_curve_annual=input_load_curve_annual
    )
    # Write output metadata file
    output_path = S3Path(args.output_dir) / args.output_filename
    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True)
    output_metadata.sink_parquet(str(output_path), storage_options=STORAGE_OPTIONS)
    print(f"Added has_natgas_connection column and wrote metadata to {output_path}")
