import argparse
import warnings
from pathlib import Path
from typing import cast

import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region
from utils.types import electric_utility
from utils.utility_codes import get_std_name_to_gas_tariff_key

STORAGE_OPTIONS = {"aws_region": get_aws_region()}


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

    gas_tariff_map = get_std_name_to_gas_tariff_key()

    def _tariff_key_expr() -> pl.Expr:
        # Map sb.gas_utility (std_name) -> tariff_key via crosswalk; fallback to std_name
        return (
            pl.col("sb.gas_utility")
            .replace(gas_tariff_map)
            .fill_null(pl.col("sb.gas_utility"))
            .alias("tariff_key")
        )

    gas_tariff_mapping_df = (
        utility_metadata_df.select(pl.col("bldg_id", "sb.gas_utility"))
        .with_columns(_tariff_key_expr())
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
        "--electric_utility",
        required=True,
        help="Electric utility std_name (e.g. coned, nyseg, nimo)",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Output directory for output CSV",
    )
    args = parser.parse_args()

    try:  # If the metadata path is an S3 path, use the S3Path class.
        base_path = S3Path(args.metadata_path)
        use_s3 = True
    except ValueError:
        base_path = Path(args.metadata_path)
        use_s3 = False

    # Support metadata_utility path (utility_assignment.parquet) or metadata path (metadata-sb.parquet)
    if "metadata_utility" in str(args.metadata_path):
        metadata_path = base_path / f"state={args.state}" / "utility_assignment.parquet"
    else:
        metadata_path = (
            base_path
            / f"state={args.state}"
            / f"upgrade={args.upgrade_id}"
            / "metadata-sb.parquet"
        )

    if use_s3 and not metadata_path.exists():
        raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")
    if not use_s3 and not Path(metadata_path).exists():
        raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")

    storage_opts = STORAGE_OPTIONS if use_s3 else None
    SB_metadata_df = (
        pl.scan_parquet(str(metadata_path), storage_options=storage_opts)
        if storage_opts
        else pl.scan_parquet(str(metadata_path))
    )

    # Use real sb.electric_utility and sb.gas_utility if present; else fall back to synthetic (deprecated)
    schema_cols = SB_metadata_df.collect_schema().names()
    if "sb.electric_utility" in schema_cols and "sb.gas_utility" in schema_cols:
        SB_metadata_df_with_utilities = SB_metadata_df
    else:
        warnings.warn(
            "metadata has no sb.electric_utility/sb.gas_utility columns; using synthetic data. "
            "Run assign_utility_ny and point --metadata_path to metadata_utility for real data.",
            DeprecationWarning,
            stacklevel=2,
        )
        SB_metadata_df_with_utilities = SB_metadata_df.with_columns(
            pl.when(pl.col("bldg_id").hash() % 3 == 0)
            .then(pl.lit("coned"))
            .when(pl.col("bldg_id").hash() % 3 == 1)
            .then(pl.lit("nimo"))
            .otherwise(pl.lit("nyseg"))
            .alias("sb.electric_utility"),
            pl.when((pl.col("bldg_id").hash() % 2) == 0)
            .then(pl.lit("nimo"))
            .otherwise(pl.lit("nyseg"))
            .alias("sb.gas_utility"),
        )

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
