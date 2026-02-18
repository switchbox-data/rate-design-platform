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


def _tariff_key_expr() -> pl.Expr:
    building_type_column = pl.col("in.geometry_building_type_recs")
    stories_column = pl.col("in.geometry_stories_low_rise")
    heats_with_natgas_column = pl.col("heats_with_natgas")
    gas_utility_col = pl.col("sb.gas_utility")

    # Map std_name to gas_tariff_key
    gas_tariff_key_map = get_std_name_to_gas_tariff_key()
    gas_tariff_key_col = gas_utility_col.replace(gas_tariff_key_map)

    return (
        #### coned ####
        # coned: Single-Family
        pl.when(
            (gas_utility_col == "coned")
            & building_type_column.str.contains("Single-Family", literal=True)
        )
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_sf")]))
        # coned: Multi-Family high-rise
        .when(
            (gas_utility_col == "coned")
            & building_type_column.str.contains("Multi-Family", literal=True)
            & stories_column.str.contains("4+", literal=True)
        )
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_mf_highrise")]))
        # coned: Multi-Family low-rise
        .when(
            (gas_utility_col == "coned")
            & building_type_column.str.contains("Multi-Family", literal=True)
        )
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_mf_lowrise")]))
        #### coned ####
        #### kedny ####
        # kedny: Single-Family heating with natural gas
        .when(
            (gas_utility_col == "kedny")
            & building_type_column.str.contains("Single-Family", literal=True)
            & heats_with_natgas_column.eq(True)
        )
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_sf_heating")]))
        # kedny: Single-Family does not heat with natural gas
        .when(
            (gas_utility_col == "kedny")
            & building_type_column.str.contains("Single-Family", literal=True)
            & heats_with_natgas_column.eq(False)
        )
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_sf_nonheating")]))
        # kedny: All multi-Family
        .when(
            (gas_utility_col == "kedny")
            & building_type_column.str.contains("Multi-Family", literal=True)
        )
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_mf")]))
        #### kedny ####
        #### kedli ####
        # kedli: Single-Family heating with natural gas
        .when(
            (gas_utility_col == "kedli")
            & building_type_column.str.contains("Single-Family", literal=True)
            & heats_with_natgas_column.eq(True)
        )
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_sf_heating")]))
        # kedli: Single-Family does not heat with natural gas
        .when(
            (gas_utility_col == "kedli")
            & building_type_column.str.contains("Single-Family", literal=True)
            & heats_with_natgas_column.eq(False)
        )
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_sf_nonheating")]))
        # kedli: Multi-Family heating with natural gas
        .when(
            (gas_utility_col == "kedli")
            & building_type_column.str.contains("Multi-Family", literal=True)
            & heats_with_natgas_column.eq(True)
        )
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_mf_heating")]))
        # kedli: Multi-Family does not heat with natural gas
        .when(
            (gas_utility_col == "kedli")
            & building_type_column.str.contains("Multi-Family", literal=True)
            & heats_with_natgas_column.eq(False)
        )
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_mf_nonheating")]))
        #### kedli ####
        #### nyseg ####
        # nyseg: Heating with natural gas
        .when((gas_utility_col == "nyseg") & heats_with_natgas_column.eq(True))
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_heating")]))
        # nyseg: Does not heat with natural gas
        .when((gas_utility_col == "nyseg") & heats_with_natgas_column.eq(False))
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_nonheating")]))
        #### nyseg ####
        #### rie ####
        # rie: Heating with natural gas
        .when((gas_utility_col == "rie") & heats_with_natgas_column.eq(True))
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_heating")]))
        # rie: Does not heat with natural gas
        .when((gas_utility_col == "rie") & heats_with_natgas_column.eq(False))
        .then(pl.concat_str([gas_tariff_key_col, pl.lit("_nonheating")]))
        #### rie ####
        #### nimo | rge | cenhud | or | nfg ####
        .when(
            (gas_utility_col == "nimo")
            | (gas_utility_col == "rge")
            | (gas_utility_col == "cenhud")
            | (gas_utility_col == "or")
            | (gas_utility_col == "nfg")
        )
        .then(gas_tariff_key_col)
        #### nimo | rge | cenhud | or | nfg ####
        ### Null value in the gas_utility column gets assigned to "null_gas_tariff" ####
        .when(gas_utility_col.is_null())
        .then(pl.lit("null_gas_tariff"))
        ### Null value in the gas_utility column gets assigned to "null_gas_tariff" ####
        # Default: Use gas_tariff_key (mapped from std_name)
        .otherwise(gas_tariff_key_col)
        .fill_null(gas_utility_col)
        .alias("tariff_key")
    )


def map_gas_tariff(
    SB_metadata: pl.LazyFrame,
    electric_utility_name: electric_utility,
) -> pl.LazyFrame:
    # Filter metadata by electric utility
    utility_metadata = SB_metadata.filter(
        pl.col("sb.electric_utility") == electric_utility_name
    )

    # Check if there are any rows in the filtered dataframe
    test_sample = cast(pl.DataFrame, utility_metadata.head(1).collect())
    if test_sample.is_empty():
        return pl.LazyFrame()

    gas_tariff_mapping_df = (
        utility_metadata.select(
            pl.col(
                "bldg_id",
                "sb.gas_utility",
                "in.geometry_building_type_recs",
                "in.geometry_stories_low_rise",
                "heats_with_natgas",
            )
        )
        .with_columns(_tariff_key_expr())
        .drop(
            "sb.gas_utility",
            "in.geometry_building_type_recs",
            "in.geometry_stories_low_rise",
            "heats_with_natgas",
        )
    )

    return gas_tariff_mapping_df


if __name__ == "__main__":
    # Print warnings
    warnings.simplefilter("always", DeprecationWarning)
    parser = argparse.ArgumentParser(
        description="Utility to help assign gas tariffs to utility customers."
    )
    parser.add_argument(
        "--metadata_path",
        required=True,
        help="Absolute or s3 path to ResStock metadata",
    )
    parser.add_argument(
        "--utility_assignment_path",
        required=True,
        help="Absolute or s3 path to ResStock utility assignment",
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
        use_s3_metadata = True
    except ValueError:
        base_path = Path(args.metadata_path)
        use_s3_metadata = False

    try:  # If the utility assignment path is an S3 path, use the S3Path class.
        utility_assignment_base_path = S3Path(args.utility_assignment_path)
        use_s3_utility_assignment = True
    except ValueError:
        utility_assignment_base_path = Path(args.utility_assignment_path)
        use_s3_utility_assignment = False

    # Support metadata_utility path (utility_assignment.parquet) or metadata path (metadata-sb.parquet)
    if "metadata_utility" in str(args.utility_assignment_path):
        utility_assignment_path = (
            utility_assignment_base_path
            / f"state={args.state}"
            / "utility_assignment.parquet"
        )
    else:
        utility_assignment_path = (
            utility_assignment_base_path
            / f"state={args.state}"
            / f"upgrade={args.upgrade_id}"
            / "metadata-sb.parquet"
        )
    metadata_path = (
        base_path
        / f"state={args.state}"
        / f"upgrade={args.upgrade_id}"
        / "metadata-sb.parquet"
    )

    if use_s3_metadata and not metadata_path.exists():
        raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")
    if not use_s3_metadata and not Path(metadata_path).exists():
        raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")
    if use_s3_utility_assignment and not utility_assignment_path.exists():
        raise FileNotFoundError(
            f"Utility assignment path {utility_assignment_path} does not exist"
        )
    if not use_s3_utility_assignment and not Path(utility_assignment_path).exists():
        raise FileNotFoundError(
            f"Utility assignment path {utility_assignment_path} does not exist"
        )

    storage_opts_metadata = STORAGE_OPTIONS if use_s3_metadata else None
    storage_opts_utility_assignment = (
        STORAGE_OPTIONS if use_s3_utility_assignment else None
    )
    SB_metadata = (
        pl.scan_parquet(str(metadata_path), storage_options=storage_opts_metadata)
        if storage_opts_metadata
        else pl.scan_parquet(str(metadata_path))
    )
    utility_assignment = (
        pl.scan_parquet(
            str(utility_assignment_path),
            storage_options=storage_opts_utility_assignment,
        )
        if storage_opts_utility_assignment
        else pl.scan_parquet(str(utility_assignment_path))
    )

    # Use real sb.electric_utility and sb.gas_utility if present; else fall back to synthetic (deprecated)
    utility_assignment_schema_cols = utility_assignment.collect_schema().names()
    if (
        "sb.electric_utility" in utility_assignment_schema_cols
        and "sb.gas_utility" in utility_assignment_schema_cols
    ):
        # Check that both LazyFrames have the same bldg_id values
        metadata_bldg_ids_df = cast(
            pl.DataFrame,
            SB_metadata.select("bldg_id").unique().collect(),
        )
        metadata_bldg_ids = set(metadata_bldg_ids_df["bldg_id"].to_list())

        utility_bldg_ids_df = cast(
            pl.DataFrame,
            utility_assignment.select("bldg_id").unique().collect(),
        )
        utility_bldg_ids = set(utility_bldg_ids_df["bldg_id"].to_list())

        if metadata_bldg_ids != utility_bldg_ids:
            error_msg = (
                f"bldg_id mismatch between metadata and utility_assignment:\n"
                f"  Metadata has {len(metadata_bldg_ids)} unique bldg_ids\n"
                f"  Utility assignment has {len(utility_bldg_ids)} unique bldg_ids"
            )
            raise ValueError(error_msg)

        # Inner join ensures one-to-one matching; will fail if bldg_ids don't match
        SB_metadata_with_utilities = SB_metadata.join(
            utility_assignment.select(
                "bldg_id", "sb.electric_utility", "sb.gas_utility"
            ),
            on="bldg_id",
            how="inner",
        )

        # Verify join preserved all rows (catches duplicates or other issues)
        metadata_count_df = cast(
            pl.DataFrame,
            SB_metadata.select(pl.len()).collect(),
        )
        metadata_count = metadata_count_df.row(0)[0]

        joined_count_df = cast(
            pl.DataFrame,
            SB_metadata_with_utilities.select(pl.len()).collect(),
        )
        joined_count = joined_count_df.row(0)[0]
        if metadata_count != joined_count:
            raise ValueError(
                f"Join failed: metadata has {metadata_count} rows but inner join produced {joined_count} rows. "
                f"This indicates duplicate bldg_ids or non-matching values."
            )
    else:
        warnings.warn(
            "metadata has no sb.electric_utility/sb.gas_utility columns; using synthetic data. "
            "Run assign_utility_ny (data/resstock/) and point --metadata_path to metadata_utility for real data.",
            DeprecationWarning,
            stacklevel=2,
        )
        SB_metadata_with_utilities = SB_metadata.with_columns(
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
        SB_metadata=SB_metadata_with_utilities,
        electric_utility_name=args.electric_utility,
    )
    # Check if the result has any rows before writing. If there are no rows assigned to the electric utility, empty lazyframe is returned.
    row_count_df = cast(
        pl.DataFrame,
        gas_tariff_mapping_df.select(pl.len()).collect(),
    )
    row_count = row_count_df.row(0)[0]
    if row_count == 0:
        warnings.warn(
            f"No rows found for electric utility {args.electric_utility}.",
            DeprecationWarning,
            stacklevel=2,
        )
    else:
        output_filename = f"{args.electric_utility}.csv"
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
