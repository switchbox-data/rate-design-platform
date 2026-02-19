import argparse
import ast
import json
import warnings
from pathlib import Path
from typing import cast

import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region
from utils.types import electric_utility

STORAGE_OPTIONS = {"aws_region": get_aws_region()}


def _parse_tuple_value(raw: str | bool | int) -> str | bool | int:
    """Coerce a value from JSON (often string) to bool, int, or str in that order."""
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        return raw
    s = str(raw).strip()
    if s == "True":
        return True
    if s == "False":
        return False
    try:
        return int(s)
    except ValueError:
        pass
    return s


def _parse_group_to_tariff_key(
    raw: dict[str, str],
) -> dict[tuple[str | bool | int, ...], str]:
    """Parse JSON group_to_tariff_key: keys are tuple strings, values are coerced bool/int/str."""
    result: dict[tuple[str | bool | int, ...], str] = {}
    for k, v in raw.items():
        parsed_tuple = ast.literal_eval(k)
        if not isinstance(parsed_tuple, tuple):
            raise TypeError(
                f"Expected tuple key, got {type(parsed_tuple).__name__}: {k!r}"
            )
        key = tuple(_parse_tuple_value(x) for x in parsed_tuple)
        result[key] = v
    return result


def _eq_for_key_value(col_name: str, value: str | bool | int) -> pl.Expr:
    """Build (col == value) with column cast to match value type so comparison is correct."""
    if isinstance(value, bool):
        return pl.col(col_name).cast(pl.Boolean) == pl.lit(value)
    if isinstance(value, int):
        return pl.col(col_name).cast(pl.Int64) == pl.lit(value)
    return pl.col(col_name).cast(pl.Utf8) == pl.lit(value)


def define_electrical_tariff_key(
    electrical_utility_name: electric_utility,
    grouping_cols: list[str],
    group_to_tariff_key: dict[tuple[str | bool | int, ...], str],
) -> pl.Expr:
    """Build a Polars expression that maps each row's grouping-col values to a tariff key string.

    grouping_cols: column names in the frame (e.g. ["postprocess_group.has_hp", "heats_with_natgas"]).
    group_to_tariff_key: map from (value_per_col, ...) to tariff key, e.g. {(True, False): "no_HP_natgas_flat"}.
    """

    # If default, everyone gets the same default tariff key.
    if grouping_cols == ["default"]:
        default_tariff_key = group_to_tariff_key[("default",)]
        return pl.lit(f"{electrical_utility_name}_{default_tariff_key}")

    # Build when/then from the end so first key in dict is checked first
    expr: pl.Expr = pl.lit(None).cast(pl.Utf8)
    for key_tuple, tariff_key in reversed(list(group_to_tariff_key.items())):
        cond = _eq_for_key_value(grouping_cols[0], key_tuple[0])
        for i in range(1, len(grouping_cols)):
            cond = cond & _eq_for_key_value(grouping_cols[i], key_tuple[i])
        expr = (
            pl.when(cond)
            .then(pl.lit(f"{electrical_utility_name}_{tariff_key}"))
            .otherwise(expr)
        )
    return expr


def generate_electrical_tariff_mapping(
    metadata: pl.LazyFrame,
    grouping_cols: list[str],
    group_to_tariff_key: dict[tuple[str | bool | int, ...], str],
    electric_utility: electric_utility,
) -> pl.LazyFrame:
    """Build a LazyFrame with bldg_id and tariff_key from metadata and the group→tariff mapping."""
    electrical_tariff_mapping_df = metadata.select(
        pl.col("bldg_id"),
        define_electrical_tariff_key(
            electric_utility, grouping_cols, group_to_tariff_key
        ).alias("tariff_key"),
    )
    return electrical_tariff_mapping_df


def map_electric_tariff(
    SB_metadata_df: pl.LazyFrame,
    electric_utility: electric_utility,
    grouping_cols: list[str],
    group_to_tariff_key: dict[tuple[str | bool | int, ...], str],
) -> pl.LazyFrame:
    utility_metadata_df = SB_metadata_df.filter(
        pl.col("sb.electric_utility") == electric_utility
    )

    if grouping_cols != ["default"]:
        metadata_with_grouping_cols = utility_metadata_df.select(
            pl.col(*grouping_cols, "bldg_id")
        )
        # Check if there are any rows in the filtered dataframe
        test_sample = cast(pl.DataFrame, metadata_with_grouping_cols.head(1).collect())
        if test_sample.is_empty():
            raise ValueError(f"No rows found for electric utility {electric_utility}")
    else:
        metadata_with_grouping_cols = utility_metadata_df.select(pl.col("bldg_id"))

    electrical_tariff_mapping_df = generate_electrical_tariff_mapping(
        metadata_with_grouping_cols,
        grouping_cols,
        group_to_tariff_key,
        electric_utility,
    )

    return electrical_tariff_mapping_df


if __name__ == "__main__":
    warnings.simplefilter("always", DeprecationWarning)

    parser = argparse.ArgumentParser(
        description="Utility to help assign electricity tariffs to utility customers."
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
    parser.add_argument("--state", required=True, help="State code (e.g. RI)")
    parser.add_argument("--upgrade_id", required=True, help="Upgrade id (e.g. 00)")
    parser.add_argument(
        "--electrical_tariff_key_map_path",
        required=True,
        help="Path to electrical tariff key map JSON file (e.g. utils/pre/electrical_tariff_key_map.json)",
    )
    parser.add_argument(
        "--run_name",
        required=True,
        help="Run name (e.g. ri_rie_run1_up00_precalc__flat__n100)",
    )
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
        # Check that both LazyFrames have the same bldg_id values (anti-join to avoid materializing full id lists)
        meta_bldg = SB_metadata.select("bldg_id").unique()
        util_bldg = utility_assignment.select("bldg_id").unique()
        in_meta_not_util = cast(
            pl.DataFrame,
            meta_bldg.join(util_bldg, on="bldg_id", how="anti").collect(),
        )
        in_util_not_meta = cast(
            pl.DataFrame,
            util_bldg.join(meta_bldg, on="bldg_id", how="anti").collect(),
        )
        if in_meta_not_util.height > 0 or in_util_not_meta.height > 0:
            meta_count = cast(
                pl.DataFrame,
                SB_metadata.select(pl.col("bldg_id").n_unique().alias("n")).collect(),
            )["n"][0]
            util_count = cast(
                pl.DataFrame,
                utility_assignment.select(
                    pl.col("bldg_id").n_unique().alias("n")
                ).collect(),
            )["n"][0]
            raise ValueError(
                f"bldg_id mismatch between metadata and utility_assignment:\n"
                f"  Metadata has {meta_count} unique bldg_ids\n"
                f"  Utility assignment has {util_count} unique bldg_ids"
            )

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

    # Read in electrical tariff key map JSON file and group-tariff key mapping.
    if not Path(args.electrical_tariff_key_map_path).exists():
        raise FileNotFoundError(
            f"Electrical tariff key map path {args.electrical_tariff_key_map_path} does not exist"
        )
    with open(args.electrical_tariff_key_map_path, "r") as f:
        electrical_tariff_key_map = json.load(f)
    if args.run_name not in electrical_tariff_key_map:
        raise KeyError(
            f"Run name {args.run_name!r} not found in electrical tariff key map. "
            f"Available runs: {list(electrical_tariff_key_map.keys())}"
        )
    run_config = electrical_tariff_key_map[args.run_name]
    grouping_cols: list[str] = run_config["grouping_cols"]
    raw_group_to_tariff_key: dict[str, str] = run_config["group_to_tariff_key"]
    group_to_tariff_key = _parse_group_to_tariff_key(raw_group_to_tariff_key)
    electrical_tariff_mapping_df = map_electric_tariff(
        SB_metadata_df=SB_metadata_with_utilities,
        electric_utility=args.electric_utility,
        grouping_cols=grouping_cols,
        group_to_tariff_key=group_to_tariff_key,
    )
    output_filename = run_config["output_filename"]
    try:
        base_path = S3Path(args.output_dir)
        output_path = base_path / output_filename
        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True)
        electrical_tariff_mapping_df.sink_csv(
            str(output_path), storage_options=STORAGE_OPTIONS
        )
    except ValueError:
        base_path = Path(args.output_dir)
        output_path = base_path / output_filename
        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True)
        electrical_tariff_mapping_df.sink_csv(str(output_path))
