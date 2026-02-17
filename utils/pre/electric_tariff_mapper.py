import argparse
import warnings
from pathlib import Path
from typing import cast

import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region
from utils.types import SBScenario, ElectricUtility

STORAGE_OPTIONS = {"aws_region": get_aws_region()}


def define_electrical_tariff_key(
    SB_scenario: SBScenario,
    electric_utility: ElectricUtility,
    has_hp: pl.Expr,
) -> pl.Expr:
    if SB_scenario.analysis_type == "default":
        return pl.lit(f"{electric_utility}_{SB_scenario}_default")
    elif SB_scenario.analysis_type in ["seasonal", "class_specific_seasonal"]:
        return (
            pl.when(has_hp)
            .then(pl.lit(f"{electric_utility}_{SB_scenario}_HP.csv"))
            .otherwise(pl.lit(f"{electric_utility}_{SB_scenario}_flat.csv"))
        )
    else:
        raise ValueError(f"Invalid SB scenario: {SB_scenario}")


def generate_electrical_tariff_mapping(
    lazy_metadata_has_hp: pl.LazyFrame,
    SB_scenario: SBScenario,
    electric_utility: ElectricUtility,
) -> pl.LazyFrame:
    electrical_tariff_mapping_df = lazy_metadata_has_hp.select(
        pl.col("bldg_id"),
        define_electrical_tariff_key(
            SB_scenario, electric_utility, pl.col("postprocess_group.has_hp")
        ).alias("tariff_key"),
    )

    return electrical_tariff_mapping_df


def map_electric_tariff(
    SB_metadata_df: pl.LazyFrame,
    electric_utility: ElectricUtility,
    SB_scenario: SBScenario,
    state: str,
) -> pl.LazyFrame:
    utility_metadata_df = SB_metadata_df.filter(
        pl.col("sb.electric_utility") == electric_utility
    )

    metadata_has_hp = utility_metadata_df.select(
        pl.col("bldg_id", "postprocess_group.has_hp")
    )

    # Check if there are any rows in the filtered dataframe
    test_sample = cast(pl.DataFrame, metadata_has_hp.head(1).collect())
    if test_sample.is_empty():
        raise ValueError(f"No rows found for electric utility {electric_utility}")

    electrical_tariff_mapping_df = generate_electrical_tariff_mapping(
        metadata_has_hp, SB_scenario, electric_utility
    )

    return electrical_tariff_mapping_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Utility to help assign electricity tariffs to utility customers."
    )
    parser.add_argument(
        "--metadata_path",
        required=True,
        help="Absolute or s3 path to ResStock metadata",
    )
    parser.add_argument("--state", required=True, help="State code (e.g. RI)")
    parser.add_argument("--upgrade_id", required=True, help="Upgrade id (e.g. 00)")
    parser.add_argument(
        "--electric_utility",
        required=True,
        help="Electric utility std_name (e.g. coned, nyseg, nimo)",
    )
    parser.add_argument(
        "--SB_scenario_type",
        required=True,
        help="SB scenario type (e.g. default, seasonal, class_specific_seasonal)",
    )
    parser.add_argument(
        "--SB_scenario_year", required=True, help="SB scenario year (e.g. 1 , 2, 3)"
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

    # Use real sb.electric_utility if present; else fall back to synthetic (deprecated)
    schema_cols = SB_metadata_df.collect_schema().names()
    if "sb.electric_utility" in schema_cols:
        SB_metadata_df_with_electric_utility = SB_metadata_df
    else:
        warnings.warn(
            "metadata has no sb.electric_utility column; using synthetic data. "
            "Run assign_utility_ny (data/resstock/) and point --metadata_path to metadata_utility for real data.",
            DeprecationWarning,
            stacklevel=2,
        )
        SB_metadata_df_with_electric_utility = SB_metadata_df.with_columns(
            pl.when(pl.col("bldg_id").hash() % 3 == 0)
            .then(pl.lit("coned"))
            .when(pl.col("bldg_id").hash() % 3 == 1)
            .then(pl.lit("nimo"))
            .otherwise(pl.lit("nyseg"))
            .alias("sb.electric_utility")
        )

    sb_scenario = SBScenario(args.SB_scenario_type, args.SB_scenario_year)
    electrical_tariff_mapping_df = map_electric_tariff(
        SB_metadata_df=SB_metadata_df_with_electric_utility,
        electric_utility=args.electric_utility,
        SB_scenario=sb_scenario,
        state=args.state,
    )
    try:
        base_path = S3Path(args.output_dir)
        output_path = base_path / f"{args.electric_utility}_{sb_scenario}.csv"
        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True)
        electrical_tariff_mapping_df.sink_csv(
            str(output_path), storage_options=STORAGE_OPTIONS
        )
    except ValueError:
        base_path = Path(args.output_dir)
        output_path = base_path / f"{args.electric_utility}_{sb_scenario}.csv"
        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True)
        electrical_tariff_mapping_df.sink_csv(str(output_path))
