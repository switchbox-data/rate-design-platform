import argparse
from pathlib import Path
from typing import cast

import polars as pl
from cloudpathlib import S3Path

from utils.types import SBScenario, electric_utility

# Project root (rate-design-platform); independent of cwd or caller
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
RATE_DESIGN_DIR = _PROJECT_ROOT / "rate_design"

AWS_REGION = "us-west-2"

STORAGE_OPTIONS = {"aws_region": AWS_REGION}


def define_electrical_tariff_key(
    SB_scenario: SBScenario,
    electric_utility: electric_utility,
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
    electric_utility: electric_utility,
) -> pl.LazyFrame:
    electrical_tariff_mapping_df = lazy_metadata_has_hp.select(
        pl.col("bldg_id"),
        define_electrical_tariff_key(
            SB_scenario, electric_utility, pl.col("postprocess_group.has_hp")
        ).alias("tariff_key"),
    )

    return electrical_tariff_mapping_df


def map_electric_tariff(
    SB_metadata_lazy_df: pl.LazyFrame,
    electric_utility: electric_utility,
    SB_scenario: SBScenario,
    state: str,
) -> pl.LazyFrame:
    utility_metadata_df = SB_metadata_lazy_df.filter(
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
    parser = argparse.ArgumentParser(description="Map electrical tariff.")
    parser.add_argument(
        "--metadata_path",
        required=True,
        help="Absolute or s3 path to ResStock metadata",
    )
    parser.add_argument("--state", required=True, help="State code (e.g. RI)")
    parser.add_argument("--upgrade_id", required=True, help="Upgrade id (e.g. 00)")
    parser.add_argument(
        "--electric_utility", required=True, help="Electric utility (e.g. Coned)"
    )
    parser.add_argument(
        "--SB_scenario_type",
        required=True,
        help="SB scenario type (e.g. default, seasonal, class_specific_seasonal)",
    )
    parser.add_argument(
        "--SB_scenario_year", required=True, help="SB scenario year (e.g. 2024)"
    )
    parser.add_argument(
        "--output_dir",
        default=None,
        help="Optional directory for output CSV; default is rate_design/<state>/hp_rates/data/tariff_map/",
    )
    args = parser.parse_args()

    #########################################################
    # For now, we will manually add the electric utility name column. Later on, the metadata parquet will be updated to include this column.
    # Assign first ~1/3 to Coned, next ~1/3 to National Grid, last ~1/3 to NYSEG.
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
        # Polars scan_parquet needs a string path; S3Path.as_uri() gives s3:// URL
        SB_metadata_lazy_df = pl.scan_parquet(
            str(metadata_path), storage_options=STORAGE_OPTIONS
        )
    except ValueError:
        # If the metadata path is a local path, use the Path class.
        base_path = Path(args.metadata_path)
        metadata_path = (
            base_path
            / f"state={args.state}"
            / f"upgrade={args.upgrade_id}"
            / "metadata-sb.parquet"
        )
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")
        SB_metadata_lazy_df = pl.scan_parquet(str(metadata_path))

    # Add dummy electric utility column (deterministic by bldg_id). Later this column will be pre-existing in the SB metadata parquet.
    SB_metadata_lazy_df_with_electric_utility = SB_metadata_lazy_df.with_columns(
        pl.when(pl.col("bldg_id").hash() % 3 == 0)
        .then(pl.lit("Coned"))
        .when(pl.col("bldg_id").hash() % 3 == 1)
        .then(pl.lit("National Grid"))
        .otherwise(pl.lit("NYSEG"))
        .alias("sb.electric_utility")
    )
    #########################################################

    sb_scenario = SBScenario(args.SB_scenario_type, args.SB_scenario_year)
    electrical_tariff_mapping_df = map_electric_tariff(
        SB_metadata_lazy_df=SB_metadata_lazy_df_with_electric_utility,
        electric_utility=args.electric_utility,
        SB_scenario=sb_scenario,
        state=args.state,
    )
    if args.output_dir:
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
    else:
        output_path = (
            RATE_DESIGN_DIR
            / args.state.lower()
            / "hp_rates"
            / "data"
            / "tariff_map"
            / f"{args.electric_utility}_{sb_scenario}.csv"
        )
        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True)
        electrical_tariff_mapping_df.sink_csv(str(output_path))
