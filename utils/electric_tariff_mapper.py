import argparse
import io
from pathlib import Path

import polars as pl
from cloudpathlib import S3Path

from utils.types import SB_scenario, electric_utility

# Project root (rate-design-platform); independent of cwd or caller
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
RATE_DESIGN_DIR = _PROJECT_ROOT / "rate_design"


def define_electrical_tariff_key(
    SB_scenario: SB_scenario,
    electric_utility: electric_utility,
    has_hp: pl.Series,
) -> pl.Expr:
    if SB_scenario.analysis_type == "default":
        return pl.lit(f"{electric_utility}_{SB_scenario}_default")
    elif (
        SB_scenario.analysis_type == "seasonal"
        or SB_scenario.analysis_type == "class_specific_seasonal"
    ):
        return (
            pl.when(has_hp)
            .then(pl.lit(f"{electric_utility}_{SB_scenario}_HP.csv"))
            .otherwise(pl.lit(f"{electric_utility}_{SB_scenario}_flat.csv"))
        )
    else:
        raise ValueError(f"Invalid SB scenario: {SB_scenario}")


def generate_electrical_tariff_mapping(
    metadata_has_hp: pl.DataFrame,
    SB_scenario: SB_scenario,
    electric_utility: electric_utility,
) -> pl.DataFrame:
    has_hp = metadata_has_hp["postprocess_group.has_hp"]

    electrical_tariff_mapping_df = metadata_has_hp.select(
        pl.col("bldg_id")
    ).with_columns(
        define_electrical_tariff_key(SB_scenario, electric_utility, has_hp).alias(
            "tariff_key"
        )
    )

    return electrical_tariff_mapping_df


def map_electric_tariff(
    SB_metadata_path: S3Path,
    electric_utility: electric_utility,
    SB_scenario: SB_scenario,
    state: str,
):
    if not SB_metadata_path.exists():
        raise FileNotFoundError(f"SB metadata path {SB_metadata_path} does not exist")

    metadata_df = pl.read_parquet(io.BytesIO(SB_metadata_path.read_bytes()))
    utility_metadata_df = metadata_df.filter(
        pl.col("sb.electric_utility") == electric_utility
    )
    if utility_metadata_df.is_empty():
        return

    metadata_has_hp = utility_metadata_df.select(
        pl.col("bldg_id", "postprocess_group.has_hp")
    )
    electrical_tariff_mapping_df = generate_electrical_tariff_mapping(
        metadata_has_hp, SB_scenario, electric_utility
    )

    print(electrical_tariff_mapping_df.head(20))

    output_path = (
        RATE_DESIGN_DIR
        / state.lower()
        / "hp_rates"
        / "data"
        / "tariff_map"
        / f"{electric_utility}_{SB_scenario}.csv"
    )
    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True)
    electrical_tariff_mapping_df.write_csv(output_path)

    return


def main():
    parser = argparse.ArgumentParser(description="Map electrical tariff.")
    parser.add_argument(
        "--metadata_path", required=True, help="Base path for resstock data"
    )
    parser.add_argument("--state", required=True, help="State code (e.g. RI)")
    parser.add_argument(
        "--electric_utility", required=True, help="Electric utility (e.g. Coned)"
    )
    parser.add_argument(
        "--SB_scenario_name", required=True, help="SB scenario name (e.g. default_1)"
    )
    parser.add_argument(
        "--SB_scenario_year", required=True, help="SB scenario year (e.g. 2024)"
    )
    args = parser.parse_args()

    #########################################################
    # For now, we will manually add the electric utility name column. Later on, the metadata parquet will be updated to include this column.
    # Assign first ~1/3 to Coned, next ~1/3 to National Grid, last ~1/3 to NYSEG.
    metadata_path = S3Path(args.metadata_path)
    metadata_df = pl.read_parquet(io.BytesIO(metadata_path.read_bytes()))
    n = len(metadata_df)
    metadata_df = (
        metadata_df.with_row_index("_row_idx")
        .with_columns(
            pl.when(pl.col("_row_idx") < n // 3)
            .then(pl.lit("Coned"))
            .when(pl.col("_row_idx") < 2 * n // 3)
            .then(pl.lit("National Grid"))
            .otherwise(pl.lit("NYSEG"))
            .alias("sb.electric_utility")
        )
        .drop("_row_idx")
    )
    temp_path = metadata_path.parent / f"{metadata_path.stem}_temp.parquet"
    buf = io.BytesIO()
    metadata_df.write_parquet(buf)
    temp_path.write_bytes(buf.getvalue())
    #########################################################

    if (
        args.SB_scenario_name != "default"
        and args.SB_scenario_name != "seasonal"
        and args.SB_scenario_name != "class_specific_seasonal"
    ):
        raise ValueError(f"Invalid SB scenario name: {args.SB_scenario_name}")

    sb_scenario = SB_scenario(args.SB_scenario_name, args.SB_scenario_year)
    map_electric_tariff(
        SB_metadata_path=temp_path,
        electric_utility=args.electric_utility,
        SB_scenario=sb_scenario,
        state=args.state,
    )

    #########################################################
    # Remove the temp file that contains electric utility name column.
    temp_path.unlink()
    #########################################################


if __name__ == "__main__":
    main()
