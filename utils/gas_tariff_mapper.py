import argparse
import io
from pathlib import Path

import polars as pl
from cloudpathlib import S3Path

from utils.types import SB_scenario, electric_utility, gas_utility

# Project root (rate-design-platform); independent of cwd or caller
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
RATE_DESIGN_DIR = _PROJECT_ROOT / "rate_design"


def map_gas_tariff(
    SB_metadata_path: S3Path,
    electric_utility_name: electric_utility,
    gas_utility_name: gas_utility,
    SB_scenario: SB_scenario,
    state: str,
):
    pass


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
        "--gas_utility", required=True, help="Gas utility (e.g. National Grid)"
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
    # Create electricity utility name column
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
    # Create gas utility name column
    metadata_df = (
        metadata_df.with_row_index("_row_idx")
        .with_columns(
            pl.when(pl.col("_row_idx") < n // 2)
            .then(pl.lit("National Grid"))
            .otherwise(pl.lit("NYSEG"))
            .alias("sb.gas_utility")
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
    map_gas_tariff(
        SB_metadata_path=temp_path,
        electric_utility_name=args.electric_utility,
        gas_utility_name=args.gas_utility,
        SB_scenario=sb_scenario,
        state=args.state,
    )

    #########################################################
    # Remove the temp file that contains electric utility name column.
    temp_path.unlink()
    #########################################################


if __name__ == "__main__":
    main()
