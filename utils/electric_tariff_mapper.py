import io
from pathlib import Path
from typing import cast

import polars as pl
from cloudpathlib import S3Path

from utils.types import SB_scenario, electric_utility

SB_scenarios_path = Path(__file__).parent / "SB_scenarios.json"


def map_electric_tariff(
    SB_metadata_path: S3Path,
    electric_utility: electric_utility,
    SB_scenario_name: SB_scenario,
):
    if not SB_metadata_path.exists():
        raise FileNotFoundError(f"SB metadata path {SB_metadata_path} does not exist")

    metadata_df = pl.read_parquet(io.BytesIO(SB_metadata_path.read_bytes()))
    utility_metadata_df = metadata_df.filter(
        pl.col("sb.electric_utility") == electric_utility
    )
    if utility_metadata_df.is_empty():
        return

    print(utility_metadata_df.head())
    print(len(utility_metadata_df))

    return utility_metadata_df


if __name__ == "__main__":
    data_path = S3Path("s3://data.sb/nrel/resstock/")
    release_dir = data_path / "res_2024_amy2018_2"
    state = "NY"
    upgrade_id = "00"
    metadata_path = (
        release_dir
        / "metadata"
        / f"state={state}"
        / f"upgrade={upgrade_id}"
        / "metadata-sb.parquet"
    )

    #########################################################
    # For now, we will manually add the electric utility name column. Later on, the metadata parquet will be updated to include this column.
    # Assign first ~1/3 to Coned, next ~1/3 to National Grid, last ~1/3 to NYSEG.
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

    map_electric_tariff(
        SB_metadata_path=temp_path,
        electric_utility=cast(electric_utility, "Coned"),
        SB_scenario_name=cast(SB_scenario, "default_1"),
    )
    map_electric_tariff(
        SB_metadata_path=temp_path,
        electric_utility=cast(electric_utility, "National Grid"),
        SB_scenario_name=cast(SB_scenario, "default_1"),
    )
    map_electric_tariff(
        SB_metadata_path=temp_path,
        electric_utility=cast(electric_utility, "NYSEG"),
        SB_scenario_name=cast(SB_scenario, "default_1"),
    )

    #########################################################
    # Remove the temp file that contains electric utility name column.
    temp_path.unlink()
    #########################################################
