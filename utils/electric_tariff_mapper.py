import io
from pathlib import Path
from typing import cast

import polars as pl
from cloudpathlib import S3Path

from utils.types import SB_scenario, electric_utility

# Project root (rate-design-platform); independent of cwd or caller
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
RATE_DESIGN_DIR = _PROJECT_ROOT / "rate_design"

SB_scenarios_path = Path(__file__).parent / "SB_scenarios.json"


def define_electrical_tariff_key(
    SB_scenario_name: SB_scenario,
    electric_utility: electric_utility,
    customer_class: str,
) -> str:
    tariff_key = ""
    match SB_scenario_name:
        # Default scenario
        case "default_1" | "default_2" | "default_3":
            tariff_key = "default"

        # Seasonal flat scenarios
        case "seasonal_1" | "seasonal_2" | "seasonal_3":
            if electric_utility == "Coned":
                if customer_class == "has_HP":
                    tariff_key = "Coned_seasonal_HP"
                elif customer_class == "no_HP":
                    tariff_key = "Coned_seasonal_default"
                else:
                    raise ValueError(f"Invalid customer class: {customer_class}")
            elif electric_utility == "National Grid":
                if customer_class == "has_HP":
                    tariff_key = "National_Grid_seasonal_HP"
                elif customer_class == "no_HP":
                    tariff_key = "National_Grid_seasonal_default"
                else:
                    raise ValueError(f"Invalid customer class: {customer_class}")
            elif electric_utility == "NYSEG":
                if customer_class == "has_HP":
                    tariff_key = "NYSEG_seasonal_HP"
                elif customer_class == "no_HP":
                    tariff_key = "NYSEG_seasonal_default"
                else:
                    raise ValueError(f"Invalid customer class: {customer_class}")
            else:
                raise ValueError(f"Invalid electric utility: {electric_utility}")

        # Class-specific seasonal flat scenarios
        case (
            "class_specific_seasonal_1"
            | "class_specific_seasonal_2"
            | "class_specific_seasonal_3"
        ):
            if electric_utility == "Coned":
                if customer_class == "has_HP":
                    tariff_key = "Coned_class_specific_seasonal_HP"
                elif customer_class == "no_HP":
                    tariff_key = "Coned_class_specific_seasonal_default"
                else:
                    raise ValueError(f"Invalid customer class: {customer_class}")
            elif electric_utility == "National Grid":
                if customer_class == "has_HP":
                    tariff_key = "National_Grid_class_specific_seasonal_HP"
                elif customer_class == "no_HP":
                    tariff_key = "National_Grid_class_specific_seasonal_default"
                else:
                    raise ValueError(f"Invalid customer class: {customer_class}")
            elif electric_utility == "NYSEG":
                if customer_class == "has_HP":
                    tariff_key = "NYSEG_class_specific_seasonal_HP"
                elif customer_class == "no_HP":
                    tariff_key = "NYSEG_class_specific_seasonal_default"
                else:
                    raise ValueError(f"Invalid customer class: {customer_class}")

        # Class-specific seasonal TOU scenarios
        case (
            "class_specific_seasonal_TOU_1"
            | "class_specific_seasonal_TOU_2"
            | "class_specific_seasonal_TOU_3"
        ):
            if electric_utility == "Coned":
                if customer_class == "has_HP":
                    tariff_key = "Coned_class_specific_seasonal_TOU_HP"
                elif customer_class == "no_HP":
                    tariff_key = "Coned_class_specific_seasonal_TOU_default"
                else:
                    raise ValueError(f"Invalid customer class: {customer_class}")
            elif electric_utility == "National Grid":
                if customer_class == "has_HP":
                    tariff_key = "National_Grid_class_specific_seasonal_TOU_HP"
                elif customer_class == "no_HP":
                    tariff_key = "National_Grid_class_specific_seasonal_TOU_default"
                else:
                    raise ValueError(f"Invalid customer class: {customer_class}")
            elif electric_utility == "NYSEG":
                if customer_class == "has_HP":
                    tariff_key = "NYSEG_class_specific_seasonal_TOU_HP"
                elif customer_class == "no_HP":
                    tariff_key = "NYSEG_class_specific_seasonal_TOU_default"
                else:
                    raise ValueError(f"Invalid customer class: {customer_class}")
            else:
                raise ValueError(f"Invalid electric utility: {electric_utility}")
        case _:
            raise ValueError(f"Invalid SB scenario name: {SB_scenario_name}")
    return tariff_key


def generate_electrical_tariff_mapping(
    metadata_df: pl.DataFrame,
    SB_scenario_name: SB_scenario,
    electric_utility: electric_utility,
) -> pl.DataFrame:
    electrical_tariff_mapping_df = metadata_df.select(pl.col("bldg_id")).with_columns(
        pl.lit("").alias("tariff_key")
    )
    has_hp = metadata_df["postprocess_group.has_hp"]
    for customer_class in ["has_HP", "no_HP"]:
        tariff_key = define_electrical_tariff_key(
            SB_scenario_name, electric_utility, customer_class
        )
        if customer_class == "has_HP":
            electrical_tariff_mapping_df = electrical_tariff_mapping_df.with_columns(
                pl.when(has_hp)  # Only change rows where has_hp is True
                .then(pl.lit(tariff_key))
                .otherwise(pl.col("tariff_key"))
                .alias("tariff_key")
            )
        elif customer_class == "no_HP":
            electrical_tariff_mapping_df = electrical_tariff_mapping_df.with_columns(
                pl.when(~has_hp)  # Only change rows where has_hp is False
                .then(pl.lit(tariff_key))
                .otherwise(pl.col("tariff_key"))
                .alias("tariff_key")
            )
    return electrical_tariff_mapping_df


def map_electric_tariff(
    SB_metadata_path: S3Path,
    electric_utility: electric_utility,
    SB_scenario_name: SB_scenario,
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

    print(utility_metadata_df.head(20))
    print(len(utility_metadata_df))

    electrical_tariff_mapping_df = generate_electrical_tariff_mapping(
        utility_metadata_df, SB_scenario_name, electric_utility
    )

    print(electrical_tariff_mapping_df.head(20))

    output_path = (
        RATE_DESIGN_DIR
        / state.lower()
        / "hp_rates"
        / "data"
        / "tariff_map"
        / f"{electric_utility}_{SB_scenario_name}.csv"
    )
    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True)
    electrical_tariff_mapping_df.write_csv(output_path)

    return


if __name__ == "__main__":
    data_path = S3Path("s3://data.sb/nrel/resstock/")
    release_dir = data_path / "res_2024_amy2018_2"
    state = "NY"
    upgrade_id = "04"
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
        state=state,
    )

    #########################################################
    # Remove the temp file that contains electric utility name column.
    temp_path.unlink()
    #########################################################
