import argparse

import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region

IN_HVAC_COLUMN = "in.hvac_heating_type_and_fuel"
UPGRADE_HVAC_COLUMN = "upgrade.hvac_heating_efficiency"
STORAGE_OPTIONS = {"aws_region": get_aws_region()}
HP_SUBSTRINGS = ("MSHP", "ASHP", "GSHP")
ELECTRICITY_SUBSTRING = ("Electricity",)
FOSSIL_SUBSTRINGS = ("Fuel Oil", "Natural Gas", "Other Fuel", "Propane")


def _col_contains_any(column: str, substrings: tuple[str, ...]) -> pl.Expr:
    """True where the column value contains any of the given substrings (literal match)."""
    return pl.any_horizontal(
        [pl.col(column).str.contains(s, literal=True) for s in substrings]
    ).fill_null(False)


def identify_heating_type(metadata: pl.LazyFrame, upgrade_id: str) -> pl.LazyFrame:
    """Add postprocess_group.heating_type: 'heat_pump', 'electrical_resistance', 'fossil_fuel', or 'none'."""
    # Identify heat pumps (has "ASHP", "MSHP", or "GSHP" in the column value)
    if upgrade_id == "00":
        heating_type_is_hp = _col_contains_any(IN_HVAC_COLUMN, HP_SUBSTRINGS)
    else:
        upgrade_is_hp = _col_contains_any(UPGRADE_HVAC_COLUMN, HP_SUBSTRINGS)
        in_is_hp = _col_contains_any(IN_HVAC_COLUMN, HP_SUBSTRINGS)
        heating_type_is_hp = (upgrade_is_hp | in_is_hp).fill_null(False)

    # Identify electrical resistance heating (contains "Electricity" but not HP)
    if upgrade_id == "00":
        heating_fuel_is_electric = _col_contains_any(
            IN_HVAC_COLUMN, ELECTRICITY_SUBSTRING
        )
        heating_type_is_electric_resistance = (
            heating_fuel_is_electric & ~heating_type_is_hp
        ).fill_null(False)
    else:
        # First check the upgrade.hvac_heating_efficiency column for electrical resistance (contains "Electricity" but not HP)
        upgrade_heating_fuel_is_electric = _col_contains_any(
            UPGRADE_HVAC_COLUMN, ELECTRICITY_SUBSTRING
        )
        upgrade_heating_type_is_electric_resistance = (
            upgrade_heating_fuel_is_electric & ~upgrade_is_hp
        ).fill_null(False)
        # Sometimes there is a null value in the upgrade.hvac_heating_efficiency column, indicating no additional upgrade to hvac heating.
        # In this case, check the in.hvac_heating_type_and_fuel column for electrical resistance.
        is_null_upgrade = pl.col(UPGRADE_HVAC_COLUMN).is_null()
        in_heating_fuel_is_electric = _col_contains_any(
            IN_HVAC_COLUMN, ELECTRICITY_SUBSTRING
        )
        upgrade_is_null_and_in_electric = (
            is_null_upgrade & in_heating_fuel_is_electric & ~in_is_hp
        ).fill_null(False)
        heating_type_is_electric_resistance = (
            upgrade_heating_type_is_electric_resistance
            | upgrade_is_null_and_in_electric
        ).fill_null(False)

    # Identify fossil fuel heating (contains "Fuel Oil", "Natural Gas", "Other Fuel", or "Propane" in the column value)
    if upgrade_id == "00":
        heating_type_is_fossil = _col_contains_any(IN_HVAC_COLUMN, FOSSIL_SUBSTRINGS)
    else:
        # First check the upgrade.hvac_heating_efficiency column for fossil fuel
        upgrade_heating_type_is_fossil = _col_contains_any(
            UPGRADE_HVAC_COLUMN, FOSSIL_SUBSTRINGS
        )
        # Sometimes there is a null value in the upgrade.hvac_heating_efficiency column, indicating no additional upgrade to hvac heating.
        # In this case, check the in.hvac_heating_type_and_fuel column for fossil fuel.
        is_null_upgrade = pl.col(UPGRADE_HVAC_COLUMN).is_null()
        in_heating_fuel_is_fossil = _col_contains_any(IN_HVAC_COLUMN, FOSSIL_SUBSTRINGS)
        upgrade_is_null_and_in_fossil = (
            is_null_upgrade & in_heating_fuel_is_fossil
        ).fill_null(False)
        heating_type_is_fossil = (
            upgrade_heating_type_is_fossil | upgrade_is_null_and_in_fossil
        ).fill_null(False)

    heating_type = (
        pl.when(heating_type_is_hp)
        .then(pl.lit("heat_pump"))
        .when(heating_type_is_electric_resistance)
        .then(pl.lit("electrical_resistance"))
        .when(heating_type_is_fossil)
        .then(pl.lit("fossil_fuel"))
        # the otherwise clause below shouldn't happen, but just in case.
        # This would indicate that both the `in.hvac_heating_type_and_fuel`
        # and `upgrade.hvac_heating_efficiency` columns are null.
        .otherwise(pl.lit(None))
    )

    return metadata.with_columns(heating_type.alias("postprocess_group.heating_type"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add heating type column to ResStock metadata."
    )
    parser.add_argument(
        "--input_dir",
        required=True,
        help="Full S3Path url pointing to the input metadata file directory",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Full S3Path url pointing to the output directory",
    )
    parser.add_argument(
        "--input_filename",
        required=True,
        help="Input filename (e.g. 'metadata.parquet')",
    )
    parser.add_argument(
        "--output_filename",
        required=True,
        help="Output filename (e.g. 'metadata-sb.parquet')",
    )
    parser.add_argument(
        "--upgrade_id",
        required=True,
        help="Upgrade id (e.g. '00')",
    )
    args = parser.parse_args()
    upgrade_id = args.upgrade_id

    # Read input metadata file
    metadata_path = S3Path(args.input_dir) / args.input_filename
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file {metadata_path} does not exist")
    input_metadata = pl.scan_parquet(
        str(metadata_path), storage_options=STORAGE_OPTIONS
    )

    # Add `heating_type` column
    output_metadata = identify_heating_type(
        metadata=input_metadata, upgrade_id=upgrade_id
    )

    # Write output metadata file
    output_path = S3Path(args.output_dir) / args.output_filename
    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True)
    output_metadata.sink_parquet(str(output_path), storage_options=STORAGE_OPTIONS)
    print(f"Added heating type column and wrote metadata to {output_path}")
