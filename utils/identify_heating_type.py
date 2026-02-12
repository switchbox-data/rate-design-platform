import argparse

import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region

IN_HVAC_COLUMN = "in.hvac_heating_type_and_fuel"

UPGRADE_HVAC_COLUMN = "upgrade.hvac_heating_efficiency"

STORAGE_OPTIONS = {"aws_region": get_aws_region()}

HP_SUBSTRINGS = ("MSHP", "ASHP", "GSHP")
FOSSIL_SUBSTRINGS = ("Fuel Oil", "Natural Gas", "Other Fuel", "Propane")


def _col_contains_any(column: str, substrings: tuple[str, ...]) -> pl.Expr:
    """True where the column value contains any of the given substrings (literal match)."""
    return pl.any_horizontal(
        [pl.col(column).str.contains(s, literal=True) for s in substrings]
    ).fill_null(False)


def get_upgrade_ids(data_path: str, release: str, state: str) -> list[str]:
    """Get the upgrade ids for the given state, release, and data path."""
    base = S3Path(data_path)
    release_dir = base / release
    if not release_dir.exists():
        raise FileNotFoundError(f"Release directory {release_dir} does not exist")

    metadata_dir = release_dir / "metadata" / f"state={state}"
    if not metadata_dir.exists():
        raise FileNotFoundError(f"Metadata directory {metadata_dir} does not exist")

    upgrade_ids = [
        path.name.split("=")[1]
        for path in metadata_dir.iterdir()
        if path.is_dir() and path.name.startswith("upgrade=")
    ]
    print(f"Upgrade ids: {upgrade_ids}")
    return upgrade_ids


def identify_heating_type(metadata_df: pl.LazyFrame, upgrade_id: str) -> pl.LazyFrame:
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
        heating_fuel_is_electric = pl.col(IN_HVAC_COLUMN).str.contains(
            "Electricity", literal=True
        )
        heating_type_is_electric_resistance = (
            heating_fuel_is_electric & ~heating_type_is_hp
        ).fill_null(False)
    else:
        upgrade_heating_fuel_is_electric = pl.col(UPGRADE_HVAC_COLUMN).str.contains(
            "Electricity", literal=True
        )
        heating_type_is_electric_resistance = (
            upgrade_heating_fuel_is_electric & ~upgrade_is_hp
        ).fill_null(False)

    # Identify fossil fuel heating (contains "Fuel Oil", "Natural Gas", "Other Fuel", or "Propane" in the column value)
    if upgrade_id == "00":
        heating_type_is_fossil = _col_contains_any(IN_HVAC_COLUMN, FOSSIL_SUBSTRINGS)
    else:
        heating_type_is_fossil = _col_contains_any(
            UPGRADE_HVAC_COLUMN, FOSSIL_SUBSTRINGS
        )

    heating_type = (
        pl.when(heating_type_is_hp)
        .then(pl.lit("heat_pump"))
        .when(heating_type_is_electric_resistance)
        .then(pl.lit("electrical_resistance"))
        .when(heating_type_is_fossil)
        .then(pl.lit("fossil_fuel"))
        .otherwise(
            pl.lit("none")
        )  # double checked that the rest are all "none" to begin with.
    )

    return metadata_df.with_columns(
        heating_type.alias("postprocess_group.heating_type")
    )


def add_heating_type_column_and_save_to_s3(
    metadata_path: S3Path, upgrade_id: str, output_path: S3Path
) -> None:
    metadata_df = pl.scan_parquet(str(metadata_path), storage_options=STORAGE_OPTIONS)
    sb_metadata_df = identify_heating_type(metadata_df, upgrade_id)
    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True)
    sb_metadata_df.sink_parquet(str(output_path), storage_options=STORAGE_OPTIONS)
    print(f"Added heating type column metadata written to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add heating type column to metadata.")
    parser.add_argument(
        "--data_path", required=True, help="Base path for resstock data"
    )
    parser.add_argument(
        "--output_path", required=True, help="Output path for modified metadata"
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
        help="Space separated list of upgrade ids (e.g. '00 01 02 03 04 05')",
    )
    args = parser.parse_args()
    upgrade_ids = args.upgrade_id.split(" ")

    for upgrade_id in upgrade_ids:
        metadata_path = (
            S3Path(args.data_path) / f"upgrade={upgrade_id}" / args.input_filename
        )
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")
        output_path = (
            S3Path(args.output_path) / f"upgrade={upgrade_id}" / args.output_filename
        )
        add_heating_type_column_and_save_to_s3(
            metadata_path=metadata_path, upgrade_id=upgrade_id, output_path=output_path
        )
