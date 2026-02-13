import argparse

import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region

IN_HVAC_COOLING_COLUMN = "in.hvac_cooling_type"
IN_HVAC_HEATING_COLUMN = "in.hvac_heating_type"
UPGRADE_HVAC_COOLING_COLUMN = "upgrade.hvac_cooling_efficiency"
UPGRADE_HVAC_HEATING_COLUMN = "upgrade.hvac_heating_efficiency"
STORAGE_OPTIONS = {"aws_region": get_aws_region()}
HP_SUBSTRINGS = ("Heat Pump", "MSHP", "ASHP", "GSHP")


def _col_contains_any(column: str, substrings: tuple[str, ...]) -> pl.Expr:
    """True where the column value contains any of the given substrings (literal match)."""
    return pl.any_horizontal(
        [pl.col(column).str.contains(s, literal=True) for s in substrings]
    ).fill_null(False)


def identify_hp_customers(metadata: pl.LazyFrame, upgrade_id: str) -> pl.LazyFrame:
    """Add postprocess_group.has_hp column: True where building has heat pump."""
    if upgrade_id == "00":
        hvac_cooling_is_hp = _col_contains_any(IN_HVAC_COOLING_COLUMN, HP_SUBSTRINGS)
        hvac_heating_is_hp = _col_contains_any(IN_HVAC_HEATING_COLUMN, HP_SUBSTRINGS)
        hp_customers = (hvac_cooling_is_hp & hvac_heating_is_hp).fill_null(False)
    else:
        in_hvac_cooling_is_hp = _col_contains_any(IN_HVAC_COOLING_COLUMN, HP_SUBSTRINGS)
        in_hvac_heating_is_hp = _col_contains_any(IN_HVAC_HEATING_COLUMN, HP_SUBSTRINGS)
        in_hvac_is_hp = (in_hvac_cooling_is_hp & in_hvac_heating_is_hp).fill_null(False)
        upgrade_hvac_cooling_is_hp = _col_contains_any(
            UPGRADE_HVAC_COOLING_COLUMN, HP_SUBSTRINGS
        )
        upgrade_hvac_heating_is_hp = _col_contains_any(
            UPGRADE_HVAC_HEATING_COLUMN, HP_SUBSTRINGS
        )
        upgrade_hvac_is_hp = (
            upgrade_hvac_cooling_is_hp & upgrade_hvac_heating_is_hp
        ).fill_null(False)
        hp_customers = (upgrade_hvac_is_hp | in_hvac_is_hp).fill_null(False)
    return metadata.with_columns(hp_customers.alias("postprocess_group.has_hp"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add has_HP column to ResStock metadata."
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
    # Add `has_hp` column
    output_metadata = identify_hp_customers(
        metadata=input_metadata, upgrade_id=upgrade_id
    )
    # Write output metadata file
    output_path = S3Path(args.output_dir) / args.output_filename
    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True)
    output_metadata.sink_parquet(str(output_path), storage_options=STORAGE_OPTIONS)
    print(f"Added has_hp column and wrote metadata to {output_path}")
