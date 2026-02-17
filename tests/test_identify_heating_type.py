from typing import cast

import polars as pl

from data.resstock.identify_heating_type import (
    IN_HVAC_COLUMN,
    UPGRADE_HVAC_COLUMN,
    identify_heating_type,
)

upgrade_ids = ["00", "01", "02", "03", "04", "05"]


def test_identify_heating_type():
    for upgrade_id in upgrade_ids:
        # Create test data with 10 rows covering different heating types
        test_metadata = {
            "bldg_id": list(range(1, 11)),
            IN_HVAC_COLUMN: [
                "MSHP Heat Pump",  # HP
                "ASHP Heat Pump",  # HP
                "Electricity Resistance",  # Electrical resistance
                "Electricity Baseboard",  # Electrical resistance
                "Natural Gas Furnace",  # Fossil fuel
                "Fuel Oil Fuel Boiler",  # Fossil fuel
                "Propane Heater",  # Fossil fuel
                "GSHP Heat Pump",  # HP
                "Electricity Baseboard",  # Electrical resistance
                "Fuel Oil Fuel Boiler",  # Fossil fuel
            ],
        }

        # Add UPGRADE_HVAC_COLUMN only if upgrade_id != "00"
        if upgrade_id != "00":
            test_metadata[UPGRADE_HVAC_COLUMN] = [
                "ASHP, SEER 16, 9.2 HSPF",  # HP
                None,  # Null - should fallback to IN column
                "ASHP, SEER 16, 9.2 HSPF",  # HP
                None,  # Null - should fallback to IN column
                "MSHP, SEER 16, 9.2 HSPF, Max Load",  # HP
                None,  # Null - should fallback to IN column
                "MSHP, SEER 16, 9.2 HSPF, Max Load",  # HP
                None,  # Null - should fallback to IN column
                "MSHP, SEER 16, 9.2 HSPF, Max Load",  # HP
                "MSHP, SEER 16, 9.2 HSPF, Max Load",  # HP
            ]

        test_input_metadata = pl.DataFrame(test_metadata).lazy()
        test_output_metadata = identify_heating_type(test_input_metadata, upgrade_id)
        test_output_metadata_df = cast(pl.DataFrame, test_output_metadata.collect())
        assert "postprocess_group.heating_type" in test_output_metadata_df.columns
        if upgrade_id == "00":
            assert test_output_metadata_df[
                "postprocess_group.heating_type"
            ].to_list() == [
                "heat_pump",
                "heat_pump",
                "electrical_resistance",
                "electrical_resistance",
                "fossil_fuel",
                "fossil_fuel",
                "fossil_fuel",
                "heat_pump",
                "electrical_resistance",
                "fossil_fuel",
            ]
        else:
            assert test_output_metadata_df[
                "postprocess_group.heating_type"
            ].to_list() == [
                "heat_pump",
                "heat_pump",
                "heat_pump",
                "electrical_resistance",
                "heat_pump",
                "fossil_fuel",
                "heat_pump",
                "heat_pump",
                "heat_pump",
                "heat_pump",
            ]


def test_heating_fuel_boolean_columns_upgrade_00():
    """Boolean fuel columns for baseline buildings (upgrade 00)."""
    test_metadata = {
        "bldg_id": list(range(1, 8)),
        IN_HVAC_COLUMN: [
            "MSHP Heat Pump",  # HP → electricity
            "Electricity Resistance",  # ER → electricity
            "Natural Gas Furnace",  # natgas
            "Fuel Oil Fuel Boiler",  # oil
            "Propane Heater",  # propane
            "ASHP Heat Pump",  # HP → electricity
            "Electricity Baseboard",  # ER → electricity
        ],
    }
    df = cast(
        pl.DataFrame,
        identify_heating_type(pl.DataFrame(test_metadata).lazy(), "00").collect(),
    )
    assert df["heats_with_electricity"].to_list() == [
        True,
        True,
        False,
        False,
        False,
        True,
        True,
    ]
    assert df["heats_with_natgas"].to_list() == [
        False,
        False,
        True,
        False,
        False,
        False,
        False,
    ]
    assert df["heats_with_oil"].to_list() == [
        False,
        False,
        False,
        True,
        False,
        False,
        False,
    ]
    assert df["heats_with_propane"].to_list() == [
        False,
        False,
        False,
        False,
        True,
        False,
        False,
    ]


def test_heating_fuel_boolean_columns_upgrade():
    """Boolean fuel columns for upgraded buildings — upgrade overrides in column."""
    test_metadata = {
        "bldg_id": [1, 2, 3, 4],
        IN_HVAC_COLUMN: [
            "Natural Gas Furnace",  # was gas
            "Fuel Oil Fuel Boiler",  # was oil
            "Propane Heater",  # was propane
            "Electricity Baseboard",  # was electric resistance
        ],
        UPGRADE_HVAC_COLUMN: [
            "ASHP, SEER 16, 9.2 HSPF",  # upgraded to HP → electricity
            None,  # null → fall back to in column → oil
            "MSHP, SEER 16, 9.2 HSPF",  # upgraded to HP → electricity
            None,  # null → fall back → electricity
        ],
    }
    df = cast(
        pl.DataFrame,
        identify_heating_type(pl.DataFrame(test_metadata).lazy(), "01").collect(),
    )
    # bldg 1: upgraded to HP → electricity only
    assert df["heats_with_electricity"].to_list() == [True, False, True, True]
    # bldg 2: null upgrade, falls back to oil
    assert df["heats_with_natgas"].to_list() == [False, False, False, False]
    assert df["heats_with_oil"].to_list() == [False, True, False, False]
    assert df["heats_with_propane"].to_list() == [False, False, False, False]
