from typing import cast

import polars as pl

from utils.identify_hp_customers import (
    IN_HVAC_COOLING_COLUMN,
    IN_HVAC_HEATING_COLUMN,
    UPGRADE_HVAC_COOLING_COLUMN,
    UPGRADE_HVAC_HEATING_COLUMN,
    identify_hp_customers,
)

upgrade_ids = ["00", "01", "02", "03", "04", "05"]


def test_identify_HP_customers():
    for upgrade_id in upgrade_ids:
        # Create test data with 10 rows covering different heating types
        test_metadata = {
            "bldg_id": list(range(1, 11)),
            IN_HVAC_COOLING_COLUMN: [
                "Ducted Heat Pump",  # HP
                "Ducted Heat Pump",  # HP
                "Room AC",  # Not HP
                "Room AC",  # Not HP
                "Ducted Heat Pump",  # HP
                "Ducted Heat Pump",  # HP
                "Non-Ducted Heat Pump",  # HP
                "Non-Ducted Heat Pump",  # HP
                "Room AC",  # Not HP
                None,  # Not HP
            ],
            IN_HVAC_HEATING_COLUMN: [
                "Ducted Heat Pump",  # HP
                "Ducted Heat Pump",  # HP
                "Non-Ducted Heating",  # Not HP
                "Non-Ducted Heating",  # Not HP
                "Ducted Heat Pump",  # HP
                "Ducted Heat Pump",  # HP
                "Non-Ducted Heat Pump",  # HP
                "Non-Ducted Heat Pump",  # HP
                "Non-Ducted Heating",  # Not HP
                "Non-Ducted Heating",  # Not HP
            ],
        }

        # Add UPGRADE_HVAC_COLUMN only if upgrade_id != "00"
        if upgrade_id != "00":
            test_metadata[UPGRADE_HVAC_COOLING_COLUMN] = [
                "Ducted Heat Pump",  # HP
                "Ducted Heat Pump",  # HP
                "Ducted Heat Pump",  # HP
                "Non-Ducted Heat Pump",  # HP
                "Non-Ducted Heat Pump",  # HP
                None,  # Null - should fallback to IN column
                "Non-Ducted Heat Pump",  # HP
                None,  # Null - should fallback to IN column
                "Non-Ducted Heat Pump",  # HP
                "Non-Ducted Heat Pump",  # HP
            ]
            test_metadata[UPGRADE_HVAC_HEATING_COLUMN] = [
                "ASHP, SEER 16, 9.2 HSPF",  # HP
                "ASHP, SEER 16, 9.2 HSPF",  # HP
                "ASHP, SEER 16, 9.2 HSPF",  # HP
                "MSHP, SEER 16, 9.2 HSPF, Max Load",  # HP
                "MSHP, SEER 16, 9.2 HSPF, Max Load",  # HP
                None,  # Null - should fallback to IN column
                "MSHP, SEER 16, 9.2 HSPF, Max Load",  # HP
                None,  # Null - should fallback to IN column
                "MSHP, SEER 16, 9.2 HSPF, Max Load",  # HP
                "MSHP, SEER 16, 9.2 HSPF, Max Load",  # HP
            ]

        test_input_metadata = pl.DataFrame(test_metadata).lazy()
        test_output_metadata = identify_hp_customers(test_input_metadata, upgrade_id)
        test_output_metadata_df = cast(pl.DataFrame, test_output_metadata.collect())
        assert "postprocess_group.has_hp" in test_output_metadata_df.columns
        if upgrade_id == "00":
            assert test_output_metadata_df["postprocess_group.has_hp"].to_list() == [
                True,
                True,
                False,
                False,
                True,
                True,
                True,
                True,
                False,
                False,
            ]
        else:
            assert test_output_metadata_df["postprocess_group.has_hp"].to_list() == [
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
            ]
