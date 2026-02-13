import io
import os
import random
import time
from typing import cast

import polars as pl
import pytest
from cloudpathlib import S3Path

from utils.identify_heating_type import (
    ELECTRICITY_SUBSTRING,
    FOSSIL_SUBSTRINGS,
    HP_SUBSTRINGS,
    IN_HVAC_COLUMN,
    UPGRADE_HVAC_COLUMN,
    _col_contains_any,
    identify_heating_type,
)

data_path = "s3://data.sb/nrel/resstock/"
release = "res_2024_amy2018_2"
states = ["RI", "NY"]
upgrade_ids = ["00", "01", "02", "03", "04", "05"]


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="Skipping in GitHub Actions CI - requires AWS credentials",
)
def test_identify_heating_type():
    for state in states:
        for upgrade_id in upgrade_ids:
            metadata_path = (
                S3Path(data_path)
                / release
                / "metadata"
                / f"state={state}"
                / f"upgrade={upgrade_id}"
                / "metadata-sb.parquet"
            )
            assert metadata_path.exists()

            metadata_df = pl.read_parquet(io.BytesIO(metadata_path.read_bytes()))
            assert IN_HVAC_COLUMN in metadata_df.columns

            all_bldg_ids = metadata_df["bldg_id"].unique().to_list()
            rng = random.Random(int(time.time_ns()))
            testing_bldg_ids = rng.sample(all_bldg_ids, min(20, len(all_bldg_ids)))
            testing_metadata_df = metadata_df.filter(
                pl.col("bldg_id").is_in(testing_bldg_ids)
            )

            # Compute expected heating_type manually
            if upgrade_id == "00":
                heating_type_is_hp = _col_contains_any(IN_HVAC_COLUMN, HP_SUBSTRINGS)
                heating_fuel_is_electric = _col_contains_any(
                    IN_HVAC_COLUMN, ELECTRICITY_SUBSTRING
                )
                heating_type_is_electric_resistance = (
                    heating_fuel_is_electric & ~heating_type_is_hp
                ).fill_null(False)
                heating_type_is_fossil = _col_contains_any(
                    IN_HVAC_COLUMN, FOSSIL_SUBSTRINGS
                )
            else:
                assert UPGRADE_HVAC_COLUMN in testing_metadata_df.columns
                upgrade_is_hp = _col_contains_any(UPGRADE_HVAC_COLUMN, HP_SUBSTRINGS)
                in_is_hp = _col_contains_any(IN_HVAC_COLUMN, HP_SUBSTRINGS)
                heating_type_is_hp = (upgrade_is_hp | in_is_hp).fill_null(False)

                upgrade_heating_fuel_is_electric = _col_contains_any(
                    UPGRADE_HVAC_COLUMN, ELECTRICITY_SUBSTRING
                )
                upgrade_heating_type_is_electric_resistance = (
                    upgrade_heating_fuel_is_electric & ~upgrade_is_hp
                ).fill_null(False)

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

                upgrade_heating_type_is_fossil = _col_contains_any(
                    UPGRADE_HVAC_COLUMN, FOSSIL_SUBSTRINGS
                )
                # Reuse is_null_upgrade from above
                in_heating_fuel_is_fossil = _col_contains_any(
                    IN_HVAC_COLUMN, FOSSIL_SUBSTRINGS
                )
                upgrade_is_null_and_in_fossil = (
                    is_null_upgrade & in_heating_fuel_is_fossil
                ).fill_null(False)
                heating_type_is_fossil = (
                    upgrade_heating_type_is_fossil | upgrade_is_null_and_in_fossil
                ).fill_null(False)

            expected_heating_type = (
                pl.when(heating_type_is_hp)
                .then(pl.lit("heat_pump"))
                .when(heating_type_is_electric_resistance)
                .then(pl.lit("electrical_resistance"))
                .when(heating_type_is_fossil)
                .then(pl.lit("fossil_fuel"))
                .otherwise(pl.lit(None))
            )

            # Call the function
            testing_metadata_lazy = testing_metadata_df.lazy()
            result_lazy = identify_heating_type(
                metadata=testing_metadata_lazy, upgrade_id=upgrade_id
            )
            result_df = cast(pl.DataFrame, result_lazy.collect())

            assert "postprocess_group.heating_type" in result_df.columns

            # Compare results
            expected_values = testing_metadata_df.with_columns(
                expected_heating_type.alias("expected_heating_type")
            )["expected_heating_type"]
            actual_values = result_df["postprocess_group.heating_type"]

            # Handle null comparison (Polars null == null is null, not True)
            comparison = (expected_values == actual_values) | (
                expected_values.is_null() & actual_values.is_null()
            )
            assert comparison.all(), (
                f"Mismatch for state={state}, upgrade_id={upgrade_id}. "
                f"Expected: {expected_values.to_list()}, "
                f"Actual: {actual_values.to_list()}"
            )
            print(f"Test passed for state: {state} and upgrade id: {upgrade_id}")

    print("All tests passed")


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="Skipping in GitHub Actions CI - requires AWS credentials",
)
def test_identify_heating_type_null_upgrade_column():
    """Test heating_type identification specifically for rows where upgrade.hvac_heating_efficiency is null."""
    for state in states:
        for upgrade_id in upgrade_ids:
            if upgrade_id == "00":
                continue  # Skip "00" as it doesn't have upgrade column

            metadata_path = (
                S3Path(data_path)
                / release
                / "metadata"
                / f"state={state}"
                / f"upgrade={upgrade_id}"
                / "metadata-sb.parquet"
            )
            assert metadata_path.exists()

            metadata_df = pl.read_parquet(io.BytesIO(metadata_path.read_bytes()))
            assert UPGRADE_HVAC_COLUMN in metadata_df.columns

            # Filter for rows where upgrade column is null
            null_upgrade_df = metadata_df.filter(pl.col(UPGRADE_HVAC_COLUMN).is_null())

            if len(null_upgrade_df) == 0:
                print(
                    f"No null upgrade rows for state={state}, upgrade_id={upgrade_id}, skipping"
                )
                continue

            # Sample up to 20 rows (or all if fewer)
            all_null_bldg_ids = null_upgrade_df["bldg_id"].unique().to_list()
            rng = random.Random(int(time.time_ns()))
            testing_bldg_ids = rng.sample(
                all_null_bldg_ids, min(20, len(all_null_bldg_ids))
            )
            testing_metadata_df = null_upgrade_df.filter(
                pl.col("bldg_id").is_in(testing_bldg_ids)
            )

            # Compute expected heating_type manually for null upgrade rows
            # When upgrade is null, we only check the in column
            in_is_hp = _col_contains_any(IN_HVAC_COLUMN, HP_SUBSTRINGS)
            heating_type_is_hp = in_is_hp.fill_null(False)

            in_heating_fuel_is_electric = _col_contains_any(
                IN_HVAC_COLUMN, ELECTRICITY_SUBSTRING
            )
            heating_type_is_electric_resistance = (
                in_heating_fuel_is_electric & ~in_is_hp
            ).fill_null(False)

            in_heating_fuel_is_fossil = _col_contains_any(
                IN_HVAC_COLUMN, FOSSIL_SUBSTRINGS
            )
            heating_type_is_fossil = in_heating_fuel_is_fossil.fill_null(False)

            expected_heating_type = (
                pl.when(heating_type_is_hp)
                .then(pl.lit("heat_pump"))
                .when(heating_type_is_electric_resistance)
                .then(pl.lit("electrical_resistance"))
                .when(heating_type_is_fossil)
                .then(pl.lit("fossil_fuel"))
                .otherwise(pl.lit(None))
            )

            # Call the function
            testing_metadata_lazy = testing_metadata_df.lazy()
            result_lazy = identify_heating_type(
                metadata=testing_metadata_lazy, upgrade_id=upgrade_id
            )
            result_df = cast(pl.DataFrame, result_lazy.collect())

            assert "postprocess_group.heating_type" in result_df.columns

            # Compare results
            expected_values = testing_metadata_df.with_columns(
                expected_heating_type.alias("expected_heating_type")
            )["expected_heating_type"]
            actual_values = result_df["postprocess_group.heating_type"]

            # Handle null comparison (Polars null == null is null, not True)
            comparison = (expected_values == actual_values) | (
                expected_values.is_null() & actual_values.is_null()
            )
            assert comparison.all(), (
                f"Mismatch for null upgrade rows, state={state}, upgrade_id={upgrade_id}. "
                f"Expected: {expected_values.to_list()}, "
                f"Actual: {actual_values.to_list()}"
            )
            print(
                f"Test passed for null upgrade rows, state: {state}, upgrade id: {upgrade_id} "
                f"({len(testing_metadata_df)} rows)"
            )

    print("All null upgrade column tests passed")
