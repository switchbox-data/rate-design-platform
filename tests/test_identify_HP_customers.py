import io
import random
import time

import polars as pl
from cloudpathlib import S3Path

IN_HVAC_COLUMNS = ["in.hvac_cooling_type", "in.hvac_heating_type"]

UPGRADE_HVAC_COLUMNS = [
    "upgrade.hvac_cooling_efficiency",
    "upgrade.hvac_heating_efficiency",
]

data_path = "s3://data.sb/nrel/resstock/"
release = "res_2024_amy2018_2"
states = ["RI", "NY"]
upgrade_ids = ["00", "01", "02", "03", "04", "05"]


def test_identify_HP_customers():
    for state in states:
        for upgrade_id in upgrade_ids:
            SB_metadata_path = (
                S3Path(data_path)
                / release
                / "metadata"
                / f"state={state}"
                / f"upgrade={upgrade_id}"
                / "metadata-sb.parquet"
            )
            assert SB_metadata_path.exists()

            SB_metadata_df = pl.read_parquet(io.BytesIO(SB_metadata_path.read_bytes()))
            assert "postprocess_group.has_hp" in SB_metadata_df.columns

            all_bldg_ids = SB_metadata_df["bldg_id"].unique().to_list()
            rng = random.Random(int(time.time_ns()))
            testing_bldg_ids = rng.sample(all_bldg_ids, min(20, len(all_bldg_ids)))
            testing_SB_metadata_df = SB_metadata_df.filter(
                pl.col("bldg_id").is_in(testing_bldg_ids)
            )

            if upgrade_id == "00":
                hvac_cooling_type = testing_SB_metadata_df[IN_HVAC_COLUMNS[0]]
                hvac_heating_type = testing_SB_metadata_df[IN_HVAC_COLUMNS[1]]
                hp_customers = (
                    hvac_cooling_type.str.contains("Heat Pump", literal=True)
                    & hvac_heating_type.str.contains("Heat Pump", literal=True)
                ).fill_null(False)
            else:
                in_hvac_cooling_type = testing_SB_metadata_df[IN_HVAC_COLUMNS[0]]
                in_hvac_heating_type = testing_SB_metadata_df[IN_HVAC_COLUMNS[1]]
                upgrade_hvac_cooling_type = testing_SB_metadata_df[
                    UPGRADE_HVAC_COLUMNS[0]
                ]
                upgrade_hvac_heating_type = testing_SB_metadata_df[
                    UPGRADE_HVAC_COLUMNS[1]
                ]
                # Row-wise OR: upgrade path is HP *or* in.path is HP
                upgrade_is_hp = upgrade_hvac_cooling_type.str.contains(
                    "Heat Pump", literal=True
                ) & (
                    upgrade_hvac_heating_type.str.contains("MSHP", literal=True)
                    | upgrade_hvac_heating_type.str.contains("ASHP", literal=True)
                    | upgrade_hvac_heating_type.str.contains("GSHP", literal=True)
                )
                in_is_hp = in_hvac_cooling_type.str.contains(
                    "Heat Pump", literal=True
                ) & (in_hvac_heating_type.str.contains("Heat Pump", literal=True))
                hp_customers = (upgrade_is_hp | in_is_hp).fill_null(False)

            expected = testing_SB_metadata_df["postprocess_group.has_hp"]
            assert (hp_customers == expected).all()
            print(f"Test passed for state: {state} and upgrade id: {upgrade_id}")

    print("All tests passed")


if __name__ == "__main__":
    test_identify_HP_customers()
