"""Tests for electric tariff mapper."""

from typing import cast

import polars as pl

from utils.electric_tariff_mapper import map_electric_tariff
from utils.types import SBScenario


def test_map_electric_tariff_filters_by_std_name():
    """map_electric_tariff filters metadata by sb.electric_utility (std_name)."""
    metadata = pl.LazyFrame({
        "bldg_id": [1, 2, 3, 4],
        "sb.electric_utility": ["coned", "coned", "nyseg", "nimo"],
        "postprocess_group.has_hp": [True, False, True, False],
    })
    sb_scenario = SBScenario("default", 1)
    result = map_electric_tariff(
        SB_metadata_df=metadata,
        electric_utility="coned",
        SB_scenario=sb_scenario,
        state="NY",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 2
    assert set(df["bldg_id"].to_list()) == {1, 2}
    assert all(df["tariff_key"].str.starts_with("coned_"))


def test_map_electric_tariff_tariff_key_format():
    """tariff_key uses std_name and scenario."""
    metadata = pl.LazyFrame({
        "bldg_id": [1],
        "sb.electric_utility": ["nyseg"],
        "postprocess_group.has_hp": [True],
    })
    sb_scenario = SBScenario("seasonal", 1)
    result = map_electric_tariff(
        SB_metadata_df=metadata,
        electric_utility="nyseg",
        SB_scenario=sb_scenario,
        state="NY",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df["tariff_key"][0] == "nyseg_seasonal_1_HP.csv"
