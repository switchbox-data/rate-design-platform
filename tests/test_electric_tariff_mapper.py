"""Tests for electric tariff mapper."""

from typing import cast

import polars as pl

from utils.pre.electric_tariff_mapper import map_electric_tariff


def test_map_electric_tariff_filters_by_std_name():
    """map_electric_tariff filters metadata by sb.electric_utility (std_name)."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "sb.electric_utility": ["coned", "coned", "nyseg", "nimo"],
            "postprocess_group.has_hp": [True, False, True, False],
        }
    )
    result = map_electric_tariff(
        SB_metadata_df=metadata,
        electric_utility="coned",
        grouping_cols=["default"],
        group_to_tariff_key={("default",): "default"},
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 2
    assert set(df["bldg_id"].to_list()) == {1, 2}
    assert all(df["tariff_key"].str.starts_with("coned_"))


def test_map_electric_tariff_tariff_key_format():
    """tariff_key uses std_name and mapping from grouping_cols."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1],
            "sb.electric_utility": ["nyseg"],
            "postprocess_group.has_hp": [True],
        }
    )
    result = map_electric_tariff(
        SB_metadata_df=metadata,
        electric_utility="nyseg",
        grouping_cols=["postprocess_group.has_hp"],
        group_to_tariff_key={(True,): "seasonal_1_HP.csv"},
    )
    df = cast(pl.DataFrame, result.collect())
    assert df["tariff_key"][0] == "nyseg_seasonal_1_HP.csv"


def test_map_electric_tariff_seasonal_discount_tariff_key_format():
    """Grouping by has_hp produces HP vs flat tariff keys."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2],
            "sb.electric_utility": ["nyseg", "nyseg"],
            "postprocess_group.has_hp": [True, False],
        }
    )
    result = map_electric_tariff(
        SB_metadata_df=metadata,
        electric_utility="nyseg",
        grouping_cols=["postprocess_group.has_hp"],
        group_to_tariff_key={
            (True,): "seasonal_discount_1_HP.csv",
            (False,): "seasonal_discount_1_flat.csv",
        },
    )
    df = cast(pl.DataFrame, result.collect()).sort("bldg_id")
    assert df["tariff_key"].to_list() == [
        "nyseg_seasonal_discount_1_HP.csv",
        "nyseg_seasonal_discount_1_flat.csv",
    ]
