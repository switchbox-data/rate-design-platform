"""Tests for electric tariff mapper."""

from typing import cast

import polars as pl
import pytest

from utils.pre.electric_tariff_mapper import (
    generate_tariff_map_from_scenario_keys,
    map_electric_tariff,
)
from utils.types import SBScenario


def test_map_electric_tariff_filters_by_std_name():
    """map_electric_tariff filters metadata by sb.electric_utility (std_name)."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "sb.electric_utility": ["coned", "coned", "nyseg", "nimo"],
            "postprocess_group.has_hp": [True, False, True, False],
        }
    )
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
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1],
            "sb.electric_utility": ["nyseg"],
            "postprocess_group.has_hp": [True],
        }
    )
    sb_scenario = SBScenario("seasonal", 1)
    result = map_electric_tariff(
        SB_metadata_df=metadata,
        electric_utility="nyseg",
        SB_scenario=sb_scenario,
        state="NY",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df["tariff_key"][0] == "nyseg_seasonal_1_HP.csv"


def test_map_electric_tariff_seasonal_discount_tariff_key_format():
    """seasonal_discount uses HP/flat split keys for mapping."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2],
            "sb.electric_utility": ["nyseg", "nyseg"],
            "postprocess_group.has_hp": [True, False],
        }
    )
    sb_scenario = SBScenario("seasonal_discount", 1)
    result = map_electric_tariff(
        SB_metadata_df=metadata,
        electric_utility="nyseg",
        SB_scenario=sb_scenario,
        state="NY",
    )
    df = cast(pl.DataFrame, result.collect()).sort("bldg_id")
    assert df["tariff_key"].to_list() == [
        "nyseg_seasonal_discount_1_HP.csv",
        "nyseg_seasonal_discount_1_flat.csv",
    ]


# ---------------------------------------------------------------------------
# Tests for generate_tariff_map_from_scenario_keys
# ---------------------------------------------------------------------------


def _bldg_data_has_hp() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "postprocess_group.has_hp": ["true", "false", "true", "false"],
        }
    )


def _bldg_data_heating_type_v2() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5],
            "postprocess_group.heating_type_v2": [
                "heat_pump",
                "electrical_resistance",
                "natgas",
                "delivered_fuels",
                "other",
            ],
        }
    )


def test_generate_tariff_map_all_key():
    """{"all"} key assigns a single tariff stem to every building."""
    bldg_data = pl.DataFrame(
        {
            "bldg_id": [10, 20, 30],
            "postprocess_group.has_hp": [True, False, True],
        }
    )
    result = generate_tariff_map_from_scenario_keys(
        {"all": "tariffs/electric/util_default.json"},
        bldg_data,
    )
    assert result.columns == ["bldg_id", "tariff_key"]
    assert result["tariff_key"].to_list() == ["util_default"] * 3


def test_generate_tariff_map_two_way_subclass_config():
    """Two-subclass config (hp / non-hp via has_hp) assigns correct stems."""
    subclass_config = {
        "group_col": "has_hp",
        "selectors": {"hp": "true", "non-hp": "false"},
    }
    path_tariffs = {
        "hp": "tariffs/electric/util_hp.json",
        "non-hp": "tariffs/electric/util_nonhp.json",
    }
    result = generate_tariff_map_from_scenario_keys(
        path_tariffs, _bldg_data_has_hp(), subclass_config
    ).sort("bldg_id")

    assert result["tariff_key"].to_list() == [
        "util_hp",
        "util_nonhp",
        "util_hp",
        "util_nonhp",
    ]


def test_generate_tariff_map_two_way_multi_value_selectors():
    """Two-subclass config with comma-separated multi-value selectors collapses five
    heating_type_v2 values into two tariff keys."""
    subclass_config = {
        "group_col": "heating_type_v2",
        "selectors": {
            "electric_heating": "heat_pump,electrical_resistance",
            "non_electric_heating": "natgas,delivered_fuels,other",
        },
    }
    path_tariffs = {
        "electric_heating": "tariffs/electric/util_elec_heating.json",
        "non_electric_heating": "tariffs/electric/util_non_elec_heating.json",
    }
    result = generate_tariff_map_from_scenario_keys(
        path_tariffs, _bldg_data_heating_type_v2(), subclass_config
    ).sort("bldg_id")

    assert result["tariff_key"].to_list() == [
        "util_elec_heating",
        "util_elec_heating",
        "util_non_elec_heating",
        "util_non_elec_heating",
        "util_non_elec_heating",
    ]


def test_generate_tariff_map_three_way_subclass_config():
    """Three-subclass config correctly handles N-ary (N > 2) selectors."""
    bldg_data = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5, 6],
            "postprocess_group.rate_class": ["res", "res", "sml", "sml", "lrg", "lrg"],
        }
    )
    subclass_config = {
        "group_col": "rate_class",
        "selectors": {
            "residential": "res",
            "small_commercial": "sml",
            "large_commercial": "lrg",
        },
    }
    path_tariffs = {
        "residential": "tariffs/electric/util_res.json",
        "small_commercial": "tariffs/electric/util_sml.json",
        "large_commercial": "tariffs/electric/util_lrg.json",
    }
    result = generate_tariff_map_from_scenario_keys(
        path_tariffs, bldg_data, subclass_config
    ).sort("bldg_id")

    assert result["tariff_key"].to_list() == [
        "util_res",
        "util_res",
        "util_sml",
        "util_sml",
        "util_lrg",
        "util_lrg",
    ]


def test_generate_tariff_map_missing_subclass_config_raises():
    """Multi-key path_tariffs_electric without subclass_config raises ValueError."""
    path_tariffs = {
        "key_a": "tariffs/electric/util_a.json",
        "key_b": "tariffs/electric/util_b.json",
    }
    bldg_data = pl.DataFrame({"bldg_id": [1], "postprocess_group.has_hp": ["true"]})
    with pytest.raises(ValueError, match="subclass_config is required"):
        generate_tariff_map_from_scenario_keys(path_tariffs, bldg_data)


def test_generate_tariff_map_unmatched_value_raises():
    """A building whose group_col value is not covered by any selector raises ValueError."""
    bldg_data = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "postprocess_group.has_hp": ["true", "unknown_value"],
        }
    )
    subclass_config = {
        "group_col": "has_hp",
        "selectors": {"hp": "true", "non-hp": "false"},
    }
    path_tariffs = {
        "hp": "tariffs/electric/util_hp.json",
        "non-hp": "tariffs/electric/util_nonhp.json",
    }
    with pytest.raises(ValueError, match="did not match any selector"):
        generate_tariff_map_from_scenario_keys(path_tariffs, bldg_data, subclass_config)


def test_generate_tariff_map_selector_key_missing_from_tariffs_raises():
    """A tariff key with no matching entry in subclass_config.selectors raises ValueError."""
    bldg_data = pl.DataFrame({"bldg_id": [1], "postprocess_group.has_hp": ["true"]})
    subclass_config = {
        "group_col": "has_hp",
        "selectors": {"hp": "true"},  # missing "non-hp"
    }
    path_tariffs = {
        "hp": "tariffs/electric/util_hp.json",
        "non-hp": "tariffs/electric/util_nonhp.json",  # no matching selector
    }
    with pytest.raises(ValueError, match="have no matching entry"):
        generate_tariff_map_from_scenario_keys(path_tariffs, bldg_data, subclass_config)
