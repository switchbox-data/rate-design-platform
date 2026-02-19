"""Tests for electric tariff mapper."""

from typing import cast

import polars as pl
import pytest

from utils.pre.electric_tariff_mapper import (
    _parse_group_to_tariff_key,
    _parse_tuple_value,
    define_electrical_tariff_key,
    generate_electrical_tariff_mapping,
    map_electric_tariff,
)

# --- _parse_tuple_value ---


def test_parse_tuple_value_bool_strings():
    """Coerce 'True'/'False' strings to bool."""
    assert _parse_tuple_value("True") is True
    assert _parse_tuple_value("False") is False
    assert _parse_tuple_value("  True  ") is True


def test_parse_tuple_value_int_strings():
    """Coerce numeric strings to int."""
    assert _parse_tuple_value("1") == 1
    assert _parse_tuple_value("0") == 0
    assert _parse_tuple_value("42") == 42


def test_parse_tuple_value_str_passthrough():
    """Non-bool, non-int strings remain str."""
    assert _parse_tuple_value("default") == "default"
    assert _parse_tuple_value("flat") == "flat"
    assert _parse_tuple_value("HP") == "HP"


def test_parse_tuple_value_already_bool_int_passthrough():
    """Already bool or int are returned unchanged."""
    assert _parse_tuple_value(True) is True
    assert _parse_tuple_value(False) is False
    assert _parse_tuple_value(1) == 1


# --- _parse_group_to_tariff_key ---


def test_parse_group_to_tariff_key_valid():
    """Parse tuple keys and coerce tuple elements."""
    raw = {"('default',)": "flat", "('True', 'False')": "hp_not_sf"}
    result = _parse_group_to_tariff_key(raw)
    assert result == {("default",): "flat", (True, False): "hp_not_sf"}


def test_parse_group_to_tariff_key_coerces_int_in_tuple():
    """Tuple elements like '1' become int."""
    raw = {"('1',)": "one", "('0',)": "zero"}
    result = _parse_group_to_tariff_key(raw)
    assert result == {(1,): "one", (0,): "zero"}


def test_parse_group_to_tariff_key_invalid_key_raises():
    """Non-tuple key raises TypeError."""
    raw = {"[1, 2]": "list_key"}
    with pytest.raises(TypeError, match="Expected tuple key"):
        _parse_group_to_tariff_key(raw)


# --- define_electrical_tariff_key ---


def test_define_electrical_tariff_key_default():
    """Default grouping returns literal utility_tariff_key."""
    expr = define_electrical_tariff_key(
        "rie",
        ["default"],
        {("default",): "flat"},
    )
    df = pl.LazyFrame({"bldg_id": [1]}).select(expr.alias("tariff_key"))
    result = cast(pl.DataFrame, df.collect())
    assert result["tariff_key"][0] == "rie_flat"


def test_define_electrical_tariff_key_one_col_bool():
    """One boolean column maps True/False to different keys."""
    expr = define_electrical_tariff_key(
        "nyseg",
        ["has_hp"],
        {(True,): "HP", (False,): "flat"},
    )
    df = pl.LazyFrame({"bldg_id": [1, 2], "has_hp": [True, False]}).select(
        pl.col("bldg_id"), expr.alias("tariff_key")
    )
    result = cast(pl.DataFrame, df.collect()).sort("bldg_id")
    assert result["tariff_key"].to_list() == ["nyseg_HP", "nyseg_flat"]


def test_define_electrical_tariff_key_one_col_str():
    """One string column maps category to key."""
    expr = define_electrical_tariff_key(
        "coned",
        ["category"],
        {("A",): "tariff_a", ("B",): "tariff_b"},
    )
    df = pl.LazyFrame({"bldg_id": [1, 2], "category": ["A", "B"]}).select(
        pl.col("bldg_id"), expr.alias("tariff_key")
    )
    result = cast(pl.DataFrame, df.collect()).sort("bldg_id")
    assert result["tariff_key"].to_list() == ["coned_tariff_a", "coned_tariff_b"]


def test_define_electrical_tariff_key_one_col_int():
    """One integer column maps tier to key."""
    expr = define_electrical_tariff_key(
        "nimo",
        ["tier"],
        {(1,): "tier1", (2,): "tier2"},
    )
    df = pl.LazyFrame({"bldg_id": [1, 2], "tier": [1, 2]}).select(
        pl.col("bldg_id"), expr.alias("tariff_key")
    )
    result = cast(pl.DataFrame, df.collect()).sort("bldg_id")
    assert result["tariff_key"].to_list() == ["nimo_tier1", "nimo_tier2"]


def test_define_electrical_tariff_key_two_cols():
    """Two grouping columns combine for composite key."""
    expr = define_electrical_tariff_key(
        "rie",
        ["has_hp", "category"],
        {(True, "residential"): "hp_res", (False, "commercial"): "flat_com"},
    )
    df = pl.LazyFrame(
        {
            "bldg_id": [1, 2],
            "has_hp": [True, False],
            "category": ["residential", "commercial"],
        }
    ).select(pl.col("bldg_id"), expr.alias("tariff_key"))
    result = cast(pl.DataFrame, df.collect()).sort("bldg_id")
    assert result["tariff_key"].to_list() == ["rie_hp_res", "rie_flat_com"]


def test_define_electrical_tariff_key_unmatched_gets_null():
    """Row that matches no key gets null tariff_key."""
    expr = define_electrical_tariff_key(
        "coned",
        ["has_hp"],
        {(True,): "HP"},
    )
    df = pl.LazyFrame({"bldg_id": [1, 2], "has_hp": [True, False]}).select(
        pl.col("bldg_id"), expr.alias("tariff_key")
    )
    result = cast(pl.DataFrame, df.collect()).sort("bldg_id")
    assert result["tariff_key"][0] == "coned_HP"
    assert result["tariff_key"][1] is None


# --- generate_electrical_tariff_mapping ---


def test_generate_electrical_tariff_mapping_output_shape():
    """Output has bldg_id and tariff_key only."""
    metadata = pl.LazyFrame(
        {"bldg_id": [1, 2], "postprocess_group.has_hp": [True, False]}
    )
    result = generate_electrical_tariff_mapping(
        metadata,
        ["postprocess_group.has_hp"],
        {(True,): "HP", (False,): "flat"},
        "nyseg",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.columns == ["bldg_id", "tariff_key"]
    assert df.height == 2
    assert df["tariff_key"].to_list() == ["nyseg_HP", "nyseg_flat"]


# --- map_electric_tariff ---


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


def test_map_electric_tariff_no_rows_for_utility_raises():
    """Raises ValueError when no rows have the requested electric_utility."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2],
            "sb.electric_utility": ["nyseg", "nimo"],
            "postprocess_group.has_hp": [True, False],
        }
    )
    with pytest.raises(ValueError, match="No rows found for electric utility coned"):
        map_electric_tariff(
            SB_metadata_df=metadata,
            electric_utility="coned",
            grouping_cols=["postprocess_group.has_hp"],
            group_to_tariff_key={(True,): "flat"},
        ).collect()
