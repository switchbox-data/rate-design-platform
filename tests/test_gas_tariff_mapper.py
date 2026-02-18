"""Tests for gas tariff mapper."""

from typing import cast

import polars as pl

from utils.pre.gas_tariff_mapper import map_gas_tariff


def test_map_gas_tariff_uses_crosswalk_for_tariff_key():
    """map_gas_tariff maps sb.gas_utility (std_name) to tariff_key via crosswalk."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3],
            "sb.electric_utility": ["coned", "coned", "coned"],
            "sb.gas_utility": ["nimo", "nyseg", "coned"],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Single-Family Detached",
                "Multi-Family with 2 - 4 Units",
            ],
            "in.geometry_stories_low_rise": ["2", "2", "4+"],
            "heats_with_natgas": [True, True, True],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="coned",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 3
    # nimo -> national_grid, nyseg -> nyseg, coned -> coned
    tariff_keys = df["tariff_key"].to_list()
    assert "national_grid" in tariff_keys
    assert "nyseg_heating" in tariff_keys
    assert "coned_mf_highrise" in tariff_keys


def test_map_gas_tariff_coned_building_types():
    """Test coned mapping for different building types."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3],
            "sb.electric_utility": ["coned", "coned", "coned"],
            "sb.gas_utility": ["coned", "coned", "coned"],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Multi-Family with 5+ units",
                "Multi-Family with 2 - 4 Units",
            ],
            "in.geometry_stories_low_rise": ["2", "4+", "2"],
            "heats_with_natgas": [True, True, True],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="coned",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 3
    tariff_keys = df["tariff_key"].to_list()
    assert "coned_sf" in tariff_keys
    assert "coned_mf_highrise" in tariff_keys
    assert "coned_mf_lowrise" in tariff_keys


def test_map_gas_tariff_kedny_heating_conditions():
    """Test kedny mapping based on heating with natural gas."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3],
            "sb.electric_utility": ["coned", "coned", "coned"],
            "sb.gas_utility": ["kedny", "kedny", "kedny"],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Single-Family Attached",
                "Multi-Family with 5+ units",
            ],
            "in.geometry_stories_low_rise": ["2", "2", "4+"],
            "heats_with_natgas": [True, False, True],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="coned",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 3
    tariff_keys = df["tariff_key"].to_list()
    # kedny maps to national_grid
    assert "national_grid_sf_heating" in tariff_keys
    assert "national_grid_sf_nonheating" in tariff_keys
    assert "national_grid_mf" in tariff_keys


def test_map_gas_tariff_kedli_all_conditions():
    """Test kedli mapping for all building type and heating combinations."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "sb.electric_utility": ["coned", "coned", "coned", "coned"],
            "sb.gas_utility": ["kedli", "kedli", "kedli", "kedli"],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Single-Family Attached",
                "Multi-Family with 2 - 4 Units",
                "Multi-Family with 5+ units",
            ],
            "in.geometry_stories_low_rise": ["2", "2", "2", "4+"],
            "heats_with_natgas": [True, False, True, False],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="coned",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 4
    tariff_keys = df["tariff_key"].to_list()
    # kedli maps to national_grid
    assert "national_grid_sf_heating" in tariff_keys
    assert "national_grid_sf_nonheating" in tariff_keys
    assert "national_grid_mf_heating" in tariff_keys
    assert "national_grid_mf_nonheating" in tariff_keys


def test_map_gas_tariff_nyseg_heating_conditions():
    """Test nyseg mapping based on heating with natural gas."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2],
            "sb.electric_utility": ["nyseg", "nyseg"],
            "sb.gas_utility": ["nyseg", "nyseg"],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Multi-Family with 5+ units",
            ],
            "in.geometry_stories_low_rise": ["2", "4+"],
            "heats_with_natgas": [True, False],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="nyseg",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 2
    tariff_keys = df["tariff_key"].to_list()
    assert "nyseg_heating" in tariff_keys
    assert "nyseg_nonheating" in tariff_keys


def test_map_gas_tariff_simple_utilities_no_suffix():
    """Test utilities that map directly to gas_tariff_key without suffix."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5],
            "sb.electric_utility": ["coned", "coned", "coned", "coned", "coned"],
            "sb.gas_utility": ["nimo", "rge", "cenhud", "or", "nfg"],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Multi-Family with 2 - 4 Units",
                "Single-Family Attached",
                "Multi-Family with 5+ units",
                "Single-Family Detached",
            ],
            "in.geometry_stories_low_rise": ["2", "4+", "2", "4+", "2"],
            "heats_with_natgas": [True, False, True, True, False],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="coned",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 5
    tariff_keys = df["tariff_key"].to_list()
    # nimo -> national_grid, others stay the same
    assert "national_grid" in tariff_keys
    assert "rge" in tariff_keys
    assert "cenhud" in tariff_keys
    assert "or" in tariff_keys
    assert "nfg" in tariff_keys


def test_map_gas_tariff_rie_heating_conditions():
    """Test rie mapping based on heating with natural gas."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3],
            "sb.electric_utility": ["rie", "rie", "rie"],
            "sb.gas_utility": ["rie", "rie", None],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Single-Family Detached",
                "Multi-Family with 5+ units",
            ],
            "in.geometry_stories_low_rise": ["2", "2", "4+"],
            "heats_with_natgas": [True, False, False],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="rie",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 3
    tariff_keys = df["tariff_key"].to_list()
    # rie with heats_with_natgas=True -> rie_heating
    # rie with heats_with_natgas=False -> null_gas_tariff (default/otherwise case)
    assert "rie_heating" in tariff_keys
    assert "rie_nonheating" in tariff_keys
    assert "null_gas_tariff" in tariff_keys


def test_map_gas_tariff_null_gas_utility():
    """Test that null gas_utility values get assigned to null_gas_tariff."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2],
            "sb.electric_utility": ["coned", "coned"],
            "sb.gas_utility": [None, "coned"],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Single-Family Detached",
            ],
            "in.geometry_stories_low_rise": ["2", "2"],
            "heats_with_natgas": [True, True],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="coned",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 2
    tariff_keys = df["tariff_key"].to_list()
    assert "null_gas_tariff" in tariff_keys
    assert "coned_sf" in tariff_keys
