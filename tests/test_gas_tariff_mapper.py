"""Tests for gas tariff mapper."""

from pathlib import Path
from typing import cast

import polars as pl

from utils.pre.fetch_gas_tariffs_rateacuity import load_config
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
            "heats_with_natgas": [True, True, True],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="coned",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 3
    # nimo -> nimo, nyseg -> nyseg_heating, coned MF 2-4 + heating -> coned_sf_heating (SF = 1-4 units)
    tariff_keys = df["tariff_key"].to_list()
    assert "nimo" in tariff_keys
    assert "nyseg_heating" in tariff_keys
    assert "coned_sf_heating" in tariff_keys


def test_map_gas_tariff_coned_building_types():
    """Test coned mapping: non-heating, SF heating, MF heating."""
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
            "heats_with_natgas": [False, True, True],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="coned",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 3
    tariff_keys = df["tariff_key"].to_list()
    assert "coned_nonheating" in tariff_keys
    assert "coned_sf_heating" in tariff_keys
    assert "coned_mf_heating" in tariff_keys


def test_map_gas_tariff_kedny_heating_conditions():
    """Test kedny mapping: SF heating/non-heating; MF gets single rate."""
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
    assert "kedny_sf_heating" in tariff_keys
    assert "kedny_sf_nonheating" in tariff_keys
    assert "kedny_mf" in tariff_keys


def test_map_gas_tariff_kedli_all_conditions():
    """Test kedli mapping: SF heating/non-heating; MF (5+) gets single rate."""
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
    assert "kedli_sf_heating" in tariff_keys
    assert "kedli_sf_nonheating" in tariff_keys
    assert "kedli_mf" in tariff_keys


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
    # All use utility code (std_name)
    assert "nimo" in tariff_keys
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
    assert "coned_sf_heating" in tariff_keys


def test_map_gas_tariff_small_utilities_become_null():
    """Small utilities (bath, chautauqua, corning, fillmore, reserve, stlaw) map to null_gas_tariff."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3],
            "sb.electric_utility": ["nyseg", "nyseg", "nyseg"],
            "sb.gas_utility": ["bath", "corning", "nyseg"],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Multi-Family with 2 - 4 Units",
                "Single-Family Detached",
            ],
            "heats_with_natgas": [True, True, True],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="nyseg",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 3
    keys = df.sort("bldg_id")["tariff_key"].to_list()
    assert keys[0] == "null_gas_tariff"  # bath
    assert keys[1] == "null_gas_tariff"  # corning
    assert keys[2] == "nyseg_heating"  # nyseg


def test_map_gas_tariff_ny_keys_match_yaml() -> None:
    """Every tariff_key produced by the mapper for NY utilities exists in rateacuity_tariffs.yaml."""
    project_root = Path(__file__).resolve().parents[1]
    yaml_path = (
        project_root
        / "rate_design/ny/hp_rates/config/tariffs/gas/rateacuity_tariffs.yaml"
    )
    _state, utilities = load_config(yaml_path)
    valid_tariff_keys = {
        tk for _u, tariffs in utilities.items() for tk in tariffs.keys()
    }
    # null_gas_tariff is assigned by mapper for small utilities / null gas_utility; not in YAML
    valid_tariff_keys.add("null_gas_tariff")

    # Fixture that hits coned, kedli, kedny in all variants plus other NY gas utilities
    metadata = pl.LazyFrame(
        {
            "bldg_id": list(range(14)),
            "sb.electric_utility": ["coned"] * 7 + ["coned"] * 7,
            "sb.gas_utility": [
                "coned",
                "coned",
                "coned",
                "kedli",
                "kedli",
                "kedli",
                "kedny",
                "kedny",
                "kedny",
                "nimo",
                "nyseg",
                "nyseg",
                "cenhud",
                "rge",
            ],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Single-Family Detached",
                "Multi-Family with 5+ units",
                "Single-Family Detached",
                "Single-Family Detached",
                "Multi-Family with 5+ units",
                "Single-Family Detached",
                "Single-Family Detached",
                "Multi-Family with 5+ units",
                "Single-Family Detached",
                "Single-Family Detached",
                "Single-Family Detached",
                "Single-Family Detached",
                "Single-Family Detached",
            ],
            "heats_with_natgas": [
                False,
                True,
                True,
                True,
                False,
                True,
                True,
                False,
                True,
                True,
                True,
                False,
                True,
                False,
            ],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="coned",
    )
    df = cast(pl.DataFrame, result.collect())
    produced_keys = set(df["tariff_key"].to_list())
    missing = produced_keys - valid_tariff_keys
    assert not missing, (
        f"Mapper produced tariff_key(s) not in NY rateacuity_tariffs.yaml: {missing}"
    )
