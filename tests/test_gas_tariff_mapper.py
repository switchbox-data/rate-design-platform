"""Tests for gas tariff mapper."""

from collections.abc import Sequence
from pathlib import Path
from typing import cast

import polars as pl
import pytest

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
        / "rate_design/hp_rates/ny/config/tariffs/gas/rateacuity_tariffs.yaml"
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


def test_map_gas_tariff_md_single_key_utilities():
    """MD utilities with one residential JSON map to that stem for all buildings."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3],
            "sb.electric_utility": ["bge", "bge", "bge"],
            "sb.gas_utility": ["bge", "columbia_gas_md", "ugi_central_penn"],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Multi-Family with 5+ units",
                "Single-Family Attached",
            ],
            "heats_with_natgas": [True, False, True],
        }
    )
    result = map_gas_tariff(SB_metadata=metadata, electric_utility_name="bge")
    df = cast(pl.DataFrame, result.collect()).sort("bldg_id")
    assert df["tariff_key"].to_list() == [
        "bge_residential",
        "columbia_gas_md_residential",
        "ugi_central_penn_residential",
    ]


def test_map_gas_tariff_md_washington_gas_heating():
    """Washington Gas splits heating vs non-heating on heats_with_natgas."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3],
            "sb.electric_utility": ["pepco", "pepco", "pepco"],
            "sb.gas_utility": ["washington_gas", "washington_gas", None],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Multi-Family with 5+ units",
                "Single-Family Detached",
            ],
            "heats_with_natgas": [True, False, False],
        }
    )
    result = map_gas_tariff(SB_metadata=metadata, electric_utility_name="pepco")
    df = cast(pl.DataFrame, result.collect()).sort("bldg_id")
    assert df["tariff_key"].to_list() == [
        "washington_gas_heating",
        "washington_gas_nonheating",
        "null_gas_tariff",
    ]


def test_map_gas_tariff_md_easton_muni_residential():
    """MD easton_muni maps to easton_muni_residential (combined dist + commodity)."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2],
            "sb.electric_utility": ["bge", "bge"],
            "sb.gas_utility": ["easton_muni", "bge"],
            "in.geometry_building_type_recs": [
                "Single-Family Detached",
                "Single-Family Detached",
            ],
            "heats_with_natgas": [True, True],
        }
    )
    result = map_gas_tariff(SB_metadata=metadata, electric_utility_name="bge")
    df = cast(pl.DataFrame, result.collect()).sort("bldg_id")
    assert df["tariff_key"].to_list() == [
        "easton_muni_residential",
        "bge_residential",
    ]


def test_map_gas_tariff_md_chesapeake_res1_res2():
    """Chesapeake territory: county prefix from gas util, RES-1/2 from annual therms."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5, 6],
            "sb.electric_utility": ["choptank"] * 6,
            "sb.gas_utility": [
                "chesapeake_utilities",
                "chesapeake_utilities",
                "elkton_gas",
                "elkton_gas",
                "sandpiper",
                "sandpiper",
            ],
            "in.geometry_building_type_recs": ["Single-Family Detached"] * 6,
            "heats_with_natgas": [True] * 6,
        }
    )
    # 150 therms is still RES-1; >150 is RES-2.
    annual = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5, 6],
            "annual_gas_therms": [150.0, 150.1, 50.0, 400.0, 149.9, 151.0],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="choptank",
        annual_gas_therms=annual,
    )
    df = cast(pl.DataFrame, result.collect()).sort("bldg_id")
    assert df["tariff_key"].to_list() == [
        "chesapeake_main_res1",
        "chesapeake_main_res2",
        "chesapeake_cecil_res1",
        "chesapeake_cecil_res2",
        "chesapeake_worcester_res1",
        "chesapeake_worcester_res2",
    ]


def test_map_gas_tariff_md_chesapeake_requires_annual_therms():
    """Chesapeake buildings without annual_gas_therms raise."""
    metadata = pl.LazyFrame(
        {
            "bldg_id": [1],
            "sb.electric_utility": ["choptank"],
            "sb.gas_utility": ["sandpiper"],
            "in.geometry_building_type_recs": ["Single-Family Detached"],
            "heats_with_natgas": [True],
        }
    )
    with pytest.raises(ValueError, match="annual_gas_therms"):
        map_gas_tariff(SB_metadata=metadata, electric_utility_name="choptank")


def test_load_annual_gas_therms_converts_kwh(tmp_path: Path):
    """load_annual_gas_therms converts ResStock kWh to therms."""
    from utils.pre.gas_tariff_mapper import KWH_PER_THERM, load_annual_gas_therms

    path = tmp_path / "annual.parquet"
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "out.natural_gas.total.energy_consumption.kwh": [
                150.0 * KWH_PER_THERM,
                300.0 * KWH_PER_THERM,
            ],
        }
    ).write_parquet(path)
    df = cast(pl.DataFrame, load_annual_gas_therms(path).collect())
    assert abs(df.filter(pl.col("bldg_id") == 1)["annual_gas_therms"][0] - 150.0) < 1e-9
    assert abs(df.filter(pl.col("bldg_id") == 2)["annual_gas_therms"][0] - 300.0) < 1e-9


def _md_metadata(
    bldg_ids: list[int],
    gas_utilities: Sequence[str | None],
    heats_with_natgas: list[bool],
    electric_utility: str = "bge",
) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "bldg_id": bldg_ids,
            "sb.electric_utility": [electric_utility] * len(bldg_ids),
            "sb.gas_utility": gas_utilities,
            "in.geometry_building_type_recs": ["Single-Family Detached"]
            * len(bldg_ids),
            "heats_with_natgas": heats_with_natgas,
        }
    )


def test_map_gas_tariff_md_chesapeake_res1_res2_boundary():
    """Chesapeake RES-1/RES-2 boundary: exactly 150 therms is RES-1, not RES-2."""
    metadata = _md_metadata(
        [1, 2, 3, 4],
        ["chesapeake_utilities"] * 4,
        [True] * 4,
        electric_utility="choptank",
    )
    annual = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "annual_gas_therms": [0.0, 149.999, 150.0, 150.001],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="choptank",
        annual_gas_therms=annual,
    )
    df = cast(pl.DataFrame, result.collect()).sort("bldg_id")
    assert df["tariff_key"].to_list() == [
        "chesapeake_main_res1",
        "chesapeake_main_res1",
        "chesapeake_main_res1",
        "chesapeake_main_res2",
    ]


def test_map_gas_tariff_md_chesapeake_missing_therms_for_one_building():
    """Chesapeake building missing from the annual_gas_therms table raises, even if others are covered."""
    metadata = _md_metadata(
        [1, 2],
        ["sandpiper", "sandpiper"],
        [True, True],
        electric_utility="choptank",
    )
    # bldg_id 2 has no matching row in annual_gas_therms.
    annual = pl.LazyFrame({"bldg_id": [1], "annual_gas_therms": [100.0]})
    with pytest.raises(ValueError, match="annual_gas_therms"):
        map_gas_tariff(
            SB_metadata=metadata,
            electric_utility_name="choptank",
            annual_gas_therms=annual,
        ).collect()


def test_map_gas_tariff_md_non_chesapeake_buildings_do_not_need_therms():
    """Non-Chesapeake MD buildings map correctly even when annual_gas_therms is omitted,
    as long as no Chesapeake-territory gas utility appears in the batch."""
    metadata = _md_metadata(
        [1, 2, 3, 4],
        ["bge", "columbia_gas_md", "ugi_central_penn", None],
        [True, True, True, False],
    )
    result = map_gas_tariff(SB_metadata=metadata, electric_utility_name="bge")
    df = cast(pl.DataFrame, result.collect()).sort("bldg_id")
    assert df["tariff_key"].to_list() == [
        "bge_residential",
        "columbia_gas_md_residential",
        "ugi_central_penn_residential",
        "null_gas_tariff",
    ]


def test_map_gas_tariff_md_mixed_chesapeake_and_non_chesapeake():
    """A batch with both Chesapeake and non-Chesapeake MD utilities: only Chesapeake
    buildings need annual_gas_therms; others map independently of it."""
    metadata = _md_metadata(
        [1, 2, 3],
        ["bge", "sandpiper", "washington_gas"],
        [True, True, False],
        electric_utility="choptank",
    )
    # Only bldg_id 2 (sandpiper/Chesapeake) needs a therms row.
    annual = pl.LazyFrame({"bldg_id": [2], "annual_gas_therms": [50.0]})
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="choptank",
        annual_gas_therms=annual,
    )
    df = cast(pl.DataFrame, result.collect()).sort("bldg_id")
    assert df["tariff_key"].to_list() == [
        "bge_residential",
        "chesapeake_worcester_res1",
        "washington_gas_nonheating",
    ]


def test_map_gas_tariff_md_all_gas_utilities_no_unexpected_warning(caplog):
    """No 'unexpected gas_utility' warning is logged for any MD gas utility."""
    md_gas_utilities = [
        "bge",
        "columbia_gas_md",
        "ugi_central_penn",
        "washington_gas",
        "chesapeake_utilities",
        "elkton_gas",
        "sandpiper",
        "easton_muni",
        None,
    ]
    metadata = _md_metadata(
        list(range(len(md_gas_utilities))),
        md_gas_utilities,
        [True] * len(md_gas_utilities),
    )
    annual = pl.LazyFrame(
        {
            "bldg_id": list(range(len(md_gas_utilities))),
            "annual_gas_therms": [50.0] * len(md_gas_utilities),
        }
    )
    with caplog.at_level("WARNING"):
        map_gas_tariff(
            SB_metadata=metadata,
            electric_utility_name="bge",
            annual_gas_therms=annual,
        ).collect()
    assert "unexpected gas_utility" not in caplog.text


def test_map_gas_tariff_md_keys_match_tariff_json_files() -> None:
    """Every non-null tariff_key the mapper can produce for MD has a matching JSON
    file under rate_design/hp_rates/md/config/tariffs/gas/."""
    project_root = Path(__file__).resolve().parents[1]
    tariffs_dir = project_root / "rate_design/hp_rates/md/config/tariffs/gas"
    available_stems = {p.stem for p in tariffs_dir.glob("*.json")}

    # Every MD gas utility, exercised through both heating states and both
    # sides of the Chesapeake RES-1/RES-2 boundary.
    md_gas_utilities = [
        "bge",
        "columbia_gas_md",
        "ugi_central_penn",
        "easton_muni",
        "washington_gas",
        "washington_gas",
        "chesapeake_utilities",
        "chesapeake_utilities",
        "elkton_gas",
        "elkton_gas",
        "sandpiper",
        "sandpiper",
    ]
    heats_with_natgas = [
        True,
        True,
        True,
        True,
        True,
        False,
        True,
        True,
        True,
        True,
        True,
        True,
    ]
    bldg_ids = list(range(len(md_gas_utilities)))
    metadata = _md_metadata(bldg_ids, md_gas_utilities, heats_with_natgas)
    annual = pl.LazyFrame(
        {
            "bldg_id": bldg_ids,
            "annual_gas_therms": [
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                50.0,  # chesapeake_utilities RES-1
                400.0,  # chesapeake_utilities RES-2
                50.0,  # elkton_gas RES-1
                400.0,  # elkton_gas RES-2
                50.0,  # sandpiper RES-1
                400.0,  # sandpiper RES-2
            ],
        }
    )
    result = map_gas_tariff(
        SB_metadata=metadata,
        electric_utility_name="bge",
        annual_gas_therms=annual,
    )
    df = cast(pl.DataFrame, result.collect())
    produced_keys = set(df["tariff_key"].to_list()) - {"null_gas_tariff"}
    missing = produced_keys - available_stems
    assert not missing, (
        f"Mapper produced MD tariff_key(s) with no matching JSON file: {missing}"
    )
