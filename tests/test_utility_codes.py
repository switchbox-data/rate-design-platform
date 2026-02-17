"""Tests for utils/utility_codes.py."""

from typing import get_args

from utils.types import electric_utility, gas_utility
from utils.utility_codes import (
    UTILITIES,
    get_all_std_names,
    get_eia_utility_id_to_std_name,
    get_electric_std_names,
    get_gas_std_names,
    get_ny_open_data_to_std_name,
    get_std_name_to_gas_tariff_key,
    std_name_to_display_name,
)


def test_get_ny_open_data_to_std_name_returns_expected_mappings():
    """Consolidated Edison, NYSEG, etc. map to std_name."""
    mapping = get_ny_open_data_to_std_name()
    assert mapping["Consolidated Edison"] == "coned"
    assert mapping["NYS Electric and Gas"] == "nyseg"
    assert mapping["National Grid"] == "nimo"
    assert mapping["Rochester Gas and Electric"] == "rge"
    assert mapping["Central Hudson Gas and Electric"] == "cenhud"
    assert mapping["Orange and Rockland Utilities"] == "or"
    assert mapping["National Grid - NYC"] == "kedny"
    assert mapping["National Grid - Long Island"] == "kedli"


def test_get_eia_utility_id_to_std_name_ny():
    """EIA utility IDs map to expected std_names for NY."""
    mapping = get_eia_utility_id_to_std_name("NY")
    assert mapping[4226] == "coned"
    assert mapping[13573] == "nimo"  # Niagara Mohawk Power Corp.
    assert mapping[13511] == "nyseg"
    assert mapping[16183] == "rge"
    assert mapping[3249] == "cenhud"
    assert mapping[14154] == "or"


def test_get_std_name_to_gas_tariff_key():
    """nimo, kedny, kedli -> national_grid; nyseg -> nyseg."""
    mapping = get_std_name_to_gas_tariff_key()
    assert mapping["nimo"] == "national_grid"
    assert mapping["kedny"] == "national_grid"
    assert mapping["kedli"] == "national_grid"
    assert mapping["nyseg"] == "nyseg"
    assert mapping["coned"] == "coned"
    assert mapping["rge"] == "rge"


def test_no_duplicate_std_names():
    """Each std_name appears exactly once in UTILITIES."""
    std_names = [u["std_name"] for u in UTILITIES]
    assert len(std_names) == len(set(std_names))


def test_eia_utility_ids_unique_per_state():
    """No EIA ID maps to multiple std_names in same state."""
    for state in ("NY", "RI"):
        ids_seen: set[int] = set()
        for u in UTILITIES:
            if u.get("state") != state:
                continue
            for eia_id in u.get("eia_utility_ids", []):
                assert eia_id not in ids_seen, f"EIA ID {eia_id} duplicated in {state}"
                ids_seen.add(eia_id)


def test_get_electric_std_names_includes_electric_utilities():
    """Electric std_names include coned, nyseg, nimo, etc."""
    electric = get_electric_std_names()
    assert "coned" in electric
    assert "nyseg" in electric
    assert "nimo" in electric
    assert "rge" in electric
    assert "rie" in electric


def test_get_gas_std_names_includes_gas_utilities():
    """Gas std_names include coned, nyseg, kedny, kedli, etc."""
    gas = get_gas_std_names()
    assert "coned" in gas
    assert "nyseg" in gas
    assert "nimo" in gas
    assert "kedny" in gas
    assert "kedli" in gas


def test_get_all_std_names_is_union():
    """get_all_std_names returns all utilities."""
    all_names = set(get_all_std_names())
    electric = set(get_electric_std_names())
    gas = set(get_gas_std_names())
    assert electric | gas <= all_names
    assert "coned" in all_names
    assert "none" in all_names


def test_get_electric_and_gas_return_tuples():
    """Functions return tuples for Literal type inference."""
    assert isinstance(get_all_std_names(), tuple)
    assert isinstance(get_electric_std_names(), tuple)
    assert isinstance(get_gas_std_names(), tuple)


def test_std_name_to_display_name():
    """Display names are human-readable."""
    assert std_name_to_display_name("coned") == "Coned"
    assert std_name_to_display_name("nyseg") == "NYSEG"
    assert std_name_to_display_name("nimo") == "National Grid"
    assert std_name_to_display_name("unknown") == "unknown"


def test_types_literal_in_sync_with_utility_codes():
    """utils.types electric_utility and gas_utility Literals include all from utility_codes."""
    electric_from_types = set(get_args(electric_utility))
    gas_from_types = set(get_args(gas_utility))
    assert set(get_electric_std_names()) <= electric_from_types, (
        f"types.electric_utility missing: {set(get_electric_std_names()) - electric_from_types}"
    )
    assert set(get_gas_std_names()) <= gas_from_types, (
        f"types.gas_utility missing: {set(get_gas_std_names()) - gas_from_types}"
    )
