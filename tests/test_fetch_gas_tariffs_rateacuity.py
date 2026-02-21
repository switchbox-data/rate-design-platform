"""Tests for gas Rate Acuity fetch script (shortcode resolution only; no live browser)."""

import pytest

from utils.pre.fetch_gas_tariffs_rateacuity import _resolve_utility
from utils.pre.ny_gas_tariff_mapping import match_tariff_key as match_tariff_key_ny
from utils.pre.ri_gas_tariff_mapping import match_tariff_key as match_tariff_key_ri
from utils.utility_codes import get_rate_acuity_utility_names, get_utilities_for_state


# Example dropdown names that might appear in Rate Acuity (or exact candidates we use).
FAKE_UTILITIES_NY = [
    "Consolidated Edison Company of New York",
    "The Brooklyn Union Gas Company",
    "Keyspan Gas East",
    "New York State Electric & Gas",
    "Niagara Mohawk Power Corporation",
    "Rochester Gas and Electric",
    "Central Hudson Gas & Electric",
    "Orange and Rockland Utilities",
    "National Fuel Gas",
]

FAKE_UTILITIES_RI = [
    "Rhode Island Energy (formally National Grid)",
    "The Narragansett Electric Company",
]


@pytest.mark.parametrize(
    "shortcode,expected_match",
    [
        ("coned", "Consolidated Edison Company of New York"),
        ("kedny", "The Brooklyn Union Gas Company"),
        ("kedli", "Keyspan Gas East"),
        ("nyseg", "New York State Electric & Gas"),
        ("nimo", "Niagara Mohawk Power Corporation"),
        ("rge", "Rochester Gas and Electric"),
        ("cenhud", "Central Hudson Gas & Electric"),
        ("or", "Orange and Rockland Utilities"),
        ("nfg", "National Fuel Gas"),
    ],
)
def test_resolve_utility_shortcode_ny(shortcode: str, expected_match: str) -> None:
    """Shortcode resolves to the expected utility when that name is in the dropdown."""
    assert _resolve_utility("NY", FAKE_UTILITIES_NY, shortcode) == expected_match


def test_resolve_utility_shortcode_ri() -> None:
    """RI rie shortcode resolves when RI Energy (or Narragansett) is in the dropdown."""
    assert _resolve_utility("RI", FAKE_UTILITIES_RI, "rie") in (
        "Rhode Island Energy (formally National Grid)",
        "The Narragansett Electric Company",
    )


def test_resolve_utility_exact_name_passthrough() -> None:
    """Exact utility name in dropdown is returned as-is."""
    name = "Consolidated Edison Company of New York"
    assert _resolve_utility("NY", FAKE_UTILITIES_NY, name) == name


def test_resolve_utility_unknown_shortcode_raises() -> None:
    """Unknown shortcode raises ValueError with available options."""
    with pytest.raises(ValueError, match="not found"):
        _resolve_utility("NY", FAKE_UTILITIES_NY, "unknown_code")


def test_resolve_utility_candidates_from_utility_codes_exist_in_fake_list() -> None:
    """Every NY gas utility with rate_acuity_utility_names resolves to a name in our fake list."""
    for std_name in get_utilities_for_state("NY", "gas"):
        try:
            get_rate_acuity_utility_names("NY", std_name)
        except ValueError:
            continue  # no rate_acuity_utility_names for this utility
        resolved = _resolve_utility("NY", FAKE_UTILITIES_NY, std_name)
        assert resolved in FAKE_UTILITIES_NY, f"{std_name} resolved to {resolved!r}"


def test_match_tariff_key_ny() -> None:
    """match_tariff_key returns coned_sf for NY ConEd residential firm sales."""
    got = match_tariff_key_ny(
        "Consolidated Edison Company of New York",
        "ON HOLD-1-RESIDENTIAL AND RELIGIOUS FIRM SALES",
    )
    assert got == "coned_sf"


def test_match_tariff_key_ri_heating() -> None:
    """match_tariff_key returns rie_heating for RI heating schedule."""
    got = match_tariff_key_ri(
        "The Narragansett Electric Company", "Residential Heating"
    )
    assert got == "rie_heating"


def test_match_tariff_key_ri_nonheating() -> None:
    """match_tariff_key returns rie_nonheating for RI non-heating schedule."""
    got = match_tariff_key_ri(
        "The Narragansett Electric Company", "Residential Non-Heating"
    )
    assert got == "rie_nonheating"
