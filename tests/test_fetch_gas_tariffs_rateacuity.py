"""Tests for gas Rate Acuity fetch script (shortcode resolution and YAML config; no live browser)."""

from pathlib import Path

import pytest

from utils.pre.fetch_gas_tariffs_rateacuity import _resolve_utility, load_config
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


def test_load_config_ny() -> None:
    """load_config returns NY and utility keys from the NY rateacuity_tariffs.yaml."""
    project_root = Path(__file__).resolve().parents[1]
    yaml_path = (
        project_root
        / "rate_design/ny/hp_rates/config/tariffs/gas/rateacuity_tariffs.yaml"
    )
    state, utilities = load_config(yaml_path)
    assert state == "NY"
    assert "coned" in utilities
    assert (
        utilities["coned"]["coned_sf"]
        == "1-RESIDENTIAL AND RELIGIOUS FIRM SALES SERVICE"
    )
    assert "rie" not in utilities


def test_load_config_ri() -> None:
    """load_config returns RI and rie with heating/nonheating from the RI rateacuity_tariffs.yaml."""
    project_root = Path(__file__).resolve().parents[1]
    yaml_path = (
        project_root
        / "rate_design/ri/hp_rates/config/tariffs/gas/rateacuity_tariffs.yaml"
    )
    state, utilities = load_config(yaml_path)
    assert state == "RI"
    assert "rie" in utilities and len(utilities) == 1
    assert utilities["rie"]["rie_nonheating"] == "Non-Heating"
    assert utilities["rie"]["rie_heating"] == "Heating"
