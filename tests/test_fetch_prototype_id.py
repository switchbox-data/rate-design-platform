"""Tests for _fetch_prototype_ids_by_electric_util function."""

from typing import cast

import polars as pl
import pytest

from utils.cairo import _fetch_prototype_ids_by_electric_util
from utils.types import ElectricUtility


def test_fetch_prototype_ids_single_utility() -> None:
    """Test fetching building IDs for a single utility."""
    # Create dummy data with one utility
    data = {
        "bldg_id": [1, 2, 3, 4, 5],
        "sb.electric_utility": ["coned", "coned", "coned", "coned", "coned"],
        "sb.gas_utility": ["nfg", "nfg", "nfg", "nfg", "nfg"],
    }
    utility_assignment = pl.LazyFrame(data)

    result = _fetch_prototype_ids_by_electric_util("coned", utility_assignment)

    assert result == [1, 2, 3, 4, 5]
    assert isinstance(result, list)
    assert all(isinstance(bldg_id, int) for bldg_id in result)


def test_fetch_prototype_ids_multiple_utilities() -> None:
    """Test fetching building IDs when multiple utilities are present."""
    # Create dummy data with multiple utilities
    data = {
        "bldg_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "sb.electric_utility": [
            "coned",
            "coned",
            "psegli",
            "psegli",
            "psegli",
            "rge",
            "rge",
            "nimo",
            "nimo",
            "nimo",
        ],
        "sb.gas_utility": [
            "nfg",
            "nfg",
            "kedli",
            "kedli",
            "kedli",
            "rge",
            "rge",
            "nimo",
            "nimo",
            "nimo",
        ],
    }
    utility_assignment = pl.LazyFrame(data)

    # Test coned
    result_coned = _fetch_prototype_ids_by_electric_util("coned", utility_assignment)
    assert result_coned == [1, 2]

    # Test psegli
    result_psegli = _fetch_prototype_ids_by_electric_util("psegli", utility_assignment)
    assert result_psegli == [3, 4, 5]

    # Test rge
    result_rge = _fetch_prototype_ids_by_electric_util("rge", utility_assignment)
    assert result_rge == [6, 7]

    # Test nimo
    result_nimo = _fetch_prototype_ids_by_electric_util("nimo", utility_assignment)
    assert result_nimo == [8, 9, 10]


def test_fetch_prototype_ids_different_utility_values() -> None:
    """Test fetching building IDs for different utility values from the type."""
    # Test with various utilities from ElectricUtility Literal type
    utilities_to_test = [
        "bath",
        "cenhud",
        "chautauqua",
        "coned",
        "nimo",
        "nyseg",
        "or",
        "psegli",
        "rge",
        "rie",
    ]

    for utility_str in utilities_to_test:
        utility = cast(ElectricUtility, utility_str)
        data = {
            "bldg_id": [100, 200, 300],
            "sb.electric_utility": [utility_str, utility_str, utility_str],
            "sb.gas_utility": ["nfg", "nfg", "nfg"],
        }
        utility_assignment = pl.LazyFrame(data)

        result = _fetch_prototype_ids_by_electric_util(utility, utility_assignment)
        assert result == [100, 200, 300]
        assert len(result) == 3


def test_fetch_prototype_ids_empty_result_raises_error() -> None:
    """Test that ValueError is raised when no buildings are assigned to the utility."""
    data = {
        "bldg_id": [1, 2, 3],
        "sb.electric_utility": ["coned", "coned", "psegli"],
        "sb.gas_utility": ["nfg", "nfg", "kedli"],
    }
    utility_assignment = pl.LazyFrame(data)

    with pytest.raises(ValueError, match="No buildings assigned to rge"):
        _fetch_prototype_ids_by_electric_util("rge", utility_assignment)


def test_fetch_prototype_ids_missing_column_raises_error() -> None:
    """Test that ValueError is raised when sb.electric_utility column is missing."""
    data = {
        "bldg_id": [1, 2, 3],
        "sb.gas_utility": ["nfg", "nfg", "kedli"],
    }
    utility_assignment = pl.LazyFrame(data)

    with pytest.raises(ValueError, match="sb.electric_utility column not found"):
        _fetch_prototype_ids_by_electric_util("coned", utility_assignment)


def test_fetch_prototype_ids_large_dataset() -> None:
    """Test fetching building IDs with a larger dataset."""
    # Create a larger dataset with mixed utilities
    bldg_ids = list(range(1, 1001))
    utilities = ["coned"] * 300 + ["psegli"] * 400 + ["rge"] * 300
    gas_utilities = ["nfg"] * 1000

    data = {
        "bldg_id": bldg_ids,
        "sb.electric_utility": utilities,
        "sb.gas_utility": gas_utilities,
    }
    utility_assignment = pl.LazyFrame(data)

    result_psegli = _fetch_prototype_ids_by_electric_util("psegli", utility_assignment)
    assert len(result_psegli) == 400
    assert result_psegli == list(range(301, 701))  # IDs 301-700
