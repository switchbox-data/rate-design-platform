from typing import cast

import polars as pl
import pytest

from data.resstock.identify_natgas_connection import (
    NATGAS_CONSUMPTION_COLUMN,
    identify_natgas_connection,
)


def test_identify_natgas_connection_happy_path():
    """Test normal case: some buildings have natgas, some don't."""
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5],
            "heats_with_natgas": [True, False, True, False, False],
            "other_col": ["a", "b", "c", "d", "e"],
        }
    ).lazy()

    load_curve_annual = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4, 5],
            NATGAS_CONSUMPTION_COLUMN: [100.0, 0.0, 50.0, 0.0, 0.0],
            "other_col": ["x", "y", "z", "w", "v"],
        }
    ).lazy()

    result = identify_natgas_connection(metadata, load_curve_annual)
    result_df = cast(pl.DataFrame, result.collect())

    assert "has_natgas_connection" in result_df.columns
    assert result_df["has_natgas_connection"].to_list() == [
        True,  # bldg 1: has natgas consumption
        False,  # bldg 2: no natgas consumption
        True,  # bldg 3: has natgas consumption
        False,  # bldg 4: no natgas consumption
        False,  # bldg 5: no natgas consumption
    ]
    # Original columns preserved
    assert "heats_with_natgas" in result_df.columns
    assert "other_col" in result_df.columns


def test_identify_natgas_connection_missing_heats_with_natgas():
    """Test that missing heats_with_natgas column raises ValueError."""
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "other_col": ["a", "b"],
        }
    ).lazy()

    load_curve_annual = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            NATGAS_CONSUMPTION_COLUMN: [100.0, 0.0],
        }
    ).lazy()

    with pytest.raises(ValueError, match="heats_with_natgas"):
        identify_natgas_connection(metadata, load_curve_annual)


def test_identify_natgas_connection_row_count_mismatch_missing_bldg_id():
    """Test that missing bldg_id in load_curve_annual raises ValueError."""
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3],
            "heats_with_natgas": [True, False, False],
        }
    ).lazy()

    # Missing bldg_id 3
    load_curve_annual = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            NATGAS_CONSUMPTION_COLUMN: [100.0, 0.0],
        }
    ).lazy()

    with pytest.raises(ValueError, match="Row count mismatch"):
        identify_natgas_connection(metadata, load_curve_annual)


def test_identify_natgas_connection_row_count_mismatch_duplicate_bldg_id():
    """Test that duplicate bldg_id in load_curve_annual raises ValueError."""
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "heats_with_natgas": [True, False],
        }
    ).lazy()

    # Duplicate bldg_id 1
    load_curve_annual = pl.DataFrame(
        {
            "bldg_id": [1, 1, 2],
            NATGAS_CONSUMPTION_COLUMN: [100.0, 50.0, 0.0],
        }
    ).lazy()

    with pytest.raises(ValueError, match="Row count mismatch"):
        identify_natgas_connection(metadata, load_curve_annual)


def test_identify_natgas_connection_sanity_check_violation():
    """Test that heats_with_natgas=True but has_natgas_connection=False raises ValueError."""
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "heats_with_natgas": [True, False],  # bldg 1 heats with natgas
        }
    ).lazy()

    # bldg 1 has heats_with_natgas=True but zero natgas consumption (violation)
    load_curve_annual = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            NATGAS_CONSUMPTION_COLUMN: [0.0, 0.0],  # bldg 1 has zero consumption
        }
    ).lazy()

    with pytest.raises(ValueError, match="Sanity check failed"):
        identify_natgas_connection(metadata, load_curve_annual)


def test_identify_natgas_connection_drops_existing_column():
    """Test that existing has_natgas_connection column is dropped and recomputed."""
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "heats_with_natgas": [True, False],
            "has_natgas_connection": [False, False],  # Old value, should be replaced
        }
    ).lazy()

    load_curve_annual = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            NATGAS_CONSUMPTION_COLUMN: [100.0, 0.0],
        }
    ).lazy()

    result = identify_natgas_connection(metadata, load_curve_annual)
    result_df = cast(pl.DataFrame, result.collect())

    # Should have new values, not the old False values
    assert result_df["has_natgas_connection"].to_list() == [True, False]


def test_identify_natgas_connection_zero_consumption():
    """Test that zero consumption correctly sets has_natgas_connection=False."""
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "heats_with_natgas": [False, False],
        }
    ).lazy()

    load_curve_annual = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            NATGAS_CONSUMPTION_COLUMN: [0.0, 0.0],
        }
    ).lazy()

    result = identify_natgas_connection(metadata, load_curve_annual)
    result_df = cast(pl.DataFrame, result.collect())

    assert result_df["has_natgas_connection"].to_list() == [False, False]


def test_identify_natgas_connection_positive_consumption():
    """Test that positive consumption correctly sets has_natgas_connection=True."""
    metadata = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "heats_with_natgas": [True, False],
        }
    ).lazy()

    load_curve_annual = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            NATGAS_CONSUMPTION_COLUMN: [
                0.1,
                0.0,
            ],  # Even tiny positive value should be True
        }
    ).lazy()

    result = identify_natgas_connection(metadata, load_curve_annual)
    result_df = cast(pl.DataFrame, result.collect())

    assert result_df["has_natgas_connection"].to_list() == [True, False]
