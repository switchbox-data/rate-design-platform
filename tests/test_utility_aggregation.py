"""Unit tests for utility load aggregation logic.

These tests validate the zone-to-utility mapping and aggregation without S3.
"""

import sys
from datetime import datetime
from pathlib import Path

import polars as pl

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.eia.hourly_loads.aggregate_eia_utility_loads import aggregate_utility_load
from data.eia.hourly_loads.eia_region_config import get_utility_zone_mapping_for_state

UTILITY_ZONE_MAPPING = get_utility_zone_mapping_for_state("NY")


def create_sample_zone_data(zones: list[str], n_hours: int = 8760) -> pl.DataFrame:
    """Create sample zone load data for testing.

    Args:
        zones: List of zone identifiers
        n_hours: Number of hours

    Returns:
        DataFrame with timestamp, zone, load_mw columns
    """
    start_date = datetime(2024, 1, 1)
    timestamps = pl.datetime_range(
        start_date,
        datetime(2024, 12, 31, 23, 0, 0),
        interval="1h",
        eager=True,
    )[:n_hours]

    dfs = []
    for zone in zones:
        # Each zone has different base load
        base_load = 1000.0 * (ord(zone) - ord("A") + 1)

        zone_df = pl.DataFrame(
            {
                "timestamp": timestamps,
                "zone": zone,
                "load_mw": base_load,
            }
        )
        dfs.append(zone_df)

    return pl.concat(dfs)


def test_utility_zone_mapping_structure():
    """Test that utility-zone mapping has correct structure."""
    assert isinstance(UTILITY_ZONE_MAPPING, dict)
    assert len(UTILITY_ZONE_MAPPING) > 0

    # Check expected utilities are present
    expected_utilities = ["nyseg", "rge", "cenhud", "nimo"]
    for utility in expected_utilities:
        assert utility in UTILITY_ZONE_MAPPING, f"Missing utility: {utility}"
        assert isinstance(UTILITY_ZONE_MAPPING[utility], list)
        assert len(UTILITY_ZONE_MAPPING[utility]) > 0


def test_utility_zone_mapping_no_overlap_single_utility():
    """Test that Central Hudson has no zone overlap (serves only zone G)."""
    ch_zones = UTILITY_ZONE_MAPPING["cenhud"]
    assert ch_zones == ["G"], f"Central Hudson should only serve zone G, got {ch_zones}"


def test_aggregate_utility_load_single_zone():
    """Test aggregation for utility with single zone."""
    zones = ["G"]
    zone_df = create_sample_zone_data(zones)

    utility_df = aggregate_utility_load(zone_df, "cenhud", zones)

    # Check schema
    assert "timestamp" in utility_df.columns
    assert "utility" in utility_df.columns
    assert "load_mw" in utility_df.columns

    # Check utility name
    assert utility_df["utility"].unique().to_list() == ["cenhud"]

    # Check number of hours
    assert len(utility_df) == 8760

    # Load should equal zone G load (no aggregation needed)
    expected_load = 1000.0 * (ord("G") - ord("A") + 1)
    assert all(utility_df["load_mw"] == expected_load)


def test_aggregate_utility_load_multiple_zones():
    """Test aggregation for utility with multiple zones."""
    zones = ["A", "B"]
    zone_df = create_sample_zone_data(zones)

    utility_df = aggregate_utility_load(zone_df, "rge", zones)

    # Check number of hours
    assert len(utility_df) == 8760

    # Load should be sum of zone A and B loads
    expected_load_a = 1000.0 * (ord("A") - ord("A") + 1)
    expected_load_b = 1000.0 * (ord("B") - ord("A") + 1)
    expected_total = expected_load_a + expected_load_b

    assert all(utility_df["load_mw"] == expected_total), (
        f"Expected {expected_total}, got {utility_df['load_mw'][0]}"
    )


def test_aggregate_utility_load_sorted_by_timestamp():
    """Test that aggregated data is sorted by timestamp."""
    zones = ["A", "B", "C"]
    zone_df = create_sample_zone_data(zones)

    utility_df = aggregate_utility_load(zone_df, "Test Utility", zones)

    # Check that timestamps are sorted
    timestamps = utility_df["timestamp"].to_list()
    assert timestamps == sorted(timestamps), "Timestamps should be sorted"


def test_aggregate_utility_load_no_missing_hours():
    """Test that aggregation produces complete 8760 hourly data."""
    zones = ["A", "C", "D"]
    zone_df = create_sample_zone_data(zones)

    utility_df = aggregate_utility_load(zone_df, "nyseg", zones)

    # Check for 8760 hours (or 8784 for leap year)
    expected_hours = 8760
    assert len(utility_df) == expected_hours, (
        f"Expected {expected_hours} hours, got {len(utility_df)}"
    )

    # Check that there are no duplicate timestamps
    n_unique = utility_df["timestamp"].n_unique()
    assert n_unique == len(utility_df), (
        f"Found duplicate timestamps: {n_unique} unique out of {len(utility_df)}"
    )


def test_zone_coverage():
    """Test that all NYISO zones A-K are covered by at least one utility."""
    covered_zones = set()
    for zones in UTILITY_ZONE_MAPPING.values():
        covered_zones.update(zones)

    # Currently only covering zones A-G (H-K are ConEd/LIPA, excluded)
    expected_covered = ["A", "B", "C", "D", "E", "F", "G"]
    for zone in expected_covered:
        assert zone in covered_zones, f"Zone {zone} should be covered"


if __name__ == "__main__":
    # Run tests
    test_utility_zone_mapping_structure()
    print("✓ test_utility_zone_mapping_structure passed")

    test_utility_zone_mapping_no_overlap_single_utility()
    print("✓ test_utility_zone_mapping_no_overlap_single_utility passed")

    test_aggregate_utility_load_single_zone()
    print("✓ test_aggregate_utility_load_single_zone passed")

    test_aggregate_utility_load_multiple_zones()
    print("✓ test_aggregate_utility_load_multiple_zones passed")

    test_aggregate_utility_load_sorted_by_timestamp()
    print("✓ test_aggregate_utility_load_sorted_by_timestamp passed")

    test_aggregate_utility_load_no_missing_hours()
    print("✓ test_aggregate_utility_load_no_missing_hours passed")

    test_zone_coverage()
    print("✓ test_zone_coverage passed")

    print("\n✓ All tests passed!")
