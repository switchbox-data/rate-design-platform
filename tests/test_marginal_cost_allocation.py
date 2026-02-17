"""Unit tests for marginal cost allocation logic.

These tests validate the core allocation algorithms without requiring S3 access.
"""

import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.pre.generate_utility_tx_dx_mc import (
    allocate_costs_to_hours,
    calculate_pop_weights,
    get_marginal_costs_for_year,
)


def create_sample_load_profile(n_hours: int = 8760) -> pl.DataFrame:
    """Create a sample load profile for testing.

    Args:
        n_hours: Number of hours (default: 8760 for non-leap year)

    Returns:
        DataFrame with timestamp, utility, load_mw columns
    """
    start_date = datetime(2024, 1, 1)
    timestamps = pl.datetime_range(
        start_date,
        datetime(2024, 12, 31, 23, 0, 0),
        interval="1h",
        eager=True,
    )[:n_hours]

    # Create varying load pattern (higher during day, lower at night)
    # plus a tiny monotonic trend to avoid tied peak loads.
    hours_of_day = pl.Series(range(n_hours)) % 24
    hour_index = pl.Series(range(n_hours))
    base_load = 1000.0
    daily_variation = 500.0
    load_mw = base_load + daily_variation * (hours_of_day / 24.0) + 0.001 * hour_index

    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "utility": "Test Utility",
            "load_mw": load_mw,
        }
    )

    return df


def create_sample_mc_table() -> pl.DataFrame:
    """Create a sample marginal cost table for testing.

    Returns:
        DataFrame with utility, year, upstream, distribution_substation,
        primary_feeder, total_mc columns
    """
    data = {
        "utility": ["Test Utility", "Test Utility"],
        "year": [2026, 2027],
        "upstream": [10.0, 15.0],
        "distribution_substation": [5.0, 7.0],
        "primary_feeder": [3.0, 4.0],
        "total_mc": [18.0, 26.0],
    }

    return pl.DataFrame(data)


def test_calculate_pop_weights_normalization():
    """Test that PoP weights sum to 1.0."""
    df = create_sample_load_profile()

    result_df = calculate_pop_weights(df, n_upstream_hours=100, n_dist_hours=50)

    # Check weights sum to 1.0
    sum_w_upstream = result_df["w_upstream"].sum()
    sum_w_dist = result_df["w_dist"].sum()

    assert abs(sum_w_upstream - 1.0) < 1e-6, (
        f"Upstream weights sum to {sum_w_upstream}, not 1.0"
    )
    assert abs(sum_w_dist - 1.0) < 1e-6, (
        f"Distribution weights sum to {sum_w_dist}, not 1.0"
    )


def test_calculate_pop_weights_peak_identification():
    """Test that peak hours are correctly identified."""
    df = create_sample_load_profile()

    result_df = calculate_pop_weights(df, n_upstream_hours=100, n_dist_hours=50)

    # Check correct number of peak hours
    n_upstream_peaks = result_df["is_upstream_peak"].sum()
    n_dist_peaks = result_df["is_dist_peak"].sum()

    assert n_upstream_peaks == 100, (
        f"Expected 100 upstream peaks, got {n_upstream_peaks}"
    )
    assert n_dist_peaks == 50, f"Expected 50 distribution peaks, got {n_dist_peaks}"

    # Check that distribution peaks are subset of upstream peaks (top 50 of top 100)
    dist_peak_timestamps = result_df.filter(pl.col("is_dist_peak"))["timestamp"]
    upstream_peak_timestamps = result_df.filter(pl.col("is_upstream_peak"))["timestamp"]

    # All dist peaks should also be upstream peaks
    assert all(ts in upstream_peak_timestamps for ts in dist_peak_timestamps), (
        "Distribution peaks should be subset of upstream peaks"
    )


def test_calculate_pop_weights_load_weighted():
    """Test that weights are load-weighted, not uniform."""
    df = create_sample_load_profile()

    result_df = calculate_pop_weights(df, n_upstream_hours=100, n_dist_hours=50)

    # Get weights for peak hours
    upstream_weights = result_df.filter(pl.col("is_upstream_peak"))["w_upstream"]

    # Weights should not all be equal (not uniform distribution)
    unique_weights = upstream_weights.unique()
    assert len(unique_weights) > 1, "Weights should be load-weighted, not uniform"

    # Higher load hours should have higher weights
    peak_df = result_df.filter(pl.col("is_upstream_peak")).sort(
        "load_mw", descending=True
    )
    max_load_weight = peak_df.head(1)["w_upstream"][0]
    min_load_weight = peak_df.tail(1)["w_upstream"][0]

    assert max_load_weight > min_load_weight, (
        "Higher load hours should have higher weights"
    )


def test_allocate_costs_to_hours():
    """Test cost allocation to hourly signals."""
    df = create_sample_load_profile()
    df = calculate_pop_weights(df, n_upstream_hours=100, n_dist_hours=50)

    mc_upstream = 10.0  # $/kW-yr
    mc_dist = 8.0  # $/kW-yr

    result_df = allocate_costs_to_hours(df, mc_upstream, mc_dist)

    # Check that columns were added
    assert "mc_upstream_per_kwh" in result_df.columns
    assert "mc_dist_per_kwh" in result_df.columns
    assert "mc_total_per_kwh" in result_df.columns

    # Check that only peak hours have non-zero costs
    non_peak_costs = result_df.filter(
        ~pl.col("is_upstream_peak") & ~pl.col("is_dist_peak")
    )["mc_total_per_kwh"]

    assert all(cost == 0 for cost in non_peak_costs), (
        "Non-peak hours should have zero marginal cost"
    )


def test_validation_1kw_constant_load():
    """Test that 1 kW constant load validation passes."""
    df = create_sample_load_profile()
    df = calculate_pop_weights(df, n_upstream_hours=100, n_dist_hours=50)

    mc_upstream = 10.0  # $/kW-yr
    mc_dist = 8.0  # $/kW-yr

    result_df = allocate_costs_to_hours(df, mc_upstream, mc_dist)

    # Calculate total cost for 1 kW constant load
    total_upstream_cost = result_df["mc_upstream_per_kwh"].sum()
    total_dist_cost = result_df["mc_dist_per_kwh"].sum()
    total_cost = result_df["mc_total_per_kwh"].sum()

    # Should equal input $/kW-yr values
    assert abs(total_upstream_cost - mc_upstream) < 0.001, (
        f"Upstream cost {total_upstream_cost} != {mc_upstream}"
    )
    assert abs(total_dist_cost - mc_dist) < 0.001, (
        f"Distribution cost {total_dist_cost} != {mc_dist}"
    )
    assert abs(total_cost - (mc_upstream + mc_dist)) < 0.001, (
        f"Total cost {total_cost} != {mc_upstream + mc_dist}"
    )


def test_validation_with_zero_costs():
    """Test that allocation works correctly with zero marginal costs."""
    df = create_sample_load_profile()
    df = calculate_pop_weights(df, n_upstream_hours=100, n_dist_hours=50)

    mc_upstream = 0.0  # $/kW-yr (no upstream costs)
    mc_dist = 8.0  # $/kW-yr

    result_df = allocate_costs_to_hours(df, mc_upstream, mc_dist)

    # Calculate total cost for 1 kW constant load
    total_upstream_cost = result_df["mc_upstream_per_kwh"].sum()
    total_dist_cost = result_df["mc_dist_per_kwh"].sum()

    assert abs(total_upstream_cost - 0.0) < 1e-10, (
        f"Upstream cost should be 0, got {total_upstream_cost}"
    )
    assert abs(total_dist_cost - mc_dist) < 0.001, (
        f"Distribution cost {total_dist_cost} != {mc_dist}"
    )


def test_get_marginal_costs_for_year():
    """Test extraction of marginal costs from table."""
    mc_df = create_sample_mc_table()

    mc_upstream, mc_dist = get_marginal_costs_for_year(mc_df, "Test Utility", 2026)

    assert mc_upstream == 10.0, f"Expected 10.0, got {mc_upstream}"
    assert mc_dist == 8.0, f"Expected 8.0 (5.0 + 3.0), got {mc_dist}"


def test_get_marginal_costs_for_year_not_found():
    """Test that error is raised when utility/year not in table."""
    mc_df = create_sample_mc_table()

    with pytest.raises(ValueError, match="No marginal cost data found"):
        get_marginal_costs_for_year(mc_df, "Nonexistent Utility", 2026)


def test_leap_year_handling():
    """Test that allocation handles leap years correctly (8784 hours)."""
    # Create 8784 hour profile for leap year
    start_date = datetime(2024, 1, 1)
    timestamps = pl.datetime_range(
        start_date,
        datetime(2024, 12, 31, 23, 0, 0),
        interval="1h",
        eager=True,
    )  # 2024 is a leap year

    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "utility": "Test Utility",
            "load_mw": 1000.0,
        }
    )

    df = calculate_pop_weights(df, n_upstream_hours=100, n_dist_hours=50)
    df = allocate_costs_to_hours(df, 10.0, 8.0)

    # Validation should still pass
    total_cost = df["mc_total_per_kwh"].sum()
    assert abs(total_cost - 18.0) < 0.001, (
        f"Leap year validation failed: {total_cost} != 18.0"
    )


def test_different_allocation_windows():
    """Test allocation with different window sizes."""
    df = create_sample_load_profile()

    # Test with different window sizes
    for upstream_hours, dist_hours in [(200, 100), (50, 25), (10, 5)]:
        result_df = calculate_pop_weights(df, upstream_hours, dist_hours)
        result_df = allocate_costs_to_hours(result_df, 10.0, 8.0)

        # Validation should pass regardless of window size
        total_cost = result_df["mc_total_per_kwh"].sum()
        assert abs(total_cost - 18.0) < 0.001, (
            f"Validation failed for window ({upstream_hours}, {dist_hours}): {total_cost}"
        )


if __name__ == "__main__":
    # Run tests
    test_calculate_pop_weights_normalization()
    print("✓ test_calculate_pop_weights_normalization passed")

    test_calculate_pop_weights_peak_identification()
    print("✓ test_calculate_pop_weights_peak_identification passed")

    test_calculate_pop_weights_load_weighted()
    print("✓ test_calculate_pop_weights_load_weighted passed")

    test_allocate_costs_to_hours()
    print("✓ test_allocate_costs_to_hours passed")

    test_validation_1kw_constant_load()
    print("✓ test_validation_1kw_constant_load passed")

    test_validation_with_zero_costs()
    print("✓ test_validation_with_zero_costs passed")

    test_get_marginal_costs_for_year()
    print("✓ test_get_marginal_costs_for_year passed")

    test_get_marginal_costs_for_year_not_found()
    print("✓ test_get_marginal_costs_for_year_not_found passed")

    test_leap_year_handling()
    print("✓ test_leap_year_handling passed")

    test_different_allocation_windows()
    print("✓ test_different_allocation_windows passed")

    print("\n✓ All tests passed!")
