"""Unit tests for marginal cost allocation logic.

These tests validate the core allocation algorithms without requiring S3 access.
"""

import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.pre.generate_utility_tx_dx_mc import (
    allocate_costs_to_hours,
    calculate_pop_weights,
    get_marginal_cost_for_utility,
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

    return pl.DataFrame(
        {
            "timestamp": timestamps,
            "utility": "Test Utility",
            "load_mw": load_mw,
        }
    )


def create_sample_mc_table() -> pl.DataFrame:
    """Create a sample marginal cost table for testing.

    Returns:
        DataFrame with columns: utility, sub_tx_and_dist_mc_kw_yr
    """
    return pl.DataFrame(
        {
            "utility": ["Test Utility", "Other Utility"],
            "sub_tx_and_dist_mc_kw_yr": [18.0, 25.0],
        }
    )


def test_calculate_pop_weights_normalization():
    """Test that PoP weights sum to 1.0."""
    df = create_sample_load_profile()

    result_df = calculate_pop_weights(df, n_hours=100)

    sum_w = result_df["w_sub_tx_and_dist"].sum()

    assert abs(sum_w - 1.0) < 1e-6, f"Weights sum to {sum_w}, not 1.0"


def test_calculate_pop_weights_peak_identification():
    """Test that peak hours are correctly identified."""
    df = create_sample_load_profile()

    result_df = calculate_pop_weights(df, n_hours=100)

    n_peaks = result_df["is_peak"].sum()

    assert n_peaks == 100, f"Expected 100 peaks, got {n_peaks}"


def test_calculate_pop_weights_load_weighted():
    """Test that weights are load-weighted, not uniform."""
    df = create_sample_load_profile()

    result_df = calculate_pop_weights(df, n_hours=100)

    weights = result_df.filter(pl.col("is_peak"))["w_sub_tx_and_dist"]

    unique_weights = weights.unique()
    assert len(unique_weights) > 1, "Weights should be load-weighted, not uniform"

    peak_df = result_df.filter(pl.col("is_peak")).sort("load_mw", descending=True)
    max_load_weight = peak_df.head(1)["w_sub_tx_and_dist"][0]
    min_load_weight = peak_df.tail(1)["w_sub_tx_and_dist"][0]

    assert max_load_weight > min_load_weight, (
        "Higher load hours should have higher weights"
    )


def test_allocate_costs_to_hours():
    """Test cost allocation to hourly signals."""
    df = create_sample_load_profile()
    df = calculate_pop_weights(df, n_hours=100)

    mc = 18.0  # $/kW-yr

    result_df = allocate_costs_to_hours(df, mc)

    assert "mc_total_per_kwh" in result_df.columns

    non_peak_costs = result_df.filter(~pl.col("is_peak"))["mc_total_per_kwh"]
    assert all(cost == 0 for cost in non_peak_costs), (
        "Non-peak hours should have zero marginal cost"
    )


def test_validation_1kw_constant_load():
    """Test that 1 kW constant load sums back to the input $/kW-yr."""
    df = create_sample_load_profile()
    df = calculate_pop_weights(df, n_hours=100)

    mc = 18.0  # $/kW-yr

    result_df = allocate_costs_to_hours(df, mc)
    total_cost = result_df["mc_total_per_kwh"].sum()

    assert abs(total_cost - mc) < 0.001, f"Total cost {total_cost} != {mc}"


def test_get_marginal_cost_for_utility():
    """Test extraction of marginal cost from table."""
    mc_df = create_sample_mc_table()

    mc = get_marginal_cost_for_utility(mc_df, "Test Utility")

    assert mc == 18.0, f"Expected 18.0, got {mc}"


def test_get_marginal_cost_for_utility_not_found():
    """Test that error is raised when utility not in table."""
    mc_df = create_sample_mc_table()

    with pytest.raises(ValueError, match="No marginal cost data found"):
        get_marginal_cost_for_utility(mc_df, "Nonexistent Utility")


def test_leap_year_handling():
    """Test that allocation handles leap years correctly (8784 hours)."""
    start_date = datetime(2024, 1, 1)
    timestamps = pl.datetime_range(
        start_date,
        datetime(2024, 12, 31, 23, 0, 0),
        interval="1h",
        eager=True,
    )

    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "utility": "Test Utility",
            "load_mw": 1000.0,
        }
    )

    df = calculate_pop_weights(df, n_hours=100)
    df = allocate_costs_to_hours(df, 18.0)

    total_cost = df["mc_total_per_kwh"].sum()
    assert abs(total_cost - 18.0) < 0.001, (
        f"Leap year validation failed: {total_cost} != 18.0"
    )


def test_different_allocation_windows():
    """Test allocation with different window sizes."""
    df = create_sample_load_profile()

    for n_hours in [200, 50, 10]:
        result_df = calculate_pop_weights(df, n_hours=n_hours)
        result_df = allocate_costs_to_hours(result_df, 18.0)

        total_cost = result_df["mc_total_per_kwh"].sum()
        assert abs(total_cost - 18.0) < 0.001, (
            f"Validation failed for window {n_hours}: {total_cost}"
        )


if __name__ == "__main__":
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

    test_get_marginal_cost_for_utility()
    print("✓ test_get_marginal_cost_for_utility passed")

    test_get_marginal_cost_for_utility_not_found()
    print("✓ test_get_marginal_cost_for_utility_not_found passed")

    test_leap_year_handling()
    print("✓ test_leap_year_handling passed")

    test_different_allocation_windows()
    print("✓ test_different_allocation_windows passed")

    print("\n✓ All tests passed!")
