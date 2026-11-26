"""Unit tests for marginal cost allocation logic.

These tests validate the core allocation algorithms without requiring S3 access.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.data_prep.marginal_costs.inputs.generic import get_marginal_cost_for_utility
from utils.data_prep.marginal_costs.methods.top_n_load_share import top_n_load_share
from utils.data_prep.marginal_costs.supply_utils import (
    output_base_for_load_year,
    remap_year_if_needed,
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


def test_top_n_load_share_weights_sum_to_one():
    """Hourly costs divided by the annual cost are weights that sum to 1."""
    df = create_sample_load_profile()
    annual = 18.0

    result_df = top_n_load_share(df, annual, 100)

    sum_w = float(result_df["mc_total_per_kwh"].sum()) / annual
    assert abs(sum_w - 1.0) < 1e-6, f"Weights sum to {sum_w}, not 1.0"


def test_top_n_load_share_peak_identification():
    """Exactly n_hours receive a nonzero cost, and the other hours stay zero."""
    df = create_sample_load_profile()

    result_df = top_n_load_share(df, 18.0, 100)

    n_peaks = result_df.filter(pl.col("mc_total_per_kwh") > 0).height
    assert n_peaks == 100, f"Expected 100 peaks, got {n_peaks}"
    assert result_df.height == df.height
    assert result_df.filter(pl.col("mc_total_per_kwh") == 0).height == df.height - 100


def test_top_n_load_share_is_load_weighted():
    """Costs follow load within the top hours, rather than a uniform share."""
    df = create_sample_load_profile()

    result_df = top_n_load_share(df, 18.0, 100)
    peaks = result_df.join(df.select("timestamp", "load_mw"), on="timestamp").filter(pl.col("mc_total_per_kwh") > 0)

    assert peaks["mc_total_per_kwh"].n_unique() > 1, "Costs should be load-weighted, not uniform"

    ordered = peaks.sort("load_mw", descending=True)
    assert ordered["mc_total_per_kwh"][0] > ordered["mc_total_per_kwh"][-1]


def test_top_n_load_share_output_column():
    """The default cost column is mc_total_per_kwh, on every input hour."""
    df = create_sample_load_profile()

    result_df = top_n_load_share(df, 18.0, 100)

    assert result_df.columns == ["timestamp", "mc_total_per_kwh"]


def test_validation_1kw_constant_load():
    """Test that 1 kW constant load sums back to the input $/kW-yr."""
    df = create_sample_load_profile()
    annual = 18.0

    result_df = top_n_load_share(df, annual, 100)
    total_cost = float(result_df["mc_total_per_kwh"].sum())

    assert abs(total_cost - annual) < 0.001, f"Total cost {total_cost} != {annual}"


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


def test_default_load_year_preserves_canonical_output_base() -> None:
    base = "s3://data.sb/switchbox/marginal_costs/md/dist_and_sub_tx/"

    assert output_base_for_load_year(base, output_year=2025, load_year=2025) == base


def test_alternate_load_year_uses_isolated_sibling_output_base() -> None:
    base = "s3://data.sb/switchbox/marginal_costs/md/dist_and_sub_tx/"

    assert (
        output_base_for_load_year(
            base,
            output_year=2025,
            load_year=2018,
        )
        == "s3://data.sb/switchbox/marginal_costs/md/dist_and_sub_tx_load2018/"
    )


def test_bulk_tx_alternate_load_year_uses_sibling_output_base() -> None:
    base = "s3://data.sb/switchbox/marginal_costs/md/bulk_tx"

    assert (
        output_base_for_load_year(base, output_year=2025, load_year=2018)
        == "s3://data.sb/switchbox/marginal_costs/md/bulk_tx_load2018/"
    )


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

    result = top_n_load_share(df, 18.0, 100)

    total_cost = float(result["mc_total_per_kwh"].sum())
    assert abs(total_cost - 18.0) < 0.001, f"Leap year validation failed: {total_cost} != 18.0"
    assert result.height == df.height


def test_different_allocation_windows():
    """Test allocation with different window sizes."""
    df = create_sample_load_profile()

    for n_hours in [200, 50, 10]:
        result_df = top_n_load_share(df, 18.0, n_hours)

        total_cost = float(result_df["mc_total_per_kwh"].sum())
        assert abs(total_cost - 18.0) < 0.001, f"Validation failed for window {n_hours}: {total_cost}"
        assert result_df.filter(pl.col("mc_total_per_kwh") > 0).height == n_hours


def test_pop_timestamp_remap_preserves_allocation() -> None:
    """Remapping 2018->2025 timestamps preserves peak hour ordinals and totals."""
    start = datetime(2018, 1, 1)
    timestamps = [start + timedelta(hours=h) for h in range(8760)]
    loads = [100.0 + (h % 24) * 10.0 + h * 0.001 for h in range(8760)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "load_mw": loads,
            "utility": ["test"] * 8760,
        }
    )

    allocated = top_n_load_share(df, 18.0, 100)

    remapped = remap_year_if_needed(allocated, "timestamp", 2018, 2025)

    years = remapped["timestamp"].dt.year().unique().to_list()
    assert years == [2025]

    peak_orig = allocated.filter(pl.col("mc_total_per_kwh") > 0)
    peak_remap = remapped.filter(pl.col("mc_total_per_kwh") > 0)
    assert peak_remap.height == peak_orig.height

    assert float(remapped["mc_total_per_kwh"].sum()) == pytest.approx(float(allocated["mc_total_per_kwh"].sum()))

    def ordinals(frame: pl.DataFrame) -> list[tuple[int, int, int]]:
        ts = frame.filter(pl.col("mc_total_per_kwh") > 0).sort("timestamp")["timestamp"]
        return [(t.month, t.day, t.hour) for t in ts.to_list()]

    assert ordinals(allocated) == ordinals(remapped)


if __name__ == "__main__":
    test_top_n_load_share_weights_sum_to_one()
    print("✓ test_top_n_load_share_weights_sum_to_one passed")

    test_top_n_load_share_peak_identification()
    print("✓ test_top_n_load_share_peak_identification passed")

    test_top_n_load_share_is_load_weighted()
    print("✓ test_top_n_load_share_is_load_weighted passed")

    test_top_n_load_share_output_column()
    print("✓ test_top_n_load_share_output_column passed")

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
