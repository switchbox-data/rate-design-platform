"""
Tests for rate_design_platform.utils.rates module

Each function in rates.py has a corresponding test_functionname test here.
"""

# Skip all tests in this file if Python < 3.10 (OCHRE requires 3.10+ union syntax)

from datetime import datetime, timedelta

from rate_design_platform.utils.rates import calculate_monthly_intervals  # type: ignore[import-unresolved]


def test_calculate_monthly_intervals():
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    end_time = datetime(2024, 6, 1, 0, 0, 0)
    time_step = timedelta(hours=1)
    intervals = calculate_monthly_intervals(start_time, end_time, time_step)
    assert intervals == [744, 696, 744, 720, 744]

    start_time = datetime(2024, 1, 1, 0, 0, 0)
    end_time = datetime(2024, 1, 2, 0, 0, 0)
    time_step = timedelta(minutes=15)
    intervals = calculate_monthly_intervals(start_time, end_time, time_step)
    assert intervals == [96]

    start_time = datetime(2024, 1, 1, 0, 0, 0)
    end_time = datetime(2024, 1, 1, 0, 0, 0)
    time_step = timedelta(hours=1)
    intervals = calculate_monthly_intervals(start_time, end_time, time_step)
    assert intervals == []
