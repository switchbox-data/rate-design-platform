"""
Tests for rate_design_platform.utils.rates module

Each function in rates.py has a corresponding test_functionname test here.
"""

# Skip all tests in this file if Python < 3.10 (OCHRE requires 3.10+ union syntax)

from datetime import datetime, timedelta

from rate_design_platform.utils.rates import (  # type: ignore[import-unresolved]
    MonthlyRateStructure,
    TOUParameters,
    calculate_monthly_intervals,
    define_peak_hours,
)


def test_calculate_monthly_intervals():
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    end_time = datetime(2024, 6, 1, 0, 0, 0)
    time_step = timedelta(hours=1)
    intervals = calculate_monthly_intervals(start_time, end_time, time_step)

    expected = [
        MonthlyRateStructure(year=2024, month=1, intervals=744),
        MonthlyRateStructure(year=2024, month=2, intervals=696),
        MonthlyRateStructure(year=2024, month=3, intervals=744),
        MonthlyRateStructure(year=2024, month=4, intervals=720),
        MonthlyRateStructure(year=2024, month=5, intervals=744),
    ]

    assert len(intervals) == len(expected)
    for actual, expected_item in zip(intervals, expected):
        assert actual.year == expected_item.year
        assert actual.month == expected_item.month
        assert actual.intervals == expected_item.intervals
        assert len(actual.rates) == 0  # rates should be empty array

    start_time = datetime(2024, 1, 1, 0, 0, 0)
    end_time = datetime(2024, 1, 2, 0, 0, 0)
    time_step = timedelta(minutes=15)
    intervals = calculate_monthly_intervals(start_time, end_time, time_step)
    assert len(intervals) == 1
    assert intervals[0].year == 2024
    assert intervals[0].month == 1
    assert intervals[0].intervals == 96

    start_time = datetime(2024, 1, 1, 0, 0, 0)
    end_time = datetime(2024, 1, 1, 0, 0, 0)
    time_step = timedelta(hours=1)
    intervals = calculate_monthly_intervals(start_time, end_time, time_step)
    assert len(intervals) == 0


def test_define_peak_hours():
    TOU_params = TOUParameters(peak_start_hour=timedelta(hours=12), peak_end_hour=timedelta(hours=20))
    time_step = timedelta(hours=2)
    peak_hours = define_peak_hours(TOU_params, time_step)
    assert peak_hours.tolist() == [False, False, False, False, False, False, True, True, True, True, False, False]
