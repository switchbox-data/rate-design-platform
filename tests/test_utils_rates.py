"""
Tests for rate_design_platform.utils.rates module

Each function in rates.py has a corresponding test_functionname test here.
"""

# Skip all tests in this file if Python < 3.10 (OCHRE requires 3.10+ union syntax)

from datetime import datetime, timedelta

import numpy as np

from rate_design_platform.utils.rates import (  # type: ignore[import-unresolved]
    MonthlyRateStructure,
    TOUParameters,
    calculate_monthly_intervals,
    create_building_dependent_tou_params,
    create_operation_schedule,
    create_tou_rates,
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


def test_create_tou_rates():
    TOU_params = TOUParameters(peak_start_hour=timedelta(hours=12), peak_end_hour=timedelta(hours=20))
    time_step = timedelta(hours=2)
    timesteps = np.array([datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 2, 0, 0)])
    rates = create_tou_rates(timesteps, time_step, TOU_params)
    assert rates[0].rates.tolist() == [0.12, 0.12]

    TOU_params = TOUParameters(peak_start_hour=timedelta(hours=12), peak_end_hour=timedelta(hours=20))
    time_step = timedelta(hours=2)
    timesteps = np.arange(datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 2, 0, 0, 0), time_step)
    rates = create_tou_rates(timesteps, time_step, TOU_params)
    assert rates[0].rates.tolist() == [0.12, 0.12, 0.12, 0.12, 0.12, 0.12, 0.48, 0.48, 0.48, 0.48, 0.12, 0.12]


def test_create_operation_schedule():
    """Test create_operation_schedule function"""
    time_step = timedelta(minutes=15)
    monthly_rates = [
        MonthlyRateStructure(year=2024, month=1, intervals=96),
        MonthlyRateStructure(year=2024, month=2, intervals=96),
    ]  # Two months
    TOU_params = TOUParameters(peak_start_hour=timedelta(hours=12), peak_end_hour=timedelta(hours=20))

    # Test default schedule
    default_schedule = create_operation_schedule("default", monthly_rates, TOU_params, time_step)
    assert len(default_schedule) == 192  # Sum of monthly intervals
    assert np.all(default_schedule)  # Always allowed

    # Test TOU schedule
    tou_schedule = create_operation_schedule("tou", monthly_rates, TOU_params, time_step)
    assert len(tou_schedule) == 192
    assert np.sum(tou_schedule) == 128  # 64 off-peak intervals per day * 2 days

    # Peak hours should be restricted
    peak_hours = define_peak_hours(TOU_params, time_step)
    daily_pattern = np.tile(peak_hours, 2)  # Two days
    assert np.all(~tou_schedule[daily_pattern])  # Restricted during peak
    assert np.all(tou_schedule[~daily_pattern])  # Allowed during off-peak


def test_tou_parameters_defaults():
    """Test TOUParameters class with default values."""
    params = TOUParameters()

    assert params.r_on == 0.48
    assert params.r_off == 0.12
    assert params.c_switch == 3.0
    assert params.c_switch_to is None
    assert params.c_switch_back is None
    assert params.alpha is None
    assert params.peak_start_hour == timedelta(hours=12)
    assert params.peak_end_hour == timedelta(hours=20)


def test_tou_parameters_get_switching_cost_to():
    """Test get_switching_cost_to method."""
    # Test with building-dependent cost
    params = TOUParameters(c_switch_to=45.0)
    assert params.get_switching_cost_to() == 45.0

    # Test fallback to legacy cost
    params = TOUParameters(c_switch_to=None, c_switch=25.0)
    assert params.get_switching_cost_to() == 25.0


def test_tou_parameters_get_switching_cost_back():
    """Test get_switching_cost_back method."""
    # Test with building-dependent cost
    params = TOUParameters(c_switch_back=18.0)
    assert params.get_switching_cost_back() == 18.0

    # Test fallback calculation (0.4 * switch_to_cost)
    params = TOUParameters(c_switch_to=50.0, c_switch_back=None)
    assert params.get_switching_cost_back() == 20.0  # 0.4 * 50

    # Test fallback to legacy cost
    params = TOUParameters(c_switch_to=None, c_switch_back=None, c_switch=30.0)
    assert params.get_switching_cost_back() == 12.0  # 0.4 * 30


def test_tou_parameters_get_comfort_penalty_factor():
    """Test get_comfort_penalty_factor method."""
    # Test with building-dependent alpha
    params = TOUParameters(alpha=0.25)
    assert params.get_comfort_penalty_factor() == 0.25

    # Test fallback to default
    params = TOUParameters(alpha=None)
    assert params.get_comfort_penalty_factor() == 0.15


def test_create_building_dependent_tou_params():
    """Test creation of building-dependent TOU parameters."""
    xml_path = "rate_design_platform/inputs/bldg0000072-up00.xml"
    params = create_building_dependent_tou_params(xml_path)

    # Should have building-specific values
    assert params.c_switch_to is not None
    assert params.c_switch_back is not None
    assert params.alpha is not None

    # Should maintain base rates
    assert params.r_on == 0.48
    assert params.r_off == 0.12

    # Building-dependent costs should be different from defaults
    assert params.get_switching_cost_to() != 3.0
    assert params.get_comfort_penalty_factor() != 0.15

    # Switch-back should be 40% of switch-to
    expected_switch_back = 0.4 * params.c_switch_to
    assert abs(params.c_switch_back - expected_switch_back) < 0.01


def test_create_building_dependent_tou_params_with_base():
    """Test creation with custom base parameters."""
    base_params = TOUParameters(r_on=0.60, r_off=0.08)
    xml_path = "rate_design_platform/inputs/bldg0000072-up00.xml"

    params = create_building_dependent_tou_params(xml_path, base_params)

    # Should use custom base rates
    assert params.r_on == 0.60
    assert params.r_off == 0.08

    # Should still have building-specific costs
    assert params.c_switch_to is not None
    assert params.c_switch_back is not None
    assert params.alpha is not None


def test_tou_parameters_with_all_building_dependent_values():
    """Test TOUParameters with all building-dependent values set."""
    params = TOUParameters(
        r_on=0.50,
        r_off=0.10,
        c_switch_to=42.5,
        c_switch_back=17.0,
        alpha=0.22,
        c_switch=5.0,  # Legacy value
    )

    # Should use building-dependent values
    assert params.get_switching_cost_to() == 42.5
    assert params.get_switching_cost_back() == 17.0
    assert params.get_comfort_penalty_factor() == 0.22

    # Legacy value should still be accessible
    assert params.c_switch == 5.0
