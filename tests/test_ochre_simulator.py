"""
Tests for rate_design_platform.OchreSimulator module

Tests for OCHRE simulation functions.
"""

import os
from datetime import datetime, timedelta

import numpy as np
import pytest
from ochre import Dwelling
from ochre.utils import default_input_path

from rate_design_platform.Analysis import SimulationResults
from rate_design_platform.OchreSimulator import (
    calculate_simulation_months,
    run_ochre_wh_dynamic_control,
)


@pytest.fixture
def sample_dwelling():
    """Create a sample OCHRE dwelling for testing"""
    house_args = {
        "start_time": datetime(2018, 1, 1, 0, 0),
        "end_time": datetime(2018, 1, 2, 0, 0),  # One day for fast testing
        "time_res": timedelta(minutes=15),
        "duration": timedelta(days=1),
        "initialization_time": timedelta(hours=1),
        "save_results": False,
        "verbosity": 9,
        "metrics_verbosity": 7,
        "hpxml_file": os.path.join(default_input_path, "Input Files", "bldg0112631-up11.xml"),
        "hpxml_schedule_file": os.path.join(default_input_path, "Input Files", "bldg0112631_schedule.csv"),
        "weather_file": os.path.join(default_input_path, "Weather", "USA_CO_Denver.Intl.AP.725650_TMY3.epw"),
    }
    return Dwelling(**house_args)


@pytest.fixture
def sample_operation_schedule():
    """Create a sample operation schedule for testing"""
    # 96 intervals for one day at 15-minute intervals
    schedule = np.ones(96, dtype=bool)  # All allowed initially
    # Restrict during peak hours (12 PM to 8 PM = intervals 48-79)
    schedule[48:80] = False
    return schedule


def test_run_ochre_wh_dynamic_control(sample_dwelling, sample_operation_schedule):
    """Test run_ochre_wh_dynamic_control function with various schedule scenarios"""
    time_step = timedelta(minutes=15)

    # Test 1: Basic schedule (provided by fixture)
    results = run_ochre_wh_dynamic_control(sample_dwelling, sample_operation_schedule, time_step)

    # Check that results are returned as SimulationResults
    assert isinstance(results, SimulationResults)

    # Check that all arrays have the same length
    assert len(results.Time) == len(results.E_mt)
    assert len(results.Time) == len(results.T_tank_mt)
    assert len(results.Time) == len(results.D_unmet_mt)

    # Check array types
    assert isinstance(results.Time, np.ndarray)
    assert isinstance(results.E_mt, np.ndarray)
    assert isinstance(results.T_tank_mt, np.ndarray)
    assert isinstance(results.D_unmet_mt, np.ndarray)

    # Check that electricity consumption is non-negative
    assert all(e >= 0 for e in results.E_mt)

    # Check that unmet demand is non-negative
    assert all(d >= 0 for d in results.D_unmet_mt)

    # Check that tank temperature is reasonable (should be between 0 and 100°C)
    assert all(0 <= t <= 100 for t in results.T_tank_mt)

    # Test 2: All allowed schedule
    all_allowed_schedule = np.ones(96, dtype=bool)
    results_all_allowed = run_ochre_wh_dynamic_control(sample_dwelling, all_allowed_schedule, time_step)
    assert isinstance(results_all_allowed, SimulationResults)
    assert len(results_all_allowed.Time) == 96

    # Test 3: All restricted schedule
    all_restricted_schedule = np.zeros(96, dtype=bool)
    results_all_restricted = run_ochre_wh_dynamic_control(sample_dwelling, all_restricted_schedule, time_step)
    assert isinstance(results_all_restricted, SimulationResults)
    assert len(results_all_restricted.Time) == 96
    # Should have higher unmet demand when all restricted
    assert np.sum(results_all_restricted.D_unmet_mt) >= np.sum(results_all_allowed.D_unmet_mt)

    # Test 4: Mixed schedule with different pattern
    mixed_schedule = np.ones(96, dtype=bool)
    mixed_schedule[0:24] = False  # First 6 hours restricted
    mixed_schedule[72:96] = False  # Last 6 hours restricted
    results_mixed = run_ochre_wh_dynamic_control(sample_dwelling, mixed_schedule, time_step)
    assert isinstance(results_mixed, SimulationResults)
    assert len(results_mixed.Time) == 96


def test_calculate_simulation_months():
    """Test calculate_simulation_months function with various date ranges"""
    # Test 1: Three months
    house_args_3_months = {
        "start_time": datetime(2018, 1, 1, 0, 0),
        "end_time": datetime(2018, 4, 1, 0, 0),
    }

    year_months = calculate_simulation_months(house_args_3_months)
    expected = [(2018, 1), (2018, 2), (2018, 3)]
    assert year_months == expected

    # Check types
    assert isinstance(year_months, list)
    for year_month in year_months:
        assert isinstance(year_month, tuple)
        assert len(year_month) == 2
        year, month = year_month
        assert isinstance(year, int)
        assert isinstance(month, int)
        assert 1 <= month <= 12
        assert year > 0

    # Test 2: Single month
    house_args_single = {
        "start_time": datetime(2018, 6, 1, 0, 0),
        "end_time": datetime(2018, 7, 1, 0, 0),
    }
    single_month = calculate_simulation_months(house_args_single)
    assert single_month == [(2018, 6)]

    # Test 3: Multiple months across year boundary
    house_args_year_boundary = {
        "start_time": datetime(2018, 11, 1, 0, 0),
        "end_time": datetime(2019, 2, 1, 0, 0),
    }
    cross_year = calculate_simulation_months(house_args_year_boundary)
    expected_cross = [(2018, 11), (2018, 12), (2019, 1)]
    assert cross_year == expected_cross

    # Test 4: Full year
    house_args_full_year = {
        "start_time": datetime(2018, 1, 1, 0, 0),
        "end_time": datetime(2019, 1, 1, 0, 0),
    }
    full_year = calculate_simulation_months(house_args_full_year)
    expected_full = [(2018, month) for month in range(1, 13)]
    assert full_year == expected_full
    assert len(full_year) == 12
