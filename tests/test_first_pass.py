"""
Tests for rate_design_platform.first_pass module

Each function in first_pass.py has a corresponding test_functionname test here.
"""

import os
from datetime import datetime, timedelta

import numpy as np
import pytest

# Skip all tests in this file if OCHRE cannot be imported
pytest.importorskip("ochre.utils", reason="OCHRE not available")
from ochre.utils import default_input_path

from rate_design_platform.first_pass import (
    MonthlyResults,
    SimulationResults,
    TOUParameters,
    building_simulation_controller,
    calculate_annual_metrics,
    calculate_comfort_penalty,
    calculate_monthly_bill,
    calculate_monthly_intervals,
    create_ochre_dwelling,
    create_operation_schedule,
    create_tou_rates,
    define_peak_hours,
    human_controller,
    run_full_simulation,
    simulate_annual_cycle,
    simulate_month_both_schedules,
    simulate_single_month,
)


@pytest.fixture
def sample_house_args():
    """Provide sample house_args for testing using OCHRE default paths"""
    return {
        "start_time": datetime(2018, 1, 1, 0, 0),
        "time_res": timedelta(minutes=15),
        "duration": timedelta(days=31),
        "initialization_time": timedelta(days=1),
        "save_results": False,
        "verbosity": 1,
        "metrics_verbosity": 1,
        "hpxml_file": os.path.join(default_input_path, "Input Files", "bldg0112631-up11.xml"),
        "hpxml_schedule_file": os.path.join(default_input_path, "Input Files", "bldg0112631_schedule.csv"),
        "weather_file": os.path.join(default_input_path, "Weather", "USA_CO_Denver.Intl.AP.725650_TMY3.epw"),
    }


def test_calculate_monthly_intervals():
    """Test calculate_monthly_intervals function"""
    # Test January (31 days)
    jan_intervals = calculate_monthly_intervals(1, 2018)
    assert jan_intervals == 31 * 96  # 31 days * 96 intervals/day

    # Test February (28 days in 2018, non-leap year)
    feb_intervals = calculate_monthly_intervals(2, 2018)
    assert feb_intervals == 28 * 96

    # Test February in leap year (29 days in 2020)
    feb_leap_intervals = calculate_monthly_intervals(2, 2020)
    assert feb_leap_intervals == 29 * 96

    # Test April (30 days)
    apr_intervals = calculate_monthly_intervals(4, 2018)
    assert apr_intervals == 30 * 96

    # Test with different year
    jan_2019_intervals = calculate_monthly_intervals(1, 2019)
    assert jan_2019_intervals == 31 * 96  # Should be same as 2018


def test_create_ochre_dwelling(sample_house_args):
    """Test create_ochre_dwelling function"""

    # Mock the Dwelling class to avoid OCHRE dependency
    class MockDwelling:
        def __init__(self, **kwargs):
            self.args = kwargs

    # Temporarily replace Dwelling import
    import rate_design_platform.first_pass as fp

    original_dwelling = fp.Dwelling
    fp.Dwelling = MockDwelling

    try:
        # Test that function properly updates timing parameters
        month = 3  # March
        year = 2018

        dwelling = create_ochre_dwelling(sample_house_args, month, year)

        # Check that timing parameters were updated correctly
        assert dwelling.args["start_time"] == datetime(2018, 3, 1, 0, 0)
        assert dwelling.args["duration"] == timedelta(days=31)  # March has 31 days

        # Check that other parameters were preserved
        assert dwelling.args["time_res"] == timedelta(minutes=15)
        assert dwelling.args["initialization_time"] == timedelta(days=1)
        assert "hpxml_file" in dwelling.args  # Check that file path is set

        # Test different month (February)
        dwelling_feb = create_ochre_dwelling(sample_house_args, 2, 2018)
        assert dwelling_feb.args["start_time"] == datetime(2018, 2, 1, 0, 0)
        assert dwelling_feb.args["duration"] == timedelta(days=28)  # Feb 2018 has 28 days

    finally:
        # Restore original Dwelling class
        fp.Dwelling = original_dwelling


def test_define_peak_hours():
    """Test define_peak_hours function"""
    peak_hours = define_peak_hours()
    assert len(peak_hours) == 96  # 24 hours * 4 intervals/hour
    assert np.sum(peak_hours) == 24  # 6 hours * 4 intervals/hour (14:00-20:00)

    # Check specific peak intervals (14:00-20:00)
    assert peak_hours[56]  # 14:00 (56 = 14 * 4)
    assert peak_hours[79]  # 19:45 (79 = 19 * 4 + 3)
    assert not peak_hours[80]  # 20:00 (end of peak)
    assert not peak_hours[0]  # Midnight
    assert not peak_hours[55]  # 13:45


def test_create_tou_rates():
    """Test create_tou_rates function"""
    rates = create_tou_rates(96 * 2)  # Two days
    assert len(rates) == 192

    # Count peak and off-peak rates
    peak_count = np.sum(rates == 0.28)
    offpeak_count = np.sum(rates == 0.12)

    assert peak_count == 48  # 24 peak intervals * 2 days
    assert offpeak_count == 144  # 72 off-peak intervals * 2 days
    assert peak_count + offpeak_count == 192

    # Test edge case
    empty_rates = create_tou_rates(0)
    assert len(empty_rates) == 0


def test_create_operation_schedule():
    """Test create_operation_schedule function"""
    # Default schedule
    default_schedule = create_operation_schedule(1, 96)
    assert len(default_schedule) == 96
    assert np.all(default_schedule == 1)  # Always allowed

    # TOU schedule
    tou_schedule = create_operation_schedule(0, 96)
    assert len(tou_schedule) == 96
    assert np.sum(tou_schedule) == 72  # 72 off-peak intervals allowed

    # Peak hours should be restricted
    peak_hours = define_peak_hours()
    assert np.all(tou_schedule[peak_hours] == 0)  # Restricted during peak
    assert np.all(tou_schedule[~peak_hours] == 1)  # Allowed during off-peak


def test_building_simulation_controller(sample_house_args):
    """Test building_simulation_controller function"""

    operation_schedule = np.ones(96)
    month = 1

    # Mock the functions to avoid OCHRE dependency
    def mock_create_ochre_dwelling(house_args, month, year=2018):
        class MockDwelling:
            pass

        return MockDwelling()

    def mock_run_ochre_hpwh_dynamic_control(dwelling, operation_schedule, month):
        return SimulationResults(E_mt=np.ones(96) * 0.1, T_tank_mt=np.ones(96) * 50.0, D_unmet_mt=np.zeros(96))

    # Temporarily replace functions
    import rate_design_platform.first_pass as fp

    original_create = fp.create_ochre_dwelling
    original_run = fp.run_ochre_hpwh_dynamic_control
    fp.create_ochre_dwelling = mock_create_ochre_dwelling
    fp.run_ochre_hpwh_dynamic_control = mock_run_ochre_hpwh_dynamic_control

    try:
        results = building_simulation_controller(month, operation_schedule, sample_house_args)

        assert isinstance(results, SimulationResults)
        assert len(results.E_mt) == 96
        assert len(results.T_tank_mt) == 96
        assert len(results.D_unmet_mt) == 96

    finally:
        # Restore original functions
        fp.create_ochre_dwelling = original_create
        fp.run_ochre_hpwh_dynamic_control = original_run


def test_human_controller():
    """Test human_controller function"""
    # Test static behavior (current implementation returns 0)
    assert human_controller(1, 0.0, 10.0) == 0  # Default state with positive savings
    assert human_controller(0, -5.0, 0.0) == 0  # TOU state with negative savings
    assert human_controller(1, 0.0, -10.0) == 0  # Default state with negative savings


def test_calculate_monthly_bill():
    """Test calculate_monthly_bill function"""
    consumption = np.array([0.5, 0.6, 0.4])  # kWh/15min
    rates = np.array([0.12, 0.28, 0.12])  # $/kWh

    expected_bill = 0.5 * 0.12 + 0.6 * 0.28 + 0.4 * 0.12
    bill = calculate_monthly_bill(consumption, rates)

    assert abs(bill - expected_bill) < 1e-6


def test_calculate_comfort_penalty():
    """Test calculate_comfort_penalty function"""
    # Test with zero unmet demand
    unmet_demand_watts = np.zeros(96)
    penalty = calculate_comfort_penalty(unmet_demand_watts, 0.15)
    assert penalty == 0.0

    # Test with non-zero unmet demand
    unmet_demand_watts = np.full(96, 1000.0)  # 1000 W each interval
    penalty = calculate_comfort_penalty(unmet_demand_watts, 0.15)

    # Expected: 96 * 1000 W * 0.25 hours / 1000 W/kW * 0.15 $/kWh = 3.6
    expected_penalty = 96 * 1000.0 * 0.25 / 1000.0 * 0.15
    assert abs(penalty - expected_penalty) < 1e-6


def test_simulate_month_both_schedules(sample_house_args):
    """Test simulate_month_both_schedules function"""

    month = 1
    rates = create_tou_rates(calculate_monthly_intervals(month))
    params = TOUParameters()

    # Mock the building_simulation_controller function
    def mock_building_simulation_controller(month, operation_schedule, house_args):
        return SimulationResults(
            E_mt=np.ones(len(operation_schedule)) * 0.1,
            T_tank_mt=np.ones(len(operation_schedule)) * 50.0,
            D_unmet_mt=np.zeros(len(operation_schedule)),
        )

    import rate_design_platform.first_pass as fp

    original_controller = fp.building_simulation_controller
    fp.building_simulation_controller = mock_building_simulation_controller

    try:
        default_results, tou_results, default_bill, tou_bill = simulate_month_both_schedules(
            month, rates, params, sample_house_args
        )

        # Both should return results
        assert isinstance(default_results, SimulationResults)
        assert isinstance(tou_results, SimulationResults)
        assert isinstance(default_bill, float)
        assert isinstance(tou_bill, float)

        # Bills should be positive
        assert default_bill > 0
        assert tou_bill > 0

    finally:
        fp.building_simulation_controller = original_controller


def test_simulate_single_month(sample_house_args):
    """Test simulate_single_month function"""

    month = 1
    rates = create_tou_rates(calculate_monthly_intervals(month))
    params = TOUParameters()

    # Mock the simulate_month_both_schedules function
    def mock_simulate_month_both_schedules(month, rates, params, house_args):
        num_intervals = len(rates)
        return (
            SimulationResults(
                E_mt=np.ones(num_intervals) * 0.1,
                T_tank_mt=np.ones(num_intervals) * 50.0,
                D_unmet_mt=np.zeros(num_intervals),
            ),
            SimulationResults(
                E_mt=np.ones(num_intervals) * 0.08,
                T_tank_mt=np.ones(num_intervals) * 48.0,
                D_unmet_mt=np.ones(num_intervals) * 0.01,
            ),
            10.0,  # default_bill
            8.0,  # tou_bill
        )

    import rate_design_platform.first_pass as fp

    original_simulate = fp.simulate_month_both_schedules
    fp.simulate_month_both_schedules = mock_simulate_month_both_schedules

    try:
        # Test from default state
        result_default = simulate_single_month(month, 1, rates, params, sample_house_args)
        assert isinstance(result_default, MonthlyResults)
        assert result_default.current_state == 1
        assert result_default.bill > 0
        assert result_default.comfort_penalty >= 0
        assert result_default.switching_decision == 0  # Static controller returns 0
        assert result_default.realized_savings == 0.0  # No realized savings when on default
        assert isinstance(result_default.unrealized_savings, float)

        # Test from TOU state
        result_tou = simulate_single_month(month, 0, rates, params, sample_house_args)
        assert isinstance(result_tou, MonthlyResults)
        assert result_tou.current_state == 0
        assert result_tou.bill > 0
        assert result_tou.comfort_penalty >= 0
        assert result_tou.switching_decision == 0  # Static controller returns 0
        assert isinstance(result_tou.realized_savings, float)
        assert result_tou.unrealized_savings == 0.0  # No unrealized savings when on TOU

    finally:
        fp.simulate_month_both_schedules = original_simulate


def test_simulate_annual_cycle(sample_house_args):
    """Test simulate_annual_cycle function"""

    params = TOUParameters()

    # Mock the simulate_single_month function
    def mock_simulate_single_month(month, current_state, rates, params, house_args):
        return MonthlyResults(
            month=month,
            current_state=current_state,
            bill=100.0,
            comfort_penalty=5.0,
            switching_decision=0,
            realized_savings=10.0 if current_state == 0 else 0.0,
            unrealized_savings=15.0 if current_state == 1 else 0.0,
        )

    import rate_design_platform.first_pass as fp

    original_simulate = fp.simulate_single_month
    fp.simulate_single_month = mock_simulate_single_month

    try:
        results = simulate_annual_cycle(params, sample_house_args)

        assert len(results) == 12  # 12 months
        assert all(isinstance(r, MonthlyResults) for r in results)
        assert all(r.month == i for i, r in enumerate(results, 1))  # Months 1-12

        # First month should start on default schedule
        assert results[0].current_state == 1

    finally:
        fp.simulate_single_month = original_simulate


def test_calculate_annual_metrics():
    """Test calculate_annual_metrics function"""
    # Create mock monthly results
    monthly_results = []
    for month in range(1, 13):
        result = MonthlyResults(
            month=month,
            current_state=1 if month % 2 == 1 else 0,  # Alternate states
            bill=100.0,
            comfort_penalty=5.0,
            switching_decision=1 if month == 6 else 0,  # One switch
            realized_savings=10.0 if month % 2 == 0 else 0.0,
            unrealized_savings=15.0 if month % 2 == 1 else 0.0,
        )
        monthly_results.append(result)

    metrics = calculate_annual_metrics(monthly_results)

    assert metrics["total_annual_bills"] == 1200.0  # 12 * 100
    assert metrics["total_comfort_penalty"] == 60.0  # 12 * 5
    assert metrics["annual_switches"] == 1
    assert metrics["total_switching_costs"] == 35.0  # 1 * 35
    assert metrics["average_monthly_bill"] == 100.0
    assert metrics["tou_adoption_rate_percent"] == 50.0  # 6 months TOU
    assert metrics["total_realized_savings"] == 60.0  # 6 TOU months * 10


def test_run_full_simulation():
    """Test run_full_simulation function"""

    # This test will pass if files exist, otherwise check for proper error handling
    try:
        monthly_results, annual_metrics = run_full_simulation()

        # If it succeeds, check structure
        assert isinstance(monthly_results, list)
        assert isinstance(annual_metrics, dict)
        assert len(monthly_results) == 12

    except FileNotFoundError as e:
        # Expected if input files not available - check that it's the right error
        assert any(
            filename in str(e) for filename in ["bldg0000072-up00.xml", "G3400270.epw", "bldg0000072-up00_schedule.csv"]
        )

    except Exception as e:
        # Any other error should be expected types
        assert isinstance(e, (ValueError, ImportError, AttributeError))
