"""
Tests for rate_design_platform.second_pass module

Each function in second_pass.py has a corresponding test_functionname test here.
"""

# Skip all tests in this file if Python < 3.10 (OCHRE requires 3.10+ union syntax)
import sys

import pytest

if sys.version_info < (3, 10):
    pytest.skip("OCHRE requires Python 3.10+", allow_module_level=True)

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from ochre.utils import default_input_path

from rate_design_platform.second_pass import (
    MonthlyResults,
    SimulationResults,
    TOUParameters,
    calculate_annual_metrics,
    calculate_monthly_bill,
    calculate_monthly_comfort_penalty,
    calculate_monthly_intervals,
    calculate_monthly_metrics,
    calculate_simulation_months,
    create_operation_schedule,
    create_tou_rates,
    define_peak_hours,
    evaluate_human_decision,
    extract_ochre_results,
    human_controller,
    run_full_simulation,
    simulate_full_cycle,
)


@pytest.fixture
def sample_house_args():
    """Provide sample house_args for testing using OCHRE default paths"""
    return {
        "start_time": datetime(2018, 1, 1, 0, 0),
        "end_time": datetime(2018, 12, 31, 23, 59),
        "time_res": timedelta(minutes=15),
        "duration": timedelta(days=365),
        "initialization_time": timedelta(days=1),
        "save_results": False,
        "verbosity": 1,
        "metrics_verbosity": 1,
        "hpxml_file": os.path.join(default_input_path, "Input Files", "bldg0112631-up11.xml"),
        "hpxml_schedule_file": os.path.join(default_input_path, "Input Files", "bldg0112631_schedule.csv"),
        "weather_file": os.path.join(default_input_path, "Weather", "USA_CO_Denver.Intl.AP.725650_TMY3.epw"),
    }


@pytest.fixture
def sample_tou_params():
    """Provide sample TOU parameters for testing"""
    return TOUParameters()


@pytest.fixture
def sample_timesteps():
    """Provide sample timesteps for testing"""
    start = datetime(2018, 1, 1, 0, 0)
    end = datetime(2018, 3, 1, 0, 0)  # Two months
    return pd.date_range(start=start, end=end, freq=timedelta(minutes=15))[:-1]


def test_calculate_monthly_intervals(sample_house_args):
    """Test calculate_monthly_intervals function"""
    start_time = datetime(2018, 1, 1, 0, 0)
    end_time = datetime(2018, 3, 1, 0, 0)  # Two months
    time_step = timedelta(minutes=15)

    intervals = calculate_monthly_intervals(start_time, end_time, time_step)

    assert len(intervals) == 2  # Two months
    assert intervals[0] == 31 * 96  # January: 31 days * 96 intervals/day
    assert intervals[1] == 28 * 96  # February: 28 days * 96 intervals/day

    # Test  case: single day
    start_day = datetime(2018, 1, 1, 0, 0)
    end_day = datetime(2018, 1, 2, 0, 0)
    single_day_intervals = calculate_monthly_intervals(start_day, end_day, time_step)
    assert len(single_day_intervals) == 1
    assert single_day_intervals[0] == 96  # 1 day * 96 intervals/day


def test_calculate_monthly_bill():
    """Test calculate_monthly_bill function"""
    # Create sample simulation results
    time_vals = pd.date_range(start=datetime(2018, 1, 1), periods=192, freq=timedelta(minutes=15))
    consumption = np.array([0.5] * 96 + [0.6] * 96)  # Two days of consumption
    sim_results = SimulationResults(
        Time=time_vals, E_mt=consumption, T_tank_mt=np.ones(192) * 50.0, D_unmet_mt=np.zeros(192)
    )

    # Create monthly rates (two months)
    rates = [
        np.array([0.12] * 96),  # First month off-peak
        np.array([0.28] * 96),  # Second month peak
    ]

    monthly_bills = calculate_monthly_bill(sim_results, rates)

    assert len(monthly_bills) == 2
    assert abs(monthly_bills[0] - 0.5 * 96 * 0.12) < 1e-6  # First month
    assert abs(monthly_bills[1] - 0.6 * 96 * 0.28) < 1e-6  # Second month


def test_calculate_monthly_comfort_penalty(sample_tou_params):
    """Test calculate_monthly_comfort_penalty function"""
    # Create sample simulation results with unmet demand spanning two months
    # January has 31 days, February has 28 days in 2018
    jan_intervals = 31 * 96  # January
    feb_intervals = 28 * 96  # February
    total_intervals = jan_intervals + feb_intervals

    # Start at Jan 1, go through Feb
    time_vals = pd.date_range(start=datetime(2018, 1, 1), periods=total_intervals, freq=timedelta(minutes=15))
    unmet_demand = np.concatenate([
        np.full(jan_intervals, 0.1),  # January unmet demand
        np.full(feb_intervals, 0.2),  # February unmet demand
    ])

    sim_results = SimulationResults(
        Time=time_vals,
        E_mt=np.ones(total_intervals) * 0.5,
        T_tank_mt=np.ones(total_intervals) * 50.0,
        D_unmet_mt=unmet_demand,
    )

    penalties = calculate_monthly_comfort_penalty(sim_results, sample_tou_params)

    assert len(penalties) == 2
    expected_penalty_1 = 0.1 * jan_intervals * 0.15  # January
    expected_penalty_2 = 0.2 * feb_intervals * 0.15  # February
    assert abs(penalties[0] - expected_penalty_1) < 1e-6
    assert abs(penalties[1] - expected_penalty_2) < 1e-6


def test_define_peak_hours(sample_tou_params):
    """Test define_peak_hours function"""
    time_step = timedelta(minutes=15)
    peak_hours = define_peak_hours(sample_tou_params, time_step)

    assert len(peak_hours) == 96  # 24 hours * 4 intervals/hour
    assert np.sum(peak_hours) == 32  # 8 hours * 4 intervals/hour (12:00-20:00)

    # Check specific peak intervals (12:00-20:00)
    assert peak_hours[48]  # 12:00 (48 = 12 * 4)
    assert peak_hours[79]  # 19:45 (79 = 19 * 4 + 3)
    assert not peak_hours[80]  # 20:00 (end of peak)
    assert not peak_hours[0]  # Midnight
    assert not peak_hours[47]  # 11:45

    # Test case: different time step
    time_step_30min = timedelta(minutes=30)
    peak_hours_30 = define_peak_hours(sample_tou_params, time_step_30min)
    assert len(peak_hours_30) == 48  # 24 hours * 2 intervals/hour
    assert np.sum(peak_hours_30) == 16  # 8 hours * 2 intervals/hour

    # Test  case: None params
    peak_hours_none = define_peak_hours(None, time_step)
    assert len(peak_hours_none) == 96
    assert np.sum(peak_hours_none) == 32  # Should use default parameters


def test_create_operation_schedule(sample_tou_params):
    """Test create_operation_schedule function"""
    time_step = timedelta(minutes=15)
    monthly_intervals = [96, 96]  # Two days

    # Test default schedule
    default_schedule = create_operation_schedule("default", monthly_intervals, sample_tou_params, time_step)
    assert len(default_schedule) == 192  # Sum of monthly intervals
    assert np.all(default_schedule)  # Always allowed

    # Test TOU schedule
    tou_schedule = create_operation_schedule("tou", monthly_intervals, sample_tou_params, time_step)
    assert len(tou_schedule) == 192
    assert np.sum(tou_schedule) == 128  # 64 off-peak intervals per day * 2 days

    # Peak hours should be restricted
    peak_hours = define_peak_hours(sample_tou_params, time_step)
    daily_pattern = np.tile(peak_hours, 2)  # Two days
    assert np.all(~tou_schedule[daily_pattern])  # Restricted during peak
    assert np.all(tou_schedule[~daily_pattern])  # Allowed during off-peak


def test_create_tou_rates(sample_timesteps, sample_tou_params):
    """Test create_tou_rates function"""
    time_step = timedelta(minutes=15)
    monthly_rates = create_tou_rates(sample_timesteps, time_step, sample_tou_params)

    assert len(monthly_rates) == 2  # Two months

    # Check that rates are correct
    for month_rates in monthly_rates:
        peak_count = np.sum(month_rates == 0.48)
        offpeak_count = np.sum(month_rates == 0.12)
        assert peak_count + offpeak_count == len(month_rates)

    # Test  case: single day
    single_day_times = pd.date_range(start=datetime(2018, 1, 1), periods=96, freq=timedelta(minutes=15))
    single_rates = create_tou_rates(single_day_times, time_step, sample_tou_params)
    assert len(single_rates) == 1
    assert len(single_rates[0]) == 96

    # Test  case: None params
    rates_none = create_tou_rates(sample_timesteps, time_step, None)
    assert len(rates_none) == 2  # Should use default parameters


def test_extract_ochre_results():
    """Test extract_ochre_results function"""
    # Create sample DataFrame
    time_index = pd.date_range(start=datetime(2018, 1, 1), periods=96, freq=timedelta(minutes=15))
    df = pd.DataFrame(
        {
            "Water Heating Electric Power (kW)": np.ones(96) * 4.0,
            "Hot Water Average Temperature (C)": np.ones(96) * 50.0,
            "Hot Water Unmet Demand (kW)": np.ones(96) * 0.5,
        },
        index=time_index,
    )

    time_step = timedelta(minutes=15)
    results = extract_ochre_results(df, time_step)

    assert isinstance(results, SimulationResults)
    assert len(results.Time) == 96
    assert len(results.E_mt) == 96
    assert len(results.T_tank_mt) == 96
    assert len(results.D_unmet_mt) == 96

    # Check conversion from kW to kWh
    expected_energy = 4.0 * 0.25  # 4 kW * 0.25 hours
    assert np.all(results.E_mt == expected_energy)

    expected_unmet = 0.5 * 0.25  # 0.5 kW * 0.25 hours
    assert np.all(results.D_unmet_mt == expected_unmet)


def test_human_controller(sample_tou_params):
    """Test human_controller function"""
    # Test from default state with positive savings
    decision = human_controller("default", 100.0, 80.0, 5.0, sample_tou_params)
    assert decision == "switch"  # Should switch because net savings positive (20 - 3 = 17 > 0)

    # Test from default state with small savings
    decision = human_controller("default", 100.0, 98.0, 5.0, sample_tou_params)
    assert decision == "stay"  # Should stay because net savings negative (2 - 3 = -1 < 0)

    # Test from TOU state with good realized savings
    decision = human_controller("tou", 100.0, 80.0, 5.0, sample_tou_params)
    assert decision == "stay"  # Should stay because net savings positive (20 - 5 = 15 > 0)

    # Test from TOU state with poor performance (negative savings)
    decision = human_controller("tou", 100.0, 110.0, 5.0, sample_tou_params)
    assert decision == "switch"  # Should switch back because net savings negative (-10 - 5 = -15 < 0)

    # Test case: exactly break-even from default
    decision = human_controller("default", 100.0, 97.0, 5.0, sample_tou_params)
    assert decision == "stay"  # Net savings = 3 - 3 = 0, not > 0, so stay

    # Test case: exactly break-even from TOU
    decision = human_controller("tou", 100.0, 95.0, 5.0, sample_tou_params)
    assert decision == "stay"  # Net savings = 5 - 5 = 0, not < 0, so stay on TOU

    # Test case: zero comfort penalty
    decision = human_controller("tou", 100.0, 80.0, 0.0, sample_tou_params)
    assert decision == "stay"  # Net savings = 20 - 0 = 20 > 0


@pytest.mark.xfail
def test_simulate_full_cycle(sample_house_args, sample_tou_params):
    """Test simulate_full_cycle function"""
    # Reduce simulation size for testing
    test_house_args = sample_house_args.copy()
    test_house_args["duration"] = timedelta(days=31)  # One month
    test_house_args["end_time"] = datetime(2018, 2, 1, 0, 0)

    # Test default simulation
    default_bills, default_penalties = simulate_full_cycle("default", sample_tou_params, test_house_args)

    assert isinstance(default_bills, list)
    assert isinstance(default_penalties, list)
    assert len(default_bills) == 1  # One month
    assert len(default_penalties) == 1
    assert all(bill > 0 for bill in default_bills)
    assert all(penalty >= 0 for penalty in default_penalties)

    # Test TOU simulation
    tou_bills, tou_penalties = simulate_full_cycle("tou", sample_tou_params, test_house_args)

    assert isinstance(tou_bills, list)
    assert isinstance(tou_penalties, list)
    assert len(tou_bills) == 1  # One month
    assert len(tou_penalties) == 1
    assert all(bill > 0 for bill in tou_bills)
    assert all(penalty >= 0 for penalty in tou_penalties)


def test_evaluate_human_decision(sample_tou_params):
    """Test evaluate_human_decision function"""
    initial_state = "default"
    default_bills = [100.0, 110.0, 120.0]
    tou_bills = [80.0, 90.0, 100.0]
    tou_penalties = [5.0, 6.0, 7.0]

    decisions, states = evaluate_human_decision(
        initial_state, default_bills, tou_bills, tou_penalties, sample_tou_params
    )

    assert len(decisions) == 3
    assert len(states) == 3
    assert states[0] == "default"  # Initial state
    assert all(decision in ["switch", "stay"] for decision in decisions)
    assert all(state in ["default", "tou"] for state in states)


def test_calculate_simulation_months(sample_house_args):
    """Test calculate_simulation_months function"""
    # Test with shorter period
    test_house_args = sample_house_args.copy()
    test_house_args["start_time"] = datetime(2018, 1, 1, 0, 0)
    test_house_args["end_time"] = datetime(2018, 4, 1, 0, 0)  # 3 months

    year_months = calculate_simulation_months(test_house_args)

    assert len(year_months) == 3
    assert year_months[0] == (2018, 1)  # January
    assert year_months[1] == (2018, 2)  # February
    assert year_months[2] == (2018, 3)  # March


def test_calculate_monthly_metrics():
    """Test calculate_monthly_metrics function"""
    year_months = [(2018, 1), (2018, 2)]
    decisions = ["switch", "stay"]
    states = ["default", "tou"]
    default_bills = [100.0, 110.0]
    tou_bills = [80.0, 90.0]
    default_penalties = [2.0, 3.0]
    tou_penalties = [5.0, 6.0]

    results = calculate_monthly_metrics(
        year_months, decisions, states, default_bills, tou_bills, default_penalties, tou_penalties
    )

    assert len(results) == 2
    assert all(isinstance(result, MonthlyResults) for result in results)

    # Check first month (default state)
    assert results[0].year == 2018
    assert results[0].month == 1
    assert results[0].current_state == "default"
    assert results[0].bill == 100.0  # Uses default bill
    assert results[0].comfort_penalty == 2.0  # Uses default penalty
    assert results[0].switching_decision == "switch"
    assert results[0].realized_savings == 0  # No realized savings on default
    assert results[0].unrealized_savings == 20.0  # 100 - 80

    # Check second month (TOU state)
    assert results[1].current_state == "tou"
    assert results[1].bill == 90.0  # Uses TOU bill
    assert results[1].comfort_penalty == 6.0  # Uses TOU penalty
    assert results[1].realized_savings == 20.0  # 110 - 90
    assert results[1].unrealized_savings == 0  # No unrealized savings on TOU

    # Test  case: empty inputs
    empty_results = calculate_monthly_metrics([], [], [], [], [], [], [])
    assert len(empty_results) == 0

    # Test  case: single month
    single_results = calculate_monthly_metrics([(2018, 3)], ["stay"], ["default"], [120.0], [95.0], [1.0], [4.0])
    assert len(single_results) == 1
    assert single_results[0].month == 3
    assert single_results[0].unrealized_savings == 25.0  # 120 - 95


def test_calculate_annual_metrics():
    """Test calculate_annual_metrics function"""
    # Create sample monthly results
    monthly_results = []
    for month in range(1, 13):
        result = MonthlyResults(
            year=2018,
            month=month,
            current_state="default" if month % 2 == 1 else "tou",  # Alternate states
            bill=100.0,
            comfort_penalty=5.0,
            switching_decision="switch" if month == 6 else "stay",  # One switch
            realized_savings=10.0 if month % 2 == 0 else 0.0,
            unrealized_savings=15.0 if month % 2 == 1 else 0.0,
        )
        monthly_results.append(result)

    metrics = calculate_annual_metrics(monthly_results)

    assert metrics["total_annual_bills"] == 1200.0  # 12 * 100
    assert metrics["total_comfort_penalty"] == 60.0  # 12 * 5
    assert metrics["annual_switches"] == 1
    assert metrics["total_switching_costs"] == 3.0  # 1 * 3
    assert metrics["average_monthly_bill"] == 100.0
    assert metrics["tou_adoption_rate_percent"] == 50.0  # 6 months TOU
    assert metrics["total_realized_savings"] == 60.0  # 6 TOU months * 10


@pytest.mark.xfail
def test_run_full_simulation(sample_house_args, sample_tou_params):
    """Test run_full_simulation function"""
    # Reduce simulation size for testing
    test_house_args = sample_house_args.copy()
    test_house_args["duration"] = timedelta(days=61)  # Two months
    test_house_args["end_time"] = datetime(2018, 3, 1, 0, 0)

    monthly_results, annual_metrics = run_full_simulation(sample_tou_params, test_house_args)

    # Check structure if simulation succeeds
    assert isinstance(monthly_results, list)
    assert isinstance(annual_metrics, dict)
    assert len(monthly_results) == 2  # Two months
    assert all(isinstance(result, MonthlyResults) for result in monthly_results)

    # Check annual metrics keys
    expected_keys = [
        "total_annual_bills",
        "total_comfort_penalty",
        "total_switching_costs",
        "total_realized_savings",
        "net_annual_benefit",
        "tou_adoption_rate_percent",
        "annual_switches",
        "average_monthly_bill",
    ]
    for key in expected_keys:
        assert key in annual_metrics
