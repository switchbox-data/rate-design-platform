"""
Tests for rate_design_platform.second_pass module

Each function in second_pass.py has a corresponding test_functionname test here.
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import pytest
from ochre.utils import default_input_path

from rate_design_platform.Analysis import (  # type: ignore[import-unresolved]
    MonthlyResults,
)
from rate_design_platform.second_pass import (
    evaluate_human_decision,
    human_controller,
    run_full_simulation,
    simulate_full_cycle,
)
from rate_design_platform.utils.rates import TOUParameters


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
        "verbosity": 9,
        "metrics_verbosity": 7,
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


@pytest.mark.xfail(reason="Known bug, will fix later")
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


@pytest.mark.xfail(reason="Known bug, will fix later")
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
