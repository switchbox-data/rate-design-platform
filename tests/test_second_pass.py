"""
Tests for rate_design_platform.second_pass module

Tests for refactored functions in the value learning simulation.
"""

import os
from datetime import datetime, timedelta

import pytest
from ochre.utils import default_input_path

from rate_design_platform.Analysis import MonthlyMetrics, ValueLearningResults
from rate_design_platform.second_pass import (
    evaluate_value_learning_decision,
    run_value_learning_simulation,
    simulate_full_cycle,
)
from rate_design_platform.utils.rates import TOUParameters


@pytest.fixture
def sample_house_args():
    """Provide sample house_args for testing using OCHRE default paths"""
    return {
        "start_time": datetime(2018, 1, 1, 0, 0),
        "end_time": datetime(2018, 2, 1, 0, 0),  # One month for faster testing
        "time_res": timedelta(minutes=15),
        "duration": timedelta(days=31),
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


def test_simulate_full_cycle(sample_house_args, sample_tou_params):
    """Test simulate_full_cycle function"""
    # Test default simulation
    default_monthly_bills, default_monthly_unmet_demand = simulate_full_cycle(
        "default", sample_tou_params, sample_house_args
    )

    assert isinstance(default_monthly_bills, list)
    assert isinstance(default_monthly_unmet_demand, list)
    assert len(default_monthly_bills) == 1  # One month
    assert len(default_monthly_unmet_demand) == 1  # One month
    assert all(isinstance(bill, MonthlyMetrics) for bill in default_monthly_bills)
    assert all(bill.bill > 0 for bill in default_monthly_bills)
    assert all(isinstance(demand, float) for demand in default_monthly_unmet_demand)
    assert all(demand >= 0 for demand in default_monthly_unmet_demand)

    # Test TOU simulation
    tou_monthly_bills, tou_monthly_unmet_demand = simulate_full_cycle("tou", sample_tou_params, sample_house_args)

    assert isinstance(tou_monthly_bills, list)
    assert isinstance(tou_monthly_unmet_demand, list)
    assert len(tou_monthly_bills) == 1  # One month
    assert len(tou_monthly_unmet_demand) == 1  # One month
    assert all(isinstance(bill, MonthlyMetrics) for bill in tou_monthly_bills)
    assert all(bill.bill > 0 for bill in tou_monthly_bills)
    assert all(isinstance(demand, float) for demand in tou_monthly_unmet_demand)
    assert all(demand >= 0 for demand in tou_monthly_unmet_demand)


def test_evaluate_value_learning_decision():
    """Test evaluate_value_learning_decision function"""
    from rate_design_platform.DecisionMaker import ValueLearningController
    from rate_design_platform.utils.building_characteristics import BuildingCharacteristics
    from rate_design_platform.utils.value_learning_params import ValueLearningParameters

    # Create test controller
    params = ValueLearningParameters()
    building_chars = BuildingCharacteristics(
        state_code="CO",
        zip_code="80202",
        year_built=1980,
        n_residents=2.5,
        n_bedrooms=3,
        conditioned_floor_area=1500.0,
        residential_facility_type="single-family detached",
        climate_zone="5B",
        ami=1.0,
        wh_type="storage",
    )
    controller = ValueLearningController(params, building_chars)

    # Test data for 3 months
    default_monthly_bill = [100.0, 110.0, 120.0]
    tou_monthly_bill = [80.0, 90.0, 100.0]
    default_monthly_unmet_demand = [2.0, 3.0, 4.0]
    tou_monthly_unmet_demand = [5.0, 6.0, 7.0]

    decisions, states, learning_metrics_history = evaluate_value_learning_decision(
        controller, default_monthly_bill, tou_monthly_bill, default_monthly_unmet_demand, tou_monthly_unmet_demand
    )

    assert len(decisions) == 3
    assert len(states) == 3
    assert len(learning_metrics_history) == 3
    assert states[0] == "default"  # Initial state
    assert all(decision in ["switch", "stay"] for decision in decisions)
    assert all(state in ["default", "tou"] for state in states)
    assert all(isinstance(metrics, dict) for metrics in learning_metrics_history)

    # Check learning metrics structure
    for metrics in learning_metrics_history:
        expected_keys = ["v_default", "v_tou", "epsilon_m", "alpha_m_learn", "value_difference"]
        for key in expected_keys:
            assert key in metrics


def test_run_value_learning_simulation(sample_house_args, sample_tou_params):
    """Test run_value_learning_simulation function"""
    # Reduce simulation size for testing
    test_house_args = sample_house_args.copy()
    test_house_args["duration"] = timedelta(days=61)  # Two months
    test_house_args["end_time"] = datetime(2018, 3, 1, 0, 0)

    monthly_results, annual_metrics, learning_metrics_history = run_value_learning_simulation(
        sample_tou_params, test_house_args
    )

    # Check monthly results structure
    assert isinstance(monthly_results, list)
    assert len(monthly_results) == 2  # Two months
    assert all(isinstance(result, ValueLearningResults) for result in monthly_results)

    # Check annual metrics structure
    assert isinstance(annual_metrics, dict)
    expected_annual_keys = [
        "total_annual_bills",
        "total_comfort_penalty",
        "total_realized_savings",
        "net_annual_benefit",
        "tou_adoption_rate_percent",
        "average_monthly_bill",
        "exploration_rate_percent",
        "final_value_difference",
        "final_v_default",
        "final_v_tou",
    ]
    for key in expected_annual_keys:
        assert key in annual_metrics
        assert isinstance(annual_metrics[key], (int, float))

    # Check learning metrics history
    assert isinstance(learning_metrics_history, list)
    assert len(learning_metrics_history) == 2  # Two months
    assert all(isinstance(metrics, dict) for metrics in learning_metrics_history)


def test_run_value_learning_simulation_with_building_xml(sample_tou_params):
    """Test run_value_learning_simulation with explicit building XML path"""
    # Create minimal house args for faster testing
    house_args = {
        "start_time": datetime(2018, 1, 1, 0, 0),
        "end_time": datetime(2018, 2, 1, 0, 0),  # One month
        "time_res": timedelta(minutes=15),
        "duration": timedelta(days=31),
        "initialization_time": timedelta(days=1),
        "save_results": False,
        "verbosity": 9,
        "metrics_verbosity": 7,
        "hpxml_file": os.path.join(default_input_path, "Input Files", "bldg0112631-up11.xml"),
        "hpxml_schedule_file": os.path.join(default_input_path, "Input Files", "bldg0112631_schedule.csv"),
        "weather_file": os.path.join(default_input_path, "Weather", "USA_CO_Denver.Intl.AP.725650_TMY3.epw"),
    }

    building_xml_path = os.path.join(default_input_path, "Input Files", "bldg0112631-up11.xml")

    monthly_results, annual_metrics, learning_metrics_history = run_value_learning_simulation(
        sample_tou_params, house_args, building_xml_path
    )

    # Should run successfully and return valid results
    assert isinstance(monthly_results, list)
    assert len(monthly_results) == 1  # One month
    assert isinstance(annual_metrics, dict)
    assert isinstance(learning_metrics_history, list)
    assert len(learning_metrics_history) == 1  # One month


def test_simulate_full_cycle_with_none_params(sample_house_args):
    """Test simulate_full_cycle with None parameters"""
    # Should use default parameters
    monthly_bills, monthly_unmet_demand = simulate_full_cycle("default", None, sample_house_args)

    assert isinstance(monthly_bills, list)
    assert isinstance(monthly_unmet_demand, list)
    assert len(monthly_bills) > 0
    assert len(monthly_unmet_demand) > 0


def test_simulate_full_cycle_return_types(sample_house_args, sample_tou_params):
    """Test that simulate_full_cycle returns correct types"""
    monthly_bills, monthly_unmet_demand = simulate_full_cycle("default", sample_tou_params, sample_house_args)

    # Bills should be MonthlyMetrics objects
    for bill in monthly_bills:
        assert isinstance(bill, MonthlyMetrics)
        assert hasattr(bill, "bill")
        assert hasattr(bill, "year")
        assert hasattr(bill, "month")
        assert isinstance(bill.bill, (int, float))
        assert bill.bill >= 0

    # Unmet demand should be floats
    for demand in monthly_unmet_demand:
        assert isinstance(demand, float)
        assert demand >= 0


def test_value_learning_results_structure():
    """Test ValueLearningResults structure in monthly results"""

    # Create minimal test setup
    house_args = {
        "start_time": datetime(2018, 1, 1, 0, 0),
        "end_time": datetime(2018, 2, 1, 0, 0),  # One month
        "time_res": timedelta(minutes=15),
        "duration": timedelta(days=31),
        "initialization_time": timedelta(days=1),
        "save_results": False,
        "verbosity": 9,
        "metrics_verbosity": 7,
        "hpxml_file": os.path.join(default_input_path, "Input Files", "bldg0112631-up11.xml"),
        "hpxml_schedule_file": os.path.join(default_input_path, "Input Files", "bldg0112631_schedule.csv"),
        "weather_file": os.path.join(default_input_path, "Weather", "USA_CO_Denver.Intl.AP.725650_TMY3.epw"),
    }

    monthly_results, _, _ = run_value_learning_simulation(TOUParameters(), house_args)

    # Check that results have ValueLearningResults structure
    for result in monthly_results:
        # Standard MonthlyResults fields
        assert hasattr(result, "year")
        assert hasattr(result, "month")
        assert hasattr(result, "current_state")
        assert hasattr(result, "bill")
        assert hasattr(result, "comfort_penalty")
        assert hasattr(result, "switching_decision")
        assert hasattr(result, "realized_savings")
        assert hasattr(result, "unrealized_savings")

        # Value learning specific fields
        assert hasattr(result, "v_default")
        assert hasattr(result, "v_tou")
        assert hasattr(result, "epsilon_m")
        assert hasattr(result, "alpha_m_learn")
        assert hasattr(result, "decision_type")
        assert hasattr(result, "value_difference")
        assert hasattr(result, "recent_cost_surprise")

        # Check types
        assert isinstance(result.year, int)
        assert isinstance(result.month, int)
        assert result.current_state in ["default", "tou"]
        assert isinstance(result.bill, (int, float))
        assert isinstance(result.comfort_penalty, (int, float))
        assert result.switching_decision in ["switch", "stay"]
        assert isinstance(result.realized_savings, (int, float))
        assert isinstance(result.unrealized_savings, (int, float))
        assert isinstance(result.v_default, (int, float))
        assert isinstance(result.v_tou, (int, float))
        assert isinstance(result.epsilon_m, (int, float))
        assert isinstance(result.alpha_m_learn, (int, float))
        assert result.decision_type in ["exploration", "exploitation", "unknown"]
        assert isinstance(result.value_difference, (int, float))
        assert isinstance(result.recent_cost_surprise, (int, float))
