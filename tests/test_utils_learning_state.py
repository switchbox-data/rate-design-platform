"""
Tests for rate_design_platform.utils.learning_state module

Tests for LearningState and ValueLearningStateManager classes.
"""

from collections import deque

import pytest

from rate_design_platform.utils.building_characteristics import BuildingCharacteristics
from rate_design_platform.utils.learning_state import LearningState, ValueLearningStateManager
from rate_design_platform.utils.value_learning_params import ValueLearningParameters


@pytest.fixture
def sample_params():
    """Provide sample value learning parameters for testing"""
    return ValueLearningParameters()


@pytest.fixture
def sample_building_chars():
    """Provide sample building characteristics for testing"""
    return BuildingCharacteristics(
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


@pytest.fixture
def state_manager(sample_params, sample_building_chars):
    """Provide initialized ValueLearningStateManager for testing"""
    return ValueLearningStateManager(
        params=sample_params, building_chars=sample_building_chars, default_annual_cost=1200.0, tou_annual_cost=1000.0
    )


def test_learning_state_initialization():
    """Test LearningState dataclass initialization"""
    state = LearningState()

    # Check default values
    assert state.v_default == 0.0
    assert state.v_tou == 0.0
    assert state.current_state is True  # Default state
    assert state.epsilon_m == 0.05
    assert state.alpha_m_learn == 0.1
    assert state.beta == 0.15
    assert isinstance(state.recent_costs, deque)
    assert state.recent_costs.maxlen == 3
    assert state.expected_cost == 0.0
    assert state.month == 1
    assert state.year == 2018


def test_learning_state_custom_values():
    """Test LearningState with custom values"""
    custom_costs = deque([100.0, 110.0], maxlen=3)
    state = LearningState(
        v_default=120.0,
        v_tou=100.0,
        current_state=False,  # TOU state
        epsilon_m=0.15,
        alpha_m_learn=0.25,
        beta=0.20,
        recent_costs=custom_costs,
        expected_cost=105.0,
        month=6,
        year=2019,
    )

    assert state.v_default == 120.0
    assert state.v_tou == 100.0
    assert state.current_state is False
    assert state.epsilon_m == 0.15
    assert state.alpha_m_learn == 0.25
    assert state.beta == 0.20
    assert state.recent_costs == custom_costs
    assert state.expected_cost == 105.0
    assert state.month == 6
    assert state.year == 2019


def test_value_learning_state_manager_initialization(sample_params, sample_building_chars):
    """Test ValueLearningStateManager initialization"""
    manager = ValueLearningStateManager(sample_params, sample_building_chars)

    assert manager.params == sample_params
    assert manager.building_chars == sample_building_chars
    assert isinstance(manager.state, LearningState)

    # Check that household parameters were initialized
    assert manager.state.epsilon_m > 0
    assert manager.state.alpha_m_learn > 0
    assert manager.state.beta > 0


def test_value_learning_state_manager_with_priors(sample_params, sample_building_chars):
    """Test ValueLearningStateManager initialization with prior costs"""
    manager = ValueLearningStateManager(
        params=sample_params, building_chars=sample_building_chars, default_annual_cost=1200.0, tou_annual_cost=1000.0
    )

    # Should have initialized prior values
    assert manager.state.v_default != 0.0
    assert manager.state.v_tou != 0.0
    assert manager.state.expected_cost != 0.0

    # Monthly values should be less than annual (divided by 12 with some noise)
    assert 50 < manager.state.v_default < 150  # ~100 ± noise
    assert 50 < manager.state.v_tou < 150  # ~83 ± noise


def test_update_learned_value_default_state(state_manager):
    """Test updating learned value while on default schedule"""
    # Start on default (True)
    assert state_manager.state.current_state is True

    initial_v_default = state_manager.state.v_default
    initial_v_tou = state_manager.state.v_tou

    # Update with actual cost
    actual_cost = 110.0
    state_manager.update_learned_value(actual_cost)

    # Default value should be updated
    assert state_manager.state.v_default != initial_v_default
    # TOU value should remain unchanged
    assert state_manager.state.v_tou == initial_v_tou

    # Check learning rate calculation
    alpha = state_manager.state.alpha_m_learn
    expected_v_default = (1 - alpha) * initial_v_default + alpha * actual_cost
    assert abs(state_manager.state.v_default - expected_v_default) < 1e-10


def test_update_learned_value_tou_state(state_manager):
    """Test updating learned value while on TOU schedule"""
    # Switch to TOU state
    state_manager.state.current_state = False

    initial_v_default = state_manager.state.v_default
    initial_v_tou = state_manager.state.v_tou

    # Update with actual cost
    actual_cost = 90.0
    state_manager.update_learned_value(actual_cost)

    # TOU value should be updated
    assert state_manager.state.v_tou != initial_v_tou
    # Default value should remain unchanged
    assert state_manager.state.v_default == initial_v_default

    # Check learning rate calculation
    alpha = state_manager.state.alpha_m_learn
    expected_v_tou = (1 - alpha) * initial_v_tou + alpha * actual_cost
    assert abs(state_manager.state.v_tou - expected_v_tou) < 1e-10


def test_calculate_recent_cost_surprise_insufficient_history(state_manager):
    """Test cost surprise calculation with insufficient history"""
    # Clear recent costs
    state_manager.state.recent_costs.clear()

    # No history
    assert state_manager.calculate_recent_cost_surprise() == 0.0

    # One cost entry
    state_manager.state.recent_costs.append(100.0)
    assert state_manager.calculate_recent_cost_surprise() == 0.0


def test_calculate_recent_cost_surprise_with_history(state_manager):
    """Test cost surprise calculation with sufficient history"""
    # Set learned values
    state_manager.state.v_default = 100.0
    state_manager.state.v_tou = 80.0
    state_manager.state.current_state = True  # Default

    # Add recent costs higher than expected
    state_manager.state.recent_costs.clear()
    state_manager.state.recent_costs.extend([120.0, 130.0])

    surprise = state_manager.calculate_recent_cost_surprise()
    expected_surprise = 125.0 - 100.0  # avg(120, 130) - v_default
    assert surprise == expected_surprise

    # Test with costs lower than expected
    state_manager.state.recent_costs.clear()
    state_manager.state.recent_costs.extend([80.0, 90.0])

    surprise = state_manager.calculate_recent_cost_surprise()
    assert surprise == 0.0  # max(0, 85 - 100) = 0


def test_update_exploration_rate(state_manager):
    """Test exploration rate updates"""
    initial_epsilon = state_manager.state.epsilon_m

    # Add some cost history to trigger experience replay
    state_manager.state.recent_costs.extend([120.0, 130.0])

    # Update exploration rate
    state_manager.update_exploration_rate()

    # Should be updated (likely higher due to cost surprise)
    assert state_manager.state.epsilon_m >= initial_epsilon
    assert 0 <= state_manager.state.epsilon_m <= 1


def test_make_decision_exploration(state_manager):
    """Test decision making in exploration mode"""
    # Set high exploration rate to force exploration
    state_manager.state.epsilon_m = 1.0  # Always explore

    should_explore, should_switch = state_manager.make_decision()

    assert should_explore is True
    assert should_switch is True  # Always switch when exploring


def test_make_decision_exploitation(state_manager):
    """Test decision making in exploitation mode"""
    # Set zero exploration rate to force exploitation
    state_manager.state.epsilon_m = 0.0

    # Set learned values where TOU is better
    state_manager.state.v_default = 120.0
    state_manager.state.v_tou = 80.0
    state_manager.state.current_state = True  # On default

    should_explore, should_switch = state_manager.make_decision()

    assert should_explore is False
    assert should_switch is True  # Should switch to better TOU

    # Test opposite case
    state_manager.state.current_state = False  # On TOU
    should_explore, should_switch = state_manager.make_decision()

    assert should_explore is False
    assert should_switch is False  # Should stay on TOU (better)


def test_update_state_for_next_month(state_manager):
    """Test complete state update for next month"""
    initial_month = state_manager.state.month
    initial_year = state_manager.state.year
    initial_state = state_manager.state.current_state

    # Update state with switch decision
    state_manager.update_state_for_next_month(switch_decision=True, actual_cost=110.0)

    # Month should advance
    assert state_manager.state.month == initial_month + 1
    assert state_manager.state.year == initial_year  # Same year

    # State should switch
    assert state_manager.state.current_state != initial_state

    # Cost should be added to history
    assert 110.0 in state_manager.state.recent_costs

    # Learning values should be updated
    # (exact values depend on learning algorithm)


def test_update_state_year_boundary(state_manager):
    """Test state update across year boundary"""
    # Set to December
    state_manager.state.month = 12
    state_manager.state.year = 2018

    state_manager.update_state_for_next_month(switch_decision=False, actual_cost=100.0)

    # Should advance to January next year
    assert state_manager.state.month == 1
    assert state_manager.state.year == 2019


def test_get_current_schedule_name(state_manager):
    """Test getting current schedule name"""
    state_manager.state.current_state = True
    assert state_manager.get_current_schedule_name() == "default"

    state_manager.state.current_state = False
    assert state_manager.get_current_schedule_name() == "tou"


def test_get_alternative_schedule_name(state_manager):
    """Test getting alternative schedule name"""
    state_manager.state.current_state = True
    assert state_manager.get_alternative_schedule_name() == "tou"

    state_manager.state.current_state = False
    assert state_manager.get_alternative_schedule_name() == "default"


def test_get_learning_metrics(state_manager):
    """Test getting learning metrics"""
    metrics = state_manager.get_learning_metrics()

    # Check required keys
    expected_keys = [
        "v_default",
        "v_tou",
        "current_state",
        "epsilon_m",
        "alpha_m_learn",
        "beta",
        "month",
        "year",
        "value_difference",
        "recent_cost_surprise",
    ]
    for key in expected_keys:
        assert key in metrics

    # Check types
    assert isinstance(metrics["v_default"], (int, float))
    assert isinstance(metrics["v_tou"], (int, float))
    assert metrics["current_state"] in ["default", "tou"]
    assert isinstance(metrics["epsilon_m"], (int, float))
    assert isinstance(metrics["alpha_m_learn"], (int, float))
    assert isinstance(metrics["beta"], (int, float))
    assert isinstance(metrics["month"], int)
    assert isinstance(metrics["year"], int)
    assert isinstance(metrics["value_difference"], (int, float))
    assert isinstance(metrics["recent_cost_surprise"], (int, float))

    # Check value difference calculation
    expected_diff = abs(state_manager.state.v_tou - state_manager.state.v_default)
    assert abs(metrics["value_difference"] - expected_diff) < 1e-10


def test_household_parameter_calculation(sample_params):
    """Test that different building characteristics produce different parameters"""
    # High-income, newer building
    high_income_building = BuildingCharacteristics(
        state_code="CA",
        zip_code="90210",
        year_built=2010,
        n_residents=1.0,
        n_bedrooms=2,
        conditioned_floor_area=1000.0,
        residential_facility_type="single-family detached",
        climate_zone="3B",
        ami=1.5,  # High income
        wh_type="heat_pump",
    )

    # Low-income, older building
    low_income_building = BuildingCharacteristics(
        state_code="TX",
        zip_code="78701",
        year_built=1960,
        n_residents=4.0,
        n_bedrooms=3,
        conditioned_floor_area=1200.0,
        residential_facility_type="single-family detached",
        climate_zone="2A",
        ami=0.6,  # Low income
        wh_type="tankless",
    )

    manager_high = ValueLearningStateManager(sample_params, high_income_building)
    manager_low = ValueLearningStateManager(sample_params, low_income_building)

    # Parameters should be different
    assert manager_high.state.epsilon_m != manager_low.state.epsilon_m
    assert manager_high.state.alpha_m_learn != manager_low.state.alpha_m_learn
    assert manager_high.state.beta != manager_low.state.beta

    # All should be in valid ranges
    assert 0 < manager_high.state.epsilon_m < 1
    assert 0 < manager_low.state.epsilon_m < 1
    assert 0 < manager_high.state.alpha_m_learn < 1
    assert 0 < manager_low.state.alpha_m_learn < 1
    assert manager_high.state.beta > 0
    assert manager_low.state.beta > 0


def test_recent_costs_deque_behavior(state_manager):
    """Test that recent costs deque behaves correctly"""
    # Clear existing costs
    state_manager.state.recent_costs.clear()

    # Add costs up to maxlen
    costs = [100.0, 110.0, 120.0]
    for cost in costs:
        state_manager.state.recent_costs.append(cost)

    assert len(state_manager.state.recent_costs) == 3
    assert list(state_manager.state.recent_costs) == costs

    # Add one more - should drop the first
    state_manager.state.recent_costs.append(130.0)

    assert len(state_manager.state.recent_costs) == 3
    assert list(state_manager.state.recent_costs) == [110.0, 120.0, 130.0]


def test_state_manager_without_priors(sample_params, sample_building_chars):
    """Test state manager initialization without prior costs"""
    manager = ValueLearningStateManager(sample_params, sample_building_chars)

    # Should still initialize with default zero values
    assert manager.state.v_default == 0.0
    assert manager.state.v_tou == 0.0
    assert manager.state.expected_cost == 0.0

    # But should have household parameters
    assert manager.state.epsilon_m > 0
    assert manager.state.alpha_m_learn > 0
    assert manager.state.beta > 0
