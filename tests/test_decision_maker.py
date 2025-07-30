"""
Tests for rate_design_platform.DecisionMaker module

Tests for ValueLearningController class and its methods.
"""

import pytest

from rate_design_platform.DecisionMaker import ValueLearningController
from rate_design_platform.utils.building_characteristics import BuildingCharacteristics
from rate_design_platform.utils.value_learning_params import ValueLearningParameters


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
def sample_params():
    """Provide sample value learning parameters for testing"""
    return ValueLearningParameters()


@pytest.fixture
def controller(sample_params, sample_building_chars):
    """Provide initialized ValueLearningController for testing"""
    return ValueLearningController(
        params=sample_params, building_chars=sample_building_chars, default_annual_cost=1200.0, tou_annual_cost=1000.0
    )


def test_evaluate_TOU(controller):
    """Test evaluate_TOU method"""
    # Test evaluation with sample bills and comfort penalties
    default_bill = 100.0
    tou_bill = 80.0
    default_comfort_penalty = 2.0
    tou_comfort_penalty = 5.0

    decision, learning_metrics = controller.evaluate_TOU(
        default_bill, tou_bill, default_comfort_penalty, tou_comfort_penalty
    )

    # Check decision is valid
    assert decision in ["switch", "stay"]

    # Check learning metrics structure
    assert isinstance(learning_metrics, dict)
    expected_keys = [
        "v_default",
        "v_tou",
        "epsilon_m",
        "alpha_m_learn",
        "value_difference",
        "decision_type",
        "actual_total_cost",
        "current_schedule",
    ]
    for key in expected_keys:
        assert key in learning_metrics

    # Check decision type is valid
    assert learning_metrics["decision_type"] in ["exploration", "exploitation"]

    # Check current schedule is valid (this was the state before the decision)
    assert learning_metrics["current_schedule"] in ["default", "tou"]


def test_get_current_state(controller):
    """Test get_current_state method"""
    state = controller.get_current_state()
    assert state in ["default", "tou"]
    assert isinstance(state, str)


def test_get_learning_state(controller):
    """Test get_learning_state method"""
    learning_state = controller.get_learning_state()

    assert isinstance(learning_state, dict)
    expected_keys = ["v_default", "v_tou", "epsilon_m", "alpha_m_learn", "value_difference"]
    for key in expected_keys:
        assert key in learning_state
        assert isinstance(learning_state[key], (int, float))


def test_reset_for_new_year(controller):
    """Test reset_for_new_year method"""
    # Run some evaluations to establish learning state
    for _ in range(3):
        controller.evaluate_TOU(100.0, 80.0, 2.0, 5.0)

    # Get learning state before reset
    pre_reset_state = controller.get_learning_state()
    pre_reset_v_default = pre_reset_state["v_default"]
    pre_reset_v_tou = pre_reset_state["v_tou"]

    # Reset for new year
    controller.reset_for_new_year()

    # Get learning state after reset
    post_reset_state = controller.get_learning_state()
    post_reset_v_default = post_reset_state["v_default"]
    post_reset_v_tou = post_reset_state["v_tou"]

    # Learned values should persist across years
    assert post_reset_v_default == pre_reset_v_default
    assert post_reset_v_tou == pre_reset_v_tou
