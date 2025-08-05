"""
Tests for rate_design_platform.utils.prior_initialization module

Tests for prior value calculation and controller initialization.
"""

import pytest

from rate_design_platform.Analysis import MonthlyMetrics
from rate_design_platform.DecisionMaker import ValueLearningController
from rate_design_platform.utils.building_characteristics import BuildingCharacteristics
from rate_design_platform.utils.prior_initialization import (
    calculate_prior_values,
    initialize_value_learning_controller_with_priors,
)
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
def sample_default_monthly_results():
    """Provide sample default monthly results for testing"""
    return [
        MonthlyMetrics(year=2018, month=1, bill=120.0, comfort_penalty=2.0),
        MonthlyMetrics(year=2018, month=2, bill=110.0, comfort_penalty=1.5),
        MonthlyMetrics(year=2018, month=3, bill=100.0, comfort_penalty=1.0),
        MonthlyMetrics(year=2018, month=4, bill=90.0, comfort_penalty=0.5),
        MonthlyMetrics(year=2018, month=5, bill=80.0, comfort_penalty=0.0),
        MonthlyMetrics(year=2018, month=6, bill=85.0, comfort_penalty=0.5),
        MonthlyMetrics(year=2018, month=7, bill=95.0, comfort_penalty=1.0),
        MonthlyMetrics(year=2018, month=8, bill=105.0, comfort_penalty=1.5),
        MonthlyMetrics(year=2018, month=9, bill=110.0, comfort_penalty=2.0),
        MonthlyMetrics(year=2018, month=10, bill=115.0, comfort_penalty=2.5),
        MonthlyMetrics(year=2018, month=11, bill=125.0, comfort_penalty=3.0),
        MonthlyMetrics(year=2018, month=12, bill=130.0, comfort_penalty=3.5),
    ]


@pytest.fixture
def sample_tou_monthly_results():
    """Provide sample TOU monthly results for testing"""
    return [
        MonthlyMetrics(year=2018, month=1, bill=100.0, comfort_penalty=5.0),
        MonthlyMetrics(year=2018, month=2, bill=95.0, comfort_penalty=4.5),
        MonthlyMetrics(year=2018, month=3, bill=85.0, comfort_penalty=4.0),
        MonthlyMetrics(year=2018, month=4, bill=75.0, comfort_penalty=3.5),
        MonthlyMetrics(year=2018, month=5, bill=70.0, comfort_penalty=3.0),
        MonthlyMetrics(year=2018, month=6, bill=72.0, comfort_penalty=3.5),
        MonthlyMetrics(year=2018, month=7, bill=78.0, comfort_penalty=4.0),
        MonthlyMetrics(year=2018, month=8, bill=82.0, comfort_penalty=4.5),
        MonthlyMetrics(year=2018, month=9, bill=88.0, comfort_penalty=5.0),
        MonthlyMetrics(year=2018, month=10, bill=92.0, comfort_penalty=5.5),
        MonthlyMetrics(year=2018, month=11, bill=98.0, comfort_penalty=6.0),
        MonthlyMetrics(year=2018, month=12, bill=105.0, comfort_penalty=6.5),
    ]


def test_calculate_prior_values_basic(sample_default_monthly_results, sample_tou_monthly_results):
    """Test basic prior value calculation"""
    default_annual, tou_annual = calculate_prior_values(sample_default_monthly_results, sample_tou_monthly_results)

    # Check that annual costs are calculated correctly (sum of monthly bills)
    expected_default = sum(result.bill for result in sample_default_monthly_results)
    expected_tou = sum(result.bill for result in sample_tou_monthly_results)

    assert default_annual == expected_default
    assert tou_annual == expected_tou

    # Check reasonable values
    assert default_annual > 0
    assert tou_annual > 0
    assert isinstance(default_annual, (int, float))
    assert isinstance(tou_annual, (int, float))


def test_calculate_prior_values_bills_only(sample_default_monthly_results, sample_tou_monthly_results):
    """Test that prior values use bills only, not comfort penalties"""
    default_annual, tou_annual = calculate_prior_values(sample_default_monthly_results, sample_tou_monthly_results)

    # Manually calculate expected values
    expected_default = 120 + 110 + 100 + 90 + 80 + 85 + 95 + 105 + 110 + 115 + 125 + 130  # 1265
    expected_tou = 100 + 95 + 85 + 75 + 70 + 72 + 78 + 82 + 88 + 92 + 98 + 105  # 1040

    assert default_annual == expected_default
    assert tou_annual == expected_tou

    # Comfort penalties should not be included
    total_default_comfort = sum(result.comfort_penalty for result in sample_default_monthly_results)
    total_tou_comfort = sum(result.comfort_penalty for result in sample_tou_monthly_results)

    assert default_annual != expected_default + total_default_comfort
    assert tou_annual != expected_tou + total_tou_comfort


def test_calculate_prior_values_empty_results():
    """Test prior value calculation with empty results"""
    default_annual, tou_annual = calculate_prior_values([], [])

    assert default_annual == 0.0
    assert tou_annual == 0.0


def test_calculate_prior_values_single_month():
    """Test prior value calculation with single month"""
    default_results = [MonthlyMetrics(year=2018, month=1, bill=100.0, comfort_penalty=2.0)]
    tou_results = [MonthlyMetrics(year=2018, month=1, bill=80.0, comfort_penalty=5.0)]

    default_annual, tou_annual = calculate_prior_values(default_results, tou_results)

    assert default_annual == 100.0
    assert tou_annual == 80.0


def test_calculate_prior_values_different_lengths():
    """Test prior value calculation with different length results"""
    default_results = [
        MonthlyMetrics(year=2018, month=1, bill=100.0, comfort_penalty=2.0),
        MonthlyMetrics(year=2018, month=2, bill=110.0, comfort_penalty=2.5),
    ]
    tou_results = [
        MonthlyMetrics(year=2018, month=1, bill=80.0, comfort_penalty=5.0),
    ]

    default_annual, tou_annual = calculate_prior_values(default_results, tou_results)

    assert default_annual == 210.0  # 100 + 110
    assert tou_annual == 80.0


def test_initialize_value_learning_controller_with_priors_basic(
    sample_params, sample_building_chars, sample_default_monthly_results, sample_tou_monthly_results
):
    """Test basic controller initialization with priors"""
    controller = initialize_value_learning_controller_with_priors(
        sample_params, sample_building_chars, sample_default_monthly_results, sample_tou_monthly_results
    )

    # Check that controller is created
    assert isinstance(controller, ValueLearningController)
    assert controller.params == sample_params
    assert controller.building_chars == sample_building_chars

    # Check that priors were initialized
    learning_state = controller.get_learning_state()
    assert learning_state["v_default"] != 0.0
    assert learning_state["v_tou"] != 0.0

    # Values should be monthly (annual / 12 with some noise)
    # Default annual: 1265, TOU annual: 1040
    assert 80 < learning_state["v_default"] < 130  # ~105 ± noise
    assert 65 < learning_state["v_tou"] < 110  # ~87 ± noise


def test_initialize_value_learning_controller_with_priors_state(
    sample_params, sample_building_chars, sample_default_monthly_results, sample_tou_monthly_results
):
    """Test that controller starts in correct initial state"""
    controller = initialize_value_learning_controller_with_priors(
        sample_params, sample_building_chars, sample_default_monthly_results, sample_tou_monthly_results
    )

    # Should start on default schedule
    assert controller.get_current_state() == "default"

    # Should have household-specific parameters
    learning_state = controller.get_learning_state()
    assert learning_state["epsilon_m"] > 0
    assert learning_state["alpha_m_learn"] > 0
    assert learning_state["beta"] > 0


def test_initialize_value_learning_controller_with_priors_no_results(sample_params, sample_building_chars):
    """Test controller initialization with empty results"""
    controller = initialize_value_learning_controller_with_priors(sample_params, sample_building_chars, [], [])

    # Should still create controller but with zero priors
    assert isinstance(controller, ValueLearningController)

    learning_state = controller.get_learning_state()
    # Prior values should be zero since no data provided
    # (The initialize_prior_values function will generate small random values around 0)
    assert abs(learning_state["v_default"]) < 5.0  # Small noise around 0
    assert abs(learning_state["v_tou"]) < 5.0  # Small noise around 0


def test_prior_calculation_preserves_data_integrity(sample_default_monthly_results, sample_tou_monthly_results):
    """Test that prior calculation doesn't modify input data"""
    # Create copies to verify originals aren't modified
    original_default = [
        MonthlyMetrics(r.year, r.month, r.bill, r.comfort_penalty) for r in sample_default_monthly_results
    ]
    original_tou = [MonthlyMetrics(r.year, r.month, r.bill, r.comfort_penalty) for r in sample_tou_monthly_results]

    # Calculate priors
    calculate_prior_values(sample_default_monthly_results, sample_tou_monthly_results)

    # Verify originals are unchanged
    for orig, current in zip(original_default, sample_default_monthly_results):
        assert orig.year == current.year
        assert orig.month == current.month
        assert orig.bill == current.bill
        assert orig.comfort_penalty == current.comfort_penalty

    for orig, current in zip(original_tou, sample_tou_monthly_results):
        assert orig.year == current.year
        assert orig.month == current.month
        assert orig.bill == current.bill
        assert orig.comfort_penalty == current.comfort_penalty


def test_controller_initialization_different_building_types(sample_params):
    """Test controller initialization with different building characteristics"""
    # Test with high-income building
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

    # Simple monthly results
    monthly_results = [MonthlyMetrics(year=2018, month=1, bill=100.0, comfort_penalty=2.0)]

    controller = initialize_value_learning_controller_with_priors(
        sample_params, high_income_building, monthly_results, monthly_results
    )

    # Should create controller with building-specific parameters
    assert isinstance(controller, ValueLearningController)
    assert controller.building_chars == high_income_building

    learning_state = controller.get_learning_state()
    # High-income household should have different parameters
    assert learning_state["epsilon_m"] > 0
    assert learning_state["alpha_m_learn"] > 0
    assert learning_state["beta"] > 0


def test_prior_values_realistic_range(sample_default_monthly_results, sample_tou_monthly_results):
    """Test that calculated priors are in realistic ranges"""
    default_annual, tou_annual = calculate_prior_values(sample_default_monthly_results, sample_tou_monthly_results)

    # Should be reasonable annual electricity costs (hundreds to thousands)
    assert 500 < default_annual < 5000
    assert 500 < tou_annual < 5000

    # TOU should typically be lower than default (savings)
    assert tou_annual < default_annual


def test_controller_can_make_decisions_after_initialization(
    sample_params, sample_building_chars, sample_default_monthly_results, sample_tou_monthly_results
):
    """Test that initialized controller can make decisions"""
    controller = initialize_value_learning_controller_with_priors(
        sample_params, sample_building_chars, sample_default_monthly_results, sample_tou_monthly_results
    )

    # Should be able to make a decision immediately
    decision, metrics = controller.evaluate_TOU(100.0, 80.0, 2.0, 5.0)

    assert decision in ["switch", "stay"]
    assert isinstance(metrics, dict)
    assert "v_default" in metrics
    assert "v_tou" in metrics
    assert "decision_type" in metrics


def test_prior_calculation_with_zero_bills():
    """Test prior calculation with zero bills (edge case)"""
    zero_results = [MonthlyMetrics(year=2018, month=1, bill=0.0, comfort_penalty=10.0)]

    default_annual, tou_annual = calculate_prior_values(zero_results, zero_results)

    assert default_annual == 0.0
    assert tou_annual == 0.0


def test_prior_calculation_with_negative_bills():
    """Test prior calculation with negative bills (edge case)"""
    negative_results = [MonthlyMetrics(year=2018, month=1, bill=-50.0, comfort_penalty=10.0)]
    normal_results = [MonthlyMetrics(year=2018, month=1, bill=100.0, comfort_penalty=5.0)]

    default_annual, tou_annual = calculate_prior_values(negative_results, normal_results)

    assert default_annual == -50.0
    assert tou_annual == 100.0

    # Controller should still initialize (though with unusual priors)
    params = ValueLearningParameters()
    building = BuildingCharacteristics(
        state_code="CO",
        zip_code="80202",
        year_built=1980,
        n_residents=2.0,
        n_bedrooms=2,
        conditioned_floor_area=1000.0,
        residential_facility_type="single-family detached",
        climate_zone="5B",
        ami=1.0,
        wh_type="storage",
    )

    controller = initialize_value_learning_controller_with_priors(params, building, negative_results, normal_results)

    assert isinstance(controller, ValueLearningController)
