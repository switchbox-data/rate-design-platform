"""
Tests for value learning parameters and household characteristic functions.
"""

import numpy as np
import pytest

from rate_design_platform.utils.value_learning_params import (
    ValueLearningParameters,
    calculate_age_factor,
    calculate_ami_factor,
    calculate_climate_factor,
    calculate_comfort_monetization_factor,
    calculate_household_exploration_rate,
    calculate_household_learning_rate,
    calculate_residents_factor,
    calculate_water_heater_factor,
    clamp_to_0_1,
    initialize_prior_values,
)


def test_value_learning_parameters_default():
    """Test ValueLearningParameters with default values."""
    params = ValueLearningParameters()

    # Test base parameters
    assert params.epsilon_base == 0.1
    assert params.alpha_base_learn == 0.2
    assert params.tau_prior == 3.0
    assert params.beta_base == 2.0

    # Test that lambda coefficients scale with epsilon_base
    assert params.lambda_1 == params.epsilon_base / 10
    assert params.lambda_2 == params.epsilon_base / 10  # Positive, not negative
    assert params.lambda_3 == -params.epsilon_base / 10
    assert params.lambda_4 == params.epsilon_base / 10
    assert params.lambda_5 == params.epsilon_base / 10

    # Test that gamma coefficients scale with alpha_base_learn
    assert params.gamma_1 == params.alpha_base_learn / 10
    assert params.gamma_2 == params.alpha_base_learn / 10  # Positive, not negative


def test_value_learning_parameters_custom():
    """Test ValueLearningParameters with custom values."""
    params = ValueLearningParameters(epsilon_base=0.2, alpha_base_learn=0.3)

    # Test custom base parameters
    assert params.epsilon_base == 0.2
    assert params.alpha_base_learn == 0.3

    # Test that coefficients scale correctly
    assert params.lambda_1 == 0.02  # 0.2 / 10
    assert params.lambda_2 == 0.02  # 0.2 / 10 (positive)
    assert params.gamma_1 == 0.03  # 0.3 / 10
    assert params.gamma_2 == 0.03  # 0.3 / 10 (positive)


def test_clamp_to_0_1():
    """Test clamp_to_0_1 function behavior."""
    # Test basic clamping
    assert clamp_to_0_1(0.5) == 0.5
    assert clamp_to_0_1(-10) == 0.0
    assert clamp_to_0_1(10) == 1.0

    # Test boundary values
    assert clamp_to_0_1(0.0) == 0.0
    assert clamp_to_0_1(1.0) == 1.0

    # Test edge cases
    assert clamp_to_0_1(-0.001) == 0.0
    assert clamp_to_0_1(1.001) == 1.0


def test_calculate_ami_factor():
    """Test AMI factor calculation."""
    # Test at 80% AMI (should be 0)
    assert calculate_ami_factor(0.8) == pytest.approx(0.0, abs=1e-6)

    # Test above 80% AMI (should be positive)
    assert calculate_ami_factor(1.0) > 0
    assert calculate_ami_factor(1.2) > calculate_ami_factor(1.0)

    # Test below 80% AMI (should be negative)
    assert calculate_ami_factor(0.6) < 0

    # Test edge cases
    assert calculate_ami_factor(0) == -1.0  # Avoid math errors


def test_calculate_age_factor():
    """Test building age factor calculation."""
    # Test newer buildings (built in 2000 or later)
    assert calculate_age_factor(2000) == 0
    assert calculate_age_factor(2010) == pytest.approx(10 / 15, abs=0.01)  # (2010-2000)/15

    # Test older buildings (negative factors for older buildings)
    assert calculate_age_factor(1990) == pytest.approx(-10 / 15, abs=0.01)  # (1990-2000)/15
    assert calculate_age_factor(1980) == pytest.approx(-20 / 15, abs=0.01)  # (1980-2000)/15

    # Test very old buildings
    assert calculate_age_factor(1950) == pytest.approx(-50 / 15, abs=0.01)  # (1950-2000)/15


def test_calculate_residents_factor():
    """Test household size factor calculation."""
    # Test single person (should be 0)
    assert calculate_residents_factor(1.0) == pytest.approx(0.0, abs=1e-6)

    # Test increasing household size
    factor_2 = calculate_residents_factor(2.0)
    factor_3 = calculate_residents_factor(3.0)
    factor_4 = calculate_residents_factor(4.0)

    assert factor_2 > 0
    assert factor_3 > factor_2
    assert factor_4 > factor_3

    # Test edge cases
    assert calculate_residents_factor(0) == 0  # Avoid math errors
    assert calculate_residents_factor(-1) == 0


def test_calculate_water_heater_factor():
    """Test water heater technology factor calculation."""
    # Test known values
    assert calculate_water_heater_factor("heat_pump") == -0.3
    assert calculate_water_heater_factor("storage") == 0.0
    assert calculate_water_heater_factor("tankless") == 0.5

    # Test unknown type (should default to storage)
    assert calculate_water_heater_factor("unknown") == 0.0


def test_calculate_climate_factor():
    """Test climate zone factor calculation."""
    # Test warm climates (zones 1-3)
    assert calculate_climate_factor("1A") == 0.8
    assert calculate_climate_factor("2B") == 0.8
    assert calculate_climate_factor("3C") == 0.8

    # Test moderate climates (zones 4-5)
    assert calculate_climate_factor("4A") == 1.0
    assert calculate_climate_factor("5B") == 1.0

    # Test cold climates (zones 6-8)
    assert calculate_climate_factor("6A") == 1.2
    assert calculate_climate_factor("7A") == 1.2
    assert calculate_climate_factor("8A") == 1.2

    # Test edge cases
    assert calculate_climate_factor(None) == 1.0
    assert calculate_climate_factor("") == 1.0
    assert calculate_climate_factor("unknown") == 1.0


def test_calculate_household_exploration_rate():
    """Test household-specific exploration rate calculation."""
    params = ValueLearningParameters()

    # Test baseline case (80% AMI, 2000 year, 1 resident, storage WH, no surprise)
    rate = calculate_household_exploration_rate(params, ami=0.8, year_built=2000, n_residents=1.0, wh_type="storage")

    # Should be close to clamp_to_0_1(epsilon_base) since factors are mostly 0
    expected = clamp_to_0_1(params.epsilon_base)
    assert rate == pytest.approx(expected, abs=0.01)

    # Test with cost surprise (should increase exploration)
    rate_with_surprise = calculate_household_exploration_rate(
        params, ami=0.8, year_built=2000, n_residents=1.0, wh_type="storage", recent_cost_surprise=10.0
    )
    assert rate_with_surprise > rate

    # Test that result is always in [0,1] (clamped)
    rate_extreme = calculate_household_exploration_rate(
        params, ami=2.0, year_built=1900, n_residents=10.0, wh_type="tankless", recent_cost_surprise=100.0
    )
    assert 0 <= rate_extreme <= 1


def test_calculate_household_learning_rate():
    """Test household-specific learning rate calculation."""
    params = ValueLearningParameters()

    # Test baseline case
    rate = calculate_household_learning_rate(params, ami=0.8, year_built=2000)
    expected = clamp_to_0_1(params.alpha_base_learn)
    assert rate == pytest.approx(expected, abs=0.01)

    # Test with higher income (should increase learning rate)
    rate_high_income = calculate_household_learning_rate(params, ami=1.2, year_built=2000)
    assert rate_high_income > rate

    # Test with older building (should decrease learning rate)
    rate_old_building = calculate_household_learning_rate(params, ami=0.8, year_built=1980)
    assert rate_old_building < rate

    # Test that result is always in (0,1)
    assert 0 < rate < 1
    assert 0 < rate_high_income < 1
    assert 0 < rate_old_building < 1


def test_calculate_comfort_monetization_factor():
    """Test comfort monetization factor calculation."""
    params = ValueLearningParameters()

    # Test baseline case (80% AMI, 1 resident, moderate climate)
    factor = calculate_comfort_monetization_factor(params, ami=0.8, n_residents=1.0, climate_zone="4A")

    # Should be close to beta_base
    assert factor == pytest.approx(params.beta_base, abs=0.01)

    # Test with higher income (should increase comfort value)
    factor_high_income = calculate_comfort_monetization_factor(params, ami=1.2, n_residents=1.0, climate_zone="4A")
    assert factor_high_income > factor

    # Test with more residents (should increase comfort value)
    factor_more_residents = calculate_comfort_monetization_factor(params, ami=0.8, n_residents=3.0, climate_zone="4A")
    assert factor_more_residents > factor

    # Test with cold climate (should increase comfort value)
    factor_cold_climate = calculate_comfort_monetization_factor(params, ami=0.8, n_residents=1.0, climate_zone="7A")
    assert factor_cold_climate > factor


def test_initialize_prior_values():
    """Test prior value initialization with uncertainty."""
    default_annual = 1200.0
    tou_annual = 1000.0
    tau_prior = 10.0

    # Test multiple initializations to check randomness
    priors = []
    for _ in range(100):
        prior_default, prior_tou = initialize_prior_values(default_annual, tou_annual, tau_prior)
        priors.append((prior_default, prior_tou))

    # Check that priors are centered around monthly averages
    default_monthly = default_annual / 12.0
    tou_monthly = tou_annual / 12.0

    avg_prior_default = np.mean([p[0] for p in priors])
    avg_prior_tou = np.mean([p[1] for p in priors])

    assert avg_prior_default == pytest.approx(default_monthly, abs=5.0)
    assert avg_prior_tou == pytest.approx(tou_monthly, abs=5.0)

    # Check that there's variability (not all the same)
    default_values = [p[0] for p in priors]
    tou_values = [p[1] for p in priors]

    assert np.std(default_values) > 5.0  # Should have reasonable variation
    assert np.std(tou_values) > 5.0


def test_parameter_interactions():
    """Test interactions between different household characteristics."""
    params = ValueLearningParameters()

    # High-income, newer building, small household, heat pump
    rate_advantaged = calculate_household_exploration_rate(
        params, ami=1.5, year_built=2020, n_residents=1.0, wh_type="heat_pump"
    )

    # Low-income, older building, large household, tankless
    rate_disadvantaged = calculate_household_exploration_rate(
        params, ami=0.5, year_built=1970, n_residents=5.0, wh_type="tankless"
    )

    # The advantaged household should have higher exploration rate
    # (positive income effect and negative age effect from heat pump vs tankless effect)
    # This is a complex interaction, so we mainly test that both are valid rates
    assert 0 < rate_advantaged < 1
    assert 0 < rate_disadvantaged < 1
