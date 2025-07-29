"""
Value learning parameters and data structures for the TOU scheduling decision model.

This module implements the value-learning approach described in the documentation,
including exploration rates, learning rates, and household characteristic functions.
"""

import math
from dataclasses import dataclass
from typing import Optional

import numpy as np


@dataclass
class ValueLearningParameters:
    """Value learning parameters for TOU scheduling decision model."""

    # Base learning parameters
    epsilon_base: float = 0.1  # Base exploration rate
    alpha_base_learn: float = 0.2  # Base learning rate

    # Exploration coefficients (lambda parameters) - scale with epsilon_base
    lambda_1: Optional[float] = None  # Income coefficient (epsilon_base/10)
    lambda_2: Optional[float] = None  # Building age coefficient (-epsilon_base/10)
    lambda_3: Optional[float] = None  # Household size coefficient (-epsilon_base/10)
    lambda_4: Optional[float] = None  # Water heater technology coefficient (epsilon_base/10)
    lambda_5: Optional[float] = None  # Experience replay coefficient (epsilon_base/10)

    # Learning rate coefficients (gamma parameters) - scale with alpha_base_learn
    gamma_1: Optional[float] = None  # Income coefficient (alpha_base_learn/10)
    gamma_2: Optional[float] = None  # Building age coefficient (-alpha_base_learn/10)

    def __post_init__(self) -> None:
        """Set lambda and gamma coefficients based on base parameters if not provided."""
        if self.lambda_1 is None:
            self.lambda_1 = self.epsilon_base / 10
        if self.lambda_2 is None:
            self.lambda_2 = -self.epsilon_base / 100
        if self.lambda_3 is None:
            self.lambda_3 = -self.epsilon_base / 10
        if self.lambda_4 is None:
            self.lambda_4 = self.epsilon_base / 10
        if self.lambda_5 is None:
            self.lambda_5 = self.epsilon_base / 10
        if self.gamma_1 is None:
            self.gamma_1 = self.alpha_base_learn / 10
        if self.gamma_2 is None:
            self.gamma_2 = -self.alpha_base_learn / 100

    # Prior uncertainty and comfort parameters
    tau_prior: float = 3.0  # Prior uncertainty standard deviation ($)
    beta_base: float = 2.0  # Base comfort monetization factor ($/kWh)


def sigmoid(z: float) -> float:
    """
    Sigmoid function to constrain rates to (0,1).

    Args:
        z: Input value

    Returns:
        Sigmoid value in range (0,1)
    """
    return float(1.0 / (1.0 + math.exp(-z)))


def calculate_ami_factor(ami: float) -> float:
    """
    Calculate income factor for household characteristics.

    From documentation: f_AMI = (AMI/80%)^0.6 - 1
    Higher income → more experimentation and faster learning.
    Centered at zero for 80% AMI.

    Args:
        ami: Area Median Income as fraction of 80% AMI (1.0 = 80% AMI)

    Returns:
        Income factor centered at zero
    """
    if ami <= 0:
        return -1.0  # Avoid math errors
    return float((ami / 0.8) ** 0.6 - 1.0)


def calculate_age_factor(year_built: int) -> float:
    """
    Calculate building age factor for household characteristics.

    From documentation: f_age = max(0, 2000 - YearBuilt)
    Newer buildings (f_age = 0) → higher tech adoption.
    Older buildings have positive values.

    Args:
        year_built: Year the building was built

    Returns:
        Building age factor (0 for buildings built in 2000 or later)
    """
    return max(0.0, 2000 - year_built)


def calculate_residents_factor(n_residents: float) -> float:
    """
    Calculate household size factor for household characteristics.

    From documentation: f_residents = ln(N_residents)
    More residents → harder coordination, more comfort conflicts.
    Centered at zero for single person household.

    Args:
        n_residents: Number of residents in household

    Returns:
        Household size factor (0 for single person household)
    """
    if n_residents <= 0:
        return 0.0  # Avoid math errors
    return float(math.log(max(1.0, n_residents)))  # Explicitly cast to float


def calculate_water_heater_factor(wh_type: str) -> float:
    """
    Calculate water heater technology factor for household characteristics.

    From documentation:
    f_WH = -0.3 (heat pump - smart controls)
           0.0 (storage - baseline)
           0.5 (tankless - complex scheduling)

    Args:
        wh_type: Water heater type ("heat_pump", "storage", "tankless")

    Returns:
        Water heater technology factor
    """
    wh_factors = {
        "heat_pump": -0.3,
        "storage": 0.0,
        "tankless": 0.5,
    }
    return float(wh_factors.get(wh_type, 0.0))


def calculate_climate_factor(climate_zone: Optional[str]) -> float:
    """
    Calculate climate zone factor for comfort monetization.

    From documentation:
    f_climate = 0.8 (zones 1-3 - warm)
                1.0 (zones 4-5 - moderate)
                1.2 (zones 6-8 - cold)
    Colder climates → higher hot water importance → greater comfort sensitivity.

    Args:
        climate_zone: IECC climate zone (e.g., "4A", "5B")

    Returns:
        Climate factor for comfort sensitivity
    """
    if not climate_zone:
        return 1.0  # Default to moderate climate

    # Extract numeric part of climate zone
    numeric_part = "".join(filter(str.isdigit, climate_zone))
    if numeric_part:
        zone_num = min(8, max(1, int(numeric_part)))
        if zone_num <= 3:
            return 0.8  # Warm climates
        elif zone_num <= 5:
            return 1.0  # Moderate climates
        else:
            return 1.2  # Cold climates
    else:
        return 1.0  # Default to moderate


def calculate_household_exploration_rate(
    params: ValueLearningParameters,
    ami: float,
    year_built: int,
    n_residents: float,
    wh_type: str,
    recent_cost_surprise: float = 0.0,
) -> float:
    """
    Calculate household-specific exploration rate.

    From documentation:
    epsilon_m = sigmoid(epsilon_base + lambda_1*f_AMI + lambda_2*f_age + lambda_3*f_residents +
                        lambda_4*f_WH + lambda_5*max(0, C_recent - E[C]))

    Args:
        params: Value learning parameters
        ami: Area Median Income as fraction of 80% AMI
        year_built: Year building was built
        n_residents: Number of residents
        wh_type: Water heater type
        recent_cost_surprise: Recent cost above expectations (≥ 0)

    Returns:
        Household-specific exploration rate in (0,1)
    """
    # Ensure all lambdas are float
    lambda_1 = params.lambda_1 if params.lambda_1 is not None else params.epsilon_base / 10
    lambda_2 = params.lambda_2 if params.lambda_2 is not None else -params.epsilon_base / 10
    lambda_3 = params.lambda_3 if params.lambda_3 is not None else -params.epsilon_base / 10
    lambda_4 = params.lambda_4 if params.lambda_4 is not None else params.epsilon_base / 10
    lambda_5 = params.lambda_5 if params.lambda_5 is not None else params.epsilon_base / 10

    f_ami = calculate_ami_factor(ami)
    f_age = calculate_age_factor(year_built)
    f_residents = calculate_residents_factor(n_residents)
    f_wh = calculate_water_heater_factor(wh_type)

    # Calculate pre-sigmoid exploration rate
    pre_sigmoid = (
        params.epsilon_base
        + lambda_1 * f_ami
        + lambda_2 * f_age
        + lambda_3 * f_residents
        + lambda_4 * f_wh
        + lambda_5 * max(0.0, recent_cost_surprise)
    )

    return float(sigmoid(pre_sigmoid))


def calculate_household_learning_rate(params: ValueLearningParameters, ami: float, year_built: int) -> float:
    """
    Calculate household-specific learning rate.

    From documentation:
    alpha_learn_m = sigmoid(alpha_learn_base + gamma_1*f_AMI + gamma_2*f_age)

    Args:
        params: Value learning parameters
        ami: Area Median Income as fraction of 80% AMI
        year_built: Year building was built

    Returns:
        Household-specific learning rate in (0,1)
    """
    # Ensure gamma_1 and gamma_2 are float
    gamma_1 = params.gamma_1 if params.gamma_1 is not None else params.alpha_base_learn / 10
    gamma_2 = params.gamma_2 if params.gamma_2 is not None else -params.alpha_base_learn / 10

    f_ami = calculate_ami_factor(ami)
    f_age = calculate_age_factor(year_built)

    # Calculate pre-sigmoid learning rate
    pre_sigmoid = params.alpha_base_learn + gamma_1 * f_ami + gamma_2 * f_age

    return float(sigmoid(pre_sigmoid))


def calculate_comfort_monetization_factor(
    params: ValueLearningParameters, ami: float, n_residents: float, climate_zone: Optional[str]
) -> float:
    """
    Calculate household-specific comfort monetization factor.

    From documentation:
    beta = beta_base * f_AMI * f_residents * f_climate

    Args:
        params: Value learning parameters
        ami: Area Median Income as fraction of 80% AMI
        n_residents: Number of residents
        climate_zone: IECC climate zone

    Returns:
        Household-specific comfort monetization factor ($/kWh)
    """
    # Use the AMI factor directly (not the centered version for comfort)
    f_ami = (ami / 0.8) ** 0.6 if ami > 0 else 1.0

    # Residents factor for comfort (different from exploration factor)
    f_residents = 1.0 + 0.2 * max(0, n_residents - 1)

    # Climate factor
    f_climate = calculate_climate_factor(climate_zone)

    return float(params.beta_base * f_ami * f_residents * f_climate)


def initialize_prior_values(
    default_annual_cost: float, tou_annual_cost: float, tau_prior: float
) -> tuple[float, float]:
    """
    Initialize prior value expectations with uncertainty.

    From documentation:
    V_1^default = P^default + N(0, τ_prior^2)
    V_1^TOU = P^TOU + N(0, τ_prior^2)

    Args:
        default_annual_cost: Average annual cost for default schedule
        tou_annual_cost: Average annual cost for TOU schedule
        tau_prior: Prior uncertainty standard deviation

    Returns:
        Tuple of (prior_default_value, prior_tou_value)
    """
    # Convert annual costs to monthly averages
    prior_default = default_annual_cost / 12.0
    prior_tou = tou_annual_cost / 12.0

    # Add Gaussian noise for uncertainty
    prior_default_noisy = prior_default + np.random.normal(0, tau_prior)
    prior_tou_noisy = prior_tou + np.random.normal(0, tau_prior)

    return prior_default_noisy, prior_tou_noisy
