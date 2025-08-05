"""
Learning state management for value learning TOU scheduling.

This module manages the state variables and experience tracking needed
for the value learning approach.
"""

from collections import deque
from dataclasses import dataclass, field

import numpy as np

from rate_design_platform.utils.building_characteristics import BuildingCharacteristics
from rate_design_platform.utils.value_learning_params import ValueLearningParameters


@dataclass
class LearningState:
    """
    State variables for value learning decision model.

    Tracks learned values, household-specific parameters, and experience
    for monthly decision making.
    """

    # Learned values for each schedule type (monthly costs in $)
    v_default: float = 0.0  # V_m^default - learned value for default schedule
    v_tou: float = 0.0  # V_m^TOU - learned value for TOU schedule

    # Current schedule state (binary: True=default, False=TOU)
    current_state: bool = True  # S_m^current (True=default, False=TOU)

    # Household-specific learning parameters
    epsilon_m: float = 0.05  # Household exploration rate
    alpha_m_learn: float = 0.1  # Household learning rate
    beta: float = 0.15  # Comfort monetization factor ($/kWh)

    # Experience tracking for experience replay
    recent_costs: deque = field(default_factory=lambda: deque(maxlen=3))  # Last 2-3 months
    expected_cost: float = 0.0  # Expected cost based on learned values

    # Monthly tracking
    month: int = 1
    year: int = 2018


class ValueLearningStateManager:
    """
    Manages learning state evolution and experience tracking.
    """

    def __init__(
        self,
        params: ValueLearningParameters,
        building_chars: BuildingCharacteristics,
        default_annual_cost: float = 0.0,
        tou_annual_cost: float = 0.0,
    ):
        """
        Initialize learning state manager.

        Args:
            params: Value learning parameters
            building_chars: Building characteristics
            default_annual_cost: Annual cost for default schedule (for priors)
            tou_annual_cost: Annual cost for TOU schedule (for priors)
        """
        self.params = params
        self.building_chars = building_chars
        self.state = LearningState()

        # Initialize household-specific parameters
        self._initialize_household_parameters()

        # Initialize prior values if cost data available
        if default_annual_cost > 0 and tou_annual_cost > 0:
            self._initialize_prior_values(default_annual_cost, tou_annual_cost)

    def _initialize_household_parameters(self) -> None:
        """Initialize household-specific learning parameters."""
        from rate_design_platform.utils.value_learning_params import (
            calculate_comfort_monetization_factor,
            calculate_household_exploration_rate,
            calculate_household_learning_rate,
        )

        # Get building characteristics with defaults
        ami = self.building_chars.ami or 1.0
        year_built = self.building_chars.year_built
        n_residents = self.building_chars.n_residents
        wh_type = self.building_chars.wh_type
        climate_zone = self.building_chars.climate_zone

        # Calculate household-specific parameters
        self.state.epsilon_m = calculate_household_exploration_rate(self.params, ami, year_built, n_residents, wh_type)

        self.state.alpha_m_learn = calculate_household_learning_rate(self.params, ami, year_built)

        self.state.beta = calculate_comfort_monetization_factor(self.params, ami, n_residents, climate_zone)

    def _initialize_prior_values(self, default_annual_cost: float, tou_annual_cost: float) -> None:
        """
        Initialize prior values with uncertainty.

        Args:
            default_annual_cost: Annual cost for default schedule
            tou_annual_cost: Annual cost for TOU schedule
        """
        from rate_design_platform.utils.value_learning_params import initialize_prior_values

        prior_default, prior_tou = initialize_prior_values(default_annual_cost, tou_annual_cost, self.params.tau_prior)

        self.state.v_default = prior_default
        self.state.v_tou = prior_tou
        self.state.expected_cost = prior_default  # Start with default expectation

    def update_learned_value(self, actual_total_cost: float) -> None:
        """
        Update learned value for current schedule using learning rate.

        From documentation:
        V_m^current = (1 - alpha_m^learn) * V_{m-1}^current + alpha_m^learn * C_m^total

        Args:
            actual_total_cost: Actual total cost (bill + comfort penalty) for current month
        """
        if self.state.current_state:  # Currently on default
            self.state.v_default = (
                1 - self.state.alpha_m_learn
            ) * self.state.v_default + self.state.alpha_m_learn * actual_total_cost
        else:  # Currently on TOU
            self.state.v_tou = (
                1 - self.state.alpha_m_learn
            ) * self.state.v_tou + self.state.alpha_m_learn * actual_total_cost

    def calculate_recent_cost_surprise(self) -> float:
        """
        Calculate recent cost surprise for experience replay.

        Returns:
            Cost surprise: max(0, C_recent - E[C]) where C_recent is average
            of last 2-3 months and E[C] is expected cost based on learned values
        """
        if len(self.state.recent_costs) < 2:
            return 0.0  # Not enough history

        # Calculate average of recent costs
        recent_avg = float(sum(self.state.recent_costs)) / float(len(self.state.recent_costs))

        # Expected cost is learned value for current schedule
        expected = float(self.state.v_default if self.state.current_state else self.state.v_tou)

        return max(0.0, recent_avg - expected)

    def update_exploration_rate(self) -> None:
        """Update exploration rate including experience replay effects."""
        from rate_design_platform.utils.value_learning_params import calculate_household_exploration_rate

        # Calculate cost surprise for experience replay
        cost_surprise = self.calculate_recent_cost_surprise()

        # Get building characteristics
        ami = self.building_chars.ami or 1.0
        year_built = self.building_chars.year_built
        n_residents = self.building_chars.n_residents
        wh_type = self.building_chars.wh_type

        # Update exploration rate with experience replay
        self.state.epsilon_m = calculate_household_exploration_rate(
            self.params, ami, year_built, n_residents, wh_type, cost_surprise
        )

    def make_decision(self) -> tuple[bool, bool]:
        """
        Make exploration vs exploitation decision.

        Returns:
            Tuple of (should_explore, should_switch)
            - should_explore: True if random draw < exploration rate
            - should_switch: True if should explore OR exploitation favors alternative
        """
        # Generate random number for exploration decision
        random_draw = np.random.random()
        should_explore = random_draw < self.state.epsilon_m

        if should_explore:
            # Exploration: switch to alternative schedule
            return True, True
        else:
            # Exploitation: choose schedule with lower learned value
            if self.state.current_state:  # Currently on default
                should_switch = self.state.v_tou < self.state.v_default
            else:  # Currently on TOU
                should_switch = self.state.v_default < self.state.v_tou

            return False, should_switch

    def update_state_for_next_month(self, switch_decision: bool, actual_cost: float) -> None:
        """
        Update state for next month after decision.

        Args:
            switch_decision: Whether household decided to switch schedules
            actual_cost: Actual total cost for current month
        """
        # Update learned values first
        self.update_learned_value(actual_cost)

        # Add to recent cost history
        self.state.recent_costs.append(actual_cost)

        # Update schedule state if switching
        if switch_decision:
            self.state.current_state = not self.state.current_state

        # Update exploration rate with new experience
        self.update_exploration_rate()

        # Advance month
        self.state.month += 1
        if self.state.month > 12:
            self.state.month = 1
            self.state.year += 1

    def get_current_schedule_name(self) -> str:
        """Get current schedule name as string."""
        return "default" if self.state.current_state else "tou"

    def get_alternative_schedule_name(self) -> str:
        """Get alternative schedule name as string."""
        return "tou" if self.state.current_state else "default"

    def get_learning_metrics(self) -> dict:
        """
        Get current learning state metrics.

        Returns:
            Dictionary with learning state information
        """
        return {
            "v_default": self.state.v_default,
            "v_tou": self.state.v_tou,
            "current_state": self.get_current_schedule_name(),
            "epsilon_m": self.state.epsilon_m,
            "alpha_m_learn": self.state.alpha_m_learn,
            "beta": self.state.beta,
            "month": self.state.month,
            "year": self.state.year,
            "value_difference": abs(self.state.v_tou - self.state.v_default),
            "recent_cost_surprise": self.calculate_recent_cost_surprise(),
        }
