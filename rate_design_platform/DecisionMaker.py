from typing import Optional

from rate_design_platform.utils.building_characteristics import BuildingCharacteristics
from rate_design_platform.utils.learning_state import ValueLearningStateManager
from rate_design_platform.utils.value_learning_params import ValueLearningParameters


class ValueLearningController:
    """
    Value learning controller implementing exploration/exploitation framework.

    This controller learns schedule performance through direct experience and
    makes decisions using household-specific exploration and learning rates.
    """

    def __init__(
        self,
        params: ValueLearningParameters,
        building_chars: BuildingCharacteristics,
        default_annual_cost: Optional[float] = None,
        tou_annual_cost: Optional[float] = None,
    ):
        """
        Initialize value learning controller.

        Args:
            params: Value learning parameters
            building_chars: Building characteristics
            default_annual_cost: Annual cost for default schedule (for priors)
            tou_annual_cost: Annual cost for TOU schedule (for priors)
        """
        self.params = params
        self.building_chars = building_chars

        # Initialize state manager
        self.state_manager = ValueLearningStateManager(
            params=params,
            building_chars=building_chars,
            default_annual_cost=default_annual_cost or 0.0,
            tou_annual_cost=tou_annual_cost or 0.0,
        )

    def evaluate_TOU(
        self, default_bill: float, tou_bill: float, default_comfort_penalty: float, tou_comfort_penalty: float
    ) -> tuple[str, dict]:
        """
        Make monthly TOU decision using value learning approach.

        Args:
            default_bill: Monthly bill for default schedule
            tou_bill: Monthly bill for TOU schedule
            default_comfort_penalty: Comfort penalty for default schedule
            tou_comfort_penalty: Comfort penalty for TOU schedule

        Returns:
            Tuple of (decision, learning_metrics)
            - decision: "switch" or "stay"
            - learning_metrics: Dictionary with learning state information
        """
        # Calculate total cost for current schedule
        current_schedule = self.state_manager.get_current_schedule_name()
        if current_schedule == "default":
            actual_total_cost = default_bill + default_comfort_penalty
        else:
            actual_total_cost = tou_bill + tou_comfort_penalty

        # Make exploration vs exploitation decision
        should_explore, should_switch = self.state_manager.make_decision()

        # Convert to string decision
        decision = "switch" if should_switch else "stay"

        # Update state for next month
        self.state_manager.update_state_for_next_month(should_switch, actual_total_cost)

        # Get learning metrics
        learning_metrics = self.state_manager.get_learning_metrics()
        learning_metrics.update({
            "decision_type": "exploration" if should_explore else "exploitation",
            "actual_total_cost": actual_total_cost,
            "current_schedule": current_schedule,
        })

        return decision, learning_metrics

    def get_current_state(self) -> str:
        """Get current schedule state."""
        return self.state_manager.get_current_schedule_name()

    def get_learning_state(self) -> dict:
        """Get current learning state metrics."""
        return self.state_manager.get_learning_metrics()

    def reset_for_new_year(self) -> None:
        """Reset state for new simulation year while preserving learned values."""
        # Learning values persist across years, just reset month counter
        self.state_manager.state.month = 1
        self.state_manager.state.year += 1
