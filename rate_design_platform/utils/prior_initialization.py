"""
Prior value initialization using OCHRE pre-simulation results.

This module implements the prior calculation method described in the documentation
to initialize consumer expectations about schedule performance.
"""

from collections.abc import Sequence
from typing import Any

from rate_design_platform.DecisionMaker import ValueLearningController
from rate_design_platform.utils.value_learning_params import ValueLearningParameters


def calculate_prior_values(default_monthly_results: Sequence, tou_monthly_results: Sequence) -> tuple[float, float]:
    """
    Calculate prior expectations from pre-computed OCHRE simulation results.

    From documentation:
    "Before the agent learning begins, we actually run OCHRE to produce
    complete annual building simulations for both schedule types to reduce
    computational load."

    Args:
        default_monthly_results: Pre-computed default schedule monthly bill results
        tou_monthly_results: Pre-computed TOU schedule monthly bill results

    Returns:
        Tuple of (default_annual_cost, tou_annual_cost) - bills only, no comfort penalty
    """
    # Calculate annual costs from monthly bill results only (priors don't include comfort penalty)
    default_annual_cost = sum(result.bill for result in default_monthly_results)
    tou_annual_cost = sum(result.bill for result in tou_monthly_results)

    print("Prior calculation complete (bills only for priors):")
    print(f"  Default annual bill: ${default_annual_cost:.2f}")
    print(f"  TOU annual bill: ${tou_annual_cost:.2f}")
    print(f"  Potential annual bill savings: ${default_annual_cost - tou_annual_cost:.2f}")

    # Show comfort penalties are available but not used in priors
    default_comfort_total = sum(result.comfort_penalty for result in default_monthly_results)
    tou_comfort_total = sum(result.comfort_penalty for result in tou_monthly_results)
    print(f"  Default comfort penalty total: ${default_comfort_total:.2f} (not included in priors)")
    print(f"  TOU comfort penalty total: ${tou_comfort_total:.2f} (not included in priors)")

    return default_annual_cost, tou_annual_cost


def initialize_value_learning_controller_with_priors(
    params: ValueLearningParameters,
    building_chars: Any,
    default_monthly_results: Sequence,
    tou_monthly_results: Sequence,
) -> ValueLearningController:
    """
    Initialize a ValueLearningController with realistic priors from pre-computed OCHRE simulation results.

    Args:
        params: Value learning parameters
        building_chars: Building characteristics
        default_monthly_results: Pre-computed default schedule monthly results
        tou_monthly_results: Pre-computed TOU schedule monthly results

    Returns:
        ValueLearningController with initialized priors
    """
    # Calculate prior values using pre-computed results
    default_annual_cost, tou_annual_cost = calculate_prior_values(default_monthly_results, tou_monthly_results)

    # Create controller with calculated priors
    controller = ValueLearningController(
        params=params,
        building_chars=building_chars,
        default_annual_cost=default_annual_cost,
        tou_annual_cost=tou_annual_cost,
    )

    return controller
    return controller
