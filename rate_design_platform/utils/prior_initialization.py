"""
Prior value initialization using OCHRE pre-simulation results.

This module implements the prior calculation method described in the documentation
to initialize consumer expectations about schedule performance.
"""

import os

from rate_design_platform.second_pass import simulate_full_cycle
from rate_design_platform.utils.rates import TOUParameters
from rate_design_platform.utils.value_learning_params import ValueLearningParameters


def calculate_prior_values(
    house_args: dict, TOU_params: TOUParameters, output_path_suffix: str = "prior_calc"
) -> tuple[float, float]:
    """
    Calculate prior expectations by running complete OCHRE simulations.

    From documentation:
    "Before the agent learning begins, we actually run OCHRE to produce
    complete annual building simulations for both schedule types to reduce
    computational load."

    Args:
        house_args: OCHRE house arguments
        TOU_params: TOU parameters
        output_path_suffix: Suffix for output directory to avoid conflicts

    Returns:
        Tuple of (default_annual_cost, tou_annual_cost)
    """
    # Create separate output directories to avoid conflicts
    original_output_path = house_args.get("output_path", "")

    # Update house args with separate output paths for prior calculations
    default_house_args = house_args.copy()
    tou_house_args = house_args.copy()

    if original_output_path:
        base_dir = os.path.dirname(original_output_path)
        default_house_args["output_path"] = os.path.join(base_dir, f"prior_default_{output_path_suffix}")
        tou_house_args["output_path"] = os.path.join(base_dir, f"prior_tou_{output_path_suffix}")

    # Run full annual simulations for both schedule types
    print("Calculating priors: Running default schedule simulation...")
    default_monthly_results = simulate_full_cycle("default", TOU_params, default_house_args)

    print("Calculating priors: Running TOU schedule simulation...")
    tou_monthly_results = simulate_full_cycle("tou", TOU_params, tou_house_args)

    # Calculate annual costs from monthly results
    default_annual_cost = sum(result.bill + result.comfort_penalty for result in default_monthly_results)
    tou_annual_cost = sum(result.bill + result.comfort_penalty for result in tou_monthly_results)

    print("Prior calculation complete:")
    print(f"  Default annual cost: ${default_annual_cost:.2f}")
    print(f"  TOU annual cost: ${tou_annual_cost:.2f}")
    print(f"  Potential annual savings: ${default_annual_cost - tou_annual_cost:.2f}")

    return default_annual_cost, tou_annual_cost


def initialize_value_learning_controller_with_priors(
    params: ValueLearningParameters,
    building_chars,  # BuildingCharacteristics - avoiding import for now
    house_args: dict,
    TOU_params: TOUParameters,
):
    """
    Initialize a ValueLearningController with realistic priors from OCHRE simulation.

    Args:
        params: Value learning parameters
        building_chars: Building characteristics
        house_args: OCHRE house arguments
        TOU_params: TOU parameters

    Returns:
        ValueLearningController with initialized priors
    """
    from rate_design_platform.DecisionMaker import ValueLearningController

    # Calculate prior values using OCHRE simulations
    default_annual_cost, tou_annual_cost = calculate_prior_values(house_args, TOU_params)

    # Create controller with calculated priors
    controller = ValueLearningController(
        params=params,
        building_chars=building_chars,
        default_annual_cost=default_annual_cost,
        tou_annual_cost=tou_annual_cost,
    )

    return controller


def create_default_value_learning_setup(
    building_chars,  # BuildingCharacteristics
    house_args: dict,
    TOU_params: TOUParameters = None,
    params: ValueLearningParameters = None,
):
    """
    Create a complete value learning setup with reasonable defaults.

    Args:
        building_chars: Building characteristics
        house_args: OCHRE house arguments
        TOU_params: TOU parameters (uses default if None)
        params: Value learning parameters (uses default if None)

    Returns:
        ValueLearningController ready for simulation
    """
    if TOU_params is None:
        TOU_params = TOUParameters()

    if params is None:
        params = ValueLearningParameters()

    return initialize_value_learning_controller_with_priors(params, building_chars, house_args, TOU_params)
