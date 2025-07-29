"""
Second-Pass TOU Scheduling Decision Model for HPWHs in OCHRE

This module implements the heuristic-based decision model for consumer response
to time-of-use (TOU) electricity rates in residential building simulations.
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from ochre import Dwelling  # type: ignore[import-untyped]
from ochre.utils import default_input_path  # type: ignore[import-untyped]

from rate_design_platform.Analysis import (
    MonthlyMetrics,
    calculate_monthly_bill,
    calculate_value_learning_annual_metrics,
    calculate_value_learning_monthly_metrics,
)
from rate_design_platform.DecisionMaker import ValueLearningController
from rate_design_platform.OchreSimulator import calculate_simulation_months, run_ochre_wh_dynamic_control
from rate_design_platform.utils.rates import (
    MonthlyRateStructure,
    TOUParameters,
    calculate_monthly_intervals,
    create_operation_schedule,
    create_tou_rates,
)


def simulate_full_cycle(
    simulation_type: str, TOU_params: TOUParameters, house_args: dict
) -> tuple[list[MonthlyMetrics], list[float]]:
    """
    Simulate complete annual cycle to get bills and unmet demand for prior initialization

    Args:
        simulation_type: Type of simulation to run ("default" or "tou")
        TOU_params: TOU parameters (uses default if None)
        house_args: Base house arguments dictionary

    Returns:
        Tuple of (monthly_bills, monthly_unmet_demand_totals)
        - monthly_bills: List of MonthlyMetrics with bill amounts only
        - monthly_unmet_demand_totals: List of monthly unmet demand totals in kWh
    """
    if TOU_params is None:
        TOU_params = TOUParameters()

    if house_args is None:
        house_args = HOUSE_ARGS

    # Create separate output directories for each simulation type
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    simulation_output_path = os.path.join(base_path, "outputs", f"{simulation_type}_simulation")

    # Create the directory if it doesn't exist
    os.makedirs(simulation_output_path, exist_ok=True)

    # Update house_args with the new output path
    house_args.update({"output_path": simulation_output_path})
    dwelling = Dwelling(**house_args)

    start_time = house_args["start_time"]
    end_time = house_args["end_time"]
    time_step = house_args["time_res"]
    monthly_rate_structure = calculate_monthly_intervals(start_time, end_time, time_step)

    if simulation_type == "default":
        operation_schedule = create_operation_schedule("default", monthly_rate_structure, TOU_params, time_step)
    else:
        operation_schedule = create_operation_schedule("tou", monthly_rate_structure, TOU_params, time_step)

    simulation_results = run_ochre_wh_dynamic_control(dwelling, operation_schedule, time_step)

    monthly_rates = create_tou_rates(simulation_results.Time, time_step, TOU_params)

    # Combine monthly interals and rates into a single list
    monthly_rates = [
        MonthlyRateStructure(
            year=monthly_rate_structure.year,
            month=monthly_rate_structure.month,
            intervals=monthly_rate_structure.intervals,
            rates=monthly_rate_structure.rates,
        )
        for monthly_rate_structure in monthly_rates
    ]

    # Calculate monthly bills only
    monthly_bills = calculate_monthly_bill(simulation_results, monthly_rates)

    # Extract monthly unmet demand totals for later comfort penalty calculation
    monthly_unmet_demand = []
    time_stamps = simulation_results.Time
    unmet_demand_kWh = simulation_results.D_unmet_mt

    # Group by month
    current_month = None
    month_start_idx = 0

    for i, timestamp in enumerate(time_stamps):
        try:
            month = timestamp.month
        except AttributeError:
            month = timestamp.astype("datetime64[M]").astype(int) % 12 + 1

        if current_month is None:
            current_month = month
        elif month != current_month:
            # Month changed, calculate total unmet demand for the previous month
            month_intervals = i - month_start_idx
            month_demand = unmet_demand_kWh[month_start_idx : month_start_idx + month_intervals]
            monthly_unmet_demand.append(float(np.sum(month_demand)))

            # Start new month
            current_month = month
            month_start_idx = i

    # Handle the last month
    if month_start_idx < len(time_stamps):
        month_intervals = len(time_stamps) - month_start_idx
        month_demand = unmet_demand_kWh[month_start_idx : month_start_idx + month_intervals]
        monthly_unmet_demand.append(float(np.sum(month_demand)))

    return monthly_bills, monthly_unmet_demand


def evaluate_value_learning_decision(
    controller: ValueLearningController,
    default_monthly_bill: list[float],
    tou_monthly_bill: list[float],
    default_monthly_unmet_demand: list[float],
    tou_monthly_unmet_demand: list[float],
) -> tuple[list[str], list[str], list[dict]]:
    """
    Evaluate value learning decisions for each month.

    Args:
        controller: ValueLearningController instance
        default_monthly_bill: List of default monthly bills
        tou_monthly_bill: List of TOU monthly bills
        default_monthly_unmet_demand: List of default monthly unmet demand in kWh
        tou_monthly_unmet_demand: List of TOU monthly unmet demand in kWh

    Returns:
        Tuple of (monthly_decisions, states, learning_metrics_history)
        - monthly_decisions: List of "switch" or "stay" for each month
        - states: List of "default" or "tou" for each month
        - learning_metrics_history: List of learning metrics for each month
    """
    decisions = []
    states = []
    learning_metrics_history = []

    print("Starting value learning simulation...")
    print(f"Initial state: {controller.get_current_state()}")
    print(f"Initial learning state: {controller.get_learning_state()}")

    # Get building-specific beta for comfort penalty calculation
    building_beta = controller.state_manager.state.beta
    print(f"Using building-specific beta for comfort penalties: {building_beta:.4f}")

    for i in range(len(default_monthly_bill)):
        # Record current state before decision
        current_state = controller.get_current_state()
        states.append(current_state)

        # Calculate comfort penalties on-the-fly using building-specific beta
        default_comfort_penalty = building_beta * default_monthly_unmet_demand[i]
        tou_comfort_penalty = building_beta * tou_monthly_unmet_demand[i]

        # Make value learning decision
        decision, learning_metrics = controller.evaluate_TOU(
            default_monthly_bill[i],
            tou_monthly_bill[i],
            default_comfort_penalty,
            tou_comfort_penalty,
        )

        decisions.append(decision)
        learning_metrics_history.append(learning_metrics)

        # Log monthly progress
        print(
            f"Month {i + 1}: {current_state} -> {decision} "
            f"(exploration rate: {learning_metrics['epsilon_m']:.3f}, "
            f"value diff: ${learning_metrics['value_difference']:.2f})"
        )

    # Print final learning state
    final_state = controller.get_learning_state()
    print("\nFinal learning state:")
    print(f"  V_default: ${final_state['v_default']:.2f}")
    print(f"  V_tou: ${final_state['v_tou']:.2f}")
    print(f"  Final state: {final_state['current_state']}")
    print(f"  Final exploration rate: {final_state['epsilon_m']:.3f}")

    return decisions, states, learning_metrics_history


def run_value_learning_simulation(
    TOU_params: TOUParameters, house_args: dict, building_xml_path: Optional[str] = None
) -> tuple[list, dict[str, float], list[dict]]:
    """
    Run complete value learning TOU HPWH simulation.

    Args:
        TOU_params: TOU parameters
        house_args: Base house arguments dictionary
        building_xml_path: Path to building XML file (for building characteristics)

    Returns:
        Tuple of (monthly_results, annual_metrics, learning_metrics_history)
    """
    from rate_design_platform.utils.building_characteristics import (
        enrich_building_characteristics,
        parse_building_xml,
    )
    from rate_design_platform.utils.prior_initialization import initialize_value_learning_controller_with_priors
    from rate_design_platform.utils.value_learning_params import ValueLearningParameters

    # Use default parameters if None provided
    if TOU_params is None:
        TOU_params = TOUParameters()

    # Parse building characteristics
    if building_xml_path:
        building_chars = parse_building_xml(building_xml_path)
        building_chars = enrich_building_characteristics(building_chars)
    else:
        # Use default building characteristics for the XML file in house_args
        hpxml_file = house_args.get("hpxml_file")
        if hpxml_file:
            building_chars = parse_building_xml(hpxml_file)
            building_chars = enrich_building_characteristics(building_chars)
        else:
            # Fallback to default characteristics
            from rate_design_platform.utils.building_characteristics import BuildingCharacteristics

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

    # 1. Initialize ValueLearning Parameters
    params = ValueLearningParameters()
    print(f"Initialized value learning parameters with beta_base: {params.beta_base}")

    # 2. Run Year-Long Simulations to get bills and unmet demand
    print("Running year-long pre-simulations to get bills and unmet demand...")
    default_monthly_bills, default_monthly_unmet_demand = simulate_full_cycle("default", TOU_params, house_args)
    tou_monthly_bills, tou_monthly_unmet_demand = simulate_full_cycle("tou", TOU_params, house_args)

    # 3. Initialize Priors (bills only)
    print("Initializing controller with priors (bills only)...")
    controller = initialize_value_learning_controller_with_priors(
        params, building_chars, default_monthly_bills, tou_monthly_bills
    )

    # 4. Run Value Learning Simulation
    print("Starting value learning simulation with on-the-fly comfort penalty calculation...")

    # Extract monthly bill data
    default_monthly_bill = [result.bill for result in default_monthly_bills]
    tou_monthly_bill = [result.bill for result in tou_monthly_bills]

    # Run value learning decision process with unmet demand for on-the-fly comfort penalty calculation
    monthly_decisions, states, learning_metrics_history = evaluate_value_learning_decision(
        controller, default_monthly_bill, tou_monthly_bill, default_monthly_unmet_demand, tou_monthly_unmet_demand
    )

    # Calculate simulation year and months
    simulation_year_months = calculate_simulation_months(house_args)

    # Calculate comfort penalties for final metrics using building-specific beta
    building_beta = controller.state_manager.state.beta
    default_monthly_comfort_penalty = [building_beta * unmet for unmet in default_monthly_unmet_demand]
    tou_monthly_comfort_penalty = [building_beta * unmet for unmet in tou_monthly_unmet_demand]

    # Calculate value learning monthly metrics
    monthly_results = calculate_value_learning_monthly_metrics(
        simulation_year_months,
        monthly_decisions,
        states,
        default_monthly_bill,
        tou_monthly_bill,
        default_monthly_comfort_penalty,
        tou_monthly_comfort_penalty,
        learning_metrics_history,
    )

    # Calculate value learning annual metrics
    annual_metrics = calculate_value_learning_annual_metrics(monthly_results)

    return monthly_results, annual_metrics, learning_metrics_history


def _calculate_value_learning_annual_metrics(learning_metrics_history: list[dict]) -> dict[str, float]:
    """
    Calculate annual metrics specific to value learning.

    Args:
        learning_metrics_history: List of monthly learning metrics

    Returns:
        Dictionary with value learning annual metrics
    """
    if not learning_metrics_history:
        return {}

    # Calculate exploration vs exploitation rates
    exploration_decisions = sum(
        1 for metrics in learning_metrics_history if metrics.get("decision_type") == "exploration"
    )
    total_decisions = len(learning_metrics_history)
    exploration_rate = (exploration_decisions / total_decisions * 100) if total_decisions > 0 else 0

    # Final value difference
    final_metrics = learning_metrics_history[-1]
    final_value_difference = final_metrics.get("value_difference", 0)

    # TOU adoption rate (percentage of months on TOU)
    tou_months = sum(1 for metrics in learning_metrics_history if metrics.get("current_schedule") == "tou")
    tou_adoption_rate = (tou_months / total_decisions * 100) if total_decisions > 0 else 0

    # Average exploration rate over the year
    avg_exploration_rate = sum(metrics.get("epsilon_m", 0) for metrics in learning_metrics_history) / total_decisions

    # Learning convergence (difference between first and last value difference)
    first_value_diff = learning_metrics_history[0].get("value_difference", 0)
    last_value_diff = final_value_difference
    learning_convergence = abs(last_value_diff - first_value_diff)

    return {
        "annual_exploration_rate_percent": exploration_rate,
        "final_value_difference": final_value_difference,
        "tou_adoption_rate_percent": tou_adoption_rate,
        "avg_exploration_rate": avg_exploration_rate,
        "learning_convergence": learning_convergence,
        "final_v_default": final_metrics.get("v_default", 0),
        "final_v_tou": final_metrics.get("v_tou", 0),
    }


if __name__ == "__main__":
    # Test full simulation with sample data

    # # Input/Output file paths
    # bldg_id = 72
    # upgrade_id = 0
    # weather_station = "G3400270"

    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    # input_path = os.path.join(base_path, "inputs")
    output_path = os.path.join(base_path, "outputs")
    # xml_path = os.path.join(input_path, f"bldg{bldg_id:07d}-up{upgrade_id:02d}.xml")
    # weather_path = os.path.join(input_path, f"{weather_station}.epw")
    # schedule_path = os.path.join(input_path, f"bldg{bldg_id:07d}-up{upgrade_id:02d}_schedule.csv")

    # # Check that files exist before proceeding
    # if not Path(xml_path).exists():
    #     raise FileNotFoundError(xml_path)
    # if not Path(weather_path).exists():
    #     raise FileNotFoundError(weather_path)
    # if not Path(schedule_path).exists():
    #     raise FileNotFoundError(schedule_path)

    # Simulation parameters
    year = 2018
    month = 1
    start_date = 1
    start_time = datetime(year, month, start_date, 0, 0)  # (Year, Month, Day, Hour, Min)
    duration = timedelta(days=365)
    time_step = timedelta(minutes=15)
    end_time = start_time + duration
    sim_times = pd.date_range(start=start_time, end=end_time, freq=time_step)[:-1]
    initialization_time = timedelta(days=1)

    HOUSE_ARGS = {
        # Timing parameters (will be updated per month)
        "start_time": start_time,
        "end_time": end_time,
        "time_res": time_step,
        "duration": duration,
        "initialization_time": initialization_time,
        # Output settings
        "save_results": True,
        "verbosity": 9,
        "metrics_verbosity": 7,
        "output_path": output_path,
        # Input file settings
        "hpxml_file": os.path.join(default_input_path, "Input Files", "bldg0112631-up11.xml"),
        "hpxml_schedule_file": os.path.join(default_input_path, "Input Files", "bldg0112631_schedule.csv"),
        "weather_file": os.path.join(default_input_path, "Weather", "USA_CO_Denver.Intl.AP.725650_TMY3.epw"),
    }

    print("=== Value Learning Simulation Test ===")
    try:
        # Run value learning simulation
        TOU_PARAMS = TOUParameters()
        monthly_results, annual_metrics, learning_history = run_value_learning_simulation(TOU_PARAMS, HOUSE_ARGS)

        print("Value Learning Simulation completed")
        print(f"Simulation completed for {len(monthly_results)} months")

        # Display key results
        print("\n=== Annual Results ===")
        for key, value in annual_metrics.items():
            if key.endswith("_percent"):
                print(f"{key}: {value:.1f}%")
            elif "cost" in key or "bill" in key or "saving" in key or "benefit" in key or key.startswith("final_v_"):
                print(f"{key}: ${value:.2f}")
            elif "rate" in key or "convergence" in key or "difference" in key:
                print(f"{key}: {value:.3f}")
            else:
                print(f"{key}: {value:.2f}")

        # Display monthly state progression
        print("\n=== Monthly Learning Progression ===")
        states = [r.current_state for r in monthly_results]
        switches = [r.switching_decision for r in monthly_results]
        exploration_types = [metrics.get("decision_type", "unknown") for metrics in learning_history]

        print(f"States (default, tou): {states}")
        print(f"Switches (switch, stay): {switches}")
        print(f"Decision types (exploration, exploitation): {exploration_types}")

        # Show learning trajectory
        print("\n=== Learning Value Trajectory ===")
        for i, metrics in enumerate(learning_history[:6]):  # Show first 6 months
            print(
                f"Month {i + 1}: V_default=${metrics['v_default']:.2f}, "
                f"V_tou=${metrics['v_tou']:.2f}, "
                f"epsilon={metrics['epsilon_m']:.3f}"
            )
        if len(learning_history) > 6:
            print("...")
            final_metrics = learning_history[-1]
            print(
                f"Month 12: V_default=${final_metrics['v_default']:.2f}, "
                f"V_tou=${final_metrics['v_tou']:.2f}, "
                f"epsilon={final_metrics['epsilon_m']:.3f}"
            )

    except Exception as e:
        print(f"Value Learning Simulation failed: {e}")
        import traceback

        traceback.print_exc()
