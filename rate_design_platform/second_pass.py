"""
Second-Pass TOU Scheduling Decision Model for HPWHs in OCHRE

This module implements the heuristic-based decision model for consumer response
to time-of-use (TOU) electricity rates in residential building simulations.
"""

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from ochre import Dwelling  # type: ignore[import-untyped]
from ochre.utils import default_input_path  # type: ignore[import-untyped]

from rate_design_platform.Analysis import (
    MonthlyMetrics,
    MonthlyResults,
    SimulationResults,
    calculate_annual_metrics,
    calculate_monthly_bill_and_comfort_penalty,
    calculate_monthly_metrics,
    extract_ochre_results,
)
from rate_design_platform.utils.constants import HOURS_PER_DAY, SECONDS_PER_HOUR
from rate_design_platform.utils.rates import (
    MonthlyRateStructure,
    TOUParameters,
    calculate_monthly_intervals,
    create_tou_rates,
    define_peak_hours,
)


def create_operation_schedule(
    current_state: str, monthly_intervals: list[int], TOU_params: TOUParameters, time_step: timedelta
) -> np.ndarray:
    """
    Create operational schedule based on current state

    Args:
        current_state: Current schedule state ("default" or "tou")
        monthly_intervals: Number of intervals for each month in the simulation period
        TOU_params: TOU parameters (uses default if None)
        time_step: Time step of the simulation

    Returns:
        Boolean array where True=operation allowed, False=restricted.
        Length of array is sum of monthly_intervals, which is the total number of intervals in the simulation period.
    """
    if current_state == "default":  # Default schedule. Operation allowed at all times.
        return np.ones(sum(monthly_intervals), dtype=bool)
    else:  # TOU schedule. Operation restricted during peak hours.
        daily_peak_pattern = define_peak_hours(TOU_params, time_step)

        intervals_per_day = int(HOURS_PER_DAY * SECONDS_PER_HOUR / time_step.total_seconds())
        peak_pattern = np.array([], dtype=bool)
        for num_intervals in monthly_intervals:
            # Repeat pattern for the month
            num_days = num_intervals // intervals_per_day  # Number of days in the month
            month_pattern = np.tile(daily_peak_pattern, num_days)

            # Handle remainder
            remainder = num_intervals % intervals_per_day
            if remainder > 0:
                month_pattern = np.concatenate([month_pattern, daily_peak_pattern[:remainder]])

            peak_pattern = np.concatenate([peak_pattern, month_pattern])

        return (~peak_pattern[: sum(monthly_intervals)]).astype(
            bool
        )  # Operation is restricted during peak hours, hence the negation.


def run_ochre_hpwh_dynamic_control(  # type: ignore[no-any-unimported]
    dwelling: Dwelling,
    operation_schedule: np.ndarray,
    time_step: timedelta,
) -> SimulationResults:
    """
    Run OCHRE simulation with dynamic HPWH control based on operation schedule

    Args:
        dwelling: OCHRE dwelling object
        operation_schedule: Boolean array (True=allowed, False=restricted) for each interval
        time_step: Time step of the simulation

    Returns:
        SimulationResults with electricity consumption, tank temps, and unmet demand
    """
    num_intervals = len(operation_schedule)
    # Get water heater equipment
    water_heater = dwelling.get_equipment_by_end_use("Water Heating")
    if water_heater is None:
        msg = "No water heating equipment found in dwelling"
        raise ValueError(msg)

    # Reset dwelling to start state
    dwelling.reset_time()

    # Dynamic control loop - step through each time interval

    for interval_idx, _date in enumerate(dwelling.sim_times):
        if interval_idx >= num_intervals:
            break

        # Set control signal based on operation schedule
        if not operation_schedule[interval_idx]:
            # Peak hour restriction - force water heater off
            control_signal = {"Water Heating": {"Load Fraction": 0}}
        else:
            # Normal operation allowed
            control_signal = {"Water Heating": {"Load Fraction": 1}}

        # Apply control and step simulation
        dwelling.update(control_signal=control_signal)

    # Finalize simulation and get results
    df, _, _ = dwelling.finalize()

    # Check if df is None and handle appropriately
    if df is None:
        raise ValueError()

    # Extract results from OCHRE output
    return extract_ochre_results(df, time_step)


def human_controller(
    current_state: str,
    default_bill: float,
    tou_bill: float,
    tou_comfort_penalty: float,
    TOU_params: TOUParameters,
) -> str:
    """
    Human controller for TOU scheduling

    Args:
        current_state: Current schedule state ("default" or "tou")
        realized_savings: Realized savings (if on TOU)
        unrealized_savings: Unrealized savings (if on default)

    Returns:
        New schedule state ("default" or "tou")
    """
    if current_state == "default":
        anticipated_savings = default_bill - tou_bill
        net_savings = anticipated_savings - TOU_params.c_switch
        if net_savings > 0:
            return "switch"
        else:
            return "stay"
    else:
        realized_savings = default_bill - tou_bill
        net_savings = realized_savings - tou_comfort_penalty
        if net_savings < 0:
            return "switch"
        else:
            return "stay"


def simulate_full_cycle(simulation_type: str, TOU_params: TOUParameters, house_args: dict) -> list[MonthlyMetrics]:
    """
    Simulate complete annual cycle with monthly decision-making

    Args:
        simulation_type: Type of simulation to run ("default" or "tou")
        TOU_params: TOU parameters (uses default if None)
        house_args: Base house arguments dictionary

    Returns:
        List of MonthlyResults for each month
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
    monthly_intervals = [monthly_rate_structure.intervals for monthly_rate_structure in monthly_rate_structure]

    if simulation_type == "default":
        operation_schedule = create_operation_schedule("default", monthly_intervals, TOU_params, time_step)
    else:
        operation_schedule = create_operation_schedule("tou", monthly_intervals, TOU_params, time_step)

    simulation_results = run_ochre_hpwh_dynamic_control(dwelling, operation_schedule, time_step)

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

    monthly_bill_and_comfort_penalty = calculate_monthly_bill_and_comfort_penalty(
        simulation_results, monthly_rates, TOU_params
    )

    return monthly_bill_and_comfort_penalty


def evaluate_human_decision(
    initial_state: str,
    default_monthly_bill: list[float],
    tou_monthly_bill: list[float],
    tou_monthly_comfort_penalty: list[float],
    TOU_params: TOUParameters,
) -> tuple[list[str], list[str]]:  # monthly decisions ("switch" or "stay"), states ("default" or "tou")
    """
    Evaluate human decision

    Args:
        initial_state: Initial state ("default" or "tou")
        default_monthly_bill: List of default monthly bills
        tou_monthly_bill: List of TOU monthly bills
        tou_monthly_comfort_penalty: List of TOU monthly comfort penalties
        TOU_params: TOU parameters

    Returns:
        List of "default" or "tou" for each month
    """
    human_decisions = []
    states = []
    current_state = initial_state
    print(f"default_monthly_bill: {default_monthly_bill}")
    print(f"tou_monthly_bill: {tou_monthly_bill}")
    for i in range(len(default_monthly_bill)):
        states.append(current_state)
        current_decision = human_controller(
            current_state, default_monthly_bill[i], tou_monthly_bill[i], tou_monthly_comfort_penalty[i], TOU_params
        )
        human_decisions.append(current_decision)
        if current_decision == "switch":
            current_state = "tou" if current_state == "default" else "default"
        else:
            current_state = current_state

    return human_decisions, states


def calculate_simulation_months(house_args: dict) -> list[tuple[int, int]]:
    """
    Return list of (year, month) tuples for each month in the simulation period
    """
    start_time = house_args["start_time"]
    end_time = house_args["end_time"]
    year_months = []
    current_time = start_time
    while current_time < end_time:
        year_months.append((current_time.year, current_time.month))
        if current_time.month == 12:
            current_time = current_time.replace(
                year=current_time.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
        else:
            current_time = current_time.replace(
                month=current_time.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
    return year_months


def run_full_simulation(TOU_params: TOUParameters, house_args: dict) -> tuple[list[MonthlyResults], dict[str, float]]:
    """
    Run complete TOU HPWH simulation

    Args:
        simulation_type: Type of simulation to run ("default" or "tou")
        TOU_params: TOU parameters (uses default if None)
        house_args: Base house arguments dictionary

    Returns:
        Tuple of (monthly_results, annual_metrics)
    """
    # Use default parameters if None provided
    if TOU_params is None:
        TOU_params = TOUParameters()

    # Run default annual simulation with house args
    default_monthly_bill_and_comfort_penalty = simulate_full_cycle("default", TOU_params, house_args)
    tou_monthly_bill_and_comfort_penalty = simulate_full_cycle("tou", TOU_params, house_args)

    default_monthly_bill = [
        monthly_bill_and_comfort_penalty.bill
        for monthly_bill_and_comfort_penalty in default_monthly_bill_and_comfort_penalty
    ]
    tou_monthly_bill = [
        monthly_bill_and_comfort_penalty.bill
        for monthly_bill_and_comfort_penalty in tou_monthly_bill_and_comfort_penalty
    ]
    default_monthly_comfort_penalty = [
        monthly_bill_and_comfort_penalty.comfort_penalty
        for monthly_bill_and_comfort_penalty in default_monthly_bill_and_comfort_penalty
    ]
    tou_monthly_comfort_penalty = [
        monthly_bill_and_comfort_penalty.comfort_penalty
        for monthly_bill_and_comfort_penalty in tou_monthly_bill_and_comfort_penalty
    ]

    # Evaluate human decisions
    initial_state = "default"
    monthly_decisions, states = evaluate_human_decision(
        initial_state,
        default_monthly_bill,
        tou_monthly_bill,
        tou_monthly_comfort_penalty,
        TOU_params,
    )

    # Calculate simulation year and months
    simulation_year_months = calculate_simulation_months(house_args)

    monthly_results = calculate_monthly_metrics(
        simulation_year_months,
        monthly_decisions,
        states,
        default_monthly_bill,
        tou_monthly_bill,
        default_monthly_comfort_penalty,
        tou_monthly_comfort_penalty,
    )

    # Calculate annual metrics
    annual_metrics = calculate_annual_metrics(monthly_results)

    return monthly_results, annual_metrics


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

    print("\n=== Full Simulation Test ===")
    try:
        # Load real data and run simulation
        TOU_PARAMS = TOUParameters()
        monthly_results, annual_metrics = run_full_simulation(TOU_PARAMS, HOUSE_ARGS)

        print("Simulation completed")
        print(f"Simulation completed for {len(monthly_results)} months")

        # Display key results
        print("\n=== Annual Results ===")
        for key, value in annual_metrics.items():
            if key.endswith("_percent"):
                print(f"{key}: {value:.1f}%")
            elif "cost" in key or "bill" in key or "saving" in key or "benefit" in key:
                print(f"{key}: ${value:.2f}")
            else:
                print(f"{key}: {value:.2f}")

        # Display monthly state progression
        print("\n=== Monthly State Progression ===")
        states = [r.current_state for r in monthly_results]
        switches = [r.switching_decision for r in monthly_results]
        print(f"States (default, tou): {states}")
        print(f"Switches (switch, stay): {switches}")

    except Exception as e:
        print(f"Simulation failed: {e}")
