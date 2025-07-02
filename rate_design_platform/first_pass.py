"""
First-Pass TOU Scheduling Decision Model for HPWHs in OCHRE

This module implements the heuristic-based decision model for consumer response
to time-of-use (TOU) electricity rates in residential building simulations.
"""

import calendar
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd
from ochre import Dwelling  # type: ignore[import-untyped]

# Define data classes for the simulation


@dataclass
class TOUParameters:
    """TOU rate structure and simulation parameters"""

    r_on: float = 0.28  # $/kWh - peak rate
    r_off: float = 0.12  # $/kWh - off-peak rate
    c_switch: float = 35.0  # $ - switching cost
    alpha: float = 0.15  # $/kWh - comfort penalty factor
    cop: float = 3.0  # heat pump coefficient of performance

    # Peak hours: 2 PM to 8 PM (14:00 to 20:00)
    peak_start_hour: int = 14
    peak_end_hour: int = 20


@dataclass
class MonthlyResults:
    """Results from a single month's simulation"""

    month: int
    current_state: int  # 1=default, 0=TOU
    bill: float  # Monthly electricity bill [$]
    comfort_penalty: float  # Monthly comfort penalty [$]
    switching_decision: int  # 1=switch, 0=stay
    realized_savings: float  # Realized savings (if on TOU)
    unrealized_savings: float  # Unrealized/anticipated savings (if on default)


class SimulationResults(NamedTuple):
    """Results from HPWH simulation for a given month"""

    E_mt: np.ndarray  # Electricity consumption [kWh/15min]
    T_tank_mt: np.ndarray  # Tank temperature [°C]
    D_unmet_mt: np.ndarray  # Electrical unmet demand [W] (operational power deficit)


# Input/Output file paths
bldg_id = 72
upgrade_id = 0
weather_station = "G3400270"

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
input_path = os.path.join(base_path, "inputs")
output_path = os.path.join(base_path, "outputs")
xml_path = os.path.join(input_path, f"bldg{bldg_id:07d}-up{upgrade_id:02d}.xml")
weather_path = os.path.join(input_path, f"{weather_station}.epw")
schedule_path = os.path.join(input_path, f"bldg{bldg_id:07d}-up{upgrade_id:02d}_schedule.csv")

# Check that files exist before proceeding
if not Path(xml_path).exists():
    raise FileNotFoundError(xml_path)
if not Path(weather_path).exists():
    raise FileNotFoundError(weather_path)
if not Path(schedule_path).exists():
    raise FileNotFoundError(schedule_path)

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
    "hpxml_file": xml_path,
    "hpxml_schedule_file": schedule_path,
    "weather_file": weather_path,
}


def calculate_monthly_intervals(month: int, year: int, time_step: timedelta) -> int:
    """
    Calculate number of time_step-long intervals in a given month

    Args:
        month: Month number (1-12)
        year: Year
        time_step: Time step of the simulation

    Returns:
        Number of time_step-long intervals intervals in the month
    """
    days_in_month = calendar.monthrange(year, month)[1]
    return int(days_in_month * (24 * 3600 / time_step.total_seconds()))


def create_ochre_dwelling(house_args: dict, month: int, year: int = 2018) -> Dwelling:  # type: ignore[no-any-unimported]
    """
    Create OCHRE dwelling object for a specific month

    Args:
        house_args: Base house arguments dictionary
        month: Month number (1-12)
        year: Year for simulation (default 2018)

    Returns:
        OCHRE dwelling object configured for monthly simulation
    """
    # Create a copy of house_args to avoid modifying the original
    monthly_args = house_args.copy()

    # Update timing parameters for the specific month
    monthly_args["start_time"] = datetime(year, month, 1, 0, 0)

    # Calculate duration for the specific month
    days_in_month = calendar.monthrange(year, month)[1]
    monthly_args["duration"] = timedelta(days=days_in_month)

    # Create OCHRE dwelling
    dwelling = Dwelling(**monthly_args)

    return dwelling


def define_peak_hours(intervals_per_day: int, time_step: timedelta) -> np.ndarray:
    """
    Define peak hour intervals in a day (15-minute intervals)

    Args:
        intervals_per_day: Number of time_step-long intervals per day
        time_step: Time step of the simulation

    Returns:
        Boolean array indicating peak hours
    """
    params = TOUParameters()
    peak_start_interval = params.peak_start_hour * int(3600 / time_step.total_seconds())
    peak_end_interval = params.peak_end_hour * int(3600 / time_step.total_seconds())

    peak_hours = np.zeros(intervals_per_day, dtype=bool)
    peak_hours[peak_start_interval:peak_end_interval] = True

    return peak_hours


def create_tou_rates(num_intervals: int, time_step: timedelta) -> np.ndarray:
    """
    Create TOU rate structure for entire simulation period

    Args:
        num_intervals: Total number of time_step-long intervals

    Returns:
        Array of electricity rates [$/kWh] for each interval
    """
    params = TOUParameters()
    intervals_per_day = int(24 * 3600 / time_step.total_seconds())

    # Get daily peak hour pattern
    daily_peak_pattern = define_peak_hours(intervals_per_day, time_step)

    # Repeat pattern for entire simulation
    num_days = num_intervals // intervals_per_day
    peak_pattern = np.tile(daily_peak_pattern, num_days)

    # Handle remainder if num_intervals not divisible by 96
    remainder = num_intervals % intervals_per_day
    if remainder > 0:
        peak_pattern = np.concatenate([peak_pattern, daily_peak_pattern[:remainder]])

    # Create rate array
    rates = np.where(peak_pattern, params.r_on, params.r_off)

    return rates


def create_operation_schedule(current_state: int, num_intervals: int) -> np.ndarray:
    """
    Create operational schedule based on current state

    Args:
        current_state: Current schedule state (1=default, 0=TOU)
        num_intervals: Number of intervals for the month

    Returns:
        Binary array where 1=operation allowed, 0=restricted
    """
    if current_state == 1:  # Default schedule
        return np.ones(num_intervals, dtype=int)
    else:  # TOU schedule
        intervals_per_day = 96
        daily_peak_pattern = define_peak_hours(intervals_per_day, timedelta(minutes=15))

        # Repeat pattern for the month
        num_days = num_intervals // intervals_per_day
        peak_pattern = np.tile(daily_peak_pattern, num_days)

        # Handle remainder
        remainder = num_intervals % intervals_per_day
        if remainder > 0:
            peak_pattern = np.concatenate([peak_pattern, daily_peak_pattern[:remainder]])

        # TOU schedule: restricted during peak hours
        return (~peak_pattern[:num_intervals]).astype(int)


def extract_ochre_results(df: pd.DataFrame, num_intervals: int) -> SimulationResults:
    """
    Extract simulation results from OCHRE output DataFrame

    Args:
        df: OCHRE simulation results DataFrame
        num_intervals: Expected number of 15-minute intervals

    Returns:
        SimulationResults with electricity consumption, tank temps, and unmet demand
    """
    # Extract electricity consumption for water heating [kW] -> [kWh/15min]
    E_mt = np.array(df["Water Heating Electric Power (kW)"].values, dtype=float) * 0.25  # Convert kW to kWh/15min

    # Extract tank temperature [°C]
    T_tank_mt = np.array(df["Hot Water Average Temperature (C)"].values, dtype=float)
    # Extract unmet demand [kW] -> [kWh/15min]
    D_unmet_mt = np.array(df["Hot Water Unmet Demand (kW)"].values, dtype=float) * 0.25  # Convert kW to kWh/15min
    # Ensure arrays are correct length
    E_mt = E_mt[:num_intervals] if len(E_mt) >= num_intervals else np.pad(E_mt, (0, num_intervals - len(E_mt)))
    T_tank_mt = (
        T_tank_mt[:num_intervals]
        if len(T_tank_mt) >= num_intervals
        else np.pad(T_tank_mt, (0, num_intervals - len(T_tank_mt)), constant_values=55.0)
    )
    D_unmet_mt = (
        D_unmet_mt[:num_intervals]
        if len(D_unmet_mt) >= num_intervals
        else np.pad(D_unmet_mt, (0, num_intervals - len(D_unmet_mt)))
    )

    return SimulationResults(E_mt, T_tank_mt, D_unmet_mt)


def run_ochre_hpwh_dynamic_control(  # type: ignore[no-any-unimported]
    dwelling: "Dwelling",
    operation_schedule: np.ndarray,
    month: int,
) -> SimulationResults:
    """
    Run OCHRE simulation with dynamic HPWH control based on operation schedule

    Args:
        dwelling: OCHRE dwelling object
        operation_schedule: Binary array (1=allowed, 0=restricted) for each 15-min interval
        month: Month number (1-12)

    Returns:
        SimulationResults with electricity consumption, tank temps, and unmet demand
    """
    num_intervals = len(operation_schedule)
    # TODO: CHECK IF IT'S A HEATPUMP WATER HEATER
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
        if operation_schedule[interval_idx] == 0:
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
    return extract_ochre_results(df, num_intervals)


def building_simulation_controller(month: int, operation_schedule: np.ndarray, house_args: dict) -> SimulationResults:
    """
    OCHRE-based building simulation controller

    Args:
        month: Month number (1-12)
        operation_schedule: Binary array of operation permissions
        house_args: Base house arguments dictionary
    Returns:
        SimulationResults with OCHRE-based electricity consumption, tank temps, and unmet demand
    """
    # Create OCHRE dwelling for this monthly simulation
    dwelling = create_ochre_dwelling(house_args, month)
    return run_ochre_hpwh_dynamic_control(dwelling, operation_schedule, month)


def human_controller(current_state: int, realized_savings: float = 0.0, unrealized_savings: float = 0.0) -> int:
    """
    Static human decision controller (returns no switching)

    Args:
        current_state: Current schedule state (1=default, 0=TOU)
        realized_savings: Realized net savings (for TOU�default decision)
        unrealized_savings: Unrealized net savings (for default�TOU decision)

    Returns:
        Binary switching decision (1=switch, 0=stay)
    """
    # Static return: never switch
    return 0


def calculate_monthly_bill(consumption: np.ndarray, rates: np.ndarray) -> float:
    """Calculate monthly electricity bill"""
    return float(np.sum(consumption * rates))


def calculate_comfort_penalty(unmet_demand_watts: np.ndarray, alpha: float) -> float:
    """
    Calculate comfort penalty in $ from electrical unmet demand

    Args:
        unmet_demand_watts: Electrical unmet demand [W] at each interval (15-min intervals)
        alpha: Comfort penalty factor [$/kWh]

    Returns:
        Comfort penalty in $
    """
    # Convert W to kWh: each interval is 15 minutes = 0.25 hours
    # Total unmet energy = sum(W * 0.25 hours) / 1000 W/kW = sum(W) / 4000 kWh
    total_unmet_kwh = np.sum(unmet_demand_watts) * 0.25 / 1000.0
    return float(alpha * total_unmet_kwh)


def simulate_month_both_schedules(
    month: int, year: int, time_step: timedelta, rates: np.ndarray, params: TOUParameters, house_args: dict
) -> tuple[SimulationResults, SimulationResults, float, float]:
    """
    Simulate both default and TOU schedules for comparison

    Args:
        month: Month number (1-12)
        year: Year
        time_step: Time step of the simulation
        rates: Electricity rates for each interval [$/kWh]
        params: TOU parameters
        house_args: Base house arguments dictionary

    Returns:
        Tuple of (default_results, tou_results, default_bill, tou_bill)
    """
    num_intervals = calculate_monthly_intervals(month, year, time_step)

    # Create operational schedules
    default_schedule = create_operation_schedule(1, num_intervals)  # Always allowed
    tou_schedule = create_operation_schedule(0, num_intervals)  # Peak restricted

    # Run building simulations
    default_results = building_simulation_controller(month, default_schedule, house_args)
    tou_results = building_simulation_controller(month, tou_schedule, house_args)

    # Calculate bills
    default_bill = calculate_monthly_bill(default_results.E_mt, rates)
    tou_bill = calculate_monthly_bill(tou_results.E_mt, rates)

    return default_results, tou_results, default_bill, tou_bill


def simulate_single_month(
    month: int,
    year: int,
    time_step: timedelta,
    current_state: int,
    rates: np.ndarray,
    params: TOUParameters,
    house_args: dict,
) -> MonthlyResults:
    """
    Simulate a single month with decision logic

    Args:
        month: Month number (1-12)
        year: Year
        time_step: Time step of the simulation
        current_state: Current schedule state (1=default, 0=TOU)
        rates: Electricity rates for each interval [$/kWh]
        params: TOU parameters
        house_args: Base house arguments dictionary

    Returns:
        MonthlyResults with all monthly outcomes
    """
    # Simulate both schedules for comparison
    default_results, tou_results, default_bill, tou_bill = simulate_month_both_schedules(
        month, year, time_step, rates, params, house_args
    )

    if current_state == 1:  # Currently on Default Schedule (Case A)
        # Calculate unrealized anticipated savings from switching to TOU
        actual_bill = default_bill
        actual_comfort = calculate_comfort_penalty(default_results.D_unmet_mt, params.alpha)

        # Anticipated savings (no comfort penalty considered)
        bill_savings = default_bill - tou_bill
        unrealized_net_savings = bill_savings - params.c_switch

        # Make switching decision based on anticipated savings
        switching_decision = human_controller(current_state, 0.0, unrealized_net_savings)

        return MonthlyResults(
            month=month,
            current_state=current_state,
            bill=actual_bill,
            comfort_penalty=actual_comfort,
            switching_decision=switching_decision,
            realized_savings=0.0,  # No realized savings yet
            unrealized_savings=unrealized_net_savings,
        )

    else:  # Currently on TOU Schedule (Case B)
        # Calculate realized performance with actual comfort penalty
        actual_bill = tou_bill
        actual_comfort = calculate_comfort_penalty(tou_results.D_unmet_mt, params.alpha)

        # Realized savings (including comfort penalty)
        bill_savings = default_bill - tou_bill
        realized_net_savings = bill_savings - params.c_switch - actual_comfort

        # Make continuation decision based on realized performance
        switching_decision = human_controller(current_state, realized_net_savings, 0.0)

        return MonthlyResults(
            month=month,
            current_state=current_state,
            bill=actual_bill,
            comfort_penalty=actual_comfort,
            switching_decision=switching_decision,
            realized_savings=realized_net_savings,
            unrealized_savings=0.0,  # No unrealized savings when on TOU
        )


def simulate_annual_cycle(params: TOUParameters, house_args: dict) -> list[MonthlyResults]:
    """
    Simulate complete annual cycle with monthly decision-making

    Args:
        params: TOU parameters (uses default if None)
        house_args: Base house arguments dictionary

    Returns:
        List of MonthlyResults for each month
    """

    # Initialize state (start on default schedule)
    current_state = 1
    monthly_results = []

    # Get start and end times from house_args
    start_time = house_args["start_time"]
    end_time = house_args["end_time"]

    # Generate chronological list of (year, month) tuples
    current_date = start_time.replace(day=1)  # Start at beginning of start month
    end_date = end_time.replace(day=1)  # End at beginning of end month

    time_step = house_args["time_res"]

    while current_date <= end_date:
        year = current_date.year
        month = current_date.month

        # Calculate monthly intervals and rates
        num_intervals = calculate_monthly_intervals(month, year, time_step)
        monthly_rates = create_tou_rates(num_intervals, time_step)

        # Simulate month
        result = simulate_single_month(month, year, time_step, current_state, monthly_rates, params, house_args)

        monthly_results.append(result)

        # Update state for next month based on switching decision
        if result.switching_decision == 1:
            current_state = 1 - current_state  # Toggle state

        # Move to next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

    return monthly_results


def calculate_annual_metrics(monthly_results: list[MonthlyResults]) -> dict[str, float]:
    """
    Calculate annual performance metrics from monthly results

    Args:
        monthly_results: List of MonthlyResults for each month

    Returns:
        Dictionary of annual metrics
    """
    total_bills = sum(r.bill for r in monthly_results)
    total_comfort_penalty = sum(r.comfort_penalty for r in monthly_results)
    total_switches = sum(r.switching_decision for r in monthly_results)

    # Calculate TOU adoption rate
    tou_months = sum(1 for r in monthly_results if r.current_state == 0)
    tou_adoption_rate = tou_months / 12 * 100

    # Calculate total realized savings (only when on TOU)
    total_realized_savings = sum(r.realized_savings for r in monthly_results if r.current_state == 0)

    return {
        "total_annual_bills": total_bills,
        "total_comfort_penalty": total_comfort_penalty,
        "total_switching_costs": total_switches * TOUParameters().c_switch,
        "total_realized_savings": total_realized_savings,
        "net_annual_benefit": total_realized_savings
        - (total_switches * TOUParameters().c_switch)
        - total_comfort_penalty,
        "tou_adoption_rate_percent": tou_adoption_rate,
        "annual_switches": total_switches,
        "average_monthly_bill": total_bills / 12,
    }


def run_full_simulation(params=None, house_args=HOUSE_ARGS) -> tuple[list[MonthlyResults], dict[str, float]]:  # type: ignore[no-untyped-def]
    """
    Run complete TOU HPWH simulation

    Args:
        csv_path: Path to input CSV file (optional, for backward compatibility)
        params: TOU parameters (uses default if None)

    Returns:
        Tuple of (monthly_results, annual_metrics)
    """
    # Use default parameters if None provided
    if params is None:
        params = TOUParameters()

    # Run annual simulation with house args
    monthly_results = simulate_annual_cycle(params, house_args)

    # Calculate annual metrics
    annual_metrics = calculate_annual_metrics(monthly_results)

    return monthly_results, annual_metrics


if __name__ == "__main__":
    # Test full simulation with sample data
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
        print(f"States (1=default, 0=TOU): {states}")
        print(f"Switches (1=switch, 0=stay): {switches}")

    except Exception as e:
        print(f"Simulation failed: {e}")
        print("This is expected if input files are not available or properly formatted")
