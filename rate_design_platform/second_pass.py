"""
Second-Pass TOU Scheduling Decision Model for HPWHs in OCHRE

This module implements the heuristic-based decision model for consumer response
to time-of-use (TOU) electricity rates in residential building simulations.
"""

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd
from ochre import Dwelling  # type: ignore[import-untyped]

# Define constants
seconds_per_hour = 3600
hours_per_day = 24


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

    year: int
    month: int
    current_state: str  # "default" or "tou"
    bill: float  # Monthly electricity bill [$]
    comfort_penalty: float  # Monthly comfort penalty [$]
    switching_decision: str  # "switch" or "stay"
    realized_savings: float  # Realized savings (if on TOU)
    unrealized_savings: float  # Unrealized/anticipated savings (if on default)


@dataclass
class MonthlyMetrics:
    """Metrics from a single month's simulation"""

    year: int
    month: int
    bill: float
    comfort_penalty: float


class SimulationResults(NamedTuple):
    """Results from HPWH simulation for a given month"""

    Time: np.ndarray  # Datetime array
    E_mt: np.ndarray  # Electricity consumption [kWh]
    T_tank_mt: np.ndarray  # Tank temperature [°C]
    D_unmet_mt: np.ndarray  # Electrical unmet demand [kWh] (operational power deficit)


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
duration = timedelta(days=61)
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


def calculate_monthly_intervals(start_time: datetime, end_time: datetime, time_step: timedelta) -> list[int]:
    """
    Calculate number of time_step-long intervals in a given month

    Args:
        start_time: Start time of the simulation
        end_time: End time of the simulation
        time_step: Time step of the simulation

    Returns:
        Number of time_step-long intervals intervals for each month from start_time to end_time
    """
    intervals = []
    current_time = start_time
    while current_time < end_time:
        # Calculate start and end of the current month within our time range
        month_start = current_time
        # Find the start of the next month
        if current_time.month == 12:
            next_month_start = current_time.replace(
                year=current_time.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
        else:
            next_month_start = current_time.replace(
                month=current_time.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
        month_end = min(next_month_start, end_time)
        # Calculate the duration of this month (within our time range)
        month_duration = month_end - month_start
        # Calculate number of intervals in this month
        num_intervals = int(month_duration.total_seconds() / time_step.total_seconds())
        intervals.append(num_intervals)

        # Move to next month
        current_time = next_month_start

    return intervals


def calculate_monthly_bill(simulation_results: SimulationResults, rates: list[np.ndarray]) -> list[float]:
    """Calculate monthly electricity bill"""
    monthly_bills = []
    start_idx = 0

    for month_rates in rates:
        # Get the number of intervals in this month
        month_intervals = len(month_rates)

        # Extract consumption data for this month
        month_consumption = simulation_results.E_mt[start_idx : start_idx + month_intervals]

        # Calculate bill for this month: sum(consumption * rates)
        month_bill = float(np.sum(month_consumption * month_rates))
        monthly_bills.append(month_bill)

        # Move to next month's starting index
        start_idx += month_intervals

    return monthly_bills


def calculate_monthly_comfort_penalty(simulation_results: SimulationResults, TOU_params: TOUParameters) -> list[float]:
    """
    Calculate comfort penalty in $ from electrical unmet demand

    Args:
        simulation_results: Simulation results
        TOU_params: TOU parameters

    Returns:
        List of comfort penalties for each month
    """
    time_stamps = simulation_results.Time
    unmet_demand_kWh = simulation_results.D_unmet_mt
    monthly_comfort_penalties = []

    # Group by month using the same logic as create_tou_rates
    current_month = None
    month_start_idx = 0

    for i, timestamp in enumerate(time_stamps):
        # Try to get month, with fallback for numpy.datetime64
        try:
            month = timestamp.month
        except AttributeError:
            # Fallback for numpy.datetime64 objects
            month = timestamp.astype("datetime64[M]").astype(int) % 12 + 1

        if current_month is None:
            current_month = month
        elif month != current_month:
            # Month changed, calculate penalty for the previous month
            month_intervals = i - month_start_idx
            month_unmet_demand = unmet_demand_kWh[month_start_idx : month_start_idx + month_intervals]
            total_unmet_kwh = np.sum(month_unmet_demand)
            monthly_comfort_penalties.append(float(TOU_params.alpha * total_unmet_kwh))

            # Start new month
            current_month = month
            month_start_idx = i

    # Handle the last month
    if month_start_idx < len(time_stamps):
        month_intervals = len(time_stamps) - month_start_idx
        month_unmet_demand = unmet_demand_kWh[month_start_idx : month_start_idx + month_intervals]
        total_unmet_kwh = np.sum(month_unmet_demand)
        monthly_comfort_penalties.append(float(TOU_params.alpha * total_unmet_kwh))

    return monthly_comfort_penalties


def define_peak_hours(TOU_params: TOUParameters, time_step: timedelta) -> np.ndarray:
    """
    Define peak hour intervals for a typical day

    Args:
        TOU_params: TOU parameters (uses default if None)
        time_step: Time step of the simulation

    Returns:
        Boolean array indicating peak hours
    """
    if TOU_params is None:
        TOU_params = TOUParameters()
    peak_start_interval = TOU_params.peak_start_hour * int(seconds_per_hour / time_step.total_seconds())
    peak_end_interval = TOU_params.peak_end_hour * int(seconds_per_hour / time_step.total_seconds())

    intervals_per_day = int(hours_per_day * seconds_per_hour / time_step.total_seconds())

    # Create a boolean array for peak hours
    peak_hours = np.zeros(intervals_per_day, dtype=bool)
    peak_hours[peak_start_interval:peak_end_interval] = True

    return peak_hours


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

        intervals_per_day = int(hours_per_day * seconds_per_hour / time_step.total_seconds())
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


def create_tou_rates(timesteps: np.ndarray, time_step: timedelta, TOU_params: TOUParameters) -> list[np.ndarray]:
    """
    Create TOU rate structure for each month in the simulation period

    Args:
        timesteps: Datetime array
        time_step: Time step of the simulation
        TOU_params: TOU parameters (uses default if None)

    Returns:
        List of arrays, where each array contains electricity rates [$/kWh] for each interval in that month
    """
    if TOU_params is None:
        TOU_params = TOUParameters()

    daily_peak_pattern = define_peak_hours(TOU_params, time_step)
    intervals_per_day = int(hours_per_day * seconds_per_hour / time_step.total_seconds())

    monthly_rates = []
    current_month = None
    month_start_idx = 0

    # Group timesteps by month
    for i, timestamp in enumerate(timesteps):
        # Try to get month, with fallback for numpy.datetime64
        try:
            month = timestamp.month
        except AttributeError:
            # Fallback for numpy.datetime64 objects
            month = timestamp.astype("datetime64[M]").astype(int) % 12 + 1

        if current_month is None:
            current_month = month
        elif month != current_month:
            # Month changed, create rates for the previous month
            month_intervals = i - month_start_idx
            num_days = month_intervals // intervals_per_day
            peak_pattern = np.tile(daily_peak_pattern, num_days)

            # Handle remainder
            remainder = month_intervals % intervals_per_day
            if remainder > 0:
                peak_pattern = np.concatenate([peak_pattern, daily_peak_pattern[:remainder]])

            # Create rate array for this month
            month_rates = np.where(peak_pattern, TOU_params.r_on, TOU_params.r_off)
            monthly_rates.append(month_rates)

            # Start new month
            current_month = month
            month_start_idx = i

    # Handle the last month
    if month_start_idx < len(timesteps):
        month_intervals = len(timesteps) - month_start_idx
        num_days = month_intervals // intervals_per_day
        peak_pattern = np.tile(daily_peak_pattern, num_days)

        # Handle remainder
        remainder = month_intervals % intervals_per_day
        if remainder > 0:
            peak_pattern = np.concatenate([peak_pattern, daily_peak_pattern[:remainder]])

        # Create rate array for the last month
        month_rates = np.where(peak_pattern, TOU_params.r_on, TOU_params.r_off)
        monthly_rates.append(month_rates)

    return monthly_rates


def extract_ochre_results(df: pd.DataFrame, time_step: timedelta) -> SimulationResults:
    """
    Extract simulation results from OCHRE output DataFrame

    Args:
        df: OCHRE simulation results DataFrame
        monthly_intervals: Number of intervals for each month in the simulation period
        time_step: Time step of the simulation

    Returns:
        SimulationResults with electricity consumption, tank temps, and unmet demand
    """
    # Extract time from the index
    time_values = df.index.values

    # Extract electricity consumption for water heating [kW] -> [kWh]
    time_step_fraction = time_step.total_seconds() / seconds_per_hour
    E_mt = (
        np.array(df["Water Heating Electric Power (kW)"].values, dtype=float) * time_step_fraction
    )  # Convert kW to kWh

    # Extract tank temperature [°C]
    T_tank_mt = np.array(df["Hot Water Average Temperature (C)"].values, dtype=float)
    # Extract unmet demand [kW] -> [kWh]
    D_unmet_mt = (
        np.array(df["Hot Water Unmet Demand (kW)"].values, dtype=float) * time_step_fraction
    )  # Convert kW to kWh

    return SimulationResults(time_values, E_mt, T_tank_mt, D_unmet_mt)


def run_ochre_hpwh_dynamic_control(  # type: ignore[no-any-unimported]
    dwelling: Dwelling,
    operation_schedule: np.ndarray,
) -> SimulationResults:
    """
    Run OCHRE simulation with dynamic HPWH control based on operation schedule

    Args:
        dwelling: OCHRE dwelling object
        operation_schedule: Boolean array (True=allowed, False=restricted) for each interval

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
        net_savings = realized_savings - TOU_params.c_switch - tou_comfort_penalty
        if net_savings > 0:
            return "stay"
        else:
            return "switch"


def simulate_full_cycle(
    simulation_type: str, TOU_params: TOUParameters, house_args: dict
) -> tuple[list[float], list[float]]:
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

    dwelling = Dwelling(**house_args)

    start_time = house_args["start_time"]
    end_time = house_args["end_time"]
    time_step = house_args["time_res"]
    monthly_intervals = calculate_monthly_intervals(start_time, end_time, time_step)

    if simulation_type == "default":
        operation_schedule = create_operation_schedule("default", monthly_intervals, TOU_params, time_step)
    else:
        operation_schedule = create_operation_schedule("tou", monthly_intervals, TOU_params, time_step)

    simulation_results = run_ochre_hpwh_dynamic_control(dwelling, operation_schedule)

    monthly_rates = create_tou_rates(simulation_results.Time, time_step, TOU_params)

    monthly_bill = calculate_monthly_bill(simulation_results, monthly_rates)
    monthly_comfort_penalty = calculate_monthly_comfort_penalty(simulation_results, TOU_params)

    return monthly_bill, monthly_comfort_penalty


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


def calculate_monthly_metrics(
    simulation_year_months: list[tuple[int, int]],
    monthly_decisions: list[str],
    states: list[str],
    default_monthly_bill: list[float],
    tou_monthly_bill: list[float],
    default_monthly_comfort_penalty: list[float],
    tou_monthly_comfort_penalty: list[float],
) -> list[MonthlyResults]:
    """
    Calculate monthly results
    """
    monthly_results = []
    for i in range(len(simulation_year_months)):
        current_monthly_result = MonthlyResults(
            year=simulation_year_months[i][0],
            month=simulation_year_months[i][1],
            current_state=states[i],
            bill=default_monthly_bill[i] if states[i] == "default" else tou_monthly_bill[i],
            comfort_penalty=default_monthly_comfort_penalty[i]
            if states[i] == "default"
            else tou_monthly_comfort_penalty[i],
            switching_decision=monthly_decisions[i],
            realized_savings=default_monthly_bill[i] - tou_monthly_bill[i] if states[i] == "tou" else 0,
            unrealized_savings=default_monthly_bill[i] - tou_monthly_bill[i] if states[i] == "default" else 0,
        )
        monthly_results.append(current_monthly_result)
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
    total_switches = sum(1 for r in monthly_results if r.switching_decision == "switch")

    # Calculate TOU adoption rate
    tou_months = sum(1 for r in monthly_results if r.current_state == "tou")
    tou_adoption_rate = tou_months / len(monthly_results) * 100

    # Calculate total realized savings (only when on TOU)
    total_realized_savings = sum(r.realized_savings for r in monthly_results if r.current_state == "tou")

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
        "average_monthly_bill": total_bills / len(monthly_results),
    }


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
    default_monthly_bill, default_monthly_comfort_penalty = simulate_full_cycle("tou", TOU_params, house_args)
    tou_monthly_bill, tou_monthly_comfort_penalty = simulate_full_cycle("tou", TOU_params, house_args)

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
    print("\n=== Full Simulation Test ===")
    try:
        # Load real data and run simulation
        TOU_PARAMS = TOUParameters()
        monthly_results, annual_metrics = run_full_simulation(TOU_PARAMS, HOUSE_ARGS)
        print(monthly_results)
    except Exception as e:
        print(f"Simulation failed: {e}")
        print("This is expected if input files are not available or properly formatted")
