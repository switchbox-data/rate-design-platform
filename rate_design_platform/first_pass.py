"""
First-Pass TOU Scheduling Decision Model for HPWHs in OCHRE

This module implements the heuristic-based decision model for consumer response
to time-of-use (TOU) electricity rates in residential building simulations.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd


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


class SimulationResults(NamedTuple):
    """Results from HPWH simulation for a given month"""

    E_mt: np.ndarray  # Electricity consumption [kWh/15min]
    T_tank_mt: np.ndarray  # Tank temperature [°C]
    D_unmet_mt: np.ndarray  # Electrical unmet demand [W] (operational power deficit)


def define_peak_hours(intervals_per_day: int = 96) -> np.ndarray:
    """
    Define peak hour intervals in a day (15-minute intervals)

    Args:
        intervals_per_day: Number of 15-minute intervals per day (default 96)

    Returns:
        Boolean array indicating peak hours
    """
    params = TOUParameters()
    peak_start_interval = params.peak_start_hour * 4  # 4 intervals per hour
    peak_end_interval = params.peak_end_hour * 4

    peak_hours = np.zeros(intervals_per_day, dtype=bool)
    peak_hours[peak_start_interval:peak_end_interval] = True

    return peak_hours


def create_tou_rates(num_intervals: int) -> np.ndarray:
    """
    Create TOU rate structure for entire simulation period

    Args:
        num_intervals: Total number of 15-minute intervals

    Returns:
        Array of electricity rates [$/kWh] for each interval
    """
    params = TOUParameters()
    intervals_per_day = 96  # 24 hours * 4 intervals/hour

    # Get daily peak hour pattern
    daily_peak_pattern = define_peak_hours(intervals_per_day)

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
        daily_peak_pattern = define_peak_hours(intervals_per_day)

        # Repeat pattern for the month
        num_days = num_intervals // intervals_per_day
        peak_pattern = np.tile(daily_peak_pattern, num_days)

        # Handle remainder
        remainder = num_intervals % intervals_per_day
        if remainder > 0:
            peak_pattern = np.concatenate([peak_pattern, daily_peak_pattern[:remainder]])

        # TOU schedule: restricted during peak hours
        return (~peak_pattern[:num_intervals]).astype(int)


def building_simulation_controller(
    month: int, operation_schedule: np.ndarray, hot_water_usage: np.ndarray
) -> SimulationResults:
    """
    Static building simulation controller (returns fixed values)

    Args:
        month: Month number (1-12)
        operation_schedule: Binary array of operation permissions
        hot_water_usage: Hot water usage schedule [L/15min]

    Returns:
        SimulationResults with static values
    """
    num_intervals = len(operation_schedule)

    # Static return values
    E_mt = np.full(num_intervals, 0.5)  # kWh/15min
    T_tank_mt = np.full(num_intervals, 55.0)  # °C
    D_unmet_mt = np.zeros(num_intervals)  # W (no unmet demand in static case)

    return SimulationResults(E_mt, T_tank_mt, D_unmet_mt)


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


def simulate_month_both_schedules(
    month: int, hot_water_usage: np.ndarray, rates: np.ndarray, params: TOUParameters
) -> tuple[SimulationResults, SimulationResults, float, float]:
    """
    Simulate both default and TOU schedules for comparison

    Args:
        month: Month number (1-12)
        hot_water_usage: Hot water usage schedule [L/15min]
        rates: Electricity rates for each interval [$/kWh]
        params: TOU parameters

    Returns:
        Tuple of (default_results, tou_results, default_bill, tou_bill)
    """
    num_intervals = len(hot_water_usage)

    # Create operational schedules
    default_schedule = create_operation_schedule(1, num_intervals)  # Always allowed
    tou_schedule = create_operation_schedule(0, num_intervals)  # Peak restricted

    # Run building simulations
    default_results = building_simulation_controller(month, default_schedule, hot_water_usage)
    tou_results = building_simulation_controller(month, tou_schedule, hot_water_usage)

    # Calculate bills
    default_bill = calculate_monthly_bill(default_results.E_mt, rates)
    tou_bill = calculate_monthly_bill(tou_results.E_mt, rates)

    return default_results, tou_results, default_bill, tou_bill


def simulate_single_month(
    month: int, current_state: int, hot_water_usage: np.ndarray, rates: np.ndarray, params: TOUParameters
) -> MonthlyResults:
    """
    Simulate a single month with decision logic

    Args:
        month: Month number (1-12)
        current_state: Current schedule state (1=default, 0=TOU)
        hot_water_usage: Hot water usage schedule [L/15min]
        rates: Electricity rates for each interval [$/kWh]
        params: TOU parameters

    Returns:
        MonthlyResults with all monthly outcomes
    """
    # Simulate both schedules for comparison
    default_results, tou_results, default_bill, tou_bill = simulate_month_both_schedules(
        month, hot_water_usage, rates, params
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


def simulate_annual_cycle(hot_water_data: np.ndarray, params: TOUParameters | None = None) -> list[MonthlyResults]:
    """
    Simulate complete annual cycle with monthly decision-making

    Args:
        hot_water_data: Annual hot water usage data [L/15min]
        params: TOU parameters (uses default if None)

    Returns:
        List of MonthlyResults for each month
    """
    if params is None:
        params = TOUParameters()

    # Create annual rate structure
    annual_rates = create_tou_rates(len(hot_water_data))

    # Split data into monthly chunks (assuming ~2920 intervals per month)
    intervals_per_month = len(hot_water_data) // 12

    # Initialize state (start on default schedule)
    current_state = 1
    monthly_results = []

    for month in range(1, 13):
        # Extract monthly data
        start_idx = (month - 1) * intervals_per_month
        end_idx = start_idx + intervals_per_month

        monthly_hot_water = hot_water_data[start_idx:end_idx]
        monthly_rates = annual_rates[start_idx:end_idx]

        # Simulate month
        result = simulate_single_month(month, current_state, monthly_hot_water, monthly_rates, params)

        monthly_results.append(result)

        # Update state for next month based on switching decision
        if result.switching_decision == 1:
            current_state = 1 - current_state  # Toggle state

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


def load_input_data(csv_path: str | None = None) -> np.ndarray:
    """
    Load hot water usage data from CSV file

    Args:
        csv_path: Path to CSV file (uses default if None)

    Returns:
        Hot water usage data [L/15min] as numpy array
    """
    if csv_path is None:
        # Default path relative to this file
        current_dir = Path(__file__).parent
        csv_path_resolved: str = str(current_dir / "inputs" / "bldg0000072-up00_schedule.csv")
    else:
        csv_path_resolved = csv_path

    # Load CSV data
    df = pd.read_csv(csv_path_resolved)

    # Extract hot water fixtures column
    if "hot_water_fixtures" in df.columns:
        hot_water_data: np.ndarray = np.asarray(df["hot_water_fixtures"].values)
    else:
        msg = "CSV file must contain 'hot_water_fixtures' column"
        raise ValueError(msg)

    return hot_water_data


def run_full_simulation(
    csv_path: str | None = None, params: TOUParameters | None = None
) -> tuple[list[MonthlyResults], dict[str, float]]:
    """
    Run complete TOU HPWH simulation from CSV input data

    Args:
        csv_path: Path to input CSV file (uses default if None)
        params: TOU parameters (uses default if None)

    Returns:
        Tuple of (monthly_results, annual_metrics)
    """
    # Load input data
    hot_water_data = load_input_data(csv_path)

    # Use default parameters if None provided
    if params is None:
        params = TOUParameters()

    # Run annual simulation
    monthly_results = simulate_annual_cycle(hot_water_data, params)

    # Calculate annual metrics
    annual_metrics = calculate_annual_metrics(monthly_results)

    return monthly_results, annual_metrics


if __name__ == "__main__":
    # Test TOU rate structure
    print("=== TOU Rate Structure Test ===")
    test_intervals = 96 * 7  # One week
    rates = create_tou_rates(test_intervals)

    print(f"Created {len(rates)} rate intervals")
    print(f"Peak rate: ${TOUParameters().r_on:.2f}/kWh")
    print(f"Off-peak rate: ${TOUParameters().r_off:.2f}/kWh")
    print(f"Peak hours per day: {np.sum(define_peak_hours())}")

    # Test operation schedules
    default_schedule = create_operation_schedule(1, 96)  # Default
    tou_schedule = create_operation_schedule(0, 96)  # TOU

    print(f"Default schedule allows operation: {np.sum(default_schedule)}/96 intervals")
    print(f"TOU schedule allows operation: {np.sum(tou_schedule)}/96 intervals")

    # Test full simulation with sample data
    print("\n=== Full Simulation Test ===")
    try:
        # Load real data and run simulation
        monthly_results, annual_metrics = run_full_simulation()

        print(f"Loaded hot water data with {len(load_input_data())} intervals")
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
