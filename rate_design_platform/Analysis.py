"""
Collection of dataclasses and functions for analyzing the results of the TOU scheduling decision model.
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import NamedTuple

import numpy as np
import pandas as pd

from rate_design_platform.utils.constants import SECONDS_PER_HOUR
from rate_design_platform.utils.rates import MonthlyRateStructure, TOUParameters


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

    year: int = 0
    month: int = 0
    bill: float = 0
    comfort_penalty: float = 0


class SimulationResults(NamedTuple):
    """Results from HPWH simulation for a given month"""

    Time: np.ndarray  # Datetime array
    E_mt: np.ndarray  # Electricity consumption [kWh]
    T_tank_mt: np.ndarray  # Tank temperature [°C]
    D_unmet_mt: np.ndarray  # Electrical unmet demand [kWh] (operational power deficit)


def calculate_monthly_bill(
    simulation_results: SimulationResults, monthly_rate_structure: list[MonthlyRateStructure]
) -> list[MonthlyMetrics]:
    """Calculate monthly electricity bill"""
    monthly_metrics = []

    for month_rate in monthly_rate_structure:
        # Extract consumption data for this month
        time_pd = pd.to_datetime(simulation_results.Time)
        simulation_results_year = time_pd.year == month_rate.year
        simulation_results_month = time_pd.month == month_rate.month
        month_consumption = simulation_results.E_mt[simulation_results_year & simulation_results_month]

        # Calculate bill for this month: sum(consumption * rates)
        month_bill = float(np.sum(month_consumption * month_rate.rates))
        monthly_metrics.append(MonthlyMetrics(year=month_rate.year, month=month_rate.month, bill=month_bill))

    return monthly_metrics


def calculate_monthly_comfort_penalty(
    simulation_results: SimulationResults, TOU_params: TOUParameters
) -> list[MonthlyMetrics]:
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

    # Group by month
    current_month = None
    month_start_idx = 0

    for i, timestamp in enumerate(time_stamps):
        # Try to get month, with fallback for numpy.datetime64
        try:
            year = timestamp.year
            month = timestamp.month
        except AttributeError:
            # Fallback for numpy.datetime64 objects
            year = timestamp.astype("datetime64[Y]").astype(int)
            month = timestamp.astype("datetime64[M]").astype(int) % 12 + 1

        if current_month is None:
            current_month = month
        elif month != current_month:
            # Month changed, calculate penalty for the previous month
            month_intervals = i - month_start_idx
            month_unmet_demand = unmet_demand_kWh[month_start_idx : month_start_idx + month_intervals]
            total_unmet_kwh = np.sum(month_unmet_demand)
            monthly_comfort_penalties.append(
                MonthlyMetrics(year=year, month=month, comfort_penalty=float(TOU_params.alpha * total_unmet_kwh))
            )

            # Start new month
            current_month = month
            month_start_idx = i

    # Handle the last month
    if month_start_idx < len(time_stamps):
        month_intervals = len(time_stamps) - month_start_idx
        month_unmet_demand = unmet_demand_kWh[month_start_idx : month_start_idx + month_intervals]
        total_unmet_kwh = np.sum(month_unmet_demand)
        monthly_comfort_penalties.append(
            MonthlyMetrics(year=year, month=month, comfort_penalty=float(TOU_params.alpha * total_unmet_kwh))
        )

    return monthly_comfort_penalties


def calculate_monthly_bill_and_comfort_penalty(
    simulation_results: SimulationResults, monthly_rate_structure: list[MonthlyRateStructure], TOU_params: TOUParameters
) -> list[MonthlyMetrics]:
    monthly_bills = calculate_monthly_bill(simulation_results, monthly_rate_structure)
    monthly_comfort_penalties = calculate_monthly_comfort_penalty(simulation_results, TOU_params)
    monthly_metrics = []
    for bill, comfort_penalty in zip(monthly_bills, monthly_comfort_penalties):
        monthly_metrics.append(
            MonthlyMetrics(
                year=bill.year, month=bill.month, bill=bill.bill, comfort_penalty=comfort_penalty.comfort_penalty
            )
        )
    return monthly_metrics


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
    time_step_fraction = time_step.total_seconds() / SECONDS_PER_HOUR
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
