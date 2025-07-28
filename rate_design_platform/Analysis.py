"""
Collection of dataclasses and functions for analyzing the results of the TOU scheduling decision model.
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import NamedTuple

import matplotlib

matplotlib.use("Agg")  # Use non-interactive backend
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

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
                MonthlyMetrics(
                    year=year,
                    month=month,
                    comfort_penalty=float(TOU_params.get_comfort_penalty_factor() * total_unmet_kwh),
                )
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
            MonthlyMetrics(
                year=year, month=month, comfort_penalty=float(TOU_params.get_comfort_penalty_factor() * total_unmet_kwh)
            )
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


def calculate_annual_metrics(monthly_results: list[MonthlyResults], TOU_params: TOUParameters) -> dict[str, float]:
    """
    Calculate annual performance metrics from monthly results

    Args:
        monthly_results: List of MonthlyResults for each month
        TOU_params: TOU parameters with building-dependent costs

    Returns:
        Dictionary of annual metrics
    """
    total_bills = sum(r.bill for r in monthly_results)
    total_comfort_penalty = sum(r.comfort_penalty for r in monthly_results)

    # Calculate switching costs based on direction
    # For proper implementation, we would need to track switch direction
    # For now, use average of switch-to and switch-back costs
    total_switches = sum(1 for r in monthly_results if r.switching_decision == "switch")
    avg_switching_cost = (TOU_params.get_switching_cost_to() + TOU_params.get_switching_cost_back()) / 2
    total_switching_costs = total_switches * avg_switching_cost

    # Calculate TOU adoption rate
    tou_months = sum(1 for r in monthly_results if r.current_state == "tou")
    tou_adoption_rate = tou_months / len(monthly_results) * 100

    # Calculate total realized savings (only when on TOU)
    total_realized_savings = sum(r.realized_savings for r in monthly_results if r.current_state == "tou")

    return {
        "total_annual_bills": total_bills,
        "total_comfort_penalty": total_comfort_penalty,
        "total_switching_costs": total_switching_costs,
        "total_realized_savings": total_realized_savings,
        "net_annual_benefit": total_realized_savings - total_switching_costs - total_comfort_penalty,
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


def batch_run_analysis(monthly_results: list[list[MonthlyResults]], annual_metrics: list[dict[str, float]]) -> None:
    """
    Analyze the results of the batch run
    """
    benefited_bldgs = []
    no_benefit_bldgs = []
    for i in range(len(annual_metrics)):
        if annual_metrics[i]["total_realized_savings"] > 0:
            benefited_bldgs.append(annual_metrics[i])
        else:
            no_benefit_bldgs.append(annual_metrics[i])

    print(f"Benefited bldgs: {len(benefited_bldgs)}")
    print(f"No benefit bldgs: {len(no_benefit_bldgs)}")
    print(f"Percentage of benefited bldgs: {len(benefited_bldgs) / len(annual_metrics) * 100}%")
    print(
        f"Average total savings for all homes: ${sum(annual_metric['total_realized_savings'] for annual_metric in annual_metrics) / len(annual_metrics):.2f}"
    )
    print(
        f"Average total savings for benefitting homes: {sum(benefited_bldg['total_realized_savings'] for benefited_bldg in benefited_bldgs) / len(benefited_bldgs):.2f}$"
    )

    total_savings = [annual_metric["total_realized_savings"] for annual_metric in annual_metrics]

    """Total savings plot"""
    # Create a more informative histogram
    plt.figure(figsize=(10, 6))
    plt.hist(total_savings, bins=20, alpha=0.7, color="skyblue", edgecolor="black")

    # Add vertical line for mean
    mean_savings = float(np.mean(total_savings))
    plt.axvline(mean_savings, color="red", linestyle="--", linewidth=2, label=f"Mean: ${mean_savings:.2f}")

    # Add vertical line for median
    median_savings = float(np.median(total_savings))
    plt.axvline(median_savings, color="orange", linestyle="--", linewidth=2, label=f"Median: ${median_savings:.2f}")

    # Add vertical line at zero
    plt.axvline(0, color="green", linestyle="-", linewidth=1, alpha=0.7, label="Break-even line")

    plt.xlabel("Total Realized Savings ($)", fontsize=12)
    plt.ylabel("Number of Households", fontsize=12)
    plt.title("Distribution of Annual TOU Rate Savings Across Households", fontsize=14, fontweight="bold")
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)

    # Add text box with summary statistics
    positive_savings = [s for s in total_savings if s > 0]
    negative_savings = [s for s in total_savings if s < 0]

    stats_text = f"Total Households: {len(total_savings)}\n"
    stats_text += (
        f"Households with Savings: {len(positive_savings)} ({len(positive_savings) / len(total_savings) * 100:.1f}%)\n"
    )
    stats_text += (
        f"Households with Losses: {len(negative_savings)} ({len(negative_savings) / len(total_savings) * 100:.1f}%)\n"
    )
    stats_text += f"Average Savings: ${mean_savings:.2f}\n"
    if positive_savings:
        stats_text += f"Avg Savings (Benefiting HH): ${np.mean(positive_savings):.2f}"

    plt.text(
        0.02,
        0.98,
        stats_text,
        transform=plt.gca().transAxes,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
        fontsize=9,
    )

    plt.tight_layout()
    # Save the plot to a file instead of showing it
    plt.savefig("outputs/total_savings_distribution.png", dpi=300, bbox_inches="tight")
    print("Plot saved as 'total_savings_distribution.png'")
    plt.close()  # Close the figure to free memory
