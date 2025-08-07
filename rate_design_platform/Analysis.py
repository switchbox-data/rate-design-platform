"""
Collection of dataclasses and functions for analyzing the results of the TOU scheduling decision model.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import NamedTuple

import matplotlib
import matplotlib.dates as mdates

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
class ValueLearningResults:
    """Results from value learning simulation including learning metrics"""

    # Inherit all fields from MonthlyResults
    year: int
    month: int
    current_state: str
    bill: float
    comfort_penalty: float
    switching_decision: str
    realized_savings: float
    unrealized_savings: float

    # Value learning specific fields
    v_default: float  # Learned value for default schedule
    v_tou: float  # Learned value for TOU schedule
    epsilon_m: float  # Exploration rate for this month
    alpha_m_learn: float  # Learning rate for this month
    decision_type: str  # "exploration" or "exploitation"
    value_difference: float  # |V_tou - V_default|
    recent_cost_surprise: float  # Experience replay cost surprise


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
    simulation_results: SimulationResults, TOU_params: TOUParameters, comfort_penalty_factor: float = 0.15
) -> list[MonthlyMetrics]:
    """
    Calculate comfort penalty in $ from electrical unmet demand

    Args:
        simulation_results: Simulation results
        TOU_params: TOU parameters (unused but kept for compatibility)
        comfort_penalty_factor: Monetization factor for comfort penalty ($/kWh)

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
                    comfort_penalty=float(comfort_penalty_factor * total_unmet_kwh),
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
            MonthlyMetrics(year=year, month=month, comfort_penalty=float(comfort_penalty_factor * total_unmet_kwh))
        )

    return monthly_comfort_penalties


def calculate_monthly_bill_and_comfort_penalty(
    simulation_results: SimulationResults,
    monthly_rate_structure: list[MonthlyRateStructure],
    TOU_params: TOUParameters,
    comfort_penalty_factor: float = 0.15,
) -> list[MonthlyMetrics]:
    monthly_bills = calculate_monthly_bill(simulation_results, monthly_rate_structure)
    monthly_comfort_penalties = calculate_monthly_comfort_penalty(
        simulation_results, TOU_params, comfort_penalty_factor
    )
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
    Calculate basic annual performance metrics from monthly results

    Args:
        monthly_results: List of MonthlyResults for each month

    Returns:
        Dictionary of basic annual metrics
    """
    total_bills = sum(r.bill for r in monthly_results)
    total_comfort_penalty = sum(r.comfort_penalty for r in monthly_results)

    # Calculate TOU adoption rate
    tou_months = sum(1 for r in monthly_results if r.current_state == "tou")
    tou_adoption_rate = tou_months / len(monthly_results) * 100

    # Calculate total realized savings (only when on TOU)
    total_realized_savings = sum(r.realized_savings for r in monthly_results if r.current_state == "tou")

    return {
        "total_annual_bills": total_bills,
        "total_comfort_penalty": total_comfort_penalty,
        "total_realized_savings": total_realized_savings,
        "net_annual_benefit": total_realized_savings - total_comfort_penalty,
        "tou_adoption_rate_percent": tou_adoption_rate,
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
    if len(benefited_bldgs) > 0:
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
    # Create outputs directory if it doesn't exist
    from pathlib import Path

    base_path = Path(__file__).parent.absolute()
    Path(base_path / "outputs").mkdir(exist_ok=True)
    # Save the plot to a file instead of showing it
    plt.savefig(base_path / "outputs" / "total_savings_distribution.png", dpi=300, bbox_inches="tight")
    print(f"Plot saved as '{base_path / 'outputs' / 'total_savings_distribution.png'}'")
    plt.close()  # Close the figure to free memory

    """Switching decision plot"""
    default = np.zeros(len(monthly_results[0]))
    tou = np.zeros(len(monthly_results[0]))
    month_years = []
    for i in range(len(monthly_results)):
        bldg_monthly_results = monthly_results[i]
        for j in range(len(bldg_monthly_results)):
            bldg_monthly_result = bldg_monthly_results[j]
            if i == 0:
                month_years.append(datetime(year=bldg_monthly_result.year, month=bldg_monthly_result.month, day=1))
            if bldg_monthly_result.current_state == "default":
                default[j] += 1
            else:
                tou[j] += 1

    # Create the switching decision time series plot
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Convert datetime objects to matplotlib dates
    month_years_mpl = mdates.date2num(month_years)

    # Plot both lines on primary axis
    ax1.plot(month_years_mpl, default, marker="o", linewidth=2, label="Default Rate", color="blue")
    ax1.plot(month_years_mpl, tou, marker="s", linewidth=2, label="TOU Rate", color="red")

    # Set integer y-axis ticks on the left
    total_households = len(monthly_results)
    ax1.set_ylim(0, total_households)
    ax1.yaxis.set_major_locator(plt.matplotlib.ticker.MaxNLocator(integer=True))

    # Customize the primary axis
    ax1.set_xlabel("Month", fontsize=12)
    ax1.set_ylabel("Number of Households", fontsize=12)
    ax1.set_title("Household Rate Adoption Over Time", fontsize=14, fontweight="bold")
    ax1.grid(True, alpha=0.3)

    # Format x-axis to show months nicely
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax1.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

    # Create secondary y-axis for percentages
    ax2 = ax1.twinx()

    # Calculate percentages for each month
    default_percentages = [default[i] / total_households * 100 for i in range(len(default))]
    tou_percentages = [tou[i] / total_households * 100 for i in range(len(tou))]

    # Set percentage axis limits and ticks
    ax2.set_ylim(0, 100)
    ax2.set_ylabel("Percentage of Households (%)", fontsize=12)

    # Add percentage line on secondary axis (invisible but sets the scale)
    ax2.plot(month_years_mpl, default_percentages, alpha=0)  # Invisible line to set scale
    ax2.plot(month_years_mpl, tou_percentages, alpha=0)  # Invisible line to set scale

    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    ax1.legend(lines1, labels1, fontsize=10, loc="upper left")

    plt.tight_layout()
    # Create outputs directory if it doesn't exist (already created above, but just in case)
    base_path = Path(__file__).parent.absolute()
    Path(base_path / "outputs").mkdir(exist_ok=True)
    plt.savefig(base_path / "outputs" / "rate_adoption_timeseries.png", dpi=300, bbox_inches="tight")
    print(f"Rate adoption time series plot saved as '{base_path / 'outputs' / 'rate_adoption_timeseries.png'}'")
    plt.close()


def calculate_value_learning_monthly_metrics(
    simulation_year_months: list[tuple[int, int]],
    monthly_decisions: list[str],
    states: list[str],
    default_monthly_bill: list[float],
    tou_monthly_bill: list[float],
    default_monthly_comfort_penalty: list[float],
    tou_monthly_comfort_penalty: list[float],
    learning_metrics_history: list[dict],
) -> list[ValueLearningResults]:
    """
    Calculate monthly results for value learning simulation.

    Args:
        simulation_year_months: List of (year, month) tuples
        monthly_decisions: List of decisions ("switch" or "stay")
        states: List of states ("default" or "tou")
        default_monthly_bill: List of default monthly bills
        tou_monthly_bill: List of TOU monthly bills
        default_monthly_comfort_penalty: List of default comfort penalties
        tou_monthly_comfort_penalty: List of TOU comfort penalties
        learning_metrics_history: List of learning metrics dictionaries

    Returns:
        List of ValueLearningResults for each month
    """
    monthly_results = []

    for i in range(len(monthly_decisions)):
        year, month = simulation_year_months[i]
        current_state = states[i]
        decision = monthly_decisions[i]
        learning_metrics = learning_metrics_history[i]

        # Calculate savings based on current state
        if current_state == "default":
            realized_savings = 0.0
            unrealized_savings = default_monthly_bill[i] - tou_monthly_bill[i]
            bill = default_monthly_bill[i]
            comfort_penalty = default_monthly_comfort_penalty[i]
        else:  # TOU
            realized_savings = default_monthly_bill[i] - tou_monthly_bill[i]
            unrealized_savings = 0.0
            bill = tou_monthly_bill[i]
            comfort_penalty = tou_monthly_comfort_penalty[i]

        # Create ValueLearningResults with both standard and learning metrics
        result = ValueLearningResults(
            year=year,
            month=month,
            current_state=current_state,
            bill=bill,
            comfort_penalty=comfort_penalty,
            switching_decision=decision,
            realized_savings=realized_savings,
            unrealized_savings=unrealized_savings,
            # Value learning specific fields
            v_default=learning_metrics.get("v_default", 0.0),
            v_tou=learning_metrics.get("v_tou", 0.0),
            epsilon_m=learning_metrics.get("epsilon_m", 0.0),
            alpha_m_learn=learning_metrics.get("alpha_m_learn", 0.0),
            decision_type=learning_metrics.get("decision_type", "unknown"),
            value_difference=learning_metrics.get("value_difference", 0.0),
            recent_cost_surprise=learning_metrics.get("recent_cost_surprise", 0.0),
        )

        monthly_results.append(result)

    return monthly_results


def calculate_value_learning_annual_metrics(monthly_results: list[ValueLearningResults]) -> dict[str, float]:
    """
    Calculate comprehensive annual metrics for value learning simulation.

    From documentation metrics:
    - Exploration Rate = (sum of switches) / 12 * 100%
    - Final Value Difference = |V_12^TOU - V_12^default|
    - TOU Adoption Rate = (sum of months on TOU) / 12 * 100%
    - Peak Load Reduction = comparison with baseline

    Args:
        monthly_results: List of ValueLearningResults for each month

    Returns:
        Dictionary with comprehensive annual metrics
    """
    if not monthly_results:
        return {}

    # Standard financial metrics
    total_bills = sum(r.bill for r in monthly_results)
    total_comfort_penalty = sum(r.comfort_penalty for r in monthly_results)
    total_realized_savings = sum(r.realized_savings for r in monthly_results if r.current_state == "tou")

    # Value learning specific metrics

    # 1. Exploration Rate (from documentation)
    total_switches = sum(1 for r in monthly_results if r.switching_decision == "switch")
    exploration_rate = (total_switches / len(monthly_results)) * 100

    # 2. Final Value Difference (from documentation)
    final_result = monthly_results[-1]
    final_value_difference = abs(final_result.v_tou - final_result.v_default)

    # 3. TOU Adoption Rate (from documentation)
    tou_months = sum(1 for r in monthly_results if r.current_state == "tou")
    tou_adoption_rate = (tou_months / len(monthly_results)) * 100

    # 4. Learning behavior metrics
    exploration_decisions = sum(1 for r in monthly_results if r.decision_type == "exploration")
    exploitation_decisions = len(monthly_results) - exploration_decisions
    exploration_vs_exploitation_ratio = (
        (exploration_decisions / exploitation_decisions) if exploitation_decisions > 0 else float("inf")
    )

    # 5. Learning convergence metrics
    initial_value_diff = abs(monthly_results[0].v_tou - monthly_results[0].v_default)
    learning_convergence = abs(final_value_difference - initial_value_diff)

    # 6. Exploration rate evolution
    avg_exploration_rate = sum(r.epsilon_m for r in monthly_results) / len(monthly_results)
    final_exploration_rate = final_result.epsilon_m
    exploration_rate_change = final_exploration_rate - monthly_results[0].epsilon_m

    # 7. Experience replay metrics
    total_cost_surprises = sum(r.recent_cost_surprise for r in monthly_results)
    avg_cost_surprise = total_cost_surprises / len(monthly_results) if len(monthly_results) > 0 else 0
    value_learning_efficiency = (final_value_difference / initial_value_diff) if initial_value_diff > 0 else 1.0

    return {
        # Standard financial metrics
        "total_annual_bills": total_bills,
        "total_comfort_penalty": total_comfort_penalty,
        "total_realized_savings": total_realized_savings,
        "net_annual_benefit": total_realized_savings - total_comfort_penalty,
        "average_monthly_bill": total_bills / len(monthly_results),
        # Core value learning metrics (from documentation)
        "exploration_rate_percent": exploration_rate,
        "final_value_difference": final_value_difference,
        "tou_adoption_rate_percent": tou_adoption_rate,
        # Extended learning behavior metrics
        "exploration_decisions": exploration_decisions,
        "exploitation_decisions": exploitation_decisions,
        "exploration_vs_exploitation_ratio": exploration_vs_exploitation_ratio,
        "learning_convergence": learning_convergence,
        "avg_exploration_rate": avg_exploration_rate,
        "final_exploration_rate": final_exploration_rate,
        "exploration_rate_change": exploration_rate_change,
        "avg_cost_surprise": avg_cost_surprise,
        "total_cost_surprises": total_cost_surprises,
        "value_learning_efficiency": value_learning_efficiency,
        # Final learned values
        "final_v_default": final_result.v_default,
        "final_v_tou": final_result.v_tou,
        "final_alpha_learn": final_result.alpha_m_learn,
    }
