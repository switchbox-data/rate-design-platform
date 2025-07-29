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

    # 8. Learning efficiency metrics
    months_to_stabilization = _calculate_learning_stabilization_point(monthly_results)
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
        "months_to_stabilization": months_to_stabilization,
        "value_learning_efficiency": value_learning_efficiency,
        # Final learned values
        "final_v_default": final_result.v_default,
        "final_v_tou": final_result.v_tou,
        "final_alpha_learn": final_result.alpha_m_learn,
    }


def _calculate_learning_stabilization_point(monthly_results: list[ValueLearningResults]) -> int:
    """
    Calculate the number of months it took for learning to stabilize.

    Stabilization is defined as when the value difference changes by less than 5%
    for 3 consecutive months.

    Args:
        monthly_results: List of ValueLearningResults

    Returns:
        Number of months to stabilization (or total months if never stabilized)
    """
    if len(monthly_results) < 4:
        return len(monthly_results)

    threshold = 0.05  # 5% threshold
    consecutive_stable = 0
    required_stable_months = 3

    for i in range(1, len(monthly_results)):
        prev_diff = monthly_results[i - 1].value_difference
        curr_diff = monthly_results[i].value_difference

        change_rate = (0 if curr_diff == 0 else 1) if prev_diff == 0 else abs((curr_diff - prev_diff) / prev_diff)

        if change_rate < threshold:
            consecutive_stable += 1
            if consecutive_stable >= required_stable_months:
                return i - required_stable_months + 2  # Return the month when stabilization began
        else:
            consecutive_stable = 0

    return len(monthly_results)  # Never stabilized


def generate_value_learning_summary(
    monthly_results: list[ValueLearningResults], annual_metrics: dict[str, float]
) -> str:
    """
    Generate a comprehensive summary of value learning simulation results.

    Args:
        monthly_results: List of ValueLearningResults for each month
        annual_metrics: Dictionary of annual metrics

    Returns:
        Formatted summary string
    """
    summary = []
    summary.append("=" * 60)
    summary.append("VALUE LEARNING SIMULATION SUMMARY")
    summary.append("=" * 60)

    # Learning Performance
    summary.append("\n📊 LEARNING PERFORMANCE:")
    summary.append(f"  • TOU Adoption Rate: {annual_metrics.get('tou_adoption_rate_percent', 0):.1f}%")
    summary.append(f"  • Final Value Difference: ${annual_metrics.get('final_value_difference', 0):.2f}")
    summary.append(f"  • Learning Stabilization: {annual_metrics.get('months_to_stabilization', 0)} months")
    summary.append(f"  • Learning Efficiency: {annual_metrics.get('value_learning_efficiency', 0):.2f}")

    # Decision Making Behavior
    summary.append("\n🎯 DECISION MAKING:")
    summary.append(f"  • Exploration Rate: {annual_metrics.get('exploration_rate_percent', 0):.1f}%")
    summary.append(f"  • Exploration Decisions: {annual_metrics.get('exploration_decisions', 0)}")
    summary.append(f"  • Exploitation Decisions: {annual_metrics.get('exploitation_decisions', 0)}")
    summary.append(f"  • Avg Exploration Rate: {annual_metrics.get('avg_exploration_rate', 0):.3f}")

    # Financial Performance
    summary.append("\n💰 FINANCIAL PERFORMANCE:")
    summary.append(f"  • Total Annual Bills: ${annual_metrics.get('total_annual_bills', 0):.2f}")
    summary.append(f"  • Total Realized Savings: ${annual_metrics.get('total_realized_savings', 0):.2f}")
    summary.append(f"  • Total Comfort Penalty: ${annual_metrics.get('total_comfort_penalty', 0):.2f}")
    summary.append(f"  • Net Annual Benefit: ${annual_metrics.get('net_annual_benefit', 0):.2f}")

    # Learned Values
    summary.append("\n🧠 FINAL LEARNED VALUES:")
    summary.append(f"  • V_default: ${annual_metrics.get('final_v_default', 0):.2f}")
    summary.append(f"  • V_tou: ${annual_metrics.get('final_v_tou', 0):.2f}")
    summary.append(f"  • Final Learning Rate: {annual_metrics.get('final_alpha_learn', 0):.3f}")

    # Monthly Progression
    summary.append("\n📅 MONTHLY PROGRESSION:")
    for i, result in enumerate(monthly_results[:6]):  # Show first 6 months
        summary.append(
            f"  Month {i + 1}: {result.current_state} -> {result.switching_decision} "
            f"({result.decision_type}, ε={result.epsilon_m:.3f})"
        )

    if len(monthly_results) > 6:
        summary.append("  ...")
        final = monthly_results[-1]
        summary.append(
            f"  Month 12: {final.current_state} -> {final.switching_decision} "
            f"({final.decision_type}, ε={final.epsilon_m:.3f})"
        )

    summary.append("=" * 60)

    return "\n".join(summary)
