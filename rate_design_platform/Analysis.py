"""
Collection of dataclasses and functions for analyzing the results of the TOU scheduling decision model.
"""

from dataclasses import dataclass
from typing import NamedTuple

import numpy as np
import pandas as pd

from rate_design_platform.utils.rates import MonthlyRateStructure


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
