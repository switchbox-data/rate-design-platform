"""
Collection of dataclasses and functions for analyzing the results of the TOU scheduling decision model.
"""

from dataclasses import dataclass
from typing import NamedTuple

import numpy as np


@dataclass
class TOUParameters:
    """TOU rate structure and simulation parameters"""

    r_on: float = 0.48  # $/kWh - peak rate
    r_off: float = 0.12  # $/kWh - off-peak rate
    c_switch: float = 3.0  # $ - switching cost
    alpha: float = 0.15  # $/kWh - comfort penalty factor
    # Peak hours: 12 PM to 8 PM (12:00 to 20:00)
    peak_start_hour: int = 12
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


@dataclass
class MonthlyBill:
    """Bill from a single month's simulation"""

    year: int
    month: int
    bill: float


@dataclass
class MonthlyComfortPenalty:
    """Comfort penalty from a single month's simulation"""

    year: int
    month: int
    comfort_penalty: float


class SimulationResults(NamedTuple):
    """Results from HPWH simulation for a given month"""

    Time: np.ndarray  # Datetime array
    E_mt: np.ndarray  # Electricity consumption [kWh]
    T_tank_mt: np.ndarray  # Tank temperature [°C]
    D_unmet_mt: np.ndarray  # Electrical unmet demand [kWh] (operational power deficit)
