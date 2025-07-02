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

# Define constants
seconds_per_hour = 3600


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
