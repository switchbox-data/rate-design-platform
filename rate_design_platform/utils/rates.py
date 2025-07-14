"""
Utility functions for calculating rates, intervals, peak hours, and operation schedules.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta


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
