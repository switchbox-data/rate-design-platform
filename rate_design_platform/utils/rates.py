"""
Utility functions for calculating rates, intervals, peak hours, and operation schedules.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import numpy as np

from rate_design_platform.utils.constants import HOURS_PER_DAY, SECONDS_PER_HOUR


@dataclass
class TOUParameters:
    """TOU rate structure and simulation parameters"""

    r_on: float = 0.48  # $/kWh - peak rate
    r_off: float = 0.12  # $/kWh - off-peak rate
    c_switch: float = 3.0  # $ - switching cost
    alpha: float = 0.15  # $/kWh - comfort penalty factor
    # Peak hours: 12 PM to 8 PM (12:00 to 20:00)
    peak_start_hour: timedelta = timedelta(hours=12)
    peak_end_hour: timedelta = timedelta(hours=20)


@dataclass
class MonthlyRateStructure:
    """Various values to describe monthly rates structure and simulation parameters"""

    year: int = 0
    month: int = 0
    rates: np.ndarray = field(default_factory=lambda: np.array([]))  # rates for each interval in the month
    intervals: int = 0  # number of time_step - long intervals in the month


def calculate_monthly_intervals(
    start_time: datetime, end_time: datetime, time_step: timedelta
) -> list[MonthlyRateStructure]:
    """
    Calculate number of time_step - long intervals in a given month

    Args:
        start_time: Start time of the simulation
        end_time: End time of the simulation
        time_step: Time step of the simulation

    Returns:
        Number of time_step - long intervals intervals for each month from start_time to end_time
    """
    monthly_rates = []
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
        monthly_rates.append(
            MonthlyRateStructure(year=current_time.year, month=current_time.month, intervals=num_intervals)
        )

        # Move to next month
        current_time = next_month_start

    return monthly_rates


def define_peak_hours(TOU_params: TOUParameters, time_step: timedelta) -> np.ndarray:
    """
    Define peak hour intervals for a typical day

    Args:
        TOU_params: TOU parameters (uses default if None)
        time_step: Time step of the simulation

    Returns:
        Boolean array indicating peak hours
    """
    day_intervals: np.ndarray = np.arange(timedelta(0), timedelta(days=1), time_step)
    peak_start_interval = np.where(day_intervals == TOU_params.peak_start_hour)[0][0]
    peak_end_interval = np.where(day_intervals == TOU_params.peak_end_hour)[0][0]

    # Create a boolean array for peak hours
    peak_hours = np.zeros(len(day_intervals), dtype=bool)
    peak_hours[peak_start_interval:peak_end_interval] = True

    return peak_hours


def create_tou_rates(timesteps: np.ndarray, time_step: timedelta, TOU_params: TOUParameters) -> list[np.ndarray]:
    """
    Create TOU rate structure for each month in the simulation period

    Args:
        timesteps: Datetime array for each time step in the simulation
        time_step: Time step of the simulation
        TOU_params: TOU parameters

    Returns:
        List of arrays, where each array contains electricity rates [$/kWh] for each interval in that month
    """
    daily_peak_pattern = define_peak_hours(TOU_params, time_step)
    intervals_per_day = int(HOURS_PER_DAY * SECONDS_PER_HOUR / time_step.total_seconds())

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
