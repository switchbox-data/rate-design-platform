"""
Utility functions for calculating rates, intervals, peak hours, and operation schedules.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from rate_design_platform.utils.constants import HOURS_PER_DAY, SECONDS_PER_HOUR


@dataclass
class TOUParameters:
    """TOU rate structure and simulation parameters"""

    r_on: float = 0.48  # $/kWh - peak rate
    r_off: float = 0.12  # $/kWh - off-peak rate

    # Building-dependent parameters (will be calculated based on building characteristics)
    c_switch_to: Optional[float] = None  # $ - switching cost from default to TOU
    c_switch_back: Optional[float] = None  # $ - switching cost from TOU to default
    alpha: Optional[float] = None  # $/kWh - building-specific comfort penalty factor

    # Fallback to legacy single switching cost if building-dependent costs not available
    c_switch: float = 3.0  # $ - legacy switching cost (deprecated)

    # Peak hours: 12 PM to 8 PM (12:00 to 20:00)
    peak_start_hour: timedelta = timedelta(hours=12)
    peak_end_hour: timedelta = timedelta(hours=20)

    def get_switching_cost_to(self) -> float:
        """Get switching cost from default to TOU schedule."""
        return self.c_switch_to if self.c_switch_to is not None else self.c_switch

    def get_switching_cost_back(self) -> float:
        """Get switching cost from TOU back to default schedule."""
        return self.c_switch_back if self.c_switch_back is not None else (0.4 * self.get_switching_cost_to())

    def get_comfort_penalty_factor(self) -> float:
        """Get comfort penalty monetization factor."""
        return self.alpha if self.alpha is not None else 0.15


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


def create_tou_rates(
    timesteps: np.ndarray, time_step: timedelta, TOU_params: TOUParameters
) -> list[MonthlyRateStructure]:
    """
    Create TOU rate structure for each month in the simulation period

    Args:
        timesteps: Datetime array for each time step in the simulation
        time_step: Time step of the simulation
        TOU_params: TOU parameters

    Returns:
        List of MonthlyRateStructure, with rates for each interval in the month
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
            # Convert numpy.datetime64 to datetime for year/month extraction
            timestamp_dt = pd.to_datetime(timestamp)
            monthly_rates.append(
                MonthlyRateStructure(year=timestamp_dt.year, month=timestamp_dt.month, rates=month_rates)
            )

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
        # Get year and month from the last timestamp
        last_timestamp = timesteps[-1]
        timestamp_dt = pd.to_datetime(last_timestamp)
        monthly_rates.append(MonthlyRateStructure(year=timestamp_dt.year, month=timestamp_dt.month, rates=month_rates))

    return monthly_rates


def create_operation_schedule(
    current_state: str, monthly_rates: list[MonthlyRateStructure], TOU_params: TOUParameters, time_step: timedelta
) -> np.ndarray:
    """
    Create operational schedule based on current state

    Args:
        current_state: Current schedule state ("default" or "tou")
        monthly_rates: List of MonthlyRateStructure, with rates for each interval in the month
        TOU_params: TOU parameters (uses default if None)
        time_step: Time step of the simulation

    Returns:
        Boolean array where True=operation allowed, False=restricted.
        Length of array is sum of monthly_intervals, which is the total number of intervals in the simulation period.
    """
    monthly_intervals = [monthly_rate_structure.intervals for monthly_rate_structure in monthly_rates]

    if current_state == "default":  # Default schedule. Operation allowed at all times.
        return np.ones(sum(monthly_intervals), dtype=bool)
    else:  # TOU schedule. Operation restricted during peak hours.
        daily_peak_pattern = define_peak_hours(TOU_params, time_step)

        intervals_per_day = int(HOURS_PER_DAY * SECONDS_PER_HOUR / time_step.total_seconds())
        peak_pattern = np.array([], dtype=bool)
        for num_intervals in monthly_intervals:
            # Repeat pattern for the month
            num_days = num_intervals // intervals_per_day  # Number of days in the month
            month_pattern = np.tile(daily_peak_pattern, num_days)

            # Handle remainder
            remainder = num_intervals % intervals_per_day
            if remainder > 0:
                month_pattern = np.concatenate([month_pattern, daily_peak_pattern[:remainder]])

            peak_pattern = np.concatenate([peak_pattern, month_pattern])

        return (~peak_pattern[: sum(monthly_intervals)]).astype(
            bool
        )  # Operation is restricted during peak hours, hence the negation.


def create_building_dependent_tou_params(
    building_xml_path: str, base_params: Optional[TOUParameters] = None
) -> TOUParameters:
    """
    Create TOU parameters with building-dependent switching costs and comfort penalties.

    Args:
        building_xml_path: Path to building HPXML file
        base_params: Base TOU parameters (uses default if None)

    Returns:
        TOUParameters with building-specific costs and penalties
    """
    from rate_design_platform.utils.building_characteristics import (
        calculate_comfort_penalty_factor,
        calculate_switching_cost_back,
        calculate_switching_cost_to,
        enrich_building_characteristics,
        parse_building_xml,
    )

    if base_params is None:
        base_params = TOUParameters()

    # Parse building characteristics
    building_chars = parse_building_xml(building_xml_path)
    building_chars = enrich_building_characteristics(building_chars)

    # Calculate building-dependent parameters
    c_switch_to = calculate_switching_cost_to(building_chars)
    c_switch_back = calculate_switching_cost_back(c_switch_to)
    alpha = calculate_comfort_penalty_factor(building_chars)

    # Create new TOUParameters with building-dependent values
    return TOUParameters(
        r_on=base_params.r_on,
        r_off=base_params.r_off,
        c_switch_to=c_switch_to,
        c_switch_back=c_switch_back,
        alpha=alpha,
        c_switch=base_params.c_switch,  # Keep legacy value for backward compatibility
        peak_start_hour=base_params.peak_start_hour,
        peak_end_hour=base_params.peak_end_hour,
    )
