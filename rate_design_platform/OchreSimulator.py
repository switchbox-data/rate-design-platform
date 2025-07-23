from datetime import timedelta

import numpy as np
from ochre import Dwelling  # type: ignore[import-untyped]

from rate_design_platform.Analysis import SimulationResults, extract_ochre_results


def run_ochre_wh_dynamic_control(  # type: ignore[no-any-unimported]
    dwelling: Dwelling,
    operation_schedule: np.ndarray,
    time_step: timedelta,
) -> SimulationResults:
    """
    Run OCHRE simulation with dynamic water heater control based on operation schedule

    Args:
        dwelling: OCHRE dwelling object
        operation_schedule: Boolean array (True=allowed, False=restricted) for each interval
        time_step: Time step of the simulation

    Returns:
        SimulationResults with electricity consumption, tank temps, and unmet demand
    """
    num_intervals = len(operation_schedule)
    # Get water heater equipment
    water_heater = dwelling.get_equipment_by_end_use("Water Heating")
    if water_heater is None:
        msg = "No water heating equipment found in dwelling"
        raise ValueError(msg)

    # Reset dwelling to start state
    dwelling.reset_time()

    # Dynamic control loop - step through each time interval

    for interval_idx, _date in enumerate(dwelling.sim_times):
        if interval_idx >= num_intervals:
            break

        # Set control signal based on operation schedule
        if not operation_schedule[interval_idx]:
            # Peak hour restriction - force water heater off
            control_signal = {"Water Heating": {"Load Fraction": 0}}
        else:
            # Normal operation allowed
            control_signal = {"Water Heating": {"Load Fraction": 1}}

        # Apply control and step simulation
        dwelling.update(control_signal=control_signal)

    # Finalize simulation and get results
    df, _, _ = dwelling.finalize()

    # Check if df is None and handle appropriately
    if df is None:
        raise ValueError()

    # Extract results from OCHRE output
    return extract_ochre_results(df, time_step)


def calculate_simulation_months(house_args: dict) -> list[tuple[int, int]]:
    """
    Return list of (year, month) tuples for each month in the simulation period
    """
    start_time = house_args["start_time"]
    end_time = house_args["end_time"]
    year_months = []
    current_time = start_time
    while current_time < end_time:
        year_months.append((current_time.year, current_time.month))
        if current_time.month == 12:
            current_time = current_time.replace(
                year=current_time.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
        else:
            current_time = current_time.replace(
                month=current_time.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
    return year_months
