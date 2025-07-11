"""
Plotting functions for TOU HPWH schedule analysis.

This module provides reusable plotting functions for analyzing water heating
performance under different scheduling scenarios.
"""

from pathlib import Path
from typing import Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


def plot_water_heating_comparison(
    building_id: str, base_path: Optional[Path] = None, days: int = 7, figsize: tuple[int, int] = (15, 8)
) -> plt.Figure:
    """
    Plot water heating electric power comparison between default and TOU schedules.

    Parameters
    ----------
    building_id : str
        Building ID to plot (e.g., "bldg0000072-up03")
    base_path : Path, optional
        Base path to the rate_design_platform directory. If None, uses current directory.
    days : int, default 7
        Number of days to plot
    figsize : tuple, default (15, 8)
        Figure size in inches

    Returns
    -------
    plt.Figure
        The matplotlib figure object
    """
    if base_path is None:
        base_path = Path(".")

    # Construct file paths
    default_file = base_path / "outputs" / "default_simulation" / f"{building_id}_default.csv"
    tou_file = base_path / "outputs" / "tou_simulation" / f"{building_id}_tou.csv"

    # Check if files exist
    if not default_file.exists():
        msg = f"Default simulation file not found: {default_file}"
        raise FileNotFoundError(msg)
    if not tou_file.exists():
        msg = f"TOU simulation file not found: {tou_file}"
        raise FileNotFoundError(msg)

    # Load data
    default_df = pd.read_csv(default_file, parse_dates=["Time"])
    tou_df = pd.read_csv(tou_file, parse_dates=["Time"])

    # Extract water heating power data
    default_power = default_df["Water Heating Electric Power (kW)"]
    tou_power = tou_df["Water Heating Electric Power (kW)"]
    time_index = default_df["Time"]

    # Limit to specified number of days
    days_delta = pd.Timedelta(days=days)
    end_time = time_index.min() + days_delta
    max_time = min(end_time, time_index.max())

    # Filter data
    mask = time_index <= max_time
    default_power_filtered = default_power[mask]
    tou_power_filtered = tou_power[mask]
    time_index_filtered = time_index[mask]

    # Create the plot with two y-axes
    fig, ax1 = plt.subplots(figsize=figsize)

    # Plot power data on primary y-axis (left)
    ax1.plot(
        time_index_filtered, default_power_filtered, label="Default Schedule", alpha=0.7, linewidth=2.5, color="blue"
    )
    ax1.plot(time_index_filtered, tou_power_filtered, label="TOU Schedule", alpha=0.7, linewidth=2.5, color="orange")
    ax1.set_xlabel("Time", fontsize=16)
    ax1.set_ylabel("Water Heating Electric Power (kW)", color="black", fontsize=16)
    ax1.tick_params(axis="y", labelcolor="black")
    ax1.grid(True, alpha=0.3)

    # Format x-axis to show dates nicely
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.tick_params(axis="x", labelsize=14)
    fig.autofmt_xdate()

    # Create secondary y-axis for TOU rates
    ax2 = ax1.twinx()

    # Extract water heating mode data and convert to TOU rates
    tou_mode = tou_df["Water Heating Mode"][mask]
    tou_rates = (tou_mode == "Upper On").astype(float)
    tou_rates = tou_rates * 0.12 + (1 - tou_rates) * 0.48

    # Plot TOU rates data on secondary y-axis (right)
    ax2.plot(
        time_index_filtered,
        tou_rates,
        label="TOU Rates ($/kWh)",
        alpha=0.5,
        linewidth=2.5,
        color="green",
        linestyle="--",
    )
    ax2.set_ylabel("TOU Rates ($/kWh)", color="green", fontsize=16)
    ax2.tick_params(axis="y", labelcolor="green")
    ax2.set_ylim(0.05, 0.55)

    # Add legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=16)

    plt.title(f"Water Heating Electric Power and TOU Rates Comparison: {building_id}", fontsize=20)
    plt.tight_layout()

    return fig


def plot_temperature_comparison(
    building_id: str, base_path: Optional[Path] = None, days: int = 7, figsize: tuple[int, int] = (15, 8)
) -> plt.Figure:
    """
    Plot hot water outlet temperature comparison between default and TOU schedules.

    Parameters
    ----------
    building_id : str
        Building ID to plot (e.g., "bldg0000072-up03")
    base_path : Path, optional
        Base path to the rate_design_platform directory. If None, uses current directory.
    days : int, default 7
        Number of days to plot
    figsize : tuple, default (15, 8)
        Figure size in inches

    Returns
    -------
    plt.Figure
        The matplotlib figure object
    """
    if base_path is None:
        base_path = Path(".")

    # Construct file paths
    default_file = base_path / "outputs" / "default_simulation" / f"{building_id}_default.csv"
    tou_file = base_path / "outputs" / "tou_simulation" / f"{building_id}_tou.csv"

    # Check if files exist
    if not default_file.exists():
        msg = f"Default simulation file not found: {default_file}"
        raise FileNotFoundError(msg)
    if not tou_file.exists():
        msg = f"TOU simulation file not found: {tou_file}"
        raise FileNotFoundError(msg)

    # Load data
    default_df = pd.read_csv(default_file, parse_dates=["Time"])
    tou_df = pd.read_csv(tou_file, parse_dates=["Time"])

    # Extract data
    default_temp = default_df["Hot Water Outlet Temperature (C)"]
    tou_temp = tou_df["Hot Water Outlet Temperature (C)"]
    time_index = default_df["Time"]

    # Extract deadband limits (using TOU data for consistency)
    deadband_upper = tou_df["Water Heating Deadband Upper Limit (C)"]
    deadband_lower = tou_df["Water Heating Deadband Lower Limit (C)"]

    # Limit to specified number of days
    days_delta = pd.Timedelta(days=days)
    end_time = time_index.min() + days_delta
    max_time = min(end_time, time_index.max())

    # Filter data
    mask = time_index <= max_time
    default_temp_filtered = default_temp[mask]
    tou_temp_filtered = tou_temp[mask]
    time_index_filtered = time_index[mask]
    deadband_upper_filtered = deadband_upper[mask]
    deadband_lower_filtered = deadband_lower[mask]

    # Create the plot
    fig, ax1 = plt.subplots(figsize=figsize)

    # Plot temperature data on primary y-axis (left)
    ax1.plot(
        time_index_filtered, default_temp_filtered, label="Default Schedule", alpha=0.7, linewidth=2.5, color="blue"
    )
    ax1.plot(time_index_filtered, tou_temp_filtered, label="TOU Schedule", alpha=0.7, linewidth=2.5, color="orange")
    ax1.plot(
        time_index_filtered,
        deadband_upper_filtered,
        label="Deadband Upper Limit",
        alpha=0.5,
        linewidth=2.5,
        color="red",
    )
    ax1.plot(
        time_index_filtered,
        deadband_lower_filtered,
        label="Deadband Lower Limit",
        alpha=0.5,
        linewidth=2.5,
        color="red",
    )
    ax1.set_xlabel("Time", fontsize=16)
    ax1.set_ylabel("Hot Water Outlet Temperature (°C)", color="black", fontsize=16)
    ax1.tick_params(axis="y", labelcolor="black")
    ax1.grid(True, alpha=0.3)

    # Format x-axis to show dates nicely
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.tick_params(axis="x", labelsize=14)
    fig.autofmt_xdate()

    # Create secondary y-axis for TOU rates
    ax2 = ax1.twinx()

    # Extract water heating mode data and convert to TOU rates
    tou_mode = tou_df["Water Heating Mode"][mask]
    tou_rates = (tou_mode == "Upper On").astype(float)
    tou_rates = tou_rates * 0.12 + (1 - tou_rates) * 0.48

    # Plot TOU rates data on secondary y-axis (right)
    ax2.plot(
        time_index_filtered, tou_rates, label="TOU Rates ($/kWh)", alpha=0.5, linewidth=2, color="green", linestyle="--"
    )
    ax2.set_ylabel("TOU Rates ($/kWh)", color="green", fontsize=16)
    ax2.tick_params(axis="y", labelcolor="green")
    ax2.set_ylim(0.05, 0.55)

    # Add legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=16)

    plt.title(f"Hot Water Outlet Temperature and TOU Rates Comparison: {building_id}", fontsize=20)
    plt.tight_layout()

    return fig


def plot_monthly_bills(
    building_id: str, base_path: Optional[Path] = None, figsize: tuple[int, int] = (15, 8)
) -> plt.Figure:
    """
    Plot monthly electricity bills with TOU state highlighting.

    Parameters
    ----------
    building_id : str
        Building ID to plot (e.g., "bldg0000072-up03")
    base_path : Path, optional
        Base path to the rate_design_platform directory. If None, uses current directory.
    figsize : tuple, default (15, 8)
        Figure size in inches

    Returns
    -------
    plt.Figure
        The matplotlib figure object
    """
    if base_path is None:
        base_path = Path(".")

    # Construct file paths
    default_bills_file = (
        base_path / "outputs" / "default_simulation" / "default_monthly_bills_and_comfort_penalties.csv"
    )
    tou_bills_file = base_path / "outputs" / "tou_simulation" / "tou_monthly_bills_and_comfort_penalties.csv"
    monthly_results_file = base_path / "outputs" / f"{building_id}_monthly_results.csv"

    # Load data
    default_bills_df = pd.read_csv(default_bills_file)
    tou_bills_df = pd.read_csv(tou_bills_file)
    monthly_results_df = pd.read_csv(monthly_results_file)

    # Create the plot
    fig, ax = plt.subplots(figsize=figsize)

    # Create month labels for x-axis
    month_labels = [f"{row['year']}-{row['month']:02d}" for _, row in monthly_results_df.iterrows()]

    # Plot default schedule bills
    ax.plot(
        range(len(default_bills_df)),
        default_bills_df["monthly_bills"],
        "o-",
        linewidth=2.5,
        markersize=8,
        color="blue",
        alpha=0.7,
        label="Default Schedule",
    )

    # Plot TOU schedule bills
    ax.plot(
        range(len(tou_bills_df)),
        tou_bills_df["monthly_bills"],
        "o-",
        linewidth=2.5,
        markersize=8,
        color="red",
        alpha=0.7,
        label="TOU Schedule",
    )

    # Highlight periods when consumer was actually on TOU
    tou_mask = monthly_results_df["current_state"] == "tou"
    if tou_mask.any():
        # Find consecutive TOU periods to create background shading
        tou_periods = []
        start_idx = None
        for i, is_tou in enumerate(tou_mask):
            if is_tou and start_idx is None:
                start_idx = i
            elif not is_tou and start_idx is not None:
                tou_periods.append((start_idx, i - 1))
                start_idx = None
        # Handle case where TOU period extends to the end
        if start_idx is not None:
            tou_periods.append((start_idx, len(tou_mask) - 1))

        # Add background shading for TOU periods
        for start, end in tou_periods:
            ax.axvspan(
                start - 0.5,
                end + 0.5,
                alpha=0.3,
                color="green",
                label="TOU Periods" if start == tou_periods[0][0] else "",
            )

    # Customize the plot
    ax.set_xlabel("Month", fontsize=16)
    ax.set_ylabel("Monthly Electricity Bill ($)", fontsize=16)
    ax.set_title(f"Monthly Electricity Bills: {building_id}", fontsize=20)
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=14)

    # Set x-axis ticks to show month labels
    ax.set_xticks(range(len(monthly_results_df)))
    ax.set_xticklabels(month_labels, rotation=45, ha="right", fontsize=12)

    plt.tight_layout()

    return fig


def load_monthly_results(building_id: str, base_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Load monthly results CSV file as a DataFrame.

    Parameters
    ----------
    building_id : str
        Building ID to load (e.g., "bldg0000072-up03")
    base_path : Path, optional
        Base path to the rate_design_platform directory. If None, uses current directory.

    Returns
    -------
    pd.DataFrame
        DataFrame containing monthly results with columns:
        - year, month, current_state, bill, comfort_penalty, switching_decision,
          realized_savings, unrealized_savings
    """
    if base_path is None:
        base_path = Path(".")

    # Construct file path
    monthly_results_file = base_path / "outputs" / f"{building_id}_monthly_results.csv"

    # Check if file exists
    if not monthly_results_file.exists():
        msg = f"Monthly results file not found: {monthly_results_file}"
        raise FileNotFoundError(msg)

    # Load and return the DataFrame
    df = pd.read_csv(monthly_results_file)

    # Add some helpful derived columns
    df["month_year"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
    df["total_cost"] = df["bill"] + df["comfort_penalty"]

    return df


def plot_all_comparisons(
    building_id: str, base_path: Optional[Path] = None, days: int = 7, figsize: tuple[int, int] = (15, 8)
) -> tuple[plt.Figure, plt.Figure, plt.Figure]:
    """
    Generate all three comparison plots for a building.

    Parameters
    ----------
    building_id : str
        Building ID to plot (e.g., "bldg0000072-up03")
    base_path : Path, optional
        Base path to the rate_design_platform directory. If None, uses current directory.
    days : int, default 7
        Number of days to plot for time series plots
    figsize : tuple, default (15, 8)
        Figure size in inches

    Returns
    -------
    tuple of plt.Figure
        Three matplotlib figure objects: (power_comparison, temp_comparison, monthly_bills)
    """
    power_fig = plot_water_heating_comparison(building_id, base_path, days, figsize)
    temp_fig = plot_temperature_comparison(building_id, base_path, days, figsize)
    bills_fig = plot_monthly_bills(building_id, base_path, figsize)

    return power_fig, temp_fig, bills_fig
