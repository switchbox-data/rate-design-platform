import matplotlib.pyplot as plt
import numpy as np
from ochre.utils.hpxml import parse_hpxml


def get_water_usage_timeseries(hpxml_file):
    # Parse HPXML and extract building data
    hpxml_data = parse_hpxml(hpxml_file)
    wh_data = hpxml_data["WaterHeater"]
    schedule_data = hpxml_data["Schedules"]
    # Get fixture daily usage (gallons/day, can convert to liters below)
    fixture_gal_per_day = wh_data["Fixture Average Water Draw (L/day)"] / 3.78541  # Convert L to gal if needed
    # Get HW schedule (should be a normalized array, e.g. 24 or 96 values)
    # This may need adjustment depending on the exact HPXML and OCHRE version
    hw_schedule = np.array(schedule_data["HotWaterUsage"])  # array of fractions summing to 1.0
    n_steps = len(hw_schedule)
    step_minutes = int(1440 / n_steps)  # 1440 minutes in a day
    # Compute water usage for each timestep (gallons)
    usage_per_step = fixture_gal_per_day * hw_schedule
    # Compute flow rate per timestep (gal/min)
    flow_rate = usage_per_step / step_minutes
    return flow_rate, step_minutes, n_steps


def plot_water_usage(flow_rate, step_minutes, n_steps):
    hours = np.arange(n_steps) * (step_minutes / 60)
    plt.figure(figsize=(10, 5))
    plt.step(hours, flow_rate, where="mid", label="Hot Water Usage (gal/min)")
    plt.xlabel("Hour of Day")
    plt.ylabel("Flow Rate (gal/min)")
    plt.title("OCHRE Hot Water Usage Demand Time Series")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # Path to your HPXML input file
    hpxml_file = "your_building.xml"
    flow_rate, step_minutes, n_steps = get_water_usage_timeseries(hpxml_file)
    plot_water_usage(flow_rate, step_minutes, n_steps)
