import os
from calendar import month_abbr
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import pandas as pd
from ochre import Dwelling  # type: ignore[import-untyped]

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
input_path = os.path.join(base_path, "inputs")
output_path = os.path.join(base_path, "outputs")
docs_image_path = os.path.join(base_path, "docs", "images")

# Define file paths for building and schedule HPXML
hpxml_building_file = os.path.join(input_path, "bldg0000072-up00.xml")  # Path to building HPXML
hpxml_schedule_file = os.path.join(
    input_path, "bldg0000072-up00_schedule.csv"
)  # Path to schedule HPXML (alternatively, a CSV or HDF5)
weather_file = os.path.join(input_path, "G3400270.epw")

# Define simulation parameters
year = 2007
month = "Jan"  # enumeration Jan, May, or Aug
month_num = list(month_abbr).index(month)
start_date = 1
start_time = datetime(year, month_num, start_date, 0, 0)  # (Year, Month, Day, Hour, Min)
time_step = timedelta(minutes=15)
duration = timedelta(days=7)
house_args = {
    # Timing parameters
    "start_time": start_time,
    "time_res": time_step,
    "duration": duration,
    "initialization_time": timedelta(days=1),
    # Output settings
    "save_results": True,
    "output_path": output_path,
    "verbosity": 9,  # verbosity of results file (0-9); 8: include envelope; 9: include water heater
    "metrics_verbosity": 7,
    # Input file settings
    "hpxml_file": hpxml_building_file,
    "hpxml_schedule_file": hpxml_schedule_file,
    "weather_file": weather_file,
}

# Create Dwelling object
dwelling = Dwelling(**house_args)

# Read in water demand timeseries using pandas
csv_file_path = os.path.join(output_path, "ochre_schedule.csv")
print("Reading CSV with pandas...")
df = pd.read_csv(csv_file_path)

# Convert Time column to datetime if it's not already
if df["Time"].dtype == "object":
    df["Time"] = pd.to_datetime(df["Time"])

# Create the plot using matplotlib
print("Creating plot with matplotlib...")
plt.figure(figsize=(12, 6))
plt.plot(df["Time"], df["Water Heating (L/min)"], linewidth=0.5)

# Customize the plot
plt.title("Hot water demand timeseries", fontsize=14, fontweight="bold")
plt.xlabel("Time", fontsize=12)
plt.ylabel("Hot water demand (L/min)", fontsize=12)
plt.grid(True, alpha=0.3)

# Rotate x-axis labels for better readability
plt.xticks(rotation=45)

# Adjust layout to prevent label cutoff
plt.tight_layout()

# Show the plot
plt.savefig(os.path.join(docs_image_path, "water_heating_timeseries.png"), dpi=300, bbox_inches="tight")
print(f"Plot saved to: {os.path.join(docs_image_path, 'water_heating_timeseries.png')}")
plt.close()
