import os
from datetime import datetime, timedelta

import pandas as pd
from ochre.utils import default_input_path  # type: ignore[import-untyped]

from rate_design_platform.second_pass import run_full_simulation
from rate_design_platform.utils.rates import TOUParameters

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
input_path = os.path.join(base_path, "input_files")
output_path = os.path.join(base_path, "outputs")

hpxml_path = os.path.join(input_path, "res_2024_amy2018_2", "hpxml", "NY")
schedule_path = os.path.join(input_path, "res_2024_amy2018_2", "schedule", "NY")

weather_path = os.path.join(default_input_path, "Weather", "USA_CO_Denver.Intl.AP.725650_TMY3.epw")

# Simulation parameters
year = 2018
month = 1
start_date = 1
start_time = datetime(year, month, start_date, 0, 0)  # (Year, Month, Day, Hour, Min)
duration = timedelta(days=61)
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
}
TOU_PARAMS = TOUParameters()

monthly_results = []
annual_metrics = []

for bldg_file in os.listdir(hpxml_path):
    # Only process XML files
    if bldg_file.endswith(".xml"):
        # Get the base filename without extension
        bldg_upgrade_id = os.path.splitext(bldg_file)[0]
        print(f"Processing: {bldg_upgrade_id}")

        hpxml_file = os.path.join(hpxml_path, bldg_file)
        schedule_file = os.path.join(schedule_path, f"{bldg_upgrade_id}_schedule.csv")

        HOUSE_ARGS["hpxml_file"] = hpxml_file
        HOUSE_ARGS["hpxml_schedule_file"] = schedule_file
        HOUSE_ARGS["weather_file"] = weather_path

        monthly_result, annual_metric = run_full_simulation(TOU_PARAMS, HOUSE_ARGS)

        monthly_results.append(monthly_result)
        annual_metrics.append(annual_metric)
