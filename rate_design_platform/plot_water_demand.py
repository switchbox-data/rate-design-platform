import os
from calendar import month_abbr
from datetime import datetime, timedelta

from ochre import Dwelling  # type: ignore[import-untyped]

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
input_path = os.path.join(base_path, "inputs")
output_path = os.path.join(base_path, "outputs")

# -- 1. Define file paths for building and schedule HPXMLs --
hpxml_building_file = os.path.join(input_path, "bldg0000072-up00.xml")  # Path to building HPXML
hpxml_schedule_file = os.path.join(
    input_path, "bldg0000072-up00_schedule.csv"
)  # Path to schedule HPXML (alternatively, a CSV or HDF5)
weather_file = os.path.join(input_path, "G3400270.epw")

# --2. Create sample Dwelling object --
year = 2007
month = "Jan"  # enumeration Jan, May, or Aug
month_num = list(month_abbr).index(month)
start_date = 1
start_time = datetime(year, month_num, start_date, 0, 0)  # (Year, Month, Day, Hour, Min)
time_step = timedelta(minutes=15)
duration = timedelta(days=30)
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

# --3. Create Dwelling object --
dwelling = Dwelling(**house_args)
