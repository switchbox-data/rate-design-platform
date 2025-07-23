import os
from datetime import datetime, timedelta

from ochre.utils import default_input_path

from rate_design_platform.OchreSimulator import calculate_simulation_months

SAMPLE_HOUSE_ARGS = {
    "start_time": datetime(2018, 1, 1, 0, 0),
    "end_time": datetime(2018, 12, 31, 23, 59),
    "time_res": timedelta(minutes=15),
    "duration": timedelta(days=365),
    "initialization_time": timedelta(days=1),
    "save_results": False,
    "verbosity": 9,
    "metrics_verbosity": 7,
    "hpxml_file": os.path.join(default_input_path, "Input Files", "bldg0112631-up11.xml"),
    "hpxml_schedule_file": os.path.join(default_input_path, "Input Files", "bldg0112631_schedule.csv"),
    "weather_file": os.path.join(default_input_path, "Weather", "USA_CO_Denver.Intl.AP.725650_TMY3.epw"),
}


def test_calculate_simulation_months():
    """Test calculate_simulation_months function"""
    # Test with shorter period
    test_house_args = SAMPLE_HOUSE_ARGS.copy()
    test_house_args["start_time"] = datetime(2018, 1, 1, 0, 0)
    test_house_args["end_time"] = datetime(2018, 4, 1, 0, 0)  # 3 months

    year_months = calculate_simulation_months(test_house_args)

    assert len(year_months) == 3
    assert year_months[0] == (2018, 1)  # January
    assert year_months[1] == (2018, 2)  # February
    assert year_months[2] == (2018, 3)  # March
