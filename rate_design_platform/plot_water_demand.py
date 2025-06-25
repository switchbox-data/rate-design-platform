import glob
import os
from calendar import month_abbr
from datetime import datetime, timedelta

import boto3
from ochre import Dwelling  # type: ignore[import-untyped]


def _upload_csv_files_to_s3(output_path: str, bucket_name: str, s3_prefix: str) -> None:
    """
    Upload all CSV files from the outputs directory to S3.

    Args:
        output_path (str): Path to the outputs directory
        bucket_name (str): Name of the S3 bucket
        s3_prefix (str): Prefix for the S3 keys (folder structure)
    """
    # Initialize S3 client
    s3_client = boto3.client("s3")

    # Find all CSV files in the outputs directory
    csv_files = glob.glob(os.path.join(output_path, "*.csv"))

    if not csv_files:
        print("No CSV files found in outputs directory")
        return

    print(f"Found {len(csv_files)} CSV files to upload")

    for csv_file in csv_files:
        # Get just the filename
        filename = os.path.basename(csv_file)
        s3_key = f"{s3_prefix}{filename}"

        try:
            print(f"Uploading {filename} to s3://{bucket_name}/{s3_key}")
            s3_client.upload_file(csv_file, bucket_name, s3_key)
            print(f"Successfully uploaded {filename}")
        except Exception as e:
            print(f"Error uploading {filename}: {e}")


base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
input_path = os.path.join(base_path, "inputs")
output_path = os.path.join(base_path, "outputs")
docs_image_path = os.path.join(base_path, "docs", "images")

# Define file paths for building and schedule HPXML
bldg_id = 72
upgrade_id = 0
hpxml_building_file = os.path.join(input_path, f"bldg{bldg_id:07d}-up{upgrade_id:02d}.xml")  # Path to building HPXML
hpxml_schedule_file = os.path.join(
    input_path, f"bldg{bldg_id:07d}-up{upgrade_id:02d}_schedule.csv"
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
    "name": f"bldg{bldg_id:07d}-up{upgrade_id:02d}",
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
# Upload CSV files to S3
_upload_csv_files_to_s3(
    output_path, "switchbox-rate-design-platform", f"ochre_outputs/bldg{bldg_id:07d}-up{upgrade_id:02d}/"
)
