"""PJM data package — shared constants for the PJM LMP data pipeline."""

# Canonical S3 base URI for Hive-partitioned PJM real-time hourly LMP data.
# Layout: {PJM_LMP_S3_BASE}/zone={ZONE}/year={YYYY}/data.parquet
PJM_LMP_S3_BASE = "s3://data.sb/pjm/lmp/real_time/zones"
