"""Validations for the ResStock data pipeline.

- ``preflight``: hard checks that raise and halt the pipeline (argument conflicts,
  missing files, failed uploads).
- ``real_world_checks``: advisory end-of-run comparisons of the built release
  against EIA-861, EIA-176, and ACS PUMS. These only warn.

The pre-flight functions are re-exported here so ``from data.resstock.validations
import validate_local_files`` keeps working.
"""

from data.resstock.validations.preflight import (
    validate_gas_usage_inputs,
    validate_local_files,
    validate_metadata_columns,
    validate_metadata_output,
    validate_metadata_readable,
    validate_no_stale_aggregate_loads,
    validate_s3_objects,
    validate_utility_assignment_args,
)

__all__ = [
    "validate_gas_usage_inputs",
    "validate_local_files",
    "validate_metadata_columns",
    "validate_metadata_output",
    "validate_metadata_readable",
    "validate_no_stale_aggregate_loads",
    "validate_s3_objects",
    "validate_utility_assignment_args",
]
