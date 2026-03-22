"""Tests for utils/pre/materialize_mixed_upgrade.py.

TODO: implement tests — this is a skeleton stub.

Planned coverage:
- Building assignment logic: correct fraction of buildings assigned to each
  upgrade per year (monotonicity, no double-assignment).
- Metadata combination: correct columns present, correct rows per upgrade.
- Symlink creation: correct targets, correct filenames
  (``{bldg_id}-{upgrade_id}.parquet``).
- Scenario CSV output: structure and values.
- Validation error paths:
    - upgrade data missing on disk.
    - fractions outside [0, 1].
    - total fraction > 1.0.
"""

import pytest


@pytest.mark.skip(reason="Not yet implemented")
def test_building_assignment_fractions() -> None:
    """Correct fraction of buildings is assigned to each upgrade per year."""
    raise NotImplementedError


@pytest.mark.skip(reason="Not yet implemented")
def test_building_assignment_monotonic() -> None:
    """Buildings that adopted in year N retain their upgrade in year N+1."""
    raise NotImplementedError


@pytest.mark.skip(reason="Not yet implemented")
def test_metadata_combination_columns() -> None:
    """Combined metadata parquet contains all required CAIRO columns."""
    raise NotImplementedError


@pytest.mark.skip(reason="Not yet implemented")
def test_metadata_combination_row_count() -> None:
    """Each building appears exactly once in the combined metadata."""
    raise NotImplementedError


@pytest.mark.skip(reason="Not yet implemented")
def test_symlink_targets_correct() -> None:
    """Symlinks in loads/ point to the correct upgrade's parquet file."""
    raise NotImplementedError


@pytest.mark.skip(reason="Not yet implemented")
def test_symlink_filenames_match_cairo_convention() -> None:
    """Symlink names follow the {bldg_id}-{upgrade_id}.parquet pattern."""
    raise NotImplementedError


@pytest.mark.skip(reason="Not yet implemented")
def test_scenario_csv_written() -> None:
    """Scenario CSV is written with bldg_id and one column per year."""
    raise NotImplementedError


@pytest.mark.skip(reason="Not yet implemented")
def test_missing_upgrade_directory_raises() -> None:
    """Error is raised when a required upgrade directory does not exist."""
    raise NotImplementedError


@pytest.mark.skip(reason="Not yet implemented")
def test_invalid_fractions_raise() -> None:
    """Fractions outside [0, 1] are rejected by validate_scenario()."""
    raise NotImplementedError


@pytest.mark.skip(reason="Not yet implemented")
def test_total_fraction_exceeds_one_raises() -> None:
    """Total fraction > 1.0 across upgrades is rejected by validate_scenario()."""
    raise NotImplementedError
