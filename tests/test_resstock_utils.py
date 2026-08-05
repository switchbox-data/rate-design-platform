"""Tests for data/resstock/utils.py — specifically upload() error behavior.

The upload() function was changed to raise RuntimeError when any
``aws s3 sync`` command exits with a non-zero code, rather than
silently printing a WARNING and continuing.  Two key guarantees:

1. All (file_type, state) combinations are attempted even if an
   earlier one fails, so a partial failure does not silently skip the
   remaining uploads.
2. After all combinations are tried, a single RuntimeError is raised
   that lists every failed combination.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from data.resstock.utils import upload


# ── helpers ───────────────────────────────────────────────────────────────────


def _sync_result(returncode: int) -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    return m


# ── upload() error-handling tests ────────────────────────────────────────────


def test_upload_raises_on_single_failure(tmp_path: Path) -> None:
    """RuntimeError is raised when aws s3 sync returns non-zero for one combo."""
    (tmp_path / "rel" / "metadata" / "state=MD").mkdir(parents=True)

    with patch("subprocess.run", return_value=_sync_result(1)) as mock_run:
        with pytest.raises(RuntimeError, match="1 combination"):
            upload(
                state=["MD"],
                file_types=["metadata"],
                release="rel",
                path_output_dir=tmp_path,
                path_s3_dir="s3://data.sb/nrel/resstock",
            )
    assert mock_run.call_count == 1


def test_upload_raises_listing_all_failures(tmp_path: Path) -> None:
    """All failed combos are listed in the RuntimeError, not just the first."""
    for ft in ("metadata", "load_curve_hourly"):
        (tmp_path / "rel" / ft / "state=MD").mkdir(parents=True)

    # Both combos fail
    with patch("subprocess.run", return_value=_sync_result(1)):
        with pytest.raises(RuntimeError) as exc_info:
            upload(
                state=["MD"],
                file_types=["metadata", "load_curve_hourly"],
                release="rel",
                path_output_dir=tmp_path,
                path_s3_dir="s3://data.sb/nrel/resstock",
            )

    msg = str(exc_info.value)
    assert "2 combination" in msg
    assert "metadata" in msg
    assert "load_curve_hourly" in msg


def test_upload_attempts_all_combos_despite_earlier_failure(tmp_path: Path) -> None:
    """Even when the first combination fails, all remaining combos are attempted."""
    for ft in ("metadata", "load_curve_hourly"):
        (tmp_path / "rel" / ft / "state=MD").mkdir(parents=True)

    # First call fails, second succeeds
    results = [_sync_result(1), _sync_result(0)]
    with patch("subprocess.run", side_effect=results) as mock_run:
        with pytest.raises(RuntimeError, match="1 combination"):
            upload(
                state=["MD"],
                file_types=["metadata", "load_curve_hourly"],
                release="rel",
                path_output_dir=tmp_path,
                path_s3_dir="s3://data.sb/nrel/resstock",
            )

    # Both combos were attempted
    assert mock_run.call_count == 2


def test_upload_does_not_raise_on_all_success(tmp_path: Path) -> None:
    """No exception is raised when all aws s3 sync commands succeed."""
    (tmp_path / "rel" / "metadata" / "state=MD").mkdir(parents=True)

    with patch("subprocess.run", return_value=_sync_result(0)):
        upload(
            state=["MD"],
            file_types=["metadata"],
            release="rel",
            path_output_dir=tmp_path,
            path_s3_dir="s3://data.sb/nrel/resstock",
        )


def test_upload_invokes_correct_s3_paths(tmp_path: Path) -> None:
    """aws s3 sync is called with the expected local and S3 paths."""
    (tmp_path / "rel" / "metadata" / "state=MD").mkdir(parents=True)
    expected_local = str(tmp_path / "rel" / "metadata" / "state=MD")
    expected_s3 = "s3://data.sb/nrel/resstock/rel/metadata/state=MD/"

    with patch("subprocess.run", return_value=_sync_result(0)) as mock_run:
        upload(
            state=["MD"],
            file_types=["metadata"],
            release="rel",
            path_output_dir=tmp_path,
            path_s3_dir="s3://data.sb/nrel/resstock",
        )

    invoked_cmd = mock_run.call_args[0][0]
    assert invoked_cmd[0] == "aws"
    assert invoked_cmd[1] == "s3"
    assert invoked_cmd[2] == "sync"
    assert invoked_cmd[3] == expected_local
    assert invoked_cmd[4] == expected_s3
