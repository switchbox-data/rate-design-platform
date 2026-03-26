"""Tests for utils.pre.rev_requirement.validate_psegli_flat_tariff.

Unit test writes tiny parquet fixtures under ``tests/`` via ``tempfile`` (avoids
pytest's basetemp under shared ``/ebs/tmp``, which can fail ownership checks).

Integration test scans a real ResStock release partition. Paths are built from
``--resstock-release-base``, ``--resstock-state``, and ``--resstock-upgrade``
(see ``tests/conftest.py``), matching AGENTS.md layout:

``{base}/load_curve_hourly/state={STATE}/upgrade={UP}/`` and
``{base}/metadata_utility/state={STATE}/utility_assignment.parquet``.

Example (integration test is opt-in — full partition scan)::

    uv run python -m pytest tests/test_validate_psegli_flat_tariff.py -v \\
      --run-resstock-integration \\
      --resstock-release-base=/ebs/data/nrel/resstock/res_2024_amy2018_2_sb \\
      --resstock-state=NY --resstock-upgrade=00 \\
      --resstock-electric-utilities=psegli,coned
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import cast

import polars as pl
import pytest

from utils.loads import (
    ELECTRIC_LOAD_COL,
    ELECTRIC_PV_COL,
    resstock_load_curve_hourly_partition_dir,
    resstock_utility_assignment_parquet_path,
)
from utils.pre.rev_requirement.validate_psegli_flat_tariff import (
    hourly_totals_by_utility_lazy,
)


def test_resstock_partition_path_helpers() -> None:
    assert (
        resstock_load_curve_hourly_partition_dir(
            "/data/resstock/release_root",
            state="ny",
            upgrade="2",
        )
        == "/data/resstock/release_root/load_curve_hourly/state=NY/upgrade=02"
    )
    assert resstock_utility_assignment_parquet_path(
        "s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb",
        state="ny",
    ) == (
        "s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/"
        "metadata_utility/state=NY/utility_assignment.parquet"
    )


def test_hourly_totals_by_utility_lazy_sums_per_timestamp() -> None:
    """Synthetic data; temp dir under ``tests/`` (current user), not pytest basetemp."""
    parent = Path(__file__).resolve().parent
    tmp = Path(tempfile.mkdtemp(prefix="tmp_validate_psegli_", dir=str(parent)))
    try:
        loads_dir = tmp / "load_curve_hourly"
        loads_dir.mkdir()

        ua_path = tmp / "utility_assignment.parquet"
        pl.DataFrame(
            {
                "bldg_id": [1, 2],
                "sb.electric_utility": ["acme", "other"],
            }
        ).write_parquet(ua_path)

        pl.DataFrame(
            {
                "bldg_id": [1, 1, 2],
                "timestamp": [
                    "2018-01-01T00:00:00",
                    "2018-01-01T01:00:00",
                    "2018-01-01T02:00:00",
                ],
                ELECTRIC_LOAD_COL: [10.0, 5.0, 999.0],
                ELECTRIC_PV_COL: [2.0, 0.0, 0.0],
            }
        ).write_parquet(loads_dir / "chunk.parquet")

        lf = hourly_totals_by_utility_lazy(
            str(loads_dir),
            str(ua_path),
            "acme",
        )
        df = cast(pl.DataFrame, lf.collect())

        assert df.height == 2
        rows = df.sort("total_grid_kwh")["total_grid_kwh"].to_list()
        assert rows == [5.0, 8.0]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_hourly_totals_resstock_partition_integration(
    request: pytest.FixtureRequest,
    electric_utility: str,
) -> None:
    if not request.config.getoption("--run-resstock-integration"):
        pytest.skip(
            "pass --run-resstock-integration to run (scans full load_curve_hourly partition)"
        )

    release_base = request.config.getoption("--resstock-release-base")
    state = request.config.getoption("--resstock-state")
    upgrade = request.config.getoption("--resstock-upgrade")

    loads_dir = resstock_load_curve_hourly_partition_dir(
        release_base,
        state=state,
        upgrade=upgrade,
    )
    ua_path = resstock_utility_assignment_parquet_path(
        release_base,
        state=state,
    )

    if not str(loads_dir).startswith("s3://"):
        if not Path(loads_dir).is_dir() or not Path(ua_path).is_file():
            pytest.skip(
                f"ResStock data not present (loads dir or UA missing): "
                f"{loads_dir!r} / {ua_path!r}"
            )

    lf = hourly_totals_by_utility_lazy(
        loads_dir,
        ua_path,
        electric_utility,
    )
    df = cast(pl.DataFrame, lf.collect())

    assert df.height > 0, "expected at least one hour after join/filter"
    assert df["total_grid_kwh"].null_count() == 0
