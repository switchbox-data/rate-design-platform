# Apply CAIRO performance monkey-patches before any test runs.
# This ensures that tests which call cairo.rates_tool.loads.process_building_demand_by_period
# or cairo.rates_tool.system_revenues.run_system_revenues get the patched (vectorized) versions,
# regardless of test execution order or whether the test imports patches directly.
from __future__ import annotations

import os

import pytest

import utils.mid.patches  # noqa: F401


def pytest_addoption(parser: pytest.Parser) -> None:
    """CLI for ResStock-backed integration tests (e.g. validate_psegli_flat_tariff)."""
    parser.addoption(
        "--resstock-release-base",
        action="store",
        default=os.environ.get(
            "RESSTOCK_RELEASE_BASE",
            "/ebs/data/nrel/resstock/res_2024_amy2018_2_sb",
        ),
        help=(
            "ResStock release root (directory that contains load_curve_hourly/ and "
            "metadata_utility/). Override with RESSTOCK_RELEASE_BASE."
        ),
    )
    parser.addoption(
        "--resstock-state",
        action="store",
        default=os.environ.get("RESSTOCK_STATE", "NY"),
        help="Hive partition state (e.g. NY). Override with RESSTOCK_STATE.",
    )
    parser.addoption(
        "--resstock-upgrade",
        action="store",
        default=os.environ.get("RESSTOCK_UPGRADE", "00"),
        help="Hive partition upgrade id (e.g. 00). Override with RESSTOCK_UPGRADE.",
    )
    parser.addoption(
        "--resstock-electric-utility",
        action="store",
        default=os.environ.get("RESSTOCK_ELECTRIC_UTILITY", "psegli"),
        help=(
            "sb.electric_utility for integration test when "
            "--resstock-electric-utilities is not set."
        ),
    )
    parser.addoption(
        "--resstock-electric-utilities",
        action="store",
        default=os.environ.get("RESSTOCK_ELECTRIC_UTILITIES"),
        help=(
            "Comma-separated sb.electric_utility values (e.g. psegli,coned). "
            "When set, runs the ResStock integration test once per value. "
            "Override with RESSTOCK_ELECTRIC_UTILITIES."
        ),
    )
    parser.addoption(
        "--run-resstock-integration",
        action="store_true",
        default=False,
        help=(
            "Run ResStock integration tests that scan load_curve_hourly partitions "
            "(slow; not for CI by default)."
        ),
    )
    parser.addoption(
        "--flat-default-energy-rel-tol",
        type=float,
        default=float(os.environ.get("FLAT_DEFAULT_ENERGY_REL_TOL", "0.10")),
        help=(
            "Integration test: max relative |flat−default| / max(flat,default) on "
            "annual energy $ using *_flat_calibrated.json vs *_default_calibrated.json. "
            "Pre-precal JSONs can differ more; residual mismatch vs ResStock aggregate "
            "is still possible after calibration. Override with FLAT_DEFAULT_ENERGY_REL_TOL."
        ),
    )
    parser.addoption(
        "--eia-utility-stats",
        action="store",
        default=os.environ.get("EIA_UTILITY_STATS_PATH"),
        help=(
            "Parquet path or hive template for EIA-861 utility stats (contains "
            "residential_customers). If unset, defaults to "
            "s3://data.sb/eia/861/electric_utility_stats/year=<eia-stats-year>/state=<STATE>/data.parquet "
            "with STATE from --resstock-state."
        ),
    )
    parser.addoption(
        "--eia-stats-year",
        type=int,
        default=int(os.environ.get("EIA_STATS_YEAR", "2025")),
        help="Preferred reporting year for EIA-861 parquet (falls back to earlier years).",
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if (
        metafunc.function.__name__
        != "test_hourly_totals_resstock_partition_integration"
    ):
        return
    config = metafunc.config
    multi = config.getoption("--resstock-electric-utilities")
    if multi:
        utilities = [u.strip() for u in str(multi).split(",") if u.strip()]
    else:
        utilities = [config.getoption("--resstock-electric-utility")]
    metafunc.parametrize("electric_utility", utilities, ids=utilities)
