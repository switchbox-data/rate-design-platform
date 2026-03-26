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
