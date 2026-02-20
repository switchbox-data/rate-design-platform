"""Tests for get_residential_customer_count_from_utility_stats."""

import os
from pathlib import Path

import polars as pl
import pytest

from rate_design.ri.hp_rates.run_scenario import (
    get_residential_customer_count_from_utility_stats,
)


def test_get_residential_customer_count_one_row(tmp_path: Path) -> None:
    """Returns residential_customers when exactly one row matches utility_code."""
    path = tmp_path / "stats.parquet"
    pl.DataFrame(
        {
            "utility_code": ["rie", "other"],
            "residential_customers": [451381, 100],
        }
    ).write_parquet(path)

    assert get_residential_customer_count_from_utility_stats(path, "rie") == 451381


def test_get_residential_customer_count_utility_not_found(tmp_path: Path) -> None:
    """Raises ValueError when no row has the given utility_code."""
    path = tmp_path / "stats.parquet"
    pl.DataFrame(
        {
            "utility_code": ["rie"],
            "residential_customers": [451381],
        }
    ).write_parquet(path)

    with pytest.raises(ValueError, match="No row with utility_code='coned'"):
        get_residential_customer_count_from_utility_stats(path, "coned")


def test_get_residential_customer_count_multiple_rows(tmp_path: Path) -> None:
    """Raises ValueError when more than one row matches utility_code."""
    path = tmp_path / "stats.parquet"
    pl.DataFrame(
        {
            "utility_code": ["rie", "rie"],
            "residential_customers": [451381, 451382],
        }
    ).write_parquet(path)

    with pytest.raises(ValueError, match="Expected one row for utility_code='rie'"):
        get_residential_customer_count_from_utility_stats(path, "rie")


def test_get_residential_customer_count_null_residential_raises(tmp_path: Path) -> None:
    """Raises ValueError when residential_customers is null for the utility."""
    path = tmp_path / "stats.parquet"
    pl.DataFrame(
        {
            "utility_code": ["rie"],
            "residential_customers": [None],
        }
    ).write_parquet(path)

    with pytest.raises(ValueError, match="residential_customers is null"):
        get_residential_customer_count_from_utility_stats(path, "rie")


@pytest.mark.skipif(
    not os.environ.get("AWS_REGION"),
    reason="AWS credentials/region required to read S3",
)
def test_coned_residential_customer_count_from_s3() -> None:
    """Read NY EIA-861 stats from S3 and verify ConEd residential customer count."""
    path = "s3://data.sb/eia/861/electric_utility_stats/state=NY/data.parquet"
    from utils import get_aws_region

    storage_options = {"aws_region": get_aws_region()}
    count = get_residential_customer_count_from_utility_stats(
        path,
        "coned",
        storage_options=storage_options,
    )
    assert count == 3_064_038
