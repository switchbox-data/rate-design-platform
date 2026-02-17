"""Tests for PUMS convert: path parsing and column lowercasing."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from data.census.pums.convert_pums_csv_to_parquet import (
    normalize_columns,
    parse_pums_partition_path,
)


def test_parse_pums_partition_path_valid() -> None:
    """Parse canonical partition path returns (survey, year, record_type, state)."""
    assert parse_pums_partition_path("acs1/2020/person/state=RI") == (
        "acs1",
        2020,
        "person",
        "RI",
    )
    assert parse_pums_partition_path(Path("csv/acs5/2023/housing/state=DC")) == (
        "acs5",
        2023,
        "housing",
        "DC",
    )
    assert parse_pums_partition_path("/foo/bar/acs1/2022/person/state=NY") == (
        "acs1",
        2022,
        "person",
        "NY",
    )


def test_parse_pums_partition_path_state_uppercase() -> None:
    """Returned state is uppercase."""
    r = parse_pums_partition_path("acs1/2020/person/state=ri")
    assert r is not None
    assert r[3] == "RI"


def test_parse_pums_partition_path_invalid_returns_none() -> None:
    """Non-matching paths return None."""
    assert parse_pums_partition_path("acs1/2020/person") is None
    assert parse_pums_partition_path("acs1/2020/state=RI") is None
    assert parse_pums_partition_path("acs3/2020/person/state=RI") is None
    assert parse_pums_partition_path("acs1/20/person/state=RI") is None
    assert parse_pums_partition_path("acs1/2020/housing/state=USA") is None
    assert parse_pums_partition_path("") is None


def test_parse_pums_partition_path_trailing_slash_ignored() -> None:
    """Path with trailing slash still parses (Path normalizes)."""
    r = parse_pums_partition_path(Path("csv/acs1/2020/person/state=RI/"))
    assert r == ("acs1", 2020, "person", "RI")


def test_normalize_columns_lowercases() -> None:
    """normalize_columns lowercases all column names."""
    df = pl.DataFrame({"RT": [1], "SERIALNO": [2], "SPORDER": [3]})
    out = normalize_columns(df)
    assert list(out.columns) == ["rt", "serialno", "sporder"]
    assert out.shape == df.shape
