"""Tests for convert_pums_csv_to_parquet: path parsing, column lowercasing, data dictionary."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from data.census.pums.convert_pums_csv_to_parquet import (
    _build_schema_overrides,
    _data_dict_period,
    _parse_data_dict_csv,
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


# --- Data dictionary helpers ---


def test_data_dict_period_acs1() -> None:
    """acs1 uses year string."""
    assert _data_dict_period("acs1", 2023) == "2023"
    assert _data_dict_period("acs1", 2020) == "2020"


def test_data_dict_period_acs5() -> None:
    """acs5 uses period range."""
    assert _data_dict_period("acs5", 2023) == "2019-2023"
    assert _data_dict_period("acs5", 2022) == "2018-2022"
    assert _data_dict_period("acs5", 2021) == "2017-2021"


def test_data_dict_period_invalid() -> None:
    """Invalid survey raises."""
    with pytest.raises(ValueError, match="acs1 or acs5"):
        _data_dict_period("acs3", 2023)


def test_parse_data_dict_csv(tmp_path: Path) -> None:
    """Parse NAME rows into var schema."""
    csv_path = tmp_path / "dict.csv"
    csv_path.write_text(
        'NAME,RT,C,1,"Record Type"\n'
        'NAME,SERIALNO,C,13,"Housing unit serial"\n'
        'NAME,WGTP,N,5,"Housing Unit Weight"\n'
        'VAL,RT,C,1,"H","H","Housing"\n'
    )
    result = _parse_data_dict_csv(csv_path)
    assert result["RT"] == ("C", 1)
    assert result["SERIALNO"] == ("C", 13)
    assert result["WGTP"] == ("N", 5)
    assert "VAL" not in result


def test_build_schema_overrides() -> None:
    """C->Utf8; N->Int16/Int32/Int64 by length; unknown column raises."""
    var_schema = {"SERIALNO": ("C", 13), "WGTP": ("N", 5), "AGEP": ("N", 2)}
    overrides = _build_schema_overrides(var_schema, ["SERIALNO", "WGTP", "AGEP"])
    assert overrides["SERIALNO"] == pl.Utf8
    assert overrides["WGTP"] == pl.Int32  # length 5 -> Int32
    assert overrides["AGEP"] == pl.Int16  # length 2 -> Int16


def test_build_schema_overrides_case_insensitive() -> None:
    """Column lookup is case-insensitive (dict has uppercase)."""
    var_schema = {"SERIALNO": ("C", 13)}
    overrides = _build_schema_overrides(var_schema, ["serialno"])
    assert overrides["serialno"] == pl.Utf8


def test_build_schema_overrides_unknown_column_raises() -> None:
    """Unknown column raises ValueError."""
    var_schema = {"SERIALNO": ("C", 13)}
    with pytest.raises(ValueError, match="UNKNOWN_COL"):
        _build_schema_overrides(var_schema, ["SERIALNO", "UNKNOWN_COL"])
