"""Tests for NYISO zonal LBMP pipeline: header normalization, convert, validate, fetch CLI."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from data.nyiso.lbmp.convert_lbmp_zonal_zips_to_parquet import (
    CANONICAL_COLUMNS,
    RAW_TO_CANONICAL,
    month_key_from_zip_path,
    normalize_header,
    parse_timestamp_and_types,
    read_csv_from_bytes,
)
from data.nyiso.lbmp.fetch_lbmp_zonal_zips import (
    _last_complete_month,
    _parse_yyyy_mm,
    _month_range,
)
from data.nyiso.lbmp.validate_lbmp_zonal_parquet import (
    expected_rows_day_ahead,
    expected_rows_real_time,
    check_schema,
    check_zones,
    CANONICAL_NYISO_ZONES,
)


def test_normalize_header_strip_and_typo() -> None:
    raw = ["Time Stamp", "Name", "PTID", "LBMP ($/MWHr)", "Marginal Cost Losses ($/MWHr)", 'Marginal Cost Congestion ($/MWH"']
    out = normalize_header(raw)
    assert out[-1] == "Marginal Cost Congestion ($/MWHr)"
    assert out[0] == "Time Stamp"


def test_normalize_header_strip_r() -> None:
    raw = ["Time Stamp\r", "Name", "PTID", "LBMP ($/MWHr)", "Marginal Cost Losses ($/MWHr)", "Marginal Cost Congestion ($/MWHr)"]
    out = normalize_header(raw)
    assert out[0] == "Time Stamp"


def test_read_csv_from_bytes_minimal() -> None:
    csv = (
        "Time Stamp,Name,PTID,LBMP ($/MWHr),Marginal Cost Losses ($/MWHr),Marginal Cost Congestion ($/MWHr)\n"
        "01/15/2024 00:00:00,CAPITL,61757,28.50,0.82,1.20\n"
        "01/15/2024 01:00:00,CAPITL,61757,24.30,0.71,0.88\n"
    )
    df = read_csv_from_bytes(csv.encode("utf-8"))
    assert not df.is_empty()
    assert list(df.columns) == CANONICAL_COLUMNS
    assert df.height == 2


def test_parse_timestamp_and_types() -> None:
    df = pl.DataFrame({
        "interval_start_est": ["01/15/2024 00:00:00", "01/15/2024 01:00:00"],
        "zone": ["CAPITL", "CAPITL"],
        "ptid": ["61757", "61757"],
        "lbmp_usd_per_mwh": ["28.5", "24.3"],
        "marginal_cost_losses_usd_per_mwh": ["0.82", "0.71"],
        "marginal_cost_congestion_usd_per_mwh": ["1.20", "0.88"],
    })
    out = parse_timestamp_and_types(df)
    assert out.schema["interval_start_est"] == pl.Datetime("us", "America/New_York")
    assert out.schema["ptid"] == pl.Int32
    assert out.schema["lbmp_usd_per_mwh"] == pl.Float64


def test_month_key_from_zip_path() -> None:
    assert month_key_from_zip_path(Path("20240101damlbmp_zone_csv.zip")) == "202401"
    assert month_key_from_zip_path(Path("20241201realtime_zone_csv.zip")) == "202412"
    assert month_key_from_zip_path(Path("other.zip")) is None


def test_parse_yyyy_mm() -> None:
    assert _parse_yyyy_mm("2000-01") == (2000, 1)
    assert _parse_yyyy_mm("2024-12") == (2024, 12)
    with pytest.raises(ValueError):
        _parse_yyyy_mm("2024-13")
    with pytest.raises(ValueError):
        _parse_yyyy_mm("invalid")


def test_month_range() -> None:
    assert _month_range("2024-01", "2024-01") == ["20240101"]
    assert _month_range("2024-01", "2024-03") == ["20240101", "20240201", "20240301"]


def test_last_complete_month_is_yyyy_mm() -> None:
    s = _last_complete_month()
    assert len(s) == 7 and s[4] == "-"
    y, m = int(s[:4]), int(s[5:7])
    assert 1 <= m <= 12


def test_expected_rows_day_ahead() -> None:
    # Jan 2024: 31 days * 24 = 744
    assert expected_rows_day_ahead(2024, 1) == 31 * 24
    assert expected_rows_day_ahead(2024, 2) == 29 * 24  # leap year


def test_expected_rows_real_time() -> None:
    # Jan 2024: 31 * 24 * 12 (5-min) = 8928
    assert expected_rows_real_time(2024, 1) == 31 * 24 * 12


def test_check_schema_ok() -> None:
    df = pl.DataFrame({
        "interval_start_est": pl.Series([datetime(2024, 1, 1)]).cast(pl.Datetime("us", "America/New_York")),
        "zone": ["CAPITL"],
        "ptid": [61757],
        "lbmp_usd_per_mwh": [28.5],
        "marginal_cost_losses_usd_per_mwh": [0.82],
        "marginal_cost_congestion_usd_per_mwh": [1.2],
    }).cast({"ptid": pl.Int32})
    errs = check_schema(df)
    assert errs == [], errs


def test_check_schema_rejects_wrong_types() -> None:
    df = pl.DataFrame({
        "interval_start_est": ["2024-01-01 00:00:00"],
        "zone": ["CAPITL"],
        "ptid": ["61757"],
        "lbmp_usd_per_mwh": [28.5],
        "marginal_cost_losses_usd_per_mwh": [0.82],
        "marginal_cost_congestion_usd_per_mwh": [1.2],
    })
    errs = check_schema(df)
    assert any("ptid" in e for e in errs or [])


def test_check_zones_single_known() -> None:
    df = pl.DataFrame({"zone": ["CAPITL", "CAPITL"]})
    assert check_zones(df) == []


def test_check_zones_non_canonical_allowed() -> None:
    """Non-canonical zone is allowed (no error); only exactly-one-zone is enforced."""
    df = pl.DataFrame({"zone": ["UNKNOWN"]})
    errs = check_zones(df)
    assert errs == []  # no longer an error, just logged


def test_raw_to_canonical_has_all_six() -> None:
    assert len(RAW_TO_CANONICAL) == 6
    assert set(RAW_TO_CANONICAL.values()) == set(CANONICAL_COLUMNS)


def test_nyiso_zones_eleven() -> None:
    assert len(CANONICAL_NYISO_ZONES) == 11
