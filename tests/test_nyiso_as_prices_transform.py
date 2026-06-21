"""Unit tests for NYISO AS raw-archive → Polars wide transform (no network)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


def _load_fetch_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "data/nyiso/ancillary/fetch_nyiso_as_prices_parquet.py"
    )
    spec = importlib.util.spec_from_file_location("fetch_nyiso_as_prices_parquet", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_parse_markets_cli_defaults_to_rt() -> None:
    mod = _load_fetch_module()
    assert mod.parse_markets_cli("") == frozenset({"rt"})
    assert mod.parse_markets_cli("rt") == frozenset({"rt"})
    assert mod.parse_markets_cli("both") == frozenset({"dam", "rt"})
    assert mod.parse_markets_cli("dam,rt") == frozenset({"dam", "rt"})


def test_raw_archive_pdf_to_polars_wide_dam() -> None:
    mod = _load_fetch_module()
    ts = pd.Timestamp("2024-01-01 00:00:00", tz="America/New_York")
    te = pd.Timestamp("2024-01-01 01:00:00", tz="America/New_York")
    pdf = pd.DataFrame(
        {
            "Time": [ts],
            "Time Zone": ["EST"],
            "Name": ["WEST"],
            "PTID": [61761],
            "10 Min Spinning Reserve ($/MWHr)": [1.0],
            "10 Min Non-Synchronous Reserve ($/MWHr)": [2.0],
            "30 Min Operating Reserve ($/MWHr)": [3.0],
            "NYCA Regulation Capacity ($/MWHr)": [4.0],
            "Interval Start": [ts],
            "Interval End": [te],
        }
    )
    out = mod._raw_archive_pdf_to_polars(pdf, market="dam", year=2024, month=1)
    assert out.height == 1
    assert out["zone"].to_list() == ["WEST"]
    assert out["ptid"].to_list() == [61761]
    assert out["spin_10min_usd_per_mwhr"].to_list() == [1.0]
    assert out["nyca_regulation_movement_usd_per_mw"].is_null().all()


def test_raw_archive_pdf_to_polars_rt_interval_shift() -> None:
    mod = _load_fetch_module()
    ts = pd.Timestamp("2024-01-01 00:05:00", tz="America/New_York")
    te = pd.Timestamp("2024-01-01 00:10:00", tz="America/New_York")
    pdf = pd.DataFrame(
        {
            "Time": [ts],
            "Time Zone": ["EST"],
            "Name": ["WEST"],
            "PTID": [61761],
            "10 Min Spinning Reserve ($/MWHr)": [0.0],
            "10 Min Non-Synchronous Reserve ($/MWHr)": [0.0],
            "30 Min Operating Reserve ($/MWHr)": [0.0],
            "NYCA Regulation Capacity ($/MWHr)": [6.0],
            "NYCA Regulation Movement ($/MW)": [0.5],
            "Interval Start": [ts],
            "Interval End": [te],
        }
    )
    out = mod._raw_archive_pdf_to_polars(pdf, market="rt", year=2024, month=1)
    assert out.height == 1
    # gridstatus RT: End <- old Start, Start <- old Start - 5 min
    assert out["interval_end_et"].dt.strftime("%H:%M")[0] == "00:05"
    assert out["interval_start_et"].dt.strftime("%H:%M")[0] == "00:00"
    assert out["nyca_regulation_movement_usd_per_mw"].to_list() == [0.5]


def test_legacy_east_west_wide_rt_to_polars() -> None:
    """Pre-2016 hub-wide MIS columns (East/West) normalize to zonal long rows."""
    mod = _load_fetch_module()
    ts = pd.Timestamp("2015-01-01 00:05:00", tz="America/New_York")
    te = pd.Timestamp("2015-01-01 00:10:00", tz="America/New_York")
    pdf = pd.DataFrame(
        {
            "Time": [ts],
            "East 10 Min Spinning Reserve ($/MWHr)": [1.0],
            "East 10 Min Non-Synchronous Reserve": [2.0],
            "East 30 Min Operating Reserve ($/MWH)": [3.0],
            "East Regulation ($/MWHr)": [4.0],
            "West 10 Min Spinning Reserve ($/MWHr)": [5.0],
            "West 10 Min Non-Synchronous Reserve": [6.0],
            "West 30 Min Operating Reserve ($/MWH)": [7.0],
            "West Regulation ($/MWHr)": [8.0],
            "NYCA Regulation Movement ($/MW)": [0.5],
            "Interval Start": [ts],
            "Interval End": [te],
        }
    )
    out = mod._raw_archive_pdf_to_polars(pdf, market="rt", year=2015, month=1)
    assert out.height == 2
    assert set(out["zone"].to_list()) == {"EAST", "WEST"}
    assert out["ptid"].null_count() == 2
    assert out["spin_10min_usd_per_mwhr"].to_list() == [1.0, 5.0]
    assert out["nyca_regulation_movement_usd_per_mw"].to_list() == [0.5, 0.5]


def test_legacy_dam_truncated_paren_headers() -> None:
    """DAM MIS sometimes omits the closing ``)`` on spinning-reserve headers."""
    mod = _load_fetch_module()
    ts = pd.Timestamp("2010-01-01 00:00:00", tz="America/New_York")
    te = pd.Timestamp("2010-01-01 01:00:00", tz="America/New_York")
    pdf = pd.DataFrame(
        {
            "Time": [ts],
            "East 10 Min Spinning Reserve ($/MWHr": [1.0],
            "East 10 Min Non-Synchronous Reserve": [2.0],
            "East 30 Min Operating Reserve ($/MWH": [3.0],
            "East Regulation ($/MWHr)": [4.0],
            "West 10 Min Spinning Reserve ($/MWHr": [5.0],
            "West 10 Min Non-Synchronous Reserve": [6.0],
            "West 30 Min Operating Reserve ($/MWH": [7.0],
            "West Regulation ($/MWHr)": [8.0],
            "Interval Start": [ts],
            "Interval End": [te],
        }
    )
    out = mod._raw_archive_pdf_to_polars(pdf, market="dam", year=2010, month=1)
    assert out.height == 2
    assert out["nyca_regulation_movement_usd_per_mw"].is_null().all()


def test_raw_archive_passes_through_unknown_mis_columns() -> None:
    mod = _load_fetch_module()
    ts = pd.Timestamp("2024-01-01 00:00:00", tz="America/New_York")
    te = pd.Timestamp("2024-01-01 01:00:00", tz="America/New_York")
    pdf = pd.DataFrame(
        {
            "Time": [ts],
            "Time Zone": ["EST"],
            "Name": ["WEST"],
            "PTID": [61761],
            "10 Min Spinning Reserve ($/MWHr)": [1.0],
            "10 Min Non-Synchronous Reserve ($/MWHr)": [2.0],
            "30 Min Operating Reserve ($/MWHr)": [3.0],
            "NYCA Regulation Capacity ($/MWHr)": [4.0],
            "Interval Start": [ts],
            "Interval End": [te],
            "Hypothetical New Product ($/MWHr)": [9.25],
        }
    )
    out = mod._raw_archive_pdf_to_polars(pdf, market="dam", year=2024, month=1)
    assert "hypothetical_new_product_usd_per_mwhr" in out.columns
    assert out["hypothetical_new_product_usd_per_mwhr"].to_list() == [9.25]
