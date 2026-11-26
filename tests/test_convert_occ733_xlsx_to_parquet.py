"""OCC-733 Attachment 1 -> parquet converter. No S3.

The real workbook is not in git. These tests write a two-day stand-in for the
``Code_Input`` sheet, with the millisecond drift on ``DateTime`` and the source
notes in column K that the real sheet has.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import openpyxl
import polars as pl
import pytest

from data.eversource.occ733 import convert_occ733_xlsx_to_parquet as occ733

_HEADER = ["DateTime", "Year", "Month", "Day", "Hour", "Weekday", "Total", "Lost load  PV", "Net Load"]


def _excel_weekday(value: datetime) -> int:
    return value.isoweekday() % 7 + 1


def _write_workbook(path: Path, *, n_hours: int = 48, break_row: int | None = None, field: str = "") -> Path:
    """Two days of 2022 from Jan 1 (a Saturday); ``break_row`` corrupts ``field`` on that data row."""
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    assert sheet is not None
    sheet.title = "Code_Input "
    sheet.append([*_HEADER, None, None])
    start = datetime(2022, 1, 1)
    for i in range(n_hours):
        stamp = start + timedelta(hours=i)
        total = 1500.0 + i
        lost_pv = -float(i % 12)
        net = total + lost_pv
        weekday = _excel_weekday(stamp)
        if i == break_row and field == "net":
            net += 1.0
        if i == break_row and field == "weekday":
            weekday = weekday % 7 + 1
        drifted = stamp - timedelta(milliseconds=4) if i % 5 == 1 else stamp
        sheet.append([drifted, stamp.year, stamp.month, stamp.day, stamp.hour, weekday, total, lost_pv, net])
    sheet.cell(row=14, column=11, value="Source of PV additions through 2032:")
    workbook.save(path)
    return path


@pytest.fixture(autouse=True)
def _two_days(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(occ733, "_EXPECTED_HOURS", {2022: 48})


def test_hourly_load_rounds_drift_and_keeps_values(tmp_path: Path) -> None:
    df = occ733.hourly_load(_write_workbook(tmp_path / "attachment-1.xlsx"))

    assert df.columns == [
        "timestamp",
        "year",
        "month",
        "day",
        "hour",
        "weekday",
        "total_mw",
        "lost_load_pv_mw",
        "net_load_mw",
    ]
    assert df.height == 48
    assert df["timestamp"].to_list() == [datetime(2022, 1, 1) + timedelta(hours=i) for i in range(48)]
    assert df["weekday"].head(24).unique().to_list() == [7]
    assert df["weekday"].tail(24).unique().to_list() == [1]
    assert df["net_load_mw"].to_list() == (df["total_mw"] + df["lost_load_pv_mw"]).to_list()


@pytest.mark.parametrize(
    ("field", "message"),
    [("net", "Net Load differs"), ("weekday", "does not match")],
)
def test_bad_row_raises_before_writing(tmp_path: Path, field: str, message: str) -> None:
    path_xlsx = _write_workbook(tmp_path / "attachment-1.xlsx", break_row=30, field=field)
    path_out = tmp_path / "parquet"

    with pytest.raises(ValueError, match=message):
        occ733.write_hourly_load(path_xlsx, path_out)
    assert not path_out.exists()


def test_missing_hours_raise(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="hours per year"):
        occ733.hourly_load(_write_workbook(tmp_path / "attachment-1.xlsx", n_hours=47))


def test_header_mismatch_raises(tmp_path: Path) -> None:
    path_xlsx = _write_workbook(tmp_path / "attachment-1.xlsx")
    workbook = openpyxl.load_workbook(path_xlsx)
    workbook["Code_Input "].cell(row=1, column=7, value="Gross")
    workbook.save(path_xlsx)

    with pytest.raises(ValueError, match="header"):
        occ733.hourly_load(path_xlsx)


def test_main_writes_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path_xlsx = _write_workbook(tmp_path / "attachment-1.xlsx")
    path_out = tmp_path / "parquet"
    monkeypatch.setattr(
        sys,
        "argv",
        ["convert_occ733_xlsx_to_parquet.py", "--path-xlsx", str(path_xlsx), "--path-local-parquet", str(path_out)],
    )

    occ733.main()

    assert pl.read_parquet(path_out / "hourly_load.parquet").height == 48


def test_rejects_just_placeholder(tmp_path: Path) -> None:
    path_xlsx = _write_workbook(tmp_path / "attachment-1.xlsx")
    with pytest.raises(ValueError, match="uninterpolated"):
        occ733.write_hourly_load(path_xlsx, Path("{{ path_local_parquet }}"))
