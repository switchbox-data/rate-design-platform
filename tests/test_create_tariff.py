"""Tests for tariff creation utilities."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from utils.pre.create_tariff import (
    SeasonalTouTariffSpec,
    create_default_flat_tariff,
    create_seasonal_rate,
    create_seasonal_tou_tariff,
    create_tou_tariff,
    extract_base_rate_and_fixed_charge,
)


def test_create_default_flat_tariff_shape() -> None:
    tariff = create_default_flat_tariff(
        label="test_flat",
        volumetric_rate=0.2,
        fixed_charge=5.0,
        adjustment=0.01,
        utility="TestUtility",
    )
    item = tariff["items"][0]
    assert item["label"] == "test_flat"
    assert item["name"] == "test_flat"
    assert len(item["energyweekdayschedule"]) == 12
    assert len(item["energyweekdayschedule"][0]) == 24
    assert item["energyratestructure"][0][0]["rate"] == 0.2
    assert item["energyratestructure"][0][0]["adj"] == 0.01


def test_create_seasonal_rate_sets_winter_summer_periods() -> None:
    base = create_default_flat_tariff(
        label="base",
        volumetric_rate=0.2,
        fixed_charge=7.0,
        adjustment=0.0,
        utility="TestUtility",
    )
    seasonal = create_seasonal_rate(
        base_tariff=base,
        label="seasonal_discount",
        winter_rate=0.12,
        summer_rate=0.2,
        winter_months=[1, 2, 3, 10, 11, 12],
    )
    item = seasonal["items"][0]
    assert item["label"] == "seasonal_discount"
    assert item["name"] == "seasonal_discount"
    assert item["energyratestructure"][0][0]["rate"] == 0.2
    assert item["energyratestructure"][1][0]["rate"] == 0.12
    assert item["energyratestructure"][0][0]["adj"] == 0.0
    assert item["energyratestructure"][1][0]["adj"] == 0.0

    # Months are 0-indexed in URDB schedule array (Jan at idx 0).
    jan_period = item["energyweekdayschedule"][0][0]
    feb_period = item["energyweekdayschedule"][1][0]
    jul_period = item["energyweekdayschedule"][6][0]
    dec_period = item["energyweekdayschedule"][11][0]
    assert jan_period == 1
    assert feb_period == 1
    assert jul_period == 0
    assert dec_period == 1


def test_create_tou_tariff_has_two_periods() -> None:
    tou = create_tou_tariff(
        label="rie_tou_hp",
        peak_hours=[16, 17, 18, 19],
        peak_offpeak_ratio=1.5,
        base_rate=0.1,
        fixed_charge=6.75,
    )
    item = tou["items"][0]
    assert len(item["energyratestructure"]) == 2
    assert item["energyratestructure"][0][0]["rate"] == 0.1
    assert item["energyratestructure"][1][0]["rate"] == 0.15
    assert item["energyweekdayschedule"][0][16] == 1
    assert item["energyweekdayschedule"][0][8] == 0


def test_create_seasonal_tou_tariff_has_four_periods() -> None:
    winter = SeasonalTouTariffSpec(
        months=[1, 2, 3, 4, 5, 10, 11, 12],
        base_rate=0.08,
        peak_hours=[17, 18, 19, 20],
        peak_offpeak_ratio=1.6,
    )
    summer = SeasonalTouTariffSpec(
        months=[6, 7, 8, 9],
        base_rate=0.06,
        peak_hours=[14, 15, 16, 17],
        peak_offpeak_ratio=1.4,
    )
    tariff = create_seasonal_tou_tariff(
        label="rie_seasonal_tou_hp",
        specs=[winter, summer],
        fixed_charge=6.75,
    )
    item = tariff["items"][0]
    assert len(item["energyratestructure"]) == 4
    # January (index 0) uses winter off-peak period 0 and winter peak period 1.
    assert item["energyweekdayschedule"][0][10] == 0
    assert item["energyweekdayschedule"][0][18] == 1
    # July (index 6) uses summer off-peak period 2 and summer peak period 3.
    assert item["energyweekdayschedule"][6][10] == 2
    assert item["energyweekdayschedule"][6][15] == 3


# ---------------------------------------------------------------------------
# extract_base_rate_and_fixed_charge
# ---------------------------------------------------------------------------


def _write_tariff(tmp_path: Path, tariff: dict) -> Path:
    p = tmp_path / "tariff.json"
    p.write_text(json.dumps(tariff))
    return p


def test_extract_from_flat_tariff(tmp_path: Path) -> None:
    tariff = create_default_flat_tariff(
        label="flat", volumetric_rate=0.12, fixed_charge=8.50
    )
    path = _write_tariff(tmp_path, tariff)
    base_rate, fixed_charge = extract_base_rate_and_fixed_charge(path)
    assert base_rate == pytest.approx(0.12)
    assert fixed_charge == pytest.approx(8.50)


def test_extract_from_multi_period_tariff(tmp_path: Path) -> None:
    tou = create_tou_tariff(
        label="tou",
        peak_hours=[16, 17, 18, 19],
        peak_offpeak_ratio=2.0,
        base_rate=0.10,
        fixed_charge=6.75,
    )
    path = _write_tariff(tmp_path, tou)
    base_rate, fixed_charge = extract_base_rate_and_fixed_charge(path)
    # periods: 0.10 (off-peak), 0.20 (peak) -> avg 0.15
    assert base_rate == pytest.approx(0.15)
    assert fixed_charge == pytest.approx(6.75)


def test_extract_raises_on_missing_rate_structure(tmp_path: Path) -> None:
    tariff = {"items": [{"fixedchargefirstmeter": 5.0}]}
    path = _write_tariff(tmp_path, tariff)
    with pytest.raises(ValueError, match="energyratestructure"):
        extract_base_rate_and_fixed_charge(path)


def test_extract_raises_on_missing_fixed_charge(tmp_path: Path) -> None:
    tariff = {
        "items": [{"energyratestructure": [[{"rate": 0.1, "adj": 0.0, "unit": "kWh"}]]}]
    }
    path = _write_tariff(tmp_path, tariff)
    with pytest.raises(ValueError, match="fixedchargefirstmeter"):
        extract_base_rate_and_fixed_charge(path)


def test_extract_raises_on_empty_items(tmp_path: Path) -> None:
    tariff = {"items": []}
    path = _write_tariff(tmp_path, tariff)
    with pytest.raises(ValueError, match="items"):
        extract_base_rate_and_fixed_charge(path)
