"""Tests for tariff creation utilities."""

from __future__ import annotations

from utils.pre.create_tariff import (
    WINTER_MONTHS,
    create_default_flat_tariff,
    create_seasonal_rate,
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
    assert jan_period == (1 if 1 in WINTER_MONTHS else 0)
    assert feb_period == (1 if 2 in WINTER_MONTHS else 0)
    assert jul_period == 0
    assert dec_period == (1 if 12 in WINTER_MONTHS else 0)
