"""Tests for unified winter-first season configuration."""

from __future__ import annotations

import pytest

from utils.pre.compute_tou import make_winter_summer_seasons
from utils.pre.create_tariff import create_default_flat_tariff, create_seasonal_rate
from utils.pre.season_config import resolve_winter_summer_months


def test_resolve_winter_summer_months_derives_complement() -> None:
    winter, summer = resolve_winter_summer_months([12, 1, 2, 3])
    assert winter == [1, 2, 3, 12]
    assert summer == [4, 5, 6, 7, 8, 9, 10, 11]


@pytest.mark.parametrize(
    "winter_months",
    [
        [],
        [0, 1, 2],
        [1, 2, 13],
        list(range(1, 13)),
    ],
)
def test_resolve_winter_summer_months_validates_inputs(
    winter_months: list[int],
) -> None:
    with pytest.raises(ValueError):
        resolve_winter_summer_months(winter_months)


def test_tou_and_seasonal_discount_use_same_partition() -> None:
    winter_months = [1, 2, 3, 4, 10, 11, 12]
    seasons = make_winter_summer_seasons(winter_months)
    winter = next(season for season in seasons if season.name == "winter")
    summer = next(season for season in seasons if season.name == "summer")

    base = create_default_flat_tariff(
        label="base",
        volumetric_rate=0.2,
        fixed_charge=7.0,
        utility="TestUtility",
    )
    seasonal = create_seasonal_rate(
        base_tariff=base,
        label="seasonal_discount",
        winter_rate=0.12,
        summer_rate=0.2,
        winter_months=winter_months,
    )
    schedule = seasonal["items"][0]["energyweekdayschedule"]
    winter_months_from_tariff = {
        month_idx + 1 for month_idx, hours in enumerate(schedule) if hours[0] == 1
    }
    summer_months_from_tariff = {
        month_idx + 1 for month_idx, hours in enumerate(schedule) if hours[0] == 0
    }

    assert winter_months_from_tariff == set(winter.months)
    assert summer_months_from_tariff == set(summer.months)
