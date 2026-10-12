"""Month-hour marginal costs expanded onto a Cairo 8760."""

from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

from utils.data_prep.marginal_costs.sub_tx_and_dist.generate_utility_tx_dx_mc import (
    _voltage_from_table_paths,
    expand_month_hour_mc_to_8760,
    scale_8760_to_annual,
)


def test_voltage_is_read_from_table_names() -> None:
    base = "s3://bucket/occ-863/parquet/month_hour_marginal_costs"
    assert (
        _voltage_from_table_paths(
            f"{base}_weekday_secondary.parquet",
            f"{base}_weekend_holidays_secondary.parquet",
        )
        == "secondary"
    )
    with pytest.raises(ValueError, match="--voltage"):
        _voltage_from_table_paths(
            f"{base}_weekday_secondary.parquet",
            f"{base}_weekend_holidays_primary.parquet",
        )


def _table(value_for_month_hour: float, *, weekend: bool) -> pl.DataFrame:
    sign = -1.0 if weekend else 1.0
    rows = [
        {
            "month": month,
            "hour": hour,
            "num_days": 1,
            "mc_total_per_kwh": sign * (month + hour / 100 + value_for_month_hour),
        }
        for month in range(1, 13)
        for hour in range(24)
    ]
    return pl.DataFrame(rows)


def test_weekday_weekend_and_holiday_lookup() -> None:
    expanded = expand_month_hour_mc_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2025,
        "ct_eversource",
    )
    assert expanded.columns == ["timestamp", "utility", "year", "mc_total_per_kwh"]
    assert expanded.height == 8760
    assert expanded.schema["timestamp"] == pl.Datetime("us")
    assert expanded.schema["year"] == pl.Int32

    def value_at(stamp: str) -> float:
        return expanded.filter(pl.col("timestamp") == datetime.fromisoformat(stamp))[
            "mc_total_per_kwh"
        ].item()

    # Wednesday that is not a holiday.
    assert value_at("2025-01-08 00:00:00") == pytest.approx(1.0)
    # New Year's Day and July 4 use the weekend-and-holiday table.
    assert value_at("2025-01-01 00:00:00") == pytest.approx(-1.0)
    assert value_at("2025-07-04 16:00:00") == pytest.approx(-7.16)
    # The Thursday before July 4 stays on the weekday table.
    assert value_at("2025-07-03 16:00:00") == pytest.approx(7.16)
    # Saturday and Sunday: weekend table, including the spring-forward 02:00 slot.
    assert value_at("2025-01-04 16:00:00") == pytest.approx(-1.16)
    assert value_at("2025-03-09 02:00:00") == pytest.approx(-3.02)


def test_saturday_holiday_is_observed_on_friday() -> None:
    expanded = expand_month_hour_mc_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2026,
        "ct_eversource",
    )

    def value_at(stamp: str) -> float:
        return expanded.filter(pl.col("timestamp") == datetime.fromisoformat(stamp))[
            "mc_total_per_kwh"
        ].item()

    # July 4, 2026 is a Saturday, so Friday July 3 is the observed holiday.
    assert value_at("2026-07-03 16:00:00") == pytest.approx(-7.16)
    assert value_at("2026-07-06 16:00:00") == pytest.approx(7.16)


def test_weekday_holidays_stay_on_weekday_table_when_holidays_excluded() -> None:
    expanded = expand_month_hour_mc_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2025,
        "ct_eversource",
        include_holidays=False,
    )

    def value_at(stamp: str) -> float:
        return expanded.filter(pl.col("timestamp") == datetime.fromisoformat(stamp))[
            "mc_total_per_kwh"
        ].item()

    # Wednesday New Year's and Friday July 4 stay on the weekday table.
    assert value_at("2025-01-01 00:00:00") == pytest.approx(1.0)
    assert value_at("2025-07-04 16:00:00") == pytest.approx(7.16)
    # Saturday still uses the weekend table.
    assert value_at("2025-01-04 16:00:00") == pytest.approx(-1.16)


def test_calendar_year_sets_day_types_and_year_sets_timestamps() -> None:
    expanded = expand_month_hour_mc_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2025,
        "ct_eversource",
        calendar_year=2018,
    )
    assert expanded.height == 8760
    assert expanded["timestamp"].dt.year().unique().to_list() == [2025]
    assert expanded["year"].unique().to_list() == [2025]

    def value_at(stamp: str) -> float:
        return expanded.filter(pl.col("timestamp") == datetime.fromisoformat(stamp))[
            "mc_total_per_kwh"
        ].item()

    # 2025-01-04 is a Saturday, but 2018-01-04 was a Thursday.
    assert value_at("2025-01-04 16:00:00") == pytest.approx(1.16)
    # 2025-01-06 is a Monday, but 2018-01-06 was a Saturday.
    assert value_at("2025-01-06 16:00:00") == pytest.approx(-1.16)
    # July 4 is a holiday on either calendar.
    assert value_at("2025-07-04 16:00:00") == pytest.approx(-7.16)
    # Thanksgiving 2025 is Nov 27; 2018-11-27 was an ordinary Tuesday.
    assert value_at("2025-11-27 16:00:00") == pytest.approx(11.16)


def test_calendar_year_must_match_leap_status() -> None:
    with pytest.raises(ValueError, match="leap"):
        expand_month_hour_mc_to_8760(
            _table(0.0, weekend=False),
            _table(0.0, weekend=True),
            2024,
            "ct_eversource",
            calendar_year=2018,
        )


def test_uniform_scale_matches_annual_total_and_keeps_shape() -> None:
    expanded = expand_month_hour_mc_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2025,
        "ct_eversource",
    )
    before = expanded["mc_total_per_kwh"]
    scaled, factor = scale_8760_to_annual(expanded, 21.22)
    after = scaled["mc_total_per_kwh"]

    assert factor == pytest.approx(21.22 / float(before.sum()))
    assert float(after.sum()) == pytest.approx(21.22)
    assert after.to_list() == pytest.approx((before * factor).to_list())
    assert (after == 0).sum() == (before == 0).sum()


def test_scale_refuses_a_zero_8760() -> None:
    zeros = pl.DataFrame({"mc_total_per_kwh": [0.0, 0.0]})
    with pytest.raises(ValueError, match="sums to 0"):
        scale_8760_to_annual(zeros, 21.22)


def test_leap_year_drops_december_31() -> None:
    expanded = expand_month_hour_mc_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2024,
        "ct_eversource",
    )
    assert expanded.height == 8760
    assert expanded.filter(pl.col("timestamp").dt.month() == 2).height == 29 * 24
    assert (
        expanded.filter(
            (pl.col("timestamp").dt.month() == 12)
            & (pl.col("timestamp").dt.day() == 31)
        ).height
        == 0
    )
