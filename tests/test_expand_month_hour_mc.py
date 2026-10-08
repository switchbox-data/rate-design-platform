"""Month-hour marginal costs expanded onto a Cairo 8760."""

from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

from utils.data_prep.marginal_costs.generate_utility_tx_dx_mc import (
    eversource_derived_output_base,
    expand_month_hour_mc_to_8760,
)


def test_eversource_derived_output_base() -> None:
    assert (
        eversource_derived_output_base(
            "s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/"
        )
        == "s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx_eversource_derived/"
    )
    assert (
        eversource_derived_output_base(
            "/ebs/data/switchbox/marginal_costs/ct/dist_and_sub_tx"
        )
        == "/ebs/data/switchbox/marginal_costs/ct/dist_and_sub_tx_eversource_derived/"
    )
    already = (
        "s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx_eversource_derived/"
    )
    assert eversource_derived_output_base(already) == already
    with pytest.raises(ValueError, match="dist_and_sub_tx/"):
        eversource_derived_output_base(
            "s3://data.sb/switchbox/marginal_costs/ct/bulk_tx/"
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
