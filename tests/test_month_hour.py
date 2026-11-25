"""Month-hour calendar expansion, and the CT config that calls it."""

from __future__ import annotations

import tempfile
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.data_prep.marginal_costs.methods.month_hour import (
    expand_month_hour_to_8760,
    main,
    month_hour,
    month_hour_costs_from_probability,
    scale_8760_to_annual,
    us_federal_holidays,
)
from utils.data_prep.marginal_costs.sub_tx_and_dist.generate_utility_tx_dx_mc import (
    _voltage_from_table_paths,
)

_REPO = Path(__file__).parent.parent
_CONFIGS = _REPO / "rate_design" / "hp_rates"
_ANNUAL = 69.0


def _allocation(state: str) -> dict:
    path = _CONFIGS / state / "config" / "marginal_costs" / "allocation.yaml"
    loaded = yaml.safe_load(path.read_text())
    assert isinstance(loaded, dict)
    return loaded


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


def _pop(at: tuple[int, int], value: float, days: int) -> pl.DataFrame:
    month_at, hour_at = at
    return pl.DataFrame(
        [
            {
                "month": month,
                "hour": hour,
                "num_days": days,
                "probability_of_peak": value if (month, hour) == (month_at, hour_at) else 0.0,
            }
            for month in range(1, 13)
            for hour in range(24)
        ]
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


def test_weekday_weekend_and_holiday_lookup() -> None:
    expanded = expand_month_hour_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2025,
    )
    assert expanded.columns == ["timestamp", "mc_total_per_kwh"]
    assert expanded.height == 8760
    assert expanded.schema["timestamp"] == pl.Datetime("us")

    def value_at(stamp: str) -> float:
        return expanded.filter(pl.col("timestamp") == datetime.fromisoformat(stamp))["mc_total_per_kwh"].item()

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
    expanded = expand_month_hour_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2026,
    )

    def value_at(stamp: str) -> float:
        return expanded.filter(pl.col("timestamp") == datetime.fromisoformat(stamp))["mc_total_per_kwh"].item()

    # July 4, 2026 is a Saturday, so Friday July 3 is the observed holiday.
    assert value_at("2026-07-03 16:00:00") == pytest.approx(-7.16)
    assert value_at("2026-07-06 16:00:00") == pytest.approx(7.16)


def test_juneteenth_counts_from_2021() -> None:
    assert date(2020, 6, 19) not in us_federal_holidays(2020)
    assert date(2021, 6, 18) in us_federal_holidays(2021)
    assert date(2026, 7, 3) in us_federal_holidays(2026)


def test_weekday_holidays_stay_on_weekday_table_when_holidays_excluded() -> None:
    expanded = expand_month_hour_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2025,
        include_holidays=False,
    )

    def value_at(stamp: str) -> float:
        return expanded.filter(pl.col("timestamp") == datetime.fromisoformat(stamp))["mc_total_per_kwh"].item()

    # Wednesday New Year's and Friday July 4 stay on the weekday table.
    assert value_at("2025-01-01 00:00:00") == pytest.approx(1.0)
    assert value_at("2025-07-04 16:00:00") == pytest.approx(7.16)
    # Saturday still uses the weekend table.
    assert value_at("2025-01-04 16:00:00") == pytest.approx(-1.16)


def test_calendar_year_sets_day_types_and_year_sets_timestamps() -> None:
    expanded = expand_month_hour_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2025,
        calendar_year=2018,
    )
    assert expanded.height == 8760
    assert expanded["timestamp"].dt.year().unique().to_list() == [2025]

    def value_at(stamp: str) -> float:
        return expanded.filter(pl.col("timestamp") == datetime.fromisoformat(stamp))["mc_total_per_kwh"].item()

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
        expand_month_hour_to_8760(
            _table(0.0, weekend=False),
            _table(0.0, weekend=True),
            2024,
            calendar_year=2018,
        )


def test_uniform_scale_matches_annual_total_and_keeps_shape() -> None:
    expanded = expand_month_hour_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2025,
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
    expanded = expand_month_hour_to_8760(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        2024,
    )
    assert expanded.height == 8760
    assert expanded.filter(pl.col("timestamp").dt.month() == 2).height == 29 * 24
    assert expanded.filter((pl.col("timestamp").dt.month() == 12) & (pl.col("timestamp").dt.day() == 31)).height == 0


def test_month_hour_scales_published_rates_to_the_annual() -> None:
    result = month_hour(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        21.22,
        2025,
        table_kind="rates",
        output_column="mc_total_per_kwh",
    )
    assert result.columns == ["timestamp", "mc_total_per_kwh"]
    assert result.height == 8760
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(21.22)


def test_output_column_name_is_the_callers() -> None:
    result = month_hour(
        _table(0.0, weekend=False),
        _table(0.0, weekend=True),
        21.22,
        2025,
        output_column="bulk_tx_cost_enduse",
    )
    assert result.columns == ["timestamp", "bulk_tx_cost_enduse"]
    assert float(result["bulk_tx_cost_enduse"].sum()) == pytest.approx(21.22)


def test_divides_annual_cost_by_the_day_count() -> None:
    costs = month_hour_costs_from_probability(_pop((7, 18), 0.5, 21), _ANNUAL, label="weekday")
    july = costs.filter((pl.col("month") == 7) & (pl.col("hour") == 18))
    assert july["mc_total_per_kwh"].item() == pytest.approx(_ANNUAL * 0.5 / 21)
    assert costs.filter(pl.col("mc_total_per_kwh") > 0).height == 1


def test_rejects_a_probability_outside_zero_to_one() -> None:
    with pytest.raises(ValueError, match="outside 0–1"):
        month_hour_costs_from_probability(_pop((7, 18), 1.5, 21), _ANNUAL, label="weekday")


def test_rejects_a_non_positive_day_count() -> None:
    with pytest.raises(ValueError, match="day count"):
        month_hour_costs_from_probability(_pop((7, 18), 1.0, 0), _ANNUAL, label="weekday")


def test_matching_day_count_recovers_the_annual_before_scaling() -> None:
    """When num_days equals how often that month-hour occurs, the 8760 sums to the annual."""
    weekend = month_hour_costs_from_probability(_pop((1, 0), 0.0, 1), _ANNUAL, label="weekend")
    probe = month_hour_costs_from_probability(_pop((7, 18), 1.0, 1), _ANNUAL, label="weekday")
    expanded = expand_month_hour_to_8760(probe, weekend, 2025, include_holidays=True)
    n_days = expanded.filter(pl.col("mc_total_per_kwh") > 0).height
    costs = month_hour_costs_from_probability(_pop((7, 18), 1.0, n_days), _ANNUAL, label="weekday")
    expanded = expand_month_hour_to_8760(costs, weekend, 2025, include_holidays=True)
    assert float(expanded["mc_total_per_kwh"].sum()) == pytest.approx(_ANNUAL)
    _, scale = scale_8760_to_annual(expanded, _ANNUAL)
    assert scale == pytest.approx(1.0)


def test_probability_table_scales_to_the_annual() -> None:
    result = month_hour(
        _pop((7, 18), 1.0, 21),
        _pop((1, 0), 0.0, 1),
        _ANNUAL,
        2025,
        table_kind="probability",
        output_column="bulk_tx_cost_enduse",
    )
    assert float(result["bulk_tx_cost_enduse"].sum()) == pytest.approx(_ANNUAL)
    assert result.filter(pl.col("bulk_tx_cost_enduse") > 0).height > 0


def test_rejects_an_unknown_table_kind_and_a_short_table() -> None:
    with pytest.raises(ValueError, match="table_kind"):
        month_hour(_table(0.0, weekend=False), _table(0.0, weekend=True), 1.0, 2025, table_kind="load")
    with pytest.raises(ValueError, match="288"):
        expand_month_hour_to_8760(
            _table(0.0, weekend=False).head(10),
            _table(0.0, weekend=True),
            2025,
        )


def test_cli_writes_a_scaled_8760() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path_weekday = Path(directory) / "weekday.parquet"
        path_weekend = Path(directory) / "weekend.parquet"
        path_output = Path(directory) / "out" / "allocated.parquet"
        _table(0.0, weekend=False).write_parquet(path_weekday)
        _table(0.0, weekend=True).write_parquet(path_weekend)
        main(
            [
                "--table-kind",
                "rates",
                "--path-weekday",
                str(path_weekday),
                "--path-weekend",
                str(path_weekend),
                "--annual-cost-kw-year",
                "21.22",
                "--year",
                "2025",
                "--path-output",
                str(path_output),
            ]
        )
        result = pl.read_parquet(path_output)
    assert result.columns == ["timestamp", "mc_total_per_kwh"]
    assert result.height == 8760
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(21.22)


def test_state_config_names_this_method_and_its_inputs() -> None:
    dist = _allocation("ct")["dist_and_sub_tx"]
    assert dist["method"] == "month_hour"
    assert dist["output_column"] == "mc_total_per_kwh"
    assert dist["manifest_method"] == "month_hour"
    assert dist["utilities"] == ["ct_eversource"]
    assert dist["output_base"] == "s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/"
    assert dist["inputs"]["table_kind"] == "rates"
    assert dist["inputs"]["include_holidays"] is True
    assert dist["inputs"]["year"] == "{year}"
    assert dist["inputs"]["calendar_year"] == "{year}"
    annual = dist["inputs"]["annual_cost_kw_year"]
    assert annual["from"] == "mc_table"
    assert annual["column"] == "sub_tx_and_dist_mc_kw_yr"
    assert annual["target_dollar_year"] == 2026
    assert annual["path"].endswith("ct_marginal_costs_{year}.csv")
    assert dist["inputs"]["weekday"]["from"] == "parquet"
    assert dist["inputs"]["weekday"]["path"].endswith("month_hour_marginal_costs_weekday_secondary.parquet")
    assert dist["inputs"]["weekend"]["path"].endswith("month_hour_marginal_costs_weekend_holidays_secondary.parquet")

    alternative = _allocation("ct")["bulk_tx"]["alternatives"]["month_hour"]
    assert alternative["method"] == "month_hour"
    assert alternative["manifest_method"] == "month_hour"
    assert alternative["utilities"] == ["ct_eversource"]
    assert alternative["inputs"]["table_kind"] == "probability"
    assert alternative["inputs"]["include_holidays"] is True
    assert alternative["inputs"]["year"] == "{year}"
    assert alternative["inputs"]["calendar_year"] == "{year}"
    bulk_annual = alternative["inputs"]["annual_cost_kw_year"]
    assert bulk_annual == {
        "from": "cpi",
        "value": 69.0,
        "dollar_year": 2024,
        "target_dollar_year": 2026,
        "cpi_s3_base": "s3://data.sb/fred/cpi/",
    }
    assert alternative["inputs"]["weekday"]["path"].endswith("month_hour_probability_of_peak_weekday.parquet")
    assert alternative["inputs"]["weekend"]["path"].endswith("month_hour_probability_of_peak_weekend_holidays.parquet")
