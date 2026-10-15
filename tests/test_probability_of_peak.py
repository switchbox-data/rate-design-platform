"""Probability of peak from hourly history, and its $/kW-yr allocation."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import polars as pl
import pytest
from scipy.stats import norm, truncnorm

from utils.data_prep.marginal_costs.probability_of_peak import (
    WEEKDAY,
    WEEKEND_HOLIDAY,
    allocate_annual_cost_by_pop,
    count_day_types,
    label_hour_types,
    month_hour_costs_from_pop,
    probability_of_peak,
    scott_bandwidth,
    truncated_kde_tail_share,
    us_federal_holidays,
)


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------


def test_federal_holidays_observe_weekend_dates_and_juneteenth_from_2021() -> None:
    h2022 = us_federal_holidays(2022)
    # Juneteenth 2022 is a Sunday, observed Monday June 20.
    assert date(2022, 6, 20) in h2022 and date(2022, 6, 19) not in h2022
    # Christmas 2022 is a Sunday, observed Monday December 26.
    assert date(2022, 12, 26) in h2022
    # January 1, 2022 is a Saturday, observed Friday December 31, 2021: that
    # date belongs to 2021, so 2022 has ten observed holidays and 2021 twelve.
    assert date(2022, 1, 1) not in h2022
    assert len(h2022) == 10
    assert date(2021, 12, 31) in us_federal_holidays(2021)
    assert len(us_federal_holidays(2021)) == 12
    assert len(us_federal_holidays(2025)) == 11

    # July 4, 2026 is a Saturday, observed Friday July 3.
    assert date(2026, 7, 3) in us_federal_holidays(2026)
    # Juneteenth was not a federal holiday before 2021.
    assert date(2020, 6, 19) not in us_federal_holidays(2020)
    assert len(us_federal_holidays(2020)) == 10


def test_label_hour_types_marks_weekends_and_holidays() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [
                datetime(2025, 1, 8, 17),  # Wednesday
                datetime(2025, 1, 4, 17),  # Saturday
                datetime(2025, 7, 4, 17),  # Friday, Independence Day
                datetime(2025, 7, 3, 17),  # Thursday before it
            ]
        }
    )
    labelled = label_hour_types(frame)
    assert labelled["daytype"].to_list() == [
        WEEKDAY,
        WEEKEND_HOLIDAY,
        WEEKEND_HOLIDAY,
        WEEKDAY,
    ]
    assert labelled["month"].to_list() == [1, 1, 7, 7]
    assert labelled["hour"].to_list() == [17, 17, 17, 17]
    assert labelled["year"].unique().to_list() == [2025]


def test_day_type_counts_remove_observed_holidays() -> None:
    # June 2027 has 22 Mon-Fri minus Juneteenth observed Friday June 18 = 21.
    days = count_day_types(2027)
    june = days.filter(pl.col("month") == 6)
    assert june.filter(pl.col("daytype") == WEEKDAY)["num_days"].item() == 21
    assert june.filter(pl.col("daytype") == WEEKEND_HOLIDAY)["num_days"].item() == 9
    assert days["num_days"].sum() == 365


def test_day_type_counts_follow_the_cairo_8760_in_a_leap_year() -> None:
    days = count_day_types(2024)
    # February keeps the 29th; December 31 is dropped by the 8760.
    assert days.filter(pl.col("month") == 2)["num_days"].sum() == 29
    assert days.filter(pl.col("month") == 12)["num_days"].sum() == 30
    assert days["num_days"].sum() == 365


# ---------------------------------------------------------------------------
# Truncated KDE tail
# ---------------------------------------------------------------------------


def test_scott_bandwidth_matches_scipy_default() -> None:
    from scipy.stats import gaussian_kde

    rng = np.random.default_rng(0)
    x = rng.normal(size=200)
    kde = gaussian_kde(x)
    assert scott_bandwidth(x) == pytest.approx(float(np.sqrt(kde.covariance[0, 0])))
    assert scott_bandwidth(np.array([1.0])) == 0.0


def test_tail_share_matches_truncnorm_and_untruncated_normal() -> None:
    rng = np.random.default_rng(1)
    x = rng.normal(1.0, 0.2, size=60)
    bw = scott_bandwidth(x)
    thr = 1.25

    expected_truncated = float(truncnorm.sf(thr, -3, 3, loc=x, scale=bw).mean())
    assert truncated_kde_tail_share(x, thr) == pytest.approx(expected_truncated)

    expected_plain = float(norm.sf(thr, loc=x, scale=bw).mean())
    assert truncated_kde_tail_share(
        x, thr, truncation_sigmas=float("inf")
    ) == pytest.approx(expected_plain)

    # Tighter truncation gives a thinner tail at a threshold above the data.
    assert truncated_kde_tail_share(x, 1.6, truncation_sigmas=2.0) < (
        truncated_kde_tail_share(x, 1.6, truncation_sigmas=3.0)
    )


def test_tail_share_limits_and_degenerate_inputs() -> None:
    x = np.array([1.0, 1.1, 1.2])
    assert truncated_kde_tail_share(x, -10.0) == pytest.approx(1.0)
    assert truncated_kde_tail_share(x, 10.0) == pytest.approx(0.0)
    # Beyond three bandwidths of every observation the truncated tail is 0.
    bw = scott_bandwidth(x)
    assert truncated_kde_tail_share(x, 1.2 + 3.01 * bw) == 0.0
    # A single kernel is symmetric about its centre.
    assert truncated_kde_tail_share(np.array([2.0]), 2.0, bandwidth=0.3) == (
        pytest.approx(0.5)
    )
    # Zero bandwidth (constant data, or passed explicitly) falls back to the
    # empirical share above the threshold.
    assert truncated_kde_tail_share(np.array([1.0, 1.0, 1.0]), 0.5) == 1.0
    assert truncated_kde_tail_share(
        np.array([1.0, 1.0, 2.0]), 1.5, bandwidth=0.0
    ) == pytest.approx(1 / 3)
    with pytest.raises(ValueError, match="zero observations"):
        truncated_kde_tail_share(np.array([]), 1.0)
    with pytest.raises(ValueError, match="truncation_sigmas"):
        truncated_kde_tail_share(x, 1.0, truncation_sigmas=0.0)


# ---------------------------------------------------------------------------
# Probability of peak on a synthetic history
# ---------------------------------------------------------------------------


def _synthetic_history(years: tuple[int, ...], *, seed: int = 0) -> pl.DataFrame:
    """Hourly load with a July weekday late-afternoon peak and a flat winter."""
    rng = np.random.default_rng(seed)
    stamps: list[datetime] = []
    for year in years:
        cur = datetime(year, 1, 1)
        while cur.year == year:
            stamps.append(cur)
            cur += timedelta(hours=1)
    frame = label_hour_types(pl.DataFrame({"timestamp": stamps}))
    month = frame["month"].to_numpy().astype(int)
    hour = frame["hour"].to_numpy().astype(float)
    weekday = (frame["daytype"] == WEEKDAY).to_numpy()
    summer = np.isin(month, (6, 7, 8)).astype(float) * (1.0 + 0.5 * (month == 7))
    afternoon = np.exp(-((hour - 17) ** 2) / 8.0)
    base = 1000.0 + 600.0 * summer * afternoon * (1.0 + 0.3 * weekday)
    load = base * rng.lognormal(0.0, 0.08, size=base.size)
    return frame.select("timestamp").with_columns(pl.Series("load_mw", load))


def test_probability_of_peak_shape_and_normalization() -> None:
    history = _synthetic_history((2022, 2023))
    result = probability_of_peak(history)

    cells = result.cells
    assert cells.height == 576
    assert cells.select("month", "daytype", "hour").n_unique() == 576
    assert set(cells["daytype"].unique().to_list()) == {WEEKDAY, WEEKEND_HOLIDAY}
    assert float(cells["probability_of_peak"].sum()) == pytest.approx(1.0)
    assert cells.select(pl.col("probability_of_peak").min()).item() >= 0.0
    assert cells["n_obs"].sum() == history.height

    # Peak probability concentrates on July weekday afternoons; winter is zero.
    top = cells.sort("probability_of_peak", descending=True).head(1)
    assert top["month"].item() == 7
    assert top["daytype"].item() == WEEKDAY
    assert 15 <= top["hour"].item() <= 19
    winter = cells.filter(pl.col("month").is_in([11, 12, 1, 2, 3]))
    assert float(winter["probability_of_peak"].sum()) == 0.0

    # The delimiter is the mean over years of each year's top cell mean.
    assert result.years["year"].to_list() == [2022, 2023]
    assert result.delimiter == pytest.approx(
        result.years.select(pl.col("max_cell_mean").mean()).item()
    )
    assert result.years["delimiter_load"].to_list() == pytest.approx(
        (result.delimiter * result.years["mean_load"]).to_list()
    )
    assert 0.5 < result.delimiter_share_of_peak < 1.0


def test_probability_of_peak_is_invariant_to_load_units() -> None:
    history = _synthetic_history((2022,))
    base = probability_of_peak(history)
    scaled = probability_of_peak(
        history.with_columns(pl.col("load_mw") * 1000.0).rename({"load_mw": "load_kw"}),
        load_col="load_kw",
    )
    assert scaled.delimiter == pytest.approx(base.delimiter)
    assert scaled.cells["probability_of_peak"].to_list() == pytest.approx(
        base.cells["probability_of_peak"].to_list()
    )


def test_probability_of_peak_sensitivity_knobs_and_input_checks() -> None:
    history = _synthetic_history((2022,))
    base = probability_of_peak(history)
    untruncated = probability_of_peak(history, truncation_sigmas=float("inf"))
    assert untruncated.delimiter == base.delimiter
    assert untruncated.cells["p_exceed"].sum() >= base.cells["p_exceed"].sum()
    wider = probability_of_peak(history, bandwidth_multiplier=2.0)
    assert wider.cells["bandwidth"].to_list() == pytest.approx(
        (base.cells["bandwidth"] * 2.0).to_list()
    )

    with pytest.raises(ValueError, match="complete"):
        probability_of_peak(history.head(5000))
    with pytest.raises(ValueError, match="missing columns"):
        probability_of_peak(history.rename({"load_mw": "mw"}))
    with pytest.raises(ValueError, match="nulls"):
        probability_of_peak(
            history.with_columns(
                pl.when(pl.arange(0, history.height) == 0)
                .then(None)
                .otherwise(pl.col("load_mw"))
                .alias("load_mw")
            )
        )
    with pytest.raises(TypeError, match="Datetime"):
        probability_of_peak(history.with_columns(pl.col("timestamp").cast(pl.String)))
    with pytest.raises(ValueError, match="bandwidth_multiplier"):
        probability_of_peak(history, bandwidth_multiplier=0.0)


# ---------------------------------------------------------------------------
# Allocation of an annual $/kW-yr
# ---------------------------------------------------------------------------


def _two_cell_pop() -> pl.DataFrame:
    """All probability on July weekday 17h (0.75) and July weekend 16h (0.25)."""
    rows = [
        {"month": m, "daytype": d, "hour": h, "probability_of_peak": 0.0}
        for m in range(1, 13)
        for d in (WEEKDAY, WEEKEND_HOLIDAY)
        for h in range(24)
    ]
    frame = pl.DataFrame(rows)
    return frame.with_columns(
        pl.when(
            (pl.col("month") == 7)
            & (pl.col("daytype") == WEEKDAY)
            & (pl.col("hour") == 17)
        )
        .then(0.75)
        .when(
            (pl.col("month") == 7)
            & (pl.col("daytype") == WEEKEND_HOLIDAY)
            & (pl.col("hour") == 16)
        )
        .then(0.25)
        .otherwise(0.0)
        .alias("probability_of_peak")
    )


def test_month_hour_costs_divide_by_the_calendar_day_count() -> None:
    costs = month_hour_costs_from_pop(_two_cell_pop(), 69.0, 2026)
    assert costs.height == 576
    # July 2026: 23 weekdays less July 3 (observed Independence Day) = 22,
    # and 8 weekend days plus the observed holiday = 9.
    weekday = costs.filter(
        (pl.col("month") == 7) & (pl.col("daytype") == WEEKDAY) & (pl.col("hour") == 17)
    )
    weekend = costs.filter(
        (pl.col("month") == 7)
        & (pl.col("daytype") == WEEKEND_HOLIDAY)
        & (pl.col("hour") == 16)
    )
    assert weekday["num_days"].item() == 22
    assert weekend["num_days"].item() == 9
    assert weekday["mc_total_per_kwh"].item() == pytest.approx(69.0 * 0.75 / 22)
    assert weekend["mc_total_per_kwh"].item() == pytest.approx(69.0 * 0.25 / 9)
    assert costs.filter(pl.col("month") != 7)["mc_total_per_kwh"].sum() == 0.0


def test_allocation_recovers_the_annual_cost_on_the_8760() -> None:
    hourly = allocate_annual_cost_by_pop(_two_cell_pop(), 69.0, 2026)
    assert hourly.columns == ["timestamp", "mc_total_per_kwh"]
    assert hourly.height == 8760
    assert hourly.schema["timestamp"] == pl.Datetime("us")
    assert float(hourly["mc_total_per_kwh"].sum()) == pytest.approx(69.0)
    nonzero = hourly.filter(pl.col("mc_total_per_kwh") > 0)
    assert nonzero.height == 22 + 9

    def value_at(stamp: str) -> float:
        return hourly.filter(pl.col("timestamp") == datetime.fromisoformat(stamp))[
            "mc_total_per_kwh"
        ].item()

    assert value_at("2026-07-01 17:00:00") == pytest.approx(69.0 * 0.75 / 22)
    # Friday July 3, 2026 is the observed holiday: weekend table, 16h.
    assert value_at("2026-07-03 16:00:00") == pytest.approx(69.0 * 0.25 / 9)
    assert value_at("2026-07-03 17:00:00") == 0.0
    assert value_at("2026-07-04 16:00:00") == pytest.approx(69.0 * 0.25 / 9)


def test_allocation_uses_calendar_year_day_types_and_stamps_year() -> None:
    hourly = allocate_annual_cost_by_pop(
        _two_cell_pop(), 10.0, 2026, calendar_year=2025, value_col="bulk_tx"
    )
    assert hourly.columns == ["timestamp", "bulk_tx"]
    assert hourly["timestamp"].dt.year().unique().to_list() == [2026]
    assert float(hourly["bulk_tx"].sum()) == pytest.approx(10.0)
    # 2025-07-05 is a Saturday; 2026-07-05 is a Sunday. Both weekend.
    # 2025-07-07 is a Monday; 2026-07-07 is a Tuesday. Both weekday.
    # 2025-07-04 is Friday and the holiday; 2026-07-04 is Saturday, so the
    # 2025 calendar leaves July 3 as a weekday (it is 2026's observed holiday).
    july3 = hourly.filter(pl.col("timestamp") == datetime(2026, 7, 3, 17))
    assert july3["bulk_tx"].item() > 0.0


def test_allocation_rejects_malformed_pop_tables() -> None:
    cells = _two_cell_pop()
    with pytest.raises(ValueError, match="sums to"):
        allocate_annual_cost_by_pop(
            cells.with_columns(pl.col("probability_of_peak") * 2), 1.0, 2026
        )
    with pytest.raises(ValueError, match="576"):
        allocate_annual_cost_by_pop(cells.head(100), 1.0, 2026)
    with pytest.raises(ValueError, match="missing columns"):
        allocate_annual_cost_by_pop(cells.drop("hour"), 1.0, 2026)
    with pytest.raises(ValueError, match="daytype"):
        allocate_annual_cost_by_pop(
            cells.with_columns(pl.lit("saturday").alias("daytype")), 1.0, 2026
        )
