"""Unit tests for PJM RPM supply capacity marginal cost logic.

Tests cover:
- RPM price selection for a (delivery_year, zone), incl. constrained-LDA zones
- Calendar-year day-count blend of two overlapping delivery years
- 5CP hour selection: hour-ending -> hour-beginning conversion, count, season
- Equal 1/5 allocation and 1 kW recovery validation
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from utils.data_prep.marginal_costs.supply_capacity_pjm import (
    _get_rpm_price_per_mw_day,
    resolve_rpm_price_for_calendar_year,
    select_5cp_hours,
    validate_pjm_allocation,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_rpm_df(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal RPM DataFrame matching the curated parquet schema."""
    schema = {
        "delivery_year": pl.String,
        "dy_start": pl.Date,
        "zone": pl.String,
        "final_zonal_capacity_price_per_mw_day": pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema)


def _make_5cp_df(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal 5CP DataFrame matching the curated parquet schema."""
    schema = {
        "summer_year": pl.Int64,
        "rank": pl.Int64,
        "peak_date": pl.Date,
        "hour_ending_ept": pl.Int64,
        "zone": pl.String,
        "mw_unrestricted": pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema)


def _rpm_row(dy_start_year: int, zone: str, price: float) -> dict:
    return {
        "delivery_year": f"{dy_start_year}/{str(dy_start_year + 1)[-2:]}",
        "dy_start": date(dy_start_year, 6, 1),
        "zone": zone,
        "final_zonal_capacity_price_per_mw_day": price,
    }


def _rto_5cp_rows(summer_year: int) -> list[dict]:
    """Five RTO peak rows for a summer (Jun–Sep), HE18 except rank 4 (HE15)."""
    specs = [
        (1, date(summer_year, 6, 23), 18, 160000.0),
        (2, date(summer_year, 6, 24), 18, 159000.0),
        (3, date(summer_year, 7, 29), 18, 156000.0),
        (4, date(summer_year, 6, 25), 15, 152000.0),
        (5, date(summer_year, 7, 28), 18, 151000.0),
    ]
    return [
        {
            "summer_year": summer_year,
            "rank": rank,
            "peak_date": d,
            "hour_ending_ept": he,
            "zone": "RTO",
            "mw_unrestricted": mw,
        }
        for rank, d, he, mw in specs
    ]


# ---------------------------------------------------------------------------
# _get_rpm_price_per_mw_day
# ---------------------------------------------------------------------------


def test_get_rpm_price_returns_matching_zone_and_year() -> None:
    rpm_df = _make_rpm_df([_rpm_row(2024, "BGE", 76.755373)])
    price, dy = _get_rpm_price_per_mw_day(rpm_df, 2024, "BGE")
    assert price == pytest.approx(76.755373)
    assert dy == "2024/25"


def test_get_rpm_price_selects_constrained_lda_not_rto() -> None:
    """BGE is a constrained LDA: selecting 'BGE' must not return the RTO price."""
    rpm_df = _make_rpm_df(
        [
            _rpm_row(2025, "RTO", 270.432933),
            _rpm_row(2025, "BGE", 471.328782),  # constrained, higher
        ]
    )
    price, _ = _get_rpm_price_per_mw_day(rpm_df, 2025, "BGE")
    assert price == pytest.approx(471.328782)


def test_get_rpm_price_raises_when_absent() -> None:
    rpm_df = _make_rpm_df([_rpm_row(2024, "BGE", 76.0)])
    with pytest.raises(ValueError, match="No RPM price found"):
        _get_rpm_price_per_mw_day(rpm_df, 2099, "BGE")


# ---------------------------------------------------------------------------
# resolve_rpm_price_for_calendar_year
# ---------------------------------------------------------------------------


def test_resolve_rpm_day_count_blend_non_leap() -> None:
    """CY2025 (non-leap): 151 days DY1 + 214 days DY2, MW->kW /1000."""
    rpm_df = _make_rpm_df(
        [
            _rpm_row(2024, "BGE", 100.0),  # DY1 covers Jan 1–May 31 (151 d)
            _rpm_row(2025, "BGE", 200.0),  # DY2 covers Jun 1–Dec 31 (214 d)
        ]
    )
    result = resolve_rpm_price_for_calendar_year(rpm_df, "BGE", 2025)
    expected = (100.0 * 151 + 200.0 * 214) / 1000.0  # = 57.9
    assert result == pytest.approx(expected, abs=1e-9)


def test_resolve_rpm_day_count_blend_leap_year() -> None:
    """CY2024 (leap): Jan–May has 152 days; equal prices -> P*366/1000."""
    rpm_df = _make_rpm_df(
        [
            _rpm_row(2023, "BGE", 100.0),
            _rpm_row(2024, "BGE", 100.0),
        ]
    )
    result = resolve_rpm_price_for_calendar_year(rpm_df, "BGE", 2024)
    assert result == pytest.approx(100.0 * 366 / 1000.0, abs=1e-9)


def test_resolve_rpm_constant_price_equals_annualized() -> None:
    """Equal DY prices in a non-leap year recover P*365/1000."""
    rpm_df = _make_rpm_df(
        [
            _rpm_row(2024, "BGE", 150.0),
            _rpm_row(2025, "BGE", 150.0),
        ]
    )
    result = resolve_rpm_price_for_calendar_year(rpm_df, "BGE", 2025)
    assert result == pytest.approx(150.0 * 365 / 1000.0, abs=1e-9)


# ---------------------------------------------------------------------------
# select_5cp_hours
# ---------------------------------------------------------------------------


def test_select_5cp_converts_hour_ending_to_hour_beginning() -> None:
    """HE18 -> hour-beginning 17:00; HE15 -> 14:00."""
    fivecp_df = _make_5cp_df(_rto_5cp_rows(2025))
    peaks = select_5cp_hours(fivecp_df, 2025)
    ts = set(peaks["timestamp"].to_list())
    assert datetime(2025, 6, 23, 17) in ts  # HE18
    assert datetime(2025, 6, 25, 14) in ts  # HE15
    assert peaks.height == 5


def test_select_5cp_sorted_and_filters_to_rto() -> None:
    rows = _rto_5cp_rows(2025) + [
        {
            "summer_year": 2025,
            "rank": 1,
            "peak_date": date(2025, 6, 23),
            "hour_ending_ept": 18,
            "zone": "BGE",  # must be ignored
            "mw_unrestricted": 6587.7,
        }
    ]
    peaks = select_5cp_hours(_make_5cp_df(rows), 2025)
    assert peaks.height == 5
    ts = peaks["timestamp"].to_list()
    assert ts == sorted(ts)


def test_select_5cp_raises_when_not_five_hours() -> None:
    rows = _rto_5cp_rows(2025)[:4]
    with pytest.raises(ValueError, match="Expected 5 RTO 5CP hours"):
        select_5cp_hours(_make_5cp_df(rows), 2025)


def test_select_5cp_raises_outside_summer() -> None:
    rows = _rto_5cp_rows(2025)
    rows[0]["peak_date"] = date(2025, 1, 15)  # winter — invalid for 5CP
    with pytest.raises(ValueError, match="outside Jun–Sep"):
        select_5cp_hours(_make_5cp_df(rows), 2025)


# ---------------------------------------------------------------------------
# Equal allocation + validation
# ---------------------------------------------------------------------------


def test_equal_weight_allocation_sums_to_annual() -> None:
    fivecp_df = _make_5cp_df(_rto_5cp_rows(2025))
    peaks = select_5cp_hours(fivecp_df, 2025)
    annual = 57.9
    per_hour = annual / 5
    peak_df = peaks.select("timestamp", pl.lit(per_hour).alias("capacity_cost_per_kw"))
    assert peak_df.height == 5
    assert float(peak_df["capacity_cost_per_kw"].sum()) == pytest.approx(annual)
    # Each of the 5 hours carries an identical share.
    assert peak_df["capacity_cost_per_kw"].n_unique() == 1
    validate_pjm_allocation(peak_df, annual)  # should not raise


def test_validate_pjm_allocation_fails_for_wrong_sum() -> None:
    bad_df = pl.DataFrame(
        {
            "timestamp": [datetime(2025, 6, 23, 17)],
            "capacity_cost_per_kw": [1.0],
        }
    )
    with pytest.raises(ValueError, match="PJM validation failed"):
        validate_pjm_allocation(bad_df, capacity_cost_kw_year=100.0)
