"""Bulk-tx month-hour $/kWh from probability of peak. No S3."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.data_prep.marginal_costs.generate_utility_tx_dx_mc import (
    expand_month_hour_mc_to_8760,
    inflate_annual_mc,
    load_cpi_inflation_factor,
    month_hour_costs_from_probability,
    scale_8760_to_annual,
)

_ANNUAL = 69.0


def _pop(at: tuple[int, int], value: float, days: int) -> pl.DataFrame:
    month_at, hour_at = at
    return pl.DataFrame(
        [
            {
                "month": month,
                "hour": hour,
                "num_days": days,
                "probability_of_peak": value
                if (month, hour) == (month_at, hour_at)
                else 0.0,
            }
            for month in range(1, 13)
            for hour in range(24)
        ]
    )


def test_divides_annual_cost_by_the_day_count() -> None:
    costs = month_hour_costs_from_probability(
        _pop((7, 18), 0.5, 21), _ANNUAL, label="weekday"
    )
    july = costs.filter((pl.col("month") == 7) & (pl.col("hour") == 18))
    assert july["mc_total_per_kwh"].item() == pytest.approx(_ANNUAL * 0.5 / 21)
    assert costs.filter(pl.col("mc_total_per_kwh") > 0).height == 1


def test_rejects_a_probability_outside_zero_to_one() -> None:
    with pytest.raises(ValueError, match="outside 0–1"):
        month_hour_costs_from_probability(
            _pop((7, 18), 1.5, 21), _ANNUAL, label="weekday"
        )


def test_rejects_a_non_positive_day_count() -> None:
    with pytest.raises(ValueError, match="day count"):
        month_hour_costs_from_probability(
            _pop((7, 18), 1.0, 0), _ANNUAL, label="weekday"
        )


def test_matching_day_count_recovers_the_annual_before_scaling() -> None:
    """When num_days equals how often that month-hour occurs, the 8760 sums to the annual."""
    weekend = month_hour_costs_from_probability(
        _pop((1, 0), 0.0, 1), _ANNUAL, label="weekend"
    )
    probe = month_hour_costs_from_probability(
        _pop((7, 18), 1.0, 1), _ANNUAL, label="weekday"
    )
    expanded = expand_month_hour_mc_to_8760(
        probe, weekend, 2025, "ct_eversource", include_holidays=True
    )
    n_days = expanded.filter(pl.col("mc_total_per_kwh") > 0).height
    costs = month_hour_costs_from_probability(
        _pop((7, 18), 1.0, n_days), _ANNUAL, label="weekday"
    )
    expanded = expand_month_hour_mc_to_8760(
        costs, weekend, 2025, "ct_eversource", include_holidays=True
    )
    assert float(expanded["mc_total_per_kwh"].sum()) == pytest.approx(_ANNUAL)
    _, scale = scale_8760_to_annual(expanded, _ANNUAL)
    assert scale == pytest.approx(1.0)


def test_cpi_factor_uses_the_latest_snapshot(tmp_path: Path) -> None:
    older = tmp_path / "cpiaucsl_2019_2026_20260803.parquet"
    newer = tmp_path / "cpiaucsl_2019_2026_20261009.parquet"
    pl.DataFrame({"year": [2024, 2026], "value": [100.0, 110.0]}).write_parquet(older)
    pl.DataFrame({"year": [2024, 2026], "value": [100.0, 120.0]}).write_parquet(newer)
    assert load_cpi_inflation_factor(str(tmp_path), 2024, 2026, {}) == pytest.approx(
        1.2
    )


def test_cpi_changes_the_annual_the_8760_must_sum_to(tmp_path: Path) -> None:
    pl.DataFrame({"year": [2024, 2026], "value": [100.0, 105.0]}).write_parquet(
        tmp_path / "cpiaucsl_2019_2026_20261009.parquet"
    )
    resolved = inflate_annual_mc(_ANNUAL, 2024, 2026, str(tmp_path), {})
    assert resolved.allocated == pytest.approx(_ANNUAL * 1.05)
    same_year = inflate_annual_mc(_ANNUAL, 2026, 2026, "s3://unused/", {})
    assert same_year.allocated == pytest.approx(_ANNUAL)
    assert same_year.cpi_factor == 1.0

    costs = month_hour_costs_from_probability(
        _pop((7, 18), 1.0, 21), resolved.allocated, label="weekday"
    )
    weekend = month_hour_costs_from_probability(
        _pop((1, 0), 0.0, 1), resolved.allocated, label="weekend"
    )
    expanded = expand_month_hour_mc_to_8760(
        costs, weekend, 2025, "ct_eversource", include_holidays=True
    )
    scaled, _ = scale_8760_to_annual(expanded, resolved.allocated)
    assert float(scaled["mc_total_per_kwh"].sum()) == pytest.approx(resolved.allocated)
