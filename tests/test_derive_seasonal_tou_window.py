"""Tests for TOU window sweep metric and selection logic."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from utils.pre.compute_tou import find_tou_peak_window, make_winter_summer_seasons
from utils.pre.derive_seasonal_tou_window import (
    compute_tou_fit_metric,
    sweep_tou_window_hours,
    update_periods_yaml,
)


def _make_hourly_index(year: int = 2025) -> pd.DatetimeIndex:
    """8760-hour DatetimeIndex for a non-leap year."""
    return pd.date_range(f"{year}-01-01", periods=8760, freq="h", tz="UTC")


def _flat_mc_and_load(
    index: pd.DatetimeIndex,
    mc_value: float = 0.10,
    load_value: float = 100.0,
) -> tuple[pd.Series, pd.Series]:
    """Constant MC and load -- any window split should yield zero residual."""
    mc = pd.Series(mc_value, index=index, name="total_mc_per_kwh")
    load = pd.Series(load_value, index=index, name="load")
    return mc, load


def _spike_mc_profile(
    index: pd.DatetimeIndex,
    spike_hours: list[int],
    spike_mc: float = 0.50,
    base_mc: float = 0.05,
    load_value: float = 100.0,
) -> tuple[pd.Series, pd.Series]:
    """MC profile with a sharp spike at specific hours-of-day."""
    hours = np.asarray(index.hour)  # type: ignore[union-attr]
    mc_values = np.where(np.isin(hours, spike_hours), spike_mc, base_mc)
    mc = pd.Series(mc_values, index=index, name="total_mc_per_kwh")
    load = pd.Series(load_value, index=index, name="load")
    return mc, load


# ---------------------------------------------------------------------------
# compute_tou_fit_metric
# ---------------------------------------------------------------------------


class TestComputeTouFitMetric:
    def test_flat_mc_yields_zero_metric(self) -> None:
        """When MC is flat, any window split has zero within-period variance."""
        idx = _make_hourly_index()
        mc, load = _flat_mc_and_load(idx)
        metric = compute_tou_fit_metric(mc, load, peak_hours=[16, 17, 18, 19])
        assert metric == pytest.approx(0.0, abs=1e-12)

    def test_spike_at_peak_has_lower_metric(self) -> None:
        """A window that captures the spike should have a lower metric than one
        that misses it."""
        idx = _make_hourly_index()
        mc, load = _spike_mc_profile(idx, spike_hours=[17, 18, 19, 20])

        metric_correct = compute_tou_fit_metric(mc, load, [17, 18, 19, 20])
        metric_wrong = compute_tou_fit_metric(mc, load, [5, 6, 7, 8])

        assert metric_correct < metric_wrong

    def test_metric_is_nonnegative(self) -> None:
        idx = _make_hourly_index()
        mc, load = _spike_mc_profile(idx, spike_hours=[10, 11])
        metric = compute_tou_fit_metric(mc, load, peak_hours=[10, 11, 12, 13])
        assert metric >= 0.0

    def test_perfect_split_yields_zero(self) -> None:
        """When MC takes exactly two values that perfectly align with the
        peak/off-peak split, the metric should be zero."""
        idx = _make_hourly_index()
        mc, load = _spike_mc_profile(
            idx, spike_hours=list(range(12, 24)), spike_mc=0.20, base_mc=0.05
        )
        metric = compute_tou_fit_metric(mc, load, peak_hours=list(range(12, 24)))
        assert metric == pytest.approx(0.0, abs=1e-12)


# ---------------------------------------------------------------------------
# sweep_tou_window_hours
# ---------------------------------------------------------------------------


class TestSweepTouWindowHours:
    def test_find_tou_peak_window_minimizes_fit_metric(self) -> None:
        """Peak-window selection should minimize the same metric the sweep reports."""
        rng = np.random.default_rng(0)
        idx = pd.date_range("2025-01-01", periods=24 * 30, freq="h", tz="UTC")
        base_mc = rng.uniform(0.01, 0.3, 24)
        base_load = rng.uniform(1.0, 10.0, 24)

        mc = pd.Series(
            [base_mc[ts.hour] + rng.normal(0, 0.02) for ts in idx],
            index=idx,
            name="total_mc_per_kwh",
        )
        load = pd.Series(
            [max(0.1, base_load[ts.hour] + rng.normal(0, 1.5)) for ts in idx],
            index=idx,
            name="load",
        )

        peak_hours = find_tou_peak_window(mc, load, window_hours=1)

        candidate_metrics = {
            start: compute_tou_fit_metric(mc, load, [start]) for start in range(24)
        }
        best_metric = min(candidate_metrics.values())

        assert compute_tou_fit_metric(mc, load, peak_hours) == pytest.approx(
            best_metric
        )

    def test_picks_correct_width_for_sharp_spike(self) -> None:
        """A 3-hour MC spike should yield N=3 as the best window width."""
        idx = _make_hourly_index()
        mc, load = _spike_mc_profile(idx, spike_hours=[17, 18, 19])
        seasons = make_winter_summer_seasons()

        results = sweep_tou_window_hours(mc, load, seasons, window_range=range(1, 24))

        assert len(results) == 23
        assert results[0].window_hours == 3

    def test_results_sorted_by_metric(self) -> None:
        idx = _make_hourly_index()
        mc, load = _spike_mc_profile(idx, spike_hours=[15, 16, 17, 18])
        seasons = make_winter_summer_seasons()

        results = sweep_tou_window_hours(mc, load, seasons)

        metrics = [r.metric_total for r in results]
        assert metrics == sorted(metrics)

    def test_each_result_has_all_seasons(self) -> None:
        idx = _make_hourly_index()
        mc, load = _spike_mc_profile(idx, spike_hours=[10, 11])
        seasons = make_winter_summer_seasons()

        results = sweep_tou_window_hours(mc, load, seasons, window_range=range(1, 5))

        for r in results:
            assert "winter" in r.peak_hours_by_season
            assert "summer" in r.peak_hours_by_season
            assert "winter" in r.ratio_by_season
            assert "summer" in r.ratio_by_season

    def test_window_range_respected(self) -> None:
        """Window range should be respected, but only valid candidates (ratio > 1.0) are returned."""
        idx = _make_hourly_index()
        # Use spike profile to ensure ratios > 1.0 (flat MC would be filtered out)
        mc, load = _spike_mc_profile(idx, spike_hours=[15, 16, 17, 18])
        seasons = make_winter_summer_seasons()

        results = sweep_tou_window_hours(mc, load, seasons, window_range=range(3, 7))

        window_hours = sorted(r.window_hours for r in results)
        # All window widths in range should be valid (have ratio > 1.0)
        assert window_hours == [3, 4, 5, 6]

    def test_n1_and_n23_work(self) -> None:
        """Edge cases: single-hour and 23-hour windows."""
        idx = _make_hourly_index()
        mc, load = _spike_mc_profile(idx, spike_hours=[17])
        seasons = make_winter_summer_seasons()

        results = sweep_tou_window_hours(mc, load, seasons, window_range=range(1, 24))

        window_hours_set = {r.window_hours for r in results}
        assert 1 in window_hours_set
        assert 23 in window_hours_set

    def test_peak_hours_contiguous(self) -> None:
        """All returned peak windows should be contiguous (mod 24)."""
        idx = _make_hourly_index()
        mc, load = _spike_mc_profile(idx, spike_hours=[23, 0, 1])
        seasons = make_winter_summer_seasons()

        results = sweep_tou_window_hours(mc, load, seasons, window_range=range(1, 6))

        for r in results:
            for peak_hours in r.peak_hours_by_season.values():
                n = len(peak_hours)
                if n <= 1:
                    continue
                hours_sorted = sorted(peak_hours)
                gaps = [
                    (hours_sorted[(i + 1) % n] - hours_sorted[i]) % 24 for i in range(n)
                ]
                is_contiguous = all(g == 1 or g == 24 - n + 1 for g in gaps)
                assert is_contiguous, (
                    f"Non-contiguous peak: {peak_hours} for N={r.window_hours}"
                )

    def test_filters_out_flat_or_inverted_rates(self) -> None:
        """Candidates with on-peak price <= off-peak price (ratio <= 1.0) should be filtered out."""
        idx = _make_hourly_index()
        # Flat MC profile: all ratios will be 1.0, so all should be filtered
        mc, load = _flat_mc_and_load(idx)
        seasons = make_winter_summer_seasons()

        results = sweep_tou_window_hours(mc, load, seasons, window_range=range(1, 24))

        # All candidates should be filtered out (ratio = 1.0 for flat MC)
        assert len(results) == 0

    def test_filters_out_candidates_with_ratio_le_one_in_any_season(self) -> None:
        """If any season has ratio <= 1.0, the candidate should be filtered out."""
        idx = _make_hourly_index()
        # Create a profile where some window widths might have ratio <= 1.0
        # Use a very small spike that might not create ratio > 1.0 for all window widths
        mc, load = _spike_mc_profile(
            idx, spike_hours=[17, 18], spike_mc=0.11, base_mc=0.10
        )
        seasons = make_winter_summer_seasons()

        results = sweep_tou_window_hours(mc, load, seasons, window_range=range(1, 24))

        # All returned results should have ratio > 1.0 in all seasons
        for r in results:
            for season_name, ratio in r.ratio_by_season.items():
                assert ratio > 1.0, (
                    f"Result with N={r.window_hours} has ratio={ratio} <= 1.0 "
                    f"in season {season_name}"
                )


# ---------------------------------------------------------------------------
# update_periods_yaml
# ---------------------------------------------------------------------------


class TestUpdatePeriodsYaml:
    def test_creates_file_if_missing(self, tmp_path: object) -> None:
        import yaml as _yaml

        p = tmp_path / "periods" / "test.yaml"  # type: ignore[operator]
        update_periods_yaml(p, 5)
        assert p.exists()
        data = _yaml.safe_load(p.read_text())
        assert data["tou_window_hours"] == 5

    def test_preserves_existing_keys(self, tmp_path: object) -> None:
        import yaml as _yaml

        p = tmp_path / "test.yaml"  # type: ignore[operator]
        p.write_text("winter_months: [10, 11, 12, 1, 2, 3]\ntou_window_hours: 4\n")
        update_periods_yaml(p, 7)
        data = _yaml.safe_load(p.read_text())
        assert data["tou_window_hours"] == 7
        assert data["winter_months"] == [10, 11, 12, 1, 2, 3]
