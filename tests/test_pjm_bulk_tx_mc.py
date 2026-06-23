"""Tests for PJM bulk transmission marginal cost logic (NITS + PCAF engine).

Covers:
- compute_blended_nits_rate: day-weighting arithmetic, leap/non-leap year,
  edge cases (equal Jan/Jun rates, missing data)
- allocate_pcaf: 1 kW cost recovery, exactly K non-zero hours, correct peak
  selection, error on insufficient data
- Utility → NITS zone mapping and VALID_PJM_UTILITIES completeness
- load_nits_rates: smoke test against the real committed CSV
"""

from __future__ import annotations

import calendar
from datetime import datetime

import polars as pl
import pytest

from utils.data_prep.marginal_costs.bulk_tx_pjm import (
    UTILITY_TO_NITS_ZONE,
    VALID_PJM_UTILITIES,
    allocate_pcaf,
    compute_blended_nits_rate,
    load_nits_rates,
)

# ── helpers ───────────────────────────────────────────────────────────────────


def _make_nits_df(
    year: int,
    zone: str,
    jan_kw: float,
    jun_kw: float,
) -> pl.DataFrame:
    """Minimal synthetic NITS DataFrame: one zone, one year, two effective dates."""
    return pl.DataFrame(
        {
            "year": [year, year],
            "effective_date": [f"{year}-01-01", f"{year}-06-01"],
            "zone": [zone, zone],
            "nits_rate_mw_yr": [jan_kw * 1000, jun_kw * 1000],
            "nits_rate_kw_yr": [jan_kw, jun_kw],
            "source_url": ["https://example.com", "https://example.com"],
        }
    )


def _make_load_df(year: int = 2025, base_mw: float = 1000.0) -> pl.DataFrame:
    """Synthetic full-year hourly load: monotonically increasing so top-K hours
    are unambiguous (last K hours of the year)."""
    timestamps = pl.datetime_range(
        datetime(year, 1, 1, 0),
        datetime(year, 12, 31, 23),
        interval="1h",
        eager=True,
    )
    n = len(timestamps)
    return pl.DataFrame(
        {
            "timestamp": timestamps,
            "load_mw": [base_mw + float(i) for i in range(n)],
        }
    )


# ── compute_blended_nits_rate ─────────────────────────────────────────────────


class TestComputeBlendedNitsRate:
    def test_non_leap_year_day_weights(self) -> None:
        """Non-leap year: 151 Jan days + 214 Jun days = 365."""
        year = 2025
        assert not calendar.isleap(year)
        jan_kw, jun_kw = 40.0, 50.0
        nits_df = _make_nits_df(year, "BGE", jan_kw, jun_kw)

        result = compute_blended_nits_rate(nits_df, "BGE", year)

        expected = (151 * jan_kw + 214 * jun_kw) / 365
        assert result == pytest.approx(expected, rel=1e-9)

    def test_leap_year_day_weights(self) -> None:
        """Leap year: 152 Jan days + 214 Jun days = 366."""
        year = 2024
        assert calendar.isleap(year)
        jan_kw, jun_kw = 40.0, 50.0
        nits_df = _make_nits_df(year, "BGE", jan_kw, jun_kw)

        result = compute_blended_nits_rate(nits_df, "BGE", year)

        expected = (152 * jan_kw + 214 * jun_kw) / 366
        assert result == pytest.approx(expected, rel=1e-9)

    def test_equal_jan_jun_rates_returns_same_rate(self) -> None:
        """When Jan == Jun, the blended rate equals that rate exactly."""
        rate = 55.0
        nits_df = _make_nits_df(2025, "DPL", rate, rate)
        result = compute_blended_nits_rate(nits_df, "DPL", 2025)
        assert result == pytest.approx(rate, rel=1e-9)

    def test_blended_is_between_jan_and_jun(self) -> None:
        """Blended rate must lie strictly between the two component rates."""
        jan_kw, jun_kw = 30.0, 60.0
        nits_df = _make_nits_df(2025, "PEPCO", jan_kw, jun_kw)
        result = compute_blended_nits_rate(nits_df, "PEPCO", 2025)
        assert jan_kw < result < jun_kw

    def test_jun_weighted_more_than_jan_in_non_leap(self) -> None:
        """214 Jun days > 151 Jan days, so blended should be closer to Jun."""
        jan_kw, jun_kw = 0.0, 100.0
        nits_df = _make_nits_df(2025, "APS", jan_kw, jun_kw)
        result = compute_blended_nits_rate(nits_df, "APS", 2025)
        # 214/365 ≈ 0.586 of the way to jun_kw
        assert result == pytest.approx(214 / 365 * 100.0, rel=1e-9)

    def test_raises_on_missing_zone_year(self) -> None:
        nits_df = _make_nits_df(2025, "BGE", 40.0, 50.0)
        with pytest.raises(ValueError, match="No NITS data for zone=DPL"):
            compute_blended_nits_rate(nits_df, "DPL", 2025)

    def test_raises_on_missing_jun_rate(self) -> None:
        """Only a Jan row present — should raise about missing Jun rate."""
        nits_df = pl.DataFrame(
            {
                "year": [2025],
                "effective_date": ["2025-01-01"],
                "zone": ["BGE"],
                "nits_rate_mw_yr": [40_000.0],
                "nits_rate_kw_yr": [40.0],
                "source_url": ["https://example.com"],
            }
        )
        with pytest.raises(ValueError, match="Missing Jan or Jun rate"):
            compute_blended_nits_rate(nits_df, "BGE", 2025)

    def test_multiple_zones_selects_correct_one(self) -> None:
        """Ensure zone filtering works when multiple zones are present."""
        bge_nits = _make_nits_df(2025, "BGE", 40.0, 50.0)
        dpl_nits = _make_nits_df(2025, "DPL", 60.0, 70.0)
        combined = pl.concat([bge_nits, dpl_nits])

        bge_result = compute_blended_nits_rate(combined, "BGE", 2025)
        dpl_result = compute_blended_nits_rate(combined, "DPL", 2025)

        expected_bge = (151 * 40.0 + 214 * 50.0) / 365
        expected_dpl = (151 * 60.0 + 214 * 70.0) / 365
        assert bge_result == pytest.approx(expected_bge, rel=1e-9)
        assert dpl_result == pytest.approx(expected_dpl, rel=1e-9)


# ── allocate_pcaf ─────────────────────────────────────────────────────────────


class TestAllocatePcaf:
    def test_1kw_cost_recovery(self) -> None:
        """Sum of allocated costs must equal the annual rate (1 kW basis)."""
        load_df = _make_load_df()
        rate = 57.74
        result = allocate_pcaf(load_df, annual_cost_kw_year=rate, k_peak_hours=150)
        total = float(result["bulk_tx_cost_enduse"].sum())
        assert total == pytest.approx(rate, rel=1e-6)

    def test_exactly_k_nonzero_rows(self) -> None:
        """allocate_pcaf returns exactly K rows, all non-zero."""
        load_df = _make_load_df()
        k = 150
        result = allocate_pcaf(load_df, annual_cost_kw_year=50.0, k_peak_hours=k)
        assert result.height == k
        assert result.filter(pl.col("bulk_tx_cost_enduse") <= 0).height == 0

    def test_default_k_is_150(self) -> None:
        load_df = _make_load_df()
        result = allocate_pcaf(load_df, annual_cost_kw_year=50.0)
        assert result.height == 150

    def test_custom_k(self) -> None:
        load_df = _make_load_df()
        for k in [5, 50, 200]:
            result = allocate_pcaf(load_df, annual_cost_kw_year=50.0, k_peak_hours=k)
            assert result.height == k
            assert float(result["bulk_tx_cost_enduse"].sum()) == pytest.approx(
                50.0, rel=1e-6
            )

    def test_peak_hours_are_highest_load_hours(self) -> None:
        """The K allocated hours must be the K highest-load hours in the year."""
        load_df = _make_load_df()
        k = 150
        result = allocate_pcaf(load_df, annual_cost_kw_year=50.0, k_peak_hours=k)

        top_k_ts = set(
            load_df.sort("load_mw", descending=True).head(k)["timestamp"].to_list()
        )
        allocated_ts = set(result["timestamp"].to_list())
        assert allocated_ts == top_k_ts

    def test_proportional_to_load(self) -> None:
        """Higher-load hours receive proportionally larger allocations."""
        load_df = _make_load_df()
        result = allocate_pcaf(
            load_df, annual_cost_kw_year=50.0, k_peak_hours=150
        ).sort("bulk_tx_cost_enduse", descending=True)

        # The highest-cost hour should correspond to the highest-load hour among
        # the top-K.  Since load is monotonically increasing in our fixture, the
        # hour with the highest timestamp in the top-K has the largest load.
        top_k = load_df.sort("load_mw", descending=True).head(150)
        max_load_ts = top_k.sort("load_mw", descending=True)["timestamp"][0]
        assert result["timestamp"][0] == max_load_ts

    def test_cost_sums_to_rate_with_uniform_load(self) -> None:
        """With uniform load, all K hours get equal share = rate / K."""
        n = 8760
        timestamps = pl.datetime_range(
            datetime(2025, 1, 1, 0),
            datetime(2025, 12, 31, 23),
            interval="1h",
            eager=True,
        )
        uniform_load = pl.DataFrame({"timestamp": timestamps, "load_mw": [100.0] * n})
        rate = 60.0
        k = 150
        result = allocate_pcaf(uniform_load, annual_cost_kw_year=rate, k_peak_hours=k)

        # Every allocated hour should receive rate / K
        expected_per_hour = rate / k
        for cost in result["bulk_tx_cost_enduse"].to_list():
            assert cost == pytest.approx(expected_per_hour, rel=1e-9)

    def test_raises_on_insufficient_hours(self) -> None:
        """Fewer load rows than K should raise."""
        timestamps = pl.datetime_range(
            datetime(2025, 1, 1, 0),
            datetime(2025, 1, 5, 23),
            interval="1h",
            eager=True,
        )
        small_df = pl.DataFrame(
            {"timestamp": timestamps, "load_mw": [1000.0] * len(timestamps)}
        )
        with pytest.raises(ValueError, match="need at least"):
            allocate_pcaf(small_df, annual_cost_kw_year=50.0, k_peak_hours=150)

    def test_raises_on_zero_total_load(self) -> None:
        load_df = _make_load_df().with_columns(pl.lit(0.0).alias("load_mw"))
        with pytest.raises(ValueError, match="zero or negative"):
            allocate_pcaf(load_df, annual_cost_kw_year=50.0, k_peak_hours=150)

    def test_output_columns(self) -> None:
        load_df = _make_load_df()
        result = allocate_pcaf(load_df, annual_cost_kw_year=50.0)
        assert set(result.columns) == {"timestamp", "bulk_tx_cost_enduse"}


# ── utility → NITS zone mapping ───────────────────────────────────────────────


class TestUtilityZoneMapping:
    def test_all_ten_md_utilities_present(self) -> None:
        expected = {
            "bge",
            "dpl",
            "pepco",
            "poted",
            "smeco",
            "choptank",
            "somerset_rec",
            "hagerstown_muni",
            "easton_muni",
            "berlin_muni",
        }
        assert set(UTILITY_TO_NITS_ZONE.keys()) == expected

    def test_iou_zone_labels(self) -> None:
        assert UTILITY_TO_NITS_ZONE["bge"] == "BGE"
        assert UTILITY_TO_NITS_ZONE["dpl"] == "DPL"
        assert UTILITY_TO_NITS_ZONE["pepco"] == "PEPCO"
        assert UTILITY_TO_NITS_ZONE["poted"] == "APS"

    def test_coops_map_to_host_zone(self) -> None:
        assert UTILITY_TO_NITS_ZONE["smeco"] == "PEPCO"
        assert UTILITY_TO_NITS_ZONE["choptank"] == "DPL"
        assert UTILITY_TO_NITS_ZONE["somerset_rec"] == "APS"

    def test_municipals_map_to_host_zone(self) -> None:
        assert UTILITY_TO_NITS_ZONE["hagerstown_muni"] == "APS"
        assert UTILITY_TO_NITS_ZONE["easton_muni"] == "DPL"
        assert UTILITY_TO_NITS_ZONE["berlin_muni"] == "DPL"

    def test_valid_pjm_utilities_matches_mapping_keys(self) -> None:
        """VALID_PJM_UTILITIES must be exactly the keys of UTILITY_TO_NITS_ZONE."""
        assert VALID_PJM_UTILITIES == frozenset(UTILITY_TO_NITS_ZONE.keys())

    def test_all_zones_are_known_pjm_zones(self) -> None:
        known = {"BGE", "DPL", "PEPCO", "APS"}
        assert set(UTILITY_TO_NITS_ZONE.values()) == known


# ── load_nits_rates (committed CSV smoke test) ────────────────────────────────


class TestLoadNitsRates:
    def test_loads_committed_csv(self) -> None:
        """The real committed nits_rates.csv must load without error."""
        df = load_nits_rates()
        assert df.height == 40  # 4 zones × 2 periods × 5 years
        assert set(df.columns) == {
            "year",
            "effective_date",
            "zone",
            "nits_rate_mw_yr",
            "nits_rate_kw_yr",
            "source_url",
        }

    def test_all_four_zones_present(self) -> None:
        df = load_nits_rates()
        assert set(df["zone"].unique().to_list()) == {"BGE", "DPL", "PEPCO", "APS"}

    def test_five_years_present(self) -> None:
        df = load_nits_rates()
        assert set(df["year"].unique().to_list()) == {2021, 2022, 2023, 2024, 2025}

    def test_each_year_has_jan_and_jun(self) -> None:
        df = load_nits_rates()
        for year in [2021, 2022, 2023, 2024, 2025]:
            year_rows = df.filter(pl.col("year") == year)
            dates = set(year_rows["effective_date"].to_list())
            assert f"{year}-01-01" in dates, f"Missing Jan row for {year}"
            assert f"{year}-06-01" in dates, f"Missing Jun row for {year}"

    def test_kw_yr_is_mw_yr_divided_by_1000(self) -> None:
        """nits_rate_kw_yr must equal round(nits_rate_mw_yr / 1000, 2) for all rows."""
        df = load_nits_rates()
        for row in df.iter_rows(named=True):
            expected = round(row["nits_rate_mw_yr"] / 1000, 2)
            assert row["nits_rate_kw_yr"] == pytest.approx(expected, abs=0.005)

    def test_rates_are_positive(self) -> None:
        df = load_nits_rates()
        assert (df["nits_rate_mw_yr"] > 0).all()
        assert (df["nits_rate_kw_yr"] > 0).all()
