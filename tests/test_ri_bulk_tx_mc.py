"""Tests for ISO-NE bulk transmission marginal cost logic (AESC PTF engine)."""

from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

from utils.pre.marginal_costs.bulk_tx_isone import (
    AESC_2024_AVOIDED_PTF_KW_YEAR,
    DEFAULT_N_PEAK_HOURS,
    compute_isone_bulk_tx_signal,
    prepare_output,
    validate_allocation,
)
from utils.pre.marginal_costs.supply_utils import (
    ISONE_ALL_LOAD_ZONES,
    allocate_annual_exceedance_to_hours,
)


# ── helpers ──────────────────────────────────────────────────────────────────


def _make_ne_load_profile(year: int = 2025, n_hours: int = 8760) -> pl.DataFrame:
    """Synthetic NE system load: summer higher than winter, with trend."""
    timestamps = pl.datetime_range(
        datetime(year, 1, 1, 0, 0, 0),
        datetime(year, 12, 31, 23, 0, 0),
        interval="1h",
        eager=True,
    )
    df = pl.DataFrame({"timestamp": timestamps}).head(n_hours).with_row_index("idx")
    summer = {5, 6, 7, 8, 9}
    return df.with_columns(
        (
            pl.when(pl.col("timestamp").dt.month().is_in(list(summer)))
            .then(20_000.0)
            .otherwise(14_000.0)
            + (pl.col("idx") % 24).cast(pl.Float64) * 100.0
            + (pl.col("idx") * 0.01)
        ).alias("load_mw")
    ).select("timestamp", "load_mw")


def _make_ri_zone_load(ne_load: pl.DataFrame, share: float = 0.05) -> pl.DataFrame:
    """Synthetic RI zone load as a fixed share of NE total."""
    return ne_load.with_columns((pl.col("load_mw") * share).alias("load_mw"))


# ── allocate_annual_exceedance_to_hours ──────────────────────────────────────


class TestExceedanceAllocation:
    def test_returns_n_peak_hours_rows(self) -> None:
        load_df = _make_ne_load_profile()
        result = allocate_annual_exceedance_to_hours(
            load_df, annual_cost_kw_year=69.0, n_peak_hours=100
        )
        assert result.height == 100

    def test_1kw_recovery(self) -> None:
        """Sum of allocated cost equals the annual cost (1 kW constant load)."""
        load_df = _make_ne_load_profile()
        ptf = 69.0
        result = allocate_annual_exceedance_to_hours(
            load_df, annual_cost_kw_year=ptf, n_peak_hours=100
        )
        total = float(result["cost_per_kw"].sum())
        assert total == pytest.approx(ptf, rel=1e-4)

    def test_custom_n_peak_hours(self) -> None:
        load_df = _make_ne_load_profile()
        result = allocate_annual_exceedance_to_hours(
            load_df, annual_cost_kw_year=50.0, n_peak_hours=50
        )
        assert result.height == 50
        assert float(result["cost_per_kw"].sum()) == pytest.approx(50.0, rel=1e-4)

    def test_raises_on_insufficient_hours(self) -> None:
        # Only 24 hours of data, ask for 100
        load_df = _make_ne_load_profile(n_hours=24)
        with pytest.raises(ValueError, match="need at least"):
            allocate_annual_exceedance_to_hours(
                load_df, annual_cost_kw_year=69.0, n_peak_hours=100
            )

    def test_custom_cost_col_name(self) -> None:
        load_df = _make_ne_load_profile()
        result = allocate_annual_exceedance_to_hours(
            load_df, annual_cost_kw_year=42.0, n_peak_hours=50, cost_col="my_cost"
        )
        assert "my_cost" in result.columns
        assert float(result["my_cost"].sum()) == pytest.approx(42.0, rel=1e-4)

    def test_peak_hours_are_highest_load(self) -> None:
        """Allocated hours should be the highest-load hours."""
        load_df = _make_ne_load_profile()
        result = allocate_annual_exceedance_to_hours(
            load_df, annual_cost_kw_year=69.0, n_peak_hours=100
        )
        peak_ts = set(result["timestamp"].to_list())
        top100 = (
            load_df.sort("load_mw", descending=True).head(100)["timestamp"].to_list()
        )
        assert peak_ts == set(top100)


# ── compute_isone_bulk_tx_signal ─────────────────────────────────────────────


class TestComputeIsoneBulkTxSignal:
    def test_returns_correct_shape(self) -> None:
        ne_load = _make_ne_load_profile()
        result = compute_isone_bulk_tx_signal(ne_load, aesc_ptf_kw_year=69.0)
        assert result.height == DEFAULT_N_PEAK_HOURS
        assert "bulk_tx_cost_enduse" in result.columns
        assert "timestamp" in result.columns

    def test_1kw_recovery(self) -> None:
        ne_load = _make_ne_load_profile()
        ptf = 69.0
        result = compute_isone_bulk_tx_signal(ne_load, aesc_ptf_kw_year=ptf)
        total = float(result["bulk_tx_cost_enduse"].sum())
        assert total == pytest.approx(ptf, rel=1e-4)

    def test_custom_peak_hours(self) -> None:
        ne_load = _make_ne_load_profile()
        result = compute_isone_bulk_tx_signal(
            ne_load, aesc_ptf_kw_year=69.0, n_peak_hours=50
        )
        assert result.height == 50

    def test_ri_zone_load_informational_only(self) -> None:
        """Passing ri_zone_load_df should not change the cost values."""
        ne_load = _make_ne_load_profile()
        ri_load = _make_ri_zone_load(ne_load)
        ptf = 69.0

        result_without_ri = compute_isone_bulk_tx_signal(ne_load, aesc_ptf_kw_year=ptf)
        result_with_ri = compute_isone_bulk_tx_signal(
            ne_load, aesc_ptf_kw_year=ptf, ri_zone_load_df=ri_load
        )

        # Same cost allocation regardless of whether RI zone load is provided
        assert float(result_without_ri["bulk_tx_cost_enduse"].sum()) == pytest.approx(
            float(result_with_ri["bulk_tx_cost_enduse"].sum()), rel=1e-6
        )


# ── prepare_output ───────────────────────────────────────────────────────────


class TestPrepareOutput:
    def test_output_has_8760_rows(self) -> None:
        ne_load = _make_ne_load_profile()
        signal = compute_isone_bulk_tx_signal(ne_load, aesc_ptf_kw_year=69.0)
        output = prepare_output(signal, year=2025)
        assert output.height == 8760

    def test_non_peak_hours_are_zero(self) -> None:
        ne_load = _make_ne_load_profile()
        signal = compute_isone_bulk_tx_signal(
            ne_load, aesc_ptf_kw_year=69.0, n_peak_hours=100
        )
        output = prepare_output(signal, year=2025)
        n_nonzero = output.filter(pl.col("bulk_tx_cost_enduse") > 0).height
        assert n_nonzero == 100
        n_zero = output.filter(pl.col("bulk_tx_cost_enduse") == 0.0).height
        assert n_zero == 8760 - 100

    def test_no_nulls(self) -> None:
        ne_load = _make_ne_load_profile()
        signal = compute_isone_bulk_tx_signal(ne_load, aesc_ptf_kw_year=69.0)
        output = prepare_output(signal, year=2025)
        assert output.filter(pl.col("bulk_tx_cost_enduse").is_null()).height == 0


# ── validate_allocation ─────────────────────────────────────────────────────


class TestValidateAllocation:
    def test_passes_on_correct_allocation(self) -> None:
        ne_load = _make_ne_load_profile()
        ptf = 69.0
        signal = compute_isone_bulk_tx_signal(ne_load, aesc_ptf_kw_year=ptf)
        output = prepare_output(signal, year=2025)
        # Should not raise
        validate_allocation(output, ptf)

    def test_raises_on_bad_allocation(self) -> None:
        ne_load = _make_ne_load_profile()
        ptf = 69.0
        signal = compute_isone_bulk_tx_signal(ne_load, aesc_ptf_kw_year=ptf)
        output = prepare_output(signal, year=2025)
        # Validate with a different PTF value → should fail
        with pytest.raises(ValueError, match="PTF validation failed"):
            validate_allocation(output, ptf * 2.0)


# ── AESC constant ────────────────────────────────────────────────────────────


class TestAescConstant:
    def test_aesc_2024_value(self) -> None:
        """AESC 2024 avoided PTF is $69/kW-year."""
        assert AESC_2024_AVOIDED_PTF_KW_YEAR == 69.0


# ── ISO-NE zone constants ───────────────────────────────────────────────────


class TestIsoneZoneConstants:
    def test_all_eight_zones(self) -> None:
        assert len(ISONE_ALL_LOAD_ZONES) == 8
        assert "RI" in ISONE_ALL_LOAD_ZONES
        assert "CT" in ISONE_ALL_LOAD_ZONES
