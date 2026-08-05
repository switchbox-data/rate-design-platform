"""Tests for ISO-NE bulk transmission marginal cost logic (AESC PTF engine)."""

from __future__ import annotations

from datetime import datetime
from typing import cast

import polars as pl
import pytest

from utils.data_prep.marginal_costs.bulk_tx_isone import (
    AESC_2024_AVOIDED_PTF_KW_YEAR,
    DEFAULT_N_PEAK_HOURS,
    compute_isone_bulk_tx_signal,
    prepare_output,
    validate_allocation,
)
from utils.data_prep.marginal_costs.supply_utils import (
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

    def test_utility_zone_load_informational_only(self) -> None:
        """Passing utility_zone_load_df should not change the cost values."""
        ne_load = _make_ne_load_profile()
        ri_load = _make_ri_zone_load(ne_load)
        ptf = 69.0

        result_without = compute_isone_bulk_tx_signal(ne_load, aesc_ptf_kw_year=ptf)
        result_with = compute_isone_bulk_tx_signal(
            ne_load, aesc_ptf_kw_year=ptf, utility_zone_load_df=ri_load
        )

        # Same cost allocation regardless of whether utility zone load is provided
        assert float(result_without["bulk_tx_cost_enduse"].sum()) == pytest.approx(
            float(result_with["bulk_tx_cost_enduse"].sum()), rel=1e-6
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


# ── allocation-load modes: system-wide vs. single-zone ───────────────────────


def _make_zone_load(
    year: int = 2025,
    base_mw: float = 5_000.0,
    summer_boost_mw: float = 2_000.0,
    hour_trend: float = 50.0,
    noise_offset: int = 0,
) -> pl.DataFrame:
    """Synthetic single-zone load.

    Parameterised so callers can produce a zone whose summer/shoulder peaks
    differ from the system-wide aggregate returned by ``_make_ne_load_profile``.
    ``noise_offset`` shifts the intra-day trend phase so peak hours can diverge
    across zones.
    """
    timestamps = pl.datetime_range(
        datetime(year, 1, 1, 0, 0, 0),
        datetime(year, 12, 31, 23, 0, 0),
        interval="1h",
        eager=True,
    )
    df = pl.DataFrame({"timestamp": timestamps}).head(8760).with_row_index("idx")
    summer = {5, 6, 7, 8, 9}
    return df.with_columns(
        (
            pl.when(pl.col("timestamp").dt.month().is_in(list(summer)))
            .then(base_mw + summer_boost_mw)
            .otherwise(base_mw)
            + ((pl.col("idx") + noise_offset) % 24).cast(pl.Float64) * hour_trend
            + (pl.col("idx") * 0.005)
        ).alias("load_mw")
    ).select("timestamp", "load_mw")


def _sum_zone_loads(*zone_dfs: pl.DataFrame) -> pl.DataFrame:
    """Sum multiple zone load DataFrames into a single aggregate."""
    agg = zone_dfs[0]
    for other in zone_dfs[1:]:
        agg = (
            agg.join(other.rename({"load_mw": "_other"}), on="timestamp", how="left")
            .with_columns((pl.col("load_mw") + pl.col("_other")).alias("load_mw"))
            .select("timestamp", "load_mw")
        )
    return agg


class TestAllocationLoadModes:
    """Verify that system-wide vs. single-zone allocation loads produce
    valid, internally-consistent signals with the expected differences."""

    def test_both_recover_annual_ptf_cost(self) -> None:
        """Both allocation-load modes should recover the full AESC PTF cost."""
        ptf = 69.0
        n = 100
        system_load = _make_ne_load_profile()
        zone_load = _make_zone_load()

        system_signal = prepare_output(
            compute_isone_bulk_tx_signal(system_load, ptf, n_peak_hours=n), 2025
        )
        zone_signal = prepare_output(
            compute_isone_bulk_tx_signal(zone_load, ptf, n_peak_hours=n), 2025
        )

        assert float(system_signal["bulk_tx_cost_enduse"].sum()) == pytest.approx(
            ptf, rel=1e-4
        )
        assert float(zone_signal["bulk_tx_cost_enduse"].sum()) == pytest.approx(
            ptf, rel=1e-4
        )

    def test_both_have_exactly_n_peak_hours(self) -> None:
        n = 50
        system_load = _make_ne_load_profile()
        zone_load = _make_zone_load()

        system_signal = compute_isone_bulk_tx_signal(system_load, 69.0, n_peak_hours=n)
        zone_signal = compute_isone_bulk_tx_signal(zone_load, 69.0, n_peak_hours=n)

        assert system_signal.height == n
        assert zone_signal.height == n

    def test_peak_thresholds_differ_by_load_magnitude(self) -> None:
        """The system aggregate has a higher peak threshold than a single zone
        because summing zones produces higher absolute MW values."""
        system_load = _make_ne_load_profile()  # ~14–22 GW synthetic aggregate
        zone_load = _make_zone_load(base_mw=5_000.0, summer_boost_mw=2_000.0)  # ~5–7 GW

        system_top = cast(
            float,
            system_load.sort("load_mw", descending=True).head(100)["load_mw"].min(),
        )
        zone_top = cast(
            float, zone_load.sort("load_mw", descending=True).head(100)["load_mw"].min()
        )
        assert system_top is not None and zone_top is not None

        assert system_top > zone_top

    def test_zone_load_is_subset_of_system_peak_hours(self) -> None:
        """When zone load tracks the system aggregate closely (fixed share),
        almost all zone-only peak hours should overlap with system peak hours."""
        system_load = _make_ne_load_profile()
        # Zone load is a strict proportional share of the system — peaks are identical
        zone_load = _make_ri_zone_load(system_load, share=0.25)
        n = 100

        system_signal = compute_isone_bulk_tx_signal(system_load, 69.0, n_peak_hours=n)
        zone_signal = compute_isone_bulk_tx_signal(zone_load, 69.0, n_peak_hours=n)

        system_peak_ts = set(system_signal["timestamp"].to_list())
        zone_peak_ts = set(zone_signal["timestamp"].to_list())

        # A proportional zone has the same rank ordering → identical peak windows
        assert system_peak_ts == zone_peak_ts

    def test_divergent_zone_produces_different_peak_hours(self) -> None:
        """A zone whose intra-day peak phase differs from the system aggregate
        identifies a different set of peak hours."""
        system_load = _make_ne_load_profile()
        # noise_offset=12 shifts the intra-day peak by 12 hours so zone peaks
        # at different hours than the system
        zone_load = _make_zone_load(noise_offset=12)
        n = 100

        system_signal = compute_isone_bulk_tx_signal(system_load, 69.0, n_peak_hours=n)
        zone_signal = compute_isone_bulk_tx_signal(zone_load, 69.0, n_peak_hours=n)

        system_peak_ts = set(system_signal["timestamp"].to_list())
        zone_peak_ts = set(zone_signal["timestamp"].to_list())

        # With a shifted phase, some peak hours must differ
        assert system_peak_ts != zone_peak_ts

    def test_system_aggregate_sums_zones(self) -> None:
        """A hand-summed multi-zone aggregate produces the same peak hours
        as loading zones individually and summing via _sum_zone_loads."""
        zone_a = _make_zone_load(
            base_mw=10_000.0, summer_boost_mw=3_000.0, noise_offset=0
        )
        zone_b = _make_zone_load(
            base_mw=5_000.0, summer_boost_mw=1_500.0, noise_offset=3
        )
        zone_c = _make_zone_load(base_mw=3_000.0, summer_boost_mw=800.0, noise_offset=6)

        system = _sum_zone_loads(zone_a, zone_b, zone_c)
        n = 100

        system_signal = compute_isone_bulk_tx_signal(system, 69.0, n_peak_hours=n)
        # Peak hours of the sum must be the top-n hours of the combined load
        top_n_ts = set(
            system.sort("load_mw", descending=True).head(n)["timestamp"].to_list()
        )
        assert set(system_signal["timestamp"].to_list()) == top_n_ts

    def test_informational_zone_load_does_not_change_signal(self) -> None:
        """Passing utility_zone_load_df changes only what's printed, not the signal."""
        system_load = _make_ne_load_profile()
        zone_load = _make_zone_load()
        ptf = 69.0
        n = 100

        without_info = compute_isone_bulk_tx_signal(system_load, ptf, n_peak_hours=n)
        with_info = compute_isone_bulk_tx_signal(
            system_load, ptf, n_peak_hours=n, utility_zone_load_df=zone_load
        )

        assert set(without_info["timestamp"].to_list()) == set(
            with_info["timestamp"].to_list()
        )
        assert float(without_info["bulk_tx_cost_enduse"].sum()) == pytest.approx(
            float(with_info["bulk_tx_cost_enduse"].sum()), rel=1e-6
        )


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
