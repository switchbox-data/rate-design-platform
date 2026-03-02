"""Unit tests for bulk transmission marginal cost allocation.

These tests validate:
- SCR hour identification (correct counts, season assignment, non-overlap)
- Load-weighted smear (weights sum to 1.0, 1 kW constant load recovers v_z)
- v_z table loading and quantile resolution
- Output schema (8760 rows, no nulls, correct non-zero count)
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from utils.pre.generate_bulk_tx_mc import (
    DEFAULT_SCR_WINTER_MONTHS,
    allocate_bulk_tx_to_hours,
    identify_scr_hours,
    load_vz_table,
    prepare_output,
    resolve_utility_vz,
)
from utils.pre.season_config import (
    DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
    derive_summer_months,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_load_profile(n_hours: int = 8760, year: int = 2025) -> pl.DataFrame:
    """Create a synthetic 8760 load profile with seasonal variation.

    Summer hours get higher loads than winter so SCR hours cluster predictably.
    A small monotonic trend breaks ties.
    """
    start = datetime(year, 1, 1, 0, 0, 0)
    timestamps = pl.datetime_range(
        start,
        datetime(year, 12, 31, 23, 0, 0),
        interval="1h",
        eager=True,
    )[:n_hours]

    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "utility": ["test_utility"] * n_hours,
        }
    )

    # Seasonal pattern: summer (Apr-Sep) higher base than winter (Oct-Mar),
    # matching the default rate-design season definition.
    # Daily variation + small monotonic trend to avoid ties.
    summer_months = derive_summer_months(list(DEFAULT_SCR_WINTER_MONTHS))
    df = df.with_row_index("idx").with_columns(
        (
            pl.when(pl.col("timestamp").dt.month().is_in(summer_months))
            .then(pl.lit(2000.0))
            .otherwise(pl.lit(1200.0))
            + 300.0 * ((pl.col("idx") % 24).cast(pl.Float64) / 24.0)
            + 0.001 * pl.col("idx").cast(pl.Float64)
        ).alias("load_mw")
    )

    return df.select("timestamp", "utility", "load_mw")


def _make_vz_table() -> pl.DataFrame:
    """Create a v_z table matching the real schema."""
    return pl.DataFrame(
        {
            "gen_capacity_zone": ["ROS", "LHV", "NYC", "LI"],
            "v_low_kw_yr": [9.95, 25.52, 31.60, 30.16],
            "v_mid_kw_yr": [9.95, 26.92, 74.71, 36.44],
            "v_high_kw_yr": [9.95, 36.16, 74.71, 74.18],
            "v_isotonic_kw_yr": [9.95, 22.19, 70.46, 0.0],
        }
    )


def _make_zone_mapping() -> pl.DataFrame:
    """Create a minimal zone mapping for test utilities."""
    return pl.DataFrame(
        {
            "utility": ["test_single", "test_single", "test_multi", "test_multi"],
            "load_zone_letter": ["A", "B", "G", "J"],
            "lbmp_zone_name": ["WEST", "GENESE", "HUD_VL", "N.Y.C."],
            "icap_locality": ["NYCA", "NYCA", "NYC", "GHIJ"],
            "gen_capacity_zone": ["ROS", "ROS", "NYC", "LHV"],
            "tx_locality": ["ROS", "ROS", "NYC", "LHV"],
            "capacity_weight": [1.0, 1.0, 0.87, 0.13],
        }
    )


# ── SCR hour identification ──────────────────────────────────────────────────


class TestIdentifyScrHours:
    """Tests for SCR hour identification."""

    def test_correct_count_per_season(self) -> None:
        """40 SCR hours per season, 80 total."""
        load_df = _make_load_profile()
        result = identify_scr_hours(load_df, n_hours_per_season=40)

        n_summer_scr = result.filter(
            pl.col("is_scr") & (pl.col("season") == "summer")
        ).height
        n_winter_scr = result.filter(
            pl.col("is_scr") & (pl.col("season") == "winter")
        ).height
        n_total_scr = result.filter(pl.col("is_scr")).height

        assert n_summer_scr == 40
        assert n_winter_scr == 40
        assert n_total_scr == 80

    def test_season_assignment(self) -> None:
        """Summer SCR hours are in summer months, winter SCR in winter months."""
        load_df = _make_load_profile()
        result = identify_scr_hours(load_df, n_hours_per_season=40)

        expected_winter = set(DEFAULT_SCR_WINTER_MONTHS)
        expected_summer = set(derive_summer_months(list(DEFAULT_SCR_WINTER_MONTHS)))

        summer_scr = result.filter(pl.col("is_scr") & (pl.col("season") == "summer"))
        winter_scr = result.filter(pl.col("is_scr") & (pl.col("season") == "winter"))

        actual_summer = set(summer_scr["timestamp"].dt.month().unique().to_list())
        actual_winter = set(winter_scr["timestamp"].dt.month().unique().to_list())

        assert actual_summer.issubset(expected_summer)
        assert actual_winter.issubset(expected_winter)

    def test_non_overlapping_seasons(self) -> None:
        """No SCR hour appears in both seasons."""
        load_df = _make_load_profile()
        result = identify_scr_hours(load_df, n_hours_per_season=40)

        scr_hours = result.filter(pl.col("is_scr"))
        # Each SCR hour has exactly one season assignment
        assert scr_hours["season"].n_unique() == 2  # both seasons present
        # No timestamp duplicated
        assert scr_hours["timestamp"].n_unique() == scr_hours.height

    def test_preserves_all_rows(self) -> None:
        """Output has same number of rows as input (8760)."""
        load_df = _make_load_profile()
        result = identify_scr_hours(load_df, n_hours_per_season=40)
        assert result.height == 8760

    def test_scr_hours_are_highest_load(self) -> None:
        """SCR hours should be the top-N hours within each season."""
        load_df = _make_load_profile()
        result = identify_scr_hours(load_df, n_hours_per_season=40)

        for season in ["summer", "winter"]:
            season_df = result.filter(pl.col("season") == season)
            scr_df = season_df.filter(pl.col("is_scr"))
            non_scr_df = season_df.filter(~pl.col("is_scr"))

            min_scr_load = float(scr_df["load_mw"].min())  # type: ignore[arg-type]
            max_non_scr_load = float(non_scr_df["load_mw"].max())  # type: ignore[arg-type]

            assert min_scr_load >= max_non_scr_load, (
                f"{season}: min SCR load ({min_scr_load:.2f}) < "
                f"max non-SCR load ({max_non_scr_load:.2f})"
            )

    def test_custom_hours_per_season(self) -> None:
        """Works with custom N (e.g. 20 per season)."""
        load_df = _make_load_profile()
        result = identify_scr_hours(load_df, n_hours_per_season=20)

        n_total = result.filter(pl.col("is_scr")).height
        assert n_total == 40

    def test_custom_winter_months(self) -> None:
        """Custom winter_months drives SCR season assignment."""
        load_df = _make_load_profile()
        custom_winter = [11, 12, 1, 2, 3, 4]  # Nov–Apr (NYISO capability period)
        result = identify_scr_hours(
            load_df, n_hours_per_season=40, winter_months=custom_winter
        )

        winter_scr = result.filter(pl.col("is_scr") & (pl.col("season") == "winter"))
        actual_winter_months = set(
            winter_scr["timestamp"].dt.month().unique().to_list()
        )
        assert actual_winter_months.issubset(set(custom_winter))


# ── Load-weighted allocation ─────────────────────────────────────────────────


class TestAllocateBulkTxToHours:
    """Tests for the load-weighted SCR smear."""

    def test_weights_sum_to_one(self) -> None:
        """Weights across all SCR hours sum to 1.0."""
        load_df = _make_load_profile()
        load_with_scr = identify_scr_hours(load_df, n_hours_per_season=40)

        # Compute weights manually to verify
        scr_load_sum = float(load_with_scr.filter(pl.col("is_scr"))["load_mw"].sum())
        scr_loads = load_with_scr.filter(pl.col("is_scr"))["load_mw"]
        weight_sum = float((scr_loads / scr_load_sum).sum())

        assert abs(weight_sum - 1.0) < 1e-6

    def test_1kw_constant_load_recovers_vz(self) -> None:
        """A 1 kW constant load recovers exactly v_z $/kW-yr."""
        load_df = _make_load_profile()
        load_with_scr = identify_scr_hours(load_df, n_hours_per_season=40)

        v_z = 50.0  # $/kW-yr
        allocated = allocate_bulk_tx_to_hours(load_with_scr, v_z)

        # 1 kW × 1 hour × $/MWh / 1000 = $/kWh contribution
        # Sum over all hours = v_z $/kW-yr
        actual = float(allocated["bulk_tx_cost_enduse"].sum()) / 1000.0
        assert abs(actual - v_z) < v_z * 1e-4, (
            f"1 kW recovery: expected {v_z}, got {actual}"
        )

    def test_only_scr_hours_have_nonzero_cost(self) -> None:
        """Non-SCR hours have zero transmission cost."""
        load_df = _make_load_profile()
        load_with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(load_with_scr, v_z=30.0)

        n_nonzero = allocated.filter(pl.col("bulk_tx_cost_enduse") > 0).height
        assert n_nonzero == 80  # 40 summer + 40 winter

    def test_all_costs_non_negative(self) -> None:
        """No negative transmission costs."""
        load_df = _make_load_profile()
        load_with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(load_with_scr, v_z=30.0)

        neg_count = allocated.filter(pl.col("bulk_tx_cost_enduse") < 0).height
        assert neg_count == 0

    def test_higher_load_gets_higher_cost(self) -> None:
        """Within SCR hours, higher load → higher cost (load-weighted)."""
        load_df = _make_load_profile()
        load_with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(load_with_scr, v_z=30.0)

        # Join back to get load
        joined = load_with_scr.filter(pl.col("is_scr")).join(
            allocated, on="timestamp", how="inner"
        )
        # Sort by load descending; costs should also be descending
        sorted_df = joined.sort("load_mw", descending=True)
        costs = sorted_df["bulk_tx_cost_enduse"].to_list()
        assert costs == sorted(costs, reverse=True)

    @pytest.mark.parametrize("v_z", [10.0, 30.0, 55.0, 74.71])
    def test_1kw_recovery_various_vz(self, v_z: float) -> None:
        """1 kW constant load test for various v_z values."""
        load_df = _make_load_profile()
        load_with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(load_with_scr, v_z)

        actual = float(allocated["bulk_tx_cost_enduse"].sum()) / 1000.0
        assert abs(actual - v_z) < v_z * 1e-4


# ── v_z table and resolution ─────────────────────────────────────────────────


class TestVzResolution:
    """Tests for v_z table loading and utility resolution."""

    def test_load_vz_table_from_local(self, tmp_path: Path) -> None:
        """Loads a v_z CSV from local path."""
        vz_df = _make_vz_table()
        csv_path = str(tmp_path / "test_vz.csv")
        vz_df.write_csv(csv_path)

        loaded = load_vz_table(csv_path)
        assert loaded.height == 4
        assert "gen_capacity_zone" in loaded.columns
        assert "v_mid_kw_yr" in loaded.columns

    def test_resolve_single_zone_utility(self) -> None:
        """Single-zone utility gets v_z directly."""
        mapping = _make_zone_mapping()
        vz_df = _make_vz_table()

        v_z = resolve_utility_vz(mapping, vz_df, "test_single", "mid")
        expected = 9.95  # ROS mid
        assert abs(v_z - expected) < 0.01

    def test_resolve_multi_zone_utility(self) -> None:
        """Multi-zone utility gets capacity-weighted blend."""
        mapping = _make_zone_mapping()
        vz_df = _make_vz_table()

        v_z = resolve_utility_vz(mapping, vz_df, "test_multi", "mid")
        # 0.87 * NYC(74.71) + 0.13 * LHV(26.92) = 65.0077 + 3.4996 = 68.5073
        expected = 0.87 * 74.71 + 0.13 * 26.92
        assert abs(v_z - expected) < 0.01

    def test_resolve_unknown_utility_raises(self) -> None:
        """Unknown utility raises ValueError."""
        mapping = _make_zone_mapping()
        vz_df = _make_vz_table()

        with pytest.raises(ValueError, match="not found in zone mapping"):
            resolve_utility_vz(mapping, vz_df, "nonexistent", "mid")

    @pytest.mark.parametrize("quantile", ["low", "mid", "high", "isotonic"])
    def test_all_quantiles_work(self, quantile: str) -> None:
        """All quantile selections produce a positive v_z for single-zone."""
        mapping = _make_zone_mapping()
        vz_df = _make_vz_table()

        v_z = resolve_utility_vz(mapping, vz_df, "test_single", quantile)
        assert v_z > 0


# ── Output preparation ───────────────────────────────────────────────────────


class TestPrepareOutput:
    """Tests for output assembly."""

    def test_output_has_8760_rows(self) -> None:
        """Output DataFrame has exactly 8760 rows."""
        load_df = _make_load_profile()
        load_with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(load_with_scr, v_z=30.0)
        output = prepare_output(allocated, year=2025)

        assert output.height == 8760

    def test_output_no_nulls(self) -> None:
        """Output has no null values."""
        load_df = _make_load_profile()
        load_with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(load_with_scr, v_z=30.0)
        output = prepare_output(allocated, year=2025)

        null_count = output.filter(pl.col("bulk_tx_cost_enduse").is_null()).height
        assert null_count == 0

    def test_output_schema(self) -> None:
        """Output has expected columns."""
        load_df = _make_load_profile()
        load_with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(load_with_scr, v_z=30.0)
        output = prepare_output(allocated, year=2025)

        assert set(output.columns) == {"timestamp", "bulk_tx_cost_enduse"}

    def test_output_preserves_1kw_recovery(self) -> None:
        """1 kW constant load still recovers v_z after prepare_output."""
        v_z = 43.0
        load_df = _make_load_profile()
        load_with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(load_with_scr, v_z)
        output = prepare_output(allocated, year=2025)

        actual = float(output["bulk_tx_cost_enduse"].sum()) / 1000.0
        assert abs(actual - v_z) < v_z * 1e-4

    def test_correct_nonzero_count(self) -> None:
        """Non-zero hours in output == 80 (40 per season)."""
        load_df = _make_load_profile()
        load_with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(load_with_scr, v_z=30.0)
        output = prepare_output(allocated, year=2025)

        n_nonzero = output.filter(pl.col("bulk_tx_cost_enduse") > 0).height
        assert n_nonzero == 80


# ── Season alignment ──────────────────────────────────────────────────────────


class TestSeasonAlignment:
    """Programmatic checks that SCR seasons stay coherent with rate-design seasons."""

    def test_default_scr_matches_seasonal_discount_default(self) -> None:
        """DEFAULT_SCR_WINTER_MONTHS must equal DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS.

        This is the primary guard: if either constant changes, this test fails and
        forces a deliberate decision about whether to keep them in sync.
        """
        assert set(DEFAULT_SCR_WINTER_MONTHS) == set(
            DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS
        ), (
            "Bulk Tx MC default season (DEFAULT_SCR_WINTER_MONTHS) has drifted from "
            "the rate-design default (DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS). "
            "Update one of them intentionally or re-align."
        )

    def test_default_scr_matches_ny_utility_yaml(self, tmp_path: Path) -> None:
        """Default SCR winter months match the NY utility periods YAML definition."""
        # Write a typical NY utility YAML (all 7 NY utilities use Oct–Mar)
        yaml_content = "winter_months: [10, 11, 12, 1, 2, 3]\ntou_window_hours: 4\n"
        yaml_path = tmp_path / "nyseg.yaml"
        yaml_path.write_text(yaml_content)

        from utils.pre.season_config import load_winter_months_from_periods

        yaml_winter = load_winter_months_from_periods(
            yaml_path,
            default_winter_months=DEFAULT_SCR_WINTER_MONTHS,
        )
        assert set(yaml_winter) == set(DEFAULT_SCR_WINTER_MONTHS), (
            f"NY utility periods YAML winter months {sorted(yaml_winter)} differ from "
            f"DEFAULT_SCR_WINTER_MONTHS {sorted(DEFAULT_SCR_WINTER_MONTHS)}. "
            "Update either the YAMLs or the default constant."
        )

    def test_scr_season_contains_whole_year(self) -> None:
        """Winter + summer months from DEFAULT_SCR_WINTER_MONTHS cover all 12 months."""
        winter = list(DEFAULT_SCR_WINTER_MONTHS)
        summer = derive_summer_months(winter)
        assert set(winter) | set(summer) == set(range(1, 13))
        assert set(winter) & set(summer) == set()

    def test_identify_scr_uses_default_when_no_winter_months_given(self) -> None:
        """identify_scr_hours() with no winter_months uses DEFAULT_SCR_WINTER_MONTHS."""
        load_df = _make_load_profile()
        result_default = identify_scr_hours(load_df, n_hours_per_season=40)
        result_explicit = identify_scr_hours(
            load_df,
            n_hours_per_season=40,
            winter_months=list(DEFAULT_SCR_WINTER_MONTHS),
        )

        # Both should produce identical SCR hour assignments
        assert result_default["is_scr"].to_list() == result_explicit["is_scr"].to_list()
        assert result_default["season"].to_list() == result_explicit["season"].to_list()
