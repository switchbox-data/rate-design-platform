"""Tests for NY bulk transmission marginal cost generation (constraint-group engine)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import cast

import polars as pl
import pytest

from utils.pre.generate_bulk_tx_mc import (
    DEFAULT_SCR_WINTER_MONTHS,
    aggregate_paying_locality_hourly_signals_from_constraint_groups,
    allocate_bulk_tx_to_hours,
    build_nested_locality_load_profiles,
    compute_nested_locality_scr_weights,
    compute_paying_locality_costs,
    compute_utility_bulk_tx_signal,
    identify_scr_hours,
    load_constraint_group_table,
    prepare_output,
    resolve_utility_paying_locality_signal,
)


def _make_hourly_load_profile(year: int = 2025) -> pl.DataFrame:
    timestamps = pl.datetime_range(
        datetime(year, 1, 1, 0, 0, 0),
        datetime(year, 12, 31, 23, 0, 0),
        interval="1h",
        eager=True,
    )
    df = pl.DataFrame({"timestamp": timestamps}).with_row_index("idx")
    # Summer higher than winter; deterministic tie-break trend.
    summer = {4, 5, 6, 7, 8, 9}
    return df.with_columns(
        (
            pl.when(pl.col("timestamp").dt.month().is_in(list(summer)))
            .then(2000.0)
            .otherwise(1200.0)
            + (pl.col("idx") % 24).cast(pl.Float64)
            + (pl.col("idx") * 0.0001)
        ).alias("load_mw")
    ).select("timestamp", "load_mw")


def _make_zone_loads(year: int = 2025) -> pl.DataFrame:
    base = _make_hourly_load_profile(year)
    zone_scale = {
        "WEST": 0.8,
        "GENESE": 0.9,
        "CENTRAL": 1.0,
        "NORTH": 0.95,
        "MHK_VL": 0.85,
        "CAPITL": 0.9,
        "HUD_VL": 0.7,
        "MILLWD": 0.75,
        "DUNWOD": 0.8,
        "N.Y.C.": 0.65,
        "LONGIL": 0.6,
    }
    frames: list[pl.DataFrame] = []
    for zone, scale in zone_scale.items():
        frames.append(
            base.with_columns(
                pl.lit(zone).alias("zone"),
                (pl.col("load_mw") * scale).alias("load_mw"),
            ).select("timestamp", "zone", "load_mw")
        )
    return pl.concat(frames)


def _make_constraint_group_hourly() -> pl.DataFrame:
    hours = pl.datetime_range(
        datetime(2025, 1, 1, 0, 0, 0),
        datetime(2025, 1, 1, 2, 0, 0),
        interval="1h",
        eager=True,
    )
    return pl.DataFrame(
        {
            "timestamp": hours.to_list() * 3,
            "constraint_group": ["cg1"] * 3 + ["cg2"] * 3 + ["cg3"] * 3,
            "tightest_nested_locality": ["NYCA"] * 9,
            "paying_localities": ["ROS|LHV"] * 3 + ["LHV"] * 3 + ["NYC|LI"] * 3,
            "constraint_group_cost_enduse": [
                1.0,
                2.0,
                3.0,
                5.0,
                7.0,
                9.0,
                2.0,
                2.0,
                2.0,
            ],
        }
    )


def _make_mapping() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "utility": ["u1", "u1", "u2", "u2"],
            "gen_capacity_zone": ["ROS", "LHV", "NYC", "LI"],
            "capacity_weight": [0.6, 0.4, 0.87, 0.13],
            "load_zone_letter": ["A", "G", "J", "K"],
            "lbmp_zone_name": ["WEST", "HUD_VL", "N.Y.C.", "LONGIL"],
            "icap_locality": ["NYCA", "GHIJ", "NYC", "LI"],
        }
    )


class TestConstraintGroupTable:
    def test_load_constraint_group_table(self, tmp_path: Path) -> None:
        path_csv = tmp_path / "constraint_groups.csv"
        pl.DataFrame(
            {
                "nested_localities_str": ["NYCA", "LHV|NYCA"],
                "constraint_group": ["nimo_mcos", "li_export"],
                "v_constraint_group_kw_yr": [10.0, 20.0],
                "tightest_nested_locality": ["NYCA", "LHV"],
                "paying_localities": ["ROS", "LHV|LI"],
            }
        ).write_csv(path_csv)

        loaded = load_constraint_group_table(str(path_csv))
        assert loaded.height == 2
        assert set(loaded.columns) == {
            "nested_localities_str",
            "constraint_group",
            "v_constraint_group_kw_yr",
            "tightest_nested_locality",
            "paying_localities",
        }


class TestScrAllocation:
    def test_identify_scr_hours_has_80_hours(self) -> None:
        load_df = _make_hourly_load_profile()
        result = identify_scr_hours(load_df, n_hours_per_season=40)
        assert result.filter(pl.col("is_scr")).height == 80

    def test_scr_weights_sum_to_one(self) -> None:
        load_df = _make_hourly_load_profile()
        weights = compute_nested_locality_scr_weights(
            load_df,
            n_hours_per_season=40,
            winter_months=list(DEFAULT_SCR_WINTER_MONTHS),
        )
        assert abs(float(weights["scr_weight"].sum()) - 1.0) < 1e-4

    def test_1kw_recovery(self) -> None:
        load_df = _make_hourly_load_profile()
        with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(with_scr, v_constraint_group_kw_yr=47.5)
        assert abs(float(allocated["bulk_tx_cost_enduse"].sum()) - 47.5) < 1e-3


class TestLocalityProfiles:
    def test_build_nested_locality_profiles(self) -> None:
        zone_loads = _make_zone_loads()
        profiles = build_nested_locality_load_profiles(
            zone_loads, ["NYCA", "LHV", "NYC", "LI"]
        )
        assert set(profiles) == {"NYCA", "LHV", "NYC", "LI"}
        assert profiles["NYCA"].height == 8760
        nyca_avg = cast(float, profiles["NYCA"]["load_mw"].mean())
        nyc_avg = cast(float, profiles["NYC"]["load_mw"].mean())
        assert nyca_avg > nyc_avg


class TestAggregationAndUtilityResolution:
    def test_aggregate_paying_locality_hourly_signals(self) -> None:
        hourly = _make_constraint_group_hourly()
        locality_hourly = (
            aggregate_paying_locality_hourly_signals_from_constraint_groups(hourly)
        )

        assert set(locality_hourly) == {"ROS", "LHV", "NYC", "LI"}
        # LHV receives cg1 and cg2 => mean at first hour = (1 + 5) / 2
        lhv_first = float(
            locality_hourly["LHV"].sort("timestamp")["bulk_tx_cost_enduse"][0]
        )
        assert abs(lhv_first - 3.0) < 1e-9

    def test_resolve_utility_paying_locality_signal(self) -> None:
        hourly = _make_constraint_group_hourly()
        locality_hourly = (
            aggregate_paying_locality_hourly_signals_from_constraint_groups(hourly)
        )
        mapping = _make_mapping()

        utility_hourly = resolve_utility_paying_locality_signal(
            mapping, "u1", locality_hourly
        )
        assert utility_hourly.height == 3
        # u1 = 0.6*ROS + 0.4*LHV; first hour: 0.6*1 + 0.4*3 = 1.8
        first = float(utility_hourly.sort("timestamp")["bulk_tx_cost_enduse"][0])
        assert abs(first - 1.8) < 1e-9


class TestPrepareOutput:
    def test_prepare_output_8760(self) -> None:
        load_df = _make_hourly_load_profile()
        with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(with_scr, v_constraint_group_kw_yr=30.0)
        output = prepare_output(allocated, year=2025)

        assert output.height == 8760
        assert output.filter(pl.col("bulk_tx_cost_enduse").is_null()).height == 0


class TestTimestampRemap:
    def test_bulk_tx_remap_preserves_allocation(self) -> None:
        """Remapping 2018->2025 timestamps preserves ordinal positions and totals."""
        load_df = _make_hourly_load_profile(year=2018)
        with_scr = identify_scr_hours(load_df, n_hours_per_season=40)
        allocated = allocate_bulk_tx_to_hours(with_scr, v_constraint_group_kw_yr=30.0)

        remapped = allocated.with_columns(
            pl.col("timestamp").dt.offset_by(f"{2025 - 2018}y")
        )

        years = remapped["timestamp"].dt.year().unique().to_list()
        assert years == [2025]

        nonzero_orig = allocated.filter(pl.col("bulk_tx_cost_enduse") > 0)
        nonzero_remap = remapped.filter(pl.col("bulk_tx_cost_enduse") > 0)
        assert nonzero_remap.height == nonzero_orig.height

        assert float(remapped["bulk_tx_cost_enduse"].sum()) == pytest.approx(
            float(allocated["bulk_tx_cost_enduse"].sum())
        )

        def ordinals(df: pl.DataFrame) -> list[tuple[int, int, int]]:
            ts = df.filter(pl.col("bulk_tx_cost_enduse") > 0).sort("timestamp")[
                "timestamp"
            ]
            return [(t.month, t.day, t.hour) for t in ts.to_list()]

        assert ordinals(allocated) == ordinals(remapped)


class TestPayingLocalityCosts:
    def test_compute_paying_locality_costs_returns_mean(self) -> None:
        """Test that compute_paying_locality_costs returns correct mean per locality."""
        constraint_group_df = pl.DataFrame(
            {
                "constraint_group": ["cg1", "cg2", "cg3", "cg4"],
                "v_constraint_group_kw_yr": [10.0, 20.0, 30.0, 40.0],
                "paying_localities": ["ROS", "ROS|LHV", "LHV", "NYC"],
                "tightest_nested_locality": ["NYCA", "NYCA", "LHV", "NYC"],
                "nested_localities_str": ["NYCA", "NYCA|LHV", "LHV", "NYC"],
            }
        )

        costs = compute_paying_locality_costs(constraint_group_df)

        # ROS: cg1 (10.0), cg2 (20.0) => mean = 15.0
        assert abs(costs["ROS"] - 15.0) < 1e-9
        # LHV: cg2 (20.0), cg3 (30.0) => mean = 25.0
        assert abs(costs["LHV"] - 25.0) < 1e-9
        # NYC: cg4 (40.0) => mean = 40.0
        assert abs(costs["NYC"] - 40.0) < 1e-9
        # LI: not present
        assert "LI" not in costs

    def test_compute_paying_locality_costs_single_locality(self) -> None:
        """Test with constraint groups that all pay to the same locality."""
        constraint_group_df = pl.DataFrame(
            {
                "constraint_group": ["cg1", "cg2"],
                "v_constraint_group_kw_yr": [50.0, 100.0],
                "paying_localities": ["ROS", "ROS"],
                "tightest_nested_locality": ["NYCA", "NYCA"],
                "nested_localities_str": ["NYCA", "NYCA"],
            }
        )

        costs = compute_paying_locality_costs(constraint_group_df)

        # ROS: mean of [50.0, 100.0] = 75.0
        assert abs(costs["ROS"] - 75.0) < 1e-9
        assert len(costs) == 1


class TestUtilityBulkTxSignal:
    def test_single_icap_locality_produces_80_hours(self) -> None:
        """Test that single-ICAP-locality utility produces exactly 80 non-zero hours."""
        # Single ICAP locality utility (e.g., nimo: NYCA -> ROS)
        utility_icap_rows = pl.DataFrame(
            {
                "icap_locality": ["NYCA"],
                "gen_capacity_zone": ["ROS"],
                "capacity_weight": [1.0],
            }
        )

        paying_locality_costs = {"ROS": 100.0}

        # Build NYCA load profile
        zone_loads = _make_zone_loads()
        locality_profiles = build_nested_locality_load_profiles(zone_loads, ["NYCA"])

        utility_hourly = compute_utility_bulk_tx_signal(
            utility_icap_rows,
            paying_locality_costs,
            locality_profiles,
            n_scr=40,
            winter_months=list(DEFAULT_SCR_WINTER_MONTHS),
        )

        # Should have exactly 80 non-zero hours (40 per season)
        nonzero = utility_hourly.filter(pl.col("bulk_tx_cost_enduse") > 0)
        assert nonzero.height == 80

        # Total cost should equal the paying locality cost
        total_cost = float(utility_hourly["bulk_tx_cost_enduse"].sum())
        assert abs(total_cost - 100.0) < 1e-3

    def test_two_icap_localities_produces_at_most_160_hours(self) -> None:
        """Test coned-style utility (two ICAP localities) produces ≤160 non-zero hours."""
        # Coned-style: NYC (0.87) + GHIJ/LHV (0.13)
        utility_icap_rows = pl.DataFrame(
            {
                "icap_locality": ["NYC", "GHIJ"],
                "gen_capacity_zone": ["NYC", "LHV"],
                "capacity_weight": [0.87, 0.13],
            }
        )

        paying_locality_costs = {"NYC": 200.0, "LHV": 150.0}

        # Build both NYC and LHV load profiles
        zone_loads = _make_zone_loads()
        locality_profiles = build_nested_locality_load_profiles(
            zone_loads, ["NYC", "LHV"]
        )

        utility_hourly = compute_utility_bulk_tx_signal(
            utility_icap_rows,
            paying_locality_costs,
            locality_profiles,
            n_scr=40,
            winter_months=list(DEFAULT_SCR_WINTER_MONTHS),
        )

        # Should have at most 160 non-zero hours (80 from NYC + 80 from LHV, with overlap)
        nonzero = utility_hourly.filter(pl.col("bulk_tx_cost_enduse") > 0)
        assert nonzero.height <= 160
        assert nonzero.height >= 80  # At least one full set

        # Total cost should equal weighted sum: 0.87 * 200.0 + 0.13 * 150.0 = 174.0 + 19.5 = 193.5
        total_cost = float(utility_hourly["bulk_tx_cost_enduse"].sum())
        expected_total = 0.87 * 200.0 + 0.13 * 150.0
        assert abs(total_cost - expected_total) < 1e-3

    def test_utility_bulk_tx_signal_handles_missing_locality(self) -> None:
        """Test that missing paying locality raises appropriate error."""
        utility_icap_rows = pl.DataFrame(
            {
                "icap_locality": ["NYCA"],
                "gen_capacity_zone": ["ROS"],
                "capacity_weight": [1.0],
            }
        )

        # Missing ROS in costs
        paying_locality_costs = {"LHV": 100.0}

        zone_loads = _make_zone_loads()
        locality_profiles = build_nested_locality_load_profiles(zone_loads, ["NYCA"])

        with pytest.raises(
            ValueError, match="No cost available for paying locality ROS"
        ):
            compute_utility_bulk_tx_signal(
                utility_icap_rows,
                paying_locality_costs,
                locality_profiles,
                n_scr=40,
                winter_months=list(DEFAULT_SCR_WINTER_MONTHS),
            )


class TestErrors:
    def test_invalid_paying_locality_raises(self) -> None:
        bad = _make_constraint_group_hourly().with_columns(
            pl.when(pl.col("constraint_group") == "cg3")
            .then(pl.lit("BAD"))
            .otherwise(pl.col("paying_localities"))
            .alias("paying_localities")
        )
        with pytest.raises(ValueError, match="Invalid paying_locality"):
            aggregate_paying_locality_hourly_signals_from_constraint_groups(bad)
