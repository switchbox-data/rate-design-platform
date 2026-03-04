"""Tests for NY bulk transmission marginal cost generation (constraint-group engine)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from utils.pre.generate_bulk_tx_mc import (
    DEFAULT_SCR_WINTER_MONTHS,
    aggregate_paying_locality_hourly_signals_from_constraint_groups,
    allocate_bulk_tx_to_hours,
    build_nested_locality_load_profiles,
    compute_nested_locality_scr_weights,
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
        "A": 0.8,
        "B": 0.9,
        "C": 1.0,
        "D": 0.95,
        "E": 0.85,
        "F": 0.9,
        "G": 0.7,
        "H": 0.75,
        "I": 0.8,
        "J": 0.65,
        "K": 0.6,
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
            "constraint_group_cost_enduse": [1.0, 2.0, 3.0, 5.0, 7.0, 9.0, 2.0, 2.0, 2.0],
        }
    )


def _make_mapping() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "utility": ["u1", "u1", "u2", "u2"],
            "gen_capacity_zone": ["ROS", "LHV", "NYC", "LI"],
            "capacity_weight": [0.6, 0.4, 0.87, 0.13],
            "load_zone_letter": ["A", "G", "J", "K"],
            "lbmp_zone_name": ["WEST", "HUD", "NYC", "LI"],
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
        profiles = build_nested_locality_load_profiles(zone_loads, ["NYCA", "LHV", "NYC", "LI"])
        assert set(profiles) == {"NYCA", "LHV", "NYC", "LI"}
        assert profiles["NYCA"].height == 8760
        assert float(profiles["NYCA"]["load_mw"].mean()) > float(profiles["NYC"]["load_mw"].mean())


class TestAggregationAndUtilityResolution:
    def test_aggregate_paying_locality_hourly_signals(self) -> None:
        hourly = _make_constraint_group_hourly()
        locality_hourly = aggregate_paying_locality_hourly_signals_from_constraint_groups(hourly)

        assert set(locality_hourly) == {"ROS", "LHV", "NYC", "LI"}
        # LHV receives cg1 and cg2 => mean at first hour = (1 + 5) / 2
        lhv_first = float(locality_hourly["LHV"].sort("timestamp")["bulk_tx_cost_enduse"][0])
        assert abs(lhv_first - 3.0) < 1e-9

    def test_resolve_utility_paying_locality_signal(self) -> None:
        hourly = _make_constraint_group_hourly()
        locality_hourly = aggregate_paying_locality_hourly_signals_from_constraint_groups(hourly)
        mapping = _make_mapping()

        utility_hourly = resolve_utility_paying_locality_signal(mapping, "u1", locality_hourly)
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
