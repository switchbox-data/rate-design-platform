"""Tests for seasonal SCR exceedance and the state config that calls it."""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.data_prep.marginal_costs.methods.seasonal_scr_exceedance import main, seasonal_scr_exceedance

_REPO = Path(__file__).parent.parent
_CONFIGS = _REPO / "rate_design" / "hp_rates"

_NY_UTILITIES = ["cenhud", "coned", "nimo", "nyseg", "or", "psegli", "rge"]
_WINTER = [1]


def _allocation(state: str) -> dict:
    path = _CONFIGS / state / "config" / "marginal_costs" / "allocation.yaml"
    loaded = yaml.safe_load(path.read_text())
    assert isinstance(loaded, dict)
    return loaded


def _load(rows: list[tuple[datetime, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp": [timestamp for timestamp, _load_mw in rows],
            "load_mw": [load_mw for _timestamp, load_mw in rows],
        }
    )


def _by_timestamp(result: pl.DataFrame, column: str = "mc_total_per_kwh") -> dict[datetime, float]:
    return dict(zip(result["timestamp"].to_list(), result[column].to_list(), strict=True))


def test_weights_follow_seasonal_surplus_then_exceedance() -> None:
    """January is winter. N = 2.

    Winter loads 10, 7, 4, 1. Threshold 4. Exceedances 6 and 3.
    Summer loads 12, 8, 5, 2. Threshold 5. Exceedances 7 and 3.
    The lower threshold is 4, so the seasonal surpluses are 6 and 8.
    """
    load = _load(
        [
            (datetime(2025, 1, 1, 0), 10.0),
            (datetime(2025, 1, 1, 1), 7.0),
            (datetime(2025, 1, 1, 2), 4.0),
            (datetime(2025, 1, 1, 3), 1.0),
            (datetime(2025, 7, 1, 0), 12.0),
            (datetime(2025, 7, 1, 1), 8.0),
            (datetime(2025, 7, 1, 2), 5.0),
            (datetime(2025, 7, 1, 3), 2.0),
        ]
    )

    result = seasonal_scr_exceedance(load, 14.0, 2, _WINTER)

    assert result.columns == ["timestamp", "mc_total_per_kwh"]
    assert result.height == 8
    assert result["timestamp"].to_list() == sorted(load["timestamp"].to_list())
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(14.0)
    costs = _by_timestamp(result)
    assert costs[datetime(2025, 1, 1, 0)] == pytest.approx(4.0)
    assert costs[datetime(2025, 1, 1, 1)] == pytest.approx(2.0)
    assert costs[datetime(2025, 1, 1, 2)] == pytest.approx(0.0)
    assert costs[datetime(2025, 1, 1, 3)] == pytest.approx(0.0)
    assert costs[datetime(2025, 7, 1, 0)] == pytest.approx(5.6)
    assert costs[datetime(2025, 7, 1, 1)] == pytest.approx(2.4)
    assert costs[datetime(2025, 7, 1, 2)] == pytest.approx(0.0)
    assert costs[datetime(2025, 7, 1, 3)] == pytest.approx(0.0)


def test_cli_writes_the_seasonal_weights() -> None:
    load = _load(
        [
            (datetime(2025, 1, 1, 0), 10.0),
            (datetime(2025, 1, 1, 1), 7.0),
            (datetime(2025, 1, 1, 2), 4.0),
            (datetime(2025, 1, 1, 3), 1.0),
            (datetime(2025, 7, 1, 0), 12.0),
            (datetime(2025, 7, 1, 1), 8.0),
            (datetime(2025, 7, 1, 2), 5.0),
            (datetime(2025, 7, 1, 3), 2.0),
        ]
    )
    with tempfile.TemporaryDirectory() as directory:
        path_load = Path(directory) / "load.parquet"
        path_output = Path(directory) / "out" / "allocated.parquet"
        load.write_parquet(path_load)
        main(
            [
                "--path-load",
                str(path_load),
                "--annual-cost-kw-year",
                "14",
                "--n-hours-per-season",
                "2",
                "--winter-months",
                "1",
                "--output-column",
                "bulk_tx_cost_enduse",
                "--path-output",
                str(path_output),
            ]
        )
        result = pl.read_parquet(path_output)
    assert result.columns == ["timestamp", "bulk_tx_cost_enduse"]
    assert result.height == 8
    assert float(result["bulk_tx_cost_enduse"].sum()) == pytest.approx(14.0)
    assert result.filter(pl.col("bulk_tx_cost_enduse") > 0).height == 4


def test_tie_at_the_cutoff_drops_that_hours_weight() -> None:
    """The winter Nth hour ties the (N+1)th. That hour's exceedance is zero."""
    load = _load(
        [
            (datetime(2025, 1, 1, 0), 10.0),
            (datetime(2025, 1, 1, 1), 5.0),
            (datetime(2025, 1, 1, 2), 5.0),
            (datetime(2025, 1, 1, 3), 1.0),
            (datetime(2025, 7, 1, 0), 12.0),
            (datetime(2025, 7, 1, 1), 8.0),
            (datetime(2025, 7, 1, 2), 5.0),
            (datetime(2025, 7, 1, 3), 2.0),
        ]
    )

    result = seasonal_scr_exceedance(load, 20.0, 2, _WINTER)

    costs = _by_timestamp(result)
    # Winter surplus is 5 and summer surplus is 7, so the winter peak takes 5/12 of the cost.
    assert costs[datetime(2025, 1, 1, 0)] == pytest.approx(20.0 * 5.0 / 12.0)
    assert costs[datetime(2025, 1, 1, 1)] == pytest.approx(0.0)
    assert costs[datetime(2025, 1, 1, 2)] == pytest.approx(0.0)
    assert costs[datetime(2025, 7, 1, 0)] > 0.0
    assert costs[datetime(2025, 7, 1, 1)] > 0.0
    assert result.filter(pl.col("mc_total_per_kwh") > 0).height == 3
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(20.0)


def test_winter_months_change_which_hours_are_eligible() -> None:
    """Moving April from summer into winter drops January's second hour."""
    load = _load(
        [
            (datetime(2025, 1, 1, 0), 20.0),
            (datetime(2025, 1, 1, 1), 10.0),
            (datetime(2025, 1, 1, 2), 0.0),
            (datetime(2025, 4, 1, 0), 15.0),
            (datetime(2025, 4, 1, 1), 5.0),
            (datetime(2025, 4, 1, 2), 0.0),
            (datetime(2025, 7, 1, 0), 12.0),
            (datetime(2025, 7, 1, 1), 4.0),
            (datetime(2025, 7, 1, 2), 0.0),
        ]
    )

    january_only = seasonal_scr_exceedance(load, 35.0, 2, [1])
    january_and_april = seasonal_scr_exceedance(load, 35.0, 2, [1, 4])

    january_second = datetime(2025, 1, 1, 1)
    assert _by_timestamp(january_only)[january_second] > 0.0
    assert _by_timestamp(january_and_april)[january_second] == pytest.approx(0.0)
    assert float(january_only["mc_total_per_kwh"].sum()) == pytest.approx(35.0)
    assert float(january_and_april["mc_total_per_kwh"].sum()) == pytest.approx(35.0)


def test_same_timestamp_does_not_mark_both_rows() -> None:
    """A tied timestamp is two hours. Only the selected row gets the cost."""
    load = _load(
        [
            (datetime(2025, 1, 1, 0), 10.0),
            (datetime(2025, 1, 1, 0), 1.0),
            (datetime(2025, 1, 1, 1), 4.0),
            (datetime(2025, 7, 1, 0), 12.0),
            (datetime(2025, 7, 1, 1), 3.0),
        ]
    )

    result = seasonal_scr_exceedance(load, 9.0, 1, _WINTER)

    assert result.height == 5
    assert result.filter(pl.col("mc_total_per_kwh") > 0).height == 2
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(9.0)


def test_output_column_name_is_the_callers() -> None:
    load = _load(
        [
            (datetime(2025, 1, 1, 1), 4.0),
            (datetime(2025, 1, 1, 0), 1.0),
            (datetime(2025, 7, 1, 1), 6.0),
            (datetime(2025, 7, 1, 0), 2.0),
        ]
    )

    result = seasonal_scr_exceedance(load, 8.0, 1, _WINTER, output_column="bulk_tx_cost_enduse")

    assert result.columns == ["timestamp", "bulk_tx_cost_enduse"]
    assert result["timestamp"].to_list() == sorted(load["timestamp"].to_list())
    assert float(result["bulk_tx_cost_enduse"].sum()) == pytest.approx(8.0)
    assert result.filter(pl.col("bulk_tx_cost_enduse") > 0).height == 2


def test_shift_and_scale_of_the_load_do_not_change_the_weights() -> None:
    base = _load(
        [
            (datetime(2025, 1, 1, 0), 10.0),
            (datetime(2025, 1, 1, 1), 7.0),
            (datetime(2025, 1, 1, 2), 4.0),
            (datetime(2025, 1, 1, 3), 1.0),
            (datetime(2025, 7, 1, 0), 12.0),
            (datetime(2025, 7, 1, 1), 8.0),
            (datetime(2025, 7, 1, 2), 5.0),
            (datetime(2025, 7, 1, 3), 2.0),
        ]
    )
    shifted = base.with_columns((pl.col("load_mw") + 100.0).alias("load_mw"))
    scaled = base.with_columns((pl.col("load_mw") * 3.0).alias("load_mw"))

    original = seasonal_scr_exceedance(base, 14.0, 2, _WINTER)
    assert seasonal_scr_exceedance(shifted, 14.0, 2, _WINTER)["mc_total_per_kwh"].to_list() == pytest.approx(
        original["mc_total_per_kwh"].to_list()
    )
    assert seasonal_scr_exceedance(scaled, 14.0, 2, _WINTER)["mc_total_per_kwh"].to_list() == pytest.approx(
        original["mc_total_per_kwh"].to_list()
    )


def test_full_year_marks_forty_hours_in_each_season() -> None:
    timestamps = pl.datetime_range(
        datetime(2025, 1, 1),
        datetime(2025, 12, 31, 23),
        interval="1h",
        eager=True,
    )
    load = (
        pl.DataFrame({"timestamp": timestamps})
        .with_row_index("idx")
        .with_columns(
            (
                pl.when(pl.col("timestamp").dt.month().is_in([4, 5, 6, 7, 8, 9])).then(2000.0).otherwise(1200.0)
                + (pl.col("idx") % 24).cast(pl.Float64)
                + (pl.col("idx") * 0.0001)
            ).alias("load_mw")
        )
    )

    result = seasonal_scr_exceedance(load, 47.5, 40, [10, 11, 12, 1, 2, 3])

    assert result.height == 8760
    assert result.filter(pl.col("mc_total_per_kwh") > 0).height == 80
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(47.5)


def test_rejects_bad_inputs_and_a_flat_season() -> None:
    load = _load(
        [
            (datetime(2025, 1, 1, 0), 5.0),
            (datetime(2025, 1, 1, 1), 4.0),
            (datetime(2025, 7, 1, 0), 8.0),
            (datetime(2025, 7, 1, 1), 3.0),
        ]
    )
    flat = _load(
        [
            (datetime(2025, 1, 1, 0), 5.0),
            (datetime(2025, 1, 1, 1), 5.0),
            (datetime(2025, 7, 1, 0), 8.0),
            (datetime(2025, 7, 1, 1), 3.0),
        ]
    )
    both_flat = _load(
        [
            (datetime(2025, 1, 1, 0), 5.0),
            (datetime(2025, 1, 1, 1), 5.0),
            (datetime(2025, 7, 1, 0), 5.0),
            (datetime(2025, 7, 1, 1), 5.0),
        ]
    )
    with pytest.raises(ValueError, match="n_hours_per_season must be at least 1"):
        seasonal_scr_exceedance(load, 1.0, 0, _WINTER)
    with pytest.raises(ValueError, match="needs at least"):
        seasonal_scr_exceedance(load.head(1), 1.0, 2, _WINTER)
    with pytest.raises(ValueError, match="missing columns"):
        seasonal_scr_exceedance(load.drop("load_mw"), 1.0, 1, _WINTER)
    with pytest.raises(ValueError, match="winter_months must not be empty"):
        seasonal_scr_exceedance(load, 1.0, 1, [])
    with pytest.raises(ValueError, match=r"integers in 1\.\.12"):
        seasonal_scr_exceedance(load, 1.0, 1, [0, 1])
    with pytest.raises(ValueError, match="all 12 months"):
        seasonal_scr_exceedance(load, 1.0, 1, list(range(1, 13)))
    with pytest.raises(ValueError, match="Zero winter exceedance"):
        seasonal_scr_exceedance(flat, 1.0, 1, _WINTER)
    with pytest.raises(ValueError, match="Invalid SCR surplus"):
        seasonal_scr_exceedance(both_flat, 1.0, 1, _WINTER)


def test_state_config_names_this_method_and_its_output() -> None:
    ny = _allocation("ny")["bulk_tx"]
    assert ny["method"] == "seasonal_scr_exceedance"
    assert ny["output_column"] == "bulk_tx_cost_enduse"
    assert ny["manifest_method"] == "nyiso_scr"
    assert ny["combine"] == "sum"
    assert ny["utilities"] == _NY_UTILITIES
    assert ny["output_base"] == "s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/"
    assert ny["inputs"]["n_hours_per_season"] == 40
    winter = ny["inputs"]["winter_months"]
    assert winter["from"] == "periods_yaml"
    assert winter["path"] == "rate_design/hp_rates/ny/config/periods/{utility}.yaml"
    assert winter["default"] == [10, 11, 12, 1, 2, 3]
    components = ny["inputs"]["components"]
    assert components["from"] == "nyiso_scr_components"
    assert components["constraint_groups"] == "s3://data.sb/nyiso/bulk_tx/ny_bulk_tx_constraint_groups.csv"
    assert components["zone_mapping"] == "s3://data.sb/nyiso/zone_mapping/ny_utility_zone_mapping.csv"
    assert components["zone_loads"] == "s3://data.sb/nyiso/hourly_demand/zones/"
    assert components["load_year"] == "{load_year}"
