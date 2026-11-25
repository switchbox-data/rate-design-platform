"""Tests for the top-N exceedance method and the state configs that call it."""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.data_prep.marginal_costs.methods.top_n_exceedance import main, top_n_exceedance

_REPO = Path(__file__).parent.parent
_CONFIGS = _REPO / "rate_design" / "hp_rates"

_NE_ZONES = ["CT", "ME", "NEMA", "NH", "RI", "SEMA", "VT", "WCMA"]


def _allocation(state: str) -> dict:
    path = _CONFIGS / state / "config" / "marginal_costs" / "allocation.yaml"
    loaded = yaml.safe_load(path.read_text())
    assert isinstance(loaded, dict)
    return loaded


def test_weights_are_exceedance_above_the_threshold() -> None:
    """Loads 10, 7, 6, 4, 1 and N = 3. Threshold is 4. Exceedances are 6, 3, 2."""
    load = pl.DataFrame(
        {
            "timestamp": [datetime(2025, 1, 1, hour) for hour in range(5)],
            "load_mw": [10.0, 7.0, 6.0, 4.0, 1.0],
        }
    )

    result = top_n_exceedance(load, 11.0, 3)

    assert result.columns == ["timestamp", "mc_total_per_kwh"]
    assert result.height == 5
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(11.0)
    by_time = result.sort("timestamp")["mc_total_per_kwh"].to_list()
    assert by_time == pytest.approx([6.0, 3.0, 2.0, 0.0, 0.0])


def test_cli_writes_exceedance_of_the_top_hours() -> None:
    load = pl.DataFrame(
        {
            "timestamp": [datetime(2025, 1, 1, hour) for hour in range(5)],
            "load_mw": [10.0, 7.0, 6.0, 4.0, 1.0],
        }
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
                "11",
                "--n-hours",
                "3",
                "--output-column",
                "bulk_tx_cost_enduse",
                "--path-output",
                str(path_output),
            ]
        )
        result = pl.read_parquet(path_output)
    assert result.columns == ["timestamp", "bulk_tx_cost_enduse"]
    assert result.height == 5
    assert float(result["bulk_tx_cost_enduse"].sum()) == pytest.approx(11.0)
    nonzero = result.filter(pl.col("bulk_tx_cost_enduse") > 0)
    assert nonzero.height == 3


def test_tie_at_the_cutoff_keeps_n_eligible_rows() -> None:
    """The Nth hour ties with hours below it. Those extra hours stay at zero."""
    load = pl.DataFrame(
        {
            "timestamp": [datetime(2025, 1, 1, hour) for hour in range(5)],
            "load_mw": [10.0, 8.0, 5.0, 5.0, 1.0],
        }
    )

    result = top_n_exceedance(load, 20.0, 3)

    nonzero = result.filter(pl.col("mc_total_per_kwh") > 0)
    assert nonzero.height == 3
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(20.0)
    # Threshold is 1. Exceedances of the eligible rows are 9, 7, and 4.
    assert sorted(nonzero["mc_total_per_kwh"].to_list()) == pytest.approx([4.0, 7.0, 9.0])


def test_zero_load_at_the_cutoff_drops_that_hours_weight() -> None:
    """Threshold falls back to 0, so eligible hours sitting on 0 get no cost."""
    load = pl.DataFrame(
        {
            "timestamp": [datetime(2025, 1, 1, hour) for hour in range(4)],
            "load_mw": [5.0, 0.0, 0.0, 0.0],
        }
    )

    result = top_n_exceedance(load, 5.0, 3)

    assert result.height == 4
    nonzero = result.filter(pl.col("mc_total_per_kwh") > 0)
    assert nonzero.height == 1
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(5.0)


def test_same_timestamp_does_not_mark_both_rows() -> None:
    """A tied timestamp is two hours. Only the selected row gets the cost."""
    load = pl.DataFrame(
        {
            "timestamp": [datetime(2025, 1, 1, 0), datetime(2025, 1, 1, 0), datetime(2025, 1, 1, 1)],
            "load_mw": [10.0, 1.0, 3.0],
        }
    )

    result = top_n_exceedance(load, 9.0, 1)

    assert result.filter(pl.col("mc_total_per_kwh") > 0).height == 1
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(9.0)
    assert result["timestamp"].to_list() == sorted(load["timestamp"].to_list())


def test_output_column_name_is_the_callers() -> None:
    load = pl.DataFrame(
        {
            "timestamp": [datetime(2025, 1, 1, 1), datetime(2025, 1, 1, 0)],
            "load_mw": [4.0, 1.0],
        }
    )

    result = top_n_exceedance(load, 6.0, 1, output_column="bulk_tx_cost_enduse")

    assert result.columns == ["timestamp", "bulk_tx_cost_enduse"]
    assert result["timestamp"].to_list() == [datetime(2025, 1, 1, 0), datetime(2025, 1, 1, 1)]
    assert float(result["bulk_tx_cost_enduse"][1]) == pytest.approx(6.0)
    assert float(result["bulk_tx_cost_enduse"][0]) == 0.0


def test_shift_and_scale_of_the_load_do_not_change_the_weights() -> None:
    base = pl.DataFrame(
        {
            "timestamp": [datetime(2025, 1, 1, hour) for hour in range(5)],
            "load_mw": [10.0, 7.0, 6.0, 4.0, 1.0],
        }
    )
    shifted = base.with_columns((pl.col("load_mw") + 100.0).alias("load_mw"))
    scaled = base.with_columns((pl.col("load_mw") * 3.0).alias("load_mw"))

    original = top_n_exceedance(base, 11.0, 3)
    assert top_n_exceedance(shifted, 11.0, 3)["mc_total_per_kwh"].to_list() == pytest.approx(
        original["mc_total_per_kwh"].to_list()
    )
    assert top_n_exceedance(scaled, 11.0, 3)["mc_total_per_kwh"].to_list() == pytest.approx(
        original["mc_total_per_kwh"].to_list()
    )


def test_rejects_too_few_hours_and_a_nonpositive_exceedance() -> None:
    load = pl.DataFrame({"timestamp": [datetime(2025, 1, 1)], "load_mw": [0.0]})
    with pytest.raises(ValueError, match="n_hours must be at least 1"):
        top_n_exceedance(load, 1.0, 0)
    with pytest.raises(ValueError, match="needs at least"):
        top_n_exceedance(load, 1.0, 2)
    with pytest.raises(ValueError, match="zero or negative"):
        top_n_exceedance(load, 1.0, 1)
    with pytest.raises(ValueError, match="missing columns"):
        top_n_exceedance(load.drop("load_mw"), 1.0, 1)


def test_state_configs_name_this_method_and_its_output() -> None:
    ri = _allocation("ri")["bulk_tx"]
    assert ri["method"] == "top_n_exceedance"
    assert ri["output_column"] == "bulk_tx_cost_enduse"
    assert ri["manifest_method"] == "isone_aesc_ptf"
    assert ri["utilities"] == ["rie"]
    assert ri["inputs"]["n_hours"] == 100
    assert ri["output_base"] == "s3://data.sb/switchbox/marginal_costs/ri/bulk_tx/"
    assert ri["inputs"]["annual_cost_kw_year"] == {"from": "literal", "value": 69.0}
    load = ri["inputs"]["load"]
    assert load["from"] == "zone_sum_load"
    assert load["path"] == "s3://data.sb/isone/hourly_demand/zones/"
    assert load["zones"] == _NE_ZONES
    assert load["year"] == "{load_year}"

    ct = _allocation("ct")["bulk_tx"]
    assert ct["method"] == "pop_kde"
    alternative = ct["alternatives"]["isone_aesc_ptf"]
    assert alternative["method"] == "top_n_exceedance"
    assert alternative["manifest_method"] == "isone_aesc_ptf"
    assert alternative["utilities"] == ["ct_eversource", "ct_ui"]
    assert alternative["inputs"]["n_hours"] == 100
    assert alternative["inputs"]["annual_cost_kw_year"] == {"from": "literal", "value": 69.0}
    assert alternative["inputs"]["load"]["from"] == "zone_sum_load"
    assert alternative["inputs"]["load"]["zones"] == _NE_ZONES
    assert alternative["inputs"]["load"]["path"] == "s3://data.sb/isone/hourly_demand/zones/"
