"""Tests for the top-N load-share method and the state configs that call it."""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.data_prep.marginal_costs.methods.top_n_load_share import main, top_n_load_share

_REPO = Path(__file__).parent.parent
_CONFIGS = _REPO / "rate_design" / "hp_rates"


def _allocation(state: str) -> dict:
    path = _CONFIGS / state / "config" / "marginal_costs" / "allocation.yaml"
    loaded = yaml.safe_load(path.read_text())
    assert isinstance(loaded, dict)
    return loaded


def test_cli_writes_load_share_of_the_top_hours() -> None:
    load = pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                datetime(2025, 1, 1),
                datetime(2025, 1, 1, 4),
                interval="1h",
                eager=True,
            ),
            "load_mw": [1.0, 4.0, 2.0, 0.0, 3.0],
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
                "10",
                "--n-hours",
                "2",
                "--path-output",
                str(path_output),
            ]
        )
        result = pl.read_parquet(path_output)
    assert result.columns == ["timestamp", "mc_total_per_kwh"]
    assert result.height == 5
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(10.0)
    nonzero = result.filter(pl.col("mc_total_per_kwh") > 0).sort("mc_total_per_kwh", descending=True)
    assert nonzero.height == 2
    assert float(nonzero["mc_total_per_kwh"][0]) == pytest.approx(10.0 * 4.0 / 7.0)
    assert float(nonzero["mc_total_per_kwh"][1]) == pytest.approx(10.0 * 3.0 / 7.0)


def test_same_timestamp_does_not_mark_both_rows() -> None:
    """A tied timestamp is two hours. Only the selected row gets the cost."""
    load = pl.DataFrame(
        {
            "timestamp": [datetime(2025, 1, 1, 0), datetime(2025, 1, 1, 0), datetime(2025, 1, 1, 1)],
            "load_mw": [10.0, 1.0, 3.0],
        }
    )

    result = top_n_load_share(load, 9.0, 1)

    assert result.filter(pl.col("mc_total_per_kwh") > 0).height == 1
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(9.0)
    assert result["timestamp"].to_list() == sorted(load["timestamp"].to_list())


def test_output_column_name_is_the_callers() -> None:
    load = pl.DataFrame(
        {
            "timestamp": [datetime(2025, 1, 1, 1), datetime(2025, 1, 1, 0)],
            "load_mw": [2.0, 1.0],
        }
    )

    result = top_n_load_share(load, 6.0, 1, output_column="bulk_tx_cost_enduse")

    assert result.columns == ["timestamp", "bulk_tx_cost_enduse"]
    assert result["timestamp"].to_list() == [datetime(2025, 1, 1, 0), datetime(2025, 1, 1, 1)]
    assert float(result["bulk_tx_cost_enduse"][1]) == pytest.approx(6.0)
    assert float(result["bulk_tx_cost_enduse"][0]) == 0.0


def test_rejects_too_few_hours_and_a_nonpositive_peak() -> None:
    load = pl.DataFrame({"timestamp": [datetime(2025, 1, 1)], "load_mw": [0.0]})
    with pytest.raises(ValueError, match="n_hours must be at least 1"):
        top_n_load_share(load, 1.0, 0)
    with pytest.raises(ValueError, match="needs at least"):
        top_n_load_share(load, 1.0, 2)
    with pytest.raises(ValueError, match="zero or negative"):
        top_n_load_share(load, 1.0, 1)
    with pytest.raises(ValueError, match="missing columns"):
        top_n_load_share(load.drop("load_mw"), 1.0, 1)


def test_state_configs_name_this_method_and_its_output() -> None:
    ri = _allocation("ri")["dist_and_sub_tx"]
    assert ri["method"] == "top_n_load_share"
    assert ri["output_column"] == "mc_total_per_kwh"
    assert ri["manifest_method"] == "pop"
    assert ri["utilities"] == ["rie"]
    assert ri["inputs"]["n_hours"] == 100
    assert ri["output_base"] == "s3://data.sb/switchbox/marginal_costs/ri/dist_and_sub_tx/"
    assert ri["inputs"]["load"]["path"] == "s3://data.sb/isone/hourly_demand/utilities/"
    assert ri["inputs"]["annual_cost_kw_year"]["path"].endswith("ri_marginal_costs_2025.csv")
    assert "bulk_tx" not in _allocation("ri")

    ny = _allocation("ny")["dist_and_sub_tx"]
    assert ny["method"] == "top_n_load_share"
    assert ny["inputs"]["n_hours"] == 100
    assert set(ny["utilities"]) == {"cenhud", "coned", "nimo", "nyseg", "or", "psegli", "rge"}
    assert ny["output_base"] == "s3://data.sb/switchbox/marginal_costs/ny/dist_and_sub_tx/"
    assert ny["inputs"]["load"]["path"] == "s3://data.sb/nyiso/hourly_demand/utilities/"
    assert "dollar_year_column" not in ny["inputs"]["annual_cost_kw_year"]
    assert "bulk_tx" not in _allocation("ny")

    md = _allocation("md")
    dist = md["dist_and_sub_tx"]
    bulk = md["bulk_tx"]
    assert dist["method"] == "top_n_load_share"
    assert dist["inputs"]["n_hours"] == 100
    assert dist["utilities"] == ["bge"]
    assert dist["output_base"] == "s3://data.sb/switchbox/marginal_costs/md/dist_and_sub_tx/"
    assert dist["inputs"]["load"]["path"] == "s3://data.sb/pjm/hourly_demand/utilities/"
    assert bulk["method"] == "top_n_load_share"
    assert bulk["output_column"] == "bulk_tx_cost_enduse"
    assert bulk["manifest_method"] == "pjm_nits_pcaf"
    assert bulk["inputs"]["n_hours"] == 150
    assert bulk["output_base"] == "s3://data.sb/switchbox/marginal_costs/md/bulk_tx/"
    assert bulk["inputs"]["annual_cost_kw_year"]["from"] == "pjm_nits"
    assert set(bulk["utilities"]) == {
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

    ct = _allocation("ct")
    dist = ct["dist_and_sub_tx"]
    bulk = ct["bulk_tx"]
    assert dist["method"] == "month_hour"
    assert dist["manifest_method"] == "month_hour"
    assert dist["output_base"] == "s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/"
    assert dist["utilities"] == ["ct_eversource"]
    assert bulk["method"] == "pop_kde"
    assert bulk["manifest_method"] == "isone_aesc_ptf_pop_kde"
    assert bulk["utilities"] == ["ct_eversource", "ct_ui"]
    assert bulk["output_base"] == "s3://data.sb/switchbox/marginal_costs/ct/bulk_tx/"
    for name, load_from, load_source in (
        ("pop_occ863", "parquet", "occ863att3"),
        ("pop_isone_zone", "hive_load", "isone_zone"),
    ):
        alternative = dist["alternatives"][name]
        assert alternative["method"] == "top_n_load_share"
        assert alternative["manifest_method"] == "pop"
        assert alternative["inputs"]["n_hours"] == 100
        assert alternative["inputs"]["annual_cost_kw_year"]["target_dollar_year"] == 2026
        assert alternative["inputs"]["load"]["from"] == load_from
        assert alternative["inputs"]["load"]["load_source"] == load_source
