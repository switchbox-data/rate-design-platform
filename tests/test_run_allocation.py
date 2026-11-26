"""Tests for the allocation runner: config, dispatch, checks, and writing."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.data_prep.marginal_costs.mc_manifest import UnpublishedCodeError
from utils.data_prep.marginal_costs.run_allocation import (
    METHODS,
    run,
    select_block,
    substitute,
    utilities_for,
)
from utils.data_prep.marginal_costs.supply_utils import build_cairo_8760_timestamps


def _clock(year: int, load: float = 10.0) -> pl.DataFrame:
    return build_cairo_8760_timestamps(year).with_columns(
        pl.col("timestamp").cast(pl.Datetime("us")),
        (pl.int_range(0, 8760).cast(pl.Float64) + load).alias("load_mw"),
    )


def _write_hive(base: Path, utility: str, year: int, load: float = 10.0) -> None:
    partition = base / f"utility={utility}" / f"year={year}"
    partition.mkdir(parents=True)
    _clock(year, load).write_parquet(partition / "data.parquet")


def _write_config(path: Path, document: dict) -> Path:
    path.write_text(yaml.safe_dump(document))
    return path


def _dist_pop(load_base: Path) -> dict:
    return {
        "dist_and_sub_tx": {
            "method": "top_n_load_share",
            "manifest_method": "pop",
            "output_column": "mc_total_per_kwh",
            "output_base": "s3://bucket/dist_and_sub_tx/",
            "utilities": ["acme"],
            "inputs": {
                "n_hours": 10,
                "annual_cost_kw_year": 100.0,
                "load": {"from": "hive_load", "path": str(load_base), "load_year": "{load_year}"},
            },
        }
    }


def test_substitute_turns_a_whole_string_integer_into_an_int() -> None:
    rendered = substitute(
        {"year": "{year}", "name": "ct_{year}", "nested": ["{utility}"]},
        {"utility": "acme", "year": 2025, "load_year": 2024},
    )
    assert rendered == {"year": 2025, "name": "ct_2025", "nested": ["acme"]}
    with pytest.raises(KeyError, match="season"):
        substitute("{season}", {"year": 2025})


def test_select_block_variant_utility_and_unknowns() -> None:
    document = {
        "bulk_tx": {
            "method": "top_n_exceedance",
            "output_column": "bulk_tx_cost_enduse",
            "output_base": "s3://bucket/bulk_tx/",
            "utilities": ["ri"],
            "inputs": {},
            "alternatives": {
                "month_hour": {
                    "method": "month_hour",
                    "utilities": ["ct_eversource"],
                    "inputs": {"include_holidays": True},
                }
            },
        }
    }

    default = select_block(document, "bulk_tx", "top_n_exceedance", "ri")
    assert default["method"] == "top_n_exceedance"

    alternative = select_block(document, "bulk_tx", "month_hour", "ct_eversource")
    assert alternative["output_column"] == "bulk_tx_cost_enduse"
    assert alternative["output_base"] == "s3://bucket/bulk_tx/"
    assert alternative["utilities"] == ["ct_eversource"]

    with pytest.raises(ValueError, match="not in"):
        select_block(document, "bulk_tx", None, "missing")
    with pytest.raises(ValueError, match="Unknown variant"):
        select_block(document, "bulk_tx", "pop_kde", None)
    with pytest.raises(ValueError, match="no 'supply'"):
        select_block(document, "supply", None, None)


def test_top_n_load_share_runs_end_to_end(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write_hive(tmp_path / "loads", "acme", 2025)
    config = _write_config(tmp_path / "allocation.yaml", _dist_pop(tmp_path / "loads"))

    result = run(
        config,
        "dist_and_sub_tx",
        "acme",
        2025,
        storage_options={},
        path_local_base=str(tmp_path / "out"),
    )

    assert result.height == 8760
    assert result.columns == ["timestamp", "mc_total_per_kwh"]
    assert float(result["mc_total_per_kwh"].sum()) == pytest.approx(100.0)
    written = pl.read_parquet(tmp_path / "out" / "utility=acme" / "year=2025" / "data.parquet")
    assert written.columns == ["timestamp", "utility", "year", "mc_total_per_kwh"]
    assert "pop_loads_utility_10h_load2025_2025usd" in capsys.readouterr().out


def test_each_method_recovers_its_annual_cost(tmp_path: Path) -> None:
    loads = tmp_path / "loads"
    _write_hive(loads, "acme", 2025)
    tables = tmp_path / "tables"
    tables.mkdir()
    rows = [{"month": month, "hour": hour, "mc_total_per_kwh": 1.0} for month in range(1, 13) for hour in range(24)]
    for name in ("weekday_secondary.parquet", "weekend_secondary.parquet"):
        pl.DataFrame(rows).write_parquet(tables / name)

    cases = [
        (
            "bulk_tx",
            {
                "method": "top_n_exceedance",
                "manifest_method": "isone_aesc_ptf",
                "output_column": "bulk_tx_cost_enduse",
                "output_base": "s3://bucket/bulk_tx/",
                "utilities": ["acme"],
                "inputs": {
                    "n_hours": 5,
                    "annual_cost_kw_year": 69.0,
                    "load": {"from": "hive_load", "path": str(loads), "load_year": "{load_year}"},
                },
            },
            "bulk_tx_cost_enduse",
            69.0,
        ),
        (
            "bulk_tx",
            {
                "method": "pop_kde",
                "manifest_method": "isone_aesc_ptf_pop_kde",
                "output_column": "bulk_tx_cost_enduse",
                "output_base": "s3://bucket/bulk_tx/",
                "utilities": ["acme"],
                "inputs": {
                    "year": "{year}",
                    "calendar_year": "{year}",
                    "annual_cost_kw_year": 69.0,
                    "load": {"from": "hive_load", "path": str(loads), "load_year": "{load_year}"},
                },
            },
            "bulk_tx_cost_enduse",
            69.0,
        ),
        (
            "dist_and_sub_tx",
            {
                "method": "month_hour",
                "manifest_method": "month_hour",
                "output_column": "mc_total_per_kwh",
                "output_base": "s3://bucket/dist_and_sub_tx/",
                "utilities": ["acme"],
                "inputs": {
                    "year": "{year}",
                    "calendar_year": "{year}",
                    "include_holidays": False,
                    "table_kind": "rates",
                    "annual_cost_kw_year": 40.0,
                    "weekday": {"from": "parquet", "path": str(tables / "weekday_secondary.parquet")},
                    "weekend": {"from": "parquet", "path": str(tables / "weekend_secondary.parquet")},
                },
            },
            "mc_total_per_kwh",
            40.0,
        ),
    ]
    for component, block, column, expected in cases:
        config = _write_config(tmp_path / f"{block['method']}.yaml", {component: block})
        result = run(config, component, "acme", 2025, storage_options={})
        assert result.columns == ["timestamp", column]
        assert float(result[column].sum()) == pytest.approx(expected)


def test_combine_sum_adds_component_costs(tmp_path: Path) -> None:
    year = 2025
    zone_base = tmp_path / "zones"
    partition = zone_base / "zone=LONGIL" / f"year={year}"
    partition.mkdir(parents=True)
    _clock(year).write_parquet(partition / "data.parquet")
    groups = tmp_path / "groups.csv"
    groups.write_text(
        "nested_localities_str,constraint_group,v_constraint_group_kw_yr,"
        "tightest_nested_locality,paying_localities\n"
        "LI,a,40,LI,LI\n"
    )
    mapping = tmp_path / "mapping.csv"
    mapping.write_text(
        "utility,load_zone_letter,lbmp_zone_name,icap_locality,gen_capacity_zone,capacity_weight\n"
        "toy,K,LONGIL,LI,LI,1\n"
    )
    config = _write_config(
        tmp_path / "allocation.yaml",
        {
            "bulk_tx": {
                "method": "seasonal_scr_exceedance",
                "manifest_method": "nyiso_scr",
                "combine": "sum",
                "output_column": "bulk_tx_cost_enduse",
                "output_base": "s3://bucket/bulk_tx/",
                "utilities": ["toy"],
                "inputs": {
                    "n_hours_per_season": 4,
                    "winter_months": [1, 2, 3, 10, 11, 12],
                    "components": {
                        "from": "nyiso_scr_components",
                        "constraint_groups": str(groups),
                        "zone_mapping": str(mapping),
                        "zone_loads": str(zone_base),
                        "load_year": "{load_year}",
                    },
                },
            }
        },
    )

    result = run(config, "bulk_tx", "toy", year, storage_options={})

    assert float(result["bulk_tx_cost_enduse"].sum()) == pytest.approx(40.0)


def test_unknown_method_and_annual_sum_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_hive(tmp_path / "loads", "acme", 2025)
    config = _write_config(tmp_path / "allocation.yaml", _dist_pop(tmp_path / "loads"))
    document = yaml.safe_load(config.read_text())
    document["dist_and_sub_tx"]["method"] = "not_a_method"
    config.write_text(yaml.safe_dump(document))
    with pytest.raises(ValueError, match="Unknown method"):
        run(config, "dist_and_sub_tx", "acme", 2025, storage_options={})

    config.write_text(yaml.safe_dump(_dist_pop(tmp_path / "loads")))

    def zeros(load: pl.DataFrame, annual_cost_kw_year: float, n_hours: int, *, output_column: str) -> pl.DataFrame:
        del annual_cost_kw_year, n_hours
        return load.select("timestamp", pl.lit(0.0).alias(output_column))

    monkeypatch.setitem(METHODS, "top_n_load_share", zeros)
    with pytest.raises(ValueError, match="Annual sum"):
        run(config, "dist_and_sub_tx", "acme", 2025, storage_options={})


def test_load_year_changes_the_output_base(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write_hive(tmp_path / "loads", "acme", 2018)
    config = _write_config(tmp_path / "allocation.yaml", _dist_pop(tmp_path / "loads"))

    run(config, "dist_and_sub_tx", "acme", 2025, load_year=2018, storage_options={})

    assert "dist_and_sub_tx_load2018" in capsys.readouterr().out


def test_list_utilities_follows_the_variant(tmp_path: Path) -> None:
    config = _write_config(
        tmp_path / "allocation.yaml",
        {
            "dist_and_sub_tx": {
                "method": "top_n_load_share",
                "utilities": ["one", "two"],
                "alternatives": {"month_hour": {"method": "month_hour", "utilities": ["only"]}},
            }
        },
    )
    assert utilities_for(config, "dist_and_sub_tx") == ["one", "two"]
    assert utilities_for(config, "dist_and_sub_tx", "month_hour") == ["only"]


def test_upload_refuses_unpublished_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_hive(tmp_path / "loads", "acme", 2025)
    config = _write_config(tmp_path / "allocation.yaml", _dist_pop(tmp_path / "loads"))

    def refuse() -> None:
        raise UnpublishedCodeError("not published")

    def unused(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("inputs should not be resolved")

    monkeypatch.setattr(
        "utils.data_prep.marginal_costs.run_allocation.require_published_code",
        refuse,
    )
    monkeypatch.setattr("utils.data_prep.marginal_costs.run_allocation.resolve_input", unused)

    with pytest.raises(UnpublishedCodeError):
        run(config, "dist_and_sub_tx", "acme", 2025, upload=True, storage_options={})
