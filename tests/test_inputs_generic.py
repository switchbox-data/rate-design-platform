"""Tests for the loaders in inputs/generic.py."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from utils.data_prep.marginal_costs.inputs import InputContext, resolve_input
from utils.data_prep.marginal_costs.inputs.generic import cpi, hive_load, mc_table, parquet, periods_yaml
from utils.data_prep.marginal_costs.supply_utils import build_cairo_8760_timestamps


def _ctx(year: int = 2025, load_year: int = 2025) -> InputContext:
    return InputContext(utility="acme", year=year, load_year=load_year, storage_options={})


def _clock(year: int, load: float = 10.0) -> pl.DataFrame:
    return build_cairo_8760_timestamps(year).with_columns(
        pl.col("timestamp").cast(pl.Datetime("us")),
        pl.lit(load).alias("load_mw"),
    )


def _write_cpi(directory: Path, vintage: str, rows: list[dict[str, float]]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(directory / f"cpiaucsl_{vintage}.parquet")


def test_literal_returns_the_yaml_value() -> None:
    resolved = resolve_input("literal", {"from": "literal", "value": 12}, _ctx(), role="n_hours")
    assert resolved.value == 12
    assert resolved.inputs == []


def test_unknown_loader_names_the_known_ones() -> None:
    with pytest.raises(ValueError, match="zone_sum_load") as exc:
        resolve_input("zone_sum_load", {}, _ctx(), role="load")
    assert "isone_zone_load" in str(exc.value)
    assert "pjm_hive_load" in str(exc.value)


def test_mc_table_without_dollar_year(tmp_path: Path) -> None:
    table = tmp_path / "mc.csv"
    table.write_text("utility,sub_tx_and_dist_mc_kw_yr\nacme, 21.5 \n")

    resolved = mc_table({"path": str(table), "column": "sub_tx_and_dist_mc_kw_yr"}, _ctx(), "annual")

    assert resolved.value == pytest.approx(21.5)
    assert resolved.meta["cpi_factor"] == 1.0
    assert resolved.meta["dollar_year"] is None
    assert [item["role"] for item in resolved.inputs] == ["mc_table"]


def test_mc_table_inflates_with_cpi(tmp_path: Path) -> None:
    table = tmp_path / "mc.csv"
    table.write_text("utility,bulk_tx_mc_kw_yr,dollar_year\nacme,100,2020\n")
    cpi_dir = tmp_path / "cpi"
    _write_cpi(cpi_dir, "20200101", [{"year": 2020, "value": 100.0}, {"year": 2025, "value": 110.0}])
    _write_cpi(cpi_dir, "20260101", [{"year": 2020, "value": 100.0}, {"year": 2025, "value": 120.0}])

    resolved = mc_table(
        {
            "path": str(table),
            "column": "bulk_tx_mc_kw_yr",
            "dollar_year_column": "dollar_year",
            "target_dollar_year": 2025,
            "cpi_s3_base": str(cpi_dir),
        },
        _ctx(),
        "annual",
    )

    assert resolved.value == pytest.approx(120.0)
    assert resolved.meta["as_filed"] == pytest.approx(100.0)
    assert resolved.meta["cpi_factor"] == pytest.approx(1.2)
    assert [item["role"] for item in resolved.inputs] == ["mc_table", "cpi"]


def test_cpi_loader_uses_the_latest_snapshot(tmp_path: Path) -> None:
    cpi_dir = tmp_path / "cpi"
    _write_cpi(cpi_dir, "20200101", [{"year": 2024, "value": 100.0}, {"year": 2026, "value": 100.0}])
    _write_cpi(cpi_dir, "20260101", [{"year": 2024, "value": 100.0}, {"year": 2026, "value": 150.0}])

    same = cpi(
        {"value": 69, "dollar_year": 2024, "target_dollar_year": 2024, "cpi_s3_base": str(cpi_dir)},
        _ctx(),
        "annual",
    )
    inflated = cpi(
        {"value": 69, "dollar_year": 2024, "target_dollar_year": 2026, "cpi_s3_base": str(cpi_dir)},
        _ctx(),
        "annual",
    )

    assert same.value == pytest.approx(69.0)
    assert same.inputs == []
    assert inflated.value == pytest.approx(103.5)
    assert inflated.inputs[0]["role"] == "cpi"


def test_parquet_load_checks_columns_and_filters_load_year(tmp_path: Path) -> None:
    path = tmp_path / "loads.parquet"
    older = _clock(2024, 1.0).with_columns(pl.lit(2024).alias("year"))
    newer = _clock(2025, 4.0).with_columns(pl.lit(2025).alias("year"))
    pl.concat([older, newer]).write_parquet(path)

    resolved = parquet({"path": str(path), "load_year": 2025}, _ctx(year=2026, load_year=2025), "load")

    assert resolved.value.height == 8760
    assert resolved.value["load_mw"].unique().to_list() == [4.0]
    assert resolved.value["timestamp"].dt.year().unique().to_list() == [2026]
    assert resolved.meta["load_source"] == "loads"
    assert resolved.meta["load_year"] == 2025

    bare = tmp_path / "bare.parquet"
    pl.DataFrame({"timestamp": [datetime(2025, 1, 1)]}).write_parquet(bare)
    with pytest.raises(ValueError, match="load_mw"):
        parquet({"path": str(bare), "load_year": 2025}, _ctx(), "load")


def test_hive_load_reads_the_utility_year_partition(tmp_path: Path) -> None:
    partition = tmp_path / "utility=acme" / "year=2024"
    partition.mkdir(parents=True)
    _clock(2024, 7.0).write_parquet(partition / "data.parquet")

    resolved = hive_load({"path": str(tmp_path), "load_year": 2024}, _ctx(year=2025, load_year=2024), "load")

    assert resolved.value.height == 8760
    assert resolved.value["timestamp"].dt.year().unique().to_list() == [2025]
    assert resolved.value["load_mw"].unique().to_list() == [7.0]
    assert resolved.meta["load_source"].endswith("_utility")
    assert resolved.inputs[0]["role"] == "load"
    assert resolved.inputs[0]["n_files"] == 1


def test_periods_yaml_reads_winter_months_or_the_default(tmp_path: Path) -> None:
    path = tmp_path / "periods.yaml"
    path.write_text("winter_months: [12, 1, 2]\n")

    from_file = periods_yaml({"path": str(path), "default": [10, 11, 12, 1, 2, 3]}, _ctx(), "winter_months")
    missing = periods_yaml(
        {"path": str(tmp_path / "absent.yaml"), "default": [10, 11, 12, 1, 2, 3]},
        _ctx(),
        "winter_months",
    )

    assert from_file.value == [1, 2, 12]
    assert from_file.inputs[0]["role"] == "winter_months"
    assert missing.value == [1, 2, 3, 10, 11, 12]
    assert missing.inputs == []
