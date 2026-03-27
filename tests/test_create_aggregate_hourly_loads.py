"""Tests for aggregate hourly ResStock load helper functions."""

from __future__ import annotations

import argparse
from datetime import datetime

import polars as pl
import pytest

from data.resstock.create_aggregate_hourly_loads import (
    _parse_upgrades,
    _resstock_base,
    aggregate_hourly_load_mwh,
    electric_utility_passes,
    list_unique_electric_utilities,
    load_filtered_buildings,
)


def test_parse_upgrades_zfill() -> None:
    assert _parse_upgrades("0  2  00") == ["00", "02", "00"]


def test_resstock_base_from_release() -> None:
    args = argparse.Namespace(
        resstock_base=None,
        nrel_root="s3://data.sb/nrel/resstock",
        release="res_2024_amy2018_2_sb",
    )
    assert _resstock_base(args) == "s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb"


def test_resstock_base_explicit() -> None:
    args = argparse.Namespace(
        resstock_base="s3://bucket/res_x",
        nrel_root="ignored",
        release="ignored",
    )
    assert _resstock_base(args) == "s3://bucket/res_x"


def test_load_filtered_buildings_utility_only(tmp_path) -> None:
    path = tmp_path / "ua.parquet"
    pl.DataFrame(
        {
            "bldg_id": [1, 2, 3],
            "weight": [1.0, 2.0, 3.0],
            "sb.electric_utility": ["a", "b", "a"],
            "postprocess_group.has_hp": [True, False, True],
        }
    ).write_parquet(path)
    df = load_filtered_buildings(
        str(path),
        electric_utility="a",
        has_hp_filter=None,
        storage_options={},
    )
    assert set(df["bldg_id"].to_list()) == {1, 3}


def test_load_filtered_buildings_hp_only(tmp_path) -> None:
    path = tmp_path / "ua.parquet"
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [1.0, 2.0],
            "sb.electric_utility": ["a", "a"],
            "postprocess_group.has_hp": [True, False],
        }
    ).write_parquet(path)
    df = load_filtered_buildings(
        str(path),
        electric_utility=None,
        has_hp_filter=True,
        storage_options={},
    )
    assert df["bldg_id"].to_list() == [1]


def test_load_filtered_buildings_non_hp_only(tmp_path) -> None:
    path = tmp_path / "ua.parquet"
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "weight": [1.0, 2.0],
            "sb.electric_utility": ["a", "a"],
            "postprocess_group.has_hp": [True, False],
        }
    ).write_parquet(path)
    df = load_filtered_buildings(
        str(path),
        electric_utility=None,
        has_hp_filter=False,
        storage_options={},
    )
    assert df["bldg_id"].to_list() == [2]


def test_aggregate_hourly_weighted_mwh(tmp_path) -> None:
    """Two buildings, one hour: (10*2 + 5*1) kWh -> 0.025 MWh."""
    resstock_base = str(tmp_path)
    st = "NY"
    up = "00"
    part = tmp_path / "load_curve_hourly" / f"state={st}" / f"upgrade={up}"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "timestamp": [datetime(2018, 1, 1, 0), datetime(2018, 1, 1, 0)],
            "out.electricity.net.energy_consumption": [10.0, 5.0],
        }
    ).write_parquet(part / "chunk.parquet")

    buildings = pl.DataFrame({"bldg_id": [1, 2], "weight": [2.0, 1.0]})
    out = aggregate_hourly_load_mwh(
        resstock_base,
        st,
        up,
        buildings,
        load_col="out.electricity.net.energy_consumption",
        weighted=True,
        storage_options={},
    )
    assert out.height == 1
    assert out["load_mwh"][0] == pytest.approx(25.0 / 1000.0)


def test_load_filtered_buildings_requires_filter() -> None:
    with pytest.raises(ValueError, match="electric_utility"):
        load_filtered_buildings(
            "missing.parquet",
            electric_utility=None,
            has_hp_filter=None,
            storage_options={},
        )


def test_list_unique_electric_utilities_sorted(tmp_path) -> None:
    path = tmp_path / "ua.parquet"
    pl.DataFrame(
        {"sb.electric_utility": ["zebra", "a", "a", None, "Zebra"]},
    ).write_parquet(path)
    assert list_unique_electric_utilities(str(path), {}) == ["a", "zebra"]


def test_electric_utility_passes_none_and_all(tmp_path) -> None:
    path = tmp_path / "ua.parquet"
    pl.DataFrame(
        {
            "sb.electric_utility": ["x", "y"],
        },
    ).write_parquet(path)
    assert electric_utility_passes(
        None, path_utility_assignment=str(path), storage_options={}
    ) == [None]
    assert electric_utility_passes(
        "all",
        path_utility_assignment=str(path),
        storage_options={},
    ) == ["x", "y"]
    assert electric_utility_passes(
        "x",
        path_utility_assignment=str(path),
        storage_options={},
    ) == ["x"]
