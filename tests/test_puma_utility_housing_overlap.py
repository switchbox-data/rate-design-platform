"""Tests for housing-unit-weighted PUMA-utility overlap."""

from __future__ import annotations

import geopandas as gpd
import polars as pl
import pytest
from shapely.geometry import Point, box

from data.resstock.utility.utils import (
    add_block_gas_heated_homes,
    calculate_puma_utility_housing_overlap,
    calculate_utility_probabilities,
)

CRS = 2234


def test_housing_units_set_probabilities_not_area() -> None:
    pumas = gpd.GeoDataFrame(
        {"PUMACE10": ["00100"]}, geometry=[box(0, 0, 100, 100)], crs=CRS
    )
    # Yankee covers 90% of the land; the small city polygon holds most homes.
    utilities = gpd.GeoDataFrame(
        {"utility": ["yankee_gas", "norwich_muni"]},
        geometry=[box(10, 0, 100, 100), box(0, 0, 10, 100)],
        crs=CRS,
    )
    blocks = gpd.GeoDataFrame(
        {
            "block_geoid": ["a", "b", "c", "d"],
            "housing_units": [300, 500, 200, 1000],
        },
        geometry=[Point(5, 50), Point(6, 60), Point(50, 50), Point(200, 200)],
        crs=CRS,
    )

    overlap = calculate_puma_utility_housing_overlap(pumas, utilities, blocks, CRS)
    probs = calculate_utility_probabilities(
        overlap,
        pl.DataFrame(
            {
                "state_name": pl.Series([], dtype=pl.Utf8),
                "std_name": pl.Series([], dtype=pl.Utf8),
            }
        ).lazy(),
        handle_municipal=False,
    ).collect()

    row = probs.to_dicts()[0]
    assert row["norwich_muni"] == pytest.approx(0.8)
    assert row["yankee_gas"] == pytest.approx(0.2)


def test_block_in_two_polygons_splits_its_housing_units() -> None:
    pumas = gpd.GeoDataFrame(
        {"PUMACE10": ["00100"]}, geometry=[box(0, 0, 100, 100)], crs=CRS
    )
    utilities = gpd.GeoDataFrame(
        {"utility": ["yankee_gas", "ct_natural_gas"]},
        geometry=[box(0, 0, 60, 100), box(40, 0, 100, 100)],
        crs=CRS,
    )
    blocks = gpd.GeoDataFrame(
        {"block_geoid": ["shared"], "housing_units": [100]},
        geometry=[Point(50, 50)],
        crs=CRS,
    )

    overlap = calculate_puma_utility_housing_overlap(
        pumas, utilities, blocks, CRS
    ).collect()

    assert overlap["pct_overlap"].to_list() == pytest.approx([50.0, 50.0])


def test_gas_heated_homes_spread_by_housing_units_and_weight_overlap() -> None:
    bg = "090010001001"
    blocks = gpd.GeoDataFrame(
        {
            "block_geoid": [f"{bg}001", f"{bg}002", "090010001002001"],
            "housing_units": [30, 10, 50],
        },
        geometry=[Point(5, 50), Point(50, 50), Point(60, 60)],
        crs=CRS,
    )
    block_groups = pl.DataFrame(
        {"block_group_geoid": [bg, "090010001002"], "gas_heated_homes": [20, 0]}
    )

    with_gas = add_block_gas_heated_homes(blocks, block_groups)
    assert with_gas["gas_heated_homes"].tolist() == pytest.approx([15.0, 5.0, 0.0])

    pumas = gpd.GeoDataFrame(
        {"PUMACE10": ["00100"]}, geometry=[box(0, 0, 100, 100)], crs=CRS
    )
    utilities = gpd.GeoDataFrame(
        {"utility": ["norwich_muni", "yankee_gas"]},
        geometry=[box(0, 0, 10, 100), box(10, 0, 100, 100)],
        crs=CRS,
    )
    overlap = calculate_puma_utility_housing_overlap(
        pumas, utilities, with_gas, CRS, weight_col="gas_heated_homes"
    ).collect()

    shares = dict(zip(overlap["utility"], overlap["pct_overlap"], strict=True))
    assert shares["norwich_muni"] == pytest.approx(75.0)
    assert shares["yankee_gas"] == pytest.approx(25.0)
