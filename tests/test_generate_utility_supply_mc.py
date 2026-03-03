"""Unit tests for utility supply MC helper logic."""

from __future__ import annotations

import polars as pl
import pytest

from utils.pre.generate_utility_supply_mc import (
    build_capacity_peak_load_profile_from_zone_loads,
    compute_weighted_icap_prices,
    get_partitioned_price_locality_weights,
)


def test_capacity_peak_profile_uses_nested_nyca_footprint() -> None:
    """NYCA locality should use A-K (including K), not only utility service zones."""
    locality_weights = pl.DataFrame(
        {
            "locality": ["NYCA"],
            "capacity_weight": [1.0],
        }
    )
    zone_loads_df = pl.DataFrame(
        {
            "timestamp": [1, 1, 1, 2, 2, 2],
            "zone": ["A", "B", "K", "A", "B", "K"],
            "load_mw": [10.0, 20.0, 100.0, 15.0, 25.0, 200.0],
        }
    )

    result = build_capacity_peak_load_profile_from_zone_loads(
        locality_weights, zone_loads_df
    )

    # NYCA = A-K, so K is included.
    assert result.sort("timestamp")["load_mw"].to_list() == [130.0, 240.0]


def test_capacity_peak_profile_blends_multiple_capacity_localities() -> None:
    """Split utilities should blend nested localities using capacity_weight."""
    locality_weights = pl.DataFrame(
        {
            "locality": ["NYC", "LHV"],
            "capacity_weight": [0.87, 0.13],
        }
    )
    zone_loads_df = pl.DataFrame(
        {
            "timestamp": [1, 1, 2, 2],
            "zone": ["J", "I", "J", "I"],
            "load_mw": [100.0, 30.0, 200.0, 50.0],
        }
    )

    result = build_capacity_peak_load_profile_from_zone_loads(
        locality_weights, zone_loads_df
    ).sort("timestamp")

    # NYC uses J; LHV uses G-J, so with data present that is (I+J).
    # Hour 1: 0.87*100 + 0.13*(100+30) = 103.9
    # Hour 2: 0.87*200 + 0.13*(200+50) = 206.5
    assert result["load_mw"].to_list() == [103.9, 206.5]


def test_partitioned_price_locality_weights_transform_nested_to_partitioned() -> None:
    """Price weights should come from partitioned localities (ROS/LHV/NYC/LI)."""
    utility_mapping = pl.DataFrame(
        {
            "gen_capacity_zone": ["NYC", "LHV"],
            "capacity_weight": [0.87, 0.13],
        }
    )

    weights = get_partitioned_price_locality_weights(utility_mapping)
    assert weights.to_dicts() == [
        {"locality": "LHV", "capacity_weight": 0.13},
        {"locality": "NYC", "capacity_weight": 0.87},
    ]


def test_weighted_icap_prices_uses_partitioned_localities() -> None:
    """ICAP price blending should operate on partitioned localities."""
    months = list(range(1, 13))
    icap_df = pl.DataFrame(
        {
            "month": months + months,
            "locality": ["LHV"] * 12 + ["NYC"] * 12,
            "price_per_kw_month": [10.0] * 12 + [30.0] * 12,
        }
    )
    locality_weights = pl.DataFrame(
        {
            "locality": ["LHV", "NYC"],
            "capacity_weight": [0.13, 0.87],
        }
    )
    result = compute_weighted_icap_prices(icap_df, locality_weights).sort("month")
    assert result["icap_price_per_kw_month"].to_list() == pytest.approx([27.4] * 12)
