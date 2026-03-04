"""Unit tests for utility supply MC helper logic."""

from __future__ import annotations

import polars as pl
import pytest

from utils.pre.generate_utility_supply_mc import (
    allocate_icap_to_hours,
    build_capacity_peak_load_profile_from_zone_loads,
    compute_weighted_icap_prices,
    get_partitioned_price_locality_weights,
    validate_capacity_allocation,
)


def test_capacity_peak_profile_uses_nested_nyca_footprint() -> None:
    """NYCA locality should use all 11 zones (including LONGIL), not only utility service zones."""
    locality_weights = pl.DataFrame(
        {
            "locality": ["NYCA"],
            "capacity_weight": [1.0],
        }
    )
    zone_loads_df = pl.DataFrame(
        {
            "timestamp": [1, 1, 1, 2, 2, 2],
            "zone": ["WEST", "GENESE", "LONGIL", "WEST", "GENESE", "LONGIL"],
            "load_mw": [10.0, 20.0, 100.0, 15.0, 25.0, 200.0],
        }
    )

    result = build_capacity_peak_load_profile_from_zone_loads(
        locality_weights, zone_loads_df
    )

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
            "zone": ["N.Y.C.", "DUNWOD", "N.Y.C.", "DUNWOD"],
            "load_mw": [100.0, 30.0, 200.0, 50.0],
        }
    )

    result = build_capacity_peak_load_profile_from_zone_loads(
        locality_weights, zone_loads_df
    ).sort("timestamp")

    # NYC uses N.Y.C.; LHV uses HUD_VL+MILLWD+DUNWOD+N.Y.C.,
    # so with data present that is (DUNWOD+N.Y.C.).
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


# ── allocate_icap_to_hours tie-breaking ──────────────────────────────────────


def _make_monthly_load(
    year: int = 2025, n_peak_hours: int = 8, tie_months: list[int] | None = None
) -> pl.DataFrame:
    """Build a synthetic 8760 load profile.

    For months in ``tie_months``, the Nth and (N+1)th highest loads are set equal.
    """
    from datetime import datetime, timedelta

    tie_months = tie_months or []
    start = datetime(year, 1, 1, 0, 0, 0)
    timestamps = [start + timedelta(hours=h) for h in range(8760)]
    loads: list[float] = []
    for ts in timestamps:
        month = ts.month
        base = 1000.0 + month * 100.0
        hour_offset = ts.timetuple().tm_yday * 24 + ts.hour
        load = base + hour_offset * 0.01
        loads.append(load)

    df = pl.DataFrame({"timestamp": timestamps, "load_mw": loads})

    for m in tie_months:
        month_mask = pl.col("timestamp").dt.month() == m
        month_loads = df.filter(month_mask).sort("load_mw", descending=True)
        nth_load = float(month_loads["load_mw"][n_peak_hours - 1])
        next_load = float(month_loads["load_mw"][n_peak_hours])
        # Set the (N+1)th highest load equal to the Nth
        df = df.with_columns(
            pl.when(pl.col("load_mw") == next_load)
            .then(nth_load)
            .otherwise(pl.col("load_mw"))
            .alias("load_mw")
        )

    return df


def test_allocate_icap_tie_at_nth_hour() -> None:
    """When Nth and (N+1)th loads tie, exactly N hours per month get cost."""
    n_peak = 8
    load_df = _make_monthly_load(tie_months=[1, 6, 12], n_peak_hours=n_peak)
    icap_prices = pl.DataFrame(
        {"month": list(range(1, 13)), "icap_price_per_kw_month": [5.0] * 12}
    )

    result = allocate_icap_to_hours(load_df, icap_prices, n_peak_hours=n_peak)
    nonzero = result.filter(pl.col("capacity_cost_per_kw") > 0)
    assert nonzero.height == n_peak * 12

    # Per-month: every month should have exactly n_peak nonzero hours
    monthly_counts = (
        nonzero.with_columns(pl.col("timestamp").dt.month().alias("month"))
        .group_by("month")
        .len()
    )
    for row in monthly_counts.iter_rows(named=True):
        assert row["len"] == n_peak, (
            f"month {row['month']}: expected {n_peak} nonzero, got {row['len']}"
        )

    # 1 kW recovery: sum should equal annual ICAP total
    validate_capacity_allocation(result, icap_prices)


def test_allocate_icap_no_tie_unchanged() -> None:
    """Without ties, allocation produces the same result as before."""
    n_peak = 8
    load_df = _make_monthly_load(tie_months=[], n_peak_hours=n_peak)
    icap_prices = pl.DataFrame(
        {"month": list(range(1, 13)), "icap_price_per_kw_month": [5.0] * 12}
    )

    result = allocate_icap_to_hours(load_df, icap_prices, n_peak_hours=n_peak)
    nonzero = result.filter(pl.col("capacity_cost_per_kw") > 0)
    assert nonzero.height == n_peak * 12

    validate_capacity_allocation(result, icap_prices)


# ── timestamp remap when load_year != price_year ─────────────────────────────


def test_capacity_timestamp_remap_preserves_allocation() -> None:
    """Remapping 2018->2025 timestamps preserves ordinal positions and totals."""
    n_peak = 8
    load_df = _make_monthly_load(year=2018, n_peak_hours=n_peak)
    icap_prices = pl.DataFrame(
        {"month": list(range(1, 13)), "icap_price_per_kw_month": [5.0] * 12}
    )

    result_2018 = allocate_icap_to_hours(load_df, icap_prices, n_peak_hours=n_peak)

    result_2025 = result_2018.with_columns(
        pl.col("timestamp").dt.offset_by(f"{2025 - 2018}y")
    )

    years = result_2025["timestamp"].dt.year().unique().to_list()
    assert years == [2025]

    assert result_2025.height == result_2018.height
    nonzero_2018 = result_2018.filter(pl.col("capacity_cost_per_kw") > 0)
    nonzero_2025 = result_2025.filter(pl.col("capacity_cost_per_kw") > 0)
    assert nonzero_2025.height == nonzero_2018.height

    assert float(result_2025["capacity_cost_per_kw"].sum()) == pytest.approx(
        float(result_2018["capacity_cost_per_kw"].sum())
    )

    def ordinals(df: pl.DataFrame) -> list[tuple[int, int, int]]:
        ts = df.filter(pl.col("capacity_cost_per_kw") > 0).sort("timestamp")[
            "timestamp"
        ]
        return [(t.month, t.day, t.hour) for t in ts.to_list()]

    assert ordinals(result_2018) == ordinals(result_2025)
