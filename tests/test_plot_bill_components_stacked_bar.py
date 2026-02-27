"""Tests for plot_bill_components_stacked_bar helpers (join/weight asserts, tariff reader, join+components)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import polars as pl
import pytest

from utils.post.plot_bill_components_stacked_bar import (
    _assert_join_preserves_keys,
    _assert_weights_match,
    _join_bills_and_compute_components,
    _plot_bill_components_stacked,
    _read_fixed_charge_from_tariff,
    _weighted_median_row,
)

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "plot_bill_components"
ANNUAL_FIXED = 6.75


# --- _weighted_median_row ---


def test_weighted_median_row_returns_middle_by_weight() -> None:
    """Weighted median row is the first where cumulative weight >= 50% of total."""
    df = pl.DataFrame(
        {
            "weight": [1.0, 2.0, 3.0],
            "total": [10.0, 20.0, 30.0],
        }
    )
    result = _weighted_median_row(df, sort_col="total", weight_col="weight")
    assert result.height == 1
    assert result["total"].item() == 20.0


def test_weighted_median_row_single_row() -> None:
    """Single row is returned as-is."""
    df = pl.DataFrame({"weight": [5.0], "total": [100.0]})
    result = _weighted_median_row(df)
    assert result.height == 1
    assert result["total"].item() == 100.0


def test_plot_bill_components_stacked_fails_loudly_when_median_empty() -> None:
    """When weighted median yields 0 rows (empty data or invalid weights), plot raises with clear message."""
    empty_median = pl.DataFrame(
        schema={
            "weight": pl.Float64(),
            "supply": pl.Float64(),
            "delivery_volumetric": pl.Float64(),
            "delivery_fixed": pl.Float64(),
            "total": pl.Float64(),
        }
    )
    one_row = pl.DataFrame(
        {
            "weight": [1.0],
            "supply": [10.0],
            "delivery_volumetric": [20.0],
            "delivery_fixed": [5.0],
            "total": [35.0],
        }
    )
    with pytest.raises(AssertionError, match="Expected 3 component rows.*got 0"):
        _plot_bill_components_stacked(empty_median, one_row)


# --- _assert_join_preserves_keys ---


def test_assert_join_preserves_keys_pass_same_keys_no_drop() -> None:
    """When left and right have same keys and join preserves all, no error."""
    left = pl.DataFrame({"bldg_id": [1, 2], "month": ["Jan", "Jan"]})
    right = pl.DataFrame({"bldg_id": [1, 2], "month": ["Jan", "Jan"]})
    joined = left.join(right, on=["bldg_id", "month"], how="inner")
    _assert_join_preserves_keys(left, right, joined, ["bldg_id", "month"])


def test_assert_join_preserves_keys_fail_key_mismatch() -> None:
    """When left and right have different key counts, raises with message."""
    left = pl.DataFrame({"bldg_id": [1, 2, 3], "month": ["Jan", "Jan", "Jan"]})
    right = pl.DataFrame({"bldg_id": [1, 2], "month": ["Jan", "Jan"]})
    joined = left.join(right, on=["bldg_id", "month"], how="inner")
    with pytest.raises(
        ValueError, match="Key mismatch before join.*left has 3.*right has 2"
    ):
        _assert_join_preserves_keys(left, right, joined, ["bldg_id", "month"])


def test_assert_join_preserves_keys_fail_join_drops_rows() -> None:
    """When join drops rows (same key count but non-overlapping keys), raises with message."""
    left = pl.DataFrame({"bldg_id": [1, 2], "month": ["Jan", "Jan"]})
    right = pl.DataFrame({"bldg_id": [1, 3], "month": ["Jan", "Jan"]})
    joined = left.join(right, on=["bldg_id", "month"], how="inner")
    with pytest.raises(
        ValueError, match="Inner join dropped rows.*left had 2.*join result has 1"
    ):
        _assert_join_preserves_keys(left, right, joined, ["bldg_id", "month"])


# --- _assert_weights_match ---


def test_assert_weights_match_pass_identical() -> None:
    """When both weight columns are equal, no error."""
    joined = pl.DataFrame({"weight": [1.0, 2.0], "weight_supply": [1.0, 2.0]})
    _assert_weights_match(joined, left_weight="weight", right_weight="weight_supply")


def test_assert_weights_match_pass_within_tolerance() -> None:
    """When diff is within tolerance, no error."""
    joined = pl.DataFrame(
        {
            "weight": [1.0, 2.0],
            "weight_supply": [1.0 + 1e-10, 2.0 - 1e-10],
        }
    )
    _assert_weights_match(
        joined, left_weight="weight", right_weight="weight_supply", tolerance=1e-9
    )


def test_assert_weights_match_fail_differ() -> None:
    """When weight columns differ beyond tolerance, raises with message."""
    joined = pl.DataFrame({"weight": [1.0, 2.0], "weight_supply": [1.0, 3.0]})
    with pytest.raises(
        ValueError, match="Weight columns.*differ.*1 rows.*max \\|diff\\| = 1.0"
    ):
        _assert_weights_match(
            joined, left_weight="weight", right_weight="weight_supply"
        )


# --- _read_fixed_charge_from_tariff ---


def test_read_fixed_charge_from_tariff_pass(tmp_path: Path) -> None:
    """Valid tariff JSON with fixedchargefirstmeter returns the value."""
    path = tmp_path / "tariff.json"
    path.write_text(
        json.dumps({"items": [{"fixedchargefirstmeter": 6.75, "label": "flat"}]})
    )
    assert _read_fixed_charge_from_tariff(str(path)) == 6.75


def test_read_fixed_charge_from_tariff_file_not_found() -> None:
    """Missing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="Tariff JSON not found"):
        _read_fixed_charge_from_tariff("/nonexistent/tariff.json")


def test_read_fixed_charge_from_tariff_no_items(tmp_path: Path) -> None:
    """Tariff with no items raises ValueError."""
    path = tmp_path / "tariff.json"
    path.write_text(json.dumps({"items": []}))
    with pytest.raises(ValueError, match="no 'items'"):
        _read_fixed_charge_from_tariff(str(path))


def test_read_fixed_charge_from_tariff_missing_fixedcharge(tmp_path: Path) -> None:
    """Tariff items[0] without fixedchargefirstmeter raises ValueError."""
    path = tmp_path / "tariff.json"
    path.write_text(json.dumps({"items": [{"label": "flat"}]}))
    with pytest.raises(ValueError, match="no 'fixedchargefirstmeter'"):
        _read_fixed_charge_from_tariff(str(path))


# --- _join_bills_and_compute_components (fixture-based) ---


def _fixture_path(name: str) -> Path:
    return FIXTURE_DIR / name


def test_join_bills_and_compute_components_run1_run2_fixture() -> None:
    """Run 1 & 2 fixture: join delivery+supply, compute components; assert shape, formulas, and annual = sum of months."""
    p_delivery = _fixture_path("delivery_run1_2bldgs.csv")
    p_supply = _fixture_path("supply_run2_2bldgs.csv")
    if not p_delivery.exists() or not p_supply.exists():
        pytest.skip("Fixture CSVs missing")
    delivery = pl.scan_csv(str(p_delivery))
    supply = pl.scan_csv(str(p_supply))
    df = _join_bills_and_compute_components(delivery, supply, ANNUAL_FIXED)
    assert df.height >= 1
    assert set(df.columns) >= {
        "bldg_id",
        "month",
        "supply",
        "delivery_fixed",
        "delivery_volumetric",
        "total",
        "bill_level",
        "bill_supply",
    }

    expected_supply = df["bill_supply"] - df["bill_level"]
    assert cast(float, (df["supply"] - expected_supply).abs().max()) < 1e-9
    assert (df["delivery_fixed"] == ANNUAL_FIXED).all()

    expected_volumetric = df["bill_level"] - ANNUAL_FIXED
    assert (
        cast(float, (df["delivery_volumetric"] - expected_volumetric).abs().max())
        < 1e-9
    )

    expected_total = df["supply"] + df["delivery_fixed"] + df["delivery_volumetric"]
    assert cast(float, (df["total"] - expected_total).abs().max()) < 1e-9

    monthly = df.filter(pl.col("month") != "Annual")
    annual = df.filter(pl.col("month") == "Annual")
    if monthly.height > 0 and annual.height > 0:
        summed = monthly.group_by("bldg_id").agg(
            pl.col("supply").sum().alias("supply_sum"),
            pl.col("total").sum().alias("total_sum"),
        )
        joined = annual.select("bldg_id", "supply", "total").join(
            summed, on="bldg_id", how="inner"
        )
        assert cast(float, (joined["supply"] - joined["supply_sum"]).abs().max()) < 1e-6
        assert cast(float, (joined["total"] - joined["total_sum"]).abs().max()) < 1e-6


def test_join_bills_and_compute_components_run3_run4_fixture() -> None:
    """Run 3 & 4 fixture: same checks for HP pair (formulas + annual = sum of months)."""
    p_delivery = _fixture_path("delivery_run3_2bldgs.csv")
    p_supply = _fixture_path("supply_run4_2bldgs.csv")
    if not p_delivery.exists() or not p_supply.exists():
        pytest.skip("Fixture CSVs missing")
    delivery = pl.scan_csv(str(p_delivery))
    supply = pl.scan_csv(str(p_supply))
    df = _join_bills_and_compute_components(delivery, supply, ANNUAL_FIXED)
    assert df.height >= 1
    assert set(df.columns) >= {
        "bldg_id",
        "month",
        "supply",
        "delivery_fixed",
        "delivery_volumetric",
        "total",
        "bill_level",
        "bill_supply",
    }
    expected_supply = df["bill_supply"] - df["bill_level"]
    assert cast(float, (df["supply"] - expected_supply).abs().max()) < 1e-9
    assert (df["delivery_fixed"] == ANNUAL_FIXED).all()
    expected_volumetric = df["bill_level"] - ANNUAL_FIXED
    assert (
        cast(float, (df["delivery_volumetric"] - expected_volumetric).abs().max())
        < 1e-9
    )
    expected_total = df["supply"] + df["delivery_fixed"] + df["delivery_volumetric"]
    assert cast(float, (df["total"] - expected_total).abs().max()) < 1e-9
    monthly = df.filter(pl.col("month") != "Annual")
    annual = df.filter(pl.col("month") == "Annual")
    if monthly.height > 0 and annual.height > 0:
        summed = monthly.group_by("bldg_id").agg(
            pl.col("supply").sum().alias("supply_sum"),
            pl.col("total").sum().alias("total_sum"),
        )
        joined = annual.select("bldg_id", "supply", "total").join(
            summed, on="bldg_id", how="inner"
        )
        assert cast(float, (joined["supply"] - joined["supply_sum"]).abs().max()) < 1e-6
        assert cast(float, (joined["total"] - joined["total_sum"]).abs().max()) < 1e-6
