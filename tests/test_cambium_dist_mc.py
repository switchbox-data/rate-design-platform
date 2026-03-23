"""Tests for Cambium busbar_load dist MC loading and empty bulk TX handling.

Covers:
- load_cambium_load_profile: busbar_load → load_mw rename, utility column
- Missing required columns raise a clear error
- add_bulk_tx_and_dist_and_sub_tx_marginal_cost with empty/None path_bulk_tx_mc
  returns dist-only MC (no bulk TX added)
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from utils.cairo import add_bulk_tx_and_dist_and_sub_tx_marginal_cost
from utils.pre.marginal_costs.generate_utility_tx_dx_mc import (
    load_cambium_load_profile,
)


def _make_cambium_parquet(tmp_path: Path, *, rows: int = 8760) -> Path:
    """Write a minimal Cambium-style parquet with timestamp + busbar_load."""
    timestamps = pl.datetime_range(
        datetime(2025, 1, 1, 0, 0, 0),
        datetime(2025, 12, 31, 23, 0, 0),
        interval="1h",
        eager=True,
    )[:rows]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "busbar_load": [1000.0 + i * 0.1 for i in range(rows)],
            "energy_cost_enduse": [0.05] * rows,
        }
    )
    path = tmp_path / "cambium.parquet"
    df.write_parquet(path)
    return path


def _make_dist_mc_parquet(tmp_path: Path, *, rows: int = 8760) -> Path:
    """Write a minimal dist+sub-tx MC parquet in the format cairo expects.

    The loader (load_dist_and_sub_tx_marginal_costs) expects columns:
      timestamp, mc_total_per_kwh (and optionally mc_upstream_per_kwh, mc_dist_per_kwh).
    Timestamps must be tz-naive (the loader tz-localizes to EST).
    """
    timestamps = pd.date_range("2025-01-01", periods=rows, freq="h")
    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "mc_total_per_kwh": [0.001] * rows,
            "mc_upstream_per_kwh": [0.0005] * rows,
            "mc_dist_per_kwh": [0.0005] * rows,
        }
    )
    path = tmp_path / "dist_mc.parquet"
    df.to_parquet(path, index=False)
    return path


class TestLoadCambiumLoadProfile:
    def test_renames_busbar_load_to_load_mw(self, tmp_path: Path) -> None:
        path = _make_cambium_parquet(tmp_path)
        result = load_cambium_load_profile(str(path), "nyseg", {})
        assert "load_mw" in result.columns
        assert "busbar_load" not in result.columns

    def test_adds_utility_column(self, tmp_path: Path) -> None:
        path = _make_cambium_parquet(tmp_path)
        result = load_cambium_load_profile(str(path), "nyseg", {})
        assert "utility" in result.columns
        assert result["utility"].unique().to_list() == ["nyseg"]

    def test_preserves_row_count(self, tmp_path: Path) -> None:
        path = _make_cambium_parquet(tmp_path)
        result = load_cambium_load_profile(str(path), "nyseg", {})
        assert len(result) == 8760

    def test_timestamp_column_preserved(self, tmp_path: Path) -> None:
        path = _make_cambium_parquet(tmp_path)
        result = load_cambium_load_profile(str(path), "nyseg", {})
        assert "timestamp" in result.columns

    def test_missing_busbar_load_raises(self, tmp_path: Path) -> None:
        df = pl.DataFrame(
            {
                "timestamp": [datetime(2025, 1, 1)] * 10,
                "energy_cost_enduse": [0.05] * 10,
            }
        )
        path = tmp_path / "bad.parquet"
        df.write_parquet(path)
        with pytest.raises(ValueError, match="busbar_load"):
            load_cambium_load_profile(str(path), "nyseg", {})

    def test_missing_timestamp_raises(self, tmp_path: Path) -> None:
        df = pl.DataFrame({"busbar_load": [1000.0] * 10})
        path = tmp_path / "bad.parquet"
        df.write_parquet(path)
        with pytest.raises(ValueError, match="timestamp"):
            load_cambium_load_profile(str(path), "nyseg", {})

    def test_load_mw_values_match_busbar_load(self, tmp_path: Path) -> None:
        path = _make_cambium_parquet(tmp_path)
        result = load_cambium_load_profile(str(path), "nyseg", {})
        expected = [1000.0 + i * 0.1 for i in range(8760)]
        actual = result["load_mw"].to_list()
        assert actual == pytest.approx(expected, rel=1e-6)


class TestAddBulkTxEmptyPath:
    """add_bulk_tx_and_dist_and_sub_tx_marginal_cost with empty/None bulk TX path
    returns dist-only MC (no bulk TX contribution)."""

    def _make_target_index(self) -> pd.DatetimeIndex:
        return pd.date_range("2025-01-01", periods=8760, freq="h", tz="EST")

    def test_empty_string_path_skips_bulk_tx(self, tmp_path: Path) -> None:
        path_dist = _make_dist_mc_parquet(tmp_path)
        target_idx = self._make_target_index()

        result = add_bulk_tx_and_dist_and_sub_tx_marginal_cost(
            path_dist_and_sub_tx_mc=path_dist,
            path_bulk_tx_mc="",
            target_index=target_idx,
        )

        assert result is not None
        assert len(result) == 8760
        assert result.sum() == pytest.approx(0.001 * 8760, rel=1e-4)

    def test_none_path_skips_bulk_tx(self, tmp_path: Path) -> None:
        path_dist = _make_dist_mc_parquet(tmp_path)
        target_idx = self._make_target_index()

        result = add_bulk_tx_and_dist_and_sub_tx_marginal_cost(
            path_dist_and_sub_tx_mc=path_dist,
            path_bulk_tx_mc=None,
            target_index=target_idx,
        )

        assert result.sum() == pytest.approx(0.001 * 8760, rel=1e-4)

    def test_whitespace_only_path_skips_bulk_tx(self, tmp_path: Path) -> None:
        path_dist = _make_dist_mc_parquet(tmp_path)
        target_idx = self._make_target_index()

        result = add_bulk_tx_and_dist_and_sub_tx_marginal_cost(
            path_dist_and_sub_tx_mc=path_dist,
            path_bulk_tx_mc="   ",
            target_index=target_idx,
        )

        assert result.sum() == pytest.approx(0.001 * 8760, rel=1e-4)

    def test_result_name_is_delivery_mc(self, tmp_path: Path) -> None:
        path_dist = _make_dist_mc_parquet(tmp_path)
        target_idx = self._make_target_index()

        result = add_bulk_tx_and_dist_and_sub_tx_marginal_cost(
            path_dist_and_sub_tx_mc=path_dist,
            path_bulk_tx_mc=None,
            target_index=target_idx,
        )

        assert result.name == "Marginal Distribution Costs ($/kWh)"
