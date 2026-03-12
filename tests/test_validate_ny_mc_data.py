"""Regression tests for marginal-cost validation loading behavior."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from utils.post.validate_ny_mc_data import check_mc, load_mc


def _timestamps_8760(year: int) -> list[datetime]:
    start = datetime(year, 1, 1, 0, 0, 0)
    return [start + timedelta(hours=h) for h in range(8760)]


def test_load_mc_reads_canonical_data_when_zero_file_exists(tmp_path: Path) -> None:
    """Validator reads data.parquet only when zero.parquet is also present."""
    year = 2025
    utility = "rie"
    base = tmp_path / "marginal_costs"
    partition = (
        base / "ri" / "supply" / "energy" / f"utility={utility}" / f"year={year}"
    )
    partition.mkdir(parents=True, exist_ok=True)

    ts = _timestamps_8760(year)
    pl.DataFrame(
        {"timestamp": ts, "energy_cost_enduse": [1000.0] * 8760}
    ).write_parquet(partition / "data.parquet")
    pl.DataFrame({"timestamp": ts, "energy_cost_enduse": [0.0] * 8760}).write_parquet(
        partition / "zero.parquet"
    )

    df = load_mc(
        mc_key="supply_energy",
        state="ri",
        utility=utility,
        year=year,
        storage_options={},
        mc_base_path=str(base),
    )

    assert df.height == 8760
    assert df.select(pl.col("timestamp").n_unique()).item() == 8760
    # 1000 $/MWh should normalize to 1.0 $/kWh when data.parquet is used.
    assert df.select(pl.col("mc_kwh").mean()).item() == 1.0


def test_load_mc_ignores_legacy_partition_file_for_dist_sub_tx(tmp_path: Path) -> None:
    """Validator remains stable when data.parquet and 00000000.parquet coexist."""
    year = 2025
    utility = "rie"
    base = tmp_path / "marginal_costs"
    partition = base / "ri" / "dist_and_sub_tx" / f"utility={utility}" / f"year={year}"
    partition.mkdir(parents=True, exist_ok=True)

    ts = _timestamps_8760(year)
    pl.DataFrame({"timestamp": ts, "mc_total_per_kwh": [2.0] * 8760}).write_parquet(
        partition / "data.parquet"
    )
    pl.DataFrame({"timestamp": ts, "mc_total_per_kwh": [3.0] * 8760}).write_parquet(
        partition / "00000000.parquet"
    )

    df = load_mc(
        mc_key="dist_sub_tx",
        state="ri",
        utility=utility,
        year=year,
        storage_options={},
        mc_base_path=str(base),
    )

    assert df.height == 8760
    assert df.select(pl.col("timestamp").n_unique()).item() == 8760
    assert df.select(pl.col("mc_kwh").mean()).item() == 2.0


def test_bulk_tx_expected_nonzero_is_state_aware_for_ri() -> None:
    """RI bulk_tx uses 100 expected nonzero hours; NY remains at 80."""
    year = 2025
    df = pl.DataFrame(
        {
            "timestamp": _timestamps_8760(year),
            "mc_kwh": ([1.0] * 100) + ([0.0] * (8760 - 100)),
        }
    )

    ri_result = check_mc(df, mc_key="bulk_tx", utility="rie", year=year, state="ri")
    ny_result = check_mc(df, mc_key="bulk_tx", utility="coned", year=year, state="ny")

    assert not any(
        "expected 100 nonzero hours" in issue for issue in ri_result["issues"]
    )
    assert any("expected 80 nonzero hours" in issue for issue in ny_result["issues"])
