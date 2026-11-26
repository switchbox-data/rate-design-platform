"""Tests for the PJM NITS rate and the PJM load reader."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.data_prep.marginal_costs.inputs import InputContext
from utils.data_prep.marginal_costs.inputs.generic import hive_load
from utils.data_prep.marginal_costs.inputs.pjm import (
    NITS_CSV_PATH,
    compute_blended_nits_rate,
    load_nits_rates,
    pjm_hive_load,
    pjm_nits,
)
from utils.data_prep.marginal_costs.supply_utils import build_cairo_8760_timestamps


def test_blended_nits_rate_day_weight() -> None:
    rates = pl.DataFrame(
        {
            "year": [2025, 2025, 2024, 2024],
            "zone": ["BGE", "BGE", "BGE", "BGE"],
            "effective_date": ["2025-01-01", "2025-06-01", "2024-01-01", "2024-06-01"],
            "nits_rate_kw_yr": [10.0, 20.0, 1.0, 4.0],
        }
    )

    assert compute_blended_nits_rate(rates, "BGE", 2025) == pytest.approx((151 * 10 + 214 * 20) / 365)
    assert compute_blended_nits_rate(rates, "BGE", 2024) == pytest.approx((152 * 1 + 214 * 4) / 366)


def test_load_nits_rates_reads_the_committed_csv() -> None:
    rates = load_nits_rates(NITS_CSV_PATH)
    assert {"year", "zone", "effective_date", "nits_rate_kw_yr"} <= set(rates.columns)
    assert rates.height > 0


def test_pjm_nits_blends_the_utility_zone() -> None:
    ctx = InputContext(utility="bge", year=2025, load_year=2025, storage_options={})
    resolved = pjm_nits({"path": str(NITS_CSV_PATH), "year": 2025}, ctx, "annual_cost_kw_year")
    assert resolved.value > 0
    assert resolved.meta["nits_dollar_year"] == 2025
    assert resolved.inputs[0]["role"] == "nits_rates"


def test_pjm_hive_load_fills_a_leading_gap_with_zero(tmp_path: Path) -> None:
    year = 2025
    clock = build_cairo_8760_timestamps(year).with_columns(pl.col("timestamp").cast(pl.Datetime("us")))
    frame = clock.slice(1).with_columns(pl.lit(10.0).alias("load_mw"))
    partition = tmp_path / "utility=bge" / f"year={year}"
    partition.mkdir(parents=True)
    frame.write_parquet(partition / "data.parquet")
    ctx = InputContext(utility="bge", year=year, load_year=year, storage_options={})
    entry = {"path": str(tmp_path), "load_year": year}

    pjm = pjm_hive_load(entry, ctx, "load")
    hive = hive_load(entry, ctx, "load")

    assert pjm.value["load_mw"][0] == 0.0
    assert hive.value["load_mw"][0] == pytest.approx(10.0)
    assert pjm.meta["load_source"] == "pjm_utility"
