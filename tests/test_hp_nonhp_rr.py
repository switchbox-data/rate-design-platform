"""Tests for HP/non-HP revenue requirement postprocessing."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.post.compute_hp_nonhp_rr import compute_hp_nonhp_rr, compute_rr_wide


def _write_sample_run_dir(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)

    pl.DataFrame(
        {
            "bldg_id": [1, 1, 2, 2, 3, 3],
            "weight": [1.0] * 6,
            "month": ["Jan", "Annual", "Jan", "Annual", "Jan", "Annual"],
            "bill_level": [10.0, 100.0, 20.0, 200.0, 30.0, 300.0],
            "dollar_year": [2025] * 6,
        }
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")

    pl.DataFrame(
        {
            "bldg_id": [1, 2, 3],
            "customer_level_residual_share_percustomer": [10.0, 20.0, 30.0],
        }
    ).write_csv(run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv")

    pl.DataFrame(
        {
            "bldg_id": [1, 2, 3],
            "postprocess_group.has_hp": [True, False, True],
        }
    ).write_csv(run_dir / "customer_metadata.csv")

    return run_dir


def test_compute_hp_nonhp_rr(tmp_path: Path) -> None:
    run_dir = _write_sample_run_dir(tmp_path)

    breakdown = compute_hp_nonhp_rr(run_dir)
    rr_wide = compute_rr_wide(breakdown)

    hp = breakdown.filter(pl.col("customer_group") == "HP")
    nonhp = breakdown.filter(pl.col("customer_group") == "NonHP")

    assert hp["sum_bills"][0] == pytest.approx(400.0)
    assert hp["sum_cross_subsidy_per_customer"][0] == pytest.approx(40.0)
    assert hp["revenue_requirement"][0] == pytest.approx(360.0)

    assert nonhp["sum_bills"][0] == pytest.approx(200.0)
    assert nonhp["sum_cross_subsidy_per_customer"][0] == pytest.approx(20.0)
    assert nonhp["revenue_requirement"][0] == pytest.approx(180.0)

    assert rr_wide["RR_HP"][0] == pytest.approx(360.0)
    assert rr_wide["RR_NonHP"][0] == pytest.approx(180.0)


def test_compute_hp_nonhp_rr_missing_annual_rows_raises(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "bills").mkdir(parents=True)
    (run_dir / "cross_subsidization").mkdir(parents=True)

    pl.DataFrame(
        {
            "bldg_id": [1],
            "weight": [1.0],
            "month": ["Jan"],
            "bill_level": [100.0],
            "dollar_year": [2025],
        }
    ).write_csv(run_dir / "bills" / "elec_bills_year_target.csv")
    pl.DataFrame(
        {
            "bldg_id": [1],
            "customer_level_residual_share_percustomer": [10.0],
        }
    ).write_csv(run_dir / "cross_subsidization" / "cross_subsidization_BAT_values.csv")
    pl.DataFrame(
        {
            "bldg_id": [1],
            "postprocess_group.has_hp": [True],
        }
    ).write_csv(run_dir / "customer_metadata.csv")

    with pytest.raises(ValueError, match="Missing annual target bills"):
        compute_hp_nonhp_rr(run_dir)
