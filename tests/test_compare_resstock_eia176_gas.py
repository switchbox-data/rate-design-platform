"""Tests for the ResStock vs EIA-176 gas comparison."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utils.post.compare_resstock_eia176_gas import (
    ANNUAL_GAS_COL,
    EIA_RESIDENTIAL_CUSTOMERS,
    EIA_RESIDENTIAL_KWH,
    KWH_PER_MMBTU,
    RESSTOCK_CUSTOMERS,
    RESSTOCK_TOTAL_KWH,
    compare_resstock_eia176_gas,
    eia_year_for_resstock_release,
)


def test_eia_year_comes_from_amy_in_the_release_name() -> None:
    assert eia_year_for_resstock_release("res_2024_amy2018_2_sb") == 2018
    assert eia_year_for_resstock_release("res_2024_AMY2012") == 2012
    with pytest.raises(ValueError, match="amyYYYY"):
        eia_year_for_resstock_release("res_2024_tmy3")


def test_compare_resstock_eia176_gas(tmp_path: Path) -> None:
    annual = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3],
            ANNUAL_GAS_COL: [1000.0, 2000.0, 500.0],
            "weight": [10.0, 5.0, 2.0],
        }
    )
    path_annual = tmp_path / "annual.parquet"
    annual.write_parquet(path_annual)
    path_assignment = tmp_path / "utility_assignment.parquet"
    pl.DataFrame(
        {
            "bldg_id": [1, 2, 3, 4],
            "sb.gas_utility": ["yankee_gas", "yankee_gas", None, "norwich_muni"],
        }
    ).write_parquet(path_assignment)

    # 10 Mcf * 1.0 MMBtu/Mcf * KWH_PER_MMBTU, sales + transport.
    by_consumer = pl.DataFrame(
        {
            "report_year": [2018, 2018, 2018, 2018, 2018],
            "operator_id_eia": [
                "17602792CT",
                "17602792CT",
                "17610359CT",
                "17619018CT",
                "17619865CT",
            ],
            "operating_state": ["CT", "CT", "CT", "CT", "CT"],
            "customer_class": ["residential"] * 5,
            "revenue_class": ["sales", "transport", "sales", "sales", "sales"],
            "consumers": [100, 4, 8, 1, 1],
            "revenue": [1.0, 1.0, 1.0, 1.0, 1.0],
            "volume_mcf": [8.0, 2.0, 1.0, 1.0, 1.0],
        }
    )
    path_consumer = tmp_path / "by_consumer.parquet"
    by_consumer.write_parquet(path_consumer)
    path_disposition = tmp_path / "disposition.parquet"
    pl.DataFrame(
        {
            "operator_id_eia": [
                "17602792CT",
                "17619018CT",
                "17619865CT",
                "17610359CT",
            ],
            "report_year": [2018, 2018, 2018, 2018],
            "operating_state": ["CT", "CT", "CT", "CT"],
            "delivered_gas_heat_content_mmbtu_per_mcf": [1.0, 1.0, 1.0, 2.0],
        }
    ).write_parquet(path_disposition)

    result = compare_resstock_eia176_gas(
        path_annual=str(path_annual),
        path_utility_assignment=str(path_assignment),
        path_by_consumer=str(path_consumer),
        path_disposition=str(path_disposition),
        state="CT",
        year=2018,
    )

    yankee = result.filter(pl.col("utility_code") == "yankee_gas").to_dicts()[0]
    assert yankee[RESSTOCK_TOTAL_KWH] == pytest.approx(1000 * 10 + 2000 * 5)
    assert yankee[EIA_RESIDENTIAL_KWH] == pytest.approx(10.0 * KWH_PER_MMBTU)
    assert yankee[EIA_RESIDENTIAL_CUSTOMERS] == 104
    assert yankee[RESSTOCK_CUSTOMERS] == pytest.approx(15.0)
    assert yankee["kwh_ratio"] == pytest.approx(
        yankee[RESSTOCK_TOTAL_KWH] / yankee[EIA_RESIDENTIAL_KWH]
    )

    norwich = result.filter(pl.col("utility_code") == "norwich_muni")
    assert norwich.height == 0


def test_unknown_state_raises(tmp_path: Path) -> None:
    path_annual = tmp_path / "annual.parquet"
    pl.DataFrame(
        {"bldg_id": [1], ANNUAL_GAS_COL: [1.0], "weight": [1.0]}
    ).write_parquet(path_annual)
    path_assignment = tmp_path / "utility_assignment.parquet"
    pl.DataFrame({"bldg_id": [1], "sb.gas_utility": ["yankee_gas"]}).write_parquet(
        path_assignment
    )
    with pytest.raises(ValueError, match="No EIA-176"):
        compare_resstock_eia176_gas(
            path_annual=str(path_annual),
            path_utility_assignment=str(path_assignment),
            path_by_consumer=str(path_annual),
            path_disposition=str(path_annual),
            state="NY",
            year=2018,
        )
