"""Tests for compare_resstock_eia861_loads (uses ResStock load_curve_annual)."""

from pathlib import Path

import polars as pl
import pytest

from utils.post.compare_resstock_eia861_loads import (
    ANNUAL_ELECTRICITY_COL,
    compare_resstock_eia861,
    EIA_RESIDENTIAL_KWH,
    RESSTOCK_TOTAL_KWH,
)


def test_compare_resstock_eia861_columns_and_ratio(tmp_path: Path) -> None:
    """Comparison table has expected columns and ratio/pct_diff from weighted annual sums."""
    annual = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3],
            ANNUAL_ELECTRICITY_COL: [1000.0, 2000.0, 3000.0],
            "weight": [
                1.0,
                2.0,
                1.0,
            ],  # rie: 1000*1 + 2000*2 = 5000; other: 3000*1 = 3000
        }
    )
    path_annual = tmp_path / "annual.parquet"
    annual.write_parquet(path_annual)

    ua = pl.DataFrame(
        {
            "bldg_id": [1, 2, 3],
            "sb.electric_utility": ["rie", "rie", "other_util"],
        }
    )
    path_ua = tmp_path / "utility_assignment.parquet"
    ua.write_parquet(path_ua)

    eia = pl.DataFrame(
        {
            "utility_code": ["rie", "other_util"],
            "residential_sales_mwh": [4.0, 2.0],
        }
    )
    path_eia = tmp_path / "eia861.parquet"
    eia.write_parquet(path_eia)

    result = compare_resstock_eia861(
        path_annual=str(path_annual),
        path_utility_assignment=str(path_ua),
        path_eia861=str(path_eia),
        storage_options=None,
    )

    assert result.shape[0] == 2
    assert list(result.columns) == [
        "utility_code",
        RESSTOCK_TOTAL_KWH,
        EIA_RESIDENTIAL_KWH,
        "ratio",
        "pct_diff",
    ]

    rie_row = result.filter(pl.col("utility_code") == "rie").to_dicts()[0]
    assert rie_row[RESSTOCK_TOTAL_KWH] == 5000.0  # 1000*1 + 2000*2
    assert rie_row[EIA_RESIDENTIAL_KWH] == 4000.0
    assert rie_row["ratio"] == pytest.approx(1.25)
    assert rie_row["pct_diff"] == pytest.approx(25.0)

    other_row = result.filter(pl.col("utility_code") == "other_util").to_dicts()[0]
    assert other_row[RESSTOCK_TOTAL_KWH] == 3000.0
    assert other_row[EIA_RESIDENTIAL_KWH] == 2000.0
    assert other_row["ratio"] == pytest.approx(1.5)
    assert other_row["pct_diff"] == pytest.approx(50.0)


def test_compare_resstock_eia861_load_column_override(tmp_path: Path) -> None:
    """Uses custom electricity column when passed via load_column."""
    annual = pl.DataFrame(
        {
            "bldg_id": [1],
            "custom_elec_kwh": [300.0],
            "weight": [1.0],
        }
    )
    path_annual = tmp_path / "annual.parquet"
    annual.write_parquet(path_annual)

    path_ua = tmp_path / "utility_assignment.parquet"
    pl.DataFrame({"bldg_id": [1], "sb.electric_utility": ["rie"]}).write_parquet(
        path_ua
    )
    path_eia = tmp_path / "eia861.parquet"
    pl.DataFrame(
        {"utility_code": ["rie"], "residential_sales_mwh": [0.3]}
    ).write_parquet(path_eia)

    result = compare_resstock_eia861(
        path_annual=str(path_annual),
        path_utility_assignment=str(path_ua),
        path_eia861=str(path_eia),
        storage_options=None,
        load_column="custom_elec_kwh",
    )
    ie_row = result.filter(pl.col("utility_code") == "rie").to_dicts()[0]
    assert ie_row[RESSTOCK_TOTAL_KWH] == 300.0


def test_compare_resstock_eia861_missing_electricity_column_raises(
    tmp_path: Path,
) -> None:
    """Raises ValueError when annual parquet lacks the electricity column."""
    annual = pl.DataFrame({"bldg_id": [1], "other_col": [1.0], "weight": [1.0]})
    path_annual = tmp_path / "annual.parquet"
    annual.write_parquet(path_annual)

    path_ua = tmp_path / "utility_assignment.parquet"
    pl.DataFrame({"bldg_id": [1], "sb.electric_utility": ["rie"]}).write_parquet(
        path_ua
    )
    path_eia = tmp_path / "eia861.parquet"
    pl.DataFrame(
        {"utility_code": ["rie"], "residential_sales_mwh": [1.0]}
    ).write_parquet(path_eia)

    with pytest.raises(ValueError, match="missing column"):
        compare_resstock_eia861(
            path_annual=str(path_annual),
            path_utility_assignment=str(path_ua),
            path_eia861=str(path_eia),
            storage_options=None,
        )


def test_compare_resstock_eia861_missing_sb_electric_utility_raises(
    tmp_path: Path,
) -> None:
    """Raises ValueError when utility assignment lacks sb.electric_utility."""
    path_annual = tmp_path / "annual.parquet"
    pl.DataFrame(
        {
            "bldg_id": [1],
            ANNUAL_ELECTRICITY_COL: [1000.0],
            "weight": [1.0],
        }
    ).write_parquet(path_annual)

    ua = pl.DataFrame({"bldg_id": [1], "other_col": ["rie"]})
    path_ua = tmp_path / "utility_assignment.parquet"
    ua.write_parquet(path_ua)
    path_eia = tmp_path / "eia861.parquet"
    pl.DataFrame(
        {"utility_code": ["rie"], "residential_sales_mwh": [1.0]}
    ).write_parquet(path_eia)

    with pytest.raises(ValueError, match="sb.electric_utility"):
        compare_resstock_eia861(
            path_annual=str(path_annual),
            path_utility_assignment=str(path_ua),
            path_eia861=str(path_eia),
            storage_options=None,
        )


def test_compare_resstock_eia861_missing_weight_raises(tmp_path: Path) -> None:
    """Raises ValueError when annual parquet lacks weight column."""
    path_annual = tmp_path / "annual.parquet"
    pl.DataFrame(
        {
            "bldg_id": [1],
            ANNUAL_ELECTRICITY_COL: [1000.0],
        }
    ).write_parquet(path_annual)

    path_ua = tmp_path / "utility_assignment.parquet"
    pl.DataFrame({"bldg_id": [1], "sb.electric_utility": ["rie"]}).write_parquet(
        path_ua
    )
    path_eia = tmp_path / "eia861.parquet"
    pl.DataFrame(
        {"utility_code": ["rie"], "residential_sales_mwh": [1.0]}
    ).write_parquet(path_eia)

    with pytest.raises(ValueError, match="weight"):
        compare_resstock_eia861(
            path_annual=str(path_annual),
            path_utility_assignment=str(path_ua),
            path_eia861=str(path_eia),
            storage_options=None,
        )
