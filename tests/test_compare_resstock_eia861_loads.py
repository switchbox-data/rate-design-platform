"""Tests for compare_resstock_eia861_loads (uses ResStock hourly loads, not metadata)."""

from pathlib import Path

import polars as pl
import pytest

from utils.post.compare_resstock_eia861_loads import (
    compare_resstock_eia861,
    EIA_RESIDENTIAL_KWH,
    LOAD_COL_PREFERRED,
    RESSTOCK_TOTAL_KWH,
)


def _write_hourly_load_file(
    path: Path, annual_kwh: float, col: str = LOAD_COL_PREFERRED
) -> None:
    """Write a tiny hourly load parquet: 3 hours so sum = annual_kwh (per-hour = annual_kwh/3)."""
    per_hour = annual_kwh / 3.0
    pl.DataFrame({col: [per_hour, per_hour, per_hour]}).write_parquet(path)


def test_compare_resstock_eia861_columns_and_ratio(tmp_path: Path) -> None:
    """Comparison table has expected columns and ratio/pct_diff from load sums."""
    loads_dir = tmp_path / "loads"
    loads_dir.mkdir()
    _write_hourly_load_file(loads_dir / "1-00.parquet", 1000.0)
    _write_hourly_load_file(loads_dir / "2-00.parquet", 2000.0)
    _write_hourly_load_file(loads_dir / "3-00.parquet", 3000.0)

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
        path_loads=str(loads_dir),
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
    assert rie_row[RESSTOCK_TOTAL_KWH] == 3000.0  # 1000 + 2000
    assert rie_row[EIA_RESIDENTIAL_KWH] == 4000.0
    assert rie_row["ratio"] == pytest.approx(0.75)
    assert rie_row["pct_diff"] == pytest.approx(-25.0)

    other_row = result.filter(pl.col("utility_code") == "other_util").to_dicts()[0]
    assert other_row[RESSTOCK_TOTAL_KWH] == 3000.0
    assert other_row[EIA_RESIDENTIAL_KWH] == 2000.0
    assert other_row["ratio"] == pytest.approx(1.5)
    assert other_row["pct_diff"] == pytest.approx(50.0)


def test_compare_resstock_eia861_load_column_fallback(tmp_path: Path) -> None:
    """Uses total_fuel_electricity when out.electricity.total.energy_consumption is absent."""
    loads_dir = tmp_path / "loads"
    loads_dir.mkdir()
    pl.DataFrame({"total_fuel_electricity": [100.0, 200.0]}).write_parquet(
        loads_dir / "1-00.parquet"
    )

    ua = pl.DataFrame({"bldg_id": [1], "sb.electric_utility": ["rie"]})
    path_ua = tmp_path / "utility_assignment.parquet"
    ua.write_parquet(path_ua)
    eia = pl.DataFrame({"utility_code": ["rie"], "residential_sales_mwh": [0.3]})
    path_eia = tmp_path / "eia861.parquet"
    eia.write_parquet(path_eia)

    result = compare_resstock_eia861(
        path_loads=str(loads_dir),
        path_utility_assignment=str(path_ua),
        path_eia861=str(path_eia),
        storage_options=None,
    )
    rie_row = result.filter(pl.col("utility_code") == "rie").to_dicts()[0]
    assert rie_row[RESSTOCK_TOTAL_KWH] == 300.0


def test_compare_resstock_eia861_missing_load_column_raises(tmp_path: Path) -> None:
    """Raises ValueError when a load parquet lacks the electricity column."""
    loads_dir = tmp_path / "loads"
    loads_dir.mkdir()
    pl.DataFrame({"other_col": [1.0, 2.0]}).write_parquet(loads_dir / "1-00.parquet")

    path_ua = tmp_path / "utility_assignment.parquet"
    pl.DataFrame({"bldg_id": [1], "sb.electric_utility": ["rie"]}).write_parquet(
        path_ua
    )
    path_eia = tmp_path / "eia861.parquet"
    pl.DataFrame(
        {"utility_code": ["rie"], "residential_sales_mwh": [1.0]}
    ).write_parquet(path_eia)

    with pytest.raises(ValueError, match="Load parquet is missing column"):
        compare_resstock_eia861(
            path_loads=str(loads_dir),
            path_utility_assignment=str(path_ua),
            path_eia861=str(path_eia),
            storage_options=None,
        )


def test_compare_resstock_eia861_missing_sb_electric_utility_raises(
    tmp_path: Path,
) -> None:
    """Raises ValueError when utility assignment lacks sb.electric_utility."""
    loads_dir = tmp_path / "loads"
    loads_dir.mkdir()
    _write_hourly_load_file(loads_dir / "1-00.parquet", 1000.0)

    ua = pl.DataFrame({"bldg_id": [1], "other_col": ["rie"]})
    path_ua = tmp_path / "utility_assignment.parquet"
    ua.write_parquet(path_ua)
    path_eia = tmp_path / "eia861.parquet"
    pl.DataFrame(
        {"utility_code": ["rie"], "residential_sales_mwh": [1.0]}
    ).write_parquet(path_eia)

    with pytest.raises(ValueError, match="sb.electric_utility"):
        compare_resstock_eia861(
            path_loads=str(loads_dir),
            path_utility_assignment=str(path_ua),
            path_eia861=str(path_eia),
            storage_options=None,
        )


def test_compare_resstock_eia861_empty_loads_dir_raises(tmp_path: Path) -> None:
    """Raises ValueError when loads directory has no parquet files."""
    loads_dir = tmp_path / "loads"
    loads_dir.mkdir()
    path_ua = tmp_path / "utility_assignment.parquet"
    pl.DataFrame({"bldg_id": [1], "sb.electric_utility": ["rie"]}).write_parquet(
        path_ua
    )
    path_eia = tmp_path / "eia861.parquet"
    pl.DataFrame(
        {"utility_code": ["rie"], "residential_sales_mwh": [1.0]}
    ).write_parquet(path_eia)

    with pytest.raises(ValueError, match="No parquet files found"):
        compare_resstock_eia861(
            path_loads=str(loads_dir),
            path_utility_assignment=str(path_ua),
            path_eia861=str(path_eia),
            storage_options=None,
        )
