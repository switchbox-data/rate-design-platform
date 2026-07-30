"""Tests for _sb annual / monthly load aggregation helpers."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from data.resstock.load_curve.aggregate_loads import (
    aggregate_hourly_df_to_annual_row,
    aggregate_one_building,
    annual_column_name,
    build_annual_agg_exprs,
    is_annual_metric_column,
    join_aggregated_energy_to_annual,
    monthly_aggregation_exprs,
    select_annual_params_weight_upgrade,
    write_consolidated_annual,
)


def test_is_annual_metric_column() -> None:
    assert is_annual_metric_column("out.electricity.heating.energy_consumption")
    assert is_annual_metric_column("out.load.heating.energy_delivered.kbtu")
    assert not is_annual_metric_column(
        "out.electricity.heating.energy_consumption_intensity"
    )
    assert not is_annual_metric_column("out.outdoor_air_dryblub_temp.c")
    assert not is_annual_metric_column("bldg_id")


def test_annual_column_name_adds_kwh_suffix() -> None:
    assert (
        annual_column_name("out.electricity.heating.energy_consumption")
        == "out.electricity.heating.energy_consumption.kwh"
    )
    assert (
        annual_column_name("out.load.heating.energy_delivered.kbtu")
        == "out.load.heating.energy_delivered.kbtu"
    )


def test_build_annual_agg_exprs_subset_only() -> None:
    rules = {
        "out.electricity.heating.energy_consumption": "sum",
        "out.electricity.heating.energy_consumption_intensity": "sum",
        "out.load.heating.energy_delivered.kbtu": "sum",
        "out.outdoor_air_dryblub_temp.c": "mean",
        "bldg_id": "first",
    }
    schema = list(rules)
    exprs = build_annual_agg_exprs(rules, schema)
    # bldg_id + heating energy + heating delivered (not intensity, not temp)
    aliases = []
    for e in exprs:
        meta = e.meta
        # polars Expr alias via meta.output_name()
        aliases.append(meta.output_name())
    assert aliases == [
        "bldg_id",
        "out.electricity.heating.energy_consumption.kwh",
        "out.load.heating.energy_delivered.kbtu",
    ]


def test_aggregate_hourly_df_to_annual_row_sums() -> None:
    rules = {
        "out.electricity.heating.energy_consumption": "sum",
        "out.load.heating.energy_delivered.kbtu": "sum",
        "bldg_id": "first",
    }
    hourly = pl.DataFrame(
        {
            "bldg_id": [1, 1, 1, 1],
            "out.electricity.heating.energy_consumption": [1.0, 2.0, 3.0, 4.0],
            "out.load.heating.energy_delivered.kbtu": [10.0, 20.0, 30.0, 40.0],
            "out.electricity.heating.energy_consumption_intensity": [
                9.0,
                9.0,
                9.0,
                9.0,
            ],
        }
    )
    annual = aggregate_hourly_df_to_annual_row(hourly, rules)
    assert annual.height == 1
    assert annual["bldg_id"][0] == 1
    assert annual["out.electricity.heating.energy_consumption.kwh"][0] == 10.0
    assert annual["out.load.heating.energy_delivered.kbtu"][0] == 100.0
    assert "out.electricity.heating.energy_consumption_intensity" not in annual.columns


def test_select_annual_params_keeps_upgrade_name() -> None:
    annual = pl.DataFrame(
        {
            "bldg_id": [1],
            "upgrade": [2],
            "weight": [100.0],
            "upgrade_name": ["HP package"],
            "out.params.window_area_ft_2": [10.0],
            "out.electricity.heating.energy_consumption.kwh": [999.0],
            "out.electricity.heating.energy_consumption.kwh.savings": [1.0],
        }
    )
    slim = select_annual_params_weight_upgrade(annual.lazy()).collect()
    assert isinstance(slim, pl.DataFrame)
    assert set(slim.columns) == {
        "bldg_id",
        "upgrade",
        "weight",
        "upgrade_name",
        "out.params.window_area_ft_2",
    }


def test_aggregate_one_building_monthly_and_annual(tmp_path: Path) -> None:
    rules = {
        "out.electricity.heating.energy_consumption": "sum",
        "out.load.heating.energy_delivered.kbtu": "sum",
        "out.outdoor_air_dryblub_temp.c": "mean",
        "bldg_id": "first",
    }
    hourly = pl.DataFrame(
        {
            "bldg_id": [42] * 4,
            "year": [2018] * 4,
            "month": [1, 1, 2, 2],
            "out.electricity.heating.energy_consumption": [1.0, 1.0, 2.0, 2.0],
            "out.load.heating.energy_delivered.kbtu": [4.0, 4.0, 6.0, 6.0],
            "out.outdoor_air_dryblub_temp.c": [0.0, 2.0, 4.0, 6.0],
        }
    )
    src = tmp_path / "42-0.parquet"
    hourly.write_parquet(src)
    monthly_out = tmp_path / "monthly" / "42-0.parquet"
    monthly_exprs = monthly_aggregation_exprs(rules)

    annual_row = aggregate_one_building(
        src,
        add_monthly=True,
        monthly_output_path=monthly_out,
        monthly_exprs=monthly_exprs,
        add_annual=True,
        annual_rules=rules,
    )

    assert monthly_out.exists()
    monthly = pl.read_parquet(monthly_out)
    assert monthly.height == 2
    assert set(monthly["month"].to_list()) == {1, 2}
    # January heating sum
    jan = monthly.filter(pl.col("month") == 1)
    assert jan["out.electricity.heating.energy_consumption"][0] == 2.0
    assert jan["out.load.heating.energy_delivered.kbtu"][0] == 8.0
    assert jan["out.outdoor_air_dryblub_temp.c"][0] == 1.0  # mean of 0 and 2

    assert annual_row is not None
    assert annual_row.height == 1
    assert annual_row["out.electricity.heating.energy_consumption.kwh"][0] == 6.0
    assert annual_row["out.load.heating.energy_delivered.kbtu"][0] == 20.0
    assert "out.outdoor_air_dryblub_temp.c" not in annual_row.columns


def test_write_consolidated_annual(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "load_curve_annual" / "state=CT" / "upgrade=00"
    raw_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "upgrade": [0, 0],
            "weight": [10.0, 20.0],
            "out.params.window_area_ft_2": [1.0, 2.0],
            "out.electricity.heating.energy_consumption.kwh": [999.0, 999.0],
        }
    ).write_parquet(raw_dir / "CT_upgrade00_metadata_and_annual_results.parquet")

    rows = [
        pl.DataFrame(
            {
                "bldg_id": [1],
                "out.electricity.heating.energy_consumption.kwh": [100.0],
                "out.load.heating.energy_delivered.kbtu": [50.0],
            }
        ),
        pl.DataFrame(
            {
                "bldg_id": [2],
                "out.electricity.heating.energy_consumption.kwh": [200.0],
                "out.load.heating.energy_delivered.kbtu": [75.0],
            }
        ),
    ]
    out_root = tmp_path / "sb"
    out_path = write_consolidated_annual(rows, tmp_path / "raw", out_root, "CT", "00")
    assert out_path is not None
    result = pl.read_parquet(out_path)
    assert result.height == 2
    assert "out.params.window_area_ft_2" in result.columns
    assert "weight" in result.columns
    # Rebuilt energy, not the raw 999 placeholder
    by_id = {r["bldg_id"]: r for r in result.to_dicts()}
    assert by_id[1]["out.electricity.heating.energy_consumption.kwh"] == 100.0
    assert by_id[2]["out.load.heating.energy_delivered.kbtu"] == 75.0


def test_join_left_keeps_energy_when_params_missing() -> None:
    energy = pl.DataFrame(
        {"bldg_id": [1], "out.electricity.heating.energy_consumption.kwh": [5.0]}
    ).lazy()
    params = pl.DataFrame({"bldg_id": [2], "upgrade": [0], "weight": [1.0]}).lazy()
    joined = join_aggregated_energy_to_annual(energy, params).collect()
    assert isinstance(joined, pl.DataFrame)
    assert joined.height == 1
    assert joined["bldg_id"][0] == 1
    assert joined["out.electricity.heating.energy_consumption.kwh"][0] == 5.0
    assert joined["weight"][0] is None
