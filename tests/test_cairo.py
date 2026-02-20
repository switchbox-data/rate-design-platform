"""Tests for utils.cairo (Cambium loader and related)."""

from pathlib import Path

import pandas as pd
import pytest

from utils.cairo import (
    _load_cambium_marginal_costs,
    apply_runtime_tou_demand_response,
    assign_hourly_periods,
)

# Example CSV with same structure as Cambium (5 metadata rows, then header + 8760 data rows)
EXAMPLE_CSV = (
    Path(__file__).resolve().parent.parent
    / "rate_design"
    / "ny"
    / "hp_rates"
    / "data"
    / "marginal_costs"
    / "example_marginal_costs.csv"
)


def test_load_cambium_marginal_costs_csv_output_contract() -> None:
    """Load example CSV and assert output shape, index, tz, and column names."""
    if not EXAMPLE_CSV.exists():
        pytest.skip(f"Example CSV not found: {EXAMPLE_CSV}")

    df = _load_cambium_marginal_costs(EXAMPLE_CSV, 2019)

    assert df.shape == (8760, 2), "Expected 8760 rows and 2 cost columns"
    assert df.index.name == "time"
    tz = getattr(df.index, "tz", None)
    assert tz is not None
    assert str(tz) == "EST"
    assert list(df.columns) == [
        "Marginal Energy Costs ($/kWh)",
        "Marginal Capacity Costs ($/kWh)",
    ]
    assert (df >= 0).all().all(), "Costs should be non-negative after $/MWh → $/kWh"


def test_load_cambium_marginal_costs_csv_and_parquet_same_result(
    tmp_path: Path,
) -> None:
    """Same data as CSV vs Parquet yields the same dataframe after loader post-processing."""
    if not EXAMPLE_CSV.exists():
        pytest.skip(f"Example CSV not found: {EXAMPLE_CSV}")

    # Build a Parquet with the same logical content as the CSV (timestamp + 2 cost cols)
    raw = pd.read_csv(
        EXAMPLE_CSV,
        skiprows=5,
        parse_dates=["timestamp"],
    )
    parquet_cols = ["timestamp", "energy_cost_enduse", "capacity_cost_enduse"]
    raw[parquet_cols].to_parquet(tmp_path / "example.parquet", index=False)

    target_year = 2019
    df_csv = _load_cambium_marginal_costs(EXAMPLE_CSV, target_year)
    df_parquet = _load_cambium_marginal_costs(tmp_path / "example.parquet", target_year)

    pd.testing.assert_frame_equal(df_csv, df_parquet)


def _build_single_season_tou_tariff() -> dict:
    schedule = [[1 if h >= 20 else 0 for h in range(24)] for _ in range(12)]
    return {
        "items": [
            {
                "energyratestructure": [
                    [{"rate": 0.10, "adj": 0.0, "unit": "kWh"}],
                    [{"rate": 0.30, "adj": 0.0, "unit": "kWh"}],
                ],
                "energyweekdayschedule": schedule,
                "energyweekendschedule": schedule,
            }
        ]
    }


def _build_seasonal_tou_tariff() -> dict:
    schedule = []
    for month in range(1, 13):
        if month in {1, 2, 3, 4, 5, 10, 11, 12}:  # winter
            schedule.append([1 if h >= 20 else 0 for h in range(24)])
        else:  # summer
            schedule.append([3 if h >= 20 else 2 for h in range(24)])
    return {
        "items": [
            {
                "energyratestructure": [
                    [{"rate": 0.10, "adj": 0.0, "unit": "kWh"}],  # winter offpeak
                    [{"rate": 0.30, "adj": 0.0, "unit": "kWh"}],  # winter peak
                    [{"rate": 0.12, "adj": 0.0, "unit": "kWh"}],  # summer offpeak
                    [{"rate": 0.36, "adj": 0.0, "unit": "kWh"}],  # summer peak
                ],
                "energyweekdayschedule": schedule,
                "energyweekendschedule": schedule,
            }
        ]
    }


def test_runtime_demand_response_shifts_only_tou_customers() -> None:
    idx = pd.MultiIndex.from_product(
        [[1, 2], pd.date_range("2025-01-01", periods=24, freq="h", tz="EST")],
        names=["bldg_id", "time"],
    )
    raw = pd.DataFrame({"electricity_net": 1.0}, index=idx)
    tariff = _build_single_season_tou_tariff()

    shifted, tracker = apply_runtime_tou_demand_response(
        raw_load_elec=raw,
        tou_bldg_ids=[1],
        tou_tariff=tariff,
        demand_elasticity=-0.1,
    )

    pd.testing.assert_series_equal(
        raw.loc[2, "electricity_net"],
        shifted.loc[2, "electricity_net"],
        check_names=False,
    )
    assert shifted.loc[1, "electricity_net"].sum() == pytest.approx(
        raw.loc[1, "electricity_net"].sum()
    )
    periods = assign_hourly_periods(
        pd.DatetimeIndex(raw.loc[1].index), tariff
    ).rename("energy_period")
    before = (
        raw.loc[1]
        .assign(energy_period=periods.values)
        .groupby("energy_period")["electricity_net"]
        .sum()
    )
    after = (
        shifted.loc[1]
        .assign(energy_period=periods.values)
        .groupby("energy_period")["electricity_net"]
        .sum()
    )
    assert after.loc[1] < before.loc[1]
    assert not tracker.empty


def test_runtime_demand_response_infers_seasonal_wrapper() -> None:
    winter = pd.date_range("2025-01-03", periods=24, freq="h", tz="EST")
    summer = pd.date_range("2025-07-03", periods=24, freq="h", tz="EST")
    times = winter.append(summer)
    idx = pd.MultiIndex.from_product([[1], times], names=["bldg_id", "time"])
    raw = pd.DataFrame({"electricity_net": 1.0}, index=idx)
    tariff = _build_seasonal_tou_tariff()

    shifted, tracker = apply_runtime_tou_demand_response(
        raw_load_elec=raw,
        tou_bldg_ids=[1],
        tou_tariff=tariff,
        demand_elasticity=-0.1,
        season_specs=None,
    )

    assert shifted["electricity_net"].sum() == pytest.approx(raw["electricity_net"].sum())
    assert not tracker.empty
    assert any(col.startswith("season_1_period_") for col in tracker.columns)
    assert any(col.startswith("season_2_period_") for col in tracker.columns)
