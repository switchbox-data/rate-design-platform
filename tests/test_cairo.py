"""Tests for utils.cairo (Cambium loader and related)."""

from pathlib import Path

import pandas as pd
import pytest

from utils.cairo import _load_cambium_marginal_costs

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
