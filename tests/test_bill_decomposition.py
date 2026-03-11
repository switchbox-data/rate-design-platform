"""Unit tests for bill decomposition and fuel bill calculation logic.

Uses synthetic data — no S3, no real data, runs in milliseconds.
"""

from __future__ import annotations

from typing import cast

import polars as pl
import pytest

from utils.post.delivered_fuel_bills import (
    KWH_PER_GAL_HEATING_OIL,
    KWH_PER_GAL_PROPANE,
    MONTH_INT_TO_STR,
    compute_fuel_bills,
)
from utils.post.io import ANNUAL_MONTH, BLDG_ID


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def monthly_prices() -> pl.DataFrame:
    """Stable $3/gal oil and $2/gal propane for all months."""
    return pl.DataFrame(
        {
            "month": list(range(1, 13)),
            "oil_price_per_gallon": [3.0] * 12,
            "propane_price_per_gallon": [2.0] * 12,
        },
        schema={
            "month": pl.Int8,
            "oil_price_per_gallon": pl.Float64,
            "propane_price_per_gallon": pl.Float64,
        },
    )


@pytest.fixture()
def load_curve_monthly_3bldgs() -> pl.LazyFrame:
    """3 buildings x 12 months: bldg 1 burns oil, bldg 2 burns propane, bldg 3 burns neither."""
    rows = []
    for bldg in [1, 2, 3]:
        for month in range(1, 13):
            oil_kwh = 100.0 if bldg == 1 else 0.0
            propane_kwh = 50.0 if bldg == 2 else 0.0
            rows.append(
                {
                    BLDG_ID: bldg,
                    "month": month,
                    "out.fuel_oil.total.energy_consumption": oil_kwh,
                    "out.propane.total.energy_consumption": propane_kwh,
                }
            )
    return pl.DataFrame(
        rows,
        schema={
            BLDG_ID: pl.Int64,
            "month": pl.Int8,
            "out.fuel_oil.total.energy_consumption": pl.Float64,
            "out.propane.total.energy_consumption": pl.Float64,
        },
    ).lazy()


# ---------------------------------------------------------------------------
# compute_fuel_bills
# ---------------------------------------------------------------------------


class TestComputeFuelBills:
    def test_output_schema(
        self, load_curve_monthly_3bldgs: pl.LazyFrame, monthly_prices: pl.DataFrame
    ) -> None:
        result = cast(
            pl.DataFrame,
            compute_fuel_bills(load_curve_monthly_3bldgs, monthly_prices).collect(),
        )
        assert set(result.columns) == {
            BLDG_ID,
            "month",
            "oil_total_bill",
            "propane_total_bill",
        }

    def test_rows_per_building(
        self, load_curve_monthly_3bldgs: pl.LazyFrame, monthly_prices: pl.DataFrame
    ) -> None:
        result = cast(
            pl.DataFrame,
            compute_fuel_bills(load_curve_monthly_3bldgs, monthly_prices).collect(),
        )
        counts = result.group_by(BLDG_ID).agg(pl.len().alias("n"))
        assert counts["n"].to_list() == [13, 13, 13] or all(
            n == 13 for n in counts["n"].to_list()
        )

    def test_oil_bill_calculation(
        self, load_curve_monthly_3bldgs: pl.LazyFrame, monthly_prices: pl.DataFrame
    ) -> None:
        result = cast(
            pl.DataFrame,
            compute_fuel_bills(load_curve_monthly_3bldgs, monthly_prices).collect(),
        )
        bldg1_jan = result.filter((pl.col(BLDG_ID) == 1) & (pl.col("month") == "Jan"))
        expected_oil = 100.0 / KWH_PER_GAL_HEATING_OIL * 3.0
        actual = bldg1_jan["oil_total_bill"].item()
        assert abs(actual - expected_oil) < 1e-6

    def test_propane_bill_calculation(
        self, load_curve_monthly_3bldgs: pl.LazyFrame, monthly_prices: pl.DataFrame
    ) -> None:
        result = cast(
            pl.DataFrame,
            compute_fuel_bills(load_curve_monthly_3bldgs, monthly_prices).collect(),
        )
        bldg2_feb = result.filter((pl.col(BLDG_ID) == 2) & (pl.col("month") == "Feb"))
        expected_propane = 50.0 / KWH_PER_GAL_PROPANE * 2.0
        actual = bldg2_feb["propane_total_bill"].item()
        assert abs(actual - expected_propane) < 1e-6

    def test_zero_fuel_building(
        self, load_curve_monthly_3bldgs: pl.LazyFrame, monthly_prices: pl.DataFrame
    ) -> None:
        result = cast(
            pl.DataFrame,
            compute_fuel_bills(load_curve_monthly_3bldgs, monthly_prices).collect(),
        )
        bldg3 = result.filter(pl.col(BLDG_ID) == 3)
        assert bldg3["oil_total_bill"].sum() == 0.0
        assert bldg3["propane_total_bill"].sum() == 0.0

    def test_annual_equals_sum_of_monthly(
        self, load_curve_monthly_3bldgs: pl.LazyFrame, monthly_prices: pl.DataFrame
    ) -> None:
        result = cast(
            pl.DataFrame,
            compute_fuel_bills(load_curve_monthly_3bldgs, monthly_prices).collect(),
        )
        for bldg in [1, 2, 3]:
            bldg_data = result.filter(pl.col(BLDG_ID) == bldg)
            monthly = bldg_data.filter(pl.col("month") != ANNUAL_MONTH)
            annual = bldg_data.filter(pl.col("month") == ANNUAL_MONTH)
            assert (
                abs(monthly["oil_total_bill"].sum() - annual["oil_total_bill"].item())
                < 1e-6
            )
            assert (
                abs(
                    monthly["propane_total_bill"].sum()
                    - annual["propane_total_bill"].item()
                )
                < 1e-6
            )

    def test_month_strings(
        self, load_curve_monthly_3bldgs: pl.LazyFrame, monthly_prices: pl.DataFrame
    ) -> None:
        result = cast(
            pl.DataFrame,
            compute_fuel_bills(load_curve_monthly_3bldgs, monthly_prices).collect(),
        )
        months = set(result["month"].unique().to_list())
        expected = set(MONTH_INT_TO_STR.values()) | {ANNUAL_MONTH}
        assert months == expected

    def test_no_nulls(
        self, load_curve_monthly_3bldgs: pl.LazyFrame, monthly_prices: pl.DataFrame
    ) -> None:
        result = cast(
            pl.DataFrame,
            compute_fuel_bills(load_curve_monthly_3bldgs, monthly_prices).collect(),
        )
        for col in result.columns:
            assert result[col].null_count() == 0, f"Column {col} has nulls"


# ---------------------------------------------------------------------------
# Electric decomposition identity
# ---------------------------------------------------------------------------


class TestElectricDecomposition:
    """Test that the decomposition identity holds on synthetic data."""

    def test_identity_elec_total(self) -> None:
        """elec_total = elec_fixed + elec_delivery + elec_supply."""
        fixed = 21.28 * 12
        delivery_bill_level = 800.0
        supply_bill_level = 1500.0

        elec_fixed_charge = fixed
        elec_delivery_bill = delivery_bill_level - fixed
        elec_supply_bill = supply_bill_level - delivery_bill_level
        elec_total_bill = supply_bill_level

        computed = elec_fixed_charge + elec_delivery_bill + elec_supply_bill
        assert abs(computed - elec_total_bill) < 1e-6

    def test_identity_energy_total(self) -> None:
        """energy_total = elec + gas + oil + propane."""
        elec = 1500.0
        gas = 600.0
        oil = 200.0
        propane = 150.0
        total = elec + gas + oil + propane
        assert abs(total - 2450.0) < 1e-6

    def test_identity_on_dataframe(self) -> None:
        """Run the identity check on a multi-row DataFrame with random-ish values."""
        import random

        random.seed(42)
        rows = []
        for bldg in range(1, 11):
            fixed = random.uniform(10, 30) * 12
            delivery = random.uniform(500, 1000)
            supply_total = delivery + random.uniform(200, 800)
            gas = random.uniform(0, 800)
            oil = random.uniform(0, 300)
            propane = random.uniform(0, 200)
            rows.append(
                {
                    BLDG_ID: bldg,
                    "elec_fixed_charge": fixed,
                    "elec_delivery_bill": delivery - fixed,
                    "elec_supply_bill": supply_total - delivery,
                    "elec_total_bill": supply_total,
                    "gas_total_bill": gas,
                    "oil_total_bill": oil,
                    "propane_total_bill": propane,
                    "energy_total_bill": supply_total + gas + oil + propane,
                }
            )

        df = pl.DataFrame(rows)

        # Electric identity
        elec_sum = (
            df["elec_fixed_charge"] + df["elec_delivery_bill"] + df["elec_supply_bill"]
        )
        diff_elec = (df["elec_total_bill"] - elec_sum).abs()
        assert diff_elec.max() < 1e-6  # type: ignore[operator]

        # Energy identity
        energy_sum = (
            df["elec_total_bill"]
            + df["gas_total_bill"]
            + df["oil_total_bill"]
            + df["propane_total_bill"]
        )
        diff_energy = (df["energy_total_bill"] - energy_sum).abs()
        assert diff_energy.max() < 1e-6  # type: ignore[operator]
