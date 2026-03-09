"""Unit tests for post-hoc gas bill calculation.

Uses synthetic data — no S3, no real data, runs in milliseconds.
"""

from __future__ import annotations

import math
from typing import cast

import polars as pl
import pytest

from utils.post.gas_bills import (
    GAS_CONSUMPTION_COL,
    _normalize_tariff_json,
    build_fixed_charge_table,
    build_rate_table,
    compute_gas_bills,
)
from utils.post.io import ANNUAL_MONTH, BLDG_ID


# ---------------------------------------------------------------------------
# Tariff fixtures
# ---------------------------------------------------------------------------


def _make_tiered_tariff(
    *,
    fixed: float = 20.0,
    periods: list[list[dict]],
    schedule_map: dict[int, int],
) -> dict:
    """Build a minimal URDB-like gas tariff dict.

    *periods*: list of period definitions, each a list of tier dicts with
      ``rate`` and optionally ``max``.
    *schedule_map*: month (0-based) → period index.
    """
    schedule = [[schedule_map.get(m, 0)] * 24 for m in range(12)]
    return {
        "energyratestructure": periods,
        "energyweekdayschedule": schedule,
        "energyweekendschedule": schedule,
        "fixedchargefirstmeter": fixed,
        "fixedchargeunits": "$/month",
    }


def _make_null_tariff() -> dict:
    schedule = [[0] * 24 for _ in range(12)]
    return {
        "energyratestructure": [[{"rate": 0.0, "unit": "kWh"}]],
        "energyweekdayschedule": schedule,
        "energyweekendschedule": schedule,
        "fixedchargefirstmeter": 0.0,
        "fixedchargeunits": "$/month",
    }


def _make_flat_tariff(rate: float = 0.08, fixed: float = 15.0) -> dict:
    """Single-tier flat rate for all months (RI-style)."""
    schedule_map = {m: 0 for m in range(12)}
    return _make_tiered_tariff(
        fixed=fixed,
        periods=[[{"rate": rate, "unit": "kWh"}]],
        schedule_map=schedule_map,
    )


@pytest.fixture()
def two_tier_tariff() -> dict[str, dict]:
    """Two periods: winter (months 1-3, 10-12) and summer (months 4-9).

    Winter: 2 tiers — first 100 kWh at $0.05, rest at $0.10.
    Summer: 2 tiers — first 100 kWh at $0.04, rest at $0.08.
    """
    winter_period = [
        {"rate": 0.05, "unit": "kWh", "max": 100},
        {"rate": 0.10, "unit": "kWh"},
    ]
    summer_period = [
        {"rate": 0.04, "unit": "kWh", "max": 100},
        {"rate": 0.08, "unit": "kWh"},
    ]
    schedule_map = {
        0: 0,
        1: 0,
        2: 0,  # Jan-Mar → winter (period 0)
        3: 1,
        4: 1,
        5: 1,
        6: 1,
        7: 1,
        8: 1,  # Apr-Sep → summer (period 1)
        9: 0,
        10: 0,
        11: 0,  # Oct-Dec → winter (period 0)
    }
    tariff = _make_tiered_tariff(
        fixed=20.0,
        periods=[winter_period, summer_period],
        schedule_map=schedule_map,
    )
    return {"two_tier": tariff, "null_gas_tariff": _make_null_tariff()}


@pytest.fixture()
def flat_tariff() -> dict[str, dict]:
    return {
        "flat_gas": _make_flat_tariff(rate=0.08, fixed=15.0),
        "null_gas_tariff": _make_null_tariff(),
    }


# ---------------------------------------------------------------------------
# Synthetic load data
# ---------------------------------------------------------------------------


def _make_load_curves(bldg_ids: list[int], monthly_kwh: float) -> pl.LazyFrame:
    """Build a synthetic load_curve_monthly LazyFrame (12 rows per building)."""
    rows = [
        {BLDG_ID: bid, "month": m, GAS_CONSUMPTION_COL: monthly_kwh}
        for bid in bldg_ids
        for m in range(1, 13)
    ]
    return pl.DataFrame(
        rows,
        schema={
            BLDG_ID: pl.Int64,
            "month": pl.Int8,
            GAS_CONSUMPTION_COL: pl.Float64,
        },
    ).lazy()


# ---------------------------------------------------------------------------
# Tests: build_rate_table
# ---------------------------------------------------------------------------


class TestBuildRateTable:
    def test_tiered_structure(self, two_tier_tariff: dict[str, dict]) -> None:
        rt = build_rate_table(two_tier_tariff)

        # Two tariffs × 12 months; two_tier has 2 tiers/month, null has 1 tier/month
        assert rt.height == 2 * 12 + 1 * 12

        # Check winter month (Jan=1) for two_tier
        jan = rt.filter(
            (pl.col("tariff_key") == "two_tier") & (pl.col("month") == 1)
        ).sort("tier")
        assert jan.height == 2
        assert jan["tier_floor_kwh"][0] == 0.0
        assert jan["tier_ceiling_kwh"][0] == 100.0
        assert jan["rate_per_kwh"][0] == pytest.approx(0.05)
        assert jan["tier_floor_kwh"][1] == 100.0
        assert math.isinf(jan["tier_ceiling_kwh"][1])
        assert jan["rate_per_kwh"][1] == pytest.approx(0.10)

    def test_summer_period(self, two_tier_tariff: dict[str, dict]) -> None:
        rt = build_rate_table(two_tier_tariff)
        jun = rt.filter(
            (pl.col("tariff_key") == "two_tier") & (pl.col("month") == 6)
        ).sort("tier")
        assert jun["rate_per_kwh"][0] == pytest.approx(0.04)
        assert jun["rate_per_kwh"][1] == pytest.approx(0.08)

    def test_null_tariff_zero_rate(self, two_tier_tariff: dict[str, dict]) -> None:
        rt = build_rate_table(two_tier_tariff)
        null_rows = rt.filter(pl.col("tariff_key") == "null_gas_tariff")
        assert null_rows.height == 12
        assert (null_rows["rate_per_kwh"] == 0.0).all()


class TestBuildFixedChargeTable:
    def test_values(self, two_tier_tariff: dict[str, dict]) -> None:
        fc = build_fixed_charge_table(two_tier_tariff)
        fc_dict = {
            r["tariff_key"]: r["gas_fixed_charge"] for r in fc.iter_rows(named=True)
        }
        assert fc_dict["two_tier"] == pytest.approx(20.0)
        assert fc_dict["null_gas_tariff"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Tests: compute_gas_bills
# ---------------------------------------------------------------------------


class TestComputeGasBills:
    def test_tiered_bill_calculation(self, two_tier_tariff: dict[str, dict]) -> None:
        """150 kWh/month in winter: first 100 at $0.05 + next 50 at $0.10 = $10/month."""
        rt = build_rate_table(two_tier_tariff)
        fc = build_fixed_charge_table(two_tier_tariff)
        tariff_map = pl.DataFrame({BLDG_ID: [1], "tariff_key": ["two_tier"]})
        lc = _make_load_curves([1], monthly_kwh=150.0)

        result = cast(pl.DataFrame, compute_gas_bills(lc, tariff_map, rt, fc).collect())

        assert result.height == 13  # 12 months + Annual

        # January (winter): 100 * 0.05 + 50 * 0.10 = 10.0
        jan = result.filter(pl.col("month") == "Jan")
        assert jan["gas_volumetric_bill"][0] == pytest.approx(10.0)
        assert jan["gas_fixed_charge"][0] == pytest.approx(20.0)
        assert jan["gas_total_bill"][0] == pytest.approx(30.0)

        # June (summer): 100 * 0.04 + 50 * 0.08 = 8.0
        jun = result.filter(pl.col("month") == "Jun")
        assert jun["gas_volumetric_bill"][0] == pytest.approx(8.0)
        assert jun["gas_total_bill"][0] == pytest.approx(28.0)

        # Annual: 6 winter months * 30 + 6 summer months * 28 = 180 + 168 = 348
        annual = result.filter(pl.col("month") == ANNUAL_MONTH)
        assert annual["gas_total_bill"][0] == pytest.approx(348.0)
        assert annual["gas_fixed_charge"][0] == pytest.approx(240.0)  # 12 * 20
        assert annual["gas_volumetric_bill"][0] == pytest.approx(108.0)  # 6*10 + 6*8

    def test_flat_tariff(self, flat_tariff: dict[str, dict]) -> None:
        """200 kWh/month at flat $0.08 + $15 fixed = $31/month."""
        rt = build_rate_table(flat_tariff)
        fc = build_fixed_charge_table(flat_tariff)
        tariff_map = pl.DataFrame({BLDG_ID: [1], "tariff_key": ["flat_gas"]})
        lc = _make_load_curves([1], monthly_kwh=200.0)

        result = cast(pl.DataFrame, compute_gas_bills(lc, tariff_map, rt, fc).collect())

        jan = result.filter(pl.col("month") == "Jan")
        assert jan["gas_volumetric_bill"][0] == pytest.approx(16.0)
        assert jan["gas_fixed_charge"][0] == pytest.approx(15.0)
        assert jan["gas_total_bill"][0] == pytest.approx(31.0)

        annual = result.filter(pl.col("month") == ANNUAL_MONTH)
        assert annual["gas_total_bill"][0] == pytest.approx(12 * 31.0)

    def test_null_gas_tariff_zero_bill(self, two_tier_tariff: dict[str, dict]) -> None:
        """Buildings with null_gas_tariff get zero gas bills."""
        rt = build_rate_table(two_tier_tariff)
        fc = build_fixed_charge_table(two_tier_tariff)
        tariff_map = pl.DataFrame({BLDG_ID: [1], "tariff_key": ["null_gas_tariff"]})
        lc = _make_load_curves([1], monthly_kwh=500.0)

        result = cast(pl.DataFrame, compute_gas_bills(lc, tariff_map, rt, fc).collect())

        assert (result["gas_total_bill"] == 0.0).all()
        assert (result["gas_fixed_charge"] == 0.0).all()
        assert (result["gas_volumetric_bill"] == 0.0).all()

    def test_consumption_below_first_tier(
        self, two_tier_tariff: dict[str, dict]
    ) -> None:
        """50 kWh/month in winter: all in tier 1 at $0.05 = $2.50."""
        rt = build_rate_table(two_tier_tariff)
        fc = build_fixed_charge_table(two_tier_tariff)
        tariff_map = pl.DataFrame({BLDG_ID: [1], "tariff_key": ["two_tier"]})
        lc = _make_load_curves([1], monthly_kwh=50.0)

        result = cast(pl.DataFrame, compute_gas_bills(lc, tariff_map, rt, fc).collect())
        jan = result.filter(pl.col("month") == "Jan")
        assert jan["gas_volumetric_bill"][0] == pytest.approx(2.5)

    def test_multiple_buildings(self, two_tier_tariff: dict[str, dict]) -> None:
        """Multiple buildings with different tariffs."""
        rt = build_rate_table(two_tier_tariff)
        fc = build_fixed_charge_table(two_tier_tariff)
        tariff_map = pl.DataFrame(
            {
                BLDG_ID: [1, 2],
                "tariff_key": ["two_tier", "null_gas_tariff"],
            }
        )
        lc = _make_load_curves([1, 2], monthly_kwh=150.0)

        result = cast(pl.DataFrame, compute_gas_bills(lc, tariff_map, rt, fc).collect())
        assert result.height == 26  # 2 buildings * 13 rows

        # Building 1 has real bills
        b1_annual = result.filter(
            (pl.col(BLDG_ID) == 1) & (pl.col("month") == ANNUAL_MONTH)
        )
        assert b1_annual["gas_total_bill"][0] > 0

        # Building 2 has zero bills
        b2 = result.filter(pl.col(BLDG_ID) == 2)
        assert (b2["gas_total_bill"] == 0.0).all()

    def test_zero_consumption(self, flat_tariff: dict[str, dict]) -> None:
        """Zero gas consumption still pays fixed charge."""
        rt = build_rate_table(flat_tariff)
        fc = build_fixed_charge_table(flat_tariff)
        tariff_map = pl.DataFrame({BLDG_ID: [1], "tariff_key": ["flat_gas"]})
        lc = _make_load_curves([1], monthly_kwh=0.0)

        result = cast(pl.DataFrame, compute_gas_bills(lc, tariff_map, rt, fc).collect())
        jan = result.filter(pl.col("month") == "Jan")
        assert jan["gas_volumetric_bill"][0] == pytest.approx(0.0)
        assert jan["gas_fixed_charge"][0] == pytest.approx(15.0)
        assert jan["gas_total_bill"][0] == pytest.approx(15.0)


# ---------------------------------------------------------------------------
# Tests: _normalize_tariff_json
# ---------------------------------------------------------------------------


class TestNormalizeTariffJson:
    def test_items_wrapper(self) -> None:
        raw = {"items": [{"energyratestructure": [[{"rate": 0.05}]]}]}
        result = _normalize_tariff_json(raw)
        assert "energyratestructure" in result
        assert "items" not in result

    def test_top_level(self) -> None:
        raw = {"energyratestructure": [[{"rate": 0.05}]]}
        result = _normalize_tariff_json(raw)
        assert result is raw
