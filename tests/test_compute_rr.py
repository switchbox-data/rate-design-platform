"""Tests for compute_rr revenue requirement computation."""

from __future__ import annotations

import subprocess
from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.pre.rev_requirement.compute_rr import (
    _day_weighted_avg_rate,
    _fixed_charge_annual_budget,
    _months_in_range,
    _resstock_monthly_budget,
)


class TestMonthsInRange:
    def test_single_year(self):
        months = _months_in_range("2025-01", "2025-12")
        assert len(months) == 12
        assert months[0] == (2025, 1)
        assert months[-1] == (2025, 12)

    def test_cross_year(self):
        months = _months_in_range("2024-11", "2025-02")
        assert months == [(2024, 11), (2024, 12), (2025, 1), (2025, 2)]


class TestDayWeightedAvgRate:
    def test_uniform_rate(self):
        months = _months_in_range("2025-01", "2025-12")
        rates = {f"2025-{m:02d}": 0.10 for m in range(1, 13)}
        assert _day_weighted_avg_rate(rates, months) == pytest.approx(0.10)

    def test_varying_rates(self):
        months = [(2025, 1), (2025, 2)]
        rates = {"2025-01": 0.10, "2025-02": 0.20}
        result = _day_weighted_avg_rate(rates, months)
        expected = (0.10 * 31 + 0.20 * 28) / (31 + 28)
        assert result == pytest.approx(expected)

    def test_empty_months(self):
        assert _day_weighted_avg_rate({}, []) == 0.0


class TestFixedChargeAnnualBudget:
    def test_per_day(self):
        months = [(2025, 1)]
        rates = {"2025-01": 1.0}
        result = _fixed_charge_annual_budget(rates, months, "$/day", 100)
        assert result == pytest.approx(31.0 * 100)

    def test_per_month(self):
        months = [(2025, 1), (2025, 2)]
        rates = {"2025-01": 10.0, "2025-02": 10.0}
        result = _fixed_charge_annual_budget(rates, months, "$/month", 100)
        assert result == pytest.approx(20.0 * 100)


class TestResstockMonthlyBudget:
    def test_basic(self):
        month_list = [(2025, 1), (2025, 2), (2025, 3)]
        rates = {"2025-01": 0.05, "2025-02": 0.10, "2025-03": 0.08}
        kwh = {1: 1000.0, 2: 900.0, 3: 800.0}
        result = _resstock_monthly_budget(rates, month_list, kwh)
        expected = 0.05 * 1000 + 0.10 * 900 + 0.08 * 800
        assert result == pytest.approx(expected)

    def test_missing_month_defaults_zero(self):
        month_list = [(2025, 1), (2025, 2)]
        rates = {"2025-01": 0.05}
        kwh = {1: 1000.0, 2: 500.0}
        result = _resstock_monthly_budget(rates, month_list, kwh)
        assert result == pytest.approx(0.05 * 1000 + 0.0 * 500)

    def test_full_year(self):
        month_list = _months_in_range("2025-01", "2025-12")
        rates = {f"2025-{m:02d}": 0.10 for m in range(1, 13)}
        kwh = {m: 100.0 for m in range(1, 13)}
        result = _resstock_monthly_budget(rates, month_list, kwh)
        assert result == pytest.approx(0.10 * 100.0 * 12)

    def test_resstock_vs_eia_with_seasonal_variation(self):
        """ResStock mode captures rate×load covariance that EIA mode misses."""
        month_list = _months_in_range("2025-01", "2025-12")
        rates = {}
        kwh = {}
        for m in range(1, 13):
            if m in (12, 1, 2):
                rates[f"2025-{m:02d}"] = 0.15
                kwh[m] = 2000.0
            else:
                rates[f"2025-{m:02d}"] = 0.05
                kwh[m] = 500.0
        resstock_budget = _resstock_monthly_budget(rates, month_list, kwh)
        total_kwh = sum(kwh.values())
        eia_avg_rate = _day_weighted_avg_rate(rates, month_list)
        eia_budget = eia_avg_rate * total_kwh
        assert resstock_budget > eia_budget


def _make_monthly_rates_yaml(
    path: Path, charges: dict, start: str = "2025-01", end: str = "2025-12"
) -> None:
    data = {
        "start_month": start,
        "end_month": end,
        "add_to_drr": {
            "rate_structure": "flat",
            "charges": charges,
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)


def _make_rate_case_rr_yaml(path: Path, utility: str, amount: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump({utility: amount}, f)


def _make_eia_parquet(path: Path, utility: str, kwh: float, customers: int) -> None:
    """Create a minimal EIA-861-style parquet file.

    Uses actual column names: residential_sales_mwh (kwh / 1000) and
    residential_customers.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "utility_code": [utility],
            "residential_sales_mwh": [kwh / 1000.0],
            "residential_customers": [customers],
        }
    ).write_parquet(path)


class TestCLIEIAMode:
    """Test the full CLI in default (EIA) mode."""

    def test_eia_mode_output(self, tmp_path: Path):
        utility = "testutil"
        monthly_rates_path = tmp_path / "monthly_rates.yaml"
        _make_monthly_rates_yaml(
            monthly_rates_path,
            {
                "charge_a": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": {f"2025-{m:02d}": 0.05 for m in range(1, 13)},
                },
            },
        )
        rate_case_path = tmp_path / "rate_case.yaml"
        _make_rate_case_rr_yaml(rate_case_path, utility, 1000000.0)

        eia_path = tmp_path / "eia.parquet"
        _make_eia_parquet(eia_path, utility, 5_000_000.0, 10000)

        output_path = tmp_path / "output.yaml"

        result = subprocess.run(
            [
                "uv",
                "run",
                "python",
                "-m",
                "utils.pre.rev_requirement.compute_rr",
                "--utility",
                utility,
                "--path-monthly-rates",
                str(monthly_rates_path),
                "--path-electric-utility-stats",
                str(eia_path),
                "--path-rate-case-rr",
                str(rate_case_path),
                "--output",
                str(output_path),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"

        with open(output_path) as f:
            out = yaml.safe_load(f)

        assert out["load_method"] == "eia"
        assert out["total_residential_kwh"] == pytest.approx(5_000_000.0)
        assert out["eia_total_residential_kwh"] == pytest.approx(5_000_000.0)
        assert out["delivery_revenue_requirement_from_rate_case"] == 1000000.0
        assert "resstock_monthly_kwh" not in out
        assert "resstock_scale_factor" not in out

        charge_entry = out["delivery_top_ups"]["charge_a"]
        assert charge_entry["budget_method"] == "eia"
        assert charge_entry["total_budget"] == pytest.approx(
            0.05 * 5_000_000.0, rel=1e-4
        )


class TestCLIResstockMode:
    """Test the full CLI in ResStock mode using synthetic local data."""

    def _make_resstock_fixtures(self, tmp_path: Path, utility: str, state: str) -> str:
        """Create minimal ResStock directory structure on disk.

        Returns path_resstock_release.
        """
        release = tmp_path / "resstock_release"
        bldg_ids = [100, 200, 300]
        weights = [1.5, 2.0, 0.5]

        meta_dir = release / "metadata_utility" / f"state={state}"
        meta_dir.mkdir(parents=True)
        pl.DataFrame(
            {
                "bldg_id": bldg_ids,
                "sb.electric_utility": [utility] * 3,
                "weight": weights,
            }
        ).write_parquet(meta_dir / "utility_assignment.parquet")

        loads_dir = release / "load_curve_monthly" / f"state={state}" / "upgrade=00"
        loads_dir.mkdir(parents=True)
        for bldg_id in bldg_ids:
            pl.DataFrame(
                {
                    "bldg_id": [bldg_id] * 12,
                    "month": list(range(1, 13)),
                    "out.electricity.total.energy_consumption": [100.0 + bldg_id * 0.1]
                    * 12,
                }
            ).write_parquet(loads_dir / f"{bldg_id}-0.parquet")

        return str(release)

    def test_resstock_mode_output(self, tmp_path: Path):
        utility = "testutil"
        state = "RI"

        resstock_release = self._make_resstock_fixtures(tmp_path, utility, state)

        monthly_rates_path = tmp_path / "monthly_rates.yaml"
        rates = {f"2025-{m:02d}": 0.05 + m * 0.001 for m in range(1, 13)}
        _make_monthly_rates_yaml(
            monthly_rates_path,
            {"charge_a": {"charge_unit": "$/kWh", "monthly_rates": rates}},
        )

        rate_case_path = tmp_path / "rate_case.yaml"
        _make_rate_case_rr_yaml(rate_case_path, utility, 1000000.0)

        eia_path = tmp_path / "eia.parquet"
        _make_eia_parquet(eia_path, utility, 5_000_000.0, 10000)

        output_path = tmp_path / "output.yaml"

        result = subprocess.run(
            [
                "uv",
                "run",
                "python",
                "-m",
                "utils.pre.rev_requirement.compute_rr",
                "--utility",
                utility,
                "--path-monthly-rates",
                str(monthly_rates_path),
                "--path-electric-utility-stats",
                str(eia_path),
                "--path-rate-case-rr",
                str(rate_case_path),
                "--output",
                str(output_path),
                "--use-resstock-loads",
                "--path-resstock-release",
                resstock_release,
                "--state",
                state,
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"

        with open(output_path) as f:
            out = yaml.safe_load(f)

        assert out["load_method"] == "resstock"
        assert out["eia_total_residential_kwh"] == pytest.approx(5_000_000.0)
        assert out["total_residential_kwh"] != pytest.approx(5_000_000.0)
        assert "resstock_monthly_kwh" in out
        assert len(out["resstock_monthly_kwh"]) == 12
        assert out["resstock_scale_factor"] > 0

        charge_entry = out["delivery_top_ups"]["charge_a"]
        assert charge_entry["budget_method"] == "resstock"

    def test_resstock_flag_requires_args(self, tmp_path: Path):
        """--use-resstock-loads without --path-resstock-release should fail."""
        monthly_rates_path = tmp_path / "monthly_rates.yaml"
        _make_monthly_rates_yaml(monthly_rates_path, {})
        rate_case_path = tmp_path / "rate_case.yaml"
        _make_rate_case_rr_yaml(rate_case_path, "x", 0)
        eia_path = tmp_path / "eia.parquet"
        _make_eia_parquet(eia_path, "x", 1000, 10)

        result = subprocess.run(
            [
                "uv",
                "run",
                "python",
                "-m",
                "utils.pre.rev_requirement.compute_rr",
                "--utility",
                "x",
                "--path-monthly-rates",
                str(monthly_rates_path),
                "--path-electric-utility-stats",
                str(eia_path),
                "--path-rate-case-rr",
                str(rate_case_path),
                "--output",
                str(tmp_path / "out.yaml"),
                "--use-resstock-loads",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert (
            "path-resstock-release" in result.stderr.lower()
            or "required" in result.stderr.lower()
        )
