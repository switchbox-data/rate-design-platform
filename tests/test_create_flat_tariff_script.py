from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.pre.create_flat_tariffs import (
    _avg,
    _extract_fixed_charges,
    _monthly_to_monthly_dollar,
    _process_utility,
    main,
)


# ---------------------------------------------------------------------------
# Fixture helpers — self-contained dummy data, no S3 / real data calls
# ---------------------------------------------------------------------------


def _make_eia_parquet(path: Path, utility: str, customers: int) -> None:
    """Write a minimal EIA-861-style parquet that _resolve_customer_count can read."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "utility_code": [utility],
            "residential_sales_mwh": [1000.0],
            "residential_customers": [customers],
        }
    ).write_parquet(path)


def _make_monthly_rates_yaml(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(content, default_flow_style=False), encoding="utf-8")


def _make_rev_req_yaml(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(content, default_flow_style=False), encoding="utf-8")


def _uniform_monthly_rates(rate: float, year: int = 2025) -> dict[str, float]:
    """12 identical monthly rates keyed 'YYYY-MM'."""
    return {f"{year}-{m:02d}": rate for m in range(1, 13)}


# ---------------------------------------------------------------------------
# Unit tests: _monthly_to_monthly_dollar
# ---------------------------------------------------------------------------


class TestMonthlyToMonthlyDollar:
    def test_passthrough_for_dollar_per_month(self) -> None:
        rates = {"2025-01": 5.0, "2025-02": 6.0}
        assert _monthly_to_monthly_dollar(rates, "$/month") == rates

    def test_dollar_per_day_scales_by_days_in_month(self) -> None:
        rates = {"2025-01": 1.0, "2025-02": 1.0}
        result = _monthly_to_monthly_dollar(rates, "$/day")
        assert result["2025-01"] == 31.0  # Jan has 31 days
        assert result["2025-02"] == 28.0  # Feb 2025 has 28 days

    def test_rejects_unsupported_unit(self) -> None:
        with pytest.raises(ValueError, match="Cannot convert"):
            _monthly_to_monthly_dollar({"2025-01": 1.0}, "$/kWh")


# ---------------------------------------------------------------------------
# Unit tests: _extract_fixed_charges
# ---------------------------------------------------------------------------


class TestExtractFixedCharges:
    def test_sums_dollar_per_month_charges(self) -> None:
        section = {
            "charges": {
                "customer_charge": {
                    "charge_unit": "$/month",
                    "monthly_rates": _uniform_monthly_rates(6.0),
                },
                "liheap": {
                    "charge_unit": "$/month",
                    "monthly_rates": _uniform_monthly_rates(0.79),
                },
            }
        }
        result = _extract_fixed_charges(section)
        assert len(result) == 12
        assert result["2025-01"] == pytest.approx(6.79)

    def test_includes_dollar_per_day_as_monthly(self) -> None:
        section = {
            "charges": {
                "daily_fee": {
                    "charge_unit": "$/day",
                    "monthly_rates": {"2025-01": 0.10, "2025-02": 0.10},
                },
            }
        }
        result = _extract_fixed_charges(section)
        assert result["2025-01"] == pytest.approx(3.10)  # 0.10 * 31
        assert result["2025-02"] == pytest.approx(2.80)  # 0.10 * 28

    def test_ignores_volumetric_charges(self) -> None:
        section = {
            "charges": {
                "delivery": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform_monthly_rates(0.05),
                },
            }
        }
        assert _extract_fixed_charges(section) == {}

    def test_empty_section(self) -> None:
        assert _extract_fixed_charges({}) == {}
        assert _extract_fixed_charges({"charges": {}}) == {}

    def test_skips_nested_seasonal_structures(self) -> None:
        section = {
            "charges": {
                "weird_charge": {
                    "charge_unit": "$/month",
                    "monthly_rates": {
                        "winter": {"2025-01": 5.0},
                        "summer": {"2025-07": 3.0},
                    },
                },
            }
        }
        assert _extract_fixed_charges(section) == {}


# ---------------------------------------------------------------------------
# Unit tests: _avg
# ---------------------------------------------------------------------------


class TestAvg:
    def test_simple_average(self) -> None:
        assert _avg({"a": 2.0, "b": 4.0}) == pytest.approx(3.0)

    def test_empty_returns_zero(self) -> None:
        assert _avg({}) == 0.0

    def test_single_value(self) -> None:
        assert _avg({"x": 7.5}) == pytest.approx(7.5)


# ---------------------------------------------------------------------------
# Integration test: _process_utility (self-contained fixtures)
# ---------------------------------------------------------------------------


class TestProcessUtility:
    """Test the core per-utility logic with dummy YAML + EIA parquet."""

    def _setup_fixtures(
        self,
        tmp_path: Path,
        *,
        utility: str = "testutil",
        fixed_charge: float = 6.0,
        delivery_rr: float = 100_000.0,
        supply_rr: float = 50_000.0,
        total_kwh: float = 1_000_000.0,
        customers: int = 1000,
        eia_year: int = 2024,
    ) -> tuple[Path, Path, str, Path]:
        """Create all files needed by _process_utility and return their paths."""
        mr_path = tmp_path / f"{utility}_monthly_rates_2025.yaml"
        _make_monthly_rates_yaml(
            mr_path,
            {
                "utility": utility,
                "start_month": "2025-01",
                "end_month": "2025-12",
                "already_in_drr": {
                    "charges": {
                        "customer_charge": {
                            "charge_unit": "$/month",
                            "monthly_rates": _uniform_monthly_rates(fixed_charge),
                        },
                        "core_delivery": {
                            "charge_unit": "$/kWh",
                            "monthly_rates": _uniform_monthly_rates(0.05),
                        },
                    }
                },
                "add_to_drr": {
                    "charges": {
                        "transmission": {
                            "charge_unit": "$/kWh",
                            "monthly_rates": _uniform_monthly_rates(0.04),
                        },
                    }
                },
            },
        )

        rr_path = tmp_path / "rr" / f"{utility}.yaml"
        _make_rev_req_yaml(
            rr_path,
            {
                "utility": utility,
                "total_delivery_revenue_requirement": delivery_rr,
                "total_residential_kwh": total_kwh,
                "supply_revenue_requirement_topups": supply_rr,
                "eia_year": eia_year,
            },
        )

        eia_path = tmp_path / "eia" / "data.parquet"
        _make_eia_parquet(eia_path, utility, customers)

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        return mr_path, rr_path, str(eia_path), output_dir

    def test_generates_flat_and_flat_supply(self, tmp_path: Path) -> None:
        mr_path, rr_path, eia_path, output_dir = self._setup_fixtures(tmp_path)

        _process_utility("testutil", mr_path, rr_path, eia_path, output_dir)

        flat = output_dir / "testutil_flat.json"
        flat_supply = output_dir / "testutil_flat_supply.json"
        assert flat.exists()
        assert flat_supply.exists()

    def test_flat_tariff_structure(self, tmp_path: Path) -> None:
        mr_path, rr_path, eia_path, output_dir = self._setup_fixtures(tmp_path)
        _process_utility("testutil", mr_path, rr_path, eia_path, output_dir)

        payload = json.loads((output_dir / "testutil_flat.json").read_text())
        item = payload["items"][0]
        assert item["label"] == "testutil_flat"
        assert item["name"] == "testutil_flat"
        assert item["fixedchargeunits"] == "$/month"
        assert "energyratestructure" in item
        assert "energyweekdayschedule" in item

    def test_volumetric_rate_derivation(self, tmp_path: Path) -> None:
        """delivery_vol = (delivery_rr - fixed_revenue) / total_kwh."""
        delivery_rr = 100_000.0
        supply_rr = 50_000.0
        total_kwh = 1_000_000.0
        fixed_charge = 6.0
        customers = 1000

        mr_path, rr_path, eia_path, output_dir = self._setup_fixtures(
            tmp_path,
            delivery_rr=delivery_rr,
            supply_rr=supply_rr,
            total_kwh=total_kwh,
            fixed_charge=fixed_charge,
            customers=customers,
        )
        _process_utility("testutil", mr_path, rr_path, eia_path, output_dir)

        fixed_revenue = fixed_charge * customers * 12
        expected_delivery_vol = (delivery_rr - fixed_revenue) / total_kwh
        expected_supply_vol = supply_rr / total_kwh

        flat = json.loads((output_dir / "testutil_flat.json").read_text())
        flat_rate = flat["items"][0]["energyratestructure"][0][0]["rate"]
        assert flat_rate == pytest.approx(expected_delivery_vol, abs=1e-6)

        flat_supply = json.loads((output_dir / "testutil_flat_supply.json").read_text())
        combined_rate = flat_supply["items"][0]["energyratestructure"][0][0]["rate"]
        assert combined_rate == pytest.approx(
            expected_delivery_vol + expected_supply_vol, abs=1e-6
        )

    def test_fixed_charge_from_monthly_rates(self, tmp_path: Path) -> None:
        mr_path, rr_path, eia_path, output_dir = self._setup_fixtures(
            tmp_path, fixed_charge=10.50
        )
        _process_utility("testutil", mr_path, rr_path, eia_path, output_dir)

        flat = json.loads((output_dir / "testutil_flat.json").read_text())
        assert flat["items"][0]["fixedchargefirstmeter"] == 10.50


# ---------------------------------------------------------------------------
# CLI integration test: main()
# ---------------------------------------------------------------------------


class TestCLI:
    """Test the full CLI entrypoint with self-contained dummy data."""

    def _setup_cli_fixtures(
        self, tmp_path: Path, utility: str = "acme"
    ) -> tuple[Path, Path, Path, Path]:
        """Build the directory layout that main() expects and return dirs."""
        mr_dir = tmp_path / "monthly_rates"
        mr_dir.mkdir()
        _make_monthly_rates_yaml(
            mr_dir / f"{utility}_monthly_rates_2025.yaml",
            {
                "utility": utility,
                "start_month": "2025-01",
                "end_month": "2025-12",
                "already_in_drr": {
                    "charges": {
                        "customer_charge": {
                            "charge_unit": "$/month",
                            "monthly_rates": _uniform_monthly_rates(5.0),
                        },
                    }
                },
                "add_to_drr": {
                    "charges": {
                        "fee": {
                            "charge_unit": "$/kWh",
                            "monthly_rates": _uniform_monthly_rates(0.01),
                        },
                    }
                },
            },
        )

        rr_dir = tmp_path / "rev_req"
        rr_dir.mkdir()
        _make_rev_req_yaml(
            rr_dir / f"{utility}.yaml",
            {
                "utility": utility,
                "total_delivery_revenue_requirement": 200_000.0,
                "total_residential_kwh": 2_000_000.0,
                "supply_revenue_requirement_topups": 80_000.0,
                "eia_year": 2024,
            },
        )

        eia_path = tmp_path / "eia" / "data.parquet"
        _make_eia_parquet(eia_path, utility, 500)

        output_dir = tmp_path / "output"

        return mr_dir, rr_dir, eia_path, output_dir

    def test_produces_flat_tariffs_for_discovered_utility(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mr_dir, rr_dir, eia_path, output_dir = self._setup_cli_fixtures(tmp_path)

        monkeypatch.setattr(
            "sys.argv",
            [
                "create_flat_tariffs.py",
                "--monthly-rates-dir",
                str(mr_dir),
                "--rev-requirement-dir",
                str(rr_dir),
                "--output-dir",
                str(output_dir),
                "--eia-path",
                str(eia_path),
                "--year",
                "2025",
            ],
        )
        main()

        assert (output_dir / "acme_flat.json").exists()
        assert (output_dir / "acme_flat_supply.json").exists()

    def test_multiple_utilities(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the monthly-rates dir has files for two utilities, both get tariffs."""
        mr_dir = tmp_path / "monthly_rates"
        mr_dir.mkdir()
        rr_dir = tmp_path / "rev_req"
        rr_dir.mkdir()

        for util in ("alpha", "beta"):
            _make_monthly_rates_yaml(
                mr_dir / f"{util}_monthly_rates_2025.yaml",
                {
                    "utility": util,
                    "already_in_drr": {"charges": {}},
                    "add_to_drr": {"charges": {}},
                },
            )
            _make_rev_req_yaml(
                rr_dir / f"{util}.yaml",
                {
                    "utility": util,
                    "total_delivery_revenue_requirement": 50_000.0,
                    "total_residential_kwh": 500_000.0,
                    "supply_revenue_requirement_topups": 20_000.0,
                    "eia_year": 2024,
                },
            )

        eia_parquet = tmp_path / "eia" / "data.parquet"
        eia_parquet.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "utility_code": ["alpha", "beta"],
                "residential_sales_mwh": [1000.0, 2000.0],
                "residential_customers": [300, 600],
            }
        ).write_parquet(eia_parquet)

        output_dir = tmp_path / "output"
        monkeypatch.setattr(
            "sys.argv",
            [
                "create_flat_tariffs.py",
                "--monthly-rates-dir",
                str(mr_dir),
                "--rev-requirement-dir",
                str(rr_dir),
                "--output-dir",
                str(output_dir),
                "--eia-path",
                str(eia_parquet),
            ],
        )
        main()

        for util in ("alpha", "beta"):
            assert (output_dir / f"{util}_flat.json").exists()
            assert (output_dir / f"{util}_flat_supply.json").exists()

    def test_missing_rev_req_skips_utility(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If rev-requirement YAML is missing for a utility, it is skipped (not crashed)."""
        mr_dir, _, eia_path, output_dir = self._setup_cli_fixtures(tmp_path)
        empty_rr_dir = tmp_path / "empty_rr"
        empty_rr_dir.mkdir()

        monkeypatch.setattr(
            "sys.argv",
            [
                "create_flat_tariffs.py",
                "--monthly-rates-dir",
                str(mr_dir),
                "--rev-requirement-dir",
                str(empty_rr_dir),
                "--output-dir",
                str(output_dir),
                "--eia-path",
                str(eia_path),
            ],
        )
        main()

        assert not (output_dir / "acme_flat.json").exists()

    def test_no_matching_files_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        monkeypatch.setattr(
            "sys.argv",
            [
                "create_flat_tariffs.py",
                "--monthly-rates-dir",
                str(empty_dir),
                "--rev-requirement-dir",
                str(empty_dir),
                "--output-dir",
                str(tmp_path / "out"),
                "--eia-path",
                str(tmp_path / "eia.parquet"),
            ],
        )
        with pytest.raises(FileNotFoundError, match="No files matching"):
            main()
