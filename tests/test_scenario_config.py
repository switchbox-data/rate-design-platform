"""Tests for the revenue requirement parser (utils/scenario_config.py).

V3: Verify the new _parse_utility_revenue_requirement with nested YAML format
produces the correct rr_total, subclass_rr, and run_includes_subclasses.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from utils.scenario_config import (
    RevenueRequirementConfig,
    _parse_utility_revenue_requirement,
)

_CONFIG_DIR = (
    Path(__file__).resolve().parents[1] / "rate_design" / "hp_rates" / "ny" / "config"
)


class TestParserWithNewYAMLFormat:
    """Test _parse_utility_revenue_requirement against new nested YAML files."""

    def test_scalar_delivery_only(self) -> None:
        """Base YAML (cenhud.yaml) with no subclass → scalar rr_total, no subclass_rr."""
        result = _parse_utility_revenue_requirement(
            "rev_requirement/cenhud.yaml",
            _CONFIG_DIR,
            raw_path_tariffs_electric={"all": "tariffs/electric/cenhud_flat.json"},
            add_supply=False,
            run_includes_subclasses=False,
        )
        assert isinstance(result, RevenueRequirementConfig)
        assert result.rr_total == pytest.approx(418_151_641.73)
        assert result.subclass_rr is None
        assert result.run_includes_subclasses is False

    def test_scalar_delivery_plus_supply(self) -> None:
        """Base YAML with add_supply=True → picks total_delivery_and_supply."""
        result = _parse_utility_revenue_requirement(
            "rev_requirement/cenhud.yaml",
            _CONFIG_DIR,
            raw_path_tariffs_electric={
                "all": "tariffs/electric/cenhud_flat_supply.json"
            },
            add_supply=True,
            run_includes_subclasses=False,
        )
        assert result.rr_total == pytest.approx(646_688_375.32)
        assert result.subclass_rr is None

    def test_subclass_delivery_only(self) -> None:
        """Subclass YAML (hp_vs_nonhp), delivery-only → picks delivery per subclass."""
        result = _parse_utility_revenue_requirement(
            "rev_requirement/cenhud_hp_vs_nonhp.yaml",
            _CONFIG_DIR,
            raw_path_tariffs_electric={
                "hp": "tariffs/electric/cenhud_hp_seasonal.json",
                "non-hp": "tariffs/electric/cenhud_flat.json",
            },
            add_supply=False,
            run_includes_subclasses=True,
            residual_allocation_delivery="percustomer",
            residual_allocation_supply="passthrough",
        )
        assert result.run_includes_subclasses is True
        assert result.subclass_rr is not None
        assert set(result.subclass_rr.keys()) == {
            "cenhud_hp_seasonal",
            "cenhud_flat",
        }
        assert all(v > 0 for v in result.subclass_rr.values())
        assert sum(result.subclass_rr.values()) == pytest.approx(result.rr_total)

    def test_subclass_delivery_plus_supply(self) -> None:
        """Subclass YAML with add_supply=True → picks total per subclass."""
        result = _parse_utility_revenue_requirement(
            "rev_requirement/cenhud_hp_vs_nonhp.yaml",
            _CONFIG_DIR,
            raw_path_tariffs_electric={
                "hp": "tariffs/electric/cenhud_hp_seasonal_supply.json",
                "non-hp": "tariffs/electric/cenhud_flat_supply.json",
            },
            add_supply=True,
            run_includes_subclasses=True,
            residual_allocation_delivery="percustomer",
            residual_allocation_supply="passthrough",
        )
        assert result.subclass_rr is not None
        assert set(result.subclass_rr.keys()) == {
            "cenhud_hp_seasonal_supply",
            "cenhud_flat_supply",
        }
        assert all(v > 0 for v in result.subclass_rr.values())
        assert sum(result.subclass_rr.values()) == pytest.approx(result.rr_total)

    def test_subclass_flex_delivery_only(self) -> None:
        """Subclass with flex tariff keys, delivery-only."""
        result = _parse_utility_revenue_requirement(
            "rev_requirement/cenhud_hp_vs_nonhp.yaml",
            _CONFIG_DIR,
            raw_path_tariffs_electric={
                "hp": "tariffs/electric/cenhud_hp_seasonalTOU_flex.json",
                "non-hp": "tariffs/electric/cenhud_flat.json",
            },
            add_supply=False,
            run_includes_subclasses=True,
            residual_allocation_delivery="percustomer",
            residual_allocation_supply="passthrough",
        )
        assert result.subclass_rr is not None
        assert set(result.subclass_rr.keys()) == {
            "cenhud_hp_seasonalTOU_flex",
            "cenhud_flat",
        }
        assert all(v > 0 for v in result.subclass_rr.values())
        assert sum(result.subclass_rr.values()) == pytest.approx(result.rr_total)


class TestParserWithSyntheticYAML:
    """Test parser edge cases with synthetic YAML files."""

    def test_missing_total_raises(self, tmp_path: Path) -> None:
        rr_yaml = tmp_path / "bad.yaml"
        rr_yaml.write_text(yaml.safe_dump({"utility": "test"}), encoding="utf-8")
        with pytest.raises(ValueError, match="must contain"):
            _parse_utility_revenue_requirement(
                str(rr_yaml),
                tmp_path,
                raw_path_tariffs_electric={"all": "tariffs/test.json"},
                add_supply=False,
                run_includes_subclasses=False,
            )

    def test_subclass_run_without_subclass_data_raises(self, tmp_path: Path) -> None:
        rr_yaml = tmp_path / "no_subclass.yaml"
        rr_yaml.write_text(
            yaml.safe_dump({"total_delivery_revenue_requirement": 100.0}),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="run_includes_subclasses is true"):
            _parse_utility_revenue_requirement(
                str(rr_yaml),
                tmp_path,
                raw_path_tariffs_electric={
                    "hp": "tariffs/hp.json",
                    "non-hp": "tariffs/nonhp.json",
                },
                add_supply=False,
                run_includes_subclasses=True,
                residual_allocation_delivery="percustomer",
                residual_allocation_supply="passthrough",
            )

    def test_optional_fields_absent(self, tmp_path: Path) -> None:
        """When test_year_customer_count / resstock_kwh_scale_factor are absent, fields are None."""
        rr_yaml = tmp_path / "no_overrides.yaml"
        rr_yaml.write_text(
            yaml.safe_dump({"total_delivery_revenue_requirement": 500.0}),
            encoding="utf-8",
        )
        result = _parse_utility_revenue_requirement(
            str(rr_yaml),
            tmp_path,
            raw_path_tariffs_electric={"all": "tariffs/test.json"},
            add_supply=False,
            run_includes_subclasses=False,
        )
        assert result.customer_count_override is None
        assert result.kwh_scale_factor is None

    def test_optional_fields_present(self, tmp_path: Path) -> None:
        """When test_year_customer_count / resstock_kwh_scale_factor are present, they propagate."""
        rr_yaml = tmp_path / "with_overrides.yaml"
        rr_yaml.write_text(
            yaml.safe_dump(
                {
                    "total_delivery_revenue_requirement": 500.0,
                    "test_year_customer_count": 419347.83,
                    "resstock_kwh_scale_factor": 0.9568,
                }
            ),
            encoding="utf-8",
        )
        result = _parse_utility_revenue_requirement(
            str(rr_yaml),
            tmp_path,
            raw_path_tariffs_electric={"all": "tariffs/test.json"},
            add_supply=False,
            run_includes_subclasses=False,
        )
        assert result.customer_count_override == pytest.approx(419347.83)
        assert result.kwh_scale_factor == pytest.approx(0.9568)

    def test_nested_subclass_format(self, tmp_path: Path) -> None:
        """Delivery/supply format with separate blocks."""
        rr_yaml = tmp_path / "nested.yaml"
        rr_yaml.write_text(
            yaml.safe_dump(
                {
                    "total_delivery_revenue_requirement": 1000.0,
                    "total_delivery_and_supply_revenue_requirement": 1500.0,
                    "subclass_revenue_requirements": {
                        "delivery": {
                            "percustomer": {"hp": 300.0, "non-hp": 700.0},
                        },
                        "supply": {
                            "passthrough": {"hp": 200.0, "non-hp": 300.0},
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        result = _parse_utility_revenue_requirement(
            str(rr_yaml),
            tmp_path,
            raw_path_tariffs_electric={
                "hp": "tariffs/hp_tariff.json",
                "non-hp": "tariffs/nonhp_tariff.json",
            },
            add_supply=False,
            run_includes_subclasses=True,
            residual_allocation_delivery="percustomer",
            residual_allocation_supply="passthrough",
        )
        assert result.rr_total == pytest.approx(1000.0)
        assert result.subclass_rr == {"hp_tariff": 300.0, "nonhp_tariff": 700.0}

        result_supply = _parse_utility_revenue_requirement(
            str(rr_yaml),
            tmp_path,
            raw_path_tariffs_electric={
                "hp": "tariffs/hp_supply.json",
                "non-hp": "tariffs/nonhp_supply.json",
            },
            add_supply=True,
            run_includes_subclasses=True,
            residual_allocation_delivery="percustomer",
            residual_allocation_supply="passthrough",
        )
        assert result_supply.rr_total == pytest.approx(1500.0)
        assert result_supply.subclass_rr == {
            "hp_supply": 500.0,
            "nonhp_supply": 1000.0,
        }
