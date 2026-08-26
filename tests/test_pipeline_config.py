"""Tests for the pipeline YAML loader (rate_design/hp_rates/pipeline_config.py)."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml

from rate_design.hp_rates.pipeline_config import (
    load_pipeline_config,
    validate_preflight_inputs,
)


def _minimal_pipeline_yaml() -> dict[str, Any]:
    """Smallest pipeline YAML that parses, for exercising orchestration fields."""
    return {
        "state": "md",
        "utility": "bge",
        "year": 2025,
        "output_base": "/data.sb/switchbox/cairo/outputs/hp_rates",
        "process_workers": 8,
        "max_concurrent_cairo_runs": 2,
        "resstock": {
            "base": "/ebs/data/nrel/resstock/res_2024_amy2018_2_sb",
            "upgrade_precalc": "00",
            "upgrade_calibrated": "02",
        },
        "marginal_costs": {
            "dist_and_sub_tx": "s3://bucket/dist/data.parquet",
            "bulk_tx": "s3://bucket/bulk/data.parquet",
            "supply_energy": "s3://bucket/energy/data.parquet",
            "supply_capacity": "s3://bucket/capacity/data.parquet",
        },
        "revenue_requirement": {
            "single_rate": "rev_requirement/bge.yaml",
            "single_rate_calibrated": "rev_requirement/bge_large.yaml",
            "multi_rate_calibrated": "rev_requirement/bge_large.yaml",
        },
        "scenarios": {
            "default": {"quartet": "single_rate", "tariff_base": "default"},
        },
    }


def _write(tmp_path: Path, data: dict[str, Any]) -> Path:
    path = tmp_path / "pipeline_test.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


class TestConcurrentVariants:
    """`concurrent_variants` toggles delivery/supply overlap within a stage."""

    def test_defaults_to_sequential_when_absent(self, tmp_path: Path) -> None:
        config = load_pipeline_config(_write(tmp_path, _minimal_pipeline_yaml()))
        assert config.concurrent_variants is False

    @pytest.mark.parametrize("value", [True, False])
    def test_explicit_value_is_respected(self, tmp_path: Path, value: bool) -> None:
        data = _minimal_pipeline_yaml()
        data["concurrent_variants"] = value
        config = load_pipeline_config(_write(tmp_path, data))
        assert config.concurrent_variants is value


class TestBillChangeBaseline:
    """`bill_change_baseline` is post-processing-only, so optional at load."""

    def test_absent_block_parses_to_none(self, tmp_path: Path) -> None:
        config = load_pipeline_config(_write(tmp_path, _minimal_pipeline_yaml()))
        assert config.bill_change_baseline is None

    def test_valid_block_parses(self, tmp_path: Path) -> None:
        data = _minimal_pipeline_yaml()
        data["bill_change_baseline"] = {"scenario": "default", "stage": "precalc"}
        config = load_pipeline_config(_write(tmp_path, data))
        baseline = config.bill_change_baseline
        assert baseline is not None
        assert (baseline.scenario, baseline.stage) == ("default", "precalc")
        assert baseline.segment == "default_precalc"

    def test_unknown_scenario_is_rejected(self, tmp_path: Path) -> None:
        data = _minimal_pipeline_yaml()
        data["bill_change_baseline"] = {"scenario": "nope", "stage": "precalc"}
        with pytest.raises(ValueError, match="not a declared scenario"):
            load_pipeline_config(_write(tmp_path, data))

    def test_unknown_stage_is_rejected(self, tmp_path: Path) -> None:
        data = _minimal_pipeline_yaml()
        data["bill_change_baseline"] = {"scenario": "default", "stage": "final"}
        with pytest.raises(ValueError, match="must be one of"):
            load_pipeline_config(_write(tmp_path, data))

    def test_missing_key_is_rejected(self, tmp_path: Path) -> None:
        data = _minimal_pipeline_yaml()
        data["bill_change_baseline"] = {"scenario": "default"}
        with pytest.raises(ValueError, match="missing required key"):
            load_pipeline_config(_write(tmp_path, data))


class TestMultiRateFixed:
    """`multi_rate_fixed` quartet validation and parsing."""

    def _fixed_scenario_yaml(self) -> dict[str, Any]:
        """Pipeline YAML with a valid multi_rate_fixed scenario."""
        data = _minimal_pipeline_yaml()
        data["scenarios"]["default_rd"] = {
            "quartet": "single_rate",
            "tariff_base": "rd_default",
        }
        data["scenarios"]["hp_rd_vs_default"] = {
            "quartet": "multi_rate_fixed",
            "requires": ["default", "default_rd"],
            "candidate_tariff_scenario": "default_rd",
            "promote": "hp",
            "residual_allocation": {
                "delivery": "candidate_tariff",
                "supply": "candidate_tariff",
            },
            "subclass_config": {
                "group_col": "has_hp",
                "subgroups": {
                    "hp": {
                        "values": ["true"],
                        "structure": "base",
                        "copy_from": "default_rd",
                    },
                    "non-hp": {
                        "values": ["false"],
                        "structure": "base",
                        "copy_from": "default",
                    },
                },
            },
        }
        return data

    def test_valid_fixed_scenario_loads(self, tmp_path: Path) -> None:
        config = load_pipeline_config(_write(tmp_path, self._fixed_scenario_yaml()))
        sc = config.scenario("hp_rd_vs_default")
        assert sc.quartet == "multi_rate_fixed"
        assert sc.requires == ["default", "default_rd"]
        assert sc.candidate_tariff_scenario == "default_rd"
        assert sc.promote == "hp"
        assert sc.depends_on is None
        assert sc.subclass_config is not None
        assert sc.subclass_config.subgroups[0].copy_from == "default_rd"
        assert sc.subclass_config.subgroups[1].copy_from == "default"

    def test_missing_requires_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["requires"]
        with pytest.raises(ValueError, match="requires.*list"):
            load_pipeline_config(_write(tmp_path, data))

    def test_missing_candidate_tariff_scenario_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["candidate_tariff_scenario"]
        with pytest.raises(ValueError, match="candidate_tariff_scenario"):
            load_pipeline_config(_write(tmp_path, data))

    def test_candidate_not_in_requires_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        data["scenarios"]["hp_rd_vs_default"]["candidate_tariff_scenario"] = "other"
        with pytest.raises(ValueError, match="must be in 'requires'"):
            load_pipeline_config(_write(tmp_path, data))

    def test_depends_on_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        data["scenarios"]["hp_rd_vs_default"]["depends_on"] = "default"
        with pytest.raises(ValueError, match="uses 'requires' instead of 'depends_on'"):
            load_pipeline_config(_write(tmp_path, data))

    def test_missing_copy_from_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["subclass_config"]["subgroups"]["hp"][
            "copy_from"
        ]
        with pytest.raises(ValueError, match="requires 'copy_from'"):
            load_pipeline_config(_write(tmp_path, data))

    def test_copy_from_not_in_requires_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        data["scenarios"]["hp_rd_vs_default"]["subclass_config"]["subgroups"]["hp"][
            "copy_from"
        ] = "unknown"
        with pytest.raises(ValueError, match="must be in 'requires'"):
            load_pipeline_config(_write(tmp_path, data))

    def test_missing_promote_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["promote"]
        with pytest.raises(ValueError, match="requires an explicit 'promote'"):
            load_pipeline_config(_write(tmp_path, data))


class TestFuseMountCheck:
    """`validate_preflight_inputs` must fail fast when /data.sb is not mounted."""

    def test_error_when_data_sb_not_mounted(self, tmp_path: Path) -> None:
        config = load_pipeline_config(_write(tmp_path, _minimal_pipeline_yaml()))
        with patch.object(Path, "is_mount", return_value=False):
            errors = validate_preflight_inputs(config)
        mount_errors = [e for e in errors if "/data.sb" in e and "mounted" in e]
        assert len(mount_errors) == 1

    def test_no_mount_error_when_mounted(self, tmp_path: Path) -> None:
        config = load_pipeline_config(_write(tmp_path, _minimal_pipeline_yaml()))
        with patch.object(Path, "is_mount", return_value=True):
            errors = validate_preflight_inputs(config)
        mount_errors = [e for e in errors if "/data.sb" in e and "mounted" in e]
        assert len(mount_errors) == 0

    def test_no_mount_check_for_local_output_base(self, tmp_path: Path) -> None:
        data = _minimal_pipeline_yaml()
        data["output_base"] = "/tmp/outputs"
        config = load_pipeline_config(_write(tmp_path, data))
        errors = validate_preflight_inputs(config)
        mount_errors = [e for e in errors if "mounted" in e]
        assert len(mount_errors) == 0
