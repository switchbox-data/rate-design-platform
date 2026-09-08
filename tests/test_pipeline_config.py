"""Tests for the pipeline YAML loader (rate_design/hp_rates/pipeline_config.py)."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml

from rate_design.hp_rates.pipeline_config import (
    generate_scenarios_yaml,
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
        data["scenarios"]["default_rd_uncalibrated"] = {
            "quartet": "single_rate_uncalibrated",
            "tariff_base": "rd_default",
        }
        data["scenarios"]["hp_rd_vs_default"] = {
            "quartet": "multi_rate_fixed",
            "depends_on": ["default", "default_rd_uncalibrated"],
            "candidate_tariff_scenario": "default_rd_uncalibrated",
            "candidate_tariff_supply_method": "passthrough",
            "bat_allocation_scenario": "default",
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
                        "copy_from": "default_rd_uncalibrated",
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
        assert sc.depends_on == ["default", "default_rd_uncalibrated"]
        assert sc.subclass_config is not None
        assert sc.subclass_config.subgroups[0].copy_from == "default_rd_uncalibrated"
        assert sc.subclass_config.subgroups[1].copy_from == "default"

    def test_missing_depends_on_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["depends_on"]
        with pytest.raises(ValueError, match="depends_on"):
            load_pipeline_config(_write(tmp_path, data))

    def test_missing_candidate_tariff_scenario_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["candidate_tariff_scenario"]
        with pytest.raises(ValueError, match="candidate_tariff_scenario"):
            load_pipeline_config(_write(tmp_path, data))

    def test_candidate_not_in_depends_on_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        data["scenarios"]["hp_rd_vs_default"]["candidate_tariff_scenario"] = "other"
        with pytest.raises(ValueError, match="must be in 'depends_on'"):
            load_pipeline_config(_write(tmp_path, data))

    def test_missing_copy_from_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["subclass_config"]["subgroups"]["hp"][
            "copy_from"
        ]
        with pytest.raises(ValueError, match="requires 'copy_from'"):
            load_pipeline_config(_write(tmp_path, data))

    def test_manual_rr_yaml_bypasses_candidate_tariff_scenario(
        self, tmp_path: Path
    ) -> None:
        """candidate_tariff_rr_yaml_path lets 'depends_on' omit the RD scenario."""
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["candidate_tariff_scenario"]
        data["scenarios"]["hp_rd_vs_default"]["candidate_tariff_rr_yaml_path"] = (
            "rev_requirement/bge_hp_vs_non-hp.yaml"
        )
        config = load_pipeline_config(_write(tmp_path, data))
        sc = config.scenario("hp_rd_vs_default")
        assert sc.candidate_tariff_scenario is None
        assert (
            sc.candidate_tariff_rr_yaml_path == "rev_requirement/bge_hp_vs_non-hp.yaml"
        )

    def test_manual_rr_yaml_and_candidate_tariff_scenario_both_set_rejected(
        self, tmp_path: Path
    ) -> None:
        data = self._fixed_scenario_yaml()
        data["scenarios"]["hp_rd_vs_default"]["candidate_tariff_rr_yaml_path"] = (
            "rev_requirement/bge_hp_vs_non-hp.yaml"
        )
        with pytest.raises(ValueError, match="sets both"):
            load_pipeline_config(_write(tmp_path, data))

    def test_manual_tariff_paths_bypass_copy_from(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        data["scenarios"]["hp_rd_vs_default"]["subclass_config"]["subgroups"]["hp"] = {
            "values": ["true"],
            "structure": "base",
            "tariff_json_path": "tariffs/electric/bge_rd_default_calibrated.json",
            "tariff_json_supply_path": (
                "tariffs/electric/bge_rd_default_supply_calibrated.json"
            ),
        }
        config = load_pipeline_config(_write(tmp_path, data))
        subclass_config = config.scenario("hp_rd_vs_default").subclass_config
        assert subclass_config is not None
        sg = subclass_config.subgroups[0]
        assert sg.copy_from is None
        assert sg.tariff_json_path == "tariffs/electric/bge_rd_default_calibrated.json"

    def test_manual_tariff_path_without_supply_path_rejected(
        self, tmp_path: Path
    ) -> None:
        data = self._fixed_scenario_yaml()
        data["scenarios"]["hp_rd_vs_default"]["subclass_config"]["subgroups"]["hp"] = {
            "values": ["true"],
            "structure": "base",
            "tariff_json_path": "tariffs/electric/bge_rd_default_calibrated.json",
        }
        with pytest.raises(ValueError, match="must both be set or both be omitted"):
            load_pipeline_config(_write(tmp_path, data))

    def test_manual_tariff_path_and_copy_from_both_set_rejected(
        self, tmp_path: Path
    ) -> None:
        data = self._fixed_scenario_yaml()
        data["scenarios"]["hp_rd_vs_default"]["subclass_config"]["subgroups"]["hp"][
            "tariff_json_path"
        ] = "tariffs/electric/bge_rd_default_calibrated.json"
        data["scenarios"]["hp_rd_vs_default"]["subclass_config"]["subgroups"]["hp"][
            "tariff_json_supply_path"
        ] = "tariffs/electric/bge_rd_default_supply_calibrated.json"
        with pytest.raises(ValueError, match="sets both 'copy_from'"):
            load_pipeline_config(_write(tmp_path, data))

    def test_fully_manual_scenario_needs_no_depends_on(self, tmp_path: Path) -> None:
        """When RR yaml + all tariffs are manual, 'depends_on' is optional."""
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["depends_on"]
        del data["scenarios"]["hp_rd_vs_default"]["candidate_tariff_scenario"]
        sc = data["scenarios"]["hp_rd_vs_default"]
        sc["candidate_tariff_rr_yaml_path"] = "rev_requirement/bge_hp_vs_non-hp.yaml"
        sc["subclass_config"]["subgroups"]["hp"] = {
            "values": ["true"],
            "structure": "base",
            "tariff_json_path": "tariffs/electric/bge_rd_default_calibrated.json",
            "tariff_json_supply_path": (
                "tariffs/electric/bge_rd_default_supply_calibrated.json"
            ),
        }
        sc["subclass_config"]["subgroups"]["non-hp"] = {
            "values": ["false"],
            "structure": "base",
            "tariff_json_path": "tariffs/electric/bge_default_calibrated.json",
            "tariff_json_supply_path": (
                "tariffs/electric/bge_default_supply_calibrated.json"
            ),
        }
        config = load_pipeline_config(_write(tmp_path, data))
        assert config.scenario("hp_rd_vs_default").depends_on is None

    def test_fully_manual_tariffs_but_derived_rr_still_needs_depends_on(
        self, tmp_path: Path
    ) -> None:
        """All tariffs manual but RR still derived: 'depends_on' is still needed
        (for candidate_tariff_scenario's precalc bills)."""
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["depends_on"]
        sc = data["scenarios"]["hp_rd_vs_default"]
        sc["subclass_config"]["subgroups"]["hp"] = {
            "values": ["true"],
            "structure": "base",
            "tariff_json_path": "tariffs/electric/bge_rd_default_calibrated.json",
            "tariff_json_supply_path": (
                "tariffs/electric/bge_rd_default_supply_calibrated.json"
            ),
        }
        sc["subclass_config"]["subgroups"]["non-hp"] = {
            "values": ["false"],
            "structure": "base",
            "tariff_json_path": "tariffs/electric/bge_default_calibrated.json",
            "tariff_json_supply_path": (
                "tariffs/electric/bge_default_supply_calibrated.json"
            ),
        }
        with pytest.raises(ValueError, match="depends_on"):
            load_pipeline_config(_write(tmp_path, data))

    def test_copy_from_not_in_depends_on_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        data["scenarios"]["hp_rd_vs_default"]["subclass_config"]["subgroups"]["hp"][
            "copy_from"
        ] = "unknown"
        with pytest.raises(ValueError, match="must be in 'depends_on'"):
            load_pipeline_config(_write(tmp_path, data))

    def test_missing_promote_rejected(self, tmp_path: Path) -> None:
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["promote"]
        with pytest.raises(ValueError, match="requires an explicit 'promote'"):
            load_pipeline_config(_write(tmp_path, data))

    def test_bat_allocation_scenario_parses(self, tmp_path: Path) -> None:
        config = load_pipeline_config(_write(tmp_path, self._fixed_scenario_yaml()))
        assert config.scenario("hp_rd_vs_default").bat_allocation_scenario == "default"

    def test_missing_bat_allocation_scenario_rejected(self, tmp_path: Path) -> None:
        """The BAT-allocation run is never inferred from 'depends_on' order."""
        data = self._fixed_scenario_yaml()
        del data["scenarios"]["hp_rd_vs_default"]["bat_allocation_scenario"]
        with pytest.raises(
            ValueError, match="requires an explicit 'bat_allocation_scenario'"
        ):
            load_pipeline_config(_write(tmp_path, data))

    def test_bat_allocation_scenario_not_in_depends_on_rejected(
        self, tmp_path: Path
    ) -> None:
        data = self._fixed_scenario_yaml()
        data["scenarios"]["hp_rd_vs_default"]["bat_allocation_scenario"] = "other"
        with pytest.raises(ValueError, match="bat_allocation_scenario 'other'"):
            load_pipeline_config(_write(tmp_path, data))

    def test_bat_allocation_scenario_on_non_fixed_quartet_rejected(
        self, tmp_path: Path
    ) -> None:
        data = _minimal_pipeline_yaml()
        data["scenarios"]["default"]["bat_allocation_scenario"] = "default"
        with pytest.raises(ValueError, match="only applies to 'multi_rate_fixed'"):
            load_pipeline_config(_write(tmp_path, data))


class TestDependsOn:
    """``depends_on`` is a string or list; quartet kind decides how many names."""

    def _collapsed_yaml(self) -> dict[str, Any]:
        data = _minimal_pipeline_yaml()
        data["scenarios"]["hp_seasonal"] = {
            "quartet": "multi_rate_collapsed",
            "depends_on": "default",
            "promote": "hp",
            "residual_allocation": {"delivery": "percustomer", "supply": "passthrough"},
            "subclass_config": {
                "group_col": "has_hp",
                "subgroups": {
                    "hp": {"values": ["true"], "structure": "seasonal"},
                    "non-hp": {"values": ["false"], "structure": "base"},
                },
            },
        }
        return data

    def test_string_normalizes_to_one_element_list(self, tmp_path: Path) -> None:
        config = load_pipeline_config(_write(tmp_path, self._collapsed_yaml()))
        assert config.scenario("hp_seasonal").depends_on == ["default"]

    def test_collapsed_rejects_multiple_names(self, tmp_path: Path) -> None:
        data = self._collapsed_yaml()
        data["scenarios"]["hp_seasonal"]["depends_on"] = ["default", "other"]
        with pytest.raises(ValueError, match="must be a single scenario name"):
            load_pipeline_config(_write(tmp_path, data))


class TestSingleRateUncalibrated:
    """``single_rate_uncalibrated``: CAIRO default mode, posted tariff, large RR."""

    def _yaml(self) -> dict[str, Any]:
        data = _minimal_pipeline_yaml()
        data["scenarios"]["default_rd_uncalibrated"] = {
            "quartet": "single_rate_uncalibrated",
            "tariff_base": "rd_default",
        }
        return data

    def test_loads(self, tmp_path: Path) -> None:
        config = load_pipeline_config(_write(tmp_path, self._yaml()))
        sc = config.scenario("default_rd_uncalibrated")
        assert sc.quartet == "single_rate_uncalibrated"
        assert sc.is_single_rate
        assert sc.is_uncalibrated
        assert sc.tariff_base == "rd_default"
        assert not config.scenario("default").is_uncalibrated

    def test_missing_tariff_base_rejected(self, tmp_path: Path) -> None:
        data = self._yaml()
        del data["scenarios"]["default_rd_uncalibrated"]["tariff_base"]
        with pytest.raises(ValueError, match="tariff_base"):
            load_pipeline_config(_write(tmp_path, data))

    def test_subclass_config_rejected(self, tmp_path: Path) -> None:
        data = self._yaml()
        data["scenarios"]["default_rd_uncalibrated"]["subclass_config"] = {
            "group_col": "has_hp",
            "subgroups": {
                "hp": {"values": ["true"], "structure": "base"},
            },
        }
        with pytest.raises(ValueError, match="must not declare a 'subclass_config'"):
            load_pipeline_config(_write(tmp_path, data))

    def test_generated_runs_are_default_mode_with_posted_tariff_and_large_rr(
        self, tmp_path: Path
    ) -> None:
        config = load_pipeline_config(_write(tmp_path, self._yaml()))
        out = tmp_path / "scenarios.yaml"
        generate_scenarios_yaml(
            config, "batch_test", out, scenarios=["default_rd_uncalibrated"]
        )
        doc = yaml.safe_load(out.read_text(encoding="utf-8"))
        runs = doc["runs"]
        assert len(runs) == 4
        large_rr = "rev_requirement/bge_large.yaml"
        for name, run in runs.items():
            assert run["run_type"] == "default", name
            assert run["utility_revenue_requirement"] == large_rr, name
            tariffs = run["path_tariffs_electric"]
            assert "calibrated" not in tariffs["all"], name
            assert tariffs["all"].startswith("tariffs/electric/bge_rd_default"), name

        precalc_d = runs["md_bge_default_rd_uncalibrated_precalc_delivery"]
        cal_d = runs["md_bge_default_rd_uncalibrated_calibrated_delivery"]
        assert precalc_d["path_tariffs_electric"] == cal_d["path_tariffs_electric"]
        assert "upgrade=00" in precalc_d["path_resstock_metadata"]
        assert "upgrade=02" in cal_d["path_resstock_metadata"]

    def test_ordinary_single_rate_precalc_still_solves_to_class_rr(
        self, tmp_path: Path
    ) -> None:
        config = load_pipeline_config(_write(tmp_path, self._yaml()))
        out = tmp_path / "scenarios.yaml"
        generate_scenarios_yaml(config, "batch_test", out, scenarios=["default"])
        runs = yaml.safe_load(out.read_text(encoding="utf-8"))["runs"]
        precalc = runs["md_bge_default_precalc_delivery"]
        calibrated = runs["md_bge_default_calibrated_delivery"]
        assert precalc["run_type"] == "precalc"
        assert precalc["utility_revenue_requirement"] == "rev_requirement/bge.yaml"
        assert calibrated["run_type"] == "default"
        assert (
            calibrated["utility_revenue_requirement"]
            == "rev_requirement/bge_large.yaml"
        )
        assert calibrated["path_tariffs_electric"]["all"].endswith("_calibrated.json")


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
