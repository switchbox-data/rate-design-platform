"""Tests for the `multi_rate_fixed` prep tasks in `run_pipeline.py`.

Task functions are called via their `.fn` attribute, which is the plain,
undecorated function Prefect wraps — this exercises the actual logic without
spinning up Prefect's orchestration engine.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from rate_design.hp_rates.pipeline_config import (
    PipelineConfig,
    RunDefaults,
    ScenarioConfig,
    SubclassConfig,
    SubgroupSpec,
)
from rate_design.hp_rates.run_pipeline import (
    _resolve_state_path,
    compute_candidate_tariff_rr_for_fixed,
    prepare_fixed_tariffs,
)


def _run_defaults() -> RunDefaults:
    """Dummy RunDefaults — only used by the derivation path, not the manual one."""
    return RunDefaults(
        resstock_base="/ebs/data/nrel/resstock/res_2024_amy2018_2_sb",
        upgrade_precalc="00",
        upgrade_calibrated="02",
        mc_dist_and_sub_tx="s3://bucket/dist/data.parquet",
        mc_bulk_tx="s3://bucket/bulk/data.parquet",
        mc_supply_energy="s3://bucket/energy/data.parquet",
        mc_supply_capacity="s3://bucket/capacity/data.parquet",
        mc_supply_ancillary="s3://bucket/ancillary/data.parquet",
        rr_single_rate="rev_requirement/bge.yaml",
        rr_single_rate_calibrated="rev_requirement/bge_large.yaml",
        rr_multi_rate_calibrated="rev_requirement/bge_large.yaml",
        solar_pv_compensation="net_metering",
        periods_yaml="periods.yaml",
    )


def _config(tmp_path: Path, scenarios: dict[str, ScenarioConfig]) -> PipelineConfig:
    return PipelineConfig(
        state="md",
        utility="bge",
        year=2025,
        process_workers=8,
        max_concurrent_cairo_runs=2,
        concurrent_variants=False,
        output_base=str(tmp_path / "output"),
        scenarios=scenarios,
        run_defaults=_run_defaults(),
    )


@pytest.fixture(autouse=True)
def _state_config_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point PipelineConfig.state_config_dir at tmp_path for all tests here."""
    config_dir = tmp_path / "config"
    monkeypatch.setattr(
        PipelineConfig, "state_config_dir", property(lambda self: config_dir)
    )
    return config_dir


def _write_urdb_tariff(path: Path, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"items": [{"label": label, "name": label}]}), encoding="utf-8"
    )


class TestResolveStatePath:
    def test_relative_path_joins_state_config_dir(self, tmp_path: Path) -> None:
        config = _config(tmp_path, {})
        result = _resolve_state_path(config, "rev_requirement/foo.yaml")
        assert result == tmp_path / "config" / "rev_requirement" / "foo.yaml"

    def test_absolute_path_overrides_state_config_dir(self, tmp_path: Path) -> None:
        config = _config(tmp_path, {})
        absolute = tmp_path / "elsewhere" / "foo.yaml"
        result = _resolve_state_path(config, str(absolute))
        assert result == absolute


class TestComputeCandidateTariffRrForFixedManual:
    def _fixed_scenario(self, *, rr_yaml_path: str) -> ScenarioConfig:
        return ScenarioConfig(
            name="hp_rd_vs_default",
            quartet="multi_rate_fixed",
            promote="hp",
            candidate_tariff_rr_yaml_path=rr_yaml_path,
            residual_allocation_delivery="candidate_tariff",
            residual_allocation_supply="candidate_tariff",
            subclass_config=SubclassConfig(
                group_col="has_hp",
                subgroups=[
                    SubgroupSpec(
                        alias="hp",
                        values=["true"],
                        structure="base",
                        copy_from=None,
                        tariff_json_path="tariffs/electric/a.json",
                        tariff_json_supply_path="tariffs/electric/a_supply.json",
                    ),
                    SubgroupSpec(
                        alias="non-hp",
                        values=["false"],
                        structure="base",
                        copy_from=None,
                        tariff_json_path="tariffs/electric/b.json",
                        tariff_json_supply_path="tariffs/electric/b_supply.json",
                    ),
                ],
            ),
        )

    def test_copies_manual_rr_yaml_verbatim(self, tmp_path: Path) -> None:
        config_dir = tmp_path / "config"
        manual_rr = config_dir / "rev_requirement" / "manual.yaml"
        manual_rr.parent.mkdir(parents=True)
        manual_rr.write_text(
            yaml.safe_dump({"subclass_revenue_requirements": {"delivery": {}}}),
            encoding="utf-8",
        )

        scenario = self._fixed_scenario(rr_yaml_path="rev_requirement/manual.yaml")
        config = _config(tmp_path, {"hp_rd_vs_default": scenario})

        out_path = compute_candidate_tariff_rr_for_fixed.fn(config, scenario, {})

        # Written to the canonical multi_rate_rr_path destination, not the
        # manual source path.
        assert out_path == config_dir / "rev_requirement" / "bge_hp_vs_non-hp.yaml"
        assert out_path.read_text(encoding="utf-8") == manual_rr.read_text(
            encoding="utf-8"
        )

    def test_manual_path_does_not_touch_req_outputs(self, tmp_path: Path) -> None:
        """No req_outputs entries are needed at all in the fully-manual case."""
        config_dir = tmp_path / "config"
        manual_rr = config_dir / "rev_requirement" / "manual.yaml"
        manual_rr.parent.mkdir(parents=True)
        manual_rr.write_text("subclass_revenue_requirements: {}\n", encoding="utf-8")

        scenario = self._fixed_scenario(rr_yaml_path="rev_requirement/manual.yaml")
        config = _config(tmp_path, {"hp_rd_vs_default": scenario})

        # Empty req_outputs — would raise if the function tried to derive
        # anything from prerequisite scenario outputs.
        out_path = compute_candidate_tariff_rr_for_fixed.fn(config, scenario, {})
        assert out_path.exists()


class TestPrepareFixedTariffsManual:
    def _fixed_scenario(self) -> ScenarioConfig:
        return ScenarioConfig(
            name="hp_rd_vs_default",
            quartet="multi_rate_fixed",
            promote="hp",
            residual_allocation_delivery="candidate_tariff",
            residual_allocation_supply="candidate_tariff",
            subclass_config=SubclassConfig(
                group_col="has_hp",
                subgroups=[
                    SubgroupSpec(
                        alias="hp",
                        values=["true"],
                        structure="base",
                        tariff_json_path="tariffs/electric/manual_hp.json",
                        tariff_json_supply_path="tariffs/electric/manual_hp_supply.json",
                    ),
                    SubgroupSpec(
                        alias="non-hp",
                        values=["false"],
                        structure="base",
                        tariff_json_path="tariffs/electric/manual_nonhp.json",
                        tariff_json_supply_path="tariffs/electric/manual_nonhp_supply.json",
                    ),
                ],
            ),
        )

    def test_copies_and_relabels_manual_tariffs(self, tmp_path: Path) -> None:
        config_dir = tmp_path / "config"
        _write_urdb_tariff(
            config_dir / "tariffs/electric/manual_hp.json", "manual_hp_source"
        )
        _write_urdb_tariff(
            config_dir / "tariffs/electric/manual_hp_supply.json",
            "manual_hp_supply_source",
        )
        _write_urdb_tariff(
            config_dir / "tariffs/electric/manual_nonhp.json", "manual_nonhp_source"
        )
        _write_urdb_tariff(
            config_dir / "tariffs/electric/manual_nonhp_supply.json",
            "manual_nonhp_supply_source",
        )

        scenario = self._fixed_scenario()
        config = _config(tmp_path, {"hp_rd_vs_default": scenario})

        # Empty req_outputs: manual tariff paths need no prerequisite scenario.
        written = prepare_fixed_tariffs.fn(config, scenario, {})
        assert len(written) == 4

        hp_delivery = next(
            p for p in written if "_hp_" in p.name and "_supply" not in p.name
        )
        payload = json.loads(hp_delivery.read_text(encoding="utf-8"))
        # Relabelled to this scenario's own stem, not the manual source's label.
        assert payload["items"][0]["label"] == hp_delivery.stem
        assert payload["items"][0]["label"] != "manual_hp_source"


class TestPrepareFixedTariffsCopyFrom:
    """``copy_from`` uses posted JSON for uncalibrated sources, calibrated otherwise."""

    def _fixed_scenario(self) -> ScenarioConfig:
        return ScenarioConfig(
            name="hp_rd_vs_default",
            quartet="multi_rate_fixed",
            promote="hp",
            depends_on=["default", "default_rd_uncalibrated"],
            residual_allocation_delivery="candidate_tariff",
            residual_allocation_supply="candidate_tariff",
            subclass_config=SubclassConfig(
                group_col="has_hp",
                subgroups=[
                    SubgroupSpec(
                        alias="hp",
                        values=["true"],
                        structure="base",
                        copy_from="default_rd_uncalibrated",
                    ),
                    SubgroupSpec(
                        alias="non-hp",
                        values=["false"],
                        structure="base",
                        copy_from="default",
                    ),
                ],
            ),
        )

    def test_uncalibrated_copy_from_uses_posted_tariff(self, tmp_path: Path) -> None:
        config_dir = tmp_path / "config"
        _write_urdb_tariff(
            config_dir / "tariffs/electric/bge_rd_default.json", "posted_rd"
        )
        # Tag the posted file so we can tell copy_from did not pick *_calibrated.
        posted = json.loads(
            (config_dir / "tariffs/electric/bge_rd_default.json").read_text(
                encoding="utf-8"
            )
        )
        posted["items"][0]["source_tag"] = "posted_rd"
        (config_dir / "tariffs/electric/bge_rd_default.json").write_text(
            json.dumps(posted), encoding="utf-8"
        )
        _write_urdb_tariff(
            config_dir / "tariffs/electric/bge_rd_default_supply.json",
            "posted_rd_supply",
        )
        _write_urdb_tariff(
            config_dir / "tariffs/electric/bge_default_calibrated.json",
            "calibrated_default",
        )
        cal = json.loads(
            (config_dir / "tariffs/electric/bge_default_calibrated.json").read_text(
                encoding="utf-8"
            )
        )
        cal["items"][0]["source_tag"] = "calibrated_default"
        (config_dir / "tariffs/electric/bge_default_calibrated.json").write_text(
            json.dumps(cal), encoding="utf-8"
        )
        _write_urdb_tariff(
            config_dir / "tariffs/electric/bge_default_supply_calibrated.json",
            "calibrated_default_supply",
        )

        scenarios = {
            "default": ScenarioConfig(
                name="default", quartet="single_rate", tariff_base="default"
            ),
            "default_rd_uncalibrated": ScenarioConfig(
                name="default_rd_uncalibrated",
                quartet="single_rate_uncalibrated",
                tariff_base="rd_default",
            ),
            "hp_rd_vs_default": self._fixed_scenario(),
        }
        config = _config(tmp_path, scenarios)
        written = prepare_fixed_tariffs.fn(config, scenarios["hp_rd_vs_default"], {})

        hp_delivery = next(
            p for p in written if "_hp_" in p.name and "_supply" not in p.name
        )
        nonhp_delivery = next(
            p for p in written if "_non-hp_" in p.name and "_supply" not in p.name
        )
        hp_payload = json.loads(hp_delivery.read_text(encoding="utf-8"))
        nonhp_payload = json.loads(nonhp_delivery.read_text(encoding="utf-8"))
        assert hp_payload["items"][0]["source_tag"] == "posted_rd"
        assert nonhp_payload["items"][0]["source_tag"] == "calibrated_default"
        assert hp_payload["items"][0]["label"] == hp_delivery.stem
