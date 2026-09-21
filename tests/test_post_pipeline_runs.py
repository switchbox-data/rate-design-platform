"""Tests for run discovery used by the Prefect master-table builders."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from rate_design.hp_rates.pipeline_config import (
    PipelineConfig,
    canonical_run_name,
    load_pipeline_config,
)
from utils.post.baseline_bills import baseline_ref_root
from utils.post.pipeline_runs import (
    baseline_segment,
    batch_dir,
    build_order,
    expected_segments,
    find_run_pairs,
    master_segment,
    s3_uri,
    upgrade_for_stage,
)

BATCH = "md_20260803_a"


def _pipeline_yaml(output_base: Path) -> dict[str, Any]:
    """Two-scenario pipeline YAML whose runs live under a local output base."""
    return {
        "state": "md",
        "utility": "bge",
        "year": 2025,
        "output_base": str(output_base),
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
            "hp_seasonal": {"quartet": "single_rate", "tariff_base": "hp_seasonal"},
        },
        "bill_change_baseline": {"scenario": "default", "stage": "precalc"},
    }


def _load(tmp_path: Path, data: dict[str, Any]) -> PipelineConfig:
    path = tmp_path / "pipeline_bge.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return load_pipeline_config(path)


def _config(tmp_path: Path) -> PipelineConfig:
    return _load(tmp_path, _pipeline_yaml(tmp_path / "outputs"))


def _write_run_index(
    config: PipelineConfig, scenario: str, stage: str, *variants: str
) -> None:
    """Record completed runs the way the Prefect pipeline does."""
    runs_dir = Path(batch_dir(config, BATCH)) / ".runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    for variant in variants:
        name = canonical_run_name(
            config.state, config.utility, scenario, stage, variant
        )
        target = (
            f"/data.sb/switchbox/cairo/outputs/hp_rates/md/bge/{BATCH}/"
            f"20260803_120000_{name}"
        )
        (runs_dir / f"{name}.path").write_text(f"{target}\n", encoding="utf-8")


class TestS3Uri:
    """Run directories are recorded as FUSE paths but read as S3 URIs."""

    def test_translates_fuse_path(self) -> None:
        assert s3_uri("/data.sb/switchbox/cairo/x") == "s3://data.sb/switchbox/cairo/x"

    def test_is_idempotent(self) -> None:
        assert s3_uri("s3://data.sb/switchbox/x") == "s3://data.sb/switchbox/x"

    def test_translates_mount_root(self) -> None:
        assert s3_uri("/data.sb") == "s3://data.sb/"

    def test_rejects_unmappable_path(self) -> None:
        with pytest.raises(ValueError, match="Cannot map"):
            s3_uri("/tmp/somewhere/else")


class TestSegments:
    """Master tables are keyed by ``{scenario}_{stage}``."""

    def test_segment_name(self) -> None:
        assert master_segment("default", "precalc") == "default_precalc"

    def test_expected_segments_cover_both_stages(self, tmp_path: Path) -> None:
        assert expected_segments(_config(tmp_path)) == [
            "default_precalc",
            "default_calibrated",
            "hp_seasonal_precalc",
            "hp_seasonal_calibrated",
        ]

    def test_expected_segments_can_be_filtered(self, tmp_path: Path) -> None:
        segments = expected_segments(_config(tmp_path), scenarios=["hp_seasonal"])
        assert segments == ["hp_seasonal_precalc", "hp_seasonal_calibrated"]

    def test_unknown_scenario_is_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(KeyError):
            expected_segments(_config(tmp_path), scenarios=["nope"])


class TestUpgradeForStage:
    """Stages are 1-1 with ResStock upgrades."""

    @pytest.mark.parametrize(
        ("stage", "upgrade"), [("precalc", "00"), ("calibrated", "02")]
    )
    def test_stage_maps_to_upgrade(
        self, tmp_path: Path, stage: str, upgrade: str
    ) -> None:
        assert upgrade_for_stage(_config(tmp_path), stage) == upgrade

    def test_unknown_stage_is_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Unknown stage"):
            upgrade_for_stage(_config(tmp_path), "final")


class TestBaselineSegment:
    """Baseline bill columns need an explicit baseline in the YAML."""

    def test_reads_configured_baseline(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        assert baseline_segment(config, "pipeline_bge.yaml") == "default_precalc"

    def test_missing_block_explains_the_fix(self, tmp_path: Path) -> None:
        data = _pipeline_yaml(tmp_path / "outputs")
        del data["bill_change_baseline"]
        config = _load(tmp_path, data)
        with pytest.raises(ValueError, match="bill_change_baseline"):
            baseline_segment(config, "pipeline_bge.yaml")


class TestFindRunPairs:
    """Discovery pairs delivery with supply, per (scenario, stage)."""

    def test_finds_complete_pair(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        _write_run_index(config, "default", "precalc", "delivery", "supply")

        pairs = find_run_pairs(config, BATCH)

        assert list(pairs) == ["default_precalc"]
        pair = pairs["default_precalc"]
        assert (pair.scenario, pair.stage, pair.upgrade) == (
            "default",
            "precalc",
            "00",
        )
        assert pair.dir_delivery.startswith("s3://data.sb/")
        assert pair.dir_delivery.endswith("_md_bge_default_precalc_delivery")
        assert pair.dir_supply.endswith("_md_bge_default_precalc_supply")

    def test_skips_segments_that_never_ran(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        _write_run_index(config, "default", "precalc", "delivery", "supply")
        _write_run_index(config, "hp_seasonal", "calibrated", "delivery", "supply")

        assert list(find_run_pairs(config, BATCH)) == [
            "default_precalc",
            "hp_seasonal_calibrated",
        ]

    def test_half_finished_pair_raises(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        _write_run_index(config, "default", "precalc", "delivery")

        with pytest.raises(FileNotFoundError, match="supply run never completed"):
            find_run_pairs(config, BATCH)

    def test_empty_batch_finds_nothing(self, tmp_path: Path) -> None:
        assert find_run_pairs(_config(tmp_path), BATCH) == {}

    def test_scenario_filter_limits_discovery(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        _write_run_index(config, "default", "precalc", "delivery", "supply")
        _write_run_index(config, "hp_seasonal", "precalc", "delivery", "supply")

        pairs = find_run_pairs(config, BATCH, scenarios=["hp_seasonal"])

        assert list(pairs) == ["hp_seasonal_precalc"]


class TestBuildOrder:
    """The baseline table has to exist before the segments that join it."""

    def test_baseline_is_built_first(self) -> None:
        segments = ["hp_seasonal_precalc", "default_precalc", "default_calibrated"]
        assert build_order(segments, "default_precalc") == [
            "default_precalc",
            "hp_seasonal_precalc",
            "default_calibrated",
        ]

    def test_absent_baseline_leaves_order_untouched(self) -> None:
        segments = ["hp_seasonal_precalc", "hp_seasonal_calibrated"]
        assert build_order(segments, "default_precalc") == segments


class TestBaselineRefRoot:
    """BAT and non-baseline bills read baseline columns from the master bills."""

    def test_path_points_at_all_utilities_segment(self) -> None:
        root = baseline_ref_root(
            state_lower="md", output_batch=BATCH, segment="default_precalc"
        )
        assert root == (
            "s3://data.sb/switchbox/cairo/outputs/hp_rates/md/all_utilities/"
            f"{BATCH}/default_precalc/comb_bills_year_target/"
        )
