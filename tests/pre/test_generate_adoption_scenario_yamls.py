"""Tests for utils/pre/generate_adoption_scenario_yamls.py."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from utils.pre.generate_adoption_scenario_yamls import (
    _replace_year_in_value,
    _update_run_name,
    main,
)

ADOPTION_CONFIG: dict[str, Any] = {
    "scenario_name": "test_scenario",
    "year_labels": [2025, 2030],
    "run_years": [2025, 2030],
    "upgrades": {2: {"label": "hp", "fractions": [0.1, 0.2]}},
}

BASE_RUNS: dict[str, Any] = {
    "runs": {
        1: {
            "run_name": "ny_nyseg_run1_up00_precalc__flat",
            "state": "NY",
            "utility": "nyseg",
            "run_type": "precalc",
            "upgrade": "0",
            "path_resstock_metadata": "/old/metadata-sb.parquet",
            "path_resstock_loads": "/old/loads/",
            "path_dist_and_sub_tx_mc": "s3://data.sb/switchbox/marginal_costs/ny/dist_and_sub_tx/utility=nyseg/year=2025/data.parquet",
            "path_supply_energy_mc": "s3://data.sb/switchbox/marginal_costs/ny/supply/energy/utility=nyseg/year=2025/zero.parquet",
            "path_supply_capacity_mc": "s3://data.sb/switchbox/marginal_costs/ny/supply/capacity/utility=nyseg/year=2025/zero.parquet",
            "path_bulk_tx_mc": "s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/utility=nyseg/year=2025/data.parquet",
            "utility_revenue_requirement": "rev_requirement/nyseg.yaml",
            "run_includes_supply": False,
            "year_run": 2025,
        },
        2: {
            "run_name": "ny_nyseg_run2_up00_precalc_supply__flat",
            "state": "NY",
            "utility": "nyseg",
            "run_type": "precalc",
            "upgrade": "0",
            "path_resstock_metadata": "/old/metadata-sb.parquet",
            "path_resstock_loads": "/old/loads/",
            "path_dist_and_sub_tx_mc": "s3://data.sb/switchbox/marginal_costs/ny/dist_and_sub_tx/utility=nyseg/year=2025/data.parquet",
            "path_supply_energy_mc": "s3://data.sb/switchbox/marginal_costs/ny/supply/energy/utility=nyseg/year=2025/data.parquet",
            "path_supply_capacity_mc": "s3://data.sb/switchbox/marginal_costs/ny/supply/capacity/utility=nyseg/year=2025/data.parquet",
            "path_bulk_tx_mc": "s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/utility=nyseg/year=2025/data.parquet",
            "utility_revenue_requirement": "rev_requirement/nyseg.yaml",
            "run_includes_supply": True,
            "year_run": 2025,
        },
    }
}

BASE_RUNS_WITH_CAMBIUM_T: dict[str, Any] = {
    "runs": {
        1: {
            "run_name": "ny_nyseg_run1_y2025_mixed__flat",
            "state": "NY",
            "utility": "nyseg",
            "run_type": "precalc",
            "upgrade": "0",
            "path_resstock_metadata": "/old/metadata-sb.parquet",
            "path_resstock_loads": "/old/loads/",
            "path_dist_and_sub_tx_mc": "s3://data.sb/switchbox/marginal_costs/ny/dist_and_sub_tx/utility=nyseg/year=2025/data.parquet",
            "path_supply_energy_mc": "s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=NYISO/r=p127/data.parquet",
            "path_supply_capacity_mc": "s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=NYISO/r=p127/data.parquet",
            "path_bulk_tx_mc": "",
            "utility_revenue_requirement": None,
            "run_includes_supply": True,
            "year_run": 2025,
            "residual_cost_frac": 0.0,
        },
    }
}


def _write_yaml(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data, default_flow_style=False))


def _make_test_inputs(
    tmp_path: Path,
    base_runs: dict[str, Any] | None = None,
    adoption_config: dict[str, Any] | None = None,
) -> tuple[Path, Path, Path, Path]:
    if base_runs is None:
        base_runs = BASE_RUNS
    if adoption_config is None:
        adoption_config = ADOPTION_CONFIG
    path_base = tmp_path / "scenarios_nyseg.yaml"
    _write_yaml(path_base, base_runs)
    path_adopt = tmp_path / "adoption.yaml"
    _write_yaml(path_adopt, adoption_config)
    path_mat = tmp_path / "materialized"
    for yr in [2025, 2030]:
        (path_mat / f"year={yr}").mkdir(parents=True)
    path_out = tmp_path / "scenarios_nyseg_adoption.yaml"
    return path_base, path_adopt, path_mat, path_out


class TestReplaceYearInValue:
    """_replace_year_in_value handles both year= (Hive) and t= (Cambium) tokens."""

    def test_replaces_year_token(self) -> None:
        assert (
            _replace_year_in_value("path/year=2025/data.parquet", 2025, 2030)
            == "path/year=2030/data.parquet"
        )

    def test_replaces_t_token(self) -> None:
        result = _replace_year_in_value(
            "s3://cambium/t=2025/gea=NYISO/data.parquet", 2025, 2030
        )
        assert "t=2030" in result
        assert "t=2025" not in result

    def test_replaces_both_tokens(self) -> None:
        assert (
            _replace_year_in_value("year=2025/t=2025/x", 2025, 2030)
            == "year=2030/t=2030/x"
        )

    def test_no_replacement_when_year_absent(self) -> None:
        assert (
            _replace_year_in_value("year=2024/data.parquet", 2025, 2030)
            == "year=2024/data.parquet"
        )

    def test_t_token_not_replaced_when_year_mismatch(self) -> None:
        assert (
            _replace_year_in_value("t=2024/data.parquet", 2025, 2030)
            == "t=2024/data.parquet"
        )

    def test_replaces_in_dict(self) -> None:
        d = {"a": "year=2025/a.parquet", "b": "t=2025/b.parquet"}
        result = _replace_year_in_value(d, 2025, 2030)
        assert result == {"a": "year=2030/a.parquet", "b": "t=2030/b.parquet"}

    def test_replaces_in_nested_dict(self) -> None:
        result = _replace_year_in_value(
            {"inner": {"path": "year=2025/x.parquet"}}, 2025, 2030
        )
        assert result["inner"]["path"] == "year=2030/x.parquet"

    def test_replaces_in_list(self) -> None:
        assert _replace_year_in_value(
            ["year=2025/a.parquet", "t=2025/b.parquet"], 2025, 2030
        ) == ["year=2030/a.parquet", "t=2030/b.parquet"]

    def test_non_string_unchanged(self) -> None:
        assert _replace_year_in_value(42, 2025, 2030) == 42
        assert _replace_year_in_value(None, 2025, 2030) is None


class TestUpdateRunName:
    def test_no_double_underscore_suffix(self) -> None:
        assert _update_run_name("ny_nyseg_run1", 2030) == "ny_nyseg_run1_y2030_mixed"

    def test_with_double_underscore_suffix(self) -> None:
        assert (
            _update_run_name("ny_nyseg_run1_up00_precalc__flat", 2030)
            == "ny_nyseg_run1_up00_precalc_y2030_mixed__flat"
        )


class TestMainBaseline:
    def test_generates_correct_count(self, tmp_path: Path) -> None:
        path_base, path_adopt, path_mat, path_out = _make_test_inputs(tmp_path)
        main(
            [
                "--base-scenario",
                str(path_base),
                "--runs",
                "1,2",
                "--adoption-config",
                str(path_adopt),
                "--materialized-dir",
                str(path_mat),
                "--output",
                str(path_out),
            ]
        )
        result = yaml.safe_load(path_out.read_text())
        assert len(result["runs"]) == 4  # 2 years x 2 runs

    def test_output_keys(self, tmp_path: Path) -> None:
        path_base, path_adopt, path_mat, path_out = _make_test_inputs(tmp_path)
        main(
            [
                "--base-scenario",
                str(path_base),
                "--runs",
                "1,2",
                "--adoption-config",
                str(path_adopt),
                "--materialized-dir",
                str(path_mat),
                "--output",
                str(path_out),
            ]
        )
        result = yaml.safe_load(path_out.read_text())
        assert set(result["runs"].keys()) == {101, 102, 201, 202}

    def test_year_token_replaced_in_dist_mc_path(self, tmp_path: Path) -> None:
        path_base, path_adopt, path_mat, path_out = _make_test_inputs(tmp_path)
        main(
            [
                "--base-scenario",
                str(path_base),
                "--runs",
                "1",
                "--adoption-config",
                str(path_adopt),
                "--materialized-dir",
                str(path_mat),
                "--output",
                str(path_out),
            ]
        )
        result = yaml.safe_load(path_out.read_text())
        assert "year=2030" in result["runs"][201]["path_dist_and_sub_tx_mc"]
        assert "year=2025" not in result["runs"][201]["path_dist_and_sub_tx_mc"]


class TestMainResidualCostFrac:
    def _run_with_frac(self, tmp_path: Path, frac: str = "0.0") -> dict[str, Any]:
        path_base, path_adopt, path_mat, path_out = _make_test_inputs(tmp_path)
        main(
            [
                "--base-scenario",
                str(path_base),
                "--runs",
                "1,2",
                "--adoption-config",
                str(path_adopt),
                "--materialized-dir",
                str(path_mat),
                "--output",
                str(path_out),
                "--residual-cost-frac",
                frac,
            ]
        )
        return yaml.safe_load(path_out.read_text())

    def test_frac_present_in_all_entries(self, tmp_path: Path) -> None:
        for entry in self._run_with_frac(tmp_path)["runs"].values():
            assert entry.get("residual_cost_frac") == pytest.approx(0.0)

    def test_utility_revenue_requirement_none(self, tmp_path: Path) -> None:
        for entry in self._run_with_frac(tmp_path)["runs"].values():
            assert entry.get("utility_revenue_requirement") is None

    def test_custom_frac_value(self, tmp_path: Path) -> None:
        for entry in self._run_with_frac(tmp_path, frac="0.1")["runs"].values():
            assert entry.get("residual_cost_frac") == pytest.approx(0.1)

    def test_no_flag_leaves_field_absent(self, tmp_path: Path) -> None:
        path_base, path_adopt, path_mat, path_out = _make_test_inputs(tmp_path)
        main(
            [
                "--base-scenario",
                str(path_base),
                "--runs",
                "1",
                "--adoption-config",
                str(path_adopt),
                "--materialized-dir",
                str(path_mat),
                "--output",
                str(path_out),
            ]
        )
        result = yaml.safe_load(path_out.read_text())
        for entry in result["runs"].values():
            assert "residual_cost_frac" not in entry


class TestMainCambiumSupply:
    def _run(self, tmp_path: Path, runs: str = "1,2") -> dict[str, Any]:
        path_base, path_adopt, path_mat, path_out = _make_test_inputs(tmp_path)
        main(
            [
                "--base-scenario",
                str(path_base),
                "--runs",
                runs,
                "--adoption-config",
                str(path_adopt),
                "--materialized-dir",
                str(path_mat),
                "--output",
                str(path_out),
                "--cambium-supply",
                "--cambium-gea",
                "NYISO",
                "--cambium-ba",
                "p127",
            ]
        )
        return yaml.safe_load(path_out.read_text())

    def test_supply_run_gets_cambium_energy_path(self, tmp_path: Path) -> None:
        run_102 = self._run(tmp_path)["runs"][102]
        assert "data.sb/nrel/cambium" in run_102["path_supply_energy_mc"]
        assert "MidCase" in run_102["path_supply_energy_mc"]

    def test_supply_run_gets_cambium_capacity_path(self, tmp_path: Path) -> None:
        assert (
            "data.sb/nrel/cambium"
            in self._run(tmp_path)["runs"][102]["path_supply_capacity_mc"]
        )

    def test_delivery_run_supply_paths_not_overwritten(self, tmp_path: Path) -> None:
        run_101 = self._run(tmp_path)["runs"][101]
        assert "data.sb/nrel/cambium" not in run_101["path_supply_energy_mc"]
        assert "data.sb/nrel/cambium" not in run_101["path_supply_capacity_mc"]

    def test_bulk_tx_cleared_for_all_runs(self, tmp_path: Path) -> None:
        for entry in self._run(tmp_path)["runs"].values():
            assert entry.get("path_bulk_tx_mc") == ""

    def test_cambium_path_uses_correct_year(self, tmp_path: Path) -> None:
        run_202 = self._run(tmp_path)["runs"][202]
        assert "t=2030" in run_202["path_supply_energy_mc"]
        assert "t=2025" not in run_202["path_supply_energy_mc"]

    def test_cambium_path_includes_gea_and_ba(self, tmp_path: Path) -> None:
        path = self._run(tmp_path)["runs"][102]["path_supply_energy_mc"]
        assert "gea=NYISO" in path
        assert "r=p127" in path

    def test_cambium_supply_without_ba_raises(self, tmp_path: Path) -> None:
        path_base, path_adopt, path_mat, path_out = _make_test_inputs(tmp_path)
        with pytest.raises(ValueError, match="--cambium-ba"):
            main(
                [
                    "--base-scenario",
                    str(path_base),
                    "--runs",
                    "1",
                    "--adoption-config",
                    str(path_adopt),
                    "--materialized-dir",
                    str(path_mat),
                    "--output",
                    str(path_out),
                    "--cambium-supply",
                ]
            )


class TestMainCambiumDistMcBase:
    CAMBIUM_BASE = "s3://data.sb/switchbox/marginal_costs/ny/cambium_dist_and_sub_tx"

    def _run(self, tmp_path: Path) -> dict[str, Any]:
        path_base, path_adopt, path_mat, path_out = _make_test_inputs(tmp_path)
        main(
            [
                "--base-scenario",
                str(path_base),
                "--runs",
                "1",
                "--adoption-config",
                str(path_adopt),
                "--materialized-dir",
                str(path_mat),
                "--output",
                str(path_out),
                "--cambium-dist-mc-base",
                self.CAMBIUM_BASE,
            ]
        )
        return yaml.safe_load(path_out.read_text())

    def test_dist_mc_path_uses_cambium_base(self, tmp_path: Path) -> None:
        for entry in self._run(tmp_path)["runs"].values():
            assert "cambium_dist_and_sub_tx" in entry["path_dist_and_sub_tx_mc"]

    def test_dist_mc_path_contains_utility_partition(self, tmp_path: Path) -> None:
        for entry in self._run(tmp_path)["runs"].values():
            assert "utility=nyseg" in entry["path_dist_and_sub_tx_mc"]

    def test_dist_mc_path_year_matches_run_year(self, tmp_path: Path) -> None:
        result = self._run(tmp_path)
        assert "year=2025" in result["runs"][101]["path_dist_and_sub_tx_mc"]
        assert "year=2030" in result["runs"][201]["path_dist_and_sub_tx_mc"]

    def test_dist_mc_path_ends_with_data_parquet(self, tmp_path: Path) -> None:
        for entry in self._run(tmp_path)["runs"].values():
            assert entry["path_dist_and_sub_tx_mc"].endswith("/data.parquet")


class TestMainTTokenReplacement:
    """End-to-end: base runs with Cambium t= paths get tokens updated per year."""

    def test_t_token_replaced_in_supply_mc_paths(self, tmp_path: Path) -> None:
        path_base = tmp_path / "scenarios_nyseg.yaml"
        _write_yaml(path_base, BASE_RUNS_WITH_CAMBIUM_T)
        path_adopt = tmp_path / "adoption.yaml"
        _write_yaml(path_adopt, ADOPTION_CONFIG)
        path_mat = tmp_path / "materialized"
        for yr in [2025, 2030]:
            (path_mat / f"year={yr}").mkdir(parents=True)
        path_out = tmp_path / "out.yaml"
        main(
            [
                "--base-scenario",
                str(path_base),
                "--runs",
                "1",
                "--adoption-config",
                str(path_adopt),
                "--materialized-dir",
                str(path_mat),
                "--output",
                str(path_out),
            ]
        )
        result = yaml.safe_load(path_out.read_text())
        # key 101: year_index 0 (2025) - t= should stay t=2025
        assert "t=2025" in result["runs"][101]["path_supply_energy_mc"]
        # key 201: year_index 1 (2030) - t= should be updated to t=2030
        assert "t=2030" in result["runs"][201]["path_supply_energy_mc"]
        assert "t=2025" not in result["runs"][201]["path_supply_energy_mc"]


class TestHiveLoadsPath:
    """path_resstock_loads is the hive-leaf upgrade=00/ dir, not a flat loads/ dir."""

    def _run(self, tmp_path: Path) -> dict[str, Any]:
        path_base, path_adopt, path_mat, path_out = _make_test_inputs(tmp_path)
        main(
            [
                "--base-scenario",
                str(path_base),
                "--runs",
                "1",
                "--adoption-config",
                str(path_adopt),
                "--materialized-dir",
                str(path_mat),
                "--output",
                str(path_out),
            ]
        )
        return yaml.safe_load(path_out.read_text())

    def test_loads_path_contains_load_curve_hourly(self, tmp_path: Path) -> None:
        result = self._run(tmp_path)
        for entry in result["runs"].values():
            assert "load_curve_hourly" in entry["path_resstock_loads"]

    def test_loads_path_contains_state_partition(self, tmp_path: Path) -> None:
        result = self._run(tmp_path)
        for entry in result["runs"].values():
            assert "state=NY" in entry["path_resstock_loads"]

    def test_loads_path_contains_upgrade_partition(self, tmp_path: Path) -> None:
        result = self._run(tmp_path)
        for entry in result["runs"].values():
            assert "upgrade=00" in entry["path_resstock_loads"]

    def test_loads_path_does_not_contain_flat_loads(self, tmp_path: Path) -> None:
        result = self._run(tmp_path)
        for entry in result["runs"].values():
            assert "/loads/" not in entry["path_resstock_loads"]

    def test_loads_path_year_matches_run_year(self, tmp_path: Path) -> None:
        result = self._run(tmp_path)
        assert "year=2025" in result["runs"][101]["path_resstock_loads"]
        assert "year=2030" in result["runs"][201]["path_resstock_loads"]


# Base run with run_includes_subclasses=True for adoption-tariff-dir tests.
BASE_RUNS_SUBCLASS: dict[str, Any] = {
    "runs": {
        5: {
            "run_name": "ny_nyseg_run5_up00_precalc__hp_seasonal_vs_flat",
            "state": "NY",
            "utility": "nyseg",
            "run_type": "precalc",
            "upgrade": "0",
            "path_resstock_metadata": "/old/metadata-sb.parquet",
            "path_resstock_loads": "/old/loads/",
            "path_dist_and_sub_tx_mc": "s3://dist/year=2025/data.parquet",
            "path_supply_energy_mc": "s3://supply/year=2025/zero.parquet",
            "path_supply_capacity_mc": "s3://supply/year=2025/zero.parquet",
            "path_bulk_tx_mc": "",
            "utility_revenue_requirement": None,
            "run_includes_supply": False,
            "run_includes_subclasses": True,
            "year_run": 2025,
            "path_tariffs_electric": {
                "hp": "tariffs/electric/nyseg_hp_seasonal.json",
                "non-hp": "tariffs/electric/nyseg_nonhp_flat.json",
            },
        },
    }
}


class TestAdoptionTariffDir:
    """--adoption-tariff-dir rewrites hp/non-hp paths for subclass runs only."""

    def _run(
        self,
        tmp_path: Path,
        adoption_tariff_dir: str | None = None,
        runs: str = "5",
    ) -> dict[str, Any]:
        path_base = tmp_path / "scenarios_nyseg.yaml"
        _write_yaml(path_base, BASE_RUNS_SUBCLASS)
        path_adopt = tmp_path / "adoption.yaml"
        _write_yaml(path_adopt, ADOPTION_CONFIG)
        path_mat = tmp_path / "materialized"
        for yr in [2025, 2030]:
            (path_mat / f"year={yr}").mkdir(parents=True)
        path_out = tmp_path / "out.yaml"
        args = [
            "--base-scenario",
            str(path_base),
            "--runs",
            runs,
            "--adoption-config",
            str(path_adopt),
            "--materialized-dir",
            str(path_mat),
            "--output",
            str(path_out),
        ]
        if adoption_tariff_dir is not None:
            args += ["--adoption-tariff-dir", adoption_tariff_dir]
        main(args)
        return yaml.safe_load(path_out.read_text())

    def test_hp_path_rewritten_with_tariff_dir(self, tmp_path: Path) -> None:
        result = self._run(tmp_path, adoption_tariff_dir="/tariffs/adoption/cfg")
        entry = result["runs"][105]
        assert "/tariffs/adoption/cfg/year=2025" in entry["path_tariffs_electric"]["hp"]
        assert "nyseg_hp_seasonal.json" in entry["path_tariffs_electric"]["hp"]

    def test_non_hp_path_rewritten_with_tariff_dir(self, tmp_path: Path) -> None:
        result = self._run(tmp_path, adoption_tariff_dir="/tariffs/adoption/cfg")
        entry = result["runs"][105]
        assert (
            "/tariffs/adoption/cfg/year=2025"
            in entry["path_tariffs_electric"]["non-hp"]
        )
        assert "nyseg_nonhp_flat.json" in entry["path_tariffs_electric"]["non-hp"]

    def test_tariff_dir_includes_correct_year(self, tmp_path: Path) -> None:
        result = self._run(tmp_path, adoption_tariff_dir="/tariffs/adoption/cfg")
        entry_2030 = result["runs"][205]
        assert "year=2030" in entry_2030["path_tariffs_electric"]["hp"]
        assert "year=2025" not in entry_2030["path_tariffs_electric"]["hp"]

    def test_without_tariff_dir_paths_unchanged(self, tmp_path: Path) -> None:
        result = self._run(tmp_path, adoption_tariff_dir=None)
        entry = result["runs"][105]
        assert (
            entry["path_tariffs_electric"]["hp"]
            == "tariffs/electric/nyseg_hp_seasonal.json"
        )
        assert (
            entry["path_tariffs_electric"]["non-hp"]
            == "tariffs/electric/nyseg_nonhp_flat.json"
        )

    def test_non_subclass_run_not_affected(self, tmp_path: Path) -> None:
        """Runs without run_includes_subclasses keep their original tariff paths."""
        path_base, path_adopt, path_mat, path_out = _make_test_inputs(tmp_path)
        main(
            [
                "--base-scenario",
                str(path_base),
                "--runs",
                "1",
                "--adoption-config",
                str(path_adopt),
                "--materialized-dir",
                str(path_mat),
                "--output",
                str(path_out),
                "--adoption-tariff-dir",
                "/tariffs/adoption/cfg",
            ]
        )
        result = yaml.safe_load(path_out.read_text())
        # Run 1 uses path_tariffs_electric.all (not hp/non-hp); must be untouched.
        for entry in result["runs"].values():
            tariff_elec = entry.get("path_tariffs_electric", {})
            assert "hp" not in tariff_elec or "/tariffs/adoption/cfg" not in str(
                tariff_elec.get("hp", "")
            )
