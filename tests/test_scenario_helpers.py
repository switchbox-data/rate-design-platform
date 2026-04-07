from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from utils.mid.resolve_rr_paths import resolve_rr_paths
from utils.mid.resolve_subclass_config import (
    resolve_subclass_config,
    selectors_to_group_value_map,
)
from utils.mid.scenario_has_run_num import scenario_has_run_num


def _write_scenario_config(tmp_path: Path) -> Path:
    scenario_config = (
        tmp_path / "state" / "config" / "scenarios" / "scenarios_test.yaml"
    )
    scenario_config.parent.mkdir(parents=True)
    scenario_config.write_text(
        yaml.safe_dump(
            {
                "runs": {
                    1: {
                        "utility_revenue_requirement": "rev_requirement/base.yaml",
                        "run_includes_subclasses": False,
                    },
                    5: {
                        "utility_revenue_requirement": "rev_requirement/hp.yaml",
                        "run_includes_subclasses": True,
                        "subclass_config": {
                            "group_col": "has_hp",
                            "selectors": {"hp": "true", "non-hp": "false"},
                        },
                    },
                    25: {
                        "utility_revenue_requirement": "rev_requirement/elec_heat.yaml",
                        "run_includes_subclasses": True,
                        "subclass_config": {
                            "group_col": "heating_type_v2",
                            "selectors": {
                                "electric_heating": "heat_pump,electrical_resistance",
                                "non_electric_heating": "natgas,delivered_fuels,other",
                            },
                        },
                    },
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return scenario_config


def test_selectors_to_group_value_map_expands_multi_value_selectors() -> None:
    result = selectors_to_group_value_map(
        {
            "electric_heating": "heat_pump,electrical_resistance",
            "non_electric_heating": "natgas,delivered_fuels,other",
        }
    )
    assert result == (
        "heat_pump=electric_heating,"
        "electrical_resistance=electric_heating,"
        "natgas=non_electric_heating,"
        "delivered_fuels=non_electric_heating,"
        "other=non_electric_heating"
    )


def test_resolve_rr_paths_defaults_to_first_subclass_run(tmp_path: Path) -> None:
    scenario_config = _write_scenario_config(tmp_path)

    base, diff = resolve_rr_paths(scenario_config)

    assert base == scenario_config.parent.parent.resolve() / "rev_requirement/base.yaml"
    assert diff == scenario_config.parent.parent.resolve() / "rev_requirement/hp.yaml"


def test_resolve_rr_paths_accepts_explicit_subclass_run_num(tmp_path: Path) -> None:
    scenario_config = _write_scenario_config(tmp_path)

    _, diff = resolve_rr_paths(scenario_config, subclass_run_num=25)

    assert (
        diff
        == scenario_config.parent.parent.resolve() / "rev_requirement/elec_heat.yaml"
    )


def test_resolve_rr_paths_rejects_non_subclass_run(tmp_path: Path) -> None:
    scenario_config = _write_scenario_config(tmp_path)

    with pytest.raises(ValueError, match="not a subclass run"):
        resolve_rr_paths(scenario_config, subclass_run_num=1)


def test_resolve_subclass_config_defaults_to_first_subclass_run(tmp_path: Path) -> None:
    scenario_config = _write_scenario_config(tmp_path)

    group_col, mapping = resolve_subclass_config(scenario_config)

    assert group_col == "has_hp"
    assert mapping == "true=hp,false=non-hp"


def test_resolve_subclass_config_accepts_explicit_run_num(tmp_path: Path) -> None:
    scenario_config = _write_scenario_config(tmp_path)

    group_col, mapping = resolve_subclass_config(scenario_config, run_num=25)

    assert group_col == "heating_type_v2"
    assert mapping == (
        "heat_pump=electric_heating,"
        "electrical_resistance=electric_heating,"
        "natgas=non_electric_heating,"
        "delivered_fuels=non_electric_heating,"
        "other=non_electric_heating"
    )


def test_resolve_subclass_config_rejects_non_subclass_run(tmp_path: Path) -> None:
    scenario_config = _write_scenario_config(tmp_path)

    with pytest.raises(ValueError, match="not a subclass run"):
        resolve_subclass_config(scenario_config, run_num=1)


def test_scenario_has_run_num_handles_existing_and_missing_runs(tmp_path: Path) -> None:
    scenario_config = _write_scenario_config(tmp_path)

    assert scenario_has_run_num(scenario_config, 25) is True
    assert scenario_has_run_num(scenario_config, 99) is False
