from __future__ import annotations

from pathlib import Path

import yaml

from rate_design.hp_rates.run_scenario import _load_run_from_yaml


def test_load_run_from_yaml_inherits_top_level_subclass_config(tmp_path: Path) -> None:
    scenario_config = tmp_path / "ny" / "config" / "scenarios" / "scenarios_test.yaml"
    scenario_config.parent.mkdir(parents=True)
    scenario_config.write_text(
        yaml.safe_dump(
            {
                "subclass_config": {
                    "group_col": "has_hp",
                    "selectors": {"hp": "true", "non-hp": "false"},
                },
                "runs": {
                    5: {
                        "run_includes_subclasses": True,
                        "path_tariffs_electric": {
                            "hp": "tariffs/electric/hp.json",
                            "non-hp": "tariffs/electric/nonhp.json",
                        },
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    run = _load_run_from_yaml(scenario_config, 5)

    assert run["subclass_config"] == {
        "group_col": "has_hp",
        "selectors": {"hp": "true", "non-hp": "false"},
    }


def test_load_run_from_yaml_preserves_run_level_subclass_config(tmp_path: Path) -> None:
    scenario_config = tmp_path / "ny" / "config" / "scenarios" / "scenarios_test.yaml"
    scenario_config.parent.mkdir(parents=True)
    scenario_config.write_text(
        yaml.safe_dump(
            {
                "subclass_config": {
                    "group_col": "has_hp",
                    "selectors": {"hp": "true", "non-hp": "false"},
                },
                "runs": {
                    25: {
                        "run_includes_subclasses": True,
                        "subclass_config": {
                            "group_col": "heating_type_v2",
                            "selectors": {
                                "electric_heating": "heat_pump,electrical_resistance",
                                "non_electric_heating": "natgas,delivered_fuels,other",
                            },
                        },
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    run = _load_run_from_yaml(scenario_config, 25)

    assert run["subclass_config"] == {
        "group_col": "heating_type_v2",
        "selectors": {
            "electric_heating": "heat_pump,electrical_resistance",
            "non_electric_heating": "natgas,delivered_fuels,other",
        },
    }
