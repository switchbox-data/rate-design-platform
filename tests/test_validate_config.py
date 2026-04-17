from __future__ import annotations

import pytest

from utils.post.validate.config import RunConfig, define_run_blocks
from utils.post.validate.subclasses import legacy_hp_subclass_spec


def _make_run_config(
    run_num: int,
    *,
    run_type: str,
    upgrade: str,
    cost_scope: str,
    has_subclasses: bool,
    tariff_type: str,
    elasticity: float = 0.0,
) -> RunConfig:
    return RunConfig(
        run_num=run_num,
        run_name=f"run_{run_num}",
        run_type=run_type,
        upgrade=upgrade,
        cost_scope=cost_scope,
        has_subclasses=has_subclasses,
        tariff_type=tariff_type,
        elasticity=elasticity,
        path_resstock_loads="",
        path_dist_and_sub_tx_mc="",
        path_bulk_tx_mc=None,
        path_supply_energy_mc="",
        path_supply_capacity_mc="",
    )


def test_define_run_blocks_discovers_extended_pairs() -> None:
    configs = {
        9: _make_run_config(
            9,
            run_type="precalc",
            upgrade="0",
            cost_scope="delivery",
            has_subclasses=True,
            tariff_type="seasonalTOU",
        ),
        10: _make_run_config(
            10,
            run_type="precalc",
            upgrade="0",
            cost_scope="delivery+supply",
            has_subclasses=True,
            tariff_type="seasonalTOU",
        ),
        11: _make_run_config(
            11,
            run_type="default",
            upgrade="2",
            cost_scope="delivery",
            has_subclasses=True,
            tariff_type="seasonalTOU",
        ),
        12: _make_run_config(
            12,
            run_type="default",
            upgrade="2",
            cost_scope="delivery+supply",
            has_subclasses=True,
            tariff_type="seasonalTOU",
        ),
        13: _make_run_config(
            13,
            run_type="precalc",
            upgrade="0",
            cost_scope="delivery",
            has_subclasses=True,
            tariff_type="seasonalTOU_flex",
            elasticity=-0.1,
        ),
        14: _make_run_config(
            14,
            run_type="precalc",
            upgrade="0",
            cost_scope="delivery+supply",
            has_subclasses=True,
            tariff_type="seasonalTOU_flex",
            elasticity=-0.1,
        ),
        15: _make_run_config(
            15,
            run_type="default",
            upgrade="2",
            cost_scope="delivery",
            has_subclasses=True,
            tariff_type="seasonalTOU_flex",
            elasticity=-0.1,
        ),
        16: _make_run_config(
            16,
            run_type="default",
            upgrade="2",
            cost_scope="delivery+supply",
            has_subclasses=True,
            tariff_type="seasonalTOU_flex",
            elasticity=-0.1,
        ),
    }

    blocks = define_run_blocks(configs)

    assert [block.run_nums for block in blocks] == [
        (9, 10),
        (11, 12),
        (13, 14),
        (15, 16),
    ]
    assert blocks[0].revenue_neutral is True
    assert blocks[1].tariff_should_be_unchanged is True
    assert blocks[2].bat_relevant is True
    assert (
        blocks[3].description
        == "Default seasonal TOU flex rates on upgrade 02 (runs 15-16)"
    )


def test_define_run_blocks_rejects_unpaired_runs() -> None:
    configs = {
        9: _make_run_config(
            9,
            run_type="precalc",
            upgrade="0",
            cost_scope="delivery",
            has_subclasses=True,
            tariff_type="seasonalTOU",
        ),
        10: _make_run_config(
            10,
            run_type="precalc",
            upgrade="0",
            cost_scope="delivery+supply",
            has_subclasses=True,
            tariff_type="seasonalTOU",
        ),
        11: _make_run_config(
            11,
            run_type="default",
            upgrade="2",
            cost_scope="delivery",
            has_subclasses=True,
            tariff_type="seasonalTOU",
        ),
    }

    with pytest.raises(ValueError, match="Cannot pair validation runs"):
        define_run_blocks(configs)


def test_run_config_uses_legacy_subclass_metadata_when_yaml_omits_it() -> None:
    config = RunConfig.from_yaml_run(
        17,
        {
            "run_name": "ny_coned_run17_up00_precalc__hp_flat_vs_default",
            "run_type": "precalc",
            "upgrade": "0",
            "run_includes_subclasses": True,
            "path_tariffs_electric": {
                "hp": "tariffs/electric/coned_hp_flat.json",
                "non-hp": "tariffs/electric/coned_nonhp_default.json",
            },
            "utility_revenue_requirement": "rev_requirement/coned_hp_vs_nonhp.yaml",
        },
    )

    assert config.subclass_spec == legacy_hp_subclass_spec()
    assert config.revenue_requirement_filename == "coned_hp_vs_nonhp.yaml"
    assert config.tariff_keys_by_alias == {
        "hp": "coned_hp_flat",
        "non-hp": "coned_nonhp_default",
    }


def test_run_config_reads_explicit_subclass_metadata_from_yaml() -> None:
    config = RunConfig.from_yaml_run(
        29,
        {
            "run_name": "ny_coned_run29_up00_precalc__elec_heat_flat_vs_non_elec_heat",
            "run_type": "precalc",
            "upgrade": "0",
            "run_includes_subclasses": True,
            "path_tariffs_electric": {
                "electric_heating": "tariffs/electric/coned_elec_heat_flat.json",
                "non_electric_heating": "tariffs/electric/coned_non_electric_heating.json",
            },
            "utility_revenue_requirement": "rev_requirement/coned_elec_heat_vs_non_elec_heat.yaml",
            "subclass_config": {
                "group_col": "heating_type_v2",
                "selectors": {
                    "electric_heating": "heat_pump,electrical_resistance",
                    "non_electric_heating": "natgas,delivered_fuels,other",
                },
            },
        },
    )

    assert config.subclass_spec is not None
    assert config.subclass_spec.group_col == "postprocess_group.heating_type_v2"
    assert config.subclass_spec.aliases == (
        "electric_heating",
        "non_electric_heating",
    )
    assert (
        config.revenue_requirement_filename == "coned_elec_heat_vs_non_elec_heat.yaml"
    )
