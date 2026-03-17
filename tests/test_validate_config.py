from __future__ import annotations

import pytest

from utils.post.validate.config import RunConfig, define_run_blocks


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
    assert blocks[3].description == "Default seasonal TOU flex rates on upgrade 02 (runs 15-16)"


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
