"""Default upgrade ids for a ResStock `_sb` release run."""

from __future__ import annotations

import pytest

from data.resstock.utils import resolve_upgrade_ids

_DEFAULT = ["0", "1", "2", "3", "4", "5"]


def test_state_list_replaces_the_release_catalog() -> None:
    ids = resolve_upgrade_ids(
        ["NY"],
        {"NY": {"upgrade_ids": ["0", "2"]}},
        _DEFAULT,
    )
    assert ids == ["0", "2"]


def test_missing_state_list_uses_the_release_catalog() -> None:
    ids = resolve_upgrade_ids(["MD"], {"MD": {"state_fips": "24"}}, _DEFAULT)
    assert ids == _DEFAULT


def test_states_that_agree_share_one_list() -> None:
    configs = {
        "NY": {"upgrade_ids": ["0", "2"]},
        "RI": {"upgrade_ids": ["0", "2"]},
    }
    assert resolve_upgrade_ids(["NY", "RI"], configs, _DEFAULT) == ["0", "2"]


def test_states_that_disagree_need_an_explicit_list() -> None:
    configs = {
        "NY": {"upgrade_ids": ["0", "2"]},
        "MD": {},
    }
    with pytest.raises(ValueError, match="--upgrade-ids"):
        resolve_upgrade_ids(["NY", "MD"], configs, _DEFAULT)
