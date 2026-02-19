"""Tests for RI tariff naming helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from utils.pre.tariff_naming import (
    build_ri_run_name,
    derive_tariff_key_from_electric_tariff_filename,
    parse_ri_run_name,
    parse_tariff_key_from_ri_run_name,
)


def test_derive_tariff_key_from_filename() -> None:
    path = Path("/tmp/tariffs/electric/rie_a16_supply_adj.json")
    assert derive_tariff_key_from_electric_tariff_filename(path) == "rie_a16_supply_adj"


def test_build_ri_run_name_encodes_tariff_key() -> None:
    run_name = build_ri_run_name(
        state="RI",
        utility="rie",
        run_num=2,
        run_type="precalc",
        tariff_key="rie_a16_supply_adj",
        upgrade="00",
        year_run=2025,
    )
    assert run_name == "ri_rie_run_02_precalc_rie_a16_supply_adj_up00_y2025"


def test_parse_tariff_key_from_ri_run_name() -> None:
    run_name = "ri_rie_run_03_precalc_rie_seasonal_tou_hp_up02_y2025"
    assert parse_tariff_key_from_ri_run_name(run_name) == "rie_seasonal_tou_hp"
    parsed = parse_ri_run_name(run_name)
    assert parsed["state"] == "ri"
    assert parsed["utility"] == "rie"
    assert parsed["upgrade"] == "02"


def test_parse_ri_run_name_rejects_invalid_name() -> None:
    with pytest.raises(ValueError):
        parse_ri_run_name("ri_rie_run_invalid")

