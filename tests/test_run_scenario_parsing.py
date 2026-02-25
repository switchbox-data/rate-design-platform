"""Tests for run_scenario revenue requirement parsing."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from rate_design.ri.hp_rates.run_scenario import _parse_utility_revenue_requirement


def test_parse_utility_revenue_requirement_new_schema_delivery_only(
    tmp_path: Path,
) -> None:
    """New schema with add_supply=False returns total_delivery_revenue_requirement."""
    rr_yaml = tmp_path / "rev_requirement" / "rie.yaml"
    rr_yaml.parent.mkdir(parents=True)
    rr_yaml.write_text(
        yaml.safe_dump(
            {
                "utility": "rie",
                "total_delivery_revenue_requirement": 364891202.47,
                "total_delivery_and_supply_revenue_requirement": 752731289.17,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = _parse_utility_revenue_requirement(
        "rev_requirement/rie.yaml",
        tmp_path,
        {"all": "tariffs/electric/rie_flat.json"},
        add_supply=False,
    )
    assert result == pytest.approx(364891202.47)


def test_parse_utility_revenue_requirement_new_schema_add_supply(
    tmp_path: Path,
) -> None:
    """New schema with add_supply=True returns total_delivery_and_supply_revenue_requirement."""
    rr_yaml = tmp_path / "rev_requirement" / "rie.yaml"
    rr_yaml.parent.mkdir(parents=True)
    rr_yaml.write_text(
        yaml.safe_dump(
            {
                "utility": "rie",
                "total_delivery_revenue_requirement": 364891202.47,
                "total_delivery_and_supply_revenue_requirement": 752731289.17,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = _parse_utility_revenue_requirement(
        "rev_requirement/rie.yaml",
        tmp_path,
        {"all": "tariffs/electric/rie_flat.json"},
        add_supply=True,
    )
    assert result == pytest.approx(752731289.17)


def test_parse_utility_revenue_requirement_subclass_schema(tmp_path: Path) -> None:
    """Subclass schema returns dict keyed by tariff stem."""
    rr_yaml = tmp_path / "rev_requirement" / "rie_hp_vs_nonhp.yaml"
    rr_yaml.parent.mkdir(parents=True)
    rr_yaml.write_text(
        yaml.safe_dump(
            {
                "utility": "rie",
                "group_col": "has_hp",
                "subclass_revenue_requirements": {
                    "true": 5229742.9,
                    "false": 236639858.0,
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (tmp_path / "tariffs" / "electric").mkdir(parents=True)
    (tmp_path / "tariffs/electric/rie_hp_seasonal.json").touch()
    (tmp_path / "tariffs/electric/rie_flat.json").touch()

    result = _parse_utility_revenue_requirement(
        "rev_requirement/rie_hp_vs_nonhp.yaml",
        tmp_path,
        {
            "hp": "tariffs/electric/rie_hp_seasonal.json",
            "non-hp": "tariffs/electric/rie_flat.json",
        },
        add_supply=False,
    )
    assert isinstance(result, dict)
    assert result["rie_hp_seasonal"] == pytest.approx(5229742.9)
    assert result["rie_flat"] == pytest.approx(236639858.0)


def test_parse_utility_revenue_requirement_old_schema(tmp_path: Path) -> None:
    """Old schema (revenue_requirement key) returns scalar."""
    rr_yaml = tmp_path / "rev_requirement" / "rie_large_number.yaml"
    rr_yaml.parent.mkdir(parents=True)
    rr_yaml.write_text(
        yaml.safe_dump(
            {"utility": "rie", "revenue_requirement": 1e12},
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = _parse_utility_revenue_requirement(
        "rev_requirement/rie_large_number.yaml",
        tmp_path,
        {"all": "tariffs/electric/rie_flat.json"},
        add_supply=False,
    )
    assert result == pytest.approx(1e12)
