from __future__ import annotations

import json

from utils.pre.copy_tariff_variants import (
    _copy_utility_tariffs,
    _derive_variant_name,
    _discover_utilities,
)


def test_derive_variant_name_handles_default_nonhp_alias() -> None:
    assert _derive_variant_name("cenhud_default", "default", "nonhp_default") == (
        "cenhud_nonhp_default"
    )
    assert (
        _derive_variant_name("cenhud_default_supply", "default", "nonhp_default")
        == "cenhud_nonhp_default_supply"
    )


def test_derive_variant_name_handles_non_electric_heating_alias() -> None:
    assert (
        _derive_variant_name("cenhud_default", "default", "non_electric_heating")
        == "cenhud_non_electric_heating"
    )
    assert (
        _derive_variant_name("cenhud_default_supply", "default", "non_electric_heating")
        == "cenhud_non_electric_heating_supply"
    )


def test_copy_utility_tariffs_supports_custom_dest_pattern(tmp_path) -> None:
    (tmp_path / "cenhud_default.json").write_text(
        json.dumps({"items": [{"label": "cenhud_default", "name": "cenhud_default"}]})
    )
    (tmp_path / "cenhud_default_supply.json").write_text(
        json.dumps(
            {
                "items": [
                    {
                        "label": "cenhud_default_supply",
                        "name": "cenhud_default_supply",
                    }
                ]
            }
        )
    )

    _copy_utility_tariffs(tmp_path, "cenhud", "default", "non_electric_heating")

    copied_delivery = json.loads(
        (tmp_path / "cenhud_non_electric_heating.json").read_text()
    )
    copied_supply = json.loads(
        (tmp_path / "cenhud_non_electric_heating_supply.json").read_text()
    )

    assert copied_delivery["items"][0]["label"] == "cenhud_non_electric_heating"
    assert copied_delivery["items"][0]["name"] == "cenhud_non_electric_heating"
    assert copied_supply["items"][0]["label"] == "cenhud_non_electric_heating_supply"
    assert copied_supply["items"][0]["name"] == "cenhud_non_electric_heating_supply"


def test_discover_utilities_ignores_nested_variant_sources(tmp_path) -> None:
    for name in (
        "cenhud_default.json",
        "cenhud_default_supply.json",
        "cenhud_nonhp_default.json",
        "cenhud_nonhp_default_supply.json",
        "cenhud_non_electric_heating.json",
        "cenhud_non_electric_heating_supply.json",
    ):
        (tmp_path / name).write_text(json.dumps({"items": [{"label": name[:-5]}]}))

    assert _discover_utilities(tmp_path, "default") == ["cenhud"]
