import pytest

from utils.pre.rev_requirement.fetch_electric_tariffs_genability import (
    filter_config_by_tariff_keys,
)


def test_filter_config_by_tariff_keys_selects_exact_key() -> None:
    config: dict[str, dict[str, str | int]] = {
        "bge": {
            "bge": "default",
            "bge_rd": 3350180,
        },
        "pepco": {"pepco": "default"},
    }

    assert filter_config_by_tariff_keys(config, {"bge_rd"}) == {
        "bge": {"bge_rd": 3350180}
    }


def test_filter_config_by_tariff_keys_rejects_unknown_key() -> None:
    with pytest.raises(ValueError, match="not found"):
        filter_config_by_tariff_keys({"bge": {"bge": "default"}}, {"bge_rd"})
