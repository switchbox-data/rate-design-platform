"""Tests for gas tariff mapper."""

from typing import cast

import polars as pl

from utils.gas_tariff_mapper import map_gas_tariff


def test_map_gas_tariff_uses_crosswalk_for_tariff_key():
    """map_gas_tariff maps sb.gas_utility (std_name) to tariff_key via crosswalk."""
    metadata = pl.LazyFrame({
        "bldg_id": [1, 2, 3],
        "sb.electric_utility": ["coned", "coned", "coned"],
        "sb.gas_utility": ["nimo", "nyseg", "coned"],
    })
    result = map_gas_tariff(
        SB_metadata_df=metadata,
        electric_utility_name="coned",
    )
    df = cast(pl.DataFrame, result.collect())
    assert df.height == 3
    # nimo -> national_grid, nyseg -> nyseg, coned -> coned
    tariff_keys = df["tariff_key"].to_list()
    assert "national_grid" in tariff_keys
    assert "nyseg" in tariff_keys
    assert "coned" in tariff_keys
