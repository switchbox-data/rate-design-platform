from typing import cast

from utils.pre.create_scenario_yamls import (
    _hoist_shared_subclass_config,
    _normalize_deprecated_elec_heat_seasonal_paths,
    _parse_subclass_selectors,
    _row_to_run,
)


def _base_row() -> dict[str, str]:
    return {
        "run_name": "ri_rie_run10_up00_precalc_supply__hp_seasonalTOU_vs_flat",
        "state": "RI",
        "utility": "rie",
        "run_type": "precalc",
        "upgrade": "0",
        "path_tariff_maps_electric": "tariff_maps/electric/rie_hp_seasonalTOU_vs_flat_supply.csv",
        "path_tariff_maps_gas": "tariff_maps/gas/rie.csv",
        "path_resstock_metadata": "s3://example/metadata.parquet",
        "path_resstock_loads": "s3://example/loads/",
        "path_supply_energy_mc": "s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet",
        "path_supply_capacity_mc": "s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet",
        "path_dist_and_sub_tx_mc": "s3://example/td.parquet",
        "path_utility_assignment": "s3://example/utility_assignment.parquet",
        "path_tariffs_gas": "tariffs/gas",
        "path_outputs": "/data.sb/switchbox/cairo/outputs/hp_rates/ri/test",
        "path_tariffs_electric": "hp: tariffs/electric/rie_hp_seasonalTOU_supply.json, non-hp: tariffs/electric/rie_flat_supply.json",
        "utility_revenue_requirement": "rev_requirement/rie_hp_vs_nonhp.yaml",
        "add_supply_revenue_requirement": "FALSE",
        "run_includes_subclasses": "TRUE",
        "subclass_group_col": "has_hp",
        "subclass_selectors": "hp:true,non-hp:false",
        "path_electric_utility_stats": "s3://example/eia861.parquet",
        "solar_pv_compensation": "net_metering",
        "year_run": "2025",
        "year_dollar_conversion": "2025",
        "process_workers": "8",
        "sample_size": "",
        "elasticity": "0.0",
    }


def test_row_to_run_uses_run_includes_supply() -> None:
    """run_includes_supply is emitted (from either new or old column name)."""
    row = _base_row()
    headers = list(row.keys())
    run = _row_to_run(row, headers)
    assert "run_includes_supply" in run
    assert run["run_includes_supply"] is False
    assert "add_supply_revenue_requirement" not in run


def test_row_to_run_reads_run_includes_subclasses_from_sheet() -> None:
    """run_includes_subclasses is read from the sheet column, not derived."""
    row = _base_row()
    headers = list(row.keys())
    run = _row_to_run(row, headers)
    assert run["run_includes_subclasses"] is True

    row_false = _base_row()
    row_false["run_includes_subclasses"] = "FALSE"
    run_false = _row_to_run(row_false, list(row_false.keys()))
    assert run_false["run_includes_subclasses"] is False


def test_row_to_run_reads_residual_allocation() -> None:
    """residual_allocation_delivery/supply are included when non-blank, omitted when blank."""
    row = _base_row()
    row["residual_allocation_delivery"] = "percustomer"
    row["residual_allocation_supply"] = "passthrough"
    run = _row_to_run(row, list(row.keys()))
    assert run["residual_allocation_delivery"] == "percustomer"
    assert run["residual_allocation_supply"] == "passthrough"

    row_blank = _base_row()
    row_blank["residual_allocation_delivery"] = ""
    row_blank["residual_allocation_supply"] = ""
    run_blank = _row_to_run(row_blank, list(row_blank.keys()))
    assert "residual_allocation_delivery" not in run_blank
    assert "residual_allocation_supply" not in run_blank

    row_absent = _base_row()
    run_absent = _row_to_run(row_absent, list(row_absent.keys()))
    assert "residual_allocation_delivery" not in run_absent
    assert "residual_allocation_supply" not in run_absent


def test_row_to_run_includes_path_tou_supply_mc_when_populated() -> None:
    row = _base_row()
    row["path_tou_supply_mc"] = (
        "s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet"
    )
    headers = list(row.keys())

    run = _row_to_run(row, headers)

    assert run["path_tou_supply_mc"] == row["path_tou_supply_mc"]


def test_row_to_run_omits_path_tou_supply_mc_when_blank() -> None:
    row = _base_row()
    row["path_tou_supply_mc"] = ""
    headers = list(row.keys())

    run = _row_to_run(row, headers)

    assert "path_tou_supply_mc" not in run


def test_normalize_deprecated_elec_heat_seasonal_paths() -> None:
    assert _normalize_deprecated_elec_heat_seasonal_paths("") == ""
    assert (
        _normalize_deprecated_elec_heat_seasonal_paths(
            "tariff_maps/electric/u_elec_heat_seasonal_epmc_vs_non_elec_heat_supply.csv"
        )
        == "tariff_maps/electric/u_elec_heat_seasonal_vs_non_elec_heat_supply.csv"
    )
    assert (
        _normalize_deprecated_elec_heat_seasonal_paths(
            "tariffs/electric/u_elec_heat_seasonal_epmc_supply_calibrated.json"
        )
        == "tariffs/electric/u_elec_heat_seasonal_supply_calibrated.json"
    )
    # hp_seasonal_epmc paths must be unchanged
    assert (
        _normalize_deprecated_elec_heat_seasonal_paths(
            "tariffs/electric/u_hp_seasonal_epmc.json"
        )
        == "tariffs/electric/u_hp_seasonal_epmc.json"
    )


def test_row_to_run_normalizes_deprecated_elec_heat_seasonal_sheet_paths() -> None:
    """Runs 33–36 sheet rows may still reference *_elec_heat_seasonal_epmc* files."""
    row = _base_row()
    row["path_tariff_maps_electric"] = (
        "tariff_maps/electric/rie_elec_heat_seasonal_epmc_vs_non_elec_heat.csv"
    )
    row["path_tariffs_electric"] = (
        "electric_heating: tariffs/electric/rie_elec_heat_seasonal_epmc.json, "
        "non_electric_heating: tariffs/electric/rie_non_electric_heating.json"
    )
    headers = list(row.keys())
    run = _row_to_run(row, headers)
    assert (
        run["path_tariff_maps_electric"]
        == "tariff_maps/electric/rie_elec_heat_seasonal_vs_non_elec_heat.csv"
    )
    assert run["path_tariffs_electric"]["electric_heating"] == (
        "tariffs/electric/rie_elec_heat_seasonal.json"
    )
    assert run["path_tariffs_electric"]["non_electric_heating"] == (
        "tariffs/electric/rie_non_electric_heating.json"
    )


# ---------------------------------------------------------------------------
# _parse_subclass_selectors
# ---------------------------------------------------------------------------


def test_parse_subclass_selectors_simple() -> None:
    """Single-value per subclass (hp/non-hp case)."""
    result = _parse_subclass_selectors("hp:true,non-hp:false")
    assert result == {"hp": "true", "non-hp": "false"}


def test_parse_subclass_selectors_multi_value() -> None:
    """Pipe-separated values are joined with commas."""
    result = _parse_subclass_selectors(
        "electric_heating:heat_pump|electrical_resistance,"
        "non_electric_heating:natgas|delivered_fuels|other"
    )
    assert result == {
        "electric_heating": "heat_pump,electrical_resistance",
        "non_electric_heating": "natgas,delivered_fuels,other",
    }


def test_parse_subclass_selectors_empty() -> None:
    assert _parse_subclass_selectors("") == {}
    assert _parse_subclass_selectors("   ") == {}


def test_parse_subclass_selectors_missing_colon_raises() -> None:
    import pytest

    with pytest.raises(ValueError, match="must be 'key:value"):
        _parse_subclass_selectors("hp_true,non-hp:false")


def test_parse_subclass_selectors_duplicate_key_raises() -> None:
    import pytest

    with pytest.raises(ValueError, match="duplicate key"):
        _parse_subclass_selectors("hp:true,hp:false")


# ---------------------------------------------------------------------------
# _row_to_run: subclass_config emission
# ---------------------------------------------------------------------------


def test_row_to_run_emits_subclass_config() -> None:
    """subclass_config is emitted when both subclass columns are set."""
    row = _base_row()
    headers = list(row.keys())
    run = _row_to_run(row, headers)
    assert "subclass_config" in run
    sc = cast(dict[str, object], run["subclass_config"])
    assert sc["group_col"] == "has_hp"
    assert sc["selectors"] == {"hp": "true", "non-hp": "false"}


def test_row_to_run_subclass_config_multi_value_selectors() -> None:
    """Pipe-separated selectors are parsed into comma-joined values."""
    row = _base_row()
    row["subclass_group_col"] = "heating_type_v2"
    row["subclass_selectors"] = (
        "electric_heating:heat_pump|electrical_resistance,"
        "non_electric_heating:natgas|delivered_fuels|other"
    )
    headers = list(row.keys())
    run = _row_to_run(row, headers)
    sc = cast(dict[str, object], run["subclass_config"])
    assert sc["group_col"] == "heating_type_v2"
    selectors = cast(dict[str, str], sc["selectors"])
    assert selectors["electric_heating"] == "heat_pump,electrical_resistance"
    assert selectors["non_electric_heating"] == "natgas,delivered_fuels,other"


def test_row_to_run_no_subclass_config_when_not_subclassed() -> None:
    """No subclass_config emitted when run_includes_subclasses is FALSE."""
    row = _base_row()
    row["run_includes_subclasses"] = "FALSE"
    row["subclass_group_col"] = ""
    row["subclass_selectors"] = ""
    headers = list(row.keys())
    run = _row_to_run(row, headers)
    assert "subclass_config" not in run


def test_row_to_run_subclass_config_columns_absent_non_subclassed() -> None:
    """No error when run_includes_subclasses is FALSE and subclass columns are absent."""
    row = _base_row()
    row["run_includes_subclasses"] = "FALSE"
    # Remove the subclass columns entirely (simulate old sheet without them)
    row.pop("subclass_group_col")
    row.pop("subclass_selectors")
    headers = list(row.keys())
    run = _row_to_run(row, headers)
    assert "subclass_config" not in run


def test_row_to_run_raises_when_subclassed_but_missing_subclass_config() -> None:
    """ValueError when run_includes_subclasses is TRUE but subclass columns are blank."""
    import pytest

    row = _base_row()
    row["subclass_group_col"] = ""
    row["subclass_selectors"] = ""
    headers = list(row.keys())
    with pytest.raises(ValueError, match="subclass_group_col"):
        _row_to_run(row, headers)


def test_hoist_shared_subclass_config_removes_duplicate_run_blocks() -> None:
    runs: dict[int, dict[str, object]] = {
        1: {"run_includes_subclasses": False},
        5: {
            "run_includes_subclasses": True,
            "subclass_config": {
                "group_col": "has_hp",
                "selectors": {"hp": "true", "non-hp": "false"},
            },
        },
        6: {
            "run_includes_subclasses": True,
            "subclass_config": {
                "group_col": "has_hp",
                "selectors": {"hp": "true", "non-hp": "false"},
            },
        },
    }

    shared = _hoist_shared_subclass_config(runs)

    assert shared == {
        "group_col": "has_hp",
        "selectors": {"hp": "true", "non-hp": "false"},
    }
    assert "subclass_config" not in runs[5]
    assert "subclass_config" not in runs[6]


def test_hoist_shared_subclass_config_keeps_run_level_when_configs_differ() -> None:
    runs: dict[int, dict[str, object]] = {
        5: {
            "run_includes_subclasses": True,
            "subclass_config": {
                "group_col": "has_hp",
                "selectors": {"hp": "true", "non-hp": "false"},
            },
        },
        25: {
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

    shared = _hoist_shared_subclass_config(runs)

    assert shared is None
    assert "subclass_config" in runs[5]
    assert "subclass_config" in runs[25]


def test_hoist_shared_subclass_config_keeps_outlier_run_overrides() -> None:
    runs: dict[int, dict[str, object]] = {
        5: {
            "run_includes_subclasses": True,
            "subclass_config": {
                "group_col": "has_hp",
                "selectors": {"hp": "true", "non-hp": "false"},
            },
        },
        6: {
            "run_includes_subclasses": True,
            "subclass_config": {
                "group_col": "has_hp",
                "selectors": {"hp": "true", "non-hp": "false"},
            },
        },
        25: {
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

    shared = _hoist_shared_subclass_config(runs)

    assert shared == {
        "group_col": "has_hp",
        "selectors": {"hp": "true", "non-hp": "false"},
    }
    assert "subclass_config" not in runs[5]
    assert "subclass_config" not in runs[6]
    assert runs[25]["subclass_config"] == {
        "group_col": "heating_type_v2",
        "selectors": {
            "electric_heating": "heat_pump,electrical_resistance",
            "non_electric_heating": "natgas,delivered_fuels,other",
        },
    }
