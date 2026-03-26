from utils.pre.create_scenario_yamls import _row_to_run


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
    """residual_allocation is included when non-blank, omitted when blank."""
    row = _base_row()
    row["residual_allocation"] = "percustomer"
    run = _row_to_run(row, list(row.keys()))
    assert run["residual_allocation"] == "percustomer"

    row_blank = _base_row()
    row_blank["residual_allocation"] = ""
    run_blank = _row_to_run(row_blank, list(row_blank.keys()))
    assert "residual_allocation" not in run_blank

    row_absent = _base_row()
    run_absent = _row_to_run(row_absent, list(row_absent.keys()))
    assert "residual_allocation" not in run_absent


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
