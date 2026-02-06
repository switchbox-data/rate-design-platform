"""Entrypoint for running NY heat pump rate scenarios (stub)."""

import logging
from pathlib import Path

import pandas as pd
from cairo.rates_tool.loads import _return_load, return_buildingstock
from cairo.rates_tool.systemsimulator import (
    MeetRevenueSufficiencySystemWide,
    _initialize_tariffs,
    _return_export_compensation_rate,
    _return_revenue_requirement_target,
)
from cairo.utils.marginal_costs.marginal_cost_calculator import (
    _load_cambium_marginal_costs,
    add_distribution_costs,
)

# from tests import constant_tests as const_vars
# from tests.utils import reweight_customer_metadata
from utils.cairo import build_bldg_id_to_load_filepath
from utils.reweight_customer_counts import reweight_customer_counts

log = logging.getLogger("rates_analysis").getChild("tests")

log.info(
    ".... Beginning basic test of functions - run full simulation: Dummy - Precalculation"
)

prototype_ids = [
    3837,
    8510,
    20961,
    29041,
    44581,
]

run_name = "ri_default_test_run"
path_project = Path("./rate_design/ri/hp_rates")
path_resstock = Path("/data.sb/nrel/resstock/res_2024_amy2018_2/")
path_config = path_project / "data"

state = "RI"
upgrade = "00"
path_resstock_metadata = (
    path_resstock
    / "metadata"
    / f"state={state}"
    / f"upgrade={upgrade}"
    / "metadata.parquet"
)
path_resstock_loads = (
    path_resstock / "load_curve_hourly" / f"state={state}" / f"upgrade={upgrade}"
)
# TODO: alex - make dummy zero-valued marginal costs file for RI test scenario, and update this path to point to that file
path_cambium_marginal_costs = (
    path_config / "marginal_costs" / "example_marginal_costs.csv"
)
path_results = Path("/data.sb/switchbox/cairo/ri_default_test_run/")
# TODO: alex - figure out how cairo is adjusting for inflation and make sure this is consistent with the test scenario parameters
test_revenue_requirement_target = 241869601  # $241,869,601
test_year_run = 2019
year_dollar_conversion = 2025
test_solar_pv_compensation = "net_metering"

target_customer_count = 451381  # Target customer count for utility territory
# TODO: lee - update this to point to the actual tariff map for the test scenario, and make sure it has the necessary information for the test scenario (e.g. contains the tariffs being tested, and any necessary parameters for those tariffs)
path_tariff_map = path_config / "tariff_map" / "precalculation_testing.csv"
tariff_map_name = path_tariff_map.stem
# TODO: lee - make dummy gas tariff files for RI test scenario, and update this to point to those files
gas_tariff_map_name = "dummy_gas"  # Gas tariff map name for gas bill calculation

process_workers = 20
# TODO: sherry - replace with actual tariff paths for RI test scenario
# TODO: sherry - make RIE tariffs
# Fixed Customer Charge: Both A-16 and A-60 are proposed to increase to $6.75 per month. Volumetric Distribution Charge: Both A-16 and A-60 are proposed to increase to $0.06455 per kWh
tariff_paths = [
    path_config / "tariffs" / "tariff_1.json",
    path_config / "tariffs" / "tariff_2.json",
]
# TODO: juan pablo - point tariff paths to actual RDP tariff files
# load in and manipulate tariff information as needed for bill calculation
# tariffs_params, tariff_map_df = _initialize_tariffs(
#    tariff_map=path_tariff_map,
#    building_stock_sample=prototype_ids,
#    tariff_paths=tariff_paths,
# )
tariffs_params, tariff_map_df = _initialize_tariffs(
    tariff_map=path_tariff_map,
    building_stock_sample=prototype_ids,
)

# TODO: sherry - update this depending on RIE tariff structure, make into JSON and put in config folder if needed, and make sure it has the necessary information for the test scenario (e.g. contains the periods and tiers being tested, and any necessary parameters for those)
precalc_mapping = (
    pd.DataFrame()
    .from_dict(
        {
            f"period_{i + 1}": {
                "period": p[0],
                "tier": p[1],
                "rel_value": [1.0, 1.25, 2.0, 2.5, 1.5, 1.75][i],
                "tariff": "dummy_electrical_fixed",
            }
            for i, p in enumerate(
                tariffs_params["dummy_electrical_fixed"]["ur_ec_tou_mat"]
            )
        }
    )
    .T
)


# read in basic customer-level information
customer_metadata = return_buildingstock(
    load_scenario=path_resstock_metadata,
    building_stock_sample=prototype_ids,
)

# Reweight customer metadata to match utility customer count
customer_metadata = reweight_customer_counts(customer_metadata, target_customer_count)

# read in solar PV compensation structure, and which customers have adopted solar PV
sell_rate = _return_export_compensation_rate(
    year_run=test_year_run,
    solar_pv_compensation=test_solar_pv_compensation,
    solar_pv_export_import_ratio=1.0,
    tariff_dict=tariffs_params,
)

bldg_id_to_load_filepath = build_bldg_id_to_load_filepath(
    path_resstock_loads=path_resstock_loads,
    path_resstock=path_resstock,
)

# process load directly or find where load stored
# TODO: Replace with function somewhere in test_utils or similar. Input prototype_ids
# and return 8760 profiles for each. Prototype ids and tariff. Make sure nice mathematical
# properties. Maybe constants_tests.py has data needed for this.
raw_load_elec = _return_load(
    load_type="electricity",
    target_year=test_year_run,
    building_stock_sample=prototype_ids,
    load_filepath_key=bldg_id_to_load_filepath,
    force_tz="EST",
)
raw_load_gas = _return_load(
    load_type="gas",
    target_year=test_year_run,
    building_stock_sample=prototype_ids,
    load_filepath_key=bldg_id_to_load_filepath,
    force_tz="EST",
)

# calculate and otherwise modify the revenue requirement as needed
# load bulk power marginal costs in $/kWh (assumed to be static)
bulk_marginal_costs = _load_cambium_marginal_costs(
    path_cambium_marginal_costs, test_year_run
)
# TODO: sherry - add in distribution cost parameters as config file and load in
# calculate distribution marginal costs in $/kWh (dynamic based on net load)
distribution_marginal_costs = add_distribution_costs(raw_load_elec, test_year_run)

(revenue_requirement, marginal_system_prices, marginal_system_costs, costs_by_type) = (
    _return_revenue_requirement_target(
        building_load=raw_load_elec,
        sample_weight=customer_metadata[["bldg_id", "weight"]],
        revenue_requirement_target=test_revenue_requirement_target,
        residual_cost=None,
        residual_cost_frac=None,
        bulk_marginal_costs=bulk_marginal_costs,
        distribution_marginal_costs=distribution_marginal_costs,
        low_income_strategy=None,
    )
)

bs = MeetRevenueSufficiencySystemWide(
    run_type="precalc",
    year_run=test_year_run,
    year_dollar_conversion=year_dollar_conversion,
    process_workers=process_workers,
    building_stock_sample=prototype_ids,
    run_name=run_name,
    output_dir=path_results,  # will default to this folder if not pass, mostly testing ability for user to pass arbitrary output directory
)

# TODO: add (maybe post-processing)module for delivered fuels bills
bs.simulate(
    revenue_requirement=revenue_requirement,
    tariffs_params=tariffs_params,
    tariff_map=tariff_map_df,
    precalc_period_mapping=precalc_mapping,
    customer_metadata=customer_metadata,
    customer_electricity_load=raw_load_elec,
    customer_gas_load=raw_load_gas,
    gas_tariff_map=gas_tariff_map_name,
    load_cols="total_fuel_electricity",
    marginal_system_prices=marginal_system_prices,
    costs_by_type=costs_by_type,
    solar_pv_compensation=None,
    sell_rate=sell_rate,
    low_income_strategy=None,
    low_income_participation_target=None,
    low_income_bill_assistance_program=None,
)

log.info(".... Completed full simulation run for Dummy - Precalculation")
