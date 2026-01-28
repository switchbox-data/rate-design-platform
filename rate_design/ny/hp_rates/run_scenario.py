"""Entrypoint for running NY heat pump rate scenarios (stub)."""

import logging
import json
import pandas as pd
from pathlib import Path
from cairo.rates_tool.config import OUTPUT_INTERNAL_POSTPROCESSING
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
#from tests import constant_tests as const_vars
#from tests.utils import reweight_customer_metadata
from utils.cairo import build_bldg_id_to_load_filepath

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

path_project = Path("./rate_design/ny/hp_rates")
path_data = path_project / "data"
path_tariff_map = path_data / "tariff_map" / "precalculation_testing.csv"
path_resstock = path_data / "resstock"
path_resstock_metadata = path_resstock / "results_up00.parquet"
path_resstock_loads = path_resstock / "loads"
path_cambium_marginal_costs = path_data / "marginal_costs" / "example_marginal_costs.csv"
path_results = path_project / "results"

test_revenue_requirement_target = 1000000000
test_year_run = 2019
year_dollar_conversion = 2021
test_solar_pv_compensation = "net_metering"
run_name = "tests_precalc"

process_workers = 20

tariff_paths = [
    path_data / "tariffs" / "tariff_1.json",
    path_data / "tariffs" / "tariff_2.json",
] 

# load in and manipulate tariff information as needed for bill calculation
#tariffs_params, tariff_map_df = _initialize_tariffs(
#    tariff_map=path_tariff_map,
#    building_stock_sample=prototype_ids,
#    tariff_paths=tariff_paths,
#)
tariffs_params, tariff_map_df = _initialize_tariffs(
    tariff_map=path_tariff_map,
    building_stock_sample=prototype_ids,
)

# TODO: update this depending on the tariff structure
precalc_mapping = pd.DataFrame().from_dict(
    {f"period_{i+1}":{
        "period":p[0], 
        "tier":p[1], 
        "rel_value": [1.0, 1.25, 2.0, 2.5, 1.5, 1.75][i], 
        "tariff":"dummy_electrical_fixed"
    } for i,p in enumerate(tariffs_params["dummy_electrical_fixed"]["ur_ec_tou_mat"])}
).T


# read in basic customer-level information
customer_metadata = return_buildingstock(
        load_scenario=path_resstock_metadata,
        building_stock_sample=prototype_ids,
    )


# read in solar PV compensation structure, and which customers have adopted solar PV
sell_rate = _return_export_compensation_rate(
    year_run=test_year_run,
    solar_pv_compensation=test_solar_pv_compensation,
    solar_pv_export_import_ratio=1.0,
    tariff_dict = tariffs_params,
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
    raw_load_passed=None,
    load_type="electricity",
    target_year=test_year_run,
    building_stock_sample=prototype_ids,
    load_filepath_key=bldg_id_to_load_filepath,
    force_tz="EST"
)

# calculate and otherwise modify the revenue requirement as needed
# load bulk power marginal costs in $/kWh (assumed to be static)
bulk_marginal_costs = _load_cambium_marginal_costs(
    path_cambium_marginal_costs, 
    test_year_run
)
# calculate distribution marginal costs in $/kWh (dynamic based on net load)
distribution_marginal_costs = add_distribution_costs(
    raw_load_elec, test_year_run
)

(
    revenue_requirement, 
    marginal_system_prices, 
    marginal_system_costs, 
    costs_by_type
) = _return_revenue_requirement_target(
    building_load = raw_load_elec,
    sample_weight = customer_metadata[["bldg_id", "weight"]],
    revenue_requirement_target = test_revenue_requirement_target,
    residual_cost = None,
    residual_cost_frac = None,
    bulk_marginal_costs = bulk_marginal_costs,
    distribution_marginal_costs = distribution_marginal_costs,
    low_income_strategy = None
)

bs = MeetRevenueSufficiencySystemWide(
    run_type="precalc",
    year_run=test_year_run,
    year_dollar_conversion=year_dollar_conversion,
    process_workers=process_workers,
    building_stock_sample=prototype_ids,
    run_name=run_name,
    output_dir=path_results, # will default to this folder if not pass, mostly testing ability for user to pass arbitrary output directory
)

# TODO: figure out how to get gas loads
# TODO: figure out how to get 
bs.simulate(
    revenue_requirement=revenue_requirement,
    tariffs_params = tariffs_params, 
    tariff_map=tariff_map_df,
    precalc_period_mapping=precalc_mapping,
    customer_metadata = customer_metadata, 
    customer_electricity_load=raw_load_elec,
    customer_gas_load=None,
    load_cols='total_fuel_electricity',
    marginal_system_prices=marginal_system_prices,
    costs_by_type=costs_by_type,
    solar_pv_compensation=None,
    sell_rate = sell_rate,
    low_income_strategy=None,
    low_income_participation_target=None,
    low_income_bill_assistance_program=None,
)

log.info(".... Completed full simulation run for Dummy - Precalculation")

