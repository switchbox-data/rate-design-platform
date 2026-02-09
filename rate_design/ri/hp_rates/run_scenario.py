"""Entrypoint for running RI heat pump rate scenarios - Residential (non-LMI)."""

import json
import logging
from pathlib import Path

from cairo.rates_tool.systemsimulator import (
    MeetRevenueSufficiencySystemWide,
    _return_export_compensation_rate,
    _return_revenue_requirement_target,
)
from cairo.utils.marginal_costs.marginal_cost_calculator import (
    _load_cambium_marginal_costs,
    add_distribution_costs,
)

from utils.cairo import (
    _initialize_tariffs,
    _return_load,
    build_bldg_id_to_load_filepath,
    patch_postprocessor_weight_handling,
    return_buildingstock,
)
from utils.generate_precalc_mapping import generate_default_precalc_mapping
from utils.reweight_customer_counts import reweight_customer_counts

log = logging.getLogger("rates_analysis").getChild("tests")

log.info(".... Beginning RI residential (non-LMI) rate scenario - RIE A-16 tariff")

# Apply patch for combined bill weight column handling
patch_postprocessor_weight_handling()

prototype_ids = [
    134,
    373,
    635,
    958,
    993,
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
    / "metadata-sb.parquet"
)
path_resstock_loads = (
    path_resstock / "load_curve_hourly" / f"state={state}" / f"upgrade={upgrade}"
)

path_cambium_marginal_costs = Path("/data.sb/nrel/cambium/dummy_rie_marginal_costs.csv")

path_results = Path("/data.sb/switchbox/cairo/ri_default_test_run/")
# TODO: alex - figure out how cairo is adjusting for inflation and make sure this is consistent with the test scenario parameters
test_revenue_requirement_target = 241869601  # $241,869,601
test_year_run = 2019  # set to analysis year, will be used to set datetime on load curves AND for inflation adjustment target year
year_dollar_conversion = 2025  # this is a placeholder value that is required but never applied. See dollar_year_conversion._apply_price_inflator() - functionally returns its own input df
test_solar_pv_compensation = "net_metering"

target_customer_count = 451381  # Target customer count for utility territory

# TODO: lee - take the prototype_id's above, and create a (bldg_id, tariff_key) mapping for the prototype_ids, then have path_tariff_map point to the mapping file.
path_tariff_map = path_config / "tariff_map" / "dummy_electric_tariff_ri_run_1.csv"
tariff_map_name = path_tariff_map.stem
path_gas_tariff_map = path_config / "tariff_map" / "dummy_gas_tariff_ri_run_1.csv"

process_workers = 20

# RIE Residential Tariffs (A-16 for standard residential)
# Fixed Customer Charge: $6.75/month, Volumetric Distribution Charge: $0.06455/kWh
tariff_paths = {
    "rie_a16": path_config / "tariff_structure" / "tariff_structure_rie_a16.json",
}
gas_tariff_paths = {
    "dummy_gas": path_config / "tariff_structure" / "tariff_structure_dummy_gas.json",
}

# Load in and manipulate tariff information as needed for bill calculation
tariffs_params, tariff_map_df = _initialize_tariffs(
    tariff_map=path_tariff_map,
    building_stock_sample=prototype_ids,
    tariff_paths=tariff_paths,
)

# Initialize gas tariffs using the same pattern as electricity
gas_tariffs_params, gas_tariff_map_df = _initialize_tariffs(
    tariff_map=path_gas_tariff_map,
    building_stock_sample=prototype_ids,
    tariff_paths=gas_tariff_paths,
)

# Generate precalc mapping from RIE A-16 tariff structure
# rel_values derived proportionally from rates (normalized to min rate = 1.0)
precalc_mapping = generate_default_precalc_mapping(
    tariff_path=path_config / "tariff_structure" / "tariff_structure_rie_a16.json",
    tariff_key="rie_a16",
)


# read in basic customer-level information
customer_metadata = return_buildingstock(
    metadata_path=path_resstock_metadata,
    building_ids=prototype_ids,
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
    building_ids=prototype_ids,
)

# process load directly or find where load stored
# TODO: Replace with function somewhere in test_utils or similar. Input prototype_ids
# and return 8760 profiles for each. Prototype ids and tariff. Make sure nice mathematical
# properties. Maybe constants_tests.py has data needed for this.
raw_load_elec = _return_load(
    load_type="electricity",
    target_year=test_year_run,
    load_filepath_key=bldg_id_to_load_filepath,
    force_tz="EST",
)
raw_load_gas = _return_load(
    load_type="gas",
    target_year=test_year_run,
    load_filepath_key=bldg_id_to_load_filepath,
    force_tz="EST",
)

# calculate and otherwise modify the revenue requirement as needed
# load bulk power marginal costs in $/kWh (assumed to be static)
bulk_marginal_costs = _load_cambium_marginal_costs(
    path_cambium_marginal_costs, test_year_run
)
# Load distribution cost parameters from config
# Sources: AESC 2024 ($69/kW-year), RI Energy Rate Case RY1 NCP data (nc_ratio=1.41)
with open(path_config / "distribution_cost_params.json") as f:
    dist_cost_params = json.load(f)

# Calculate distribution marginal costs in $/kWh (dynamic based on net load)
distribution_marginal_costs = add_distribution_costs(
    raw_load_elec,
    annual_future_distr_costs=dist_cost_params["annual_future_distr_costs"],
    distr_peak_hrs=dist_cost_params["distr_peak_hrs"],
    nc_ratio_baseline=dist_cost_params["nc_ratio_baseline"],
)

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
    gas_tariff_map=gas_tariff_map_df,
    load_cols="total_fuel_electricity",
    marginal_system_prices=marginal_system_prices,
    costs_by_type=costs_by_type,
    solar_pv_compensation=None,
    sell_rate=sell_rate,
    low_income_strategy=None,
    low_income_participation_target=None,
    low_income_bill_assistance_program=None,
)

log.info(".... Completed RI residential (non-LMI) rate scenario simulation")
