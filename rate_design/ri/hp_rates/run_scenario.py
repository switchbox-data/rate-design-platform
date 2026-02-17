"""Entrypoint for running RI heat pump rate scenarios - Residential (non-LMI)."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from cairo.rates_tool.loads import _return_load, return_buildingstock
from cairo.rates_tool.systemsimulator import (
    MeetRevenueSufficiencySystemWide,
    _initialize_tariffs,
    _return_export_compensation_rate,
    _return_revenue_requirement_target,
)
from cairo.utils.marginal_costs.marginal_cost_calculator import add_distribution_costs

from utils.cairo import _load_cambium_marginal_costs, build_bldg_id_to_load_filepath
from utils.pre.generate_precalc_mapping import generate_default_precalc_mapping

log = logging.getLogger("rates_analysis").getChild("tests")

# Resolve paths relative to this script so the scenario can be run from any CWD.
PATH_PROJECT = Path(__file__).resolve().parent
PATH_CONFIG = PATH_PROJECT / "config"
PATH_RESSTOCK = Path("/data.sb/nrel/resstock/res_2024_amy2018_2/")
DEFAULT_OUTPUT_DIR = Path("/data.sb/switchbox/cairo/ri_hp_rates/")
DEFAULT_SCENARIO_CONFIG = PATH_CONFIG / "run_scenarios.yaml"


@dataclass(slots=True)
class ScenarioSettings:
    """All inputs needed to run one RI scenario."""

    run_name: str
    run_type: str
    path_results: Path
    path_resstock_metadata: Path
    path_resstock_loads: Path
    path_cambium_marginal_costs: Path
    path_tariff_map: Path
    path_gas_tariff_map: Path
    tariff_paths: dict[str, Path]
    gas_tariff_paths: dict[str, Path]
    precalc_tariff_path: Path
    precalc_tariff_key: str
    target_revenue_requirement: float
    target_customer_count: int
    test_year_run: int
    year_dollar_conversion: int
    process_workers: int
    solar_pv_compensation: str = "net_metering"


def _parse_int(value: object, field_name: str) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    cleaned = str(value).strip().replace(",", "")
    if cleaned == "":
        raise ValueError(f"Missing required field: {field_name}")
    try:
        return int(float(cleaned))
    except ValueError as exc:
        raise ValueError(f"Invalid integer for {field_name}: {value}") from exc


def _resolve_path(value: str, base_dir: Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (base_dir / path)


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected mapping for `{field_name}`")
    return value


def _require_value(run: dict[str, Any], field_name: str) -> Any:
    value = run.get(field_name)
    if value is None:
        raise ValueError(f"Missing required field `{field_name}` in run config")
    return value


def _load_run_from_yaml(scenario_config: Path, run_num: int) -> dict[str, Any]:
    with scenario_config.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML format in {scenario_config}: expected top-level map")
    runs = _require_mapping(data.get("runs"), "runs")
    run = runs.get(run_num)
    if run is None:
        run = runs.get(str(run_num))
    if run is None:
        raise ValueError(f"Run {run_num} not found in {scenario_config}")
    return _require_mapping(run, f"runs[{run_num}]")


def _build_settings_from_yaml_run(
    run: dict[str, Any],
    run_num: int,
    output_dir: Path,
    run_name_override: str | None,
) -> ScenarioSettings:
    """Build runtime settings from repo YAML scenario config."""
    state = str(run.get("state", "RI")).upper()
    mode = str(run.get("run_type", "precalc"))
    upgrade = f"{_parse_int(run.get('upgrade', 0), 'upgrade'):02d}"
    test_year_run = _parse_int(run.get("test_year_run"), "test_year_run")
    year_dollar_conversion = _parse_int(
        run.get("year_dollar_conversion"),
        "year_dollar_conversion",
    )
    process_workers = _parse_int(run.get("process_workers", 20), "process_workers")
    target_revenue_requirement = float(
        _parse_int(
            _require_value(run, "target_revenue_requirement"),
            "target_revenue_requirement",
        )
    )
    target_customer_count = _parse_int(
        _require_value(run, "target_customer_count"),
        "target_customer_count",
    )
    solar_pv_compensation = str(run.get("solar_pv_compensation", "net_metering"))

    tariff_paths_raw = _require_mapping(run.get("tariff_paths"), "tariff_paths")
    tariff_paths = {
        str(key): _resolve_path(str(path), PATH_CONFIG)
        for key, path in tariff_paths_raw.items()
    }
    gas_tariff_paths_raw = _require_mapping(run.get("gas_tariff_paths"), "gas_tariff_paths")
    gas_tariff_paths = {
        str(key): _resolve_path(str(path), PATH_CONFIG)
        for key, path in gas_tariff_paths_raw.items()
    }

    default_run_name = str(run.get("run_name", f"ri_rie_run_{run_num:02d}"))

    return ScenarioSettings(
        run_name=run_name_override or default_run_name,
        run_type=mode,
        path_results=output_dir,
        path_resstock_metadata=PATH_RESSTOCK
        / "metadata"
        / f"state={state}"
        / f"upgrade={upgrade}"
        / "metadata-sb.parquet",
        path_resstock_loads=PATH_RESSTOCK
        / "load_curve_hourly"
        / f"state={state}"
        / f"upgrade={upgrade}",
        path_cambium_marginal_costs=_resolve_path(
            str(_require_value(run, "path_cambium_marginal_costs")),
            PATH_CONFIG,
        ),
        path_tariff_map=_resolve_path(
            str(_require_value(run, "path_tariff_map")),
            PATH_CONFIG,
        ),
        path_gas_tariff_map=_resolve_path(
            str(_require_value(run, "path_gas_tariff_map")),
            PATH_CONFIG,
        ),
        tariff_paths=tariff_paths,
        gas_tariff_paths=gas_tariff_paths,
        precalc_tariff_path=_resolve_path(
            str(_require_value(run, "precalc_tariff_path")),
            PATH_CONFIG,
        ),
        precalc_tariff_key=str(_require_value(run, "precalc_tariff_key")),
        target_revenue_requirement=target_revenue_requirement,
        target_customer_count=target_customer_count,
        test_year_run=test_year_run,
        year_dollar_conversion=year_dollar_conversion,
        process_workers=process_workers,
        solar_pv_compensation=solar_pv_compensation,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run RI heat-pump scenario using YAML config."
        )
    )
    parser.add_argument(
        "--scenario-config",
        type=Path,
        default=DEFAULT_SCENARIO_CONFIG,
        help=f"Path to YAML scenario config (default: {DEFAULT_SCENARIO_CONFIG}).",
    )
    parser.add_argument(
        "--run-num",
        type=int,
        required=True,
        help="Run number in the YAML `runs` mapping (e.g. 1 or 2).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output root dir passed to CAIRO.",
    )
    parser.add_argument(
        "--run-name",
        help="Optional override for run name.",
    )
    args = parser.parse_args()
    return args


def _resolve_settings(args: argparse.Namespace) -> ScenarioSettings:
    run = _load_run_from_yaml(args.scenario_config, args.run_num)
    return _build_settings_from_yaml_run(
        run=run,
        run_num=args.run_num,
        output_dir=args.output_dir,
        run_name_override=args.run_name,
    )

def run(settings: ScenarioSettings) -> None:
    log.info(
        ".... Beginning RI residential (non-LMI) rate scenario simulation: %s",
        settings.run_name,
    )

    tariffs_params, tariff_map_df = _initialize_tariffs(
        tariff_map=settings.path_tariff_map,
        building_stock_sample=None,
        tariff_paths=settings.tariff_paths,
    )

    precalc_mapping = generate_default_precalc_mapping(
        tariff_path=settings.precalc_tariff_path,
        tariff_key=settings.precalc_tariff_key,
    )

    customer_metadata = return_buildingstock(
        load_scenario=settings.path_resstock_metadata,
        customer_count=settings.target_customer_count,
        columns=[
            "applicability",
            "postprocess_group.has_hp",
            "postprocess_group.heating_type",
            "in.vintage_acs",
        ],
    )

    sell_rate = _return_export_compensation_rate(
        year_run=settings.test_year_run,
        solar_pv_compensation=settings.solar_pv_compensation,
        solar_pv_export_import_ratio=1.0,
        tariff_dict=tariffs_params,
    )

    bldg_id_to_load_filepath = build_bldg_id_to_load_filepath(
        path_resstock_loads=settings.path_resstock_loads,
    )

    raw_load_elec = _return_load(
        load_type="electricity",
        target_year=settings.test_year_run,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )
    raw_load_gas = _return_load(
        load_type="gas",
        target_year=settings.test_year_run,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )

    bulk_marginal_costs = _load_cambium_marginal_costs(
        settings.path_cambium_marginal_costs,
        settings.test_year_run,
    )
    with open(PATH_CONFIG / "distribution_cost_params.json", encoding="utf-8") as f:
        dist_cost_params = json.load(f)

    distribution_marginal_costs = add_distribution_costs(
        raw_load_elec[["electricity_net"]],
        annual_future_distr_costs=dist_cost_params["annual_future_distr_costs"],
        distr_peak_hrs=dist_cost_params["distr_peak_hrs"],
        nc_ratio_baseline=dist_cost_params["nc_ratio_baseline"],
    )

    (revenue_requirement, marginal_system_prices, marginal_system_costs, costs_by_type) = (
        _return_revenue_requirement_target(
            building_load=raw_load_elec,
            sample_weight=customer_metadata[["bldg_id", "weight"]],
            revenue_requirement_target=settings.target_revenue_requirement,
            residual_cost=None,
            residual_cost_frac=None,
            bulk_marginal_costs=bulk_marginal_costs,
            distribution_marginal_costs=distribution_marginal_costs,
            low_income_strategy=None,
            delivery_only_rev_req_passed=True,
        )
    )

    bs = MeetRevenueSufficiencySystemWide(
        run_type=settings.run_type,
        year_run=settings.test_year_run,
        year_dollar_conversion=settings.year_dollar_conversion,
        process_workers=settings.process_workers,
        run_name=settings.run_name,
        output_dir=settings.path_results,
    )

    bs.simulate(
        revenue_requirement=revenue_requirement,
        tariffs_params=tariffs_params,
        tariff_map=tariff_map_df,
        precalc_period_mapping=precalc_mapping,
        customer_metadata=customer_metadata,
        customer_electricity_load=raw_load_elec,
        customer_gas_load=raw_load_gas,
        gas_tariff_map=settings.path_gas_tariff_map,
        gas_tariff_str_loc=settings.gas_tariff_paths,
        load_cols="total_fuel_electricity",
        marginal_system_prices=marginal_system_prices,
        costs_by_type=costs_by_type,
        solar_pv_compensation=None,
        sell_rate=sell_rate,
        low_income_strategy=None,
        low_income_participation_target=None,
        low_income_bill_assistance_program=None,
    )

    save_file_loc = getattr(bs, "save_file_loc", None)
    if save_file_loc is not None:
        distribution_mc_path = Path(save_file_loc) / "distribution_marginal_costs.csv"
        distribution_marginal_costs.to_csv(distribution_mc_path, index=True)
        log.info(".... Saved distribution marginal costs: %s", distribution_mc_path)

    log.info(".... Completed RI residential (non-LMI) rate scenario simulation")


def main() -> None:
    args = _parse_args()
    settings = _resolve_settings(args)
    run(settings)


if __name__ == "__main__":
    main()
