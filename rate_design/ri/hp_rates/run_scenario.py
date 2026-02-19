"""Entrypoint for running RI heat pump rate scenarios - Residential (non-LMI)."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import polars as pl
import yaml
from cairo.rates_tool.loads import (
    _return_load,
    return_buildingstock,
)
from cairo.rates_tool.systemsimulator import (
    MeetRevenueSufficiencySystemWide,
    _initialize_tariffs,
    _return_export_compensation_rate,
    _return_revenue_requirement_target,
)

from utils.cairo import (
    _load_cambium_marginal_costs,
    build_bldg_id_to_load_filepath,
    load_distribution_marginal_costs,
)
from utils.pre.generate_precalc_mapping import generate_default_precalc_mapping

log = logging.getLogger("rates_analysis").getChild("tests")

# Resolve paths relative to this script so the scenario can be run from any CWD.
PATH_PROJECT = Path(__file__).resolve().parent
PATH_CONFIG = PATH_PROJECT / "config"
DEFAULT_OUTPUT_DIR = Path("/data.sb/switchbox/cairo/ri_hp_rates/analysis_outputs")
DEFAULT_SCENARIO_CONFIG = PATH_CONFIG / "scenarios.yaml"


@dataclass(slots=True)
class ScenarioSettings:
    """All inputs needed to run one RI scenario."""

    run_name: str
    run_type: str
    state: str
    utility: str
    path_results: Path
    path_resstock_metadata: Path
    path_resstock_loads: Path
    path_cambium_marginal_costs: str | Path
    path_td_marginal_costs: str | Path
    path_tariff_maps_electric: Path
    path_tariff_maps_gas: Path
    path_tariffs_electric: dict[str, Path]
    path_tariffs_gas: dict[str, Path]
    precalc_tariff_path: Path
    precalc_tariff_key: str
    utility_delivery_revenue_requirement: float
    utility_customer_count: int
    year_run: int
    year_dollar_conversion: int
    process_workers: int
    solar_pv_compensation: str = "net_metering"
    add_supply_revenue_requirement: bool = False


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


def _parse_bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"Invalid boolean for {field_name}: {value}")


def _resolve_path(value: str, base_dir: Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (base_dir / path)


def _resolve_path_or_uri(value: str, base_dir: Path) -> str | Path:
    if value.startswith("s3://"):
        return value
    return _resolve_path(value, base_dir)


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected mapping for `{field_name}`")
    return value


def _tariff_map_keys(path_tariff_map: Path) -> set[str]:
    """Return the set of tariff_key values in a tariff map CSV (electric or gas)."""
    df = pl.read_csv(path_tariff_map)
    if "tariff_key" not in df.columns:
        raise ValueError(
            f"Tariff map {path_tariff_map} must have a 'tariff_key' column"
        )
    return set(df["tariff_key"].unique().to_list())


def _parse_path_tariffs(
    value: Any,
    path_tariff_map: Path,
    base_dir: Path,
    label: str,
) -> dict[str, Path]:
    """Parse path_tariffs_electric from YAML (list only) and reconcile with map.

    Value must be a list of path strings; keys are derived from filename stem
    (e.g. tariffs/electric/foo.json -> foo). Every tariff_key in the map must
    have an entry, and every list entry must appear in the map.
    """
    if not isinstance(value, list):
        raise ValueError(
            f"path_tariffs_{label} must be a list of paths; got {type(value).__name__}"
        )
    path_tariffs = {}
    for item in value:
        if not isinstance(item, str):
            raise ValueError(
                f"path_tariffs_{label} list must contain path strings; "
                f"got {type(item).__name__}"
            )
        path = _resolve_path(item, base_dir)
        key = path.stem
        if key in path_tariffs:
            raise ValueError(
                f"path_tariffs_{label}: duplicate key '{key}' from paths "
                f"{path_tariffs[key]} and {path}"
            )
        path_tariffs[key] = path

    map_keys = _tariff_map_keys(path_tariff_map)
    list_keys = set(path_tariffs.keys())
    only_in_map = map_keys - list_keys
    only_in_list = list_keys - map_keys
    if only_in_map:
        raise ValueError(
            f"{label.capitalize()} tariff map references tariff_key(s) with no file "
            f"in path_tariffs_{label}: {sorted(only_in_map)}"
        )
    if only_in_list:
        raise ValueError(
            f"path_tariffs_{label} includes file(s) not referenced in {label} "
            f"tariff map: {sorted(only_in_list)}"
        )
    return path_tariffs


def _parse_path_tariffs_gas(
    value: Any,
    path_tariff_map: Path,
    base_dir: Path,
) -> dict[str, Path]:
    """Parse path_tariffs_gas from YAML: must be a directory path (string).

    Unique tariff_key values are read from the gas tariff map; each must have
    a file at directory/tariff_key.json.
    """
    if not isinstance(value, str):
        raise ValueError(
            "path_tariffs_gas must be a directory path (string) containing "
            f"gas tariff JSONs named <tariff_key>.json; got {type(value).__name__}"
        )
    path_dir = _resolve_path(value, base_dir)
    if not path_dir.is_dir():
        raise ValueError(
            f"path_tariffs_gas is a directory path but not a directory: {path_dir}"
        )
    map_keys = _tariff_map_keys(path_tariff_map)
    path_tariffs = {k: path_dir / f"{k}.json" for k in map_keys}
    missing = [k for k, p in path_tariffs.items() if not p.is_file()]
    if missing:
        raise ValueError(
            "Gas tariff map references tariff_key(s) with no file under "
            f"{path_dir}: {sorted(missing)}. "
            f"Expected e.g. {path_dir / 'tariff_key.json'}"
        )
    return path_tariffs


def _default_precalc_tariff(path_tariffs_electric: dict[str, Path]) -> tuple[str, Path]:
    """Choose default precalc tariff from first configured electric tariff."""
    if not path_tariffs_electric:
        raise ValueError("path_tariffs_electric must contain at least one tariff")
    return next(iter(path_tariffs_electric.items()))


def _require_value(run: dict[str, Any], field_name: str) -> Any:
    value = run.get(field_name)
    if value is None:
        raise ValueError(f"Missing required field `{field_name}` in run config")
    return value


def _load_run_from_yaml(scenario_config: Path, run_num: int) -> dict[str, Any]:
    with scenario_config.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(
            f"Invalid YAML format in {scenario_config}: expected top-level map"
        )
    runs = cast(
        dict[str | int, Any],
        _require_mapping(data.get("runs"), "runs"),
    )
    run = runs.get(run_num) or runs.get(str(run_num))
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
    utility = str(_require_value(run, "utility")).lower()
    mode = str(run.get("run_type", "precalc"))
    year_run = _parse_int(run.get("year_run"), "year_run")
    year_dollar_conversion = _parse_int(
        run.get("year_dollar_conversion"),
        "year_dollar_conversion",
    )
    process_workers = _parse_int(run.get("process_workers", 20), "process_workers")
    utility_delivery_revenue_requirement = float(
        _parse_int(
            _require_value(run, "utility_delivery_revenue_requirement"),
            "utility_delivery_revenue_requirement",
        )
    )
    utility_customer_count = _parse_int(
        _require_value(run, "utility_customer_count"),
        "utility_customer_count",
    )
    solar_pv_compensation = str(run.get("solar_pv_compensation", "net_metering"))

    path_tariff_maps_electric = _resolve_path(
        str(_require_value(run, "path_tariff_maps_electric")),
        PATH_CONFIG,
    )
    path_tariffs_electric = _parse_path_tariffs(
        _require_value(run, "path_tariffs_electric"),
        path_tariff_maps_electric,
        PATH_CONFIG,
        "electric",
    )
    path_tariff_maps_gas = _resolve_path(
        str(_require_value(run, "path_tariff_maps_gas")),
        PATH_CONFIG,
    )
    path_tariffs_gas = _parse_path_tariffs_gas(
        _require_value(run, "path_tariffs_gas"),
        path_tariff_maps_gas,
        PATH_CONFIG,
    )

    precalc_tariff_key_raw = run.get("precalc_tariff_key")
    precalc_tariff_path_raw = run.get("precalc_tariff_path")
    if precalc_tariff_key_raw is None and precalc_tariff_path_raw is None:
        precalc_tariff_key, precalc_tariff_path = _default_precalc_tariff(
            path_tariffs_electric
        )
    elif precalc_tariff_path_raw is None:
        precalc_tariff_key = str(precalc_tariff_key_raw)
        try:
            precalc_tariff_path = path_tariffs_electric[precalc_tariff_key]
        except KeyError as exc:
            available = sorted(path_tariffs_electric.keys())
            raise ValueError(
                "precalc_tariff_key is not in path_tariffs_electric. "
                f"precalc_tariff_key={precalc_tariff_key!r}, available={available}"
            ) from exc
    else:
        precalc_tariff_path = _resolve_path(str(precalc_tariff_path_raw), PATH_CONFIG)
        precalc_tariff_key = (
            str(precalc_tariff_key_raw)
            if precalc_tariff_key_raw is not None
            else precalc_tariff_path.stem
        )

    default_run_name = str(run.get("run_name", f"ri_rie_run_{run_num:02d}"))
    add_supply_revenue_requirement = _parse_bool(
        run.get(
            "add_supply_revenue_requirement",
            "supply_adj" in precalc_tariff_key,
        ),
        "add_supply_revenue_requirement",
    )
    return ScenarioSettings(
        run_name=run_name_override or default_run_name,
        run_type=mode,
        state=state,
        utility=utility,
        path_results=output_dir,
        path_resstock_metadata=_resolve_path(
            str(_require_value(run, "path_resstock_metadata")),
            PATH_CONFIG,
        ),
        path_resstock_loads=_resolve_path(
            str(_require_value(run, "path_resstock_loads")),
            PATH_CONFIG,
        ),
        path_cambium_marginal_costs=_resolve_path_or_uri(
            str(_require_value(run, "path_cambium_marginal_costs")),
            PATH_CONFIG,
        ),
        path_td_marginal_costs=_resolve_path_or_uri(
            str(_require_value(run, "path_td_marginal_costs")),
            PATH_CONFIG,
        ),
        path_tariff_maps_electric=path_tariff_maps_electric,
        path_tariff_maps_gas=path_tariff_maps_gas,
        path_tariffs_electric=path_tariffs_electric,
        path_tariffs_gas=path_tariffs_gas,
        precalc_tariff_path=precalc_tariff_path,
        precalc_tariff_key=precalc_tariff_key,
        utility_delivery_revenue_requirement=utility_delivery_revenue_requirement,
        utility_customer_count=utility_customer_count,
        year_run=year_run,
        year_dollar_conversion=year_dollar_conversion,
        process_workers=process_workers,
        solar_pv_compensation=solar_pv_compensation,
        add_supply_revenue_requirement=add_supply_revenue_requirement,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Run RI heat-pump scenario using YAML config.")
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


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------


def run(settings: ScenarioSettings) -> None:
    log.info(
        ".... Beginning RI residential (non-LMI) rate scenario simulation: %s",
        settings.run_name,
    )

    # ------------------------------------------------------------------
    # Phase 1: Load data (customer metadata, building loads, marginal costs)
    # ------------------------------------------------------------------

    customer_metadata = return_buildingstock(
        load_scenario=settings.path_resstock_metadata,
        customer_count=settings.utility_customer_count,
        columns=[
            "applicability",
            "postprocess_group.has_hp",
            "postprocess_group.heating_type",
            "in.vintage_acs",
        ],
    )

    bldg_id_to_load_filepath = build_bldg_id_to_load_filepath(
        path_resstock_loads=settings.path_resstock_loads,
    )

    raw_load_elec = _return_load(
        load_type="electricity",
        target_year=settings.year_run,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )
    raw_load_gas = _return_load(
        load_type="gas",
        target_year=settings.year_run,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )

    bulk_marginal_costs = _load_cambium_marginal_costs(
        settings.path_cambium_marginal_costs,
        settings.year_run,
    )

    distribution_marginal_costs = load_distribution_marginal_costs(
        settings.path_td_marginal_costs,
    )

    log.info(
        ".... Loaded distribution marginal costs rows=%s",
        len(distribution_marginal_costs),
    )

    # ------------------------------------------------------------------
    # Phase 2: Initialize tariffs and system requirements
    # ------------------------------------------------------------------

    tariffs_params, tariff_map_df = _initialize_tariffs(
        tariff_map=settings.path_tariff_maps_electric,
        building_stock_sample=None,
        tariff_paths=settings.path_tariffs_electric,
    )

    precalc_mapping = generate_default_precalc_mapping(
        tariff_path=settings.precalc_tariff_path,
        tariff_key=settings.precalc_tariff_key,
    )

    sell_rate = _return_export_compensation_rate(
        year_run=settings.year_run,
        solar_pv_compensation=settings.solar_pv_compensation,
        solar_pv_export_import_ratio=1.0,
        tariff_dict=tariffs_params,
    )

    (
        revenue_requirement,
        marginal_system_prices,
        marginal_system_costs,
        costs_by_type,
    ) = _return_revenue_requirement_target(
        building_load=raw_load_elec,
        sample_weight=customer_metadata[["bldg_id", "weight"]],
        revenue_requirement_target=settings.utility_delivery_revenue_requirement,
        residual_cost=None,
        residual_cost_frac=None,
        bulk_marginal_costs=bulk_marginal_costs,
        distribution_marginal_costs=distribution_marginal_costs,
        low_income_strategy=None,
        delivery_only_rev_req_passed=settings.add_supply_revenue_requirement,
    )

    # ------------------------------------------------------------------
    # Phase 3: Run CAIRO simulation
    # ------------------------------------------------------------------

    bs = MeetRevenueSufficiencySystemWide(
        run_type=settings.run_type,
        year_run=settings.year_run,
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
        gas_tariff_map=settings.path_tariff_maps_gas,
        gas_tariff_str_loc=settings.path_tariffs_gas,
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
