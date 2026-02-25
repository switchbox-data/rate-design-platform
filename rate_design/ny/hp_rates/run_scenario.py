"""Entrypoint for running NY heat pump rate scenarios - Residential (non-LMI)."""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import sys
import time

import dask
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd
import polars as pl
import yaml
from cairo.rates_tool.loads import (
    return_buildingstock,
)
from cairo.rates_tool.systemsimulator import (
    MeetRevenueSufficiencySystemWide,
    _initialize_tariffs,
    _return_export_compensation_rate,
    _return_revenue_requirement_target,
)

from utils import get_aws_region
from utils.mid.data_parsing import get_residential_customer_count_from_utility_stats
from utils.cairo import (
    _fetch_prototype_ids_by_electric_util,
    _load_cambium_marginal_costs,
    _load_supply_marginal_costs,
    apply_runtime_tou_demand_response,
    build_bldg_id_to_load_filepath,
    load_distribution_marginal_costs,
)
from utils.pre.compute_tou import (
    SeasonTouSpec,
    combine_marginal_costs,
    compute_tou_cost_causation_ratio,
    load_season_specs,
    season_mask,
)
from utils.pre.generate_precalc_mapping import generate_default_precalc_mapping
from utils.types import ElectricUtility
from rate_design.ny.hp_rates.patches import _return_loads_combined

log = logging.getLogger("rates_analysis").getChild("tests")

STORAGE_OPTIONS = {"aws_region": get_aws_region()}
# Resolve paths relative to this script so the scenario can be run from any CWD.
PATH_PROJECT = Path(__file__).resolve().parent
PATH_CONFIG = PATH_PROJECT / "config"
PATH_SCENARIOS = PATH_CONFIG / "scenarios"
SUBCLASS_TO_ALIAS: dict[str, str] = {
    "true": "hp",
    "false": "non-hp",
}


def _scenario_config_from_utility(utility: str) -> Path:
    return PATH_SCENARIOS / f"scenarios_{utility}.yaml"


PATH_TOU_DERIVATION = PATH_CONFIG / "tou_derivation"


@dataclass(slots=True)
class ScenarioSettings:
    """All inputs needed to run one NY scenario."""

    run_name: str
    run_type: str
    state: str
    utility: str
    path_results: Path
    path_resstock_metadata: Path
    path_resstock_loads: Path
    path_utility_assignment: str | Path
    path_td_marginal_costs: str | Path
    path_tariff_maps_electric: Path
    path_tariff_maps_gas: Path
    path_tariffs_electric: dict[str, Path]
    path_tariffs_gas: dict[str, Path]
    utility_delivery_revenue_requirement: float | dict[str, float]
    path_electric_utility_stats: str | Path
    year_run: int
    year_dollar_conversion: int
    process_workers: int
    path_supply_marginal_costs: str | Path | None = None
    path_supply_energy_mc: str | Path | None = None
    path_supply_capacity_mc: str | Path | None = None
    solar_pv_compensation: str = "net_metering"
    add_supply_revenue_requirement: bool = False
    sample_size: int | None = None
    elasticity: float = 0.0
    path_tou_supply_mc: str | Path | None = None


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


def _parse_float(value: object, field_name: str) -> float:
    if isinstance(value, int | float):
        return float(value)
    cleaned = str(value).strip().replace(",", "")
    if cleaned == "":
        raise ValueError(f"Missing required field: {field_name}")
    try:
        return float(cleaned)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {field_name}: {value}") from exc


def _parse_bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(
        f"Invalid boolean for {field_name}: {value!r}. Use unquoted YAML true/false."
    )


def apply_prototype_sample(
    prototype_ids: list[int],
    sample_size: int | None,
    *,
    rng: random.Random | None = None,
) -> list[int]:
    """Optionally take a uniform random sample of prototype IDs.

    When sample_size is None, returns prototype_ids unchanged.
    When sample_size is set, returns a random sample of that size without replacement.
    Raises ValueError if sample_size is not positive or exceeds the number of prototype IDs.
    """
    if sample_size is None:
        return prototype_ids
    if sample_size <= 0:
        raise ValueError(f"sample_size must be positive, got {sample_size}")
    if sample_size > len(prototype_ids):
        raise ValueError(
            f"sample_size {sample_size} exceeds number of prototype IDs ({len(prototype_ids)})"
        )
    gen = rng if rng is not None else random
    return gen.sample(prototype_ids, sample_size)


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
    """Parse path_tariffs_electric from YAML (dict key -> path) and reconcile with map.

    Value must be a dict mapping keys to path strings; keys used for the tariff map
    are derived from filename stem (e.g. tariffs/electric/foo.json -> foo). Every
    tariff_key in the map must have an entry, and every path must appear in the map.
    """
    if not isinstance(value, dict):
        raise ValueError(
            f"path_tariffs_{label} must be a dict of key -> path; got {type(value).__name__}"
        )
    path_tariffs = {}
    for item in value.values():
        if not isinstance(item, str):
            raise ValueError(
                f"path_tariffs_{label} dict values must be path strings; "
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


def _require_value(run: dict[str, Any], field_name: str) -> Any:
    value = run.get(field_name)
    if value is None:
        raise ValueError(f"Missing required field `{field_name}` in run config")
    return value


def _parse_single_revenue_requirement(rr_data: dict[str, Any]) -> float:
    """Extract a scalar revenue_requirement from the YAML mapping."""
    return _parse_float(rr_data["revenue_requirement"], "revenue_requirement")


def _parse_subclass_revenue_requirement(
    rr_data: dict[str, Any],
    raw_path_tariffs_electric: dict[str, Any],
    base_dir: Path,
) -> dict[str, float]:
    """Map subclass revenue requirements to tariff keys.

    Subclass YAML keys ('true'/'false') are mapped via SUBCLASS_TO_ALIAS to
    the raw YAML aliases ('hp'/'non-hp'), then resolved to tariff keys (file
    stems like 'ny_hp_seasonal') using the path strings.
    """
    subclass_rr = rr_data.get("subclass_revenue_requirements")
    if not isinstance(subclass_rr, dict) or not subclass_rr:
        raise ValueError("subclass_revenue_requirements must be a non-empty mapping")

    alias_to_tariff_key = {
        alias: _resolve_path(str(path_str), base_dir).stem
        for alias, path_str in raw_path_tariffs_electric.items()
    }

    result: dict[str, float] = {}
    for group_val, amount in subclass_rr.items():
        alias = SUBCLASS_TO_ALIAS.get(str(group_val))
        if alias is None:
            raise ValueError(
                f"Unknown subclass group value {group_val!r}; "
                f"expected one of {sorted(SUBCLASS_TO_ALIAS)}"
            )
        tariff_key = alias_to_tariff_key.get(alias)
        if tariff_key is None:
            raise ValueError(
                f"Subclass {group_val!r} maps to alias {alias!r} but "
                f"path_tariffs_electric has no alias {alias!r} "
                f"(available: {sorted(alias_to_tariff_key)})"
            )
        result[tariff_key] = _parse_float(
            amount, f"subclass_revenue_requirements[{group_val}]"
        )

    return result


def _parse_utility_revenue_requirement(
    value: Any,
    base_dir: Path,
    raw_path_tariffs_electric: dict[str, Any],
) -> float | dict[str, float]:
    """Parse utility_delivery_revenue_requirement from a YAML path.

    Returns a single float for single-RR YAMLs, or a dict keyed by tariff_key
    for subclass-RR YAMLs.  raw_path_tariffs_electric is the original YAML dict
    (alias -> path string) before alias-to-stem conversion.
    """
    if not isinstance(value, str):
        raise ValueError(
            "utility_delivery_revenue_requirement must be a YAML path string "
            f"(.yaml/.yml), got {type(value).__name__}"
        )
    raw = value.strip()
    if raw == "":
        raise ValueError("Missing required field: utility_delivery_revenue_requirement")
    if not (raw.endswith(".yaml") or raw.endswith(".yml")):
        raise ValueError(
            "utility_delivery_revenue_requirement must be a YAML file path "
            f"(.yaml/.yml), got {value!r}"
        )

    path = _resolve_path(raw, base_dir)
    with path.open(encoding="utf-8") as f:
        rr_data = yaml.safe_load(f)
    if not isinstance(rr_data, dict):
        raise ValueError(
            "Revenue requirement YAML must be a mapping; "
            f"got {type(rr_data).__name__} in {path}"
        )

    if "revenue_requirement" in rr_data:
        return _parse_single_revenue_requirement(rr_data)
    if "subclass_revenue_requirements" in rr_data:
        return _parse_subclass_revenue_requirement(
            rr_data, raw_path_tariffs_electric, base_dir
        )
    raise ValueError(
        f"{path} must contain 'revenue_requirement' or 'subclass_revenue_requirements'."
    )


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


def _resolve_output_dir(
    run: dict[str, Any],
    run_num: int,
    output_dir_override: Path | None,
) -> Path:
    """Determine the CAIRO output root directory.

    When *output_dir_override* is provided (CLI ``--output-dir``), it wins.
    Otherwise ``path_outputs`` from the YAML run dict is required:
    CAIRO creates ``{output_dir}/{timestamp}_{run_name}/``, so we pass
    ``parent(path_outputs)`` as the output root.
    """
    if output_dir_override is not None:
        return output_dir_override
    path_outputs_raw = run.get("path_outputs")
    if not path_outputs_raw:
        raise ValueError(
            f"runs[{run_num}] is missing required key 'path_outputs' "
            "and no --output-dir was provided on the CLI."
        )
    return Path(str(path_outputs_raw)).parent


def _is_diurnal_tou(tariff_path: Path) -> bool:
    """Return True if the URDB tariff has rate variation within any day (diurnal TOU).

    A tariff is diurnal TOU when any row (month) in energyweekdayschedule or
    energyweekendschedule has more than one distinct period value — meaning the
    rate changes within a single day.
    """
    with open(tariff_path) as f:
        data = json.load(f)
    item = data["items"][0]
    for sched_key in ("energyweekdayschedule", "energyweekendschedule"):
        for row in item.get(sched_key, []):
            if len(set(row)) > 1:
                return True
    return False


def _find_tou_derivation_path(tariff_key: str) -> Path | None:
    """Find the TOU derivation JSON for a tariff key, if one exists.

    Convention: strip ``_calibrated`` and ``_flex`` suffixes from the tariff key
    to get the base TOU name, then look for
    ``config/tou_derivation/{base}_derivation.json``.
    """
    base = re.sub(r"_(calibrated|flex)", "", tariff_key)
    base = re.sub(r"__+", "_", base).strip("_")
    candidate = PATH_TOU_DERIVATION / f"{base}_derivation.json"
    return candidate if candidate.exists() else None


def _build_settings_from_yaml_run(
    run: dict[str, Any],
    run_num: int,
    output_dir_override: Path | None,
    run_name_override: str | None,
) -> ScenarioSettings:
    """Build runtime settings from repo YAML scenario config."""
    state = str(run.get("state", "RI")).upper()
    utility = str(_require_value(run, "utility")).lower()
    mode = str(run.get("run_type", "precalc"))
    year_run = _parse_int(_require_value(run, "year_run"), "year_run")
    year_dollar_conversion = _parse_int(
        _require_value(run, "year_dollar_conversion"),
        "year_dollar_conversion",
    )
    process_workers = _parse_int(
        _require_value(run, "process_workers"), "process_workers"
    )
    path_electric_utility_stats = _resolve_path_or_uri(
        str(_require_value(run, "path_electric_utility_stats")),
        PATH_CONFIG,
    )
    solar_pv_compensation = str(_require_value(run, "solar_pv_compensation"))

    path_tariff_maps_electric = _resolve_path(
        str(_require_value(run, "path_tariff_maps_electric")),
        PATH_CONFIG,
    )
    raw_path_tariffs_electric = _require_value(run, "path_tariffs_electric")
    path_tariffs_electric = _parse_path_tariffs(
        raw_path_tariffs_electric,
        path_tariff_maps_electric,
        PATH_CONFIG,
        "electric",
    )
    utility_delivery_revenue_requirement = _parse_utility_revenue_requirement(
        _require_value(run, "utility_delivery_revenue_requirement"),
        PATH_CONFIG,
        raw_path_tariffs_electric,
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

    add_supply_revenue_requirement = _parse_bool(
        _require_value(run, "add_supply_revenue_requirement"),
        "add_supply_revenue_requirement",
    )
    elasticity = _parse_float(run.get("elasticity", 0.0), "elasticity")
    sample_size = (
        _parse_int(run["sample_size"], "sample_size") if "sample_size" in run else None
    )
    path_tou_supply_mc: str | Path | None = None
    if "path_tou_supply_mc" in run:
        path_tou_supply_mc = _resolve_path_or_uri(
            str(run["path_tou_supply_mc"]), PATH_CONFIG
        )
    output_dir = _resolve_output_dir(run, run_num, output_dir_override)
    run_name = run_name_override or run.get("run_name") or f"run_{run_num}"
    return ScenarioSettings(
        run_name=run_name,
        run_type=mode,
        state=state,
        utility=utility,
        path_results=output_dir,
        path_utility_assignment=_resolve_path_or_uri(
            str(_require_value(run, "path_utility_assignment")),
            PATH_CONFIG,
        ),
        path_resstock_metadata=_resolve_path(
            str(_require_value(run, "path_resstock_metadata")),
            PATH_CONFIG,
        ),
        path_resstock_loads=_resolve_path(
            str(_require_value(run, "path_resstock_loads")),
            PATH_CONFIG,
        ),
        path_supply_marginal_costs=_resolve_path_or_uri(
            str(run.get("path_supply_marginal_costs", "")), PATH_CONFIG
        )
        if run.get("path_supply_marginal_costs")
        else None,
        path_supply_energy_mc=_resolve_path_or_uri(
            str(run.get("path_supply_energy_mc", "")), PATH_CONFIG
        )
        if run.get("path_supply_energy_mc")
        else None,
        path_supply_capacity_mc=_resolve_path_or_uri(
            str(run.get("path_supply_capacity_mc", "")), PATH_CONFIG
        )
        if run.get("path_supply_capacity_mc")
        else None,
        path_td_marginal_costs=_resolve_path_or_uri(
            str(_require_value(run, "path_td_marginal_costs")),
            PATH_CONFIG,
        ),
        path_tariff_maps_electric=path_tariff_maps_electric,
        path_tariff_maps_gas=path_tariff_maps_gas,
        path_tariffs_electric=path_tariffs_electric,
        path_tariffs_gas=path_tariffs_gas,
        utility_delivery_revenue_requirement=utility_delivery_revenue_requirement,
        path_electric_utility_stats=path_electric_utility_stats,
        year_run=year_run,
        year_dollar_conversion=year_dollar_conversion,
        process_workers=process_workers,
        solar_pv_compensation=solar_pv_compensation,
        sample_size=sample_size,
        add_supply_revenue_requirement=add_supply_revenue_requirement,
        elasticity=elasticity,
        path_tou_supply_mc=path_tou_supply_mc,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Run NY heat-pump scenario using YAML config.")
    )
    parser.add_argument(
        "--scenario-config",
        type=Path,
        default=None,
        help=("Path to YAML scenario config. Required unless --utility is provided."),
    )
    parser.add_argument(
        "--utility",
        type=str,
        default=None,
        help=(
            "Utility code used to derive scenario config as "
            "config/scenarios/scenarios_<utility>.yaml. "
            "Required unless --scenario-config is provided."
        ),
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
        default=None,
        help=(
            "Output root dir passed to CAIRO. When omitted, uses "
            "parent(path_outputs) from the scenario YAML."
        ),
    )
    parser.add_argument(
        "--run-name",
        help="Optional override for run name.",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=None,
        dest="num_workers",
        help=(
            "Number of Dask process workers. When provided, overrides process_workers "
            "from the scenario YAML. When omitted, uses "
            "min(process_workers, os.cpu_count())."
        ),
    )
    args = parser.parse_args()
    if args.scenario_config is None and args.utility is None:
        parser.error("Provide either --scenario-config or --utility.")
    return args


def _resolve_settings(args: argparse.Namespace) -> ScenarioSettings:
    scenario_config = args.scenario_config
    if scenario_config is None:
        if args.utility is None:
            raise ValueError("Provide either --scenario-config or --utility.")
        scenario_config = _scenario_config_from_utility(args.utility.lower())
    run = _load_run_from_yaml(scenario_config, args.run_num)
    return _build_settings_from_yaml_run(
        run=run,
        run_num=args.run_num,
        output_dir_override=args.output_dir,
        run_name_override=args.run_name,
    )


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------


def _load_prototype_ids_for_run(
    path_utility_assignment: str | Path,
    utility: str,
    sample_size: int | None,
) -> list[int]:
    """Load utility assignment, fetch prototype IDs for the utility, optionally sample."""

    path_ua = path_utility_assignment
    storage_opts = (
        STORAGE_OPTIONS
        if isinstance(path_ua, str) and path_ua.startswith("s3://")
        else None
    )
    utility_assignment = pl.scan_parquet(str(path_ua), storage_options=storage_opts)
    prototype_ids = _fetch_prototype_ids_by_electric_util(
        cast(ElectricUtility, utility), utility_assignment
    )
    log.info(
        ".... Found %s bldgs for utility %s",
        len(prototype_ids),
        utility,
    )
    if sample_size is not None:
        rng = random.Random(42)
        try:
            prototype_ids = apply_prototype_sample(prototype_ids, sample_size, rng=rng)
        except ValueError as e:
            log.warning("%s; exiting.", e)
            sys.exit(1)
        log.info(
            ".... Selected a sample of %s of these for simulation (sample_size=%s)",
            len(prototype_ids),
            sample_size,
        )
    return prototype_ids


def _build_precalc_period_mapping(
    path_tariffs_electric: dict[str, Path],
) -> pd.DataFrame:
    """Build precalc mapping by concatenating all configured electric tariffs."""
    if not path_tariffs_electric:
        raise ValueError("path_tariffs_electric must contain at least one tariff")
    precalc_parts = [
        generate_default_precalc_mapping(tariff_path=tariff_path, tariff_key=tariff_key)
        for tariff_key, tariff_path in path_tariffs_electric.items()
    ]
    return pd.concat(precalc_parts, ignore_index=True)


def _recompute_tou_precalc_mapping(
    precalc_mapping: pd.DataFrame,
    shifted_system_load_raw: pd.Series,
    bulk_marginal_costs: pd.DataFrame,
    distribution_marginal_costs: pd.Series,
    tou_season_specs: dict[str, list[SeasonTouSpec]],
) -> pd.DataFrame:
    """Recompute precalc rel_values for TOU tariffs using shifted-load MC weights.

    After demand flex shifts load from peak to off-peak, the demand-weighted
    marginal cost profile changes.  This function recomputes the per-season
    peak/off-peak cost-causation ratios and seasonal base rates from the
    *shifted* system load, then updates the precalc_mapping rel_values so
    that CAIRO calibrates rate ratios that reflect post-flex MC responsibility.

    Non-TOU tariff entries in the mapping are left unchanged.

    Args:
        shifted_system_load_raw: Pre-aggregated system-level hourly demand
            (weight × kWh summed across buildings, indexed by time).
    """
    # Combine bulk + distribution MCs into a single $/kWh series.
    combined_mc_raw = combine_marginal_costs(
        bulk_marginal_costs, distribution_marginal_costs
    )

    # Align indices: MC and load may have different timezone representations.
    # Reindex load onto MC's DatetimeIndex so pandas operations align.
    mc_index = pd.DatetimeIndex(combined_mc_raw.index)
    combined_mc = pd.Series(
        combined_mc_raw.values, index=mc_index, name="total_mc_per_kwh"
    )
    shifted_system_load = pd.Series(
        shifted_system_load_raw.values[: len(mc_index)],
        index=mc_index,
        name="system_load",
    )

    updated = precalc_mapping.copy()

    for tou_key, specs in tou_season_specs.items():
        # Use original base_rate as nominal level; precalc calibrates the
        # absolute level, so only the *ratios* between periods matter.
        avg_base_rate = sum(spec.base_rate for spec in specs) / len(specs)

        # Recompute per-season demand-weighted MC and peak/off-peak ratios.
        # If a season has zero MC (e.g. delivery-only run with no dist MC in
        # winter), keep the original ratio/base_rate from the derivation spec.
        new_season_rates: dict[str, float] = {}
        new_ratios: dict[str, float] = {}

        for spec in specs:
            mask = season_mask(mc_index, spec.season)
            mc_season = combined_mc[mask]
            load_season = shifted_system_load[mask]

            season_mc_total = float((mc_season * load_season).sum())
            if abs(season_mc_total) < 1e-12:
                # Zero MC in this season → no cost-causation signal.
                # Use flat ratio (1.0) and unit base rate so precalc
                # treats peak and off-peak identically for this season.
                log.info(
                    ".... Season %s has zero MC; using flat ratio 1.0",
                    spec.season.name,
                )
                new_season_rates[spec.season.name] = 1.0
                new_ratios[spec.season.name] = 1.0
            else:
                total_load = float(load_season.sum())
                dw_avg = season_mc_total / total_load if total_load > 0 else 0.0
                new_season_rates[spec.season.name] = dw_avg
                new_ratios[spec.season.name] = compute_tou_cost_causation_ratio(
                    mc_season,
                    load_season,
                    spec.peak_hours,
                )

        # Scale seasonal rates so their load-weighted average equals
        # avg_base_rate (preserves inter-season ratios from MC weights).
        total_load_all = float(shifted_system_load.sum())
        raw_weighted = sum(
            new_season_rates[spec.season.name]
            * float(shifted_system_load[season_mask(mc_index, spec.season)].sum())
            for spec in specs
        )
        scale = (
            avg_base_rate * total_load_all / raw_weighted if raw_weighted != 0 else 1.0
        )
        new_season_rates = {k: v * scale for k, v in new_season_rates.items()}

        # Build new per-period rates.  Period order matches
        # create_seasonal_tou_tariff: [offpeak_s1, peak_s1, offpeak_s2, peak_s2, …]
        new_rates: list[float] = []
        for spec in specs:
            offpeak = new_season_rates[spec.season.name]
            peak = offpeak * new_ratios[spec.season.name]
            new_rates.append(offpeak)
            new_rates.append(peak)

        min_rate = min(new_rates) if new_rates else 1.0

        # Update rel_values for this tariff (1-based period indexing).
        tariff_mask = updated["tariff"] == tou_key
        for period_idx, rate in enumerate(new_rates, start=1):
            period_mask = tariff_mask & (updated["period"] == period_idx)
            updated.loc[period_mask, "rel_value"] = round(rate / min_rate, 4)

        log.info(
            ".... Recomputed TOU precalc mapping for %s: "
            "ratios=%s, rates=%s, rel_values=%s",
            tou_key,
            {k: round(v, 4) for k, v in new_ratios.items()},
            [round(r, 6) for r in new_rates],
            [round(r / min_rate, 4) for r in new_rates],
        )

    return updated


def run(settings: ScenarioSettings, num_workers: int | None = None) -> None:
    log.info(
        ".... Beginning RI residential (non-LMI) rate scenario simulation: %s",
        settings.run_name,
    )

    _effective_workers = (
        num_workers
        if num_workers is not None
        else min(settings.process_workers, os.cpu_count() or 1)
    )
    dask.config.set(scheduler="processes", num_workers=_effective_workers)
    log.info(
        "TIMING workers: %d (yaml=%d, cpu_count=%d)",
        _effective_workers,
        settings.process_workers,
        os.cpu_count() or 1,
    )

    # Phase 1 ---------------------------------------------------------------
    t0 = time.perf_counter()
    prototype_ids = _load_prototype_ids_for_run(
        settings.path_utility_assignment,
        settings.utility,
        settings.sample_size,
    )
    log.info("TIMING _load_prototype_ids_for_run: %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    tariffs_params, tariff_map_df = _initialize_tariffs(
        tariff_map=settings.path_tariff_maps_electric,
        building_stock_sample=prototype_ids,
        tariff_paths=settings.path_tariffs_electric,
    )
    log.info("TIMING _initialize_tariffs: %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    precalc_mapping = _build_precalc_period_mapping(settings.path_tariffs_electric)
    log.info("TIMING _build_precalc_period_mapping: %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    customer_count = get_residential_customer_count_from_utility_stats(
        settings.path_electric_utility_stats,
        settings.utility,
        storage_options=STORAGE_OPTIONS,
    )
    customer_metadata = return_buildingstock(
        load_scenario=settings.path_resstock_metadata,
        building_stock_sample=prototype_ids,
        customer_count=customer_count,
        columns=[
            "applicability",
            "postprocess_group.has_hp",
            "postprocess_group.heating_type",
            "in.vintage_acs",
        ],
    )
    log.info("TIMING return_buildingstock: %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    bldg_id_to_load_filepath = build_bldg_id_to_load_filepath(
        path_resstock_loads=settings.path_resstock_loads,
        building_ids=prototype_ids,
    )
    log.info("TIMING build_bldg_id_to_load_filepath: %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    raw_load_elec, raw_load_gas = _return_loads_combined(
        target_year=settings.year_run,
        building_ids=prototype_ids,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )
    log.info("TIMING _return_loads_combined: %.1fs", time.perf_counter() - t0)

    # Phase 2 ---------------------------------------------------------------
    t0 = time.perf_counter()
    # ------------------------------------------------------------------
    # Load marginal costs (needed before demand-flex and revenue calc)
    # ------------------------------------------------------------------
    # MC prices are exogenous (Cambium + distribution); load shifting
    # changes total MC dollars, not the prices themselves.

    # Load supply MCs: use separate energy/capacity files if provided, else fallback to combined
    if settings.path_supply_energy_mc and settings.path_supply_capacity_mc:
        bulk_marginal_costs = _load_supply_marginal_costs(
            settings.path_supply_energy_mc,
            settings.path_supply_capacity_mc,
            settings.year_run,
        )
    elif settings.path_supply_marginal_costs:
        bulk_marginal_costs = _load_cambium_marginal_costs(
            settings.path_supply_marginal_costs,
            settings.year_run,
        )
    else:
        raise ValueError(
            "Must provide either path_supply_marginal_costs (combined) or "
            "both path_supply_energy_mc and path_supply_capacity_mc (separate)"
        )
    distribution_marginal_costs = load_distribution_marginal_costs(
        settings.path_td_marginal_costs,
    )
    if not bulk_marginal_costs.index.equals(distribution_marginal_costs.index):
        if len(bulk_marginal_costs) == len(distribution_marginal_costs):
            # T&D MC parquet can carry a different in-file year (e.g. 2025) than
            # run year (e.g. 2026). Align by hour position onto bulk MC index so
            # CAIRO system-cost merge does not collapse to empty.
            distribution_marginal_costs = pd.Series(
                distribution_marginal_costs.values,
                index=bulk_marginal_costs.index,
                name=distribution_marginal_costs.name,
            )
            log.info(
                ".... Aligned distribution MC index to bulk MC index "
                "(bulk_year=%s, dist_year=%s)",
                bulk_marginal_costs.index[0].year,
                distribution_marginal_costs.index[0].year,
            )
        else:
            distribution_marginal_costs = distribution_marginal_costs.reindex(
                bulk_marginal_costs.index
            )
            log.info(
                ".... Reindexed distribution MC to bulk MC index "
                "(bulk_rows=%s, dist_rows=%s)",
                len(bulk_marginal_costs),
                len(distribution_marginal_costs),
            )
    log.info(
        ".... Loaded distribution marginal costs rows=%s",
        len(distribution_marginal_costs),
    )
    sell_rate = _return_export_compensation_rate(
        year_run=settings.year_run,
        solar_pv_compensation=settings.solar_pv_compensation,
        solar_pv_export_import_ratio=1.0,
        tariff_dict=tariffs_params,
    )

    # Decompose subclass revenue requirement into total + ratios so that
    # both demand-flex and non-flex paths can work with a scalar target
    # for CAIRO's _return_revenue_requirement_target, then re-split after.
    rr_setting = settings.utility_delivery_revenue_requirement
    if isinstance(rr_setting, dict):
        rr_total = sum(rr_setting.values())
        rr_ratios: dict[str, float] | None = {
            k: v / rr_total for k, v in rr_setting.items()
        }
    else:
        rr_total = rr_setting
        rr_ratios = None

    # ------------------------------------------------------------------
    # Demand-flex two-pass recalibration (Phase 1a → 1.5 → 2)
    # ------------------------------------------------------------------
    # When elasticity != 0, demand flex is enabled.  TOU tariffs are
    # auto-detected by checking which tariff JSONs have within-day rate
    # variation (diurnal TOU).  Demand response is applied separately
    # per TOU tariff to its mapped building IDs.
    # See context/tools/cairo_demand_flexibility_workflow.md for details.
    demand_flex_enabled = settings.elasticity != 0.0
    effective_load_elec = raw_load_elec
    elasticity_tracker = pd.DataFrame()

    if demand_flex_enabled:
        # Identify which tariffs are diurnal TOU
        tou_tariff_keys = [
            key
            for key, path in settings.path_tariffs_electric.items()
            if _is_diurnal_tou(path)
        ]
        if not tou_tariff_keys:
            raise ValueError(
                f"elasticity={settings.elasticity} (demand flex enabled) but no "
                f"diurnal TOU tariffs found in path_tariffs_electric "
                f"keys={sorted(settings.path_tariffs_electric)}"
            )
        log.info(
            ".... Demand flex enabled (elasticity=%.4f); detected %d TOU tariff(s): %s",
            settings.elasticity,
            len(tou_tariff_keys),
            tou_tariff_keys,
        )

        if (
            "bldg_id" not in tariff_map_df.columns
            or "tariff_key" not in tariff_map_df.columns
        ):
            raise ValueError(
                "Electric tariff map must include 'bldg_id' and 'tariff_key' columns"
            )

        # -- Phase 1a: freeze residual from original loads --
        # frozen_residual = full_RR_orig - MC_orig (embedded costs,
        # invariant to short-run demand shifting).
        log.info(".... Phase 1a: computing frozen residual from original loads")
        (
            full_rr_orig,
            _msp_orig,
            _msc_orig,
            costs_by_type_orig,
        ) = _return_revenue_requirement_target(
            building_load=raw_load_elec,
            sample_weight=customer_metadata[["bldg_id", "weight"]],
            revenue_requirement_target=rr_total,
            residual_cost=None,
            residual_cost_frac=None,
            bulk_marginal_costs=bulk_marginal_costs,
            distribution_marginal_costs=distribution_marginal_costs,
            low_income_strategy=None,
            delivery_only_rev_req_passed=settings.add_supply_revenue_requirement,
        )
        total_mc_orig = float(costs_by_type_orig["Total Marginal Costs ($)"])
        frozen_residual: float = float(full_rr_orig) - total_mc_orig
        log.info(
            ".... Frozen residual from original loads: $%.2f "
            "(full_RR_orig=$%.2f, MC_original=$%.2f)",
            frozen_residual,
            float(full_rr_orig),
            total_mc_orig,
        )

        # -- Phase 1.5: shift TOU customers (per TOU tariff) --
        tou_season_specs: dict[str, list[SeasonTouSpec]] = {}
        for tou_key in tou_tariff_keys:
            tou_tariff_path = settings.path_tariffs_electric[tou_key]
            with open(tou_tariff_path) as f:
                tou_tariff = json.load(f)

            tou_rows = tariff_map_df[tariff_map_df["tariff_key"] == tou_key]
            tou_bldg_ids = cast(list[int], tou_rows["bldg_id"].astype(int).tolist())

            # Look for a derivation spec for season-aware shifting
            derivation_path = _find_tou_derivation_path(tou_key)
            season_specs = None
            if derivation_path is not None:
                season_specs = load_season_specs(derivation_path)
                tou_season_specs[tou_key] = season_specs
                log.info(".... Using TOU derivation spec: %s", derivation_path.name)

            log.info(
                ".... Phase 1.5: applying demand response to %d bldgs on tariff %s",
                len(tou_bldg_ids),
                tou_key,
            )
            effective_load_elec, tracker = apply_runtime_tou_demand_response(
                raw_load_elec=effective_load_elec,
                tou_bldg_ids=tou_bldg_ids,
                tou_tariff=tou_tariff,
                demand_elasticity=settings.elasticity,
                season_specs=season_specs,
            )
            if elasticity_tracker.empty:
                elasticity_tracker = tracker
            else:
                elasticity_tracker = pd.concat([elasticity_tracker, tracker], axis=0)

        # -- Phase 1.75: recompute TOU cost-causation ratios from shifted load --
        # The load shift changes the demand-weighted MC profile, so the
        # peak/off-peak cost-causation ratios must be updated before CAIRO
        # calibrates.  This ensures the tariff rate *structure* (not just the
        # level) reflects the post-flex MC responsibility.
        #
        # Only for precalc runs: default runs use a pre-calibrated tariff
        # and have no revenue-neutrality constraint to recalibrate.
        #
        # For delivery-only precalc runs the scenario's Cambium file is
        # zeros; the *real* supply MC for cost-causation is provided via
        # the optional ``path_tou_supply_mc`` field.
        if tou_season_specs and settings.run_type == "precalc":
            # Load the real supply MC for cost-causation ratio computation.
            if settings.path_tou_supply_mc is not None:
                # Use the same loader as main supply MC loading.
                # _load_cambium_marginal_costs handles both Cambium files and other formats.
                tou_bulk_mc = _load_cambium_marginal_costs(
                    settings.path_tou_supply_mc, settings.year_run
                )
                log.info(
                    ".... Phase 1.75: using real supply MC from %s",
                    settings.path_tou_supply_mc,
                )
            else:
                tou_bulk_mc = bulk_marginal_costs
                log.info(
                    ".... Phase 1.75: using scenario bulk MC (no path_tou_supply_mc)"
                )

            log.info(
                ".... Phase 1.75: recomputing TOU precalc mapping from shifted load"
            )
            # Aggregate shifted building loads to system-level hourly demand
            # once, rather than passing raw building loads into
            # _recompute_tou_precalc_mapping (which would copy + merge again).
            sample_weights = customer_metadata[["bldg_id", "weight"]]
            shifted_weighted = effective_load_elec.reset_index().merge(
                sample_weights, on="bldg_id"
            )
            shifted_weighted["electricity_net"] *= shifted_weighted["weight"]
            shifted_system_load = shifted_weighted.groupby("time")[
                "electricity_net"
            ].sum()

            precalc_mapping = _recompute_tou_precalc_mapping(
                precalc_mapping=precalc_mapping,
                shifted_system_load_raw=shifted_system_load,
                bulk_marginal_costs=tou_bulk_mc,
                distribution_marginal_costs=distribution_marginal_costs,
                tou_season_specs=tou_season_specs,
            )

        # -- Phase 2: new_RR = MC_shifted + frozen_residual --
        # Supply top-up is already in frozen_residual; no delivery_only
        # adjustment needed here.
        log.info(".... Phase 2: recomputing RR with shifted loads + frozen residual")
        (
            _rr_shifted,
            marginal_system_prices,
            marginal_system_costs,
            costs_by_type,
        ) = _return_revenue_requirement_target(
            building_load=effective_load_elec,
            sample_weight=customer_metadata[["bldg_id", "weight"]],
            revenue_requirement_target=None,
            residual_cost=frozen_residual,
            residual_cost_frac=None,
            bulk_marginal_costs=bulk_marginal_costs,
            distribution_marginal_costs=distribution_marginal_costs,
            low_income_strategy=None,
            delivery_only_rev_req_passed=False,
        )
        revenue_requirement_raw = float(costs_by_type["Total System Costs ($)"])
        log.info(
            ".... Recalibrated RR=$%.2f  (MC_shifted=$%.2f + frozen_residual=$%.2f)",
            revenue_requirement_raw,
            float(costs_by_type["Total Marginal Costs ($)"]),
            frozen_residual,
        )

    else:
        # -- No demand flex: single-pass revenue requirement (unchanged) --
        (
            revenue_requirement_raw,
            marginal_system_prices,
            marginal_system_costs,
            costs_by_type,
        ) = _return_revenue_requirement_target(
            building_load=raw_load_elec,
            sample_weight=customer_metadata[["bldg_id", "weight"]],
            revenue_requirement_target=rr_total,
            residual_cost=None,
            residual_cost_frac=None,
            bulk_marginal_costs=bulk_marginal_costs,
            distribution_marginal_costs=distribution_marginal_costs,
            low_income_strategy=None,
            delivery_only_rev_req_passed=settings.add_supply_revenue_requirement,
        )

    # Apply subclass RR split if configured.
    # Compute per-subclass supply MC so each subclass's RR reflects
    # its own marginal cost responsibility.  Without this, the delivery
    # ratios would be applied to the topped-up total, mis-allocating
    # supply MC between subclasses with different load profiles.
    if rr_ratios is not None and isinstance(rr_setting, dict):
        subclass_supply_mc: dict[str, float] = {}
        if settings.add_supply_revenue_requirement:
            supply_cols = [
                c
                for c in bulk_marginal_costs.columns
                if "Energy" in c or "Capacity" in c
            ]
            supply_mc_prices = bulk_marginal_costs[supply_cols].sum(axis=1)

            # Pre-merge weights into load once to avoid redundant
            # copy + merge inside process_residential_hourly_demand
            # per subclass (N_buildings × 8760 rows each time).
            weighted_load = raw_load_elec.reset_index().merge(
                customer_metadata[["bldg_id", "weight"]], on="bldg_id"
            )
            weighted_load["weighted_kwh"] = (
                weighted_load["electricity_net"] * weighted_load["weight"]
            )

            for tariff_key in rr_setting:
                subclass_bldg_ids = set(
                    tariff_map_df.loc[
                        tariff_map_df["tariff_key"] == tariff_key, "bldg_id"
                    ]
                )
                sub = weighted_load[weighted_load["bldg_id"].isin(subclass_bldg_ids)]
                subclass_sys_load = sub.groupby("time")["weighted_kwh"].sum()
                subclass_supply_mc[tariff_key] = float(
                    supply_mc_prices.mul(subclass_sys_load).sum()
                )
            log.info(
                ".... Per-subclass supply MC: %s",
                {k: f"${v:,.0f}" for k, v in subclass_supply_mc.items()},
            )

        # Each subclass's baseline = delivery_RR_k + own supply MC
        subclass_baseline: dict[str, float] = {
            k: v + subclass_supply_mc.get(k, 0.0) for k, v in rr_setting.items()
        }

        if demand_flex_enabled:
            # Only TOU subclasses shifted load → only they absorb the RR
            # change.  Non-shifted subclasses keep their baseline.
            non_shifted_rr = sum(
                subclass_baseline[k] for k in rr_setting if k not in tou_tariff_keys
            )
            shifted_rr_total = revenue_requirement_raw - non_shifted_rr
            # Split among TOU subclasses proportionally (if more than one).
            tou_baseline_total = sum(
                subclass_baseline[k] for k in rr_setting if k in tou_tariff_keys
            )
            revenue_requirement: float | dict[str, float] = {}
            for k in rr_setting:
                if k in tou_tariff_keys:
                    revenue_requirement[k] = (
                        shifted_rr_total * (subclass_baseline[k] / tou_baseline_total)
                        if tou_baseline_total > 0
                        else shifted_rr_total
                    )
                else:
                    revenue_requirement[k] = subclass_baseline[k]
            log.info(
                ".... Subclass RR (demand-flex): %s",
                {k: f"${v:,.0f}" for k, v in revenue_requirement.items()},
            )
        else:
            revenue_requirement = subclass_baseline
    else:
        revenue_requirement = revenue_requirement_raw
    log.info("TIMING phase2_marginal_costs_rr: %.1fs", time.perf_counter() - t0)

    # Phase 3 ---------------------------------------------------------------
    t0 = time.perf_counter()
    # ------------------------------------------------------------------
    # Phase 3: Run CAIRO simulation
    # ------------------------------------------------------------------
    # Precalc calibrates rates against shifted loads so the resulting
    # tariff recovers the (lower) RR from the demand-flex load profile.

    bs = MeetRevenueSufficiencySystemWide(
        run_type=settings.run_type,
        year_run=settings.year_run,
        year_dollar_conversion=settings.year_dollar_conversion,
        process_workers=settings.process_workers,
        building_stock_sample=prototype_ids,
        run_name=settings.run_name,
        output_dir=settings.path_results,
    )
    bs.simulate(
        revenue_requirement=revenue_requirement,
        tariffs_params=tariffs_params,
        tariff_map=tariff_map_df,
        precalc_period_mapping=precalc_mapping,
        customer_metadata=customer_metadata,
        customer_electricity_load=effective_load_elec,
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
    log.info("TIMING bs.simulate: %.1fs", time.perf_counter() - t0)

    save_file_loc = getattr(bs, "save_file_loc", None)
    if save_file_loc is not None:
        distribution_mc_path = Path(save_file_loc) / "distribution_marginal_costs.csv"
        distribution_marginal_costs.to_csv(distribution_mc_path, index=True)
        log.info(".... Saved distribution marginal costs: %s", distribution_mc_path)
        if demand_flex_enabled:
            tracker_path = Path(save_file_loc) / "demand_flex_elasticity_tracker.csv"
            elasticity_tracker.to_csv(tracker_path, index=True)
            log.info(".... Saved demand-flex elasticity tracker: %s", tracker_path)

    log.info(".... Completed RI residential (non-LMI) rate scenario simulation")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    args = _parse_args()
    settings = _resolve_settings(args)
    run(settings, num_workers=args.num_workers)


if __name__ == "__main__":
    main()
