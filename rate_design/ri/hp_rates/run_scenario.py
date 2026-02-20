"""Entrypoint for running RI heat pump rate scenarios - Residential (non-LMI)."""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pandas as pd
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

from utils import get_aws_region
from utils.cairo import (
    _fetch_prototype_ids_by_electric_util,
    _load_cambium_marginal_costs,
    apply_runtime_tou_demand_response,
    build_bldg_id_to_load_filepath,
    load_distribution_marginal_costs,
)
from utils.pre.compute_tou import load_season_specs
from utils.pre.generate_precalc_mapping import generate_default_precalc_mapping
from utils.types import ElectricUtility

log = logging.getLogger("rates_analysis").getChild("tests")

STORAGE_OPTIONS = {"aws_region": get_aws_region()}
# Resolve paths relative to this script so the scenario can be run from any CWD.
PATH_PROJECT = Path(__file__).resolve().parent
PATH_CONFIG = PATH_PROJECT / "config"
PATH_SCENARIOS = PATH_CONFIG / "scenarios"
DEFAULT_OUTPUT_DIR = Path("/data.sb/switchbox/cairo/ri_hp_rates/analysis_outputs")


def _scenario_config_from_utility(utility: str) -> Path:
    return PATH_SCENARIOS / f"scenarios_{utility}.yaml"


@dataclass(slots=True)
class DemandFlexConfig:
    """Optional runtime demand-flexibility configuration."""

    enabled: bool = False
    demand_elasticity: float = -0.1
    tou_tariff_key: str = ""
    tou_derivation_path: Path = field(default_factory=Path)


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
    path_utility_assignment: str | Path
    path_cambium_marginal_costs: str | Path
    path_td_marginal_costs: str | Path
    path_tariff_maps_electric: Path
    path_tariff_maps_gas: Path
    path_tariffs_electric: dict[str, Path]
    path_tariffs_gas: dict[str, Path]
    precalc_tariff_path: Path
    precalc_tariff_key: str
    utility_delivery_revenue_requirement: float
    path_electric_utility_stats: str | Path
    year_run: int
    year_dollar_conversion: int
    process_workers: int
    solar_pv_compensation: str = "net_metering"
    add_supply_revenue_requirement: bool = False
    sample_size: int | None = None
    demand_flex: DemandFlexConfig = field(default_factory=DemandFlexConfig)


def get_residential_customer_count_from_utility_stats(
    path: str | Path,
    utility: str,
    *,
    storage_options: dict[str, str] | None = None,
) -> int:
    """Read EIA-861 utility stats parquet and return residential customer count for the utility.

    The parquet is state-partitioned (e.g. state=NY/data.parquet) with columns
    utility_code and residential_customers. Filters for utility_code == utility
    (the YAML utility field, e.g. 'coned', 'rie') and returns the single row's
    residential_customers value.

    Raises:
        ValueError: If path has no row for that utility, or more than one row.
    """
    path_str = str(path)
    opts = storage_options if path_str.startswith("s3://") else None
    lf = (
        pl.scan_parquet(path_str, storage_options=opts)
        .filter(pl.col("utility_code") == utility)
        .select("residential_customers")
    )
    df = cast(pl.DataFrame, lf.collect())
    if df.height == 0:
        raise ValueError(
            f"No row with utility_code={utility!r} in {path_str}. "
            "Check path_electric_utility_stats and utility in the scenario YAML."
        )
    if df.height > 1:
        raise ValueError(
            f"Expected one row for utility_code={utility!r} in {path_str}, got {df.height}"
        )
    value = df.item(0, 0)
    if value is None:
        raise ValueError(
            f"residential_customers is null for utility_code={utility!r} in {path_str}"
        )
    return int(value)


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
        f"Invalid boolean for {field_name}: {value!r}. "
        "Use unquoted YAML true/false."
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


def _parse_utility_revenue_requirement(value: Any, base_dir: Path) -> float:
    """Parse utility_delivery_revenue_requirement from a YAML path only."""
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
        return _parse_float(rr_data["revenue_requirement"], "revenue_requirement")
    subclass_rr = rr_data.get("subclass_revenue_requirements")
    if isinstance(subclass_rr, dict):
        if not subclass_rr:
            raise ValueError(
                f"subclass_revenue_requirements is empty in {path}; "
                "cannot derive utility delivery revenue requirement."
            )
        return float(
            sum(
                _parse_float(v, "subclass_revenue_requirements value")
                for v in subclass_rr.values()
            )
        )
    raise ValueError(
        f"{path} must contain 'revenue_requirement' or "
        "'subclass_revenue_requirements'."
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


def _parse_demand_flex_config(run: dict[str, Any]) -> DemandFlexConfig:
    raw = run.get("demand_flex")
    if raw is None or not isinstance(raw, dict):
        return DemandFlexConfig()
    derivation_raw = raw.get("tou_derivation_path", "")
    derivation_path = (
        _resolve_path(str(derivation_raw), PATH_CONFIG) if derivation_raw else Path()
    )
    return DemandFlexConfig(
        enabled=_parse_bool(raw.get("enabled", False), "demand_flex.enabled"),
        demand_elasticity=float(raw.get("demand_elasticity", -0.1)),
        tou_tariff_key=str(raw.get("tou_tariff_key", "")),
        tou_derivation_path=derivation_path,
    )


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
    year_run = _parse_int(_require_value(run, "year_run"), "year_run")
    year_dollar_conversion = _parse_int(
        _require_value(run, "year_dollar_conversion"),
        "year_dollar_conversion",
    )
    process_workers = _parse_int(
        _require_value(run, "process_workers"), "process_workers"
    )
    utility_delivery_revenue_requirement = _parse_utility_revenue_requirement(
        _require_value(run, "utility_delivery_revenue_requirement"),
        PATH_CONFIG,
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

    add_supply_revenue_requirement = _parse_bool(
        _require_value(run, "add_supply_revenue_requirement"),
        "add_supply_revenue_requirement",
    )
    demand_flex = _parse_demand_flex_config(run)
    sample_size = (
        _parse_int(run["sample_size"], "sample_size") if "sample_size" in run else None
    )
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
        path_electric_utility_stats=path_electric_utility_stats,
        year_run=year_run,
        year_dollar_conversion=year_dollar_conversion,
        process_workers=process_workers,
        solar_pv_compensation=solar_pv_compensation,
        sample_size=sample_size,
        add_supply_revenue_requirement=add_supply_revenue_requirement,
        demand_flex=demand_flex,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Run RI heat-pump scenario using YAML config.")
    )
    parser.add_argument(
        "--scenario-config",
        type=Path,
        default=None,
        help=(
            "Path to YAML scenario config. "
            "Required unless --utility is provided."
        ),
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
        default=DEFAULT_OUTPUT_DIR,
        help="Output root dir passed to CAIRO.",
    )
    parser.add_argument(
        "--run-name",
        help="Optional override for run name.",
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
        output_dir=args.output_dir,
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
    if sample_size is None:
        log.info("No sample size provided. Run with all buildings")
        return None
        
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
        try:
            prototype_ids = apply_prototype_sample(prototype_ids, sample_size)
        except ValueError as e:
            log.warning("%s; exiting.", e)
            sys.exit(1)
        log.info(
            ".... Selected a sample of %s of these for simulation (sample_size=%s)",
            len(prototype_ids),
            sample_size,
        )
    return prototype_ids


def run(settings: ScenarioSettings) -> None:
    log.info(
        ".... Beginning RI residential (non-LMI) rate scenario simulation: %s",
        settings.run_name,
    )

    # ------------------------------------------------------------------
    # Phase 1: Retrieve prototype IDs, tariffs, customer metadata, loads
    # ------------------------------------------------------------------

    prototype_ids = _load_prototype_ids_for_run(
        settings.path_utility_assignment,
        settings.utility,
        settings.sample_size,
    )

    tariffs_params, tariff_map_df = _initialize_tariffs(
        tariff_map=settings.path_tariff_maps_electric,
        building_stock_sample=prototype_ids,
        tariff_paths=settings.path_tariffs_electric,
    )

    precalc_mapping = generate_default_precalc_mapping(
        tariff_path=settings.precalc_tariff_path,
        tariff_key=settings.precalc_tariff_key,
    )

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

    bldg_id_to_load_filepath = build_bldg_id_to_load_filepath(
        path_resstock_loads=settings.path_resstock_loads,
        building_ids=prototype_ids,
    )

    raw_load_elec = _return_load(
        load_type="electricity",
        target_year=settings.year_run,
        building_stock_sample=prototype_ids,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )
    raw_load_gas = _return_load(
        load_type="gas",
        target_year=settings.year_run,
        building_stock_sample=prototype_ids,
        load_filepath_key=bldg_id_to_load_filepath,
        force_tz="EST",
    )

    # ------------------------------------------------------------------
    # Load marginal costs (needed before demand-flex and revenue calc)
    # ------------------------------------------------------------------
    # MC prices are exogenous (Cambium + distribution); load shifting
    # changes total MC dollars, not the prices themselves.

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

    sell_rate = _return_export_compensation_rate(
        year_run=settings.year_run,
        solar_pv_compensation=settings.solar_pv_compensation,
        solar_pv_export_import_ratio=1.0,
        tariff_dict=tariffs_params,
    )

    # ------------------------------------------------------------------
    # Demand-flex two-pass recalibration (Phase 1a → 1.5 → 2)
    # ------------------------------------------------------------------
    # When demand_flex is enabled:
    #   1a  — freeze residual from original loads (full_RR - MC_orig)
    #   1.5 — shift TOU-assigned bldg_ids only
    #   2   — recompute RR = MC_shifted + frozen_residual
    # See context/tools/cairo_demand_flexibility_workflow.md for details.
    effective_load_elec = raw_load_elec
    elasticity_tracker = pd.DataFrame()
    if settings.demand_flex.enabled:
        flex = settings.demand_flex
        if not flex.tou_tariff_key:
            raise ValueError(
                "demand_flex.enabled is true but demand_flex.tou_tariff_key is empty"
            )
        tou_tariff_path = settings.path_tariffs_electric.get(flex.tou_tariff_key)
        if tou_tariff_path is None:
            raise ValueError(
                f"demand_flex.tou_tariff_key={flex.tou_tariff_key!r} not found in "
                f"path_tariffs_electric keys={sorted(settings.path_tariffs_electric)}"
            )
        with open(tou_tariff_path) as f:
            tou_tariff = json.load(f)

        if (
            "bldg_id" not in tariff_map_df.columns
            or "tariff_key" not in tariff_map_df.columns
        ):
            raise ValueError(
                "Electric tariff map must include 'bldg_id' and 'tariff_key' columns"
            )
        tou_rows = tariff_map_df[tariff_map_df["tariff_key"] == flex.tou_tariff_key]
        tou_bldg_ids = cast(list[int], tou_rows["bldg_id"].astype(int).tolist())
        season_specs = None
        if flex.tou_derivation_path:
            if flex.tou_derivation_path.exists():
                season_specs = load_season_specs(flex.tou_derivation_path)
            else:
                log.warning(
                    ".... demand_flex.tou_derivation_path not found (%s); "
                    "falling back to tariff-inferred seasonal grouping",
                    flex.tou_derivation_path,
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
            revenue_requirement_target=settings.utility_delivery_revenue_requirement,
            residual_cost=None,
            residual_cost_frac=None,
            bulk_marginal_costs=bulk_marginal_costs,
            distribution_marginal_costs=distribution_marginal_costs,
            low_income_strategy=None,
            delivery_only_rev_req_passed=settings.add_supply_revenue_requirement,
        )
        # Use the full topped-up RR (not costs_by_type's delivery-only
        # residual) so the supply top-up is baked in for Phase 2.
        total_mc_orig = float(costs_by_type_orig["Total Marginal Costs ($)"])
        frozen_residual: float = float(full_rr_orig) - total_mc_orig
        log.info(
            ".... Frozen residual from original loads: $%.2f "
            "(full_RR_orig=$%.2f, MC_original=$%.2f)",
            frozen_residual,
            float(full_rr_orig),
            total_mc_orig,
        )

        # -- Phase 1.5: shift TOU customers --
        log.info(
            ".... Phase 1.5: applying runtime demand response to %d TOU customers "
            "using tariff %s",
            len(tou_bldg_ids),
            flex.tou_tariff_key,
        )
        effective_load_elec, elasticity_tracker = apply_runtime_tou_demand_response(
            raw_load_elec=raw_load_elec,
            tou_bldg_ids=tou_bldg_ids,
            tou_tariff=tou_tariff,
            demand_elasticity=flex.demand_elasticity,
            season_specs=season_specs,
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
        revenue_requirement = float(costs_by_type["Total System Costs ($)"])
        log.info(
            ".... Recalibrated RR=$%.2f  (MC_shifted=$%.2f + frozen_residual=$%.2f)",
            revenue_requirement,
            float(costs_by_type["Total Marginal Costs ($)"]),
            frozen_residual,
        )

    else:
        # -- No demand flex: single-pass revenue requirement (unchanged) --
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
    # Precalc calibrates rates to meet revenue_requirement; BAT
    # postprocessing uses costs_by_type (frozen residual + shifted MC)
    # for residual allocation.  See context doc for full data flow.

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

    save_file_loc = getattr(bs, "save_file_loc", None)
    if save_file_loc is not None:
        distribution_mc_path = Path(save_file_loc) / "distribution_marginal_costs.csv"
        distribution_marginal_costs.to_csv(distribution_mc_path, index=True)
        log.info(".... Saved distribution marginal costs: %s", distribution_mc_path)
        if settings.demand_flex.enabled:
            tracker_path = Path(save_file_loc) / "demand_flex_elasticity_tracker.csv"
            elasticity_tracker.to_csv(tracker_path, index=True)
            log.info(".... Saved demand-flex elasticity tracker: %s", tracker_path)

    log.info(".... Completed RI residential (non-LMI) rate scenario simulation")


def main() -> None:
    args = _parse_args()
    settings = _resolve_settings(args)
    run(settings)


if __name__ == "__main__":
    main()
