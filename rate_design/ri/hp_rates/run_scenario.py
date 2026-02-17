"""Entrypoint for running RI heat pump rate scenarios - Residential (non-LMI)."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pandas as pd
import polars as pl
import yaml
from cairo.rates_tool.loads import (
    _return_load,
    process_residential_hourly_demand,
    return_buildingstock,
)
from cairo.rates_tool.systemsimulator import (
    MeetRevenueSufficiencySystemWide,
    _initialize_tariffs,
    _return_export_compensation_rate,
    _return_revenue_requirement_target,
)

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.cairo import _load_cambium_marginal_costs, build_bldg_id_to_load_filepath
from utils.pre.compute_tou import (
    combine_marginal_costs,
    compute_tou_cost_causation_ratio,
    create_tou_tariff,
    find_tou_peak_window,
    generate_tou_tariff_map,
)
from utils.pre.generate_precalc_mapping import generate_default_precalc_mapping

log = logging.getLogger("rates_analysis").getChild("tests")

# Resolve paths relative to this script so the scenario can be run from any CWD.
PATH_PROJECT = Path(__file__).resolve().parent
PATH_CONFIG = PATH_PROJECT / "config"
PATH_RESSTOCK = Path("/data.sb/nrel/resstock/res_2024_amy2018_2/")
DEFAULT_OUTPUT_DIR = Path("/data.sb/switchbox/cairo/ri_hp_rates/analysis_outputs")
DEFAULT_SCENARIO_CONFIG = PATH_CONFIG / "scenarios.yaml"


@dataclass(slots=True)
class ComputeTouConfig:
    """Optional MC-driven TOU tariff computation."""

    enabled: bool = False
    tou_tariff_key: str = ""
    flat_tariff_key: str = ""
    tou_window_hours: int = 4
    tou_base_rate: float = 0.06
    tou_fixed_charge: float = 6.75


@dataclass(slots=True)
class ScenarioSettings:
    """All inputs needed to run one RI scenario."""

    run_name: str
    run_type: str
    state: str
    region: str
    utility: str
    path_results: Path
    path_resstock_metadata: Path
    path_resstock_loads: Path
    path_cambium_marginal_costs: str | Path
    path_tariff_maps_electric: Path
    path_tariff_maps_gas: Path
    path_tariffs_electric: dict[str, Path]
    path_tariffs_gas: dict[str, Path]
    precalc_tariff_path: Path
    precalc_tariff_key: str
    utility_revenue_requirement: float
    utility_customer_count: int
    year_run: int
    year_dollar_conversion: int
    process_workers: int
    solar_pv_compensation: str = "net_metering"
    delivery_only_rev_req_passed: bool = False
    compute_tou: ComputeTouConfig = field(default_factory=ComputeTouConfig)


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


def _parse_compute_tou_config(run: dict[str, Any]) -> ComputeTouConfig:
    """Parse the optional ``compute_tou`` block from a run config."""
    raw = run.get("compute_tou")
    if raw is None or not isinstance(raw, dict):
        return ComputeTouConfig()
    return ComputeTouConfig(
        enabled=_parse_bool(raw.get("enabled", False), "compute_tou.enabled"),
        tou_tariff_key=str(raw.get("tou_tariff_key", "")),
        flat_tariff_key=str(raw.get("flat_tariff_key", "")),
        tou_window_hours=_parse_int(
            raw.get("tou_window_hours", 4), "compute_tou.tou_window_hours"
        ),
        tou_base_rate=float(raw.get("tou_base_rate", 0.06)),
        tou_fixed_charge=float(raw.get("tou_fixed_charge", 6.75)),
    )


def _build_settings_from_yaml_run(
    run: dict[str, Any],
    run_num: int,
    output_dir: Path,
    run_name_override: str | None,
) -> ScenarioSettings:
    """Build runtime settings from repo YAML scenario config."""
    state = str(run.get("state", "RI")).upper()
    region = str(_require_value(run, "region")).lower()
    utility = str(_require_value(run, "utility")).lower()
    mode = str(run.get("run_type", "precalc"))
    upgrade = f"{_parse_int(run.get('upgrade', 0), 'upgrade'):02d}"
    year_run = _parse_int(run.get("year_run"), "year_run")
    year_dollar_conversion = _parse_int(
        run.get("year_dollar_conversion"),
        "year_dollar_conversion",
    )
    process_workers = _parse_int(run.get("process_workers", 20), "process_workers")
    utility_revenue_requirement = float(
        _parse_int(
            _require_value(run, "utility_revenue_requirement"),
            "utility_revenue_requirement",
        )
    )
    utility_customer_count = _parse_int(
        _require_value(run, "utility_customer_count"),
        "utility_customer_count",
    )
    solar_pv_compensation = str(run.get("solar_pv_compensation", "net_metering"))

    compute_tou_cfg = _parse_compute_tou_config(run)

    # When compute_tou is enabled the tariff paths / maps are derived at
    # runtime so the YAML fields are optional.  We still need placeholder
    # values so the dataclass can be constructed – they will be overridden
    # inside run() once the TOU tariff is generated.
    if compute_tou_cfg.enabled:
        path_tariffs_electric: dict[str, Path] = {}
        path_tariff_maps_electric = (
            PATH_CONFIG / "tariff_maps" / "electric" / "placeholder.csv"
        )
        precalc_tariff_path = PATH_CONFIG / "tariffs" / "electric" / "placeholder.json"
        precalc_tariff_key = compute_tou_cfg.tou_tariff_key

        # Include the flat tariff path so non-HP customers still have a tariff.
        flat_key = compute_tou_cfg.flat_tariff_key
        if flat_key:
            raw_flat_path = run.get("path_tariffs_electric", {}).get(flat_key)
            if raw_flat_path:
                path_tariffs_electric[flat_key] = _resolve_path(
                    str(raw_flat_path), PATH_CONFIG
                )
    else:
        path_tariffs_electric_raw = _require_mapping(
            run.get("path_tariffs_electric"), "path_tariffs_electric"
        )
        path_tariffs_electric = {
            str(key): _resolve_path(str(path), PATH_CONFIG)
            for key, path in path_tariffs_electric_raw.items()
        }
        path_tariff_maps_electric = _resolve_path(
            str(_require_value(run, "path_tariff_maps_electric")),
            PATH_CONFIG,
        )
        precalc_tariff_path = _resolve_path(
            str(_require_value(run, "precalc_tariff_path")),
            PATH_CONFIG,
        )
        precalc_tariff_key = str(_require_value(run, "precalc_tariff_key"))

    path_tariffs_gas_raw = _require_mapping(
        run.get("path_tariffs_gas"), "path_tariffs_gas"
    )
    path_tariffs_gas = {
        str(key): _resolve_path(str(path), PATH_CONFIG)
        for key, path in path_tariffs_gas_raw.items()
    }

    default_run_name = str(run.get("run_name", f"ri_rie_run_{run_num:02d}"))
    delivery_only_rev_req_passed = _parse_bool(
        run.get(
            "delivery_only_rev_req_passed",
            "supply_adj" in precalc_tariff_key,
        ),
        "delivery_only_rev_req_passed",
    )
    return ScenarioSettings(
        run_name=run_name_override or default_run_name,
        run_type=mode,
        state=state,
        region=region,
        utility=utility,
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
        path_cambium_marginal_costs=_resolve_path_or_uri(
            str(_require_value(run, "path_cambium_marginal_costs")),
            PATH_CONFIG,
        ),
        path_tariff_maps_electric=path_tariff_maps_electric,
        path_tariff_maps_gas=_resolve_path(
            str(_require_value(run, "path_tariff_maps_gas")),
            PATH_CONFIG,
        ),
        path_tariffs_electric=path_tariffs_electric,
        path_tariffs_gas=path_tariffs_gas,
        precalc_tariff_path=precalc_tariff_path,
        precalc_tariff_key=precalc_tariff_key,
        utility_revenue_requirement=utility_revenue_requirement,
        utility_customer_count=utility_customer_count,
        year_run=year_run,
        year_dollar_conversion=year_dollar_conversion,
        process_workers=process_workers,
        solar_pv_compensation=solar_pv_compensation,
        delivery_only_rev_req_passed=delivery_only_rev_req_passed,
        compute_tou=compute_tou_cfg,
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
# Loading helpers (extracted from run() for clarity)
# ---------------------------------------------------------------------------


def _load_distribution_marginal_costs(
    state: str,
    region: str,
    utility: str,
    year_run: int,
) -> pd.Series:
    """Load distribution marginal costs from S3 and return as a tz-aware Series."""
    distribution_mc_root = (
        f"s3://data.sb/switchbox/marginal_costs/{state.lower().strip('/')}/"
    )
    distribution_mc_scan: pl.LazyFrame = pl.scan_parquet(
        distribution_mc_root,
        hive_partitioning=True,
        storage_options=get_aws_storage_options(),
    )
    distribution_mc_scan = (
        distribution_mc_scan.filter(pl.col("region").cast(pl.Utf8) == region)
        .filter(pl.col("utility").cast(pl.Utf8) == utility)
        .filter(pl.col("year").cast(pl.Utf8) == str(year_run))
    )
    distribution_mc_df = cast(pl.DataFrame, distribution_mc_scan.collect())
    distribution_marginal_costs = distribution_mc_df.to_pandas()
    required_cols = {"timestamp", "mc_total_per_kwh"}
    missing_cols = required_cols.difference(distribution_marginal_costs.columns)
    if missing_cols:
        raise ValueError(
            "Distribution marginal costs parquet is missing required columns "
            f"{sorted(required_cols)}. Missing: {sorted(missing_cols)}"
        )
    distribution_marginal_costs = distribution_marginal_costs.set_index("timestamp")[
        "mc_total_per_kwh"
    ]
    distribution_marginal_costs.index = pd.DatetimeIndex(
        distribution_marginal_costs.index
    ).tz_localize("EST")
    distribution_marginal_costs.index.name = "time"
    distribution_marginal_costs.name = "Marginal Distribution Costs ($/kWh)"
    return distribution_marginal_costs


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

    distribution_marginal_costs = _load_distribution_marginal_costs(
        state=settings.state,
        region=settings.region,
        utility=settings.utility,
        year_run=settings.year_run,
    )

    log.info(
        ".... Loaded distribution marginal costs rows=%s",
        len(distribution_marginal_costs),
    )

    # ------------------------------------------------------------------
    # Phase 2 (optional): Derive TOU tariff from marginal costs
    # ------------------------------------------------------------------

    if settings.compute_tou.enabled:
        tou_cfg = settings.compute_tou
        log.info(
            ".... Computing TOU tariff from marginal costs (window=%d h, key=%s)",
            tou_cfg.tou_window_hours,
            tou_cfg.tou_tariff_key,
        )

        # System hourly load (demand-weighted aggregate of building loads)
        hourly_system_load = process_residential_hourly_demand(
            bldg_load=raw_load_elec,
            sample_weights=customer_metadata[["bldg_id", "weight"]],
        )

        combined_mc = combine_marginal_costs(
            bulk_marginal_costs, distribution_marginal_costs
        )

        peak_hours = find_tou_peak_window(
            combined_mc=combined_mc,
            hourly_system_load=hourly_system_load,
            window_hours=tou_cfg.tou_window_hours,
        )

        ratio = compute_tou_cost_causation_ratio(
            combined_mc=combined_mc,
            hourly_system_load=hourly_system_load,
            peak_hours=peak_hours,
        )

        tou_tariff = create_tou_tariff(
            label=tou_cfg.tou_tariff_key,
            peak_hours=peak_hours,
            peak_offpeak_ratio=ratio,
            base_rate=tou_cfg.tou_base_rate,
            fixed_charge=tou_cfg.tou_fixed_charge,
            utility=settings.utility,
        )

        # Write TOU tariff JSON
        tou_tariff_path = (
            PATH_CONFIG / "tariffs" / "electric" / f"{tou_cfg.tou_tariff_key}.json"
        )
        tou_tariff_path.parent.mkdir(parents=True, exist_ok=True)
        with open(tou_tariff_path, "w") as f:
            json.dump(tou_tariff, f, indent=2)
        log.info(".... Wrote TOU tariff JSON: %s", tou_tariff_path)

        # Generate tariff map (HP → TOU, non-HP → flat)
        tou_tariff_map_df = generate_tou_tariff_map(
            customer_metadata=customer_metadata,
            tou_tariff_key=tou_cfg.tou_tariff_key,
            flat_tariff_key=tou_cfg.flat_tariff_key,
        )
        tou_map_path = (
            PATH_CONFIG
            / "tariff_maps"
            / "electric"
            / f"{tou_cfg.tou_tariff_key}_tariff_map.csv"
        )
        tou_map_path.parent.mkdir(parents=True, exist_ok=True)
        tou_tariff_map_df.to_csv(tou_map_path, index=False)
        log.info(".... Wrote TOU tariff map: %s", tou_map_path)

        # Override settings so downstream code uses the computed tariff
        settings.path_tariffs_electric[tou_cfg.tou_tariff_key] = tou_tariff_path
        settings.path_tariff_maps_electric = tou_map_path
        settings.precalc_tariff_path = tou_tariff_path
        settings.precalc_tariff_key = tou_cfg.tou_tariff_key

        # Ensure the flat tariff is also in path_tariffs_electric so
        # _initialize_tariffs can resolve both keys in the tariff map.
        if (
            tou_cfg.flat_tariff_key
            and tou_cfg.flat_tariff_key not in settings.path_tariffs_electric
        ):
            flat_path = (
                PATH_CONFIG / "tariffs" / "electric" / f"{tou_cfg.flat_tariff_key}.json"
            )
            if flat_path.exists():
                settings.path_tariffs_electric[tou_cfg.flat_tariff_key] = flat_path

    # ------------------------------------------------------------------
    # Phase 3: Initialize tariffs and system requirements
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
        revenue_requirement_target=settings.utility_revenue_requirement,
        residual_cost=None,
        residual_cost_frac=None,
        bulk_marginal_costs=bulk_marginal_costs,
        distribution_marginal_costs=distribution_marginal_costs,
        low_income_strategy=None,
        delivery_only_rev_req_passed=settings.delivery_only_rev_req_passed,
    )

    # ------------------------------------------------------------------
    # Phase 4: Run CAIRO simulation
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
