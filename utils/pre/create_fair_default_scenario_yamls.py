"""Generate fair-default scenario YAMLs from baseline scenario YAMLs.

Reads rate_design/hp_rates/<state>/config/scenarios/scenarios_<utility>.yaml
(the baseline) to extract shared paths (ResStock, MC, utility_assignment, etc.),
then iterates over 6 (subclass × strategy) combos × 4 run phases (precalc-del,
precalc-sup, u2-del, u2-sup) = 24 runs and emits a standalone YAML at:

    rate_design/hp_rates/<state>/config/scenarios/fair_default/scenarios_<utility>.yaml

The generated YAML can be verified against the hand-written template by diffing
the two files (they should be identical in content, modulo YAML serialization).

Run numbers 101-124 are assigned per the plan naming convention:
  101-104  hp   × default_fixed
  105-108  hp   × default_seasonal
  109-112  hp   × default_fixed_seasonal_mc
  113-116  eheat × default_fixed
  117-120  eheat × default_seasonal
  121-124  eheat × default_fixed_seasonal_mc

Within each group: odd = delivery, even = supply; lower pair = precalc, upper = u2.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from utils import get_project_root

LOGGER = logging.getLogger(__name__)

NY_UTILITIES = [
    "nyseg",
    "coned",
    "oru",
    "niagara_mohawk",
    "central_hudson",
    "rge",
]

SUBCLASSES = [
    {
        "shorthand": "hp",
        "group_col": "has_hp",
        "group_value": "true",
    },
    {
        "shorthand": "eheat",
        "group_col": "heating_type_v2",
        "group_value": "electric_heating",
    },
]

STRATEGIES = [
    ("fixed_charge_only", "default_fixed"),
    ("seasonal_rates_only", "default_seasonal"),
    ("fixed_plus_seasonal_mc", "default_fixed_seasonal_mc"),
]


def _load_baseline_run(
    baseline_yaml_path: Path,
    run_num: int,
) -> dict[str, Any]:
    """Load one run entry from the baseline scenarios YAML."""
    with open(baseline_yaml_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    runs: dict[int, dict[str, Any]] = data.get("runs", {})
    if run_num not in runs:
        raise KeyError(
            f"Run {run_num} not found in {baseline_yaml_path}. "
            f"Available runs: {sorted(runs.keys())}"
        )
    return runs[run_num]


def _tariff_stem(
    utility: str, shorthand: str, strategy_key: str, is_supply: bool
) -> str:
    base = f"{utility}_{shorthand}_{strategy_key}"
    return f"{base}_supply" if is_supply else base


def _tariff_json(
    utility: str, shorthand: str, strategy_key: str, is_supply: bool, is_u2: bool
) -> str:
    stem = _tariff_stem(utility, shorthand, strategy_key, is_supply)
    if is_u2:
        if is_supply:
            return f"tariffs/electric/{utility}_{shorthand}_{strategy_key}_supply_calibrated.json"
        else:
            return (
                f"tariffs/electric/{utility}_{shorthand}_{strategy_key}_calibrated.json"
            )
    return f"tariffs/electric/{stem}.json"


def _tariff_map_csv(
    utility: str,
    shorthand: str,
    strategy_key: str,
    is_supply: bool,
    is_u2: bool,
) -> str:
    stem = _tariff_stem(utility, shorthand, strategy_key, is_supply)
    if is_u2:
        if is_supply:
            return f"tariff_maps/electric/{utility}_{shorthand}_{strategy_key}_supply_calibrated.csv"
        else:
            return f"tariff_maps/electric/{utility}_{shorthand}_{strategy_key}_calibrated.csv"
    return f"tariff_maps/electric/{stem}.csv"


def _run_name(
    state: str,
    utility: str,
    run_num: int,
    upgrade: str,
    run_type: str,
    phase_suffix: str,
    shorthand: str,
    strategy_key: str,
) -> str:
    upgrade_padded = upgrade.zfill(2)
    return (
        f"{state.lower()}_{utility}_run{run_num}_up{upgrade_padded}"
        f"_{run_type}_{phase_suffix}__{shorthand}_{strategy_key}"
    )


def _make_run(
    *,
    run_num: int,
    state: str,
    utility: str,
    shorthand: str,
    strategy_key: str,
    strategy_enum: str,
    group_col: str,
    group_value: str,
    is_supply: bool,
    is_u2: bool,
    baseline_delivery_run: dict[str, Any],
    baseline_supply_run: dict[str, Any],
) -> dict[str, Any]:
    """Construct one fair-default run entry."""
    if is_u2:
        upgrade = "2"
        run_type = "default"
        phase_suffix = "supply" if is_supply else "delivery"
        rev_req = f"rev_requirement/{utility}_large_number.yaml"
        resstock_upgrade = "02"
        tariff_maps_gas = f"tariff_maps/gas/{utility}_u02.csv"
    else:
        upgrade = "0"
        run_type = "precalc"
        phase_suffix = "supply" if is_supply else "delivery"
        rev_req = f"rev_requirement/{utility}.yaml"
        resstock_upgrade = "00"
        tariff_maps_gas = f"tariff_maps/gas/{utility}_u00.csv"

    run_name = _run_name(
        state=state,
        utility=utility,
        run_num=run_num,
        upgrade=upgrade,
        run_type=run_type,
        phase_suffix=phase_suffix,
        shorthand=shorthand,
        strategy_key=strategy_key,
    )

    base = baseline_supply_run if is_supply else baseline_delivery_run
    resstock_metadata = base.get("path_resstock_metadata", "").replace(
        "upgrade=00", f"upgrade={resstock_upgrade}"
    )
    resstock_loads = base.get("path_resstock_loads", "").replace(
        "upgrade=00", f"upgrade={resstock_upgrade}"
    )

    path_outputs = (
        f"/data.sb/switchbox/cairo/outputs/hp_rates/{state.lower()}/{utility}"
        f"/<execution_time>/{run_name}"
    )

    if is_supply:
        supply_energy_mc = base["path_supply_energy_mc"]
        supply_capacity_mc = base["path_supply_capacity_mc"]
    else:
        state_lower = state.lower()
        supply_energy_mc = (
            f"s3://data.sb/switchbox/marginal_costs/{state_lower}/supply/energy"
            f"/utility={utility}/year=2025/zero.parquet"
        )
        supply_capacity_mc = (
            f"s3://data.sb/switchbox/marginal_costs/{state_lower}/supply/capacity"
            f"/utility={utility}/year=2025/zero.parquet"
        )

    tariff_json = _tariff_json(utility, shorthand, strategy_key, is_supply, is_u2)
    tariff_map_csv = _tariff_map_csv(utility, shorthand, strategy_key, is_supply, is_u2)

    base_run = 2 if is_supply else 1

    run: dict[str, Any] = {
        "run_name": run_name,
        "state": state.upper(),
        "utility": utility,
        "run_type": run_type,
        "upgrade": str(int(upgrade)),
        "path_tariff_maps_electric": tariff_map_csv,
        "path_tariff_maps_gas": tariff_maps_gas,
        "path_resstock_metadata": resstock_metadata,
        "path_resstock_loads": resstock_loads,
        "path_dist_and_sub_tx_mc": base["path_dist_and_sub_tx_mc"],
        "path_utility_assignment": base["path_utility_assignment"],
        "path_tariffs_gas": base.get("path_tariffs_gas", "tariffs/gas"),
        "path_outputs": path_outputs,
        "path_supply_energy_mc": supply_energy_mc,
        "path_supply_capacity_mc": supply_capacity_mc,
        "path_tariffs_electric": {"all": tariff_json},
        "utility_revenue_requirement": rev_req,
        "run_includes_supply": is_supply,
        "run_includes_subclasses": False,
        "residual_allocation_delivery": "percustomer",
        "residual_allocation_supply": "percustomer",
        "fair_default": {
            "target_subclass": shorthand,
            "target_subclass_group_col": group_col,
            "target_subclass_group_value": group_value,
            "strategy": strategy_enum,
            "cross_subsidy_metric": "BAT_percustomer",
            "base_run": base_run,
        },
        "path_electric_utility_stats": base["path_electric_utility_stats"],
        "path_bulk_tx_mc": base["path_bulk_tx_mc"],
        "solar_pv_compensation": base.get("solar_pv_compensation", "net_metering"),
        "year_run": base.get("year_run", 2025),
        "year_dollar_conversion": base.get("year_dollar_conversion", 2025),
        "process_workers": base.get("process_workers", 8),
        "elasticity": 0.0,
    }

    return run


def generate_fair_default_runs(
    *,
    state: str,
    utility: str,
    baseline_yaml_path: Path,
) -> dict[int, dict[str, Any]]:
    """Generate all 24 fair-default run entries for one utility."""
    baseline_del = _load_baseline_run(baseline_yaml_path, 1)
    baseline_sup = _load_baseline_run(baseline_yaml_path, 2)

    runs: dict[int, dict[str, Any]] = {}
    run_num = 101

    for sub in SUBCLASSES:
        for strategy_enum, strategy_key in STRATEGIES:
            # 4 runs per (subclass, strategy): precalc-del, precalc-sup, u2-del, u2-sup
            for is_u2, is_supply in [
                (False, False),
                (False, True),
                (True, False),
                (True, True),
            ]:
                run = _make_run(
                    run_num=run_num,
                    state=state,
                    utility=utility,
                    shorthand=sub["shorthand"],
                    strategy_key=strategy_key,
                    strategy_enum=strategy_enum,
                    group_col=sub["group_col"],
                    group_value=sub["group_value"],
                    is_supply=is_supply,
                    is_u2=is_u2,
                    baseline_delivery_run=baseline_del,
                    baseline_supply_run=baseline_sup,
                )
                runs[run_num] = run
                run_num += 1

    return runs


def write_fair_default_scenario_yaml(
    *,
    state: str,
    utility: str,
    output_dir: Path | None = None,
) -> Path:
    """Read baseline YAML and write fair-default YAML for one utility.

    Returns:
        Path to the written YAML file.
    """
    project_root = get_project_root()
    state_lower = state.lower()
    config_dir = project_root / "rate_design" / "hp_rates" / state_lower / "config"
    baseline_yaml_path = config_dir / "scenarios" / f"scenarios_{utility}.yaml"

    if not baseline_yaml_path.exists():
        raise FileNotFoundError(
            f"Baseline scenario YAML not found: {baseline_yaml_path}"
        )

    runs = generate_fair_default_runs(
        state=state,
        utility=utility,
        baseline_yaml_path=baseline_yaml_path,
    )

    out_dir = output_dir or (config_dir / "scenarios" / "fair_default")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"scenarios_{utility}.yaml"

    payload: dict[str, Any] = {"runs": runs}
    yaml_str = yaml.dump(
        payload,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )
    out_path.write_text(yaml_str, encoding="utf-8")
    LOGGER.info("Wrote %s (%d runs)", out_path, len(runs))
    return out_path


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description=(
            "Generate fair-default scenario YAMLs from baseline scenario YAMLs. "
            "Writes to rate_design/hp_rates/<state>/config/scenarios/fair_default/."
        )
    )
    parser.add_argument(
        "--state",
        required=True,
        help="State abbreviation (e.g. ny).",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--utility",
        help="Single utility name (e.g. nyseg).",
    )
    group.add_argument(
        "--all-ny",
        action="store_true",
        help="Generate for all NY utilities.",
    )
    group.add_argument(
        "--utilities",
        help="Comma-separated list of utilities (e.g. nyseg,coned,rge).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Override output directory (default: scenarios/fair_default/ under state config).",
    )
    args = parser.parse_args()

    if args.all_ny:
        utilities = NY_UTILITIES
    elif args.utilities:
        utilities = [u.strip() for u in args.utilities.split(",")]
    else:
        utilities = [args.utility]

    for utility in utilities:
        try:
            out_path = write_fair_default_scenario_yaml(
                state=args.state,
                utility=utility,
                output_dir=args.output_dir,
            )
            print(f"Wrote {out_path}")
        except Exception as exc:
            LOGGER.error("Failed for %s: %s", utility, exc)
            raise


if __name__ == "__main__":
    main()
