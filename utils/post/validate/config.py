"""Run configuration and expected behaviors for validation.

Defines RunConfig and RunBlock dataclasses, parses run configs from scenario YAMLs,
and encodes expected behaviors per run block (revenue neutrality, BAT relevance,
tariff stability).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from utils import get_project_root
from utils.post.validate.subclasses import SubclassSpec, subclass_spec_from_run


@dataclass(frozen=True, slots=True)
class RunConfig:
    """Configuration for a single CAIRO run, derived from a scenario YAML entry."""

    run_num: int
    run_name: str
    run_type: str  # "precalc" or "default"
    upgrade: str  # "0" (upgrade 00) or "2" (upgrade 02)
    cost_scope: str  # "delivery" or "delivery+supply"
    has_subclasses: bool  # True if the run uses HP/non-HP subclasses
    tariff_type: str  # "flat", "seasonal", "seasonalTOU", or "seasonalTOU_flex"
    elasticity: float  # 0.0 = no demand response, -0.1 = flex variant
    path_resstock_loads: str  # Local path to ResStock load curves directory
    path_dist_and_sub_tx_mc: str  # S3 URI to dist+sub-TX marginal costs parquet
    path_bulk_tx_mc: str | None  # S3 URI to bulk TX marginal costs parquet (optional)
    path_supply_energy_mc: str  # S3 URI to supply energy marginal costs parquet
    path_supply_capacity_mc: str  # S3 URI to supply capacity marginal costs parquet
    revenue_requirement_filename: str | None = None
    residual_allocation_delivery: str | None = None
    residual_allocation_supply: str | None = None
    subclass_spec: SubclassSpec | None = None
    tariff_keys_by_alias: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_yaml_run(
        cls,
        run_num: int,
        run_dict: dict[str, Any],
        *,
        scenario_subclass_config: object | None = None,
    ) -> RunConfig:
        """Parse a RunConfig from one YAML run entry (the dict under runs[N]).

        Args:
            run_num: Integer run number (key under ``runs:`` in the YAML).
            run_dict: The dict value for that run, as loaded by yaml.safe_load.
        """
        run_name = str(run_dict.get("run_name", f"run_{run_num}"))
        run_type = str(run_dict.get("run_type", "precalc"))
        upgrade = str(run_dict.get("upgrade", "0"))
        cost_scope = (
            "delivery+supply"
            if run_dict.get("run_includes_supply", False)
            else "delivery"
        )
        has_subclasses = bool(run_dict.get("run_includes_subclasses", False))
        raw_elasticity = run_dict.get("elasticity", 0.0)
        if isinstance(raw_elasticity, dict):
            # Per-season elasticity (e.g. {summer: -0.22, winter: -0.18}); use mean.
            elasticity = sum(raw_elasticity.values()) / len(raw_elasticity)
        else:
            elasticity = float(raw_elasticity)

        # Infer tariff_type from the suffix after the double-underscore in run_name.
        # Run names follow: {state}_{utility}_run{N}_up{uu}_{type}__{tariff_suffix}
        # Examples (from scenarios_rie.yaml):
        #   ri_rie_run1_up00_precalc__flat                      → flat
        #   ri_rie_run5_up00_precalc__hp_seasonal_vs_flat        → seasonal
        #   ri_rie_run9_up00_precalc__hp_seasonalTOU_vs_flat     → seasonalTOU
        #   ri_rie_run13_up00_precalc__hp_seasonalTOU_flex_vs_flat → seasonalTOU_flex
        # Checks are ordered most-specific-first to avoid false prefix matches.
        tariff_type = "flat"
        if "__" in run_name:
            tariff_suffix = run_name.split("__", 1)[1]
            if "seasonalTOU_flex" in tariff_suffix:
                tariff_type = "seasonalTOU_flex"
            elif "seasonalTOU" in tariff_suffix:
                tariff_type = "seasonalTOU"
            elif "seasonal" in tariff_suffix:
                tariff_type = "seasonal"

        # Parse path fields from YAML (all keys exist in every run entry)
        path_resstock_loads = str(run_dict.get("path_resstock_loads", ""))
        path_dist_and_sub_tx_mc = str(run_dict.get("path_dist_and_sub_tx_mc", ""))
        path_bulk_tx_mc = (
            str(run_dict["path_bulk_tx_mc"])
            if run_dict.get("path_bulk_tx_mc")
            else None
        )
        path_supply_energy_mc = str(run_dict.get("path_supply_energy_mc", ""))
        path_supply_capacity_mc = str(run_dict.get("path_supply_capacity_mc", ""))
        rr_path = str(run_dict.get("utility_revenue_requirement", "")).strip()
        revenue_requirement_filename = Path(rr_path).name if rr_path else None
        residual_allocation_delivery = (
            str(run_dict["residual_allocation_delivery"])
            if run_dict.get("residual_allocation_delivery") is not None
            else None
        )
        residual_allocation_supply = (
            str(run_dict["residual_allocation_supply"])
            if run_dict.get("residual_allocation_supply") is not None
            else None
        )
        subclass_spec = subclass_spec_from_run(
            run_dict,
            scenario_subclass_config=scenario_subclass_config,
        )

        raw_path_tariffs = run_dict.get("path_tariffs_electric", {})
        tariff_keys_by_alias: dict[str, str] = {}
        if isinstance(raw_path_tariffs, dict):
            tariff_keys_by_alias = {
                str(alias): Path(str(path_str)).stem
                for alias, path_str in raw_path_tariffs.items()
            }

        return cls(
            run_num=run_num,
            run_name=run_name,
            run_type=run_type,
            upgrade=upgrade,
            cost_scope=cost_scope,
            has_subclasses=has_subclasses,
            tariff_type=tariff_type,
            elasticity=elasticity,
            path_resstock_loads=path_resstock_loads,
            path_dist_and_sub_tx_mc=path_dist_and_sub_tx_mc,
            path_bulk_tx_mc=path_bulk_tx_mc,
            path_supply_energy_mc=path_supply_energy_mc,
            path_supply_capacity_mc=path_supply_capacity_mc,
            revenue_requirement_filename=revenue_requirement_filename,
            residual_allocation_delivery=residual_allocation_delivery,
            residual_allocation_supply=residual_allocation_supply,
            subclass_spec=subclass_spec,
            tariff_keys_by_alias=tariff_keys_by_alias,
        )


@dataclass(frozen=True, slots=True)
class RunBlock:
    """A pair (or set) of CAIRO runs validated together.

    Run blocks correspond to delivery vs delivery+supply pairs discovered from
    the scenario YAML.  Examples include:
    - 1-2 (precalc flat), 3-4 (default flat)
    - 5-6 (precalc seasonal), 7-8 (default seasonal)
    - 9-10 / 11-12 (seasonal TOU), 13-14 / 15-16 (TOU flex)
    """

    run_nums: tuple[int, ...]
    configs: tuple[RunConfig, ...]
    # Expected behaviors used by checks.py
    revenue_neutral: bool
    """True when the run uses a real RR target (precalc runs 1-2, 5-6)."""
    bat_relevant: bool
    """True when BAT cross-subsidy direction / magnitude should be checked."""
    tariff_should_be_unchanged: bool
    """True when the output tariff must match the input exactly for default paired blocks."""
    description: str

    @property
    def name(self) -> str:
        """Stable block identifier for file naming, e.g. ``runs_1_2``."""
        nums = sorted(self.run_nums)
        if len(nums) == 1:
            return f"run_{nums[0]}"
        return "runs_" + "_".join(map(str, nums))


def load_run_configs_from_yaml(
    state: str,
    utility: str,
    run_nums: list[int] | None = None,
) -> dict[int, RunConfig]:
    """Load RunConfig objects from the scenario YAML for a state/utility pair.

    Reads ``rate_design/hp_rates/{state}/config/scenarios/scenarios_{utility}.yaml``
    relative to the project root.

    Args:
        state: State abbreviation, e.g. ``"ny"`` or ``"ri"`` (case-insensitive).
        utility: Utility identifier, e.g. ``"coned"`` or ``"rie"`` (case-insensitive).
        run_nums: Specific run numbers to load.  If ``None``, loads all runs in the file.

    Returns:
        ``{run_num: RunConfig}`` for each requested run.

    Raises:
        FileNotFoundError: If the scenario YAML does not exist.
        ValueError: If the YAML is malformed or a requested run_num is missing.
    """
    scenario_path = (
        get_project_root()
        / "rate_design"
        / "hp_rates"
        / state.lower()
        / "config"
        / "scenarios"
        / f"scenarios_{utility.lower()}.yaml"
    )

    if not scenario_path.exists():
        raise FileNotFoundError(
            f"Scenario YAML not found for state={state!r} utility={utility!r}: "
            f"{scenario_path}"
        )

    with scenario_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict) or "runs" not in data:
        raise ValueError(
            f"Malformed scenario YAML (expected top-level 'runs' key): {scenario_path}"
        )

    yaml_runs: dict[int | str, Any] = data["runs"]
    scenario_subclass_config = data.get("subclass_config")
    target_nums = run_nums if run_nums is not None else list(yaml_runs.keys())

    configs: dict[int, RunConfig] = {}
    for num in target_nums:
        # YAML integer keys may be loaded as int or str depending on quoting.
        run_dict = yaml_runs.get(num) or yaml_runs.get(str(num))
        if run_dict is None:
            raise ValueError(
                f"Run {num} not found in {scenario_path}. "
                f"Available run numbers: {sorted(yaml_runs.keys())}"
            )
        if not isinstance(run_dict, dict):
            raise ValueError(
                f"Run {num} in {scenario_path} must be a YAML mapping, "
                f"got {type(run_dict).__name__}"
            )
        configs[int(num)] = RunConfig.from_yaml_run(
            int(num),
            run_dict,
            scenario_subclass_config=scenario_subclass_config,
        )

    return configs


def define_run_blocks(configs: dict[int, RunConfig]) -> list[RunBlock]:
    """Group RunConfig objects into delivery vs delivery+supply validation blocks.

    Blocks are discovered from the scenario itself instead of being hardcoded to
    specific run numbers.  This supports both legacy 1-8 scenarios and newer
    1-16 scenarios used by NY and RI.

    A pair is formed when two runs share the same attributes except cost scope:
    one run has ``cost_scope == "delivery"`` and the other has
    ``cost_scope == "delivery+supply"``.

    Expected behaviors are derived from run type:

    - ``precalc`` pairs: revenue-neutral and BAT-relevant
    - ``default`` pairs: tariff should be unchanged from the corresponding
      precalc calibration

    Args:
        configs: ``{run_num: RunConfig}`` from :func:`load_run_configs_from_yaml`.

    Returns:
        List of RunBlock objects in ascending run-number order.
    """

    def _tariff_label(tariff_type: str) -> str:
        labels = {
            "flat": "flat",
            "seasonal": "seasonal",
            "seasonalTOU": "seasonal TOU",
            "seasonalTOU_flex": "seasonal TOU flex",
        }
        return labels.get(tariff_type, tariff_type)

    grouped: dict[tuple[str, str, bool, str, float], dict[str, list[int]]] = {}
    for run_num, cfg in configs.items():
        key = (
            cfg.run_type,
            cfg.upgrade,
            cfg.has_subclasses,
            cfg.tariff_type,
            cfg.elasticity,
        )
        bucket = grouped.setdefault(key, {"delivery": [], "delivery+supply": []})
        bucket[cfg.cost_scope].append(run_num)

    blocks: list[RunBlock] = []
    for (
        run_type,
        upgrade,
        has_subclasses,
        tariff_type,
        elasticity,
    ), scopes in grouped.items():
        delivery_runs = sorted(scopes["delivery"])
        supply_runs = sorted(scopes["delivery+supply"])
        if len(delivery_runs) != len(supply_runs):
            raise ValueError(
                "Cannot pair validation runs into delivery vs delivery+supply blocks "
                f"for run_type={run_type!r}, upgrade={upgrade!r}, "
                f"has_subclasses={has_subclasses}, tariff_type={tariff_type!r}, "
                f"elasticity={elasticity}: delivery={delivery_runs}, "
                f"delivery+supply={supply_runs}"
            )

        for r_delivery, r_supply in zip(delivery_runs, supply_runs, strict=True):
            c_delivery = configs[r_delivery]
            c_supply = configs[r_supply]
            run_nums = (r_delivery, r_supply)

            rate_desc = _tariff_label(tariff_type)
            if run_type == "precalc":
                if has_subclasses:
                    subclass_desc = "subclasses"
                    if c_delivery.subclass_spec is not None:
                        subclass_desc = "/".join(c_delivery.subclass_spec.aliases)
                    desc = (
                        f"Precalc {rate_desc} rates with {subclass_desc}: "
                        f"delivery (run {r_delivery}) and delivery+supply (run {r_supply})"
                    )
                else:
                    desc = (
                        f"Precalc {rate_desc} rates: delivery (run {r_delivery}) "
                        f"and delivery+supply (run {r_supply})"
                    )
            else:
                desc = (
                    f"Default {rate_desc} rates on upgrade {int(upgrade):02d} "
                    f"(runs {r_delivery}-{r_supply})"
                )

            blocks.append(
                RunBlock(
                    run_nums=run_nums,
                    configs=(c_delivery, c_supply),
                    revenue_neutral=(run_type == "precalc"),
                    bat_relevant=(run_type == "precalc"),
                    tariff_should_be_unchanged=(run_type == "default"),
                    description=desc,
                )
            )

    return sorted(blocks, key=lambda b: min(b.run_nums))
