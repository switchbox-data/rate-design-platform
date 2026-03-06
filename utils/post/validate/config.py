"""Run configuration and expected behaviors for validation.

Defines RunConfig and RunBlock dataclasses, parses run configs from scenario YAMLs,
and encodes expected behaviors per run block (revenue neutrality, BAT relevance,
tariff stability).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml

from utils import get_project_root


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

    @classmethod
    def from_yaml_run(cls, run_num: int, run_dict: dict[str, Any]) -> RunConfig:
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
        elasticity = float(run_dict.get("elasticity", 0.0))

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

        return cls(
            run_num=run_num,
            run_name=run_name,
            run_type=run_type,
            upgrade=upgrade,
            cost_scope=cost_scope,
            has_subclasses=has_subclasses,
            tariff_type=tariff_type,
            elasticity=elasticity,
        )


@dataclass(frozen=True, slots=True)
class RunBlock:
    """A pair (or set) of CAIRO runs validated together.

    Run blocks correspond to the logical groups in the pipeline:
    - Runs 1-2: precalc flat, delivery vs delivery+supply
    - Runs 3-4: default flat on upgrade 02
    - Runs 5-6: precalc seasonal with HP/non-HP subclasses
    - Runs 7-8: default seasonal on upgrade 02
    """

    run_nums: tuple[int, ...]
    configs: tuple[RunConfig, ...]
    # Expected behaviors used by checks.py
    revenue_neutral: bool
    """True when the run uses a real RR target (precalc runs 1-2, 5-6)."""
    bat_relevant: bool
    """True when BAT cross-subsidy direction / magnitude should be checked."""
    tariff_should_be_unchanged: bool
    """True when the output tariff must match the input exactly (default runs 3-4, 7-8)."""
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
        configs[int(num)] = RunConfig.from_yaml_run(int(num), run_dict)

    return configs


def define_run_blocks(configs: dict[int, RunConfig]) -> list[RunBlock]:
    """Group RunConfig objects into RunBlock objects with their expected behaviors.

    Defines the four standard validation blocks (runs 1-2, 3-4, 5-6, 7-8).
    A block is only created when all its constituent runs are present in ``configs``.

    Expected behaviors per block (from the plan):

    +----------+----------+---------+------------+-----------------+--------------+-----------------+
    | Block    | Runs     | Type    | Upgrade    | Revenue neutral | BAT relevant | Tariff changes? |
    +==========+==========+=========+============+=================+==============+=================+
    | 1-2      | 1 and 2  | precalc | 00         | Yes (total RR)  | Yes          | Yes (calibrated)|
    +----------+----------+---------+------------+-----------------+--------------+-----------------+
    | 3-4      | 3 and 4  | default | 02         | No (large RR)   | No           | No (same as 1-2)|
    +----------+----------+---------+------------+-----------------+--------------+-----------------+
    | 5-6      | 5 and 6  | precalc | 00         | Yes (per-class) | Yes (~0)     | Partial (non-HP)|
    +----------+----------+---------+------------+-----------------+--------------+-----------------+
    | 7-8      | 7 and 8  | default | 02         | No (large RR)   | No           | No (same as 5-6)|
    +----------+----------+---------+------------+-----------------+--------------+-----------------+

    Args:
        configs: ``{run_num: RunConfig}`` from :func:`load_run_configs_from_yaml`.

    Returns:
        List of RunBlock objects in ascending run-number order.
    """
    block_specs: list[tuple[tuple[int, int], bool, bool, bool, str]] = [
        # (run_nums, revenue_neutral, bat_relevant, tariff_unchanged, description)
        (
            (1, 2),
            True,
            True,
            False,
            "Precalc flat rates: delivery (run 1) and delivery+supply (run 2)",
        ),
        (
            (3, 4),
            False,
            False,
            True,
            "Default flat rates on upgrade 02 — tariffs inherited from runs 1-2",
        ),
        (
            (5, 6),
            True,
            True,
            False,
            "Precalc seasonal rates with HP/non-HP subclasses: delivery (run 5) and delivery+supply (run 6)",
        ),
        (
            (7, 8),
            False,
            False,
            True,
            "Default seasonal rates on upgrade 02 — tariffs inherited from runs 5-6",
        ),
    ]

    blocks: list[RunBlock] = []
    for run_nums, revenue_neutral, bat_relevant, tariff_unchanged, description in block_specs:
        r1, r2 = run_nums
        if r1 in configs and r2 in configs:
            blocks.append(
                RunBlock(
                    run_nums=run_nums,
                    configs=(configs[r1], configs[r2]),
                    revenue_neutral=revenue_neutral,
                    bat_relevant=bat_relevant,
                    tariff_should_be_unchanged=tariff_unchanged,
                    description=description,
                )
            )

    return blocks
