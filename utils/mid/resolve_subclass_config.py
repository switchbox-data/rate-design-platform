"""Resolve subclass group_col and group_value_to_subclass from a scenario config.

By default this reads the first run with ``run_includes_subclasses=True``.
Callers may instead pass ``--run-num`` to resolve a specific subclass run.

For the chosen run it extracts:
  - group_col from subclass_config.group_col
  - group_value_to_subclass derived from subclass_config.selectors
    (format: value=subclass_key, comma-separated; multi-value selectors are expanded)

Prints both on a single line, space-separated, for consumption by the Justfile.
Exits with a non-zero code and error message if no subclass run is found or if
subclass_config is missing on a run that has run_includes_subclasses=True.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


def selectors_to_group_value_map(selectors: dict[str, str]) -> str:
    """Convert subclass_config.selectors to a group_value_to_subclass string.

    Each selector maps a subclass key to a comma-separated string of group_col
    values. This function inverts the mapping: each individual group_col value
    maps to its subclass key.

    Example:
        {"hp": "true", "non-hp": "false"}
        → "true=hp,false=non-hp"

        {"electric_heating": "heat_pump,electrical_resistance",
         "non_electric_heating": "natgas,delivered_fuels,other"}
        → "heat_pump=electric_heating,electrical_resistance=electric_heating,
           natgas=non_electric_heating,delivered_fuels=non_electric_heating,
           other=non_electric_heating"
    """
    pairs: list[str] = []
    for subclass_key, values_str in selectors.items():
        for value in values_str.split(","):
            value = value.strip()
            if value:
                pairs.append(f"{value}={subclass_key}")
    return ",".join(pairs)


def _get_run(
    runs: dict[int | str, dict[str, object]],
    run_num: int,
) -> dict[str, object]:
    run = runs.get(run_num) or runs.get(str(run_num))
    if run is None:
        raise ValueError(f"run {run_num} not found in scenario config")
    return run


def resolve_subclass_config(
    scenario_config: Path,
    *,
    run_num: int | None = None,
) -> tuple[str, str]:
    with scenario_config.open(encoding="utf-8") as f:
        runs: dict[int | str, dict[str, object]] = yaml.safe_load(f)["runs"]

    if run_num is not None:
        run = _get_run(runs, run_num)
        selected_run_num = run_num
        if not run.get("run_includes_subclasses"):
            raise ValueError(
                f"run {run_num} is not a subclass run in {scenario_config}"
            )
    else:
        selected_run_num = None
        run = None
        for num in sorted(runs, key=lambda value: int(value)):
            candidate = runs[num]
            if candidate.get("run_includes_subclasses"):
                selected_run_num = int(num)
                run = candidate
                break
        if run is None or selected_run_num is None:
            raise ValueError(
                f"no run with run_includes_subclasses=True found in {scenario_config}"
            )

    subclass_config = run.get("subclass_config")
    if subclass_config is None:
        raise ValueError(
            f"run {selected_run_num} has run_includes_subclasses=True but no "
            f"subclass_config block in {scenario_config}. Regenerate scenario YAMLs "
            "after adding subclass_group_col and subclass_selectors columns to the "
            "Google Sheet."
        )

    group_col = str(subclass_config["group_col"])
    selectors = subclass_config["selectors"]
    if not isinstance(selectors, dict):
        raise ValueError(
            f"run {selected_run_num} subclass_config.selectors must be a mapping"
        )
    normalized_selectors = {str(key): str(value) for key, value in selectors.items()}
    group_value_to_subclass = selectors_to_group_value_map(normalized_selectors)
    return group_col, group_value_to_subclass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario_config", type=Path)
    parser.add_argument(
        "--run-num",
        type=int,
        help="Optional explicit subclass run number to resolve config from.",
    )
    args = parser.parse_args()

    try:
        group_col, group_value_to_subclass = resolve_subclass_config(
            args.scenario_config,
            run_num=args.run_num,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    print(group_col, group_value_to_subclass)


if __name__ == "__main__":
    main()
