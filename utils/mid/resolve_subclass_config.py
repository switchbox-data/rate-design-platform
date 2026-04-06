"""Resolve subclass group_col and group_value_to_subclass from a scenario config.

Reads the first run with run_includes_subclasses=True and extracts:
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario_config", type=Path)
    args = parser.parse_args()

    with args.scenario_config.open(encoding="utf-8") as f:
        runs: dict[int, dict[str, object]] = yaml.safe_load(f)["runs"]

    for num in sorted(runs):
        run = runs[num]
        if not run.get("run_includes_subclasses"):
            continue

        subclass_config = run.get("subclass_config")
        if subclass_config is None:
            print(
                f"ERROR: run {num} has run_includes_subclasses=True but no "
                f"subclass_config block in {args.scenario_config}. "
                "Regenerate scenario YAMLs after adding subclass_group_col and "
                "subclass_selectors columns to the Google Sheet.",
                file=sys.stderr,
            )
            sys.exit(1)

        group_col = subclass_config["group_col"]
        selectors: dict[str, str] = subclass_config["selectors"]
        group_value_to_subclass = selectors_to_group_value_map(selectors)
        print(group_col, group_value_to_subclass)
        return

    print(
        f"ERROR: no run with run_includes_subclasses=True found in {args.scenario_config}",
        file=sys.stderr,
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
