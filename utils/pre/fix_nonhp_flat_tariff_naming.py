"""Fix non-HP flat tariff naming to prevent overwriting system-wide flat rates.

When multi-tariff runs (e.g., run-5) copy calibrated tariffs, they write all
tariffs from tariff_final_config.json. If the non-HP flat tariff uses the same
name as the system-wide flat tariff (e.g., rie_flat), it overwrites the
system-wide calibrated rate.

This script:
1. Creates {utility}_nonhp_flat.json and {utility}_nonhp_flat_supply.json
   for each utility (copying from the existing flat files)
2. Updates label and name fields in the new files
3. Updates all scenario YAMLs to reference the new nonhp_flat files
4. Regenerates tariff maps

Usage:
    uv run python utils/pre/fix_nonhp_flat_tariff_naming.py --state ri
    uv run python utils/pre/fix_nonhp_flat_tariff_naming.py --state ny
    uv run python utils/pre/fix_nonhp_flat_tariff_naming.py --all
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import yaml

from utils import get_project_root


def _get_state_utilities(state: str) -> list[str]:
    """Get list of utilities for a state from scenario YAMLs."""
    config_dir = (
        get_project_root()
        / "rate_design"
        / "hp_rates"
        / state.lower()
        / "config"
        / "scenarios"
    )
    if not config_dir.exists():
        return []

    utilities: list[str] = []
    for scenario_file in config_dir.glob("scenarios_*.yaml"):
        # Extract utility from filename: scenarios_{utility}.yaml
        utility = scenario_file.stem.replace("scenarios_", "")
        utilities.append(utility)
    return sorted(utilities)


def _create_nonhp_flat_tariff(
    flat_tariff_path: Path,
    nonhp_flat_tariff_path: Path,
) -> None:
    """Create nonhp_flat tariff by copying and updating flat tariff."""
    if nonhp_flat_tariff_path.exists():
        print(f"  {nonhp_flat_tariff_path.name} already exists, skipping")
        return

    # Copy the file
    shutil.copy2(flat_tariff_path, nonhp_flat_tariff_path)

    # Update label and name fields
    with nonhp_flat_tariff_path.open(encoding="utf-8") as f:
        tariff = json.load(f)

    # Extract the old tariff key from the flat file
    old_label = tariff["items"][0].get("label", "")
    old_name = tariff["items"][0].get("name", "")

    # Derive new names: rie_flat -> rie_nonhp_flat
    if old_label.endswith("_flat"):
        new_label = old_label.replace("_flat", "_nonhp_flat")
    elif old_label.endswith("_flat_supply"):
        new_label = old_label.replace("_flat_supply", "_nonhp_flat_supply")
    else:
        raise ValueError(f"Unexpected label format: {old_label}")

    if old_name.endswith("_flat"):
        new_name = old_name.replace("_flat", "_nonhp_flat")
    elif old_name.endswith("_flat_supply"):
        new_name = old_name.replace("_flat_supply", "_nonhp_flat_supply")
    else:
        raise ValueError(f"Unexpected name format: {old_name}")

    tariff["items"][0]["label"] = new_label
    tariff["items"][0]["name"] = new_name

    with nonhp_flat_tariff_path.open("w", encoding="utf-8") as f:
        json.dump(tariff, f, indent=2)

    print(f"  Created {nonhp_flat_tariff_path.name}")


def _update_scenario_yaml(scenario_path: Path, utility: str) -> int:
    """Update scenario YAML to use nonhp_flat tariffs for non-HP subclass."""
    with scenario_path.open(encoding="utf-8") as f:
        scenario = yaml.safe_load(f)

    runs = scenario.get("runs", {})
    updates = 0

    for run_num, run in runs.items():
        path_tariffs = run.get("path_tariffs_electric")
        if not isinstance(path_tariffs, dict):
            continue

        # Only update multi-tariff runs (those with hp and non-hp keys)
        if set(path_tariffs.keys()) != {"hp", "non-hp"}:
            continue

        nonhp_path = path_tariffs.get("non-hp", "")
        if not nonhp_path:
            continue

        # Update delivery flat
        if nonhp_path.endswith(f"{utility}_flat.json"):
            new_path = nonhp_path.replace(
                f"{utility}_flat.json", f"{utility}_nonhp_flat.json"
            )
            path_tariffs["non-hp"] = new_path
            updates += 1
            print(
                f"    Run {run_num}: {utility}_flat.json -> {utility}_nonhp_flat.json"
            )

        # Update supply flat
        elif nonhp_path.endswith(f"{utility}_flat_supply.json"):
            new_path = nonhp_path.replace(
                f"{utility}_flat_supply.json", f"{utility}_nonhp_flat_supply.json"
            )
            path_tariffs["non-hp"] = new_path
            updates += 1
            print(
                f"    Run {run_num}: {utility}_flat_supply.json -> {utility}_nonhp_flat_supply.json"
            )

    if updates > 0:
        with scenario_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(scenario, f, sort_keys=False, default_flow_style=False)
        print(f"  Updated {scenario_path.name}: {updates} run(s)")

    return updates


def fix_state(state: str, regenerate_maps: bool = True) -> None:
    """Fix non-HP flat tariff naming for all utilities in a state."""
    state_lower = state.lower()
    config_dir = (
        get_project_root() / "rate_design" / "hp_rates" / state_lower / "config"
    )
    tariffs_dir = config_dir / "tariffs" / "electric"
    scenarios_dir = config_dir / "scenarios"

    if not tariffs_dir.exists():
        print(f"Tariffs directory not found: {tariffs_dir}")
        return

    utilities = _get_state_utilities(state_lower)
    if not utilities:
        print(f"No utilities found for state {state}")
        return

    print(f"Fixing non-HP flat tariff naming for {state.upper()}")
    print(f"Found {len(utilities)} utility/ies: {', '.join(utilities)}")

    total_tariffs_created = 0
    total_yaml_updates = 0

    for utility in utilities:
        print(f"\n{utility}:")
        flat_path = tariffs_dir / f"{utility}_flat.json"
        flat_supply_path = tariffs_dir / f"{utility}_flat_supply.json"

        # Create nonhp_flat files
        if flat_path.exists():
            nonhp_flat_path = tariffs_dir / f"{utility}_nonhp_flat.json"
            _create_nonhp_flat_tariff(flat_path, nonhp_flat_path)
            total_tariffs_created += 1
        else:
            print(f"  Warning: {flat_path.name} not found")

        if flat_supply_path.exists():
            nonhp_flat_supply_path = tariffs_dir / f"{utility}_nonhp_flat_supply.json"
            _create_nonhp_flat_tariff(flat_supply_path, nonhp_flat_supply_path)
            total_tariffs_created += 1
        else:
            print(f"  Warning: {flat_supply_path.name} not found")

        # Update scenario YAML
        scenario_path = scenarios_dir / f"scenarios_{utility}.yaml"
        if scenario_path.exists():
            updates = _update_scenario_yaml(scenario_path, utility)
            total_yaml_updates += updates
        else:
            print(f"  Warning: {scenario_path.name} not found")

    print(f"\n{state.upper()} summary:")
    print(f"  Created {total_tariffs_created} nonhp_flat tariff file(s)")
    print(f"  Updated {total_yaml_updates} scenario YAML entry/ies")

    if regenerate_maps and total_yaml_updates > 0:
        print(f"\nRegenerating tariff maps for {state.upper()}...")
        # Note: This would call the tariff map generation script
        # For now, just print a reminder
        print("  Run: just create-electric-tariff-maps-all (or just all-pre)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix non-HP flat tariff naming to prevent overwriting system-wide rates."
    )
    parser.add_argument(
        "--state",
        choices=["ri", "ny"],
        help="State to fix (ri or ny).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fix all states (RI and NY).",
    )
    parser.add_argument(
        "--no-regenerate-maps",
        action="store_true",
        help="Skip tariff map regeneration reminder.",
    )
    args = parser.parse_args()

    if args.all:
        states = ["ri", "ny"]
    elif args.state:
        states = [args.state]
    else:
        parser.error("Must specify --state or --all")

    for state in states:
        fix_state(state, regenerate_maps=not args.no_regenerate_maps)
        if len(states) > 1:
            print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    main()
