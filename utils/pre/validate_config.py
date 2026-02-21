"""Validate Justfile configuration against scenario YAML values.

Compares expected values (passed as CLI args from Just variables) against the
canonical scenario YAML.  Run 1 is used for most fields; run 2 (first supply
run) is used for the Cambium path since delivery runs use zero_marginal_costs.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario-config", required=True, type=Path)
    parser.add_argument("--state", required=True)
    parser.add_argument("--utility", required=True)
    parser.add_argument("--upgrade", required=True)
    parser.add_argument("--year", required=True)
    parser.add_argument("--path-td-mc", required=True)
    parser.add_argument("--path-cambium", required=True)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    with open(args.scenario_config) as f:
        runs = yaml.safe_load(f)["runs"]

    run1 = runs[1]
    run2 = runs.get(2)

    checks: list[tuple[str, str, str]] = [
        ("state", args.state, str(run1.get("state", ""))),
        ("utility", args.utility, str(run1.get("utility", ""))),
        ("upgrade", args.upgrade, str(run1.get("upgrade", "")).zfill(2)),
        ("year", args.year, str(run1.get("year_run", ""))),
        ("path_td_mc", args.path_td_mc, str(run1.get("path_td_marginal_costs", ""))),
    ]

    if run2:
        checks.append(
            (
                "path_cambium",
                args.path_cambium,
                str(run2.get("path_cambium_marginal_costs", "")),
            )
        )

    mismatches = []
    for name, expected, actual in checks:
        if expected != actual:
            mismatches.append(f"  {name}: justfile={expected!r}  yaml={actual!r}")

    if mismatches:
        banner = "\U0001f6a8" * 3  # 🚨🚨🚨
        print(f"\n{banner}  CONFIG MISMATCH  {banner}", file=sys.stderr)
        print("Justfile variables do not match scenario YAML:", file=sys.stderr)
        for m in mismatches:
            print(m, file=sys.stderr)
        print(file=sys.stderr)
        if args.strict:
            sys.exit(1)
    else:
        print("\u2705 validate-config: all checks passed", file=sys.stderr)


if __name__ == "__main__":
    main()
