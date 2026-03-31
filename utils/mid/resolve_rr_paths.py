"""Resolve base and differentiated RR YAML paths from a scenario config.

Reads run 1's utility_revenue_requirement as the base RR, and finds the first
subclass run's utility_revenue_requirement as the differentiated RR.  Prints
both paths on a single line, space-separated, for consumption by the Justfile.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario_config", type=Path)
    args = parser.parse_args()

    with args.scenario_config.open(encoding="utf-8") as f:
        runs = yaml.safe_load(f)["runs"]

    config_dir = args.scenario_config.resolve().parent.parent

    base = config_dir / runs[1]["utility_revenue_requirement"]

    diff = None
    for num in sorted(runs):
        if runs[num].get("run_includes_subclasses"):
            diff = config_dir / runs[num]["utility_revenue_requirement"]
            break

    if diff is None:
        print(
            f"ERROR: no subclass run found in {args.scenario_config}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(base, diff)


if __name__ == "__main__":
    main()
