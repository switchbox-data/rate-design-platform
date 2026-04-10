"""Return success when a scenario config contains the requested run number."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


def scenario_has_run_num(scenario_config: Path, run_num: int) -> bool:
    with scenario_config.open(encoding="utf-8") as f:
        runs: dict[int | str, dict[str, object]] = yaml.safe_load(f)["runs"]
    return run_num in runs or str(run_num) in runs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario_config", type=Path)
    parser.add_argument("run_num", type=int)
    args = parser.parse_args()

    if scenario_has_run_num(args.scenario_config, args.run_num):
        sys.exit(0)
    sys.exit(1)


if __name__ == "__main__":
    main()
