"""Resolve base and differentiated RR YAML paths from a scenario config.

<<<<<<< HEAD
Reads run 1's utility_revenue_requirement as the base RR, and finds the first
subclass run's utility_revenue_requirement as the differentiated RR.  Prints
both paths on a single line, space-separated, for consumption by the Justfile.
=======
Reads run 1's utility_revenue_requirement as the base RR and resolves the
subclass run's utility_revenue_requirement as the differentiated RR.

By default, the differentiated RR comes from the first run with
``run_includes_subclasses=True``.  Callers may instead pass
``--subclass-run-num`` to resolve a specific subclass run, which is what the NY
electric-heating track needs for runs 29-32.
>>>>>>> main
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


<<<<<<< HEAD
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
=======
def _get_run(
    runs: dict[int | str, dict[str, object]],
    run_num: int,
) -> dict[str, object]:
    run = runs.get(run_num) or runs.get(str(run_num))
    if run is None:
        raise ValueError(f"run {run_num} not found in scenario config")
    return run


def resolve_rr_paths(
    scenario_config: Path,
    *,
    subclass_run_num: int | None = None,
) -> tuple[Path, Path]:
    with scenario_config.open(encoding="utf-8") as f:
        runs: dict[int | str, dict[str, object]] = yaml.safe_load(f)["runs"]

    config_dir = scenario_config.resolve().parent.parent
    base_run = _get_run(runs, 1)
    base = config_dir / str(base_run["utility_revenue_requirement"])

    if subclass_run_num is not None:
        subclass_run = _get_run(runs, subclass_run_num)
        if not subclass_run.get("run_includes_subclasses"):
            raise ValueError(
                f"run {subclass_run_num} is not a subclass run in {scenario_config}"
            )
        diff = config_dir / str(subclass_run["utility_revenue_requirement"])
        return base, diff

    for num in sorted(runs, key=lambda value: int(value)):
        run = runs[num]
        if run.get("run_includes_subclasses"):
            diff = config_dir / str(run["utility_revenue_requirement"])
            return base, diff

    raise ValueError(f"no subclass run found in {scenario_config}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario_config", type=Path)
    parser.add_argument(
        "--subclass-run-num",
        type=int,
        help=(
            "Optional explicit subclass run number to resolve the differentiated "
            "RR path from."
        ),
    )
    args = parser.parse_args()

    try:
        base, diff = resolve_rr_paths(
            args.scenario_config,
            subclass_run_num=args.subclass_run_num,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
>>>>>>> main
        sys.exit(1)

    print(base, diff)


if __name__ == "__main__":
    main()
