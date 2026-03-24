"""Print one calendar year per line for each run year in an adoption config YAML.

Respects ``run_years`` when present; otherwise uses all ``year_labels``.
Snaps ``run_years`` entries to the nearest ``year_label`` (matching the logic
in ``materialize_mixed_upgrade`` and ``generate_adoption_scenario_yamls``).

Usage::

    uv run python utils/pre/list_adoption_years.py path/to/config.yaml

Exit code 0; prints one integer per line to stdout.  Supersedes
``count_adoption_years.py`` — callers get the actual years and the count
implicitly via ``${#year_list[@]}`` in bash.
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import yaml


def list_run_years(config: dict) -> list[int]:
    """Return the ordered list of calendar years to run, honouring ``run_years``."""
    year_labels: list[int] = [int(y) for y in config.get("year_labels", [])]
    run_years_raw: list[int] | None = config.get("run_years")

    if run_years_raw is None:
        return year_labels

    result: list[int] = []
    for yr in run_years_raw:
        distances = [abs(yl - int(yr)) for yl in year_labels]
        nearest_idx = int(np.argmin(distances))
        nearest_year = year_labels[nearest_idx]
        if nearest_year != int(yr):
            warnings.warn(
                f"run_years entry {yr} not in year_labels; "
                f"snapping to {nearest_year} (index {nearest_idx})",
                stacklevel=2,
            )
        result.append(nearest_year)
    return result


def main(args: list[str] | None = None) -> None:
    argv = args if args is not None else sys.argv[1:]
    if not argv:
        print("usage: list_adoption_years.py <adoption-config.yaml>", file=sys.stderr)
        sys.exit(1)
    path = Path(argv[0])
    with path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    for year in list_run_years(cfg):
        print(year)


if __name__ == "__main__":
    main()
