"""Generate per-year scenario YAML entries for mixed-upgrade adoption runs.

Reads a base scenario YAML, extracts selected run configs, and emits a new
YAML file (``scenarios_<utility>_adoption.yaml``) with one entry per
(year × run) combination.  The per-year ``path_resstock_metadata`` and
``path_resstock_loads`` are rewritten to point at the materialized data
produced by ``materialize_mixed_upgrade.py``.  ``year_run`` and all path
strings containing ``year={old_year_run}`` are also updated to the calendar
year for each generated entry.

Run keys in the output YAML use the scheme ``(year_index + 1) * 100 + run_num``:

- Year index 0 (first run year), base run 1  → key 101
- Year index 0,                  base run 2  → key 102
- Year index 1 (second run year), base run 1 → key 201
- Year index 1,                  base run 2  → key 202
- …

This ensures run keys are unique across (year, run) combinations and
memorable when passed to ``run-adoption-scenario``.

Usage
-----
::

    uv run python utils/pre/generate_adoption_scenario_yamls.py \\
        --base-scenario rate_design/hp_rates/ri/config/scenarios/scenarios_rie.yaml \\
        --runs 1,2,5,6 \\
        --adoption-config rate_design/hp_rates/ny/config/adoption/nyca_electrification.yaml \\
        --materialized-dir /ebs/data/nrel/resstock/res_2024_amy2018_2_sb/adoption/nyca_electrification \\
        --output rate_design/hp_rates/ri/config/scenarios/scenarios_rie_adoption.yaml
"""

from __future__ import annotations

import argparse
import copy
import sys
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import yaml


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generate per-year scenario YAMLs for mixed-upgrade adoption runs.",
    )
    p.add_argument(
        "--base-scenario",
        required=True,
        metavar="PATH",
        dest="path_base_scenario",
        help="Existing scenario YAML to use as the run config template.",
    )
    p.add_argument(
        "--runs",
        required=True,
        help="Comma-separated run numbers to include (e.g. 1,2,5,6).",
    )
    p.add_argument(
        "--adoption-config",
        required=True,
        metavar="PATH",
        dest="path_adoption_config",
        help="Path to adoption trajectory YAML (for year_labels and scenario_name).",
    )
    p.add_argument(
        "--materialized-dir",
        required=True,
        metavar="PATH",
        dest="path_materialized_dir",
        help="Root of materialized per-year data (output of materialize_mixed_upgrade).",
    )
    p.add_argument(
        "--output",
        required=True,
        metavar="PATH",
        dest="path_output",
        help="Path to write the generated adoption scenario YAML.",
    )
    return p


# ---------------------------------------------------------------------------
# Adoption config helpers (mirrors materialize_mixed_upgrade logic)
# ---------------------------------------------------------------------------


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _resolve_run_years(config: dict[str, Any]) -> list[tuple[int, int]]:
    """Return ``[(year_index, calendar_year), ...]`` to generate entries for.

    Uses ``run_years`` from the config when present; otherwise uses all
    ``year_labels``.  Snaps run_years entries to the nearest year_label when
    an exact match is not found.
    """
    year_labels: list[int] = [int(y) for y in config["year_labels"]]
    run_years_raw: list[int] | None = config.get("run_years")

    if run_years_raw is None:
        return list(enumerate(year_labels))

    result: list[tuple[int, int]] = []
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
        result.append((nearest_idx, nearest_year))
    return result


# ---------------------------------------------------------------------------
# Config transformation helpers
# ---------------------------------------------------------------------------


def _replace_year_in_value(value: Any, old_year: int, new_year: int) -> Any:
    """Recursively replace ``year={old_year}`` with ``year={new_year}`` in strings."""
    if isinstance(value, str):
        return value.replace(f"year={old_year}", f"year={new_year}")
    if isinstance(value, dict):
        return {
            k: _replace_year_in_value(v, old_year, new_year) for k, v in value.items()
        }
    if isinstance(value, list):
        return [_replace_year_in_value(item, old_year, new_year) for item in value]
    return value


def _insert_blank_lines_between_runs(yaml_str: str) -> str:
    """Insert a blank line before run keys 2+, not before the first run key."""
    lines = yaml_str.splitlines()
    out: list[str] = []
    seen_run_key = False
    for line in lines:
        stripped = line.strip()
        is_run_key = (
            line.startswith("  ") and stripped.endswith(":") and stripped[:-1].isdigit()
        )
        if is_run_key and seen_run_key and (not out or out[-1] != ""):
            out.append("")
        if is_run_key:
            seen_run_key = True
        out.append(line)
    return "\n".join(out) + ("\n" if yaml_str.endswith("\n") else "")


def _update_run_name(run_name: str, calendar_year: int) -> str:
    """Append ``_y{year}_mixed`` to a run name (before any trailing double-underscore suffix).

    Examples:
        ``ri_rie_run1_up00_precalc__flat``  →  ``ri_rie_run1_y2025_mixed_precalc__flat``
        ``ny_nyseg_run5_up02_default__tou`` →  ``ny_nyseg_run5_y2025_mixed_default__tou``
    """
    # Locate the first double-underscore which separates the "stem" from the tariff suffix.
    double_us = run_name.find("__")
    year_tag = f"_y{calendar_year}_mixed"
    if double_us == -1:
        return run_name + year_tag
    return run_name[:double_us] + year_tag + run_name[double_us:]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    path_base_scenario = Path(args.path_base_scenario)
    path_adoption_config = Path(args.path_adoption_config)
    path_materialized_dir = Path(args.path_materialized_dir)
    path_output = Path(args.path_output)

    # Parse run numbers.
    try:
        run_nums = [int(r.strip()) for r in args.runs.split(",") if r.strip()]
    except ValueError as exc:
        raise ValueError(
            f"--runs must be comma-separated integers, got: {args.runs!r}"
        ) from exc
    if not run_nums:
        raise ValueError("--runs is empty; at least one run number is required.")

    # 1. Load adoption config for year info.
    adoption_config = _load_yaml(path_adoption_config)
    scenario_name: str = adoption_config["scenario_name"]
    year_run_pairs = _resolve_run_years(adoption_config)

    # 2. Load base scenario YAML and extract requested run configs.
    base_doc = _load_yaml(path_base_scenario)
    base_runs: dict[int, dict[str, Any]] = {
        int(k): v for k, v in base_doc.get("runs", {}).items()
    }

    missing_runs = [r for r in run_nums if r not in base_runs]
    if missing_runs:
        available = sorted(base_runs.keys())
        raise KeyError(
            f"Run(s) {missing_runs} not found in {path_base_scenario}. "
            f"Available runs: {available}"
        )

    print(
        f"Generating adoption scenario YAML for '{scenario_name}': "
        f"{len(year_run_pairs)} year(s) × {len(run_nums)} run(s) = "
        f"{len(year_run_pairs) * len(run_nums)} entries"
    )

    # 3. Build generated run entries.
    output_runs: dict[int, dict[str, Any]] = {}

    for year_index, calendar_year in year_run_pairs:
        meta_path = str(
            path_materialized_dir / f"year={calendar_year}" / "metadata-sb.parquet"
        )
        loads_path = str(path_materialized_dir / f"year={calendar_year}" / "loads" / "")

        for run_num in run_nums:
            base_run = base_runs[run_num]
            old_year_run = int(base_run.get("year_run", calendar_year))

            # Deep-copy so base configs remain unmodified.
            run_entry: dict[str, Any] = copy.deepcopy(base_run)

            # Replace ResStock data paths.
            run_entry["path_resstock_metadata"] = meta_path
            run_entry["path_resstock_loads"] = loads_path

            # Update year_run to the calendar year for this adoption cohort.
            run_entry["year_run"] = calendar_year

            # Replace year= tokens in all string path values so MC data resolves
            # to the correct Cambium year.
            run_entry = _replace_year_in_value(run_entry, old_year_run, calendar_year)

            # Update run_name to include year and mixed tag.
            run_entry["run_name"] = _update_run_name(
                str(base_run.get("run_name", f"run{run_num}")),
                calendar_year,
            )

            output_key = (year_index + 1) * 100 + run_num
            output_runs[output_key] = run_entry
            print(
                f"  [{output_key}] year={calendar_year}, "
                f"base_run={run_num}: {run_entry['run_name']}"
            )

    # 4. Write combined YAML.
    path_output.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {"runs": output_runs}
    yaml_str = yaml.dump(
        payload,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )
    yaml_str = _insert_blank_lines_between_runs(yaml_str)
    path_output.write_text(yaml_str, encoding="utf-8")

    print(f"Wrote {len(output_runs)} run entries to {path_output}")


if __name__ == "__main__":
    sys.exit(main())
