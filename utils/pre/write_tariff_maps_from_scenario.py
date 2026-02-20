"""Write electric tariff map CSVs for all runs in a scenario YAML.

Pre-step: run this before run_scenario to generate the tariff_maps/electric CSVs
that each run references via ``path_tariff_maps_electric``.

For each run the script:
1. Loads bldg_ids for the utility from ``path_utility_assignment`` (Polars, no cairo).
2. Loads ``postprocess_group.has_hp`` from ``path_resstock_metadata``, filtered to
   those bldg_ids.
3. Calls ``generate_tariff_map_from_scenario_keys`` with the run's
   ``path_tariffs_electric`` dict to assign a tariff_key per building.
4. Writes the result CSV to ``config_dir / run["path_tariff_maps_electric"]``.

Usage::

    uv run python utils/pre/write_tariff_maps_from_scenario.py \\
        --scenario-config rate_design/ri/hp_rates/config/scenarios/scenarios_rie.yaml
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, cast

import polars as pl
import yaml

from utils import get_aws_region
from utils.pre.electric_tariff_mapper import generate_tariff_map_from_scenario_keys


def _storage_options(path: str) -> dict[str, str] | None:
    return {"aws_region": get_aws_region()} if path.startswith("s3://") else None


def _scan(path: str) -> pl.LazyFrame:
    opts = _storage_options(path)
    return pl.scan_parquet(path, storage_options=opts) if opts else pl.scan_parquet(path)


def _process_run(
    run_id: int | str,
    run: dict[str, Any],
    config_dir: Path,
) -> None:
    missing = [
        k
        for k in ("path_tariffs_electric", "path_tariff_maps_electric", "utility",
                  "path_utility_assignment", "path_resstock_metadata")
        if k not in run
    ]
    if missing:
        raise KeyError(f"Run {run_id}: missing required keys {missing}")

    path_tariffs_electric: dict[str, str] = run["path_tariffs_electric"]
    if not isinstance(path_tariffs_electric, dict):
        raise TypeError(
            f"Run {run_id}: path_tariffs_electric must be a dict; "
            f"got {type(path_tariffs_electric).__name__}"
        )

    utility: str = run["utility"]

    def _resolve(val: str) -> str:
        """Return val unchanged if s3 URL, else resolve relative to config_dir."""
        if val.startswith("s3://"):
            return val
        p = Path(val)
        return str(p if p.is_absolute() else config_dir / p)

    path_ua = _resolve(run["path_utility_assignment"])
    path_meta = _resolve(run["path_resstock_metadata"])

    # bldg_ids for this utility
    bldg_ids = (
        _scan(path_ua)
        .filter(pl.col("sb.electric_utility") == utility)
        .select("bldg_id")
    )

    # metadata filtered to utility buildings
    bldg_data = cast(
        pl.DataFrame,
        _scan(path_meta)
        .select("bldg_id", "postprocess_group.has_hp")
        .join(bldg_ids, on="bldg_id")
        .collect(),
    )

    if bldg_data.is_empty():
        raise ValueError(
            f"Run {run_id}: no buildings found for utility '{utility}' "
            f"in {path_ua}"
        )

    result = generate_tariff_map_from_scenario_keys(path_tariffs_electric, bldg_data)

    out_path = config_dir / run["path_tariff_maps_electric"]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.write_csv(out_path)
    print(f"  run {run_id}: wrote {out_path} ({len(result)} rows)")


def main(scenario_config: Path) -> None:
    config_dir = scenario_config.resolve().parent

    with scenario_config.open(encoding="utf-8") as fh:
        scenario = yaml.safe_load(fh)

    runs: dict[int | str, dict[str, Any]] = scenario.get("runs", {})
    if not runs:
        print(f"No runs found in {scenario_config}", file=sys.stderr)
        sys.exit(1)

    print(f"Writing tariff maps for {len(runs)} run(s) in {scenario_config}")
    errors: list[tuple[int | str, Exception]] = []
    for run_id, run in runs.items():
        try:
            _process_run(run_id, run, config_dir)
        except Exception as exc:  # noqa: BLE001
            print(f"  run {run_id}: ERROR — {exc}", file=sys.stderr)
            errors.append((run_id, exc))

    if errors:
        print(f"\n{len(errors)} run(s) failed.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Write electric tariff map CSVs for all runs in a scenario YAML."
    )
    parser.add_argument(
        "--scenario-config",
        required=True,
        help="Path to scenario YAML (e.g. config/scenarios/scenarios_rie.yaml)",
    )
    args = parser.parse_args()
    main(Path(args.scenario_config))
