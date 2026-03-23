"""Materialize per-year ResStock data for mixed-upgrade HP adoption trajectories."""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from utils.buildstock import (
    SbMixedUpgradeScenario,
    _build_load_file_map as _buildstock_load_file_map,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Materialize per-year mixed-upgrade ResStock data for adoption trajectories.",
    )
    p.add_argument(
        "--state", required=True, help="Two-letter state abbreviation (e.g. ny, ri)."
    )
    p.add_argument("--utility", required=True, help="Utility slug (e.g. rie, nyseg).")
    p.add_argument(
        "--adoption-config",
        required=True,
        metavar="PATH",
        dest="path_adoption_config",
        help="Path to adoption trajectory YAML.",
    )
    p.add_argument(
        "--path-resstock-release",
        required=True,
        help="ResStock release path or root path containing the release.",
    )
    p.add_argument(
        "--release",
        required=False,
        help="Optional release directory name under --path-resstock-release.",
    )
    p.add_argument(
        "--output-dir",
        required=True,
        metavar="PATH",
        dest="path_output_dir",
        help="Directory to write per-year materialized data.",
    )
    return p


def _load_adoption_config(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _parse_adoption_config(
    config: dict[str, Any],
) -> tuple[str, int, dict[int, list[float]], list[int], list[int]]:
    """Parse and return core fields from the adoption config."""
    scenario_name: str = config["scenario_name"]
    random_seed: int = int(config.get("random_seed", 42))

    scenario_raw: dict[Any, list[float]] = config["scenario"]
    scenario: dict[int, list[float]] = {
        int(k): [float(v) for v in vals] for k, vals in scenario_raw.items()
    }

    year_labels: list[int] = [int(y) for y in config["year_labels"]]

    run_years_raw: list[int] | None = config.get("run_years")
    if run_years_raw is None:
        run_year_indices = list(range(len(year_labels)))
    else:
        run_year_indices = []
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
            run_year_indices.append(nearest_idx)

    return scenario_name, random_seed, scenario, year_labels, run_year_indices


def _build_load_file_map(loads_dir: Path, bldg_ids: set[int]) -> dict[int, Path]:
    """Compatibility shim re-exported for existing tests/imports."""
    return _buildstock_load_file_map(loads_dir, bldg_ids)


def assign_buildings(
    bldg_ids: list[int],
    scenario: dict[int, list[float]],
    run_year_indices: list[int],
    random_seed: int,
    applicable_bldg_ids_per_upgrade: dict[int, set[int]] | None = None,
) -> dict[int, dict[int, int]]:
    """Assign buildings to upgrades by year, preserving monotonic adoption."""
    assignments = {t: {} for t in run_year_indices}
    if not bldg_ids:
        return assignments

    shuffled = np.array(sorted(bldg_ids), dtype=int)
    np.random.default_rng(random_seed).shuffle(shuffled)
    shuffled_ids = shuffled.tolist()

    upgrade_order = list(scenario.keys())
    upgrade_allocations: dict[int, list[int]] = {uid: [] for uid in upgrade_order}

    if applicable_bldg_ids_per_upgrade is None:
        for t in run_year_indices:
            assigned_any = set().union(
                *(
                    set(upgrade_allocations[uid])
                    for uid in upgrade_order
                    if upgrade_allocations[uid]
                )
            )
            for uid in upgrade_order:
                target = int(len(bldg_ids) * scenario[uid][t])
                current = len(upgrade_allocations[uid])
                needed = max(0, target - current)
                if needed == 0:
                    continue
                available = [bid for bid in shuffled_ids if bid not in assigned_any]
                take = available[:needed]
                upgrade_allocations[uid].extend(take)
                assigned_any.update(take)
                if len(take) < needed:
                    warnings.warn(
                        f"Upgrade {uid}: target {target} buildings but only "
                        f"{len(available)} available; capping at {current + len(take)}.",
                        stacklevel=2,
                    )
    else:
        filtered_pools: dict[int, list[int]] = {}
        for uid in upgrade_order:
            applicable = applicable_bldg_ids_per_upgrade.get(uid, set())
            # Keep per-upgrade pools independent. If pools overlap, final
            # assignment below resolves conflicts by iteration order.
            filtered_pools[uid] = [bid for bid in shuffled_ids if bid in applicable]

        for t in run_year_indices:
            for uid in upgrade_order:
                target = int(len(bldg_ids) * scenario[uid][t])
                current = len(upgrade_allocations[uid])
                if target <= current:
                    continue
                pool = filtered_pools[uid]
                if target > len(pool):
                    warnings.warn(
                        f"Upgrade {uid}: target {target} buildings but only "
                        f"{len(pool)} are applicable; capping at {len(pool)}.",
                        stacklevel=2,
                    )
                    target = len(pool)
                upgrade_allocations[uid].extend(pool[current:target])

    all_ids = set(bldg_ids)
    for t in run_year_indices:
        year_map = {bid: 0 for bid in bldg_ids}
        for uid in upgrade_order:
            target = int(len(bldg_ids) * scenario[uid][t])
            for bid in upgrade_allocations[uid][:target]:
                year_map[bid] = uid
        assigned = set(bid for bid, uid in year_map.items() if uid != 0)
        if not assigned.issubset(all_ids):
            raise ValueError(
                "Internal assignment bug: assigned building outside input set"
            )
        assignments[t] = year_map
    return assignments


def _resolve_release_path(path_resstock_release: Path, release: str | None) -> Path:
    """Resolve the on-disk release directory, preferring `_sb` when available."""
    if release:
        if path_resstock_release.name in {release, f"{release}_sb"}:
            return path_resstock_release
        candidate_sb = path_resstock_release / f"{release}_sb"
        if candidate_sb.exists():
            return candidate_sb
        candidate = path_resstock_release / release
        if candidate.exists():
            return candidate
        return candidate_sb

    # No explicit release: use the provided path as-is.
    return path_resstock_release


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    path_adoption_config = Path(args.path_adoption_config)
    path_output_dir = Path(args.path_output_dir)
    state = args.state.lower()
    path_release = _resolve_release_path(
        Path(args.path_resstock_release), getattr(args, "release", None)
    )

    config = _load_adoption_config(path_adoption_config)
    scenario_name, random_seed, scenario, year_labels, run_year_indices = (
        _parse_adoption_config(config)
    )

    print(
        f"Materialising '{scenario_name}' for state={state.upper()}, utility={args.utility}"
    )
    print(
        f"  upgrades: {[0, *sorted(scenario.keys())]}  |  "
        f"years: {[year_labels[t] for t in run_year_indices]}"
    )

    mixed = SbMixedUpgradeScenario(
        path_resstock_release=path_release,
        state=state,
        scenario_name=scenario_name,
        scenario=scenario,
        random_seed=random_seed,
        year_labels=year_labels,
        run_year_indices=run_year_indices,
    )
    assignments = mixed.build_assignments(assign_buildings)
    mixed.materialize(path_output_dir=path_output_dir, assignments=assignments)
    mixed.export_scenario_csv(path_output_dir=path_output_dir, assignments=assignments)

    print(f"Done. Materialised {len(run_year_indices)} year(s) to {path_output_dir}")


if __name__ == "__main__":
    sys.exit(main())
