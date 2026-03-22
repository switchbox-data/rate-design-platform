"""Materialize per-year ResStock data for mixed-upgrade HP adoption trajectories.

Reads an adoption config YAML (scenario fractions per upgrade per year), assigns
buildings to upgrades using a monotonic random-seed allocation, and writes one
directory per run year containing:

- ``metadata-sb.parquet``: combined metadata rows from the assigned upgrades.
- ``loads/``: directory of symlinks pointing each building to the correct
  upgrade's load parquet (``{bldg_id}-{N}.parquet``).

The output mirrors the layout that ``run_scenario.py`` already expects for a
single-upgrade run, so no changes are needed to the scenario runner.

Building assignment algorithm
------------------------------
Buildings are shuffled once using the adoption config's ``random_seed``.  Each
upgrade is pre-allocated a contiguous band of slots in the shuffled order (based
on its maximum fraction across all years, which is the last year's fraction since
fractions are non-decreasing).  At year *t*, the first ``int(N × f[u][t])``
buildings in upgrade *u*'s band are assigned to that upgrade; the rest remain at
upgrade 0 (baseline).  This guarantees:

- No building is assigned to more than one upgrade at a time.
- Once a building adopts an upgrade, it never reverts (monotonicity).
- The total assigned fraction never exceeds 1.0 (enforced by ``validate_scenario``).

Usage
-----
::

    uv run python utils/pre/materialize_mixed_upgrade.py \\
        --state ri \\
        --utility rie \\
        --adoption-config rate_design/hp_rates/ny/config/adoption/nyca_electrification.yaml \\
        --path-resstock-release /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \\
        --output-dir /ebs/data/nrel/resstock/res_2024_amy2018_2_sb/adoption/nyca_electrification
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import yaml

from buildstock_fetch.scenarios import validate_scenario


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
        help="Root path of the processed ResStock _sb release (local or s3://).",
    )
    p.add_argument(
        "--output-dir",
        required=True,
        metavar="PATH",
        dest="path_output_dir",
        help="Directory to write per-year materialized data.",
    )
    return p


# ---------------------------------------------------------------------------
# Adoption config helpers
# ---------------------------------------------------------------------------


def _load_adoption_config(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _parse_adoption_config(
    config: dict[str, Any],
) -> tuple[str, int, dict[int, list[float]], list[int], list[int]]:
    """Parse and return core fields from the adoption config.

    Returns:
        (scenario_name, random_seed, scenario, year_labels, run_year_indices)
        where ``run_year_indices`` are the indices into ``year_labels`` that
        correspond to the years that should be materialized.
    """
    scenario_name: str = config["scenario_name"]
    random_seed: int = int(config.get("random_seed", 42))

    # Keys may come from YAML as integers or strings; normalise to int.
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


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def _upgrade_dir_name(upgrade_id: int) -> str:
    return f"upgrade={upgrade_id:02d}"


def _metadata_path(
    path_resstock_release: Path, state_upper: str, upgrade_id: int
) -> Path:
    return (
        path_resstock_release
        / "metadata"
        / f"state={state_upper}"
        / _upgrade_dir_name(upgrade_id)
        / "metadata-sb.parquet"
    )


def _loads_dir(path_resstock_release: Path, state_upper: str, upgrade_id: int) -> Path:
    return (
        path_resstock_release
        / "load_curve_hourly"
        / f"state={state_upper}"
        / _upgrade_dir_name(upgrade_id)
    )


def _check_upgrade_paths(
    path_resstock_release: Path,
    state_upper: str,
    upgrade_ids: list[int],
) -> None:
    """Raise FileNotFoundError listing all missing upgrade metadata paths."""
    missing: list[str] = []
    for uid in upgrade_ids:
        p = _metadata_path(path_resstock_release, state_upper, uid)
        if not p.exists():
            missing.append(str(p))
    if missing:
        raise FileNotFoundError(
            "Missing required upgrade metadata files:\n" + "\n".join(missing)
        )


def _check_loads_dirs(
    path_resstock_release: Path,
    state_upper: str,
    upgrade_ids: list[int],
) -> None:
    """Raise FileNotFoundError listing all missing loads directories."""
    missing: list[str] = []
    for uid in upgrade_ids:
        d = _loads_dir(path_resstock_release, state_upper, uid)
        if not d.is_dir():
            missing.append(str(d))
    if missing:
        raise FileNotFoundError(
            "Missing required loads directories:\n" + "\n".join(missing)
        )


# ---------------------------------------------------------------------------
# Building assignment
# ---------------------------------------------------------------------------


def assign_buildings(
    eligible_bldg_ids: list[int],
    scenario: dict[int, list[float]],
    run_year_indices: list[int],
    random_seed: int,
) -> dict[int, dict[int, int]]:
    """Assign buildings to upgrades per run-year index.

    Only buildings that do **not** already have a heat pump should be passed
    via ``eligible_bldg_ids``.  Buildings already at HP in the baseline are
    excluded upstream and kept pinned to upgrade 0 in all years.

    Args:
        eligible_bldg_ids: Building IDs eligible for HP adoption (i.e. those
            whose ``postprocess_group.has_hp`` is not True in upgrade-0 metadata).
        scenario: Dict mapping upgrade_id → per-year cumulative adoption fractions.
            Fractions are relative to the *total* building population, so the
            caller is responsible for passing a proportionally correct subset.
        run_year_indices: Indices into the scenario lists to materialise.
        random_seed: Seed for reproducible shuffling.

    Returns:
        ``{year_index: {bldg_id: upgrade_id}}`` — upgrade 0 means "baseline".
        Only covers ``eligible_bldg_ids``; already-HP buildings are not included.
    """
    n_bldgs = len(eligible_bldg_ids)
    if n_bldgs == 0:
        return {t: {} for t in run_year_indices}

    rng = np.random.default_rng(random_seed)
    bldg_array = np.array(sorted(eligible_bldg_ids), dtype=np.int64)
    rng.shuffle(bldg_array)

    upgrades_sorted = sorted(scenario.keys())
    num_years = len(next(iter(scenario.values())))
    last_t = num_years - 1

    # Pre-allocate contiguous slot ranges using the last year's fractions
    # (max fractions since they are non-decreasing).  Slots don't overlap,
    # and since total adoption <= 1.0 the ranges all fit within [0, N).
    upgrade_offsets: dict[int, int] = {}
    cumulative_offset = 0
    for u in upgrades_sorted:
        upgrade_offsets[u] = cumulative_offset
        max_count = int(n_bldgs * scenario[u][last_t])
        cumulative_offset += max_count

    result: dict[int, dict[int, int]] = {}
    for t in run_year_indices:
        assignments: dict[int, int] = {int(bid): 0 for bid in bldg_array}
        for u in upgrades_sorted:
            count_t = int(n_bldgs * scenario[u][t])
            offset = upgrade_offsets[u]
            for i in range(count_t):
                assignments[int(bldg_array[offset + i])] = u
        result[t] = assignments

    return result


# ---------------------------------------------------------------------------
# Load-file discovery
# ---------------------------------------------------------------------------


def _build_load_file_map(loads_dir: Path, bldg_ids: set[int]) -> dict[int, Path]:
    """Scan ``loads_dir`` and return ``{bldg_id: path}`` for each matching building.

    Files are expected to be named ``{bldg_id}-{something}.parquet``.  Unmatched
    files and files whose bldg_id is not in ``bldg_ids`` are silently skipped.
    """
    result: dict[int, Path] = {}
    for f in loads_dir.glob("*.parquet"):
        parts = f.stem.split("-", maxsplit=1)
        if not parts:
            continue
        try:
            bldg_id = int(parts[0])
        except ValueError:
            continue
        if bldg_ids and bldg_id not in bldg_ids:
            continue
        result[bldg_id] = f
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    path_adoption_config = Path(args.path_adoption_config)
    path_resstock_release = Path(args.path_resstock_release)
    path_output_dir = Path(args.path_output_dir)
    state_upper = args.state.upper()

    # 1. Load and validate adoption config.
    config = _load_adoption_config(path_adoption_config)
    scenario_name, random_seed, scenario, year_labels, run_year_indices = (
        _parse_adoption_config(config)
    )
    validate_scenario(scenario)

    non_baseline_upgrades = sorted(scenario.keys())
    all_upgrades = sorted({0} | set(non_baseline_upgrades))

    print(
        f"Materialising '{scenario_name}' for state={state_upper}, "
        f"utility={args.utility}"
    )
    print(
        f"  upgrades: {all_upgrades}  |  "
        f"years: {[year_labels[t] for t in run_year_indices]}"
    )

    # 2. Verify all required upgrade directories exist.
    _check_upgrade_paths(path_resstock_release, state_upper, all_upgrades)
    _check_loads_dirs(path_resstock_release, state_upper, all_upgrades)

    # 3. Load baseline metadata; split into HP-eligible and already-HP buildings.
    baseline_meta_path = _metadata_path(path_resstock_release, state_upper, 0)
    baseline_df = pl.read_parquet(baseline_meta_path)
    all_bldg_ids: list[int] = baseline_df["bldg_id"].to_list()

    # Buildings that already heat with a heat pump in the baseline must NOT be
    # re-assigned — they are pinned to upgrade 0 in every year.
    has_hp_col = "postprocess_group.has_hp"
    if has_hp_col in baseline_df.columns:
        already_hp_mask = baseline_df[has_hp_col] == True  # noqa: E712
        already_hp_bldg_ids: list[int] = baseline_df.filter(already_hp_mask)[
            "bldg_id"
        ].to_list()
        eligible_bldg_ids: list[int] = baseline_df.filter(~already_hp_mask)[
            "bldg_id"
        ].to_list()
    else:
        already_hp_bldg_ids = []
        eligible_bldg_ids = all_bldg_ids

    print(
        f"  total buildings (upgrade 0): {len(all_bldg_ids)} "
        f"({len(eligible_bldg_ids)} HP-eligible, "
        f"{len(already_hp_bldg_ids)} already have HP → kept at upgrade 0)"
    )

    # 4. Assign only eligible buildings to upgrades per run year.
    eligible_assignments_by_year = assign_buildings(
        eligible_bldg_ids, scenario, run_year_indices, random_seed
    )

    # Merge already-HP buildings back in (pinned to upgrade 0 in all years).
    already_hp_baseline: dict[int, int] = {bid: 0 for bid in already_hp_bldg_ids}
    assignments_by_year: dict[int, dict[int, int]] = {
        t: {**eligible_assignments_by_year[t], **already_hp_baseline}
        for t in run_year_indices
    }

    # 5. Load all upgrade metadata DataFrames (indexed by bldg_id for fast lookup).
    upgrade_dfs: dict[int, pl.DataFrame] = {0: baseline_df}
    for uid in non_baseline_upgrades:
        upgrade_dfs[uid] = pl.read_parquet(
            _metadata_path(path_resstock_release, state_upper, uid)
        )

    path_output_dir.mkdir(parents=True, exist_ok=True)

    all_year_data: list[tuple[int, dict[int, int]]] = []

    # 6. For each run year, write materialized metadata and load symlinks.
    for t in run_year_indices:
        calendar_year = year_labels[t]
        year_dir = path_output_dir / f"year={calendar_year}"
        year_dir.mkdir(parents=True, exist_ok=True)

        assignments = assignments_by_year[t]

        # Group buildings by their assigned upgrade for this year.
        bldgs_by_upgrade: dict[int, list[int]] = {u: [] for u in all_upgrades}
        for bldg_id, upgrade_id in assignments.items():
            bldgs_by_upgrade[upgrade_id].append(bldg_id)

        # Combine metadata from each upgrade, filtering to its assigned buildings.
        parts: list[pl.DataFrame] = []
        for uid in all_upgrades:
            bldg_ids_for_upgrade = bldgs_by_upgrade[uid]
            if not bldg_ids_for_upgrade:
                continue
            df = upgrade_dfs[uid].filter(pl.col("bldg_id").is_in(bldg_ids_for_upgrade))
            parts.append(df)

        combined = pl.concat(parts)
        combined.write_parquet(year_dir / "metadata-sb.parquet")

        # Create loads/ directory with symlinks per building.
        loads_out_dir = year_dir / "loads"
        loads_out_dir.mkdir(exist_ok=True)

        for uid in all_upgrades:
            bldg_ids_for_upgrade = bldgs_by_upgrade[uid]
            if not bldg_ids_for_upgrade:
                continue
            src_loads_dir = _loads_dir(path_resstock_release, state_upper, uid)
            bldg_ids_set = set(bldg_ids_for_upgrade)
            load_map = _build_load_file_map(src_loads_dir, bldg_ids_set)

            for bldg_id in bldg_ids_for_upgrade:
                src_file = load_map.get(bldg_id)
                if src_file is None:
                    raise FileNotFoundError(
                        f"No load file found for bldg_id={bldg_id} in {src_loads_dir}"
                    )
                dst = loads_out_dir / src_file.name
                if dst.is_symlink() or dst.exists():
                    dst.unlink()
                os.symlink(src_file.resolve(), dst)

        n_assigned = sum(len(v) for v in bldgs_by_upgrade.values())
        n_hp = n_assigned - len(bldgs_by_upgrade[0])
        print(
            f"  year={calendar_year}: {n_assigned} buildings "
            f"({n_hp} HP-upgraded, {len(bldgs_by_upgrade[0])} baseline)"
        )
        all_year_data.append((calendar_year, assignments))

    # 7. Write scenario CSV (bldg_id, year_<YYYY>, ...) for reference.
    csv_path = path_output_dir / "scenario_assignments.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["bldg_id"] + [f"year_{yr}" for yr, _ in all_year_data]
        writer.writerow(header)
        for bldg_id in sorted(all_bldg_ids):
            row: list[object] = [bldg_id] + [asgn[bldg_id] for _, asgn in all_year_data]
            writer.writerow(row)

    print(f"Wrote scenario assignments to {csv_path}")
    print(f"Done. Materialised {len(run_year_indices)} year(s) to {path_output_dir}")


if __name__ == "__main__":
    sys.exit(main())
