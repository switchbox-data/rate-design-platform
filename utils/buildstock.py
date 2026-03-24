"""Buildstock integration for mixed-upgrade adoption materialization.

This module keeps CAIRO-facing materialization logic in RDP while re-exporting
scenario helpers from `buildstock-fetch`.
"""

from __future__ import annotations

import csv
import os
import warnings
from pathlib import Path
from typing import Callable

import polars as pl
from buildstock_fetch.scenarios import uniform_adoption, validate_scenario

HAS_HP_COL = "postprocess_group.has_hp"
__all__ = ["SbMixedUpgradeScenario", "validate_scenario", "uniform_adoption"]


def _build_load_file_map(loads_dir: Path, bldg_ids: set[int]) -> dict[int, Path]:
    """Return `{bldg_id: parquet_path}` for files present in `loads_dir`."""
    file_map: dict[int, Path] = {}
    if not loads_dir.exists():
        return file_map
    for path in loads_dir.glob("*.parquet"):
        parts = path.stem.split("-", maxsplit=1)
        if len(parts) != 2:
            continue
        if not parts[0].isdigit():
            continue
        bldg_id = int(parts[0])
        if bldg_id in bldg_ids:
            file_map[bldg_id] = path
    return file_map


class SbMixedUpgradeScenario:
    """Materialize mixed-upgrade scenario assignments for CAIRO input layout."""

    def __init__(
        self,
        *,
        path_resstock_release: Path,
        state: str,
        scenario_name: str,
        scenario: dict[int, list[float]],
        random_seed: int,
        year_labels: list[int],
        run_year_indices: list[int],
    ) -> None:
        self.path_resstock_release = path_resstock_release
        self.state = state.upper()
        self.scenario_name = scenario_name
        self.scenario = scenario
        self.random_seed = random_seed
        self.year_labels = year_labels
        self.run_year_indices = run_year_indices
        self._metadata_cache: dict[int, pl.DataFrame] = {}

        validate_scenario(self.scenario)

    def _path_metadata(self, upgrade_id: int) -> Path:
        return (
            self.path_resstock_release
            / "metadata"
            / f"state={self.state}"
            / f"upgrade={upgrade_id:02d}"
            / "metadata-sb.parquet"
        )

    def _path_loads(self, upgrade_id: int) -> Path:
        return (
            self.path_resstock_release
            / "load_curve_hourly"
            / f"state={self.state}"
            / f"upgrade={upgrade_id:02d}"
        )

    def _read_metadata(self, upgrade_id: int) -> pl.DataFrame:
        cached = self._metadata_cache.get(upgrade_id)
        if cached is not None:
            return cached

        path = self._path_metadata(upgrade_id)
        if not path.exists():
            raise FileNotFoundError(
                f"Missing metadata file for upgrade={upgrade_id:02d}: {path}"
            )
        df = pl.read_parquet(path)
        self._metadata_cache[upgrade_id] = df
        return df

    def compute_hp_pools(
        self,
    ) -> tuple[frozenset[int], frozenset[int], dict[int, set[int]]]:
        """Return `(all_ids, eligible_ids, applicable_by_upgrade)`."""
        baseline_df = self._read_metadata(0)
        all_ids = frozenset(baseline_df["bldg_id"].to_list())

        if HAS_HP_COL in baseline_df.columns:
            eligible = frozenset(
                baseline_df.filter(pl.col(HAS_HP_COL) != True)["bldg_id"].to_list()  # noqa: E712
            )
        else:
            eligible = all_ids

        applicable_by_upgrade: dict[int, set[int]] = {}
        for upgrade_id in sorted(self.scenario.keys()):
            upgrade_df = self._read_metadata(upgrade_id)
            if HAS_HP_COL not in upgrade_df.columns:
                warnings.warn(
                    f"Upgrade {upgrade_id}: '{HAS_HP_COL}' missing; using full eligible pool.",
                    stacklevel=2,
                )
                applicable_by_upgrade[upgrade_id] = set(eligible)
                continue
            applicable_ids = set(
                upgrade_df.filter(pl.col(HAS_HP_COL) == True)["bldg_id"].to_list()  # noqa: E712
            )
            applicable_by_upgrade[upgrade_id] = applicable_ids & set(eligible)

        return all_ids, eligible, applicable_by_upgrade

    def build_assignments(
        self,
        assign_buildings: Callable[
            [
                list[int],
                dict[int, list[float]],
                list[int],
                int,
                dict[int, set[int]] | None,
            ],
            dict[int, dict[int, int]],
        ],
    ) -> dict[int, dict[int, int]]:
        """Build year-wise assignments including baseline-pinned buildings."""
        all_ids, eligible_ids, applicable_by_upgrade = self.compute_hp_pools()

        year_indices = list(range(len(self.year_labels)))
        eligible_assignments = assign_buildings(
            sorted(eligible_ids),
            self.scenario,
            year_indices,
            self.random_seed,
            applicable_by_upgrade,
        )

        full_assignments: dict[int, dict[int, int]] = {}
        for year_idx in year_indices:
            full_year = {bldg_id: 0 for bldg_id in all_ids}
            full_year.update(eligible_assignments[year_idx])
            full_assignments[year_idx] = full_year
        return full_assignments

    def materialize(
        self, *, path_output_dir: Path, assignments: dict[int, dict[int, int]]
    ) -> None:
        """Write `metadata-sb.parquet` and load symlinks per run year."""
        path_output_dir.mkdir(parents=True, exist_ok=True)

        upgrades = [0, *sorted(self.scenario.keys())]
        for upgrade_id in upgrades:
            self._read_metadata(upgrade_id)

        for year_idx in self.run_year_indices:
            calendar_year = self.year_labels[year_idx]
            year_dir = path_output_dir / f"year={calendar_year}"
            year_dir.mkdir(parents=True, exist_ok=True)
            # All buildings land in a single upgrade=00 partition so that
            # scan_resstock_loads (hive-partitioned) and CAIRO can share the
            # same base path.  The symlink targets still point at the correct
            # per-building upgrade source files.
            loads_out_dir = (
                year_dir / "load_curve_hourly" / f"state={self.state}" / "upgrade=00"
            )
            loads_out_dir.mkdir(parents=True, exist_ok=True)
            year_map = assignments[year_idx]

            bldgs_by_upgrade: dict[int, set[int]] = {
                upgrade_id: set() for upgrade_id in upgrades
            }
            for bldg_id, upgrade_id in year_map.items():
                bldgs_by_upgrade.setdefault(upgrade_id, set()).add(bldg_id)

            metadata_parts: list[pl.DataFrame] = []
            for upgrade_id in upgrades:
                bldg_ids = bldgs_by_upgrade[upgrade_id]
                if not bldg_ids:
                    continue

                metadata_parts.append(
                    self._metadata_cache[upgrade_id].filter(
                        pl.col("bldg_id").is_in(sorted(bldg_ids))
                    )
                )

                loads_dir = self._path_loads(upgrade_id)
                if not loads_dir.exists():
                    raise FileNotFoundError(
                        f"Missing loads directory for upgrade={upgrade_id:02d}: {loads_dir}"
                    )
                load_map = _build_load_file_map(loads_dir, bldg_ids)
                missing = sorted(bldg_ids - set(load_map.keys()))
                if missing:
                    raise FileNotFoundError(
                        f"Missing load parquet(s) for upgrade={upgrade_id:02d}; "
                        f"first missing bldg_id={missing[0]}"
                    )
                for src in load_map.values():
                    dst = loads_out_dir / src.name
                    if dst.exists() or dst.is_symlink():
                        dst.unlink()
                    os.symlink(src.resolve(), dst)

            if not metadata_parts:
                raise ValueError(f"No metadata rows found for year index {year_idx}")

            metadata_df = pl.concat(metadata_parts, how="diagonal_relaxed").sort("bldg_id")
            metadata_df.write_parquet(year_dir / "metadata-sb.parquet")

    def export_scenario_csv(
        self, *, path_output_dir: Path, assignments: dict[int, dict[int, int]]
    ) -> None:
        """Write `scenario_assignments.csv` in the existing format."""
        path_output_dir.mkdir(parents=True, exist_ok=True)
        bldg_ids = sorted(assignments[0].keys())
        csv_path = path_output_dir / "scenario_assignments.csv"

        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["bldg_id", *[f"year_{y}" for y in self.year_labels]])
            for bldg_id in bldg_ids:
                writer.writerow(
                    [
                        bldg_id,
                        *[
                            assignments[year_idx][bldg_id]
                            for year_idx in range(len(self.year_labels))
                        ],
                    ]
                )
