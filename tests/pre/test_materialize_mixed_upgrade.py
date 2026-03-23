"""Tests for utils/pre/materialize_mixed_upgrade.py."""

from __future__ import annotations

import csv
from pathlib import Path

import polars as pl
import pytest

from buildstock_fetch.scenarios import InvalidScenarioError, validate_scenario
from utils.pre.materialize_mixed_upgrade import (
    _build_load_file_map,
    _parse_adoption_config,
    assign_buildings,
    main,
)

# ---------------------------------------------------------------------------
# Fixtures: minimal in-memory data
# ---------------------------------------------------------------------------

N_BLDGS = 100

# Simple two-upgrade scenario with 3 years.
SCENARIO_2UP = {
    2: [0.10, 0.20, 0.30],
    4: [0.05, 0.10, 0.15],
}
RUN_YEAR_INDICES = [0, 1, 2]


def _bldg_ids(n: int = N_BLDGS) -> list[int]:
    return list(range(1, n + 1))


def _make_metadata_df(
    bldg_ids: list[int],
    has_hp: list[bool] | None = None,
) -> pl.DataFrame:
    """Return a minimal metadata DataFrame with the columns that main() uses."""
    n = len(bldg_ids)
    if has_hp is None:
        has_hp = [False] * n
    return pl.DataFrame(
        {
            "bldg_id": bldg_ids,
            "postprocess_group.has_hp": has_hp,
            "postprocess_group.heating_type": ["Gas"] * n,
            "in.vintage_acs": ["2000s"] * n,
            "applicability": [True] * n,
        }
    )


def _write_metadata(path: Path, df: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _touch_load_file(loads_dir: Path, bldg_id: int, upgrade_id: int) -> Path:
    loads_dir.mkdir(parents=True, exist_ok=True)
    p = loads_dir / f"{bldg_id}-{upgrade_id:02d}.parquet"
    p.touch()
    return p


# ---------------------------------------------------------------------------
# 1. Building assignment: fractions
# ---------------------------------------------------------------------------


class TestAssignBuildingsFractions:
    """assign_buildings() assigns approximately the right fraction to each upgrade."""

    def test_correct_fraction_single_upgrade(self) -> None:
        bldg_ids = _bldg_ids(200)
        scenario = {2: [0.10, 0.25, 0.50]}
        assignments = assign_buildings(bldg_ids, scenario, [0, 1, 2], random_seed=0)

        for t, expected_frac in zip([0, 1, 2], [0.10, 0.25, 0.50]):
            assigned_to_2 = sum(1 for v in assignments[t].values() if v == 2)
            assert assigned_to_2 == int(200 * expected_frac), (
                f"year index {t}: expected {int(200 * expected_frac)} buildings "
                f"assigned to upgrade 2, got {assigned_to_2}"
            )

    def test_correct_fractions_two_upgrades(self) -> None:
        bldg_ids = _bldg_ids(N_BLDGS)
        assignments = assign_buildings(
            bldg_ids, SCENARIO_2UP, RUN_YEAR_INDICES, random_seed=42
        )

        for t in RUN_YEAR_INDICES:
            for u, fracs in SCENARIO_2UP.items():
                expected = int(N_BLDGS * fracs[t])
                actual = sum(1 for v in assignments[t].values() if v == u)
                assert actual == expected, (
                    f"upgrade={u} year={t}: expected {expected}, got {actual}"
                )

    def test_all_buildings_covered(self) -> None:
        bldg_ids = _bldg_ids(N_BLDGS)
        assignments = assign_buildings(
            bldg_ids, SCENARIO_2UP, RUN_YEAR_INDICES, random_seed=42
        )
        for t in RUN_YEAR_INDICES:
            assert set(assignments[t].keys()) == set(bldg_ids)

    def test_remaining_buildings_stay_at_baseline(self) -> None:
        """Buildings not yet assigned to any upgrade must be upgrade 0."""
        bldg_ids = _bldg_ids(N_BLDGS)
        assignments = assign_buildings(
            bldg_ids, SCENARIO_2UP, RUN_YEAR_INDICES, random_seed=42
        )
        for t in RUN_YEAR_INDICES:
            total_hp = sum(
                1 for v in assignments[t].values() if v in SCENARIO_2UP.keys()
            )
            total_baseline = sum(1 for v in assignments[t].values() if v == 0)
            assert total_hp + total_baseline == N_BLDGS

    def test_empty_bldg_list_returns_empty(self) -> None:
        result = assign_buildings([], SCENARIO_2UP, RUN_YEAR_INDICES, random_seed=0)
        for t in RUN_YEAR_INDICES:
            assert result[t] == {}

    def test_zero_fractions_all_baseline(self) -> None:
        bldg_ids = _bldg_ids(50)
        scenario = {2: [0.0, 0.0, 0.0]}
        assignments = assign_buildings(bldg_ids, scenario, [0, 1, 2], random_seed=0)
        for t in [0, 1, 2]:
            assert all(v == 0 for v in assignments[t].values())


# ---------------------------------------------------------------------------
# 2. Building assignment: monotonicity
# ---------------------------------------------------------------------------


class TestAssignBuildingsMonotonic:
    """Buildings that adopt in year N must keep their upgrade in year N+1."""

    def test_monotonic_adoption(self) -> None:
        bldg_ids = _bldg_ids(N_BLDGS)
        # Fractions increase over time — monotonic adoption.
        scenario = {2: [0.10, 0.20, 0.30]}
        assignments = assign_buildings(bldg_ids, scenario, [0, 1, 2], random_seed=1)

        adopted_t0 = {bid for bid, u in assignments[0].items() if u == 2}
        adopted_t1 = {bid for bid, u in assignments[1].items() if u == 2}
        adopted_t2 = {bid for bid, u in assignments[2].items() if u == 2}

        # Every building that adopted in year t must still have the same upgrade in t+1.
        assert adopted_t0.issubset(adopted_t1), "Some t0-adopters reverted in t1"
        assert adopted_t1.issubset(adopted_t2), "Some t1-adopters reverted in t2"

    def test_no_building_assigned_two_upgrades(self) -> None:
        bldg_ids = _bldg_ids(N_BLDGS)
        assignments = assign_buildings(
            bldg_ids, SCENARIO_2UP, RUN_YEAR_INDICES, random_seed=7
        )
        for t in RUN_YEAR_INDICES:
            for bid, uid in assignments[t].items():
                # At any given year each building has exactly one upgrade.
                assert uid in {0} | set(SCENARIO_2UP.keys())

    def test_reproducible_with_same_seed(self) -> None:
        bldg_ids = _bldg_ids(N_BLDGS)
        a1 = assign_buildings(bldg_ids, SCENARIO_2UP, RUN_YEAR_INDICES, random_seed=99)
        a2 = assign_buildings(bldg_ids, SCENARIO_2UP, RUN_YEAR_INDICES, random_seed=99)
        assert a1 == a2

    def test_different_seeds_differ(self) -> None:
        bldg_ids = _bldg_ids(200)
        a1 = assign_buildings(bldg_ids, SCENARIO_2UP, RUN_YEAR_INDICES, random_seed=1)
        a2 = assign_buildings(bldg_ids, SCENARIO_2UP, RUN_YEAR_INDICES, random_seed=2)
        # With 200 buildings and non-trivial fractions it is astronomically unlikely
        # for both seeds to produce identical assignments.
        assert a1[0] != a2[0]


# ---------------------------------------------------------------------------
# 3. Applicability-restricted assignment
# ---------------------------------------------------------------------------


class TestAssignBuildingsApplicability:
    """assign_buildings() with applicable_bldg_ids_per_upgrade restricts pools."""

    def test_only_applicable_buildings_assigned(self) -> None:
        """No building outside its upgrade's applicable set should be assigned to it."""
        bldg_ids = _bldg_ids(100)
        # upgrade 2 applicable to first 60; upgrade 4 applicable to last 40
        applicable = {2: set(range(1, 61)), 4: set(range(61, 101))}
        scenario = {2: [0.10, 0.20], 4: [0.05, 0.10]}
        assignments = assign_buildings(
            bldg_ids,
            scenario,
            [0, 1],
            random_seed=0,
            applicable_bldg_ids_per_upgrade=applicable,
        )
        for t in [0, 1]:
            for bid, uid in assignments[t].items():
                if uid == 2:
                    assert bid in applicable[2], (
                        f"bldg {bid} assigned to upgrade 2 but not applicable"
                    )
                if uid == 4:
                    assert bid in applicable[4], (
                        f"bldg {bid} assigned to upgrade 4 but not applicable"
                    )

    def test_non_overlapping_applicable_sets(self) -> None:
        """Buildings in only one applicable set are assigned to that upgrade."""
        bldg_ids = _bldg_ids(100)
        # Disjoint sets: upgrade 2 → 1-50, upgrade 4 → 51-100
        applicable = {2: set(range(1, 51)), 4: set(range(51, 101))}
        scenario = {2: [0.30], 4: [0.20]}
        assignments = assign_buildings(
            bldg_ids,
            scenario,
            [0],
            random_seed=0,
            applicable_bldg_ids_per_upgrade=applicable,
        )
        assigned_to_2 = {bid for bid, u in assignments[0].items() if u == 2}
        assigned_to_4 = {bid for bid, u in assignments[0].items() if u == 4}
        assert assigned_to_2.issubset(applicable[2])
        assert assigned_to_4.issubset(applicable[4])
        assert assigned_to_2.isdisjoint(assigned_to_4)

    def test_applicable_smaller_than_target_warns_and_caps(self) -> None:
        """When applicable pool is smaller than target count, a warning is emitted."""
        bldg_ids = _bldg_ids(100)
        # upgrade 5 only applicable to 5 buildings but scenario requests 30%
        applicable = {5: set(range(1, 6))}
        scenario = {5: [0.30]}
        with pytest.warns(UserWarning, match="Upgrade 5"):
            assignments = assign_buildings(
                bldg_ids,
                scenario,
                [0],
                random_seed=0,
                applicable_bldg_ids_per_upgrade=applicable,
            )
        assigned_to_5 = sum(1 for u in assignments[0].values() if u == 5)
        assert assigned_to_5 == 5  # capped at pool size

    def test_overlapping_sets_no_double_assignment(self) -> None:
        """When applicable sets overlap, each building gets at most one upgrade."""
        bldg_ids = _bldg_ids(100)
        # Both upgrades applicable to all 100 buildings; upgrade 2 (lower ID) gets
        # first pick and claims all buildings, leaving upgrade 4 with an empty pool.
        applicable = {2: set(range(1, 101)), 4: set(range(1, 101))}
        scenario = {2: [0.20], 4: [0.15]}
        with pytest.warns(UserWarning, match="Upgrade 4"):
            assignments = assign_buildings(
                bldg_ids,
                scenario,
                [0],
                random_seed=0,
                applicable_bldg_ids_per_upgrade=applicable,
            )
        for bid, uid in assignments[0].items():
            assert uid in {0, 2, 4}, f"bldg {bid} assigned unknown upgrade {uid}"
        # No building should be in two upgrade pools
        assigned_to_2 = {bid for bid, u in assignments[0].items() if u == 2}
        assigned_to_4 = {bid for bid, u in assignments[0].items() if u == 4}
        assert assigned_to_2.isdisjoint(assigned_to_4)

    def test_monotonicity_preserved_with_applicability(self) -> None:
        """Monotonic adoption holds when using applicable_bldg_ids_per_upgrade."""
        bldg_ids = _bldg_ids(N_BLDGS)
        applicable = {2: set(range(1, 81))}  # 80 buildings applicable for upgrade 2
        scenario = {2: [0.10, 0.20, 0.30]}
        assignments = assign_buildings(
            bldg_ids,
            scenario,
            [0, 1, 2],
            random_seed=3,
            applicable_bldg_ids_per_upgrade=applicable,
        )
        adopted_t0 = {bid for bid, u in assignments[0].items() if u == 2}
        adopted_t1 = {bid for bid, u in assignments[1].items() if u == 2}
        adopted_t2 = {bid for bid, u in assignments[2].items() if u == 2}
        assert adopted_t0.issubset(adopted_t1)
        assert adopted_t1.issubset(adopted_t2)

    def test_none_applicable_fallback_identical_to_unrestricted(self) -> None:
        """Passing applicable_bldg_ids_per_upgrade=None gives same result as omitting it."""
        bldg_ids = _bldg_ids(N_BLDGS)
        a_restricted = assign_buildings(
            bldg_ids,
            SCENARIO_2UP,
            RUN_YEAR_INDICES,
            random_seed=42,
            applicable_bldg_ids_per_upgrade=None,
        )
        a_unrestricted = assign_buildings(
            bldg_ids,
            SCENARIO_2UP,
            RUN_YEAR_INDICES,
            random_seed=42,
        )
        assert a_restricted == a_unrestricted


# ---------------------------------------------------------------------------
# 4. Metadata combination — unit tests via main()
# ---------------------------------------------------------------------------


class TestMetadataCombination:
    """Combined metadata parquet has correct columns and row count."""

    @pytest.fixture()
    def fs(self, tmp_path: Path) -> Path:
        """Build a minimal on-disk fixture and return the release root."""
        release = tmp_path / "release"
        bldg_ids = list(range(1, 11))  # 10 buildings
        # 3 buildings already have HPs in baseline; remaining 7 are eligible.
        has_hp = [False] * 7 + [True] * 3

        for uid in [0, 2]:
            meta_path = (
                release
                / "metadata"
                / "state=NY"
                / f"upgrade={uid:02d}"
                / "metadata-sb.parquet"
            )
            # upgrade=02: eligible buildings (first 7) have has_hp=True (upgrade applied);
            # already-HP buildings (last 3) also keep has_hp=True.
            df = _make_metadata_df(bldg_ids, has_hp if uid == 0 else [True] * 10)
            _write_metadata(meta_path, df)
            loads_dir = (
                release / "load_curve_hourly" / "state=NY" / f"upgrade={uid:02d}"
            )
            for bid in bldg_ids:
                _touch_load_file(loads_dir, bid, uid)

        return release

    @pytest.fixture()
    def adoption_yaml(self, tmp_path: Path) -> Path:
        content = (
            "scenario_name: test_scenario\n"
            "random_seed: 0\n"
            "scenario:\n"
            "  2: [0.20, 0.40]\n"
            "year_labels: [2025, 2030]\n"
        )
        p = tmp_path / "adoption.yaml"
        p.write_text(content, encoding="utf-8")
        return p

    def test_all_required_columns_present(
        self, fs: Path, adoption_yaml: Path, tmp_path: Path
    ) -> None:
        out_dir = tmp_path / "out"
        main(
            [
                "--state",
                "ny",
                "--utility",
                "test",
                "--adoption-config",
                str(adoption_yaml),
                "--path-resstock-release",
                str(fs),
                "--output-dir",
                str(out_dir),
            ]
        )
        for year in [2025, 2030]:
            df = pl.read_parquet(out_dir / f"year={year}" / "metadata-sb.parquet")
            for col in [
                "bldg_id",
                "postprocess_group.has_hp",
                "postprocess_group.heating_type",
                "in.vintage_acs",
                "applicability",
            ]:
                assert col in df.columns, f"Missing column '{col}' for year={year}"

    def test_each_building_appears_exactly_once(
        self, fs: Path, adoption_yaml: Path, tmp_path: Path
    ) -> None:
        out_dir = tmp_path / "out"
        main(
            [
                "--state",
                "ny",
                "--utility",
                "test",
                "--adoption-config",
                str(adoption_yaml),
                "--path-resstock-release",
                str(fs),
                "--output-dir",
                str(out_dir),
            ]
        )
        for year in [2025, 2030]:
            df = pl.read_parquet(out_dir / f"year={year}" / "metadata-sb.parquet")
            assert df.shape[0] == 10, (
                f"year={year}: expected 10 rows, got {df.shape[0]}"
            )
            assert df["bldg_id"].n_unique() == 10, f"year={year}: duplicate bldg_ids"

    def test_already_hp_buildings_pinned_to_baseline(
        self, fs: Path, adoption_yaml: Path, tmp_path: Path
    ) -> None:
        """The 3 buildings that already have HP stay at upgrade-0 metadata in all years."""
        out_dir = tmp_path / "out"
        main(
            [
                "--state",
                "ny",
                "--utility",
                "test",
                "--adoption-config",
                str(adoption_yaml),
                "--path-resstock-release",
                str(fs),
                "--output-dir",
                str(out_dir),
            ]
        )
        for year in [2025, 2030]:
            df = pl.read_parquet(out_dir / f"year={year}" / "metadata-sb.parquet")
            # Buildings 8, 9, 10 have has_hp=True in baseline (indices 7-9, bldg_ids 8-10).
            already_hp_ids = [8, 9, 10]
            already_hp_df = df.filter(pl.col("bldg_id").is_in(already_hp_ids))
            assert already_hp_df["postprocess_group.has_hp"].to_list() == [True] * 3

    def test_more_hp_buildings_at_later_year(
        self, fs: Path, adoption_yaml: Path, tmp_path: Path
    ) -> None:
        """Later years should have a higher fraction of HP buildings."""
        out_dir = tmp_path / "out"
        main(
            [
                "--state",
                "ny",
                "--utility",
                "test",
                "--adoption-config",
                str(adoption_yaml),
                "--path-resstock-release",
                str(fs),
                "--output-dir",
                str(out_dir),
            ]
        )
        df_early = pl.read_parquet(out_dir / "year=2025" / "metadata-sb.parquet")
        df_late = pl.read_parquet(out_dir / "year=2030" / "metadata-sb.parquet")

        # Upgrade-2 metadata has has_hp=None (defaulted to False in fixture).
        # Count rows with has_hp True: comes from already-HP + newly assigned.
        hp_early = df_early["postprocess_group.has_hp"].sum()
        hp_late = df_late["postprocess_group.has_hp"].sum()
        assert hp_late >= hp_early


# ---------------------------------------------------------------------------
# 5. Symlink creation
# ---------------------------------------------------------------------------


class TestSymlinkCreation:
    """loads/ directory contains correctly targeted symlinks."""

    @pytest.fixture()
    def fs_and_out(self, tmp_path: Path) -> tuple[Path, Path]:
        """Fixture: 5 buildings, upgrades 0 and 2."""
        release = tmp_path / "release"
        bldg_ids = list(range(1, 6))

        for uid in [0, 2]:
            meta = (
                release
                / "metadata"
                / "state=RI"
                / f"upgrade={uid:02d}"
                / "metadata-sb.parquet"
            )
            has_hp = [False] * len(bldg_ids) if uid == 0 else [True] * len(bldg_ids)
            _write_metadata(meta, _make_metadata_df(bldg_ids, has_hp))
            loads_dir = (
                release / "load_curve_hourly" / "state=RI" / f"upgrade={uid:02d}"
            )
            for bid in bldg_ids:
                _touch_load_file(loads_dir, bid, uid)

        adoption_yaml = tmp_path / "adoption.yaml"
        adoption_yaml.write_text(
            "scenario_name: test\nrandom_seed: 0\nscenario:\n  2: [0.40]\n"
            "year_labels: [2025]\n",
            encoding="utf-8",
        )
        out_dir = tmp_path / "out"
        main(
            [
                "--state",
                "ri",
                "--utility",
                "test",
                "--adoption-config",
                str(adoption_yaml),
                "--path-resstock-release",
                str(release),
                "--output-dir",
                str(out_dir),
            ]
        )
        return release, out_dir

    def test_loads_dir_exists(self, fs_and_out: tuple[Path, Path]) -> None:
        _, out_dir = fs_and_out
        assert (out_dir / "year=2025" / "loads").is_dir()

    def test_symlink_count_equals_building_count(
        self, fs_and_out: tuple[Path, Path]
    ) -> None:
        _, out_dir = fs_and_out
        links = list((out_dir / "year=2025" / "loads").iterdir())
        assert len(links) == 5

    def test_symlinks_are_actual_symlinks(self, fs_and_out: tuple[Path, Path]) -> None:
        _, out_dir = fs_and_out
        for p in (out_dir / "year=2025" / "loads").iterdir():
            assert p.is_symlink(), f"{p} is not a symlink"

    def test_symlink_targets_exist(self, fs_and_out: tuple[Path, Path]) -> None:
        release, out_dir = fs_and_out
        for p in (out_dir / "year=2025" / "loads").iterdir():
            assert p.resolve().exists(), f"Dangling symlink: {p}"

    def test_symlink_filename_convention(self, fs_and_out: tuple[Path, Path]) -> None:
        """All symlink names follow {bldg_id}-{upgrade_id}.parquet."""
        _, out_dir = fs_and_out
        for p in (out_dir / "year=2025" / "loads").iterdir():
            stem = p.stem  # e.g. "3-02"
            parts = stem.split("-", maxsplit=1)
            assert len(parts) == 2, f"Unexpected filename: {p.name}"
            bldg_id_str, upgrade_str = parts
            assert bldg_id_str.isdigit(), f"Non-numeric bldg_id in {p.name}"
            assert upgrade_str.isdigit(), f"Non-numeric upgrade_id in {p.name}"

    def test_assigned_buildings_link_to_correct_upgrade(
        self, fs_and_out: tuple[Path, Path]
    ) -> None:
        """Buildings assigned to upgrade 2 must symlink to upgrade=02 load files."""
        release, out_dir = fs_and_out
        loads_dir = out_dir / "year=2025" / "loads"
        for link in loads_dir.iterdir():
            target = link.resolve()
            # The upgrade_id is encoded in the filename (e.g. "3-02.parquet").
            stem = link.stem
            upgrade_str = stem.split("-", maxsplit=1)[1]
            expected_upgrade_dir = f"upgrade={int(upgrade_str):02d}"
            assert expected_upgrade_dir in str(target), (
                f"Symlink {link.name} → {target} does not point into {expected_upgrade_dir}"
            )


# ---------------------------------------------------------------------------
# 6. Scenario CSV output
# ---------------------------------------------------------------------------


class TestScenarioCsv:
    """Scenario CSV is written with the correct structure."""

    @pytest.fixture()
    def out_dir(self, tmp_path: Path) -> Path:
        release = tmp_path / "release"
        bldg_ids = list(range(1, 9))

        for uid in [0, 2]:
            meta = (
                release
                / "metadata"
                / "state=NY"
                / f"upgrade={uid:02d}"
                / "metadata-sb.parquet"
            )
            has_hp = [False] * len(bldg_ids) if uid == 0 else [True] * len(bldg_ids)
            _write_metadata(meta, _make_metadata_df(bldg_ids, has_hp))
            ld = release / "load_curve_hourly" / "state=NY" / f"upgrade={uid:02d}"
            for bid in bldg_ids:
                _touch_load_file(ld, bid, uid)

        adoption_yaml = tmp_path / "adoption.yaml"
        adoption_yaml.write_text(
            "scenario_name: test\nrandom_seed: 0\nscenario:\n"
            "  2: [0.25, 0.50]\nyear_labels: [2025, 2030]\n",
            encoding="utf-8",
        )
        out = tmp_path / "out"
        main(
            [
                "--state",
                "ny",
                "--utility",
                "test",
                "--adoption-config",
                str(adoption_yaml),
                "--path-resstock-release",
                str(release),
                "--output-dir",
                str(out),
            ]
        )
        return out

    def test_csv_exists(self, out_dir: Path) -> None:
        assert (out_dir / "scenario_assignments.csv").exists()

    def test_csv_columns(self, out_dir: Path) -> None:
        with open(
            out_dir / "scenario_assignments.csv", newline="", encoding="utf-8"
        ) as f:
            reader = csv.reader(f)
            header = next(reader)
        assert header[0] == "bldg_id"
        assert "year_2025" in header
        assert "year_2030" in header

    def test_csv_row_count(self, out_dir: Path) -> None:
        with open(
            out_dir / "scenario_assignments.csv", newline="", encoding="utf-8"
        ) as f:
            rows = list(csv.reader(f))
        # header + 8 buildings
        assert len(rows) == 9

    def test_csv_values_are_valid_upgrade_ids(self, out_dir: Path) -> None:
        with open(
            out_dir / "scenario_assignments.csv", newline="", encoding="utf-8"
        ) as f:
            reader = csv.DictReader(f)
            for row in reader:
                for col in ["year_2025", "year_2030"]:
                    assert int(row[col]) in {0, 2}, (
                        f"Unexpected upgrade id in CSV: {row[col]}"
                    )

    def test_csv_later_year_has_more_or_equal_adopters(self, out_dir: Path) -> None:
        with open(
            out_dir / "scenario_assignments.csv", newline="", encoding="utf-8"
        ) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        adopters_2025 = sum(1 for r in rows if int(r["year_2025"]) != 0)
        adopters_2030 = sum(1 for r in rows if int(r["year_2030"]) != 0)
        assert adopters_2030 >= adopters_2025


# ---------------------------------------------------------------------------
# 7. Validation error paths
# ---------------------------------------------------------------------------


class TestValidationErrors:
    """Error conditions: missing upgrade dirs, invalid fractions, etc."""

    def test_missing_upgrade_metadata_raises(self, tmp_path: Path) -> None:
        release = tmp_path / "release"
        # Only create upgrade=00 metadata; upgrade=02 is missing.
        meta = release / "metadata" / "state=NY" / "upgrade=00" / "metadata-sb.parquet"
        _write_metadata(meta, _make_metadata_df([1, 2]))

        adoption_yaml = tmp_path / "adoption.yaml"
        adoption_yaml.write_text(
            "scenario_name: t\nrandom_seed: 0\nscenario:\n  2: [0.10]\n"
            "year_labels: [2025]\n",
            encoding="utf-8",
        )
        with pytest.raises(FileNotFoundError, match="upgrade=02"):
            main(
                [
                    "--state",
                    "ny",
                    "--utility",
                    "test",
                    "--adoption-config",
                    str(adoption_yaml),
                    "--path-resstock-release",
                    str(release),
                    "--output-dir",
                    str(tmp_path / "out"),
                ]
            )

    def test_missing_loads_dir_raises(self, tmp_path: Path) -> None:
        release = tmp_path / "release"
        # Create metadata for both upgrades but omit loads dir for upgrade=02.
        for uid in [0, 2]:
            meta = (
                release
                / "metadata"
                / "state=NY"
                / f"upgrade={uid:02d}"
                / "metadata-sb.parquet"
            )
            _write_metadata(meta, _make_metadata_df([1, 2]))
        loads_dir_0 = release / "load_curve_hourly" / "state=NY" / "upgrade=00"
        loads_dir_0.mkdir(parents=True)

        adoption_yaml = tmp_path / "adoption.yaml"
        adoption_yaml.write_text(
            "scenario_name: t\nrandom_seed: 0\nscenario:\n  2: [0.10]\n"
            "year_labels: [2025]\n",
            encoding="utf-8",
        )
        with pytest.raises(FileNotFoundError, match="upgrade=02"):
            main(
                [
                    "--state",
                    "ny",
                    "--utility",
                    "test",
                    "--adoption-config",
                    str(adoption_yaml),
                    "--path-resstock-release",
                    str(release),
                    "--output-dir",
                    str(tmp_path / "out"),
                ]
            )

    def test_fractions_outside_range_raise(self) -> None:
        with pytest.raises(InvalidScenarioError):
            validate_scenario({2: [0.05, 1.10]})  # 1.10 > 1.0

    def test_negative_fraction_raises(self) -> None:
        with pytest.raises(InvalidScenarioError):
            validate_scenario({2: [-0.01, 0.05]})

    def test_non_monotonic_fractions_raise(self) -> None:
        with pytest.raises(InvalidScenarioError):
            validate_scenario({2: [0.30, 0.10]})  # decreasing

    def test_total_exceeds_one_raises(self) -> None:
        with pytest.raises(InvalidScenarioError):
            validate_scenario(
                {2: [0.60, 0.70], 4: [0.50, 0.40]}
            )  # sums to >1.0 in year 0

    def test_missing_load_file_for_building_raises(self, tmp_path: Path) -> None:
        """FileNotFoundError when a building's load file is absent from the loads dir."""
        release = tmp_path / "release"
        bldg_ids = list(range(1, 4))

        for uid in [0, 2]:
            meta = (
                release
                / "metadata"
                / "state=NY"
                / f"upgrade={uid:02d}"
                / "metadata-sb.parquet"
            )
            # upgrade=02 metadata must have has_hp=True so buildings are applicable.
            has_hp = [False] * len(bldg_ids) if uid == 0 else [True] * len(bldg_ids)
            _write_metadata(meta, _make_metadata_df(bldg_ids, has_hp))
            ld = release / "load_curve_hourly" / "state=NY" / f"upgrade={uid:02d}"
            for bid in bldg_ids:
                _touch_load_file(ld, bid, uid)

        # Remove load file for one building in upgrade=02.
        missing = (
            release / "load_curve_hourly" / "state=NY" / "upgrade=02" / "2-02.parquet"
        )
        missing.unlink()

        adoption_yaml = tmp_path / "adoption.yaml"
        # Assign enough buildings to upgrade 2 so bldg_id=2 gets assigned.
        adoption_yaml.write_text(
            "scenario_name: t\nrandom_seed: 0\nscenario:\n  2: [0.67]\n"
            "year_labels: [2025]\n",
            encoding="utf-8",
        )
        # The exact building assigned depends on shuffling; we accept either a
        # successful run (if bldg_id=2 was not assigned to upgrade 2) or an error.
        # To force the error deterministically, assign all buildings to upgrade 2.
        adoption_yaml.write_text(
            "scenario_name: t\nrandom_seed: 0\nscenario:\n  2: [1.00]\n"
            "year_labels: [2025]\n",
            encoding="utf-8",
        )
        with pytest.raises(FileNotFoundError):
            main(
                [
                    "--state",
                    "ny",
                    "--utility",
                    "test",
                    "--adoption-config",
                    str(adoption_yaml),
                    "--path-resstock-release",
                    str(release),
                    "--output-dir",
                    str(tmp_path / "out"),
                ]
            )


# ---------------------------------------------------------------------------
# 8. Config parsing: run_years snap + year indices
# ---------------------------------------------------------------------------


class TestParseAdoptionConfig:
    """_parse_adoption_config correctly handles run_years and year snapping."""

    def test_all_years_when_run_years_omitted(self) -> None:
        config = {
            "scenario_name": "test",
            "random_seed": 1,
            "scenario": {2: [0.1, 0.2, 0.3]},
            "year_labels": [2025, 2030, 2035],
        }
        _, _, _, year_labels, run_year_indices = _parse_adoption_config(config)
        assert run_year_indices == [0, 1, 2]
        assert year_labels == [2025, 2030, 2035]

    def test_run_years_subset_selects_correct_indices(self) -> None:
        config = {
            "scenario_name": "test",
            "random_seed": 1,
            "scenario": {2: [0.1, 0.2, 0.3]},
            "year_labels": [2025, 2030, 2035],
            "run_years": [2025, 2035],
        }
        _, _, _, _, run_year_indices = _parse_adoption_config(config)
        assert run_year_indices == [0, 2]

    def test_run_years_snaps_to_nearest(self) -> None:
        config = {
            "scenario_name": "test",
            "random_seed": 1,
            "scenario": {2: [0.1, 0.2, 0.3]},
            "year_labels": [2025, 2030, 2035],
            "run_years": [2028],
        }
        with pytest.warns(UserWarning, match="snapping"):
            _, _, _, _, run_year_indices = _parse_adoption_config(config)
        assert run_year_indices == [1]  # snaps to 2030

    def test_string_keys_normalised_to_int(self) -> None:
        config = {
            "scenario_name": "test",
            "random_seed": 1,
            "scenario": {"2": [0.1, 0.2], "4": [0.05, 0.10]},
            "year_labels": [2025, 2030],
        }
        _, _, scenario, _, _ = _parse_adoption_config(config)
        assert 2 in scenario
        assert 4 in scenario
        assert "2" not in scenario


# ---------------------------------------------------------------------------
# 9. _build_load_file_map
# ---------------------------------------------------------------------------


class TestBuildLoadFileMap:
    """_build_load_file_map scans a directory and returns {bldg_id: path}."""

    def test_finds_matching_files(self, tmp_path: Path) -> None:
        d = tmp_path / "loads"
        d.mkdir()
        (d / "1-02.parquet").touch()
        (d / "3-02.parquet").touch()
        (d / "99-02.parquet").touch()

        result = _build_load_file_map(d, {1, 3, 99})
        assert set(result.keys()) == {1, 3, 99}

    def test_filters_to_requested_bldg_ids(self, tmp_path: Path) -> None:
        d = tmp_path / "loads"
        d.mkdir()
        (d / "1-02.parquet").touch()
        (d / "2-02.parquet").touch()
        (d / "3-02.parquet").touch()

        result = _build_load_file_map(d, {1, 3})
        assert set(result.keys()) == {1, 3}

    def test_ignores_non_parquet_files(self, tmp_path: Path) -> None:
        d = tmp_path / "loads"
        d.mkdir()
        (d / "1-02.parquet").touch()
        (d / "readme.txt").touch()

        result = _build_load_file_map(d, {1})
        assert set(result.keys()) == {1}

    def test_empty_directory_returns_empty(self, tmp_path: Path) -> None:
        d = tmp_path / "loads"
        d.mkdir()
        result = _build_load_file_map(d, {1, 2})
        assert result == {}
