"""Tests for data/resstock/manifest.py provenance and sync behavior.

Covers the four fixes applied to the manifest system:

Fix 1 (upsert_run preserves history) — verified via upsert_run directly; the
       destructive write_manifest(path_sb, {"runs": []}) call was removed from
       main.py, so sequential runs now accumulate records rather than resetting.
Fix 2 (manifests uploaded after finish_run) — not tested here because upload
       requires real S3 credentials.  The ordering is verified by inspection.
Fix 3 (EBS↔S3 sync check respects --state/--upgrade filter) — tested via
       print_status with monkeypatched S3 reads.
Fix 4 (early crash leaves _sb trace) — tested by verifying upsert_run creates
       the manifest file if none exists yet (the _sb dir is now created before
       the try block in main.py).
"""

from __future__ import annotations

import io
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any
from unittest.mock import patch

from data.resstock.manifest import (
    _filter_runs,
    finish_run,
    new_run_record,
    print_status,
    read_manifest,
    record_step,
    upsert_run,
)


# ── helpers ───────────────────────────────────────────────────────────────────


def _make_run(run_id: str, states: list[str], upgrade_ids: list[str]) -> dict[str, Any]:
    run = new_run_record(
        release="res_2024_amy2018_2",
        release_sb="res_2024_amy2018_2_sb",
        states=states,
        upgrade_ids=upgrade_ids,
        file_types=["metadata"],
        flags={},
    )
    run["run_id"] = run_id
    return run


def _capture_status(**kwargs: Any) -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        print_status(**kwargs)
    return buf.getvalue()


# ── Fix 1: sequential state runs preserve _sb history ────────────────────────


def test_sequential_states_accumulate_in_sb_manifest(tmp_path: Path) -> None:
    """Running MD then NY must leave both records in the _sb manifest."""
    sb_dir = tmp_path / "res_2024_amy2018_2_sb"

    md_run = _make_run("run-MD-001", ["MD"], ["0", "2"])
    upsert_run(sb_dir, md_run)

    ny_run = _make_run("run-NY-002", ["NY"], ["0", "2"])
    upsert_run(sb_dir, ny_run)

    manifest = read_manifest(sb_dir)
    ids = [r["run_id"] for r in manifest.get("runs", [])]
    assert "run-MD-001" in ids, "MD run was erased after NY run"
    assert "run-NY-002" in ids, "NY run not recorded"
    assert len(ids) == 2


def test_upsert_updates_existing_run_in_place(tmp_path: Path) -> None:
    """Calling upsert_run twice with the same run_id must update, not duplicate."""
    release_dir = tmp_path / "release"

    run = _make_run("run-001", ["MD"], ["0"])
    upsert_run(release_dir, run)
    record_step(run, "fetch")
    upsert_run(release_dir, run)

    manifest = read_manifest(release_dir)
    ids = [r["run_id"] for r in manifest.get("runs", [])]
    assert ids.count("run-001") == 1, "Duplicate run_id inserted"
    steps = manifest["runs"][0]["steps"]
    assert any(s["step"] == "fetch" for s in steps), "Step not updated"


def test_upsert_creates_manifest_when_missing(tmp_path: Path) -> None:
    """upsert_run must create manifest.yaml if the directory has none (Fix 4)."""
    release_dir = tmp_path / "new_release"
    release_dir.mkdir()
    assert not (release_dir / "manifest.yaml").exists()

    run = _make_run("run-001", ["MD"], ["0"])
    upsert_run(release_dir, run)

    assert (release_dir / "manifest.yaml").exists()
    manifest = read_manifest(release_dir)
    assert manifest["runs"][0]["run_id"] == "run-001"


# ── Fix 2: finish_run marks status completed ─────────────────────────────────


def test_finish_run_marks_completed(tmp_path: Path) -> None:
    """finish_run + upsert_run must write status=completed to the manifest."""
    release_dir = tmp_path / "release"
    run = _make_run("run-001", ["MD"], ["0"])
    upsert_run(release_dir, run)

    finish_run(run)
    upsert_run(release_dir, run)

    manifest = read_manifest(release_dir)
    stored = manifest["runs"][0]
    assert stored["status"] == "completed"
    assert "completed_at" in stored


# ── Fix 3: EBS↔S3 sync respects --state filter ───────────────────────────────


def _fake_s3_manifest(runs: list[dict[str, Any]]) -> dict[str, Any]:
    return {"runs": runs}


def test_sync_check_reports_in_sync_for_matching_state(tmp_path: Path) -> None:
    """Both EBS and S3 have the same MD run → in sync."""
    ebs_base = str(tmp_path)
    sb_dir = tmp_path / "res_2024_amy2018_2_sb"

    md_run = _make_run("run-MD-001", ["MD"], ["0", "2"])
    finish_run(md_run)
    upsert_run(sb_dir, md_run)

    with patch(
        "data.resstock.manifest.read_manifest_from_s3",
        return_value=_fake_s3_manifest([md_run]),
    ):
        output = _capture_status(
            release="res_2024_amy2018_2",
            release_sb="res_2024_amy2018_2_sb",
            ebs_base=ebs_base,
            s3_base="s3://data.sb/nrel/resstock",
            states=["MD"],
            variant="sb",
        )

    assert "✓ in sync" in output, f"Expected in sync:\n{output}"


def test_sync_check_detects_divergence_for_matching_state(tmp_path: Path) -> None:
    """EBS has a newer MD run than S3 → out of sync."""
    ebs_base = str(tmp_path)
    sb_dir = tmp_path / "res_2024_amy2018_2_sb"

    old_md_run = _make_run("run-MD-001", ["MD"], ["0", "2"])
    finish_run(old_md_run)
    new_md_run = _make_run("run-MD-002", ["MD"], ["0", "2"])
    finish_run(new_md_run)
    upsert_run(sb_dir, old_md_run)
    upsert_run(sb_dir, new_md_run)

    with patch(
        "data.resstock.manifest.read_manifest_from_s3",
        return_value=_fake_s3_manifest([old_md_run]),
    ):
        output = _capture_status(
            release="res_2024_amy2018_2",
            release_sb="res_2024_amy2018_2_sb",
            ebs_base=ebs_base,
            s3_base="s3://data.sb/nrel/resstock",
            states=["MD"],
            variant="sb",
        )

    assert "OUT OF SYNC" in output, f"Expected out of sync:\n{output}"


def test_sync_check_does_not_use_unfiltered_run_ids(tmp_path: Path) -> None:
    """NY run is newer than MD run; status --state MD must not report in sync
    based on the NY run IDs when EBS and S3 hold different MD runs."""
    ebs_base = str(tmp_path)
    sb_dir = tmp_path / "res_2024_amy2018_2_sb"

    # EBS: old MD run, then NY run
    old_md_run = _make_run("run-MD-001", ["MD"], ["0", "2"])
    finish_run(old_md_run)
    ny_run = _make_run("run-NY-003", ["NY"], ["0", "2"])
    finish_run(ny_run)
    upsert_run(sb_dir, old_md_run)
    upsert_run(sb_dir, ny_run)

    # S3: completely different MD run, same NY run
    new_md_run_s3 = _make_run("run-MD-002", ["MD"], ["0", "2"])
    finish_run(new_md_run_s3)
    s3_manifest = _fake_s3_manifest([new_md_run_s3, ny_run])

    with patch(
        "data.resstock.manifest.read_manifest_from_s3",
        return_value=s3_manifest,
    ):
        output = _capture_status(
            release="res_2024_amy2018_2",
            release_sb="res_2024_amy2018_2_sb",
            ebs_base=ebs_base,
            s3_base="s3://data.sb/nrel/resstock",
            states=["MD"],
            variant="sb",
        )

    # NY run IDs match, but MD run IDs differ — should NOT claim in sync
    assert "OUT OF SYNC" in output, (
        f"Sync check incorrectly used unfiltered NY run IDs:\n{output}"
    )


def test_sync_check_cannot_determine_when_state_missing_from_one_side(
    tmp_path: Path,
) -> None:
    """S3 has no MD run at all → sync status should be indeterminate."""
    ebs_base = str(tmp_path)
    sb_dir = tmp_path / "res_2024_amy2018_2_sb"

    md_run = _make_run("run-MD-001", ["MD"], ["0", "2"])
    finish_run(md_run)
    upsert_run(sb_dir, md_run)

    # S3 has only a NY run
    ny_run = _fake_s3_manifest([_make_run("run-NY-001", ["NY"], ["0", "2"])])

    with patch(
        "data.resstock.manifest.read_manifest_from_s3",
        return_value=ny_run,
    ):
        output = _capture_status(
            release="res_2024_amy2018_2",
            release_sb="res_2024_amy2018_2_sb",
            ebs_base=ebs_base,
            s3_base="s3://data.sb/nrel/resstock",
            states=["MD"],
            variant="sb",
        )

    assert "cannot determine sync" in output, (
        f"Expected indeterminate sync message:\n{output}"
    )


# ── _filter_runs unit tests ───────────────────────────────────────────────────


def test_filter_runs_by_state() -> None:
    md = _make_run("r1", ["MD"], ["0"])
    ny = _make_run("r2", ["NY"], ["0"])
    assert _filter_runs([md, ny], states=["MD"]) == [md]
    assert _filter_runs([md, ny], states=["NY"]) == [ny]
    assert _filter_runs([md, ny], states=["RI"]) == []


def test_filter_runs_by_upgrade() -> None:
    u0 = _make_run("r1", ["MD"], ["0"])
    u2 = _make_run("r2", ["MD"], ["2"])
    assert _filter_runs([u0, u2], upgrades=["0"]) == [u0]
    assert _filter_runs([u0, u2], upgrades=["2"]) == [u2]
    assert _filter_runs([u0, u2], upgrades=["5"]) == []


def test_filter_runs_no_filter_returns_all() -> None:
    runs = [_make_run(f"r{i}", ["MD"], ["0"]) for i in range(3)]
    assert _filter_runs(runs) == runs


# ── Integrity check ──────────────────────────────────────────────────────────


def test_check_integrity_unions_file_types_across_runs(tmp_path: Path) -> None:
    """Expected types are the union of all matching runs, not just the latest."""
    import time

    from data.resstock.manifest import check_integrity

    ebs_base = str(tmp_path)
    release = "res_2024_amy2018_2"
    raw_dir = tmp_path / release
    sb_dir = tmp_path / f"{release}_sb"
    sb_dir.mkdir(parents=True)

    # Files from two separate runs: metadata then load_curve_hourly
    for d in (
        raw_dir / "metadata" / "state=MD" / "upgrade=00",
        raw_dir / "load_curve_hourly" / "state=MD" / "upgrade=00",
    ):
        d.mkdir(parents=True)
        (d / "data.parquet").write_bytes(b"ok")
    time.sleep(0.05)

    run1 = _make_run("run-meta", ["MD"], ["0"])
    run1["args"]["file_types"] = ["metadata"]
    record_step(run1, "fetch")
    finish_run(run1)
    upsert_run(raw_dir, run1)

    run2 = _make_run("run-loads", ["MD"], ["0"])
    run2["args"]["file_types"] = ["load_curve_hourly"]
    record_step(run2, "fetch")
    finish_run(run2)
    upsert_run(raw_dir, run2)

    # Empty _sb manifest so it doesn't confuse the raw assertion
    sb_run = _make_run("run-sb", ["MD"], ["0"])
    sb_run["args"]["file_types"] = ["metadata"]
    finish_run(sb_run)
    upsert_run(sb_dir, sb_run)

    with patch("data.resstock.manifest._list_s3_files", return_value=[]):
        report = check_integrity(
            release=release,
            state="MD",
            ebs_base=ebs_base,
            s3_base="s3://data.sb/nrel/resstock",
            location="ebs",
            output_path=tmp_path / "report.yaml",
        )

    raw_issues = [
        m
        for m in report["mismatches"]
        if m.get("release") == release
        and m.get("issue")
        in (
            "unexpected_file_type",
            "missing_file_type",
        )
    ]
    assert raw_issues == [], (
        f"Union of metadata + load_curve_hourly runs should expect both; "
        f"got issues: {raw_issues}"
    )


def test_check_integrity_file_type_bijection_passes(tmp_path: Path) -> None:
    """Expected file types present with matching timestamps → no mismatches."""
    import time

    from data.resstock.manifest import check_integrity

    ebs_base = str(tmp_path)
    release = "res_2024_amy2018_2"
    release_sb = f"{release}_sb"
    sb_dir = tmp_path / release_sb
    raw_dir = tmp_path / release

    # Create expected dirs before recording steps so mtimes precede completed_at
    for d in (
        sb_dir / "metadata" / "state=MD" / "upgrade=00",
        sb_dir / "metadata_utility" / "state=MD",
        raw_dir / "metadata" / "state=MD" / "upgrade=00",
    ):
        d.mkdir(parents=True)
        (d / "data.parquet").write_bytes(b"ok")
    time.sleep(0.05)

    sb_run = _make_run("run-MD-001", ["MD"], ["0"])
    sb_run["args"]["file_types"] = ["metadata"]
    record_step(sb_run, "clone")
    record_step(sb_run, "assign_utility")
    finish_run(sb_run)
    upsert_run(sb_dir, sb_run)

    raw_run = _make_run("run-MD-001", ["MD"], ["0"])
    raw_run["args"]["file_types"] = ["metadata"]
    record_step(raw_run, "fetch")
    finish_run(raw_run)
    upsert_run(raw_dir, raw_run)

    report_path = tmp_path / "report.yaml"
    with patch("data.resstock.manifest._list_s3_files", return_value=[]):
        report = check_integrity(
            release=release,
            state="MD",
            ebs_base=ebs_base,
            s3_base="s3://data.sb/nrel/resstock",
            location="ebs",
            output_path=report_path,
        )

    assert report["mismatches"] == []
    assert "match the manifest" in report["summary"]
    assert report_path.exists()


def test_check_integrity_flags_unexpected_file_type_on_sb(tmp_path: Path) -> None:
    """load_curve_annual on _sb must fail: excluded from _sb and not in expected set."""
    from data.resstock.manifest import check_integrity

    ebs_base = str(tmp_path)
    release = "res_2024_amy2018_2"
    release_sb = f"{release}_sb"
    sb_dir = tmp_path / release_sb
    raw_dir = tmp_path / release

    # Expected types for _sb with these steps: metadata, load_curve_hourly, metadata_utility
    for d in (
        sb_dir / "metadata" / "state=MD" / "upgrade=00",
        sb_dir / "load_curve_hourly" / "state=MD" / "upgrade=00",
        sb_dir / "metadata_utility" / "state=MD",
        # Forbidden leftover:
        sb_dir / "load_curve_annual" / "state=MD" / "upgrade=00",
    ):
        d.mkdir(parents=True)
        (d / "data.parquet").write_bytes(b"x")

    sb_run = _make_run("run-MD-001", ["MD"], ["0"])
    sb_run["args"]["file_types"] = [
        "metadata",
        "load_curve_hourly",
        "load_curve_annual",
    ]
    record_step(sb_run, "clone")
    record_step(sb_run, "assign_utility")
    finish_run(sb_run)
    upsert_run(sb_dir, sb_run)

    # Raw: no files needed for this assertion (will report missing, that's fine —
    # we only assert on the unexpected_file_type for load_curve_annual on _sb)
    raw_run = _make_run("run-MD-001", ["MD"], ["0"])
    raw_run["args"]["file_types"] = ["metadata"]
    finish_run(raw_run)
    upsert_run(raw_dir, raw_run)

    with patch("data.resstock.manifest._list_s3_files", return_value=[]):
        report = check_integrity(
            release=release,
            state="MD",
            ebs_base=ebs_base,
            s3_base="s3://data.sb/nrel/resstock",
            location="ebs",
            output_path=tmp_path / "report.yaml",
        )

    unexpected = [
        m
        for m in report["mismatches"]
        if m.get("issue") == "unexpected_file_type"
        and m.get("file_type") == "load_curve_annual"
    ]
    assert unexpected, f"Expected unexpected_file_type for load_curve_annual:\n{report}"


def test_check_integrity_flags_missing_file_type(tmp_path: Path) -> None:
    """Manifest expects metadata but directory is absent → missing_file_type."""
    from data.resstock.manifest import check_integrity

    ebs_base = str(tmp_path)
    release = "res_2024_amy2018_2"
    sb_dir = tmp_path / f"{release}_sb"
    raw_dir = tmp_path / release
    sb_dir.mkdir(parents=True)
    raw_dir.mkdir(parents=True)

    sb_run = _make_run("run-MD-001", ["MD"], ["0"])
    sb_run["args"]["file_types"] = ["metadata", "load_curve_hourly"]
    record_step(sb_run, "clone")
    finish_run(sb_run)
    upsert_run(sb_dir, sb_run)

    raw_run = _make_run("run-MD-001", ["MD"], ["0"])
    raw_run["args"]["file_types"] = ["metadata"]
    finish_run(raw_run)
    upsert_run(raw_dir, raw_run)

    with patch("data.resstock.manifest._list_s3_files", return_value=[]):
        report = check_integrity(
            release=release,
            state="MD",
            ebs_base=ebs_base,
            s3_base="s3://data.sb/nrel/resstock",
            location="ebs",
            output_path=tmp_path / "report.yaml",
        )

    missing = [
        m
        for m in report["mismatches"]
        if m.get("issue") == "missing_file_type" and m.get("release") == f"{release}_sb"
    ]
    assert any(m["file_type"] == "metadata" for m in missing)
    assert any(m["file_type"] == "load_curve_hourly" for m in missing)


def test_check_integrity_flags_timestamp_mismatch(tmp_path: Path) -> None:
    """Newest file mtime after the step completed_at (+ tolerance) → timestamp_mismatch."""
    import os
    import time

    from data.resstock.manifest import check_integrity

    ebs_base = str(tmp_path)
    release = "res_2024_amy2018_2"
    release_sb = f"{release}_sb"
    sb_dir = tmp_path / release_sb
    raw_dir = tmp_path / release

    sb_run = _make_run("run-MD-001", ["MD"], ["0"])
    sb_run["args"]["file_types"] = ["metadata"]
    record_step(sb_run, "clone")
    record_step(sb_run, "assign_utility")
    finish_run(sb_run)
    upsert_run(sb_dir, sb_run)

    raw_run = _make_run("run-MD-001", ["MD"], ["0"])
    raw_run["args"]["file_types"] = ["metadata"]
    finish_run(raw_run)
    upsert_run(raw_dir, raw_run)

    # Create files AFTER the step timestamps, with future mtime
    for d in (
        sb_dir / "metadata" / "state=MD" / "upgrade=00",
        sb_dir / "metadata_utility" / "state=MD",
    ):
        d.mkdir(parents=True)
        f = d / "data.parquet"
        f.write_bytes(b"late")
        os.utime(f, (time.time() + 3600, time.time() + 3600))

    with patch("data.resstock.manifest._list_s3_files", return_value=[]):
        report = check_integrity(
            release=release,
            state="MD",
            ebs_base=ebs_base,
            s3_base="s3://data.sb/nrel/resstock",
            location="ebs",
            tolerance_seconds=300,
            output_path=tmp_path / "report.yaml",
        )

    ts_issues = [
        m for m in report["mismatches"] if m.get("issue") == "timestamp_mismatch"
    ]
    assert ts_issues, f"Expected timestamp_mismatch:\n{report}"
