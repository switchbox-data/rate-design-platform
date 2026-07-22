"""Provenance manifest for the ResStock data pipeline.

Each pipeline run appends a run record to ``manifest.yaml`` inside the release
directory.  The manifest travels with the data (synced to S3 by ``_upload``), so
reading ``manifest.yaml`` from either local EBS or S3 always answers "what code
produced this data, when, and with what parameters?"

Git merge handling: because runs *append* rather than overwrite, the full
lineage is preserved.  The last entry in ``runs`` is always the most recent
pipeline invocation.  If branch A produced data, then branch B re-runs after a
merge, both entries are visible.

Status check examples::

    # Default release from config.yaml, both EBS and S3:
    uv run python -m data.resstock.manifest

    # Specific release (pass the full name; _sb suffix is auto-stripped):
    uv run python -m data.resstock.manifest --release res_2024_amy2018_2
    uv run python -m data.resstock.manifest --release res_2024_amy2018_2_sb

    # Filter by state and/or upgrade:
    uv run python -m data.resstock.manifest --state NY
    uv run python -m data.resstock.manifest --upgrade 0 2
    uv run python -m data.resstock.manifest --state NY --upgrade 2

    # Narrow to one variant or one location:
    uv run python -m data.resstock.manifest --variant sb
    uv run python -m data.resstock.manifest --location ebs
    uv run python -m data.resstock.manifest --location s3

    # Show last 5 runs with verbose detail:
    uv run python -m data.resstock.manifest --history 5 -v
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from data.resstock.constants import CONFIG_PATH

MANIFEST_FILENAME = "manifest.yaml"


def _git_info() -> dict[str, Any]:
    """Capture current git commit, branch, and dirty status."""

    def _run(cmd: list[str]) -> str:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        return result.stdout.strip() if result.returncode == 0 else "unknown"

    commit = _run(["git", "rev-parse", "HEAD"])
    branch = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    dirty_out = _run(["git", "status", "--porcelain"])
    dirty = bool(dirty_out and dirty_out != "unknown")
    # Record which files are dirty so the run is reproducible even from a dirty tree.
    dirty_files = dirty_out.splitlines() if dirty else []
    return {
        "git_commit": commit,
        "git_branch": branch,
        "git_dirty": dirty,
        "git_dirty_files": dirty_files,
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_run_id() -> str:
    """Globally unique run ID: timestamp + UUID4 suffix to avoid same-second collisions."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}-{uuid.uuid4().hex[:8]}"


def read_manifest(release_dir: Path) -> dict[str, Any]:
    """Read an existing manifest, or return an empty one."""
    path = release_dir / MANIFEST_FILENAME
    if path.exists():
        with path.open() as f:
            return yaml.safe_load(f) or {}
    return {}


def write_manifest(release_dir: Path, manifest: dict[str, Any]) -> Path:
    """Write the manifest to ``release_dir/manifest.yaml``."""
    release_dir.mkdir(parents=True, exist_ok=True)
    path = release_dir / MANIFEST_FILENAME
    with path.open("w") as f:
        yaml.dump(manifest, f, default_flow_style=False, sort_keys=False)
    return path


def _reconstruct_command() -> str:
    """Reconstruct the full CLI command from sys.argv, suitable for copy-paste re-execution."""
    return "uv run python " + shlex.join(sys.argv)


def new_run_record(
    *,
    release: str,
    release_sb: str,
    states: list[str],
    upgrade_ids: list[str],
    file_types: list[str],
    flags: dict[str, Any],
) -> dict[str, Any]:
    """Create a new run record (not yet written to disk)."""
    return {
        "run_id": _new_run_id(),
        "started_at": _now_iso(),
        "command": _reconstruct_command(),
        **_git_info(),
        "release": release,
        "release_sb": release_sb,
        "args": {
            "states": states,
            "upgrade_ids": upgrade_ids,
            "file_types": file_types,
            **flags,
        },
        "status": "in_progress",
        "steps": [],
    }


def record_step(run: dict[str, Any], step_name: str, **details: Any) -> None:
    """Append a completed step to the run record."""
    entry: dict[str, Any] = {
        "step": step_name,
        "completed_at": _now_iso(),
    }
    if details:
        entry["details"] = details
    run["steps"].append(entry)


def upsert_run(release_dir: Path, run: dict[str, Any]) -> Path:
    """Write the run record to the manifest, updating in place if the run_id already exists.

    This is safe to call after every step: the first call appends a new entry,
    and subsequent calls update the same entry rather than creating duplicates.
    """
    manifest = read_manifest(release_dir)
    runs: list[dict[str, Any]] = manifest.get("runs", [])
    run_id = run["run_id"]
    for i, existing in enumerate(runs):
        if existing.get("run_id") == run_id:
            runs[i] = run
            manifest["runs"] = runs
            return write_manifest(release_dir, manifest)
    runs.append(run)
    manifest["runs"] = runs
    return write_manifest(release_dir, manifest)


def finish_run(run: dict[str, Any]) -> None:
    """Mark a run as successfully completed (mutates the dict in place)."""
    run["status"] = "completed"
    run["completed_at"] = _now_iso()


def fail_run(run: dict[str, Any], error: str) -> None:
    """Mark a run as failed (mutates the dict in place)."""
    run["status"] = "failed"
    run["failed_at"] = _now_iso()
    run["error"] = error


_EXIT_CODE_SIGNALS: dict[int, str] = {
    137: "SIGKILL (likely OOM killer)",
    139: "SIGSEGV (segmentation fault)",
    134: "SIGABRT (abort)",
    136: "SIGFPE (floating-point exception)",
    143: "SIGTERM (terminated)",
}


def _crash_reason(exit_code: int | None) -> str:
    """Human-readable crash reason from an exit code (or None if unknown)."""
    if exit_code is not None and exit_code in _EXIT_CODE_SIGNALS:
        return f"Process exited with code {exit_code}: {_EXIT_CODE_SIGNALS[exit_code]}."
    if exit_code is not None:
        return f"Process exited with code {exit_code} (no Python exception was raised)."
    return (
        "Process was killed before completing (no Python exception was raised). "
        "Likely cause: OOM killer (exit code 137), hard reboot, or SIGKILL."
    )


def mark_crashed_runs(
    release_dir: Path,
    *,
    exit_code: int | None = None,
) -> int:
    """Retroactively mark any stale ``in_progress`` runs in the manifest as ``crashed``.

    When a pipeline run is killed unexpectedly (e.g. by the OOM killer, a hard
    reboot, or ``kill -9``), Python never executes the ``except`` block, so
    ``fail_run`` is never called and the manifest entry is left perpetually
    ``in_progress``.

    There are two invocation patterns:

    1. **From the Justfile wrapper** — immediately after the pipeline process
       exits, the wrapper calls ``manifest --mark-crashed --exit-code <N>``.
       The actual exit code is recorded so the crash reason is precise.
    2. **At startup of the next run** — ``main.py`` calls this without
       ``exit_code`` as a safety net (e.g. when running without the Justfile).

    Returns the number of runs that were updated.
    """
    if not release_dir.is_dir():
        return 0
    manifest = read_manifest(release_dir)
    runs: list[dict[str, Any]] = manifest.get("runs", [])
    updated = 0
    for run in runs:
        if run.get("status") == "in_progress":
            run["status"] = "crashed"
            run["crashed_at"] = _now_iso()
            run["error"] = _crash_reason(exit_code)
            if exit_code is not None:
                run["exit_code"] = exit_code
            updated += 1
    if updated:
        manifest["runs"] = runs
        write_manifest(release_dir, manifest)
        print(
            f"  NOTE: marked {updated} stale in_progress run(s) as crashed "
            f"in {release_dir / MANIFEST_FILENAME}.",
            flush=True,
        )
    return updated


# ── Status inspection ─────────────────────────────────────────────────────────


def read_manifest_from_s3(s3_uri: str) -> dict[str, Any]:
    """Download and parse manifest.yaml from S3. Returns empty dict if not found."""
    with tempfile.NamedTemporaryFile(suffix=".yaml") as tmp:
        result = subprocess.run(
            ["aws", "s3", "cp", s3_uri, tmp.name],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return {}
        with open(tmp.name) as f:
            return yaml.safe_load(f) or {}


def _filter_runs(
    runs: list[dict[str, Any]],
    *,
    states: list[str] | None = None,
    upgrades: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return only runs whose args match the requested state/upgrade filters."""
    if not states and not upgrades:
        return runs
    filtered: list[dict[str, Any]] = []
    for run in runs:
        args = run.get("args", {})
        if states:
            run_states = args.get("states", [])
            if not any(s in run_states for s in states):
                continue
        if upgrades:
            run_upgrades = args.get("upgrade_ids", [])
            if not any(u in run_upgrades for u in upgrades):
                continue
        filtered.append(run)
    return filtered


def _format_run(run: dict[str, Any], verbose: bool = False) -> str:
    """Format a single run record for display."""
    lines = [
        f"  run_id:     {run.get('run_id', '?')}",
        f"  git_commit: {run.get('git_commit', '?')}",
        f"  git_branch: {run.get('git_branch', '?')}",
        f"  git_dirty:  {run.get('git_dirty', '?')}",
        f"  command:    {run.get('command', '?')}",
    ]
    args = run.get("args", {})
    lines.append(f"  states:     {', '.join(args.get('states', ['?']))}")
    lines.append(f"  upgrades:   {', '.join(args.get('upgrade_ids', ['?']))}")
    steps = run.get("steps", [])
    step_names = [s.get("step", "?") for s in steps]
    lines.append(f"  steps:      {' → '.join(step_names)}")
    if verbose:
        flags = {
            k: v
            for k, v in args.items()
            if k not in ("states", "upgrade_ids", "file_types")
        }
        if flags:
            lines.append(f"  flags:      {flags}")
        lines.append(f"  file_types: {', '.join(args.get('file_types', ['?']))}")
    return "\n".join(lines)


def _format_location(
    label: str,
    manifest: dict[str, Any],
    current_commit: str,
    n_history: int,
    verbose: bool,
    states: list[str] | None = None,
    upgrades: list[str] | None = None,
) -> str:
    """Format manifest info for one location (EBS or S3)."""
    all_runs = manifest.get("runs", [])
    if not all_runs:
        return f"\n{label}:\n  No manifest found (no pipeline runs recorded).\n"

    runs = _filter_runs(all_runs, states=states, upgrades=upgrades)
    if not runs:
        filter_desc = _describe_filter(states, upgrades)
        return (
            f"\n{label}:\n"
            f"  No runs match filter ({filter_desc}).\n"
            f"  total_runs (unfiltered): {len(all_runs)}\n"
        )

    lines = [f"\n{label}:"]

    if states or upgrades:
        lines.append(f"  filter:     {_describe_filter(states, upgrades)}")
        lines.append(f"  matched:    {len(runs)} of {len(all_runs)} total runs")

    last_run = runs[-1]
    lines.append("── Latest run ──")
    lines.append(_format_run(last_run, verbose=verbose))

    data_commit = last_run.get("git_commit", "unknown")
    if data_commit == current_commit:
        lines.append("  status:     ✓ matches current HEAD")
    else:
        lines.append(
            f"  status:     ✗ DIFFERS from current HEAD ({current_commit[:12]})"
        )
        lines.append("              Data was produced by a different commit.")
        lines.append(
            "              To recreate, checkout the commit above and re-run the command."
        )

    if n_history > 1 and len(runs) > 1:
        older = runs[-(n_history):-1]
        if older:
            lines.append(f"── Previous runs ({len(older)} shown) ──")
            for r in reversed(older):
                lines.append(_format_run(r, verbose=False))
                lines.append("  ---")

    lines.append(f"  total_runs: {len(all_runs)}")
    return "\n".join(lines)


def _describe_filter(states: list[str] | None, upgrades: list[str] | None) -> str:
    parts: list[str] = []
    if states:
        parts.append(f"state={','.join(states)}")
    if upgrades:
        parts.append(f"upgrade={','.join(upgrades)}")
    return " & ".join(parts) if parts else "none"


def print_status(
    release: str,
    release_sb: str,
    ebs_base: str,
    s3_base: str,
    n_history: int = 1,
    verbose: bool = False,
    states: list[str] | None = None,
    upgrades: list[str] | None = None,
    location: str | None = None,
    variant: str | None = None,
) -> None:
    """Print the provenance status of raw and/or _sb releases on EBS and/or S3.

    Parameters
    ----------
    location
        ``"ebs"`` to check only local, ``"s3"`` to check only remote, or
        ``None`` (default) to check both.
    variant
        ``"raw"`` for the unmodified release, ``"sb"`` for the ``_sb``
        release, or ``None`` (default) for both.
    states
        If given, only show runs whose ``args.states`` includes at least one
        of these values.
    upgrades
        If given, only show runs whose ``args.upgrade_ids`` includes at least
        one of these values.
    """
    git = _git_info()
    current_commit = git["git_commit"]
    current_branch = git["git_branch"]
    print(f"Current HEAD: {current_commit} ({current_branch})")

    releases: list[str] = []
    if variant is None or variant == "raw":
        releases.append(release)
    if variant is None or variant == "sb":
        releases.append(release_sb)

    check_ebs = location is None or location == "ebs"
    check_s3 = location is None or location == "s3"

    for rel in releases:
        ebs_dir = Path(ebs_base) / rel
        s3_manifest_uri = f"{s3_base.rstrip('/')}/{rel}/{MANIFEST_FILENAME}"

        ebs_manifest: dict[str, Any] = {}
        s3_manifest: dict[str, Any] = {}

        if check_ebs:
            ebs_manifest = read_manifest(ebs_dir)
            print(
                _format_location(
                    f"EBS  {ebs_dir}",
                    ebs_manifest,
                    current_commit,
                    n_history,
                    verbose,
                    states=states,
                    upgrades=upgrades,
                )
            )

        if check_s3:
            s3_manifest = read_manifest_from_s3(s3_manifest_uri)
            print(
                _format_location(
                    f"S3   {s3_manifest_uri.rsplit('/', 1)[0]}",
                    s3_manifest,
                    current_commit,
                    n_history,
                    verbose,
                    states=states,
                    upgrades=upgrades,
                )
            )

        if check_ebs and check_s3:
            ebs_runs = _filter_runs(
                ebs_manifest.get("runs", []), states=states, upgrades=upgrades
            )
            s3_runs = _filter_runs(
                s3_manifest.get("runs", []), states=states, upgrades=upgrades
            )
            if not ebs_runs or not s3_runs:
                filter_desc = _describe_filter(states, upgrades)
                print(
                    f"  EBS ↔ S3:   ? cannot determine sync "
                    f"(no matching runs on {'EBS' if not ebs_runs else 'S3'}"
                    f" for filter: {filter_desc})"
                )
            else:
                ebs_last = ebs_runs[-1].get("run_id")
                s3_last = s3_runs[-1].get("run_id")
                if ebs_last == s3_last:
                    print("  EBS ↔ S3:   ✓ in sync (same last run_id)")
                else:
                    print("  EBS ↔ S3:   ✗ OUT OF SYNC")
                    print(f"              EBS last run: {ebs_last}")
                    print(f"              S3  last run: {s3_last}")
                    print(
                        "              One location has newer data than the other."
                        " Re-run the pipeline or sync manually."
                    )
        print()


# ── Integrity check ───────────────────────────────────────────────────────────

# Maps pipeline step names to the file-type directories they create or modify.
# Used to derive (1) which file types the manifest expects and (2) the
# most recent ``completed_at`` timestamp for each file type.
_STEP_FILE_TYPES: dict[str, frozenset[str]] = {
    "fetch": frozenset(),  # filled from args.file_types at runtime (raw only)
    "clone": frozenset(),  # filled from args.file_types minus SB_EXCLUDED (_sb only)
    "modify_metadata": frozenset({"metadata"}),
    "assign_utility": frozenset({"metadata_utility"}),
    "approximate_non_hp_load": frozenset({"load_curve_hourly"}),
    "adjust_mf_electricity": frozenset({"load_curve_hourly"}),
    "add_monthly_loads": frozenset({"load_curve_monthly"}),
    "upload_raw": frozenset(),  # does not create new types
    "upload_sb": frozenset(),
}


def _parse_s3_ls_line(line: str) -> tuple[str, int, datetime] | None:
    """Parse one line of ``aws s3 ls --recursive`` output.

    Each line looks like:  2026-07-17 17:25:27    12345 path/to/file.parquet
    Returns (key, size_bytes, last_modified) or None on parse failure.
    """
    parts = line.split(maxsplit=3)
    if len(parts) < 4:
        return None
    try:
        ts = datetime.fromisoformat(f"{parts[0]}T{parts[1]}+00:00")
        size = int(parts[2])
        key = parts[3]
        return (key, size, ts)
    except (ValueError, IndexError):
        return None


def _list_s3_files(s3_prefix: str) -> list[tuple[str, int, datetime]]:
    """Recursively list all objects under an S3 prefix.

    Returns a list of (relative_key, size_bytes, last_modified_utc).
    The relative_key is stripped of the prefix so it matches local paths.
    """
    result = subprocess.run(
        ["aws", "s3", "ls", "--recursive", s3_prefix],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return []

    prefix_path = s3_prefix.split("/", 3)[-1] if s3_prefix.count("/") >= 3 else ""

    entries: list[tuple[str, int, datetime]] = []
    for line in result.stdout.splitlines():
        parsed = _parse_s3_ls_line(line)
        if parsed:
            key, size, ts = parsed
            rel_key = key.removeprefix(prefix_path).lstrip("/")
            entries.append((rel_key, size, ts))
    return entries


def _list_ebs_files(local_dir: Path, state: str) -> list[tuple[str, int, datetime]]:
    """List all files under a local release directory filtered by state partition.

    Returns (relative_path, size_bytes, mtime_utc).
    """
    entries: list[tuple[str, int, datetime]] = []
    if not local_dir.is_dir():
        return entries
    for f in local_dir.rglob("*"):
        if not f.is_file():
            continue
        rel = str(f.relative_to(local_dir))
        if f"state={state}" not in rel:
            continue
        if rel == MANIFEST_FILENAME or rel.startswith(f"{MANIFEST_FILENAME}/"):
            continue
        stat = f.stat()
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        entries.append((rel, stat.st_size, mtime))
    return entries


def _file_types_from_paths(paths: list[str]) -> set[str]:
    """Extract top-level file-type directory names from relative paths."""
    types: set[str] = set()
    for p in paths:
        top = p.split("/", 1)[0]
        if top and top != MANIFEST_FILENAME:
            types.add(top)
    return types


def _expected_file_types(runs: list[dict[str, Any]], *, is_sb: bool) -> set[str]:
    """Derive expected file-type directories from *all* matching manifest runs.

    Pipeline runs are additive: a later run that only fetches load curves does
    not erase metadata written by an earlier run.  So the expected set is the
    **union** across every run that touched this state, not just the latest.

    For the raw release: union of ``args.file_types`` over all runs.
    For the _sb release: the same union minus ``SB_EXCLUDED_FILE_TYPES``, plus
    any types produced by completed steps (``metadata_utility``,
    ``load_curve_monthly``, etc.).
    """
    from data.resstock.constants import SB_EXCLUDED_FILE_TYPES

    expected: set[str] = set()
    for run in runs:
        base_types = set(run.get("args", {}).get("file_types", []))
        if is_sb:
            expected |= {ft for ft in base_types if ft not in SB_EXCLUDED_FILE_TYPES}
            for step in run.get("steps", []):
                expected |= set(_STEP_FILE_TYPES.get(step.get("step", ""), frozenset()))
        else:
            expected |= base_types
    return expected


def _latest_step_times_by_file_type(
    runs: list[dict[str, Any]], *, is_sb: bool
) -> dict[str, datetime]:
    """Map each file type to the newest step ``completed_at`` across all runs."""
    from data.resstock.constants import SB_EXCLUDED_FILE_TYPES

    times: dict[str, datetime] = {}

    for run in runs:
        args = run.get("args", {})
        base_types = set(args.get("file_types", []))
        if is_sb:
            base_types = {ft for ft in base_types if ft not in SB_EXCLUDED_FILE_TYPES}

        for step in run.get("steps", []):
            name = step.get("step", "")
            completed_raw = step.get("completed_at")
            if not completed_raw:
                continue
            completed = datetime.fromisoformat(completed_raw).astimezone(timezone.utc)

            if name == "fetch" and not is_sb:
                touched = base_types
            elif name == "clone" and is_sb:
                touched = base_types
            elif name == "upload_raw" and not is_sb:
                touched = base_types
            elif name == "upload_sb" and is_sb:
                touched = set(base_types)
                for s in run.get("steps", []):
                    touched |= set(_STEP_FILE_TYPES.get(s.get("step", ""), frozenset()))
            else:
                touched = set(_STEP_FILE_TYPES.get(name, frozenset()))

            for ft in touched:
                if ft not in times or completed > times[ft]:
                    times[ft] = completed

    return times


def _aggregate_mtimes_by_file_type(
    files: list[tuple[str, int, datetime]],
) -> dict[str, tuple[datetime, datetime, int]]:
    """Group files by top-level file type.

    Returns ``{file_type: (min_mtime, max_mtime, n_files)}``.
    """
    agg: dict[str, tuple[datetime, datetime, int]] = {}
    for path, _size, mtime in files:
        ft = path.split("/", 1)[0]
        if ft == MANIFEST_FILENAME:
            continue
        if ft not in agg:
            agg[ft] = (mtime, mtime, 1)
        else:
            mn, mx, n = agg[ft]
            agg[ft] = (min(mn, mtime), max(mx, mtime), n + 1)
    return agg


def check_integrity(
    *,
    release: str,
    state: str,
    ebs_base: str,
    s3_base: str,
    tolerance_seconds: int = 300,
    output_path: Path | None = None,
    location: str = "both",
) -> dict[str, Any]:
    """Check that file-type directories match the manifest (bijection + timestamps).

    For each of the raw and ``_sb`` releases, scoped to *state*:

    1. **Expected file types** are the **union** across *all* matching
       manifest runs (not just the latest).  Pipeline runs are additive: a
       later run that only fetches load curves does not erase metadata from
       an earlier run.  For ``_sb``, ``SB_EXCLUDED_FILE_TYPES`` are dropped
       and step-produced types (``metadata_utility``, ``load_curve_monthly``)
       are included.
    2. **Actual file types** are the top-level directories under the release that
       contain ``state=<state>`` data, on EBS and/or S3.
    3. Fail if a type exists on disk/S3 but is not expected by the manifest,
       or if the manifest expects a type that is missing.
    4. For each matching type, compare the newest file mtime against the
       newest manifest step (across all runs) that touched that type.  Fail
       if the newest file is more than ``tolerance_seconds`` after that step.
    """
    from concurrent.futures import ThreadPoolExecutor

    from tqdm import tqdm

    release_sb = f"{release}_sb"
    releases_to_check = [release, release_sb]

    check_ebs = location in ("ebs", "both")
    check_s3 = location in ("s3", "both")

    report: dict[str, Any] = {
        "state": state,
        "checked_at": _now_iso(),
        "tolerance_seconds": tolerance_seconds,
        "location": location,
        "releases_checked": releases_to_check,
        "mismatches": [],
        "summary": "",
    }

    all_mismatches: list[dict[str, Any]] = []

    for rel in releases_to_check:
        is_sb = rel.endswith("_sb")
        ebs_dir = Path(ebs_base) / rel
        s3_prefix = f"{s3_base.rstrip('/')}/{rel}/"

        # Prefer the local manifest; fall back to S3 if EBS has none.
        manifest = read_manifest(ebs_dir)
        if not manifest.get("runs") and check_s3:
            manifest = read_manifest_from_s3(f"{s3_prefix}{MANIFEST_FILENAME}")

        runs = _filter_runs(manifest.get("runs", []), states=[state])
        if not runs:
            all_mismatches.append(
                {
                    "release": rel,
                    "location": "manifest",
                    "issue": f"No pipeline run found for state={state}",
                    "file_type": None,
                }
            )
            continue

        expected = _expected_file_types(runs, is_sb=is_sb)
        step_times = _latest_step_times_by_file_type(runs, is_sb=is_sb)
        run_ids = [r.get("run_id", "?") for r in runs]

        print(
            f"  [{rel}] Expected file types from {len(runs)} manifest run(s): "
            f"{sorted(expected) or '(none)'}",
            flush=True,
        )

        # List files for this state
        loc_labels = [x for x, on in (("EBS", check_ebs), ("S3", check_s3)) if on]
        print(
            f"  [{rel}] Listing state={state} files ({' + '.join(loc_labels)})...",
            flush=True,
        )

        ebs_files: list[tuple[str, int, datetime]] = []
        s3_files: list[tuple[str, int, datetime]] = []

        if check_ebs and check_s3:
            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_ebs = pool.submit(_list_ebs_files, ebs_dir, state)
                fut_s3 = pool.submit(_list_s3_files, s3_prefix)
                ebs_files = fut_ebs.result()
                s3_files = [
                    (k, sz, ts)
                    for k, sz, ts in fut_s3.result()
                    if f"state={state}" in k and not k.endswith(MANIFEST_FILENAME)
                ]
        elif check_ebs:
            ebs_files = _list_ebs_files(ebs_dir, state)
        else:
            s3_files = [
                (k, sz, ts)
                for k, sz, ts in _list_s3_files(s3_prefix)
                if f"state={state}" in k and not k.endswith(MANIFEST_FILENAME)
            ]

        checks: list[tuple[str, list[tuple[str, int, datetime]]]] = []
        if check_ebs:
            checks.append(("ebs", ebs_files))
        if check_s3:
            checks.append(("s3", s3_files))

        for loc_name, files in checks:
            actual_types = _file_types_from_paths([p for p, _, _ in files])
            print(
                f"  [{rel}/{loc_name}] Actual file types: "
                f"{sorted(actual_types) or '(none)'} "
                f"({len(files):,} files)",
                flush=True,
            )

            unexpected = sorted(actual_types - expected)
            missing = sorted(expected - actual_types)

            for ft in unexpected:
                all_mismatches.append(
                    {
                        "release": rel,
                        "location": loc_name,
                        "issue": "unexpected_file_type",
                        "file_type": ft,
                        "detail": (
                            f"Directory '{ft}/' exists for state={state} but is not "
                            f"expected from the union of {len(runs)} manifest run(s) "
                            f"(run_ids={run_ids})."
                        ),
                    }
                )

            for ft in missing:
                all_mismatches.append(
                    {
                        "release": rel,
                        "location": loc_name,
                        "issue": "missing_file_type",
                        "file_type": ft,
                        "detail": (
                            f"Manifest expects '{ft}/' for state={state} "
                            f"(union of run_ids={run_ids}) but it was not found."
                        ),
                    }
                )

            # Timestamp check for types present in both expected and actual
            mtimes = _aggregate_mtimes_by_file_type(files)
            shared = sorted(expected & actual_types)
            with tqdm(
                total=len(shared),
                desc=f"  {rel}/{loc_name} timestamps",
                unit="type",
                leave=True,
            ) as pbar:
                for ft in shared:
                    pbar.update(1)
                    _mn, mx, n_files = mtimes[ft]
                    step_ts = step_times.get(ft)
                    if step_ts is None:
                        # No step timestamp for this type — skip time check
                        continue
                    cutoff = step_ts + timedelta(seconds=tolerance_seconds)
                    if mx > cutoff:
                        all_mismatches.append(
                            {
                                "release": rel,
                                "location": loc_name,
                                "issue": "timestamp_mismatch",
                                "file_type": ft,
                                "detail": (
                                    f"Newest file in '{ft}/' "
                                    f"({mx.isoformat(timespec='seconds')}) is "
                                    f"{int((mx - step_ts).total_seconds())}s after "
                                    f"the latest manifest step for this type "
                                    f"({step_ts.isoformat(timespec='seconds')}); "
                                    f"tolerance={tolerance_seconds}s. "
                                    f"({n_files:,} files checked)"
                                ),
                                "file_mtime_max": mx.isoformat(timespec="seconds"),
                                "step_completed_at": step_ts.isoformat(
                                    timespec="seconds"
                                ),
                                "delta_seconds": int((mx - step_ts).total_seconds()),
                                "n_files": n_files,
                            }
                        )

    report["mismatches"] = all_mismatches

    n = len(all_mismatches)
    if n == 0:
        report["summary"] = (
            f"All file types for state={state} match the manifest on "
            f"{location} (set equality + timestamps within {tolerance_seconds}s)."
        )
    else:
        by_issue: dict[str, int] = {}
        for m in all_mismatches:
            by_issue[m["issue"]] = by_issue.get(m["issue"], 0) + 1
        parts = [f"{count} {issue}" for issue, count in sorted(by_issue.items())]
        report["summary"] = f"Found {n} integrity issue(s): " + ", ".join(parts) + "."

    if output_path is None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = Path(__file__).parent / f"integrity_report_{state}_{ts}.yaml"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        yaml.dump(report, f, default_flow_style=False, sort_keys=False)

    print(f"\n{'=' * 72}")
    print(f"Integrity check: state={state}")
    print(f"{'=' * 72}")
    print(f"  Releases:  {', '.join(releases_to_check)}")
    print(f"  Location:  {location}")
    print(f"  Tolerance: {tolerance_seconds}s")
    print(f"  Result:    {report['summary']}")
    if all_mismatches:
        print("\n  Issues:")
        for m in all_mismatches[:30]:
            print(
                f"    [{m['location'].upper()}] {m['release']}/"
                f"{m.get('file_type') or '?'} — {m['issue']}"
            )
            if m.get("detail"):
                print(f"      {m['detail']}")
        if n > 30:
            print(f"    ... and {n - 30} more (see full report)")
    print(f"\n  Full report: {output_path}")
    print(f"{'=' * 72}\n")

    return report


def _status_main() -> None:
    """CLI entry point for ``uv run python -m data.resstock.manifest``."""
    with CONFIG_PATH.open() as f:
        cfg = yaml.safe_load(f)

    rs = cfg["resstock"]

    default_release = (
        f"res_{rs['release_year']}_{rs['weather_file']}_{rs['release_version']}"
    )

    parser = argparse.ArgumentParser(
        description="Check provenance status of ResStock data on EBS and S3.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""examples:
  %(prog)s                                       # default release ({default_release})
  %(prog)s --release res_2024_amy2018_2          # specific release (raw and _sb)
  %(prog)s --release res_2024_amy2018_2_sb       # _sb release only (suffix auto-detected)
  %(prog)s --state NY                            # only runs that touched NY
  %(prog)s --upgrade 0 2                         # only runs that touched upgrade 0 or 2
  %(prog)s --state NY --upgrade 2                # NY upgrade 2 only
  %(prog)s --variant sb                          # only the _sb (modified) release
  %(prog)s --variant raw                         # only the raw (unmodified) release
  %(prog)s --location ebs                        # only check local EBS, skip S3
  %(prog)s --location s3                         # only check S3, skip EBS
  %(prog)s --history 5 -v                        # last 5 runs, verbose detail
""",
    )

    release_group = parser.add_argument_group(
        "release selection",
        "Pass the full release name (e.g. res_2024_amy2018_2 or res_2024_amy2018_2_sb). "
        "A trailing _sb suffix is stripped to derive the base name; both raw and _sb "
        "variants are checked unless --variant narrows it. "
        f"Defaults to {default_release} (from config.yaml).",
    )
    release_group.add_argument(
        "--release",
        default=default_release,
        metavar="NAME",
        help=f"Full release name (default: {default_release}).",
    )

    filter_group = parser.add_argument_group(
        "run filters",
        "Filter which runs to display from the manifest history.",
    )
    filter_group.add_argument(
        "--state",
        nargs="+",
        default=None,
        metavar="ST",
        help="Only show runs that included these state(s) (e.g. NY RI).",
    )
    filter_group.add_argument(
        "--upgrade",
        nargs="+",
        default=None,
        metavar="ID",
        help="Only show runs that included these upgrade ID(s) (e.g. 0 2).",
    )

    output_group = parser.add_argument_group("output options")
    output_group.add_argument(
        "--location",
        choices=["ebs", "s3"],
        default=None,
        help="Check only this location (default: both).",
    )
    output_group.add_argument(
        "--variant",
        choices=["raw", "sb"],
        default=None,
        help="Check only 'raw' (unmodified) or 'sb' (modified) release (default: both).",
    )
    output_group.add_argument(
        "--history",
        type=int,
        default=1,
        metavar="N",
        help="Number of recent runs to show (default: 1 = latest only).",
    )
    output_group.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show additional details (flags, file types).",
    )
    output_group.add_argument(
        "--mark-crashed",
        action="store_true",
        help=(
            "Scan the local manifest(s) and retroactively mark any stale "
            "in_progress runs as crashed. Use this after an unexpected kill "
            "(OOM, SIGKILL) to clean up the manifest without starting a new run. "
            "Called automatically by the Justfile run-pipeline wrapper."
        ),
    )
    output_group.add_argument(
        "--exit-code",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Exit code of the crashed process (used with --mark-crashed). "
            "Common codes: 137=OOM/SIGKILL, 139=SIGSEGV, 143=SIGTERM."
        ),
    )

    integrity_group = parser.add_argument_group(
        "integrity check",
        "Deep per-file comparison of actual modification times against the manifest. "
        "Checks both EBS and S3, for both raw and _sb releases.",
    )
    integrity_group.add_argument(
        "--check-integrity",
        action="store_true",
        help=(
            "Run a full integrity check for the given state(s). "
            "Requires --state.  Compares every file's modification time against "
            "the pipeline's completed_at timestamp.  Outputs a YAML report."
        ),
    )
    integrity_group.add_argument(
        "--tolerance",
        type=int,
        default=300,
        metavar="SECS",
        help=(
            "Seconds after pipeline completion within which a file's mtime is "
            "still considered in sync. Default 300 (5 minutes)."
        ),
    )
    integrity_group.add_argument(
        "--check-location",
        choices=["ebs", "s3", "both"],
        default="both",
        help=(
            "Which storage to check. 'ebs' is fast (local stat calls only). "
            "'s3' requires aws s3 ls --recursive and can take minutes for large releases. "
            "Default: both."
        ),
    )
    integrity_group.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help="Path to write the integrity report YAML. Default: auto-generated in data/resstock/.",
    )

    args = parser.parse_args()

    # Strip trailing _sb so the user can pass either form and get both variants.
    release = args.release.removesuffix("_sb")
    release_sb = f"{release}_sb"
    ebs_base = cfg["paths"]["output_dir"]

    if args.mark_crashed:
        for rel in (release, release_sb):
            d = Path(ebs_base) / rel
            n = mark_crashed_runs(d, exit_code=args.exit_code)
            if n == 0:
                print(f"  {d}: no stale in_progress runs found.")
        return

    if args.check_integrity:
        if not args.state:
            parser.error("--check-integrity requires --state")
        output_path = Path(args.output) if args.output else None
        for s in args.state:
            check_integrity(
                release=release,
                state=s,
                ebs_base=ebs_base,
                s3_base=cfg["paths"]["s3_dir"],
                tolerance_seconds=args.tolerance,
                location=args.check_location,
                output_path=output_path,
            )
        return

    print_status(
        release=release,
        release_sb=release_sb,
        ebs_base=ebs_base,
        s3_base=cfg["paths"]["s3_dir"],
        n_history=args.history,
        verbose=args.verbose,
        states=args.state,
        upgrades=args.upgrade,
        location=args.location,
        variant=args.variant,
    )


if __name__ == "__main__":
    _status_main()
