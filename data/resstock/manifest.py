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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

MANIFEST_FILENAME = "manifest.yaml"


def _git_info() -> dict[str, Any]:
    """Capture current git commit, branch, and dirty status."""

    def _run(cmd: list[str]) -> str:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        return result.stdout.strip() if result.returncode == 0 else "unknown"

    commit = _run(["git", "rev-parse", "HEAD"])
    branch = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    dirty_out = _run(["git", "status", "--porcelain"])
    return {
        "git_commit": commit,
        "git_branch": branch,
        "git_dirty": bool(dirty_out and dirty_out != "unknown"),
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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
        "run_id": _now_iso(),
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


def append_run(release_dir: Path, run: dict[str, Any]) -> Path:
    """Append a run record to the manifest on disk and return the manifest path."""
    manifest = read_manifest(release_dir)
    if "runs" not in manifest:
        manifest["runs"] = []
    manifest["runs"].append(run)
    return write_manifest(release_dir, manifest)


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
        lines.append(
            "              Data was produced by a different commit."
        )
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


def _describe_filter(
    states: list[str] | None, upgrades: list[str] | None
) -> str:
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
            print(_format_location(
                f"EBS  {ebs_dir}",
                ebs_manifest,
                current_commit,
                n_history,
                verbose,
                states=states,
                upgrades=upgrades,
            ))

        if check_s3:
            s3_manifest = read_manifest_from_s3(s3_manifest_uri)
            print(_format_location(
                f"S3   {s3_manifest_uri.rsplit('/', 1)[0]}",
                s3_manifest,
                current_commit,
                n_history,
                verbose,
                states=states,
                upgrades=upgrades,
            ))

        if check_ebs and check_s3:
            ebs_runs = ebs_manifest.get("runs", [])
            s3_runs = s3_manifest.get("runs", [])
            if ebs_runs and s3_runs:
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


def _status_main() -> None:
    """CLI entry point for ``uv run python -m data.resstock.manifest``."""
    config_path = Path(__file__).parent / "config.yaml"
    with config_path.open() as f:
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
        "--verbose", "-v",
        action="store_true",
        help="Show additional details (flags, file types).",
    )

    args = parser.parse_args()

    # Strip trailing _sb so the user can pass either form and get both variants.
    release = args.release.removesuffix("_sb")

    print_status(
        release=release,
        release_sb=f"{release}_sb",
        ebs_base=cfg["paths"]["output_dir"],
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
