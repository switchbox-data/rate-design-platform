"""Provenance manifests for marginal cost 8760 parquets.

Every MC writer appends one entry to ``manifest.yaml`` in the partition
directory of the file it wrote, e.g.
``.../dist_and_sub_tx/utility=ct_eversource/year=2025/manifest.yaml``. The
manifest is append-only. Entries name the file they describe (``data.parquet``
or ``zero.parquet``), and the latest entry for a file name describes the file
currently at that path.

An entry records the method's structured params and the key built from them,
a source citation, the file's sha256, the inputs (path and sha256), a numeric
summary, the exact command, and the commit that wrote it. Writing an entry
requires that commit to be clean, pushed, and tagged ``mc/<sha12>`` on
``origin`` (``require_published_code``), so the entry's commit and command
always rebuild the file.

Consumers check files with ``verify_mc_file`` (pipeline preflight),
``commit_in_history`` (preflight warning when the generator commit is not in
the run's history), and ``describe_mc_file`` (per-run record in the CAIRO
output directory).

Keys and methods are documented in
``context/code/marginal_costs/mc_provenance_manifest.md``.
"""

from __future__ import annotations

import hashlib
import json
import shlex
import subprocess
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import polars as pl
import yaml
from cloudpathlib import S3Path

MANIFEST_FILENAME = "manifest.yaml"

MC_TYPES: frozenset[str] = frozenset(
    {
        "dist_and_sub_tx",
        "bulk_tx",
        "supply_energy",
        "supply_capacity",
        "supply_ancillary",
    }
)

ZERO_METHOD = "zero"

PATH_REPO = Path(__file__).resolve().parents[3]


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------

KeyBuilder = Callable[[Mapping[str, Any]], str]


def _num(value: float) -> str:
    """Format a number for a key: ``69.0`` -> ``69``, ``21.22`` -> ``21p22``."""
    return f"{float(value):g}".replace(".", "p").replace("-", "m")


def _key_dist_pop(p: Mapping[str, Any]) -> str:
    return (
        f"pop_{p['load_source']}_{p['n_hours']}h"
        f"_load{p['load_year']}_{p['target_dollar_year']}usd"
    )


def _key_dist_month_hour(p: Mapping[str, Any]) -> str:
    holidays = "holidays" if p["include_holidays"] else "noholidays"
    return (
        f"month_hour_{p['table_source']}_{p['voltage']}_{holidays}"
        f"_cal{p['calendar_year']}"
    )


def _key_bulk_isone(p: Mapping[str, Any]) -> str:
    return (
        f"aesc_ptf{_num(p['ptf_kw_year'])}_{p['allocation_load']}"
        f"_{p['n_peak_hours']}h_load{p['load_year']}"
    )


def _key_bulk_month_hour(p: Mapping[str, Any]) -> str:
    holidays = "holidays" if p["include_holidays"] else "noholidays"
    return (
        f"month_hour_{p['table_source']}_aesc_ptf{_num(p['ptf_kw_year'])}"
        f"_{holidays}_cal{p['calendar_year']}_{p['target_dollar_year']}usd"
    )


def _key_bulk_pjm(p: Mapping[str, Any]) -> str:
    return f"pjm_nits_pcaf_{p['k_peak_hours']}h_load{p['load_year']}"


def _key_bulk_nyiso(p: Mapping[str, Any]) -> str:
    return f"nyiso_scr_{p['scr_hours_per_season']}h_load{p['load_year']}"


def _key_zero(_: Mapping[str, Any]) -> str:
    return "zero"


KEY_BUILDERS: dict[tuple[str, str], KeyBuilder] = {
    ("dist_and_sub_tx", "pop"): _key_dist_pop,
    ("dist_and_sub_tx", "month_hour"): _key_dist_month_hour,
    ("bulk_tx", "isone_aesc_ptf"): _key_bulk_isone,
    ("bulk_tx", "month_hour"): _key_bulk_month_hour,
    ("bulk_tx", "pjm_nits_pcaf"): _key_bulk_pjm,
    ("bulk_tx", "nyiso_scr"): _key_bulk_nyiso,
    **{(mc_type, ZERO_METHOD): _key_zero for mc_type in MC_TYPES},
}


def build_key(mc_type: str, method: str, params: Mapping[str, Any]) -> str:
    """Build the structured key for one (mc_type, method) from its params."""
    builder = KEY_BUILDERS.get((mc_type, method))
    if builder is None:
        raise ValueError(
            f"No key builder for mc_type={mc_type!r}, method={method!r}. "
            "Register one in mc_manifest.KEY_BUILDERS and document it in "
            "context/code/marginal_costs/mc_provenance_manifest.md."
        )
    try:
        return builder(params)
    except KeyError as exc:
        raise ValueError(
            f"Key for mc_type={mc_type!r}, method={method!r} needs param {exc}; "
            f"got {sorted(params)}"
        ) from exc


@dataclass(frozen=True, slots=True)
class McSpec:
    """What produced one MC 8760: method, structured params, source, inputs."""

    mc_type: str
    method: str
    utility: str
    year: int
    params: dict[str, Any]
    source: str | None = None
    inputs: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.mc_type not in MC_TYPES:
            raise ValueError(
                f"mc_type must be one of {sorted(MC_TYPES)}, got {self.mc_type!r}"
            )
        build_key(self.mc_type, self.method, self.params)

    @property
    def key(self) -> str:
        return build_key(self.mc_type, self.method, self.params)


def zero_spec(mc_type: str, utility: str, year: int) -> McSpec:
    """Spec for a zero-filled placeholder 8760 (delivery-only runs)."""
    return McSpec(
        mc_type=mc_type,
        method=ZERO_METHOD,
        utility=utility,
        year=year,
        params={},
        source="Zero-filled placeholder for delivery-only runs",
    )


# ---------------------------------------------------------------------------
# Paths and hashing
# ---------------------------------------------------------------------------


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _exists(path: str) -> bool:
    return S3Path(path).exists() if _is_s3(path) else Path(path).exists()


def _read_bytes(path: str) -> bytes:
    return S3Path(path).read_bytes() if _is_s3(path) else Path(path).read_bytes()


def _write_text(path: str, text: str) -> None:
    if _is_s3(path):
        S3Path(path).write_text(text)
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(text)


def _parquet_files_under(path_dir: str) -> list[str]:
    root = S3Path(path_dir) if _is_s3(path_dir) else Path(path_dir)
    return sorted(str(p) for p in root.rglob("*.parquet"))


def partition_file_path(
    base: str, utility: str, year: int, filename: str = "data.parquet"
) -> str:
    """``{base}/utility={utility}/year={year}/{filename}``."""
    return f"{str(base).rstrip('/')}/utility={utility}/year={year}/{filename}"


def manifest_path_for(path_file: str) -> str:
    """The manifest that describes ``path_file`` (same directory)."""
    return f"{str(path_file).rstrip('/').rsplit('/', 1)[0]}/{MANIFEST_FILENAME}"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_sha256(path: str) -> str:
    return sha256_bytes(_read_bytes(str(path)))


def params_sha256(params: Mapping[str, Any]) -> str:
    return sha256_bytes(json.dumps(params, sort_keys=True, default=str).encode())


def input_file(role: str, path: str) -> dict[str, Any]:
    """Input record for one file, hashed."""
    return {"role": role, "path": str(path), "sha256": file_sha256(str(path))}


def input_partition(role: str, path_dir: str) -> dict[str, Any]:
    """Input record for a directory of parquets (e.g. one hive partition).

    The hash covers each parquet's path relative to ``path_dir`` and its
    content hash, so adding, removing, or changing a file changes it.
    """
    root = str(path_dir).rstrip("/")
    files = _parquet_files_under(root)
    if not files:
        raise FileNotFoundError(f"No parquet files under {root}")
    lines = [f"{f[len(root) + 1 :]}\t{file_sha256(f)}" for f in files]
    return {
        "role": role,
        "path": f"{root}/",
        "n_files": len(files),
        "sha256": sha256_bytes("\n".join(lines).encode()),
    }


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

GIT_REMOTE = "origin"
MC_TAG_PREFIX = "mc/"


class UnpublishedCodeError(RuntimeError):
    """The code that would write an MC upload is not committed and pushed."""


def _git_run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=PATH_REPO, capture_output=True, text=True, check=False
    )


def _git(*args: str) -> str | None:
    result = _git_run(*args)
    return result.stdout if result.returncode == 0 else None


def _dirty_files() -> list[str]:
    status = _git("status", "--porcelain") or ""
    return [line[3:] for line in status.splitlines() if line.strip()]


def mc_tag_name(commit: str) -> str:
    """The tag that pins an MC generator commit: ``mc/<first 12 of sha>``."""
    return f"{MC_TAG_PREFIX}{commit[:12]}"


_published: dict[str, Any] | None = None


def require_published_code() -> dict[str, Any]:
    """Refuse to upload unless HEAD is clean, pushed, and tagged on the remote.

    Checks, in order: no modified, staged, or untracked files; ``git fetch``
    from ``origin`` succeeds; some ``origin`` branch contains HEAD. Then
    creates the tag ``mc/<sha12>`` on HEAD (if absent) and pushes it, so the
    commit stays reachable even if its branch is rebased or deleted.

    Generators call this before computing when ``--upload`` is passed, and
    ``record_mc_write`` calls it again, so a writer cannot skip it. The result
    is cached for the process. Returns the git fields for manifest entries.
    """
    global _published
    if _published is not None:
        return _published

    commit = (_git("rev-parse", "HEAD") or "").strip()
    if not commit:
        raise UnpublishedCodeError(f"Not a git checkout: {PATH_REPO}")
    dirty = _dirty_files()
    if dirty:
        shown = "\n  ".join(dirty[:20])
        more = f"\n  ... and {len(dirty) - 20} more" if len(dirty) > 20 else ""
        raise UnpublishedCodeError(
            "MC uploads need a clean working tree. Commit (or remove) these "
            f"files, push, and re-run:\n  {shown}{more}"
        )
    fetch = _git_run("fetch", "--quiet", GIT_REMOTE)
    if fetch.returncode != 0:
        raise UnpublishedCodeError(
            f"git fetch {GIT_REMOTE} failed, so HEAD cannot be confirmed as "
            f"pushed:\n{fetch.stderr.strip()}"
        )
    remote_branches = [
        line.strip()
        for line in (_git("branch", "-r", "--contains", commit) or "").splitlines()
        if line.strip() and "->" not in line
    ]
    if not remote_branches:
        branch = (_git("rev-parse", "--abbrev-ref", "HEAD") or "").strip()
        raise UnpublishedCodeError(
            f"HEAD {commit[:12]} is not on any {GIT_REMOTE} branch. Push it "
            f"first, e.g. `git push -u {GIT_REMOTE} {branch}`, and re-run."
        )

    tag = mc_tag_name(commit)
    tagged = (
        _git("rev-parse", "--verify", "--quiet", f"{tag}^{{commit}}") or ""
    ).strip()
    if not tagged:
        created = _git_run("tag", tag, commit)
        if created.returncode != 0:
            raise UnpublishedCodeError(
                f"Could not create tag {tag}: {created.stderr.strip()}"
            )
    elif tagged != commit:
        raise UnpublishedCodeError(f"Tag {tag} exists but points at {tagged[:12]}")
    pushed = _git_run("push", "--quiet", GIT_REMOTE, f"refs/tags/{tag}")
    if pushed.returncode != 0:
        raise UnpublishedCodeError(
            f"Could not push tag {tag} to {GIT_REMOTE}: {pushed.stderr.strip()}"
        )

    _published = {
        "git_commit": commit,
        "git_branch": (_git("rev-parse", "--abbrev-ref", "HEAD") or "").strip(),
        "git_tag": tag,
        "git_remote": GIT_REMOTE,
        "git_remote_url": (_git("remote", "get-url", GIT_REMOTE) or "").strip(),
        "git_remote_branches": remote_branches,
    }
    print(f"✓ Code published: {commit[:12]} on {', '.join(remote_branches)}; tag {tag}")
    return _published


def git_provenance() -> dict[str, Any]:
    """Git fields for a manifest entry; enforces ``require_published_code``."""
    return require_published_code()


def repo_git_state() -> dict[str, Any]:
    """Commit, branch, and dirty files of the checkout, with no requirements.

    For consumers (CAIRO runs) that record which code they ran from.
    """
    dirty = _dirty_files()
    return {
        "git_commit": (_git("rev-parse", "HEAD") or "").strip() or None,
        "git_branch": (_git("rev-parse", "--abbrev-ref", "HEAD") or "").strip() or None,
        "git_dirty": bool(dirty),
        "git_dirty_files": dirty,
    }


def commit_in_history(
    commit: str | None, *, head: str = "HEAD", fetch: bool = False
) -> bool | None:
    """Whether ``commit`` is an ancestor of (or equal to) ``head``.

    ``None`` when it cannot be decided: no commit, git unavailable, or the
    commit is not in the local clone. With ``fetch=True`` a missing commit is
    looked for by fetching tags from ``origin`` first (MC uploads push an
    ``mc/<sha12>`` tag for exactly this).
    """
    if not commit:
        return None

    def _known() -> bool:
        return _git_run("cat-file", "-e", f"{commit}^{{commit}}").returncode == 0

    if not _known():
        if not fetch:
            return None
        _git_run("fetch", "--quiet", "--tags", GIT_REMOTE)
        if not _known():
            return None
    result = _git_run("merge-base", "--is-ancestor", commit, head)
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    return None


def _script_and_command() -> dict[str, str]:
    script = Path(sys.argv[0]).resolve() if sys.argv and sys.argv[0] else None
    if script is not None and script.is_relative_to(PATH_REPO):
        script_label = str(script.relative_to(PATH_REPO))
    else:
        script_label = str(script) if script else ""
    return {
        "script": script_label,
        "command": "uv run python " + shlex.join(sys.argv),
    }


# ---------------------------------------------------------------------------
# Manifest read / write
# ---------------------------------------------------------------------------


class _ManifestDumper(yaml.SafeDumper):
    pass


def _str_presenter(dumper: yaml.SafeDumper, data: str) -> yaml.ScalarNode:
    style = "|" if "\n" in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


_ManifestDumper.add_representer(str, _str_presenter)


def read_manifest(path_manifest: str) -> dict[str, Any]:
    """Read a manifest, or return an empty one when absent."""
    if not _exists(path_manifest):
        return {"entries": []}
    data = yaml.safe_load(_read_bytes(path_manifest)) or {}
    data.setdefault("entries", [])
    return data


def append_entry(path_file: str, entry: dict[str, Any]) -> str:
    """Append ``entry`` to the manifest beside ``path_file``; return its path."""
    path_manifest = manifest_path_for(path_file)
    manifest = read_manifest(path_manifest)
    manifest["entries"].append(entry)
    _write_text(
        path_manifest,
        yaml.dump(
            manifest, Dumper=_ManifestDumper, sort_keys=False, default_flow_style=False
        ),
    )
    return path_manifest


def _entries_for(path_file: str) -> list[dict[str, Any]]:
    name = str(path_file).rstrip("/").rsplit("/", 1)[-1]
    entries = read_manifest(manifest_path_for(path_file))["entries"]
    return [e for e in entries if e.get("file") == name]


def latest_entry(path_file: str) -> dict[str, Any] | None:
    """The latest manifest entry for ``path_file``'s name, if any."""
    matching = _entries_for(path_file)
    return matching[-1] if matching else None


def entry_for_sha(path_file: str, sha256: str) -> dict[str, Any] | None:
    """The latest manifest entry for ``path_file`` whose ``file_sha256`` is ``sha256``."""
    for entry in reversed(_entries_for(path_file)):
        if entry.get("file_sha256") == sha256:
            return entry
    return None


def summarize_8760(df: pl.DataFrame, value_col: str) -> dict[str, Any]:
    values = df[value_col]
    return {
        "rows": df.height,
        "value_column": value_col,
        "sum": cast(float, values.sum()),
        "max": cast(float, values.max()),
        "min": cast(float, values.min()),
        "nonzero_hours": int((values != 0).sum()),
    }


def build_entry(
    path_file: str, spec: McSpec, summary: dict[str, Any]
) -> dict[str, Any]:
    return {
        "file": str(path_file).rstrip("/").rsplit("/", 1)[-1],
        "key": spec.key,
        "mc_type": spec.mc_type,
        "method": spec.method,
        "utility": spec.utility,
        "year": spec.year,
        "source": spec.source,
        "file_sha256": file_sha256(path_file),
        "written_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "params": dict(spec.params),
        "params_sha256": params_sha256(spec.params),
        "inputs": list(spec.inputs),
        "summary": summary,
        "provenance": {**_script_and_command(), **git_provenance()},
    }


def record_mc_write(
    path_file: str, spec: McSpec, df: pl.DataFrame, value_col: str
) -> dict[str, Any]:
    """Append a manifest entry for a file that was just written."""
    entry = build_entry(path_file, spec, summarize_8760(df, value_col))
    path_manifest = append_entry(path_file, entry)
    print(f"✓ Manifest entry key={entry['key']} -> {path_manifest}")
    return entry


# ---------------------------------------------------------------------------
# Consumers
# ---------------------------------------------------------------------------


def verify_mc_file(path_file: str, expected_key: str) -> list[str]:
    """Errors when ``path_file`` is missing, undocumented, or not ``expected_key``."""
    path_file = str(path_file)
    if not _exists(path_file):
        return [f"MC file missing: {path_file}"]
    entry = latest_entry(path_file)
    if entry is None:
        return [
            f"MC file {path_file} has no entry in {manifest_path_for(path_file)}. "
            "Regenerate it with its generator script so provenance is recorded."
        ]
    errors: list[str] = []
    if entry.get("key") != expected_key:
        errors.append(
            f"MC file {path_file}: pipeline expects key {expected_key!r}, but the "
            f"latest manifest entry ({entry.get('written_at')}) is {entry.get('key')!r}."
        )
    if file_sha256(path_file) != entry.get("file_sha256"):
        errors.append(
            f"MC file {path_file} does not match the sha256 in its latest manifest "
            "entry; it was changed without recording provenance."
        )
    return errors


def describe_mc_file(path_file: str) -> dict[str, Any]:
    """What one run read from one MC file, for ``marginal_costs_used.json``.

    Records the sha256 of the bytes read and the full manifest entry with that
    sha256, which holds the generator commit, tag, and command needed to
    rebuild the file. ``matches_manifest`` is true when that entry is also the
    latest one. When no entry has the sha256, the latest entry is recorded
    instead with ``matches_manifest`` false.

    ``generator_commit_in_run_history`` is ``commit_in_history`` of the
    entry's commit against this checkout's HEAD, without fetching.
    """
    path_file = str(path_file)
    record: dict[str, Any] = {"path": path_file}
    if not _exists(path_file):
        record["exists"] = False
        return record
    sha = file_sha256(path_file)
    record["sha256"] = sha
    entries = _entries_for(path_file)
    if not entries:
        record["key"] = None
        return record
    latest = entries[-1]
    matched = next((e for e in reversed(entries) if e.get("file_sha256") == sha), None)
    entry = matched or latest
    provenance = entry.get("provenance", {})
    record.update(
        key=entry.get("key"),
        method=entry.get("method"),
        source=entry.get("source"),
        written_at=entry.get("written_at"),
        git_commit=provenance.get("git_commit"),
        git_tag=provenance.get("git_tag"),
        matches_manifest=matched is not None and matched is latest,
        sha256_in_manifest=matched is not None,
        generator_commit_in_run_history=commit_in_history(provenance.get("git_commit")),
        manifest_entry=entry,
    )
    return record
