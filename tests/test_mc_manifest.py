"""Tests for marginal cost provenance manifests (mc_manifest.py)."""

from __future__ import annotations

import subprocess
from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.data_prep.marginal_costs import mc_manifest
from utils.data_prep.marginal_costs.mc_manifest import (
    McSpec,
    build_key,
    describe_mc_file,
    input_partition,
    latest_entry,
    manifest_path_for,
    record_mc_write,
    verify_mc_file,
    zero_spec,
)


@pytest.fixture(autouse=True)
def _no_git(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mc_manifest, "git_provenance", lambda: {"git_commit": "abc"})


def _pop_params(**overrides: object) -> dict[str, object]:
    params: dict[str, object] = {
        "load_source": "occ863att3",
        "load_year": 2025,
        "n_hours": 100,
        "target_dollar_year": 2026,
    }
    params.update(overrides)
    return params


def _pop_spec(**overrides: object) -> McSpec:
    return McSpec(
        mc_type="dist_and_sub_tx",
        method="pop",
        utility="ct_eversource",
        year=2025,
        params=_pop_params(**overrides),
        source="PURA Docket 26-05-10, MCOS-2 Table 3, secondary",
    )


def _write_8760(path: Path, value: float) -> pl.DataFrame:
    df = pl.DataFrame({"mc_total_per_kwh": [value] * 8760})
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return df


class TestKeys:
    def test_pop(self) -> None:
        assert (
            build_key("dist_and_sub_tx", "pop", _pop_params())
            == "pop_occ863att3_100h_load2025_2026usd"
        )

    def test_month_hour(self) -> None:
        params = {
            "table_source": "occ863att2",
            "voltage": "secondary",
            "include_holidays": False,
            "calendar_year": 2018,
        }
        assert (
            build_key("dist_and_sub_tx", "month_hour", params)
            == "month_hour_occ863att2_secondary_noholidays_cal2018"
        )

    def test_isone_bulk_tx_formats_value(self) -> None:
        params = {
            "ptf_kw_year": 69.0,
            "allocation_load": "ne_system",
            "n_peak_hours": 100,
            "load_year": 2025,
        }
        assert (
            build_key("bulk_tx", "isone_aesc_ptf", params)
            == "aesc_ptf69_ne_system_100h_load2025"
        )
        params["ptf_kw_year"] = 184.5
        assert build_key("bulk_tx", "isone_aesc_ptf", params).startswith(
            "aesc_ptf184p5_"
        )

    @pytest.mark.parametrize("mc_type", sorted(mc_manifest.MC_TYPES))
    def test_zero_for_every_type(self, mc_type: str) -> None:
        assert build_key(mc_type, "zero", {}) == "zero"

    def test_missing_param_is_named(self) -> None:
        params = _pop_params()
        del params["n_hours"]
        with pytest.raises(ValueError, match="n_hours"):
            build_key("dist_and_sub_tx", "pop", params)

    def test_unregistered_method_rejected(self) -> None:
        with pytest.raises(ValueError, match="No key builder"):
            build_key("dist_and_sub_tx", "mystery", {})

    def test_spec_rejects_unknown_mc_type(self) -> None:
        with pytest.raises(ValueError, match="mc_type"):
            McSpec(mc_type="dist", method="zero", utility="u", year=2025, params={})


class TestManifest:
    def test_entries_append_and_latest_wins(self, tmp_path: Path) -> None:
        path = tmp_path / "utility=ct_eversource" / "year=2025" / "data.parquet"
        df = _write_8760(path, 0.0)
        record_mc_write(str(path), _pop_spec(), df, "mc_total_per_kwh")
        df = _write_8760(path, 1.0)
        record_mc_write(str(path), _pop_spec(n_hours=50), df, "mc_total_per_kwh")

        manifest = yaml.safe_load(Path(manifest_path_for(str(path))).read_text())
        assert [e["key"] for e in manifest["entries"]] == [
            "pop_occ863att3_100h_load2025_2026usd",
            "pop_occ863att3_50h_load2025_2026usd",
        ]
        latest = latest_entry(str(path))
        assert latest is not None
        assert latest["params"]["n_hours"] == 50
        assert latest["summary"]["sum"] == pytest.approx(8760.0)
        assert latest["source"] == "PURA Docket 26-05-10, MCOS-2 Table 3, secondary"
        assert latest["provenance"]["git_commit"] == "abc"

    def test_entries_are_per_file_name(self, tmp_path: Path) -> None:
        data = tmp_path / "data.parquet"
        zero = tmp_path / "zero.parquet"
        record_mc_write(
            str(data), _pop_spec(), _write_8760(data, 1.0), "mc_total_per_kwh"
        )
        record_mc_write(
            str(zero),
            zero_spec("dist_and_sub_tx", "ct_eversource", 2025),
            _write_8760(zero, 0.0),
            "mc_total_per_kwh",
        )
        data_entry = latest_entry(str(data))
        zero_entry = latest_entry(str(zero))
        assert data_entry is not None and zero_entry is not None
        assert data_entry["key"].startswith("pop_")
        assert zero_entry["key"] == "zero"


class TestVerify:
    def test_ok(self, tmp_path: Path) -> None:
        path = tmp_path / "data.parquet"
        record_mc_write(
            str(path), _pop_spec(), _write_8760(path, 1.0), "mc_total_per_kwh"
        )
        assert verify_mc_file(str(path), "pop_occ863att3_100h_load2025_2026usd") == []

    def test_key_mismatch(self, tmp_path: Path) -> None:
        path = tmp_path / "data.parquet"
        record_mc_write(
            str(path), _pop_spec(), _write_8760(path, 1.0), "mc_total_per_kwh"
        )
        errors = verify_mc_file(str(path), "month_hour_occ863att2_secondary_holidays")
        assert len(errors) == 1
        assert "pop_occ863att3_100h_load2025_2026usd" in errors[0]

    def test_file_changed_after_manifest(self, tmp_path: Path) -> None:
        path = tmp_path / "data.parquet"
        record_mc_write(
            str(path), _pop_spec(), _write_8760(path, 1.0), "mc_total_per_kwh"
        )
        _write_8760(path, 2.0)
        errors = verify_mc_file(str(path), "pop_occ863att3_100h_load2025_2026usd")
        assert len(errors) == 1
        assert "sha256" in errors[0]

    def test_no_manifest(self, tmp_path: Path) -> None:
        path = tmp_path / "data.parquet"
        _write_8760(path, 1.0)
        errors = verify_mc_file(str(path), "zero")
        assert len(errors) == 1
        assert "no entry" in errors[0]

    def test_missing_file(self, tmp_path: Path) -> None:
        errors = verify_mc_file(str(tmp_path / "data.parquet"), "zero")
        assert errors == [f"MC file missing: {tmp_path / 'data.parquet'}"]


def test_describe_mc_file(tmp_path: Path) -> None:
    path = tmp_path / "data.parquet"
    record_mc_write(str(path), _pop_spec(), _write_8760(path, 1.0), "mc_total_per_kwh")
    described = describe_mc_file(str(path))
    assert described["key"] == "pop_occ863att3_100h_load2025_2026usd"
    assert described["matches_manifest"] is True
    assert described["sha256_in_manifest"] is True
    assert described["sha256"] == mc_manifest.file_sha256(str(path))
    entry = described["manifest_entry"]
    assert entry["file_sha256"] == described["sha256"]
    assert "command" in entry["provenance"]
    # Generator commit "abc" is not a commit in this checkout.
    assert described["generator_commit_in_run_history"] is None

    undocumented = tmp_path / "other" / "data.parquet"
    _write_8760(undocumented, 1.0)
    assert describe_mc_file(str(undocumented))["key"] is None


def test_describe_mc_file_records_the_entry_for_the_bytes_read(tmp_path: Path) -> None:
    path = tmp_path / "data.parquet"
    first = _write_8760(path, 1.0)
    record_mc_write(str(path), _pop_spec(), first, "mc_total_per_kwh")
    old_bytes = path.read_bytes()
    record_mc_write(
        str(path), _pop_spec(n_hours=50), _write_8760(path, 2.0), "mc_total_per_kwh"
    )
    path.write_bytes(old_bytes)

    described = describe_mc_file(str(path))
    assert described["sha256_in_manifest"] is True
    assert described["matches_manifest"] is False
    assert described["manifest_entry"]["params"]["n_hours"] == 100


# ---------------------------------------------------------------------------
# Publishing guard and ancestry (real git, in a scratch repo with a bare remote)
# ---------------------------------------------------------------------------


def _run_git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def scratch_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    remote = tmp_path / "remote.git"
    repo = tmp_path / "repo"
    _run_git(tmp_path, "init", "--quiet", "--bare", str(remote))
    _run_git(tmp_path, "init", "--quiet", "-b", "main", str(repo))
    for key, value in (
        ("user.email", "test@example.com"),
        ("user.name", "test"),
        ("commit.gpgsign", "false"),
        ("tag.gpgsign", "false"),
    ):
        _run_git(repo, "config", key, value)
    _run_git(repo, "remote", "add", "origin", str(remote))
    (repo / "a.txt").write_text("a\n")
    _run_git(repo, "add", "a.txt")
    _run_git(repo, "commit", "--quiet", "-m", "first")
    monkeypatch.setattr(mc_manifest, "PATH_REPO", repo)
    monkeypatch.setattr(mc_manifest, "_published", None)
    return repo


class TestRequirePublishedCode:
    def test_dirty_tree_refused(self, scratch_repo: Path) -> None:
        _run_git(scratch_repo, "push", "--quiet", "-u", "origin", "main")
        (scratch_repo / "untracked.py").write_text("x = 1\n")
        with pytest.raises(mc_manifest.UnpublishedCodeError, match="untracked.py"):
            mc_manifest.require_published_code()

    def test_unpushed_commit_refused(self, scratch_repo: Path) -> None:
        with pytest.raises(mc_manifest.UnpublishedCodeError, match="git push"):
            mc_manifest.require_published_code()

    def test_pushed_commit_is_tagged_on_remote(self, scratch_repo: Path) -> None:
        _run_git(scratch_repo, "push", "--quiet", "-u", "origin", "main")
        head = _run_git(scratch_repo, "rev-parse", "HEAD")
        info = mc_manifest.require_published_code()
        tag = f"mc/{head[:12]}"
        assert info["git_commit"] == head
        assert info["git_tag"] == tag
        assert info["git_remote_branches"] == ["origin/main"]
        remote_tags = _run_git(scratch_repo, "ls-remote", "--tags", "origin")
        assert f"{head}\trefs/tags/{tag}" in remote_tags
        # Cached for the process, and re-tagging the same commit is a no-op.
        assert mc_manifest.require_published_code() is info
        mc_manifest._published = None
        assert mc_manifest.require_published_code()["git_tag"] == tag

    def test_record_mc_write_enforces_it(
        self, scratch_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            mc_manifest, "git_provenance", mc_manifest.require_published_code
        )
        path = tmp_path / "out" / "data.parquet"
        df = _write_8760(path, 1.0)
        with pytest.raises(mc_manifest.UnpublishedCodeError):
            record_mc_write(str(path), _pop_spec(), df, "mc_total_per_kwh")
        assert latest_entry(str(path)) is None


class TestCommitInHistory:
    def test_ancestor_side_branch_and_unknown(self, scratch_repo: Path) -> None:
        first = _run_git(scratch_repo, "rev-parse", "HEAD")
        _run_git(scratch_repo, "checkout", "--quiet", "-b", "side")
        (scratch_repo / "b.txt").write_text("b\n")
        _run_git(scratch_repo, "add", "b.txt")
        _run_git(scratch_repo, "commit", "--quiet", "-m", "side")
        side = _run_git(scratch_repo, "rev-parse", "HEAD")
        _run_git(scratch_repo, "checkout", "--quiet", "main")

        assert mc_manifest.commit_in_history(first) is True
        assert mc_manifest.commit_in_history(side) is False
        assert mc_manifest.commit_in_history("0" * 40) is None
        assert mc_manifest.commit_in_history(None) is None


def test_input_partition_hash_tracks_content(tmp_path: Path) -> None:
    part = tmp_path / "utility=u" / "year=2025"
    _write_8760(part / "month=01" / "data.parquet", 1.0)
    first = input_partition("load", str(part))
    assert first["n_files"] == 1
    _write_8760(part / "month=01" / "data.parquet", 2.0)
    assert input_partition("load", str(part))["sha256"] != first["sha256"]
