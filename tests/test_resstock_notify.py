"""Tests for data/resstock/notify.py and its pipeline hooks."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from data.resstock import main as pipeline_main
from data.resstock import notify
from data.resstock.manifest import (
    mark_crashed_runs,
    new_run_record,
    read_manifest,
    record_step,
    upsert_run,
)


def _run(**args: Any) -> dict[str, Any]:
    run = new_run_record(
        release="res_2024_amy2018_2",
        release_sb="res_2024_amy2018_2_sb",
        states=["CT"],
        upgrade_ids=["0", "2"],
        file_types=["metadata"],
        flags={"sample": 0, **args},
    )
    return run


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Capture Slack posts instead of sending them."""
    messages: list[str] = []

    def _fake_post(text: str, *, url: str | None = None) -> bool:
        messages.append(text)
        return True

    monkeypatch.setattr(notify, "post_slack", _fake_post)
    return messages


def test_post_slack_without_url_is_a_noop(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(notify, "webhook_url", lambda: None)

    def _fail(*_a: Any, **_k: Any) -> None:
        raise AssertionError("should not post")

    monkeypatch.setattr(notify.requests, "post", _fail)
    assert notify.post_slack("hello") is False
    assert notify.SLACK_WEBHOOK_ENV in capsys.readouterr().out


def test_post_slack_swallows_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*_a: Any, **_k: Any) -> None:
        raise ConnectionError("slack down")

    monkeypatch.setattr(notify.requests, "post", _boom)
    assert notify.post_slack("hello", url="https://hooks.slack.test/x") is False


def test_should_notify_defaults_off_for_sample_runs() -> None:
    assert notify.should_notify({"args": {"sample": 0}})
    assert not notify.should_notify({"args": {"sample": 10}})
    assert notify.should_notify({"args": {"sample": 10, "notify": True}})
    assert not notify.should_notify({"args": {"sample": 0, "notify": False}})


def test_completed_message_caps_warnings_and_lists_counts() -> None:
    run = _run()
    record_step(
        run,
        "real_world_checks",
        counts={"CT": {"ok": 36, "warn": 12, "skipped": 10, "ignored": 0}},
        reports={"CT": "/ebs/x/real_world_checks_CT.yaml"},
    )
    warnings = [f"[real-world] CT check {i} <bad>" for i in range(12)]
    text = notify.format_completed(run, warnings)

    assert text.startswith(":warning:")
    assert "CT: 36 ok, *12 warn*" in text
    assert text.count("• ") == notify.MAX_WARNINGS
    assert "…and 2 more." in text
    assert "&lt;bad&gt;" in text
    assert "[real-world]" not in text
    assert "real_world_checks_CT.yaml" in text


def test_completed_message_shortens_notebook_pointer() -> None:
    warning = (
        "[real-world] CT electric customers, statewide: off. Investigate in "
        'reports2/x/investigate_resstock.qmd → "Section 2: Electric" → '
        '"Customer counts by electric utility" (#sec-electric-customers-by-utility).'
    )
    text = notify.format_completed(_run(), [warning, warning])
    assert '• CT electric customers, statewide: off. See "Customer counts' in text
    assert text.count("Notebook: `reports2/x/investigate_resstock.qmd`") == 1


def test_completed_message_without_warnings_is_clean() -> None:
    run = _run()
    record_step(run, "real_world_checks", skipped="sample run")
    text = notify.format_completed(run)
    assert text.startswith(":white_check_mark:")
    assert "skipped (sample run)" in text


def test_failed_message_flags_preflight() -> None:
    run = _run()
    run["error"] = "utility assignment not supported for ZZ"
    text = notify.format_failed(run)
    assert text.startswith(":x:")
    assert "pre-flight" in text
    assert "not supported for ZZ" in text


def test_mark_crashed_runs_returns_runs_and_notify_dedupes(
    tmp_path: Path, posted: list[str]
) -> None:
    run = _run()
    record_step(run, "fetch")
    raw, sb = tmp_path / "raw", tmp_path / "sb"
    for d in (raw, sb):
        d.mkdir()
        upsert_run(d, run)

    crashed = mark_crashed_runs(raw, exit_code=137) + mark_crashed_runs(
        sb, exit_code=137
    )

    assert len(crashed) == 2
    assert read_manifest(raw)["runs"][0]["status"] == "crashed"
    assert notify.notify_crashed(crashed) == 1
    assert len(posted) == 1
    assert "crashed" in posted[0] and "SIGKILL" in posted[0] and "fetch" in posted[0]


def test_notify_crashed_respects_run_setting(posted: list[str]) -> None:
    assert notify.notify_crashed([_run(notify=False)]) == 0
    assert posted == []


def test_preflight_failure_posts_to_slack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    failed: list[dict[str, Any]] = []
    monkeypatch.setattr(pipeline_main, "notify_failed", failed.append)

    with pytest.raises(SystemExit):
        pipeline_main.main(
            [
                "--state",
                "ZZ",
                "--upgrade-ids",
                "0",
                "--path-output-dir",
                str(tmp_path),
            ]
        )

    assert len(failed) == 1
    assert failed[0]["status"] == "failed"
    assert failed[0]["steps"] == []
