"""Slack notifications for ResStock pipeline runs.

Posts one message per run outcome (completed, failed, crashed) to the Slack
incoming webhook in ``SLACK_WEBHOOK_URL`` (read from the environment or the
project ``.env``). Notification is best-effort: a missing URL, network error,
or Slack outage prints one line and never affects the pipeline.

Test the webhook with ``uv run python -m data.resstock.notify --test``.
"""

from __future__ import annotations

import argparse
import os
import re
import socket
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

SLACK_WEBHOOK_ENV = "SLACK_WEBHOOK_URL"
MAX_WARNINGS = 10
TIMEOUT_SECONDS = 10

_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"


def webhook_url() -> str | None:
    load_dotenv(_ENV_PATH, override=False)
    return os.environ.get(SLACK_WEBHOOK_ENV) or None


def post_slack(text: str, *, url: str | None = None) -> bool:
    """Post ``text`` to the webhook. Returns True on success; never raises."""
    url = url or webhook_url()
    if url is None:
        print(
            f"  NOTE: {SLACK_WEBHOOK_ENV} is not set; skipping Slack notification.",
            flush=True,
        )
        return False
    try:
        resp = requests.post(url, json={"text": text}, timeout=TIMEOUT_SECONDS)
        resp.raise_for_status()
    except Exception as exc:
        print(f"  NOTE: Slack notification failed ({exc}).", flush=True)
        return False
    return True


def should_notify(run: Mapping[str, Any]) -> bool:
    """Runs record their notify setting; older records notify unless sampled."""
    args = run.get("args", {})
    notify = args.get("notify")
    if notify is None:
        return not args.get("sample")
    return bool(notify)


_NOTEBOOK_POINTER = re.compile(r'Investigate in (\S+\.qmd) → "[^"]*" → ')


def _shorten_warning(text: str) -> tuple[str, str | None]:
    """Drop the tag and the repeated notebook path; return (text, notebook path)."""
    text = text.removeprefix("[real-world] ")
    match = _NOTEBOOK_POINTER.search(text)
    if match is None:
        return text, None
    return _NOTEBOOK_POINTER.sub("See ", text), match.group(1)


def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _code_block(text: str, limit: int = 1500) -> str:
    text = text.strip()
    if len(text) > limit:
        text = text[:limit] + " …"
    return f"```{_escape(text)}```"


def _run_scope(run: Mapping[str, Any]) -> str:
    args = run.get("args", {})
    states = ", ".join(args.get("states", [])) or "?"
    upgrades = ", ".join(str(u).zfill(2) for u in args.get("upgrade_ids", []))
    parts = [states, f"`{run.get('release_sb', run.get('release', '?'))}`"]
    if upgrades:
        parts.append(f"upgrades {upgrades}")
    if args.get("sample"):
        parts.append(f"sample={args['sample']}")
    return " · ".join(parts)


def _duration(run: Mapping[str, Any], end_key: str) -> str | None:
    try:
        start = datetime.fromisoformat(run["started_at"])
        end_raw = run.get(end_key)
        end = datetime.fromisoformat(end_raw) if end_raw else datetime.now(timezone.utc)
    except (KeyError, TypeError, ValueError):
        return None
    minutes = int((end - start).total_seconds() // 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m" if hours else f"{minutes}m"


def _footer(run: Mapping[str, Any], end_key: str) -> str:
    parts = [f"host `{socket.gethostname()}`", f"run `{run.get('run_id', '?')}`"]
    duration = _duration(run, end_key)
    if duration:
        parts.insert(1, duration)
    return " · ".join(parts)


def _last_step(run: Mapping[str, Any]) -> str:
    steps = run.get("steps") or []
    return steps[-1]["step"] if steps else "none (failed in pre-flight validations)"


def _step_details(run: Mapping[str, Any], name: str) -> Mapping[str, Any] | None:
    for step in run.get("steps") or []:
        if step.get("step") == name:
            return step.get("details", {})
    return None


def format_completed(
    run: Mapping[str, Any], real_world_warnings: Sequence[str] = ()
) -> str:
    """Summary of a completed run, led by the real-world check warnings."""
    icon = ":warning:" if real_world_warnings else ":white_check_mark:"
    lines = [
        f"{icon} *ResStock pipeline completed* — {_run_scope(run)}",
        _footer(run, "completed_at"),
        *_real_world_lines(
            _step_details(run, "real_world_checks"), real_world_warnings
        ),
    ]
    return "\n".join(lines)


def format_real_world_checks(
    release: str,
    details: Mapping[str, Any],
    warnings: Sequence[str],
) -> str:
    """Standalone summary for ``check-real-world --notify`` on an existing release."""
    icon = ":warning:" if warnings else ":white_check_mark:"
    lines = [
        f"{icon} *ResStock real-world checks* — `{release}`",
        f"host `{socket.gethostname()}`",
        *_real_world_lines(details, warnings),
    ]
    return "\n".join(lines)


def _real_world_lines(
    details: Mapping[str, Any] | None, real_world_warnings: Sequence[str]
) -> list[str]:
    lines: list[str] = []
    if details is None:
        lines.append("Real-world checks: not run.")
    elif "skipped" in details:
        lines.append(f"Real-world checks: skipped ({_escape(details['skipped'])}).")
    else:
        for state, c in details.get("counts", {}).items():
            lines.append(
                f"Real-world checks, {state}: {c['ok']} ok, *{c['warn']} warn*, "
                f"{c['skipped']} skipped, {c['ignored']} ignored"
            )
    if real_world_warnings:
        shortened = [_shorten_warning(w) for w in real_world_warnings]
        shown = shortened[:MAX_WARNINGS]
        lines.append("")
        lines.extend(f"• {_escape(text)}" for text, _ in shown)
        if len(shortened) > len(shown):
            lines.append(f"…and {len(shortened) - len(shown)} more.")
        notebooks = sorted({nb for _, nb in shortened if nb})
        lines.extend(f"Notebook: `{nb}`" for nb in notebooks)
    if details and details.get("reports"):
        lines.append("")
        lines.extend(
            f"Report ({state}): `{path}`" for state, path in details["reports"].items()
        )
    return lines


def format_failed(run: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f":x: *ResStock pipeline failed* — {_run_scope(run)}",
            _footer(run, "failed_at"),
            f"Last completed step: {_last_step(run)}",
            _code_block(str(run.get("error", "unknown error"))),
            f"Command: `{_escape(str(run.get('command', '?')))}`",
        ]
    )


def format_crashed(run: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f":boom: *ResStock pipeline crashed* — {_run_scope(run)}",
            _footer(run, "crashed_at"),
            f"Last completed step: {_last_step(run)}",
            _escape(str(run.get("error", "unknown cause"))),
            f"Command: `{_escape(str(run.get('command', '?')))}`",
        ]
    )


def notify_completed(
    run: Mapping[str, Any], real_world_warnings: Sequence[str] = ()
) -> bool:
    if not should_notify(run):
        return False
    return post_slack(format_completed(run, real_world_warnings))


def notify_failed(run: Mapping[str, Any]) -> bool:
    if not should_notify(run):
        return False
    return post_slack(format_failed(run))


def notify_crashed(runs: Iterable[Mapping[str, Any]]) -> int:
    """Post one message per crashed run, de-duplicated by ``run_id``.

    The same run is recorded in both the raw and ``_sb`` manifests, so callers
    pass the crashed runs from both and this posts each once.
    """
    seen: set[str] = set()
    posted = 0
    for run in runs:
        run_id = str(run.get("run_id"))
        if run_id in seen or not should_notify(run):
            continue
        seen.add(run_id)
        posted += post_slack(format_crashed(run))
    return posted


def _main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--test", action="store_true", help="Post a test message to the webhook."
    )
    args = parser.parse_args(argv)
    if not args.test:
        parser.print_help()
        return
    ok = post_slack(
        f":wave: Test message from the ResStock pipeline on `{socket.gethostname()}`."
    )
    print("Posted." if ok else "Not posted.")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    _main()
