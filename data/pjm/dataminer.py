"""Shared PJM Data Miner 2 API client.

Thin, feed-agnostic helpers for pulling hourly feeds (e.g. ``rt_hrl_lmps``,
``hrl_load_metered``) from the PJM Data Miner 2 API. Centralises the parts that
every PJM feed pull needs and that are easy to get subtly wrong:

- **Auth**: ``PJM_API_PRIMARY_KEY`` from the project ``.env`` sent via the
  ``Ocp-Apim-Subscription-Key`` header.
- **Archive vs standard boundary**: PJM data older than 731 days is "archive"
  and is subject to different request rules — a single request may not span the
  archive/standard boundary, and archive requests must stay within one calendar
  year. :func:`split_date_range` chunks a range to satisfy both.
- **Pagination + transient errors**: :func:`fetch_date_range` pages through
  ``rowCount``-sized windows, retries read timeouts and 5xx, backs off on 429,
  and bisects a chunk when PJM reports it straddles the archive boundary.

Callers supply the feed name and a ``build_params`` callback that returns the
per-chunk query params (date filter and any feed-specific server-side filters),
so feed-specific logic (e.g. ``type=ZONE``/``pnode_id`` for LMPs) stays in the
feed's own module.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import requests
from dotenv import load_dotenv

API_BASE = "https://api.pjm.com/api/v1"
ROW_COUNT = 50_000  # maximum rows the API returns per page
ARCHIVE_CUTOFF_DAYS = 731  # data older than this is "historic" (archive rules)
MAX_RANGE_DAYS = 365  # hard API limit per request for standard data
REQUEST_TIMEOUT = 180  # seconds; PJM archive pages can take >60 s
MAX_RETRIES = 3  # retry transient errors (timeout, 5xx) this many times


class ArchiveBoundaryError(Exception):
    """Raised when PJM rejects a request for spanning the archive/standard boundary."""


def load_api_key(api_key: str | None = None) -> str:
    """Return the PJM API key, loading from ``.env`` if not supplied."""
    if api_key:
        return api_key
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(dotenv_path=project_root / ".env")
    key = os.getenv("PJM_API_PRIMARY_KEY")
    if not key:
        raise ValueError(
            "PJM_API_PRIMARY_KEY not set. Add it to the project .env file "
            "or pass api_key= explicitly."
        )
    return key


def archive_cutoff() -> date:
    """The rolling date on which PJM's archive/standard boundary falls.

    PJM computes this as ``current_utc_date - 731 days``. Dates strictly before
    this are archive; dates on or after are standard. Computed in UTC to match
    PJM's server-side logic.
    """
    today_utc = datetime.now(timezone.utc).date()
    return today_utc - timedelta(days=ARCHIVE_CUTOFF_DAYS)


def split_date_range(start: date, end: date) -> list[tuple[date, date]]:
    """Split ``start..end`` into chunks that each satisfy API constraints.

    Rules enforced:
    - A single chunk must not span the archive/standard boundary.
    - Archive (historic) chunks must stay within one calendar year (UTC).
    - Standard chunks must be <= ``MAX_RANGE_DAYS`` days.
    """
    cutoff = archive_cutoff()
    chunks: list[tuple[date, date]] = []

    if start < cutoff and end >= cutoff:
        segments = [(start, cutoff - timedelta(days=1)), (cutoff, end)]
    else:
        segments = [(start, end)]

    for seg_start, seg_end in segments:
        is_archive = seg_start < cutoff
        cur = seg_start
        while cur <= seg_end:
            if is_archive:
                chunk_end = min(seg_end, date(cur.year, 12, 31))
            else:
                chunk_end = min(seg_end, cur + timedelta(days=MAX_RANGE_DAYS - 1))
            chunks.append((cur, chunk_end))
            cur = chunk_end + timedelta(days=1)

    return chunks


def ept_date_filter(start: date, end: date) -> str:
    """Build the ``datetime_beginning_ept`` range filter string PJM expects.

    Format: ``M-D-YYYY 00:00 to M-D-YYYY 23:59`` (no zero-padding on month/day).
    """
    return f"{start.strftime('%-m-%-d-%Y')} 00:00 to {end.strftime('%-m-%-d-%Y')} 23:59"


_TS_FORMATS = [
    "%m/%d/%Y %I:%M:%S %p",  # US with AM/PM  ("1/1/2019 12:00:00 AM")
    "%m/%d/%Y %H:%M:%S",  # US 24-hour      ("1/1/2019 0:00:00")
    "%Y-%m-%dT%H:%M:%S",  # ISO 8601        ("2019-01-01T00:00:00")
    "%Y-%m-%d %H:%M:%S",  # ISO-like        ("2019-01-01 00:00:00")
]


def parse_ts_column(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Parse a string timestamp column, trying multiple date formats.

    Tries each format in ``_TS_FORMATS`` and keeps the first that produces at
    least some non-null values. Raises if none succeed. No-op if the column is
    already a Datetime.
    """
    if df.schema[col] != pl.String:
        return df
    sample = df[col].drop_nulls().head(1).to_list()
    for fmt in _TS_FORMATS:
        parsed = df.with_columns(
            pl.col(col).str.to_datetime(fmt, strict=False).alias(col)
        )
        if parsed[col].null_count() < parsed.height:
            return parsed
    raise ValueError(
        f"Could not parse timestamp column '{col}' with any known format. "
        f"Sample value: {sample}"
    )


def fetch_page(
    session: requests.Session,
    api_key: str,
    feed: str,
    params: dict[str, str | int],
) -> tuple[list[dict], int]:
    """Fetch one page from a Data Miner 2 feed endpoint.

    Returns ``(rows, total_row_count)``. Retries up to ``MAX_RETRIES`` times on
    read timeouts and 5xx errors before raising; backs off on 429. Raises
    :class:`ArchiveBoundaryError` on the specific PJM 400 about spanning
    archive/standard data so callers can split and retry.
    """
    url = f"{API_BASE}/{feed}"
    headers = {"Ocp-Apim-Subscription-Key": api_key}

    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(
                url, headers=headers, params=params, timeout=REQUEST_TIMEOUT
            )
        except requests.exceptions.ReadTimeout as exc:
            last_exc = exc
            wait = 30 * attempt
            print(
                f"  Read timeout on attempt {attempt}/{MAX_RETRIES} "
                f"— waiting {wait}s before retry"
            )
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 30))
            print(f"  Rate limited — waiting {retry_after}s before retry")
            time.sleep(retry_after)
            continue

        if resp.status_code >= 500:
            last_exc = requests.exceptions.HTTPError(response=resp)
            wait = 30 * attempt
            print(
                f"  Server error {resp.status_code} on attempt "
                f"{attempt}/{MAX_RETRIES} — waiting {wait}s"
            )
            time.sleep(wait)
            continue

        if resp.status_code == 400:
            try:
                body = resp.json()
            except ValueError:
                body = {"message": resp.text[:500]}
            msg = body.get("message", "")
            errors = body.get("errors", [])
            spans_boundary = "spans over archived and standard" in msg or any(
                "spans over archived and standard" in e.get("message", "")
                for e in errors
            )
            if spans_boundary:
                raise ArchiveBoundaryError(msg)
            raise requests.exceptions.HTTPError(
                f"400 Bad Request — PJM error: {body}\nURL: {resp.url}",
                response=resp,
            )

        resp.raise_for_status()
        payload = resp.json()
        items: list[dict] = payload.get("items", [])
        total: int = payload.get("totalRows", len(items))
        return items, total

    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts. Last error: {last_exc}")


def fetch_date_range(
    feed: str,
    start: date,
    end: date,
    build_params: Callable[[date, date, bool], dict[str, str | int]],
    *,
    api_key: str | None = None,
    page_delay_seconds: float = 11.0,
) -> list[dict]:
    """Fetch all rows for ``feed`` over ``start..end`` (inclusive).

    Handles archive/standard chunking (:func:`split_date_range`), pagination,
    transient-error retries, and bisecting a chunk that straddles the archive
    boundary. ``build_params(chunk_start, chunk_end, is_archive)`` returns the
    per-chunk query params (date filter plus any feed-specific filters);
    ``startRow`` is injected automatically for pagination.
    """
    key = load_api_key(api_key)
    cutoff = archive_cutoff()
    chunks = split_date_range(start, end)
    session = requests.Session()
    all_rows: list[dict] = []

    print(
        f"Fetching {feed} {start} -> {end} ({len(chunks)} chunk(s); "
        f"archive cutoff {cutoff})"
    )

    def _fetch_chunk(chunk_start: date, chunk_end: date) -> list[dict]:
        is_archive = chunk_start < cutoff
        base_params = build_params(chunk_start, chunk_end, is_archive)
        print(
            f"  chunk {chunk_start} -> {chunk_end} "
            f"({'archive' if is_archive else 'standard'})"
        )

        chunk_rows: list[dict] = []
        start_row = 1
        try:
            while True:
                params = {**base_params, "startRow": start_row}
                rows, total = fetch_page(session, key, feed, params)
                if not rows:
                    break
                page_size = len(rows)
                chunk_rows.extend(rows)
                fetched_so_far = start_row + page_size - 1
                print(f"    rows {start_row}-{fetched_so_far} of {total}")
                if fetched_so_far >= total:
                    break
                start_row += ROW_COUNT
                time.sleep(page_delay_seconds)
        except ArchiveBoundaryError:
            if chunk_start == chunk_end:
                raise RuntimeError(
                    f"PJM boundary error on a single-day chunk ({chunk_start}). "
                    "The archive cutoff may have shifted mid-request; retry."
                ) from None
            mid = chunk_start + (chunk_end - chunk_start) // 2
            print(
                f"  -> boundary error — bisecting at {mid} and retrying "
                f"({chunk_start}->{mid}, {mid + timedelta(days=1)}->{chunk_end})"
            )
            chunk_rows = _fetch_chunk(chunk_start, mid)
            chunk_rows += _fetch_chunk(mid + timedelta(days=1), chunk_end)
        return chunk_rows

    for chunk_start, chunk_end in chunks:
        all_rows.extend(_fetch_chunk(chunk_start, chunk_end))

    return all_rows
