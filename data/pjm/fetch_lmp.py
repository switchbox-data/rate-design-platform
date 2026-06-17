"""PJM real-time hourly zone-aggregate LMP data: API fetch and S3 CSV extract.

Two entry-points:

1. **API fetch** (``fetch_rt_lmps_for_zone``): pulls data directly from the
   PJM Data Miner 2 API for a specific zone and date range.  Requires a valid
   ``PJM_API_PRIMARY_KEY`` in the project ``.env`` file.

2. **CSV extract** (``extract_md_zone_lmps``): processes a raw ``rt_hrl_lmps``
   CSV already downloaded to S3, extracting ZONE-type aggregate rows for all
   MD utilities.

**Key data quirk**: In the PJM feed the ``zone`` column is *null* for
ZONE-type aggregate nodes.  The zone name is carried in ``pnode_name``
instead (e.g. ``pnode_name == "BGE"`` for the BGE zone aggregate).
Filtering on ``zone == "BGE"`` alone returns only bus/load/gen nodes inside
that zone — never the zone aggregate.  The correct filter is::

    type == "ZONE"  AND  pnode_name == <pjm_zone_code>

Mapping from Switchbox utility std_name to PJM zone codes (pnode_name for
ZONE-type rows):

    bge            → BGE      (pnode_id 51292)
    pepco          → PEPCO    (pnode_id 51298)
    delmarva       → DPL      (pnode_id 51293)
    potomac_edison → APS      (pnode_id 8394954)

Usage::

    # Fetch via API (writes parquet to S3)
    uv run python data/pjm/fetch_lmp.py fetch-api \\
        --zone BGE --start-date 2023-01-01 --end-date 2024-01-01 \\
        --s3-output s3://data.sb/pjm/lmp/real_time/zones

    # Extract from existing S3 CSV
    uv run python data/pjm/fetch_lmp.py extract-csv \\
        --s3-path s3://data.sb/pjm/lmp/real_time/rt_hrl_lmps.csv \\
        --s3-output s3://data.sb/pjm/lmp/real_time/zones
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import requests
from dotenv import load_dotenv

from data.pjm.validate_pjm_lmp import validate_zone_lmp
from data.pjm.zone_mapping.generate_zone_mapping_csv import build_zone_mapping
from utils.file_io import read_csv_from_s3, write_hive_partitioned_parquet_to_s3

# ---------------------------------------------------------------------------
# Zone map helpers
# ---------------------------------------------------------------------------


def _zone_map_for_state(state: str) -> dict[str, str]:
    """Return {utility_slug: pnode_name} for a given state from the zone mapping CSV.

    The ``fivecp_zone_label`` column holds the canonical zone label (e.g. "BGE",
    "PEPCO", "DPL", "APS") which matches the ``pnode_name`` value for ZONE-type
    aggregate rows in PJM Data Miner's ``rt_hrl_lmps`` / ``da_hrl_lmps`` feeds.

    See ``data/pjm/zone_mapping/generate_zone_mapping_csv.py`` for the full
    crosswalk schema and vocabulary.
    """
    df = build_zone_mapping().filter(pl.col("state") == state)
    return dict(
        zip(df["utility"].to_list(), df["fivecp_zone_label"].to_list(), strict=True)
    )


# ---------------------------------------------------------------------------
# API fetch
# ---------------------------------------------------------------------------

_API_BASE = "https://api.pjm.com/api/v1"
_FEED = "rt_hrl_lmps"
_ROW_COUNT = 50_000  # maximum the API allows per page
_ARCHIVE_CUTOFF_DAYS = 731  # data older than this is "historic" (different rules)
_MAX_RANGE_DAYS = 365  # hard API limit per request for standard data
_REQUEST_TIMEOUT = 180  # seconds; PJM archive pages can take >60 s
_MAX_RETRIES = 3  # retry transient errors (timeout, 5xx) this many times


class _ArchiveBoundaryError(Exception):
    """Raised when PJM rejects a request for spanning the archive/standard boundary."""


def _load_api_key(api_key: str | None) -> str:
    """Return the PJM API key, loading from .env if not supplied."""
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


def _archive_cutoff() -> date:
    """The rolling date on which PJM's archive/standard boundary falls.

    PJM computes this as ``current_utc_date - 731 days``.  Dates strictly
    before this are archive; dates on or after are standard.  We compute in
    UTC to match PJM's server-side logic.
    """
    from datetime import datetime, timezone

    today_utc = datetime.now(timezone.utc).date()
    return today_utc - timedelta(days=_ARCHIVE_CUTOFF_DAYS)


def _split_date_range(start: date, end: date) -> list[tuple[date, date]]:
    """Split start..end into chunks that each satisfy API constraints.

    Rules enforced:
    - A single chunk must not span the archive/standard boundary.
    - Archive (historic) chunks must stay within one calendar year (UTC).
    - Standard chunks must be ≤ 365 days.
    """
    cutoff = _archive_cutoff()
    chunks: list[tuple[date, date]] = []

    # Split at the archive/standard boundary first.
    if start < cutoff and end >= cutoff:
        segments = [(start, cutoff - timedelta(days=1)), (cutoff, end)]
    else:
        segments = [(start, end)]

    for seg_start, seg_end in segments:
        is_archive = seg_start < cutoff

        cur = seg_start
        while cur <= seg_end:
            if is_archive:
                year_end = date(cur.year, 12, 31)
                chunk_end = min(seg_end, year_end)
            else:
                chunk_end = min(seg_end, cur + timedelta(days=_MAX_RANGE_DAYS - 1))

            chunks.append((cur, chunk_end))
            cur = chunk_end + timedelta(days=1)

    return chunks


def _fetch_page(
    session: requests.Session,
    api_key: str,
    params: dict[str, str | int],
) -> tuple[list[dict], int]:
    """Fetch one page from the rt_hrl_lmps API endpoint.

    Returns ``(rows, total_row_count)``.  Retries up to ``_MAX_RETRIES`` times
    on read timeouts and 5xx errors before raising.  Backs off on 429.
    Raises ``_ArchiveBoundaryError`` on the specific PJM 400 error about
    spanning archive/standard data so callers can split and retry.
    """
    url = f"{_API_BASE}/{_FEED}"
    headers = {"Ocp-Apim-Subscription-Key": api_key}

    last_exc: Exception | None = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = session.get(
                url, headers=headers, params=params, timeout=_REQUEST_TIMEOUT
            )
        except requests.exceptions.ReadTimeout as exc:
            last_exc = exc
            wait = 30 * attempt
            print(
                f"  Read timeout on attempt {attempt}/{_MAX_RETRIES} — waiting {wait}s before retry"
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
                f"  Server error {resp.status_code} on attempt {attempt}/{_MAX_RETRIES} — waiting {wait}s"
            )
            time.sleep(wait)
            continue

        if resp.status_code == 400:
            try:
                body = resp.json()
            except Exception:
                body = {"message": resp.text[:500]}
            msg = body.get("message", "")
            errors = body.get("errors", [])
            spans_boundary = "spans over archived and standard" in msg or any(
                "spans over archived and standard" in e.get("message", "")
                for e in errors
            )
            if spans_boundary:
                raise _ArchiveBoundaryError(msg)
            raise requests.exceptions.HTTPError(
                f"400 Bad Request — PJM error: {body}\nURL: {resp.url}",
                response=resp,
            )

        resp.raise_for_status()
        payload = resp.json()
        items: list[dict] = payload.get("items", [])
        total: int = payload.get("totalRows", len(items))
        return items, total

    raise RuntimeError(f"Failed after {_MAX_RETRIES} attempts. Last error: {last_exc}")


_TS_FORMATS = [
    "%m/%d/%Y %I:%M:%S %p",  # US with AM/PM  ("1/1/2019 12:00:00 AM")
    "%m/%d/%Y %H:%M:%S",  # US 24-hour      ("1/1/2019 0:00:00")
    "%Y-%m-%dT%H:%M:%S",  # ISO 8601        ("2019-01-01T00:00:00")
    "%Y-%m-%d %H:%M:%S",  # ISO-like        ("2019-01-01 00:00:00")
]


def _parse_ts_column(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Parse a string timestamp column, trying multiple date formats.

    Tries each format in ``_TS_FORMATS`` and keeps the first that produces
    at least some non-null values.  Raises if none succeed.
    """
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


def fetch_rt_lmps_for_zone(
    zone_code: str,
    start_date: date,
    end_date: date,
    api_key: str | None = None,
    *,
    row_is_current: bool = True,
    page_delay_seconds: float = 11.0,
) -> pl.DataFrame:
    """Fetch real-time hourly LMP data for a PJM zone aggregate via the API.

    Handles pagination, archive/standard boundary splitting, and date-range
    chunking automatically.  Only ZONE-type aggregate rows for the requested
    zone are returned.

    Parameters
    ----------
    zone_code:
        PJM canonical zone label, e.g. ``"BGE"``, ``"PEPCO"``, ``"DPL"``,
        ``"APS"``.  Must match ``pnode_name`` for ZONE-type aggregate rows.
    start_date:
        First date (inclusive) to fetch, in UTC.
    end_date:
        Last date (inclusive) to fetch, in UTC.
    api_key:
        PJM Data Miner 2 API key.  If omitted, read from ``PJM_API_PRIMARY_KEY``
        in the project ``.env`` file.
    row_is_current:
        When ``True`` (default), only return the latest revision of each row
        (``row_is_current=true``).  Set to ``False`` to include all versions.
    page_delay_seconds:
        Seconds to sleep between paginated requests.  Default 11 s keeps
        throughput at ~5.5 requests/minute, safely under a 6 req/min quota.
        Reduce only if your account has a higher limit.

    Returns
    -------
    pl.DataFrame
        Tidy DataFrame with one row per hour, columns matching ``_OUTPUT_COLS``
        plus a ``year`` column for Hive partitioning.  Empty DataFrame if no
        data found.

    Notes
    -----
    ``pnode_name`` is not a filterable field in the PJM Data Miner API, so
    all ZONE-type rows are fetched and filtered client-side for the requested
    zone.  This returns ~20 rows per hour (one per PJM zone) instead of 1,
    but the extra rows are dropped before returning.
    """
    key = _load_api_key(api_key)
    cutoff = _archive_cutoff()
    chunks = _split_date_range(start_date, end_date)
    session = requests.Session()
    all_rows: list[dict] = []

    print(
        f"Fetching rt_hrl_lmps for zone={zone_code} "
        f"{start_date} → {end_date} ({len(chunks)} chunk(s))"
    )

    def _fetch_chunk(chunk_start: date, chunk_end: date) -> list[dict]:
        """Fetch all pages for one date chunk.

        If PJM rejects the request because the range straddles the
        archive/standard boundary, bisect the chunk and fetch each half.
        This handles off-by-one uncertainty in the cutoff date.
        """
        is_archive = chunk_start < cutoff
        date_str = (
            f"{chunk_start.strftime('%-m-%-d-%Y')} 00:00 to "
            f"{chunk_end.strftime('%-m-%-d-%Y')} 23:59"
        )

        base_params: dict[str, str | int] = {
            "datetime_beginning_ept": date_str,
            "rowCount": _ROW_COUNT,
            "startRow": 1,
            "type": "ZONE",
            "row_is_current": "true" if row_is_current else "false",
        }

        print(
            f"  chunk {chunk_start} → {chunk_end} "
            f"({'archive' if is_archive else 'standard'})"
        )

        chunk_rows: list[dict] = []
        start_row = 1
        try:
            while True:
                params = {**base_params, "startRow": start_row}
                rows, total = _fetch_page(session, key, params)

                if not rows:
                    break

                rows = [r for r in rows if r.get("pnode_name") == zone_code]

                chunk_rows.extend(rows)

                fetched_so_far = start_row + len(rows) - 1
                print(f"    rows {start_row}–{fetched_so_far} of {total}")

                if fetched_so_far >= total:
                    break
                start_row += _ROW_COUNT

                time.sleep(page_delay_seconds)

        except _ArchiveBoundaryError:
            if chunk_start == chunk_end:
                raise RuntimeError(
                    f"PJM boundary error on a single-day chunk ({chunk_start}). "
                    "This is unexpected — the archive cutoff may have changed "
                    "mid-request. Try running again."
                )
            mid = chunk_start + (chunk_end - chunk_start) // 2
            print(
                f"  ↳ boundary error — bisecting at {mid} and retrying "
                f"({chunk_start}→{mid}, {mid + timedelta(days=1)}→{chunk_end})"
            )
            chunk_rows = _fetch_chunk(chunk_start, mid)
            chunk_rows += _fetch_chunk(mid + timedelta(days=1), chunk_end)

        return chunk_rows

    for chunk_start, chunk_end in chunks:
        all_rows.extend(_fetch_chunk(chunk_start, chunk_end))

    if not all_rows:
        print(f"  No data returned for zone={zone_code} {start_date}→{end_date}")
        return pl.DataFrame()

    df = pl.DataFrame(all_rows)

    # Normalise column names to lowercase (API returns mixed case).
    df = df.rename({c: c.lower() for c in df.columns})

    # Parse timestamps — the API may return different formats than the CSV
    # (e.g. ISO 8601 vs US-style with AM/PM).
    for ts_col in ("datetime_beginning_utc", "datetime_beginning_ept"):
        if df.schema[ts_col] == pl.String:
            df = _parse_ts_column(df, ts_col)

    df = (
        df.with_columns(pl.col("pnode_name").alias("zone"))
        .with_columns(pl.col("datetime_beginning_utc").dt.year().alias("year"))
        .select(_OUTPUT_COLS + ["year"])
        .sort("zone", "datetime_beginning_utc")
    )

    print(f"  → {df.height:,} rows for zone={zone_code}")
    return df


# Canonical output column order
_OUTPUT_COLS = [
    "datetime_beginning_utc",
    "datetime_beginning_ept",
    "pnode_id",
    "pnode_name",
    "zone",
    "total_lmp_rt",
    "system_energy_price_rt",
    "congestion_price_rt",
    "marginal_loss_price_rt",
    "row_is_current",
    "version_nbr",
]

S3_PATH_DEFAULT = "s3://data.sb/pjm/lmp/real_time/rt_hrl_lmps.csv"


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------


def extract_md_zone_lmps(
    df: pl.DataFrame,
    zone_map: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Extract ZONE-type aggregate LMP rows for Maryland utilities.

    For each utility in *zone_map*, finds all rows where:
      - ``type == "ZONE"`` (zone-level aggregate, not an individual bus/node)
      - ``pnode_name == <pjm_zone_code>``  (e.g. "BGE", "PEPCO", "DPL", "APS")

    Note: filtering on the ``zone`` column will NOT find these rows because
    ZONE-type aggregate nodes have ``zone = null`` in the PJM feed.

    Returns a tidy DataFrame with one row per (utility_zone, timestamp), with
    an added ``zone`` column populated from ``pnode_name`` and a ``year``
    column extracted from ``datetime_beginning_utc`` for Hive partitioning.

    Parameters
    ----------
    df:
        Raw LMP DataFrame read from the PJM rt_hrl_lmps CSV.
    zone_map:
        Optional override mapping of ``{utility_slug: pnode_name}``. Defaults
        to the MD entries from ``data/pjm/zone_mapping/generate_zone_mapping_csv.py``.
    """
    if zone_map is None:
        zone_map = _zone_map_for_state("md")
    pjm_zone_codes = list(zone_map.values())

    filtered = df.filter(
        (pl.col("type") == "ZONE") & pl.col("pnode_name").is_in(pjm_zone_codes)
    )

    for ts_col in ("datetime_beginning_utc", "datetime_beginning_ept"):
        if filtered.schema[ts_col] == pl.String:
            filtered = _parse_ts_column(filtered, ts_col)

    result = (
        filtered.with_columns(pl.col("pnode_name").alias("zone"))
        .with_columns(pl.col("datetime_beginning_utc").dt.year().alias("year"))
        .select(_OUTPUT_COLS + ["year"])
        .sort("zone", "datetime_beginning_utc")
    )

    _print_summary(result, zone_map)
    return result


def _print_summary(df: pl.DataFrame, zone_map: dict[str, str]) -> None:
    pjm_zone_codes = list(zone_map.values())
    print(f"\nExtracted {df.height:,} zone-aggregate rows for MD utilities")
    summary = (
        df.group_by("zone", "year")
        .agg(
            pl.len().alias("n_hours"),
            pl.col("total_lmp_rt").mean().round(2).alias("avg_lmp_rt"),
            pl.col("total_lmp_rt").min().round(2).alias("min_lmp_rt"),
            pl.col("total_lmp_rt").max().round(2).alias("max_lmp_rt"),
        )
        .sort("zone", "year")
    )
    print(summary)

    # Warn about any expected zones with no data
    found = set(df["zone"].unique().to_list())
    missing = set(pjm_zone_codes) - found
    if missing:
        print(f"\nWARNING: no ZONE-type rows found for: {sorted(missing)}")
        print("  Check that the CSV covers the expected date range and zones.")


# ---------------------------------------------------------------------------
# S3 upload (Hive-partitioned by zone and year)
# ---------------------------------------------------------------------------


def upload_to_s3(
    df: pl.DataFrame,
    s3_base: str,
    *,
    dry_run: bool = False,
) -> None:
    """Write zone-LMP DataFrame to Hive-partitioned Parquet on S3.

    Output layout::

        s3://<base>/zone={ZONE}/year={YEAR}/data.parquet

    Args:
        df:       DataFrame returned by :func:`extract_md_zone_lmps`.
        s3_base:  Base S3 prefix (e.g. ``s3://data.sb/pjm/lmp/real_time/zones``).
        dry_run:  If True, print paths without writing.
    """
    write_hive_partitioned_parquet_to_s3(
        df, s3_base, partition_cols=["zone", "year"], dry_run=dry_run
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_S3_OUTPUT_DEFAULT = "s3://data.sb/pjm/lmp/real_time/zones"


def _cmd_fetch_api(args: argparse.Namespace) -> None:
    """CLI handler for the ``fetch-api`` subcommand."""
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    zones = [z.strip() for z in args.zone.split(",")]

    if args.dry_run:
        # Print the chunk plan without making any API calls.
        cutoff = _archive_cutoff()
        for zone_code in zones:
            chunks = _split_date_range(start, end)
            print(
                f"\nzone={zone_code}: {len(chunks)} chunk(s) — no API calls in dry-run"
            )
            for chunk_start, chunk_end in chunks:
                kind = "archive" if chunk_start < cutoff else "standard"
                days = (chunk_end - chunk_start).days + 1
                print(f"  {chunk_start} → {chunk_end}  ({kind}, {days} days)")
        print(f"\nOutput would go to: {args.s3_output}")
        return

    local_dir = Path("data/pjm/_local_lmp")
    local_dir.mkdir(parents=True, exist_ok=True)

    for zone_code in zones:
        lmps = fetch_rt_lmps_for_zone(
            zone_code,
            start,
            end,
            page_delay_seconds=args.page_delay,
        )
        if lmps.is_empty():
            raise SystemExit(
                f"\nERROR: no data returned for zone={zone_code}. Stopping."
            )

        # Save locally first so data isn't lost if validation or upload fails.
        local_path = local_dir / f"{zone_code}_{start}_{end}.parquet"
        lmps.write_parquet(local_path)
        print(f"\n  Saved locally to {local_path}")

        zone_ok, msgs = validate_zone_lmp(lmps, zone_code, start, end)
        print(f"\n  Validation for {zone_code}:")
        for m in msgs:
            print(f"    {m}")
        if not zone_ok:
            raise SystemExit(
                f"\nERROR: validation failed for zone={zone_code}. "
                f"Data saved to {local_path} for inspection. Not uploaded."
            )

        print(f"\nUploading {zone_code} data to {args.s3_output} ...")
        upload_to_s3(lmps, args.s3_output)


def _cmd_extract_csv(args: argparse.Namespace) -> None:
    """CLI handler for the ``extract-csv`` subcommand."""
    raw = read_csv_from_s3(args.s3_path)
    lmps = extract_md_zone_lmps(raw)

    if args.s3_output and not args.dry_run:
        print(f"\nUploading to {args.s3_output} ...")
        upload_to_s3(lmps, args.s3_output)
    elif args.dry_run:
        out = args.s3_output or _S3_OUTPUT_DEFAULT
        print(f"\n[dry-run] Would upload to {out}")
        upload_to_s3(lmps, out, dry_run=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    # -- fetch-api subcommand ------------------------------------------------
    p_api = sub.add_parser(
        "fetch-api",
        help="Fetch RT LMP data from the PJM Data Miner 2 API.",
    )
    p_api.add_argument(
        "--zone",
        required=True,
        help="PJM zone code(s), comma-separated (e.g. BGE or BGE,PEPCO,DPL,APS).",
    )
    p_api.add_argument(
        "--start-date",
        required=True,
        help="Start date inclusive, ISO format: YYYY-MM-DD.",
    )
    p_api.add_argument(
        "--end-date",
        required=True,
        help="End date inclusive, ISO format: YYYY-MM-DD.",
    )
    p_api.add_argument(
        "--s3-output",
        default=_S3_OUTPUT_DEFAULT,
        help="S3 base URI for Hive-partitioned output (default: %(default)s).",
    )
    p_api.add_argument(
        "--page-delay",
        type=float,
        default=11.0,
        metavar="SECONDS",
        help=(
            "Seconds to sleep between paginated API requests "
            "(default: %(default)s, ≈5.5 req/min — safe for a 6 req/min quota)."
        ),
    )
    p_api.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be uploaded without writing to S3.",
    )

    # -- extract-csv subcommand ----------------------------------------------
    p_csv = sub.add_parser(
        "extract-csv",
        help="Extract MD zone LMPs from a pre-downloaded S3 CSV.",
    )
    p_csv.add_argument(
        "--s3-path",
        default=S3_PATH_DEFAULT,
        help="S3 URI of the source RT LMP CSV (default: %(default)s).",
    )
    p_csv.add_argument(
        "--s3-output",
        default=None,
        help="S3 base URI for Hive-partitioned output.",
    )
    p_csv.add_argument(
        "--dry-run",
        action="store_true",
        help="Print upload paths without writing to S3.",
    )

    args = parser.parse_args()
    if args.cmd == "fetch-api":
        _cmd_fetch_api(args)
    else:
        _cmd_extract_csv(args)


if __name__ == "__main__":
    main()
