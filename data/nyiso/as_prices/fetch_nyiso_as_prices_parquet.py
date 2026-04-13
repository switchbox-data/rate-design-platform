#!/usr/bin/env python3
"""Fetch NYISO ancillary service clearing prices via gridstatus → Hive-partitioned parquet.

Uses ``NYISO._download_nyiso_archive`` with MIS datasets ``damasp`` (day-ahead hourly)
and ``rtasp`` (real-time 5-minute) so **raw archive columns** are available. **Default**
is ``rt`` only (5-minute rows)—the finest clearing-price resolution NYISO publishes in
``rtasp``. Use ``--markets dam`` for hourly day-ahead only, or ``both`` for both.

The public
``get_as_prices_*`` methods call ``_handle_as_prices``, which drops ``Time Zone``,
``PTID``, and the full ``($/MWHr)`` / ``($/MW)`` column names; this pipeline avoids
that path.

Interval start/end are aligned to match ``gridstatus.NYISO._handle_as_prices`` (same
as ``get_as_prices_*``).

Parquet uses snake_case names; MIS CSV equivalents:

    time_zone                         ← Time Zone
    zone                              ← Name
    ptid                              ← PTID
    spin_10min_usd_per_mwhr           ← 10 Min Spinning Reserve ($/MWHr)
    non_sync_10min_usd_per_mwhr       ← 10 Min Non-Synchronous Reserve ($/MWHr)
    operating_30min_usd_per_mwhr      ← 30 Min Operating Reserve ($/MWHr)
    nyca_regulation_capacity_usd_per_mwhr ← NYCA Regulation Capacity ($/MWHr)
    nyca_regulation_movement_usd_per_mw   ← NYCA Regulation Movement ($/MW); null for DAM

Writes:
    <output_dir>/year={YYYY}/month={MM}/data.parquet

Schema (wide: one row per interval × zone × market):
    year, month, market, time_et, interval_start_et, interval_end_et,
    zone, time_zone, ptid,
    spin_10min_usd_per_mwhr, non_sync_10min_usd_per_mwhr,
    operating_30min_usd_per_mwhr, nyca_regulation_capacity_usd_per_mwhr,
    nyca_regulation_movement_usd_per_mw

Usage:
    uv run python data/nyiso/as_prices/fetch_nyiso_as_prices_parquet.py \\
        --start 2024-01 --end 2024-03 \\
        --path-local-parquet data/nyiso/as_prices/parquet
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from gridstatus import NYISO

# Raw MIS headers (post-``_handle_time`` inside ``_download_nyiso_archive``).
_MIS_MOVEMENT = "NYCA Regulation Movement ($/MW)"


def parse_markets_cli(markets: str) -> frozenset[str]:
    """Resolve ``--markets`` / Just ``markets=`` to a frozenset of ISO segments.

    Default (finest resolution): ``rt`` (5-minute real-time). ``both`` → dam + rt.
    """
    raw = {x.strip().lower() for x in markets.split(",") if x.strip()}
    if not raw:
        return frozenset({"rt"})
    if raw == {"both"}:
        return frozenset({"dam", "rt"})
    allowed = {"dam", "rt"}
    bad = raw - allowed
    if bad:
        raise ValueError(f"unknown markets {bad}; use dam, rt, or both")
    return frozenset(raw)


def _reject_just_placeholders(val: str) -> None:
    if "{{" in val or "}}" in val:
        print(
            f"ERROR: looks like an uninterpolated Just variable: {val}", file=sys.stderr
        )
        sys.exit(1)


def _parse_month(s: str) -> tuple[int, int]:
    parts = s.strip().split("-")
    if len(parts) != 2:
        raise ValueError(f"expected YYYY-MM, got {s!r}")
    y, m = int(parts[0]), int(parts[1])
    if not (1 <= m <= 12):
        raise ValueError(f"invalid month in {s!r}")
    return (y, m)


def _month_range(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    y, m = start
    while (y, m) <= end:
        out.append((y, m))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def _month_start_ts(iso: NYISO, year: int, month: int) -> pd.Timestamp:
    return pd.Timestamp(year=year, month=month, day=1, tz=iso.default_timezone)


def _month_end_exclusive_ts(iso: NYISO, year: int, month: int) -> pd.Timestamp:
    if month == 12:
        return pd.Timestamp(year=year + 1, month=1, day=1, tz=iso.default_timezone)
    return pd.Timestamp(year=year, month=month + 1, day=1, tz=iso.default_timezone)


def _apply_handle_as_prices_intervals(
    pdf: pd.DataFrame, *, rt_or_dam: str
) -> pd.DataFrame:
    """Mirror ``NYISO._handle_as_prices`` interval adjustments only."""
    df = pdf.copy()
    if rt_or_dam == "rt":
        df["Interval End"] = df["Interval Start"]
        df["Interval Start"] = df["Interval Start"] - pd.Timedelta(minutes=5)
    else:
        df["Interval End"] = df["Interval Start"] + pd.Timedelta(minutes=60)
    return df


def _raw_archive_pdf_to_polars(
    pdf: pd.DataFrame, *, market: str, year: int, month: int
) -> pl.DataFrame:
    """Wide rows: one row per (interval, zone) with all MIS-derived measures."""
    required = [
        "Time",
        "Time Zone",
        "Name",
        "PTID",
        "10 Min Spinning Reserve ($/MWHr)",
        "10 Min Non-Synchronous Reserve ($/MWHr)",
        "30 Min Operating Reserve ($/MWHr)",
        "NYCA Regulation Capacity ($/MWHr)",
        "Interval Start",
        "Interval End",
    ]
    missing = [c for c in required if c not in pdf.columns]
    if missing:
        raise ValueError(f"missing expected MIS columns: {missing}")

    pdf = _apply_handle_as_prices_intervals(
        pdf, rt_or_dam="rt" if market == "rt" else "dam"
    )
    if _MIS_MOVEMENT not in pdf.columns:
        pdf[_MIS_MOVEMENT] = pd.NA

    plf = pl.from_pandas(pdf)
    out = plf.select(
        pl.col("Time").cast(pl.Datetime("us", "America/New_York")).alias("time_et"),
        pl.col("Interval Start")
        .cast(pl.Datetime("us", "America/New_York"))
        .alias("interval_start_et"),
        pl.col("Interval End")
        .cast(pl.Datetime("us", "America/New_York"))
        .alias("interval_end_et"),
        pl.col("Time Zone").cast(pl.String).alias("time_zone"),
        pl.col("Name").cast(pl.String).alias("zone"),
        pl.col("PTID").cast(pl.Int64).alias("ptid"),
        pl.col("10 Min Spinning Reserve ($/MWHr)")
        .cast(pl.Float64)
        .alias("spin_10min_usd_per_mwhr"),
        pl.col("10 Min Non-Synchronous Reserve ($/MWHr)")
        .cast(pl.Float64)
        .alias("non_sync_10min_usd_per_mwhr"),
        pl.col("30 Min Operating Reserve ($/MWHr)")
        .cast(pl.Float64)
        .alias("operating_30min_usd_per_mwhr"),
        pl.col("NYCA Regulation Capacity ($/MWHr)")
        .cast(pl.Float64)
        .alias("nyca_regulation_capacity_usd_per_mwhr"),
        pl.col(_MIS_MOVEMENT)
        .cast(pl.Float64)
        .alias("nyca_regulation_movement_usd_per_mw"),
    ).with_columns(
        pl.lit(market).cast(pl.Categorical).alias("market"),
        pl.lit(year).cast(pl.Int16).alias("year"),
        pl.lit(month).cast(pl.UInt8).alias("month"),
    )

    ordered = [
        "year",
        "month",
        "market",
        "time_et",
        "interval_start_et",
        "interval_end_et",
        "time_zone",
        "zone",
        "ptid",
        "spin_10min_usd_per_mwhr",
        "non_sync_10min_usd_per_mwhr",
        "operating_30min_usd_per_mwhr",
        "nyca_regulation_capacity_usd_per_mwhr",
        "nyca_regulation_movement_usd_per_mw",
    ]
    return out.select(ordered).sort(
        "market",
        "interval_start_et",
        "zone",
    )


def fetch_as_month(
    iso: NYISO,
    year: int,
    month: int,
    *,
    markets: frozenset[str],
) -> pl.DataFrame | None:
    """Fetch one calendar month via archive download (preserves MIS columns)."""
    start_ts = _month_start_ts(iso, year, month)
    end_ts = _month_end_exclusive_ts(iso, year, month)
    frames: list[pl.DataFrame] = []

    if "dam" in markets:
        try:
            pdf = iso._download_nyiso_archive(
                date=start_ts,
                end=end_ts,
                dataset_name="damasp",
                verbose=False,
            )
        except Exception as e:
            print(f"    dam: error {e!s}")
            pdf = pd.DataFrame()
        if pdf is not None and len(pdf) > 0:
            frames.append(
                _raw_archive_pdf_to_polars(pdf, market="dam", year=year, month=month)
            )

    if "rt" in markets:
        try:
            pdf = iso._download_nyiso_archive(
                date=start_ts,
                end=end_ts,
                dataset_name="rtasp",
                verbose=False,
            )
        except Exception as e:
            print(f"    rt: error {e!s}")
            pdf = pd.DataFrame()
        if pdf is not None and len(pdf) > 0:
            frames.append(
                _raw_archive_pdf_to_polars(pdf, market="rt", year=year, month=month)
            )

    if not frames:
        return None
    return pl.concat(frames, how="vertical")


def _write_month_partition(df: pl.DataFrame, output_dir: Path) -> None:
    y = int(df["year"][0])
    m = int(df["month"][0])
    part_dir = output_dir / f"year={y}" / f"month={m:02d}"
    part_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(part_dir / "data.parquet", compression="snappy")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch NYISO AS prices via gridstatus archive → Hive-partitioned parquet",
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="First month (YYYY-MM)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="",
        help="Last month (YYYY-MM). Default: last complete calendar month.",
    )
    parser.add_argument(
        "--path-local-parquet",
        type=str,
        required=True,
        help="Output directory for Hive-partitioned parquet",
    )
    parser.add_argument(
        "--markets",
        type=str,
        default="rt",
        help="Comma-separated: rt (default, 5-min), dam (hourly), or both",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-fetch months even if data.parquet already exists (default: skip existing)",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=60.0,
        help="Pause between months (default: 60, same as ISO-NE ancillary fetch)",
    )
    args = parser.parse_args()

    _reject_just_placeholders(args.path_local_parquet)
    output_dir = Path(args.path_local_parquet)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        markets = parse_markets_cli(args.markets)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    start_m = _parse_month(args.start)
    if args.end:
        end_m = _parse_month(args.end)
    else:
        today = datetime.now()
        if today.month == 1:
            end_m = (today.year - 1, 12)
        else:
            end_m = (today.year, today.month - 1)

    months = _month_range(start_m, end_m)
    if not months:
        print("No months in range.")
        return 0

    to_fetch: list[tuple[int, int]] = []
    for y, m in months:
        part_file = output_dir / f"year={y}" / f"month={m:02d}" / "data.parquet"
        if part_file.exists() and not args.overwrite:
            continue
        to_fetch.append((y, m))

    if not to_fetch:
        print("All partitions already exist. Nothing to fetch.")
        return 0

    print(f"Fetching {len(to_fetch)} months...")
    iso = NYISO()
    written = 0
    failed = 0

    for i, (y, m) in enumerate(to_fetch):
        label = f"{y}-{m:02d}"
        part_file = output_dir / f"year={y}" / f"month={m:02d}" / "data.parquet"
        if i > 0 and args.sleep_seconds > 0:
            print("  (cooling down {:.0f}s...)".format(args.sleep_seconds), flush=True)
            time.sleep(args.sleep_seconds)

        print(f"  {label}: fetching...", end=" ", flush=True)
        df = fetch_as_month(iso, y, m, markets=markets)
        if df is None:
            print("no data")
            failed += 1
            continue

        n_outside = df.filter(
            (pl.col("interval_start_et").dt.year() != y)
            | (pl.col("interval_start_et").dt.month() != m)
        ).height
        _write_month_partition(df, output_dir)
        written += 1
        print(f"{len(df)} rows")
        if n_outside:
            print(
                f"  {label}: WARN {n_outside} rows with interval_start_et outside "
                f"{y}-{m:02d} (ET)",
                file=sys.stderr,
            )

    print(f"\nDone. {written} partitions written, {failed} months with no data.")
    print(f"Output: {output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
