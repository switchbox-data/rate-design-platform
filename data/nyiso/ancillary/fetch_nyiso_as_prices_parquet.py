#!/usr/bin/env python3
"""Fetch NYISO ancillary service clearing prices via gridstatus → Hive-partitioned parquet.

Uses ``NYISO._download_nyiso_archive`` with MIS datasets ``damasp`` (day-ahead hourly)
and ``rtasp`` (real-time 5-minute) so **raw archive columns** are available. **Default**
is ``rt`` only (5-minute rows)—the finest clearing-price resolution NYISO publishes in
``rtasp``. Use ``--markets dam`` for hourly day-ahead only, or ``both`` for both.

Historical MIS layouts (all normalized to the same output schema): **2010–2015** hub-wide
East/West prices (no PTID); **2016-01–2016-06-22** adds SENY and zonal columns; **2016-06**
also has a mid-month switch to zonal ``Name``/``PTID`` rows; **2016-07+** zonal only.
Duplicate CSV headers are coalesced (common in 2016 hybrid zips).

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
    uv run python data/nyiso/ancillary/fetch_nyiso_as_prices_parquet.py \\
        --start 2010-01 --end 2024-03 \\
        --path-local-parquet data/nyiso/ancillary/parquet
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from gridstatus import NYISO

# Raw MIS headers (post-``_handle_time`` inside ``_download_nyiso_archive``).
_MIS_MOVEMENT = "NYCA Regulation Movement ($/MW)"

_CANON_SPIN = "10 Min Spinning Reserve ($/MWHr)"
_CANON_NONSYNC = "10 Min Non-Synchronous Reserve ($/MWHr)"
_CANON_OPER = "30 Min Operating Reserve ($/MWHr)"
_CANON_REGCAP = "NYCA Regulation Capacity ($/MWHr)"

_MODERN_PRICE_COLS = (_CANON_SPIN, _CANON_NONSYNC, _CANON_OPER, _CANON_REGCAP)

_INTERVAL_CORE = ("Time", "Interval Start", "Interval End")


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


def _strip_column_names(pdf: pd.DataFrame) -> pd.DataFrame:
    out = pdf.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def _coerce_regulation_movement_column(pdf: pd.DataFrame) -> pd.DataFrame:
    """Rename any MIS regulation-movement column to ``_MIS_MOVEMENT``."""
    if _MIS_MOVEMENT in pdf.columns:
        return pdf
    for c in pdf.columns:
        if "Regulation Movement" in c and "$/MW" in c:
            return pdf.rename(columns={c: _MIS_MOVEMENT})
    return pdf


def _coalesce_duplicate_column_names(pdf: pd.DataFrame) -> pd.DataFrame:
    """MIS CSVs sometimes repeat the same header twice; merge left-to-right with bfill."""
    out = pdf.copy()
    dup_names = out.columns[out.columns.duplicated(keep=False)].unique()
    for name in dup_names:
        mask = out.columns == name
        block = out.loc[:, mask]
        merged = block.bfill(axis=1).iloc[:, 0]
        out = out.loc[:, ~mask]
        out[name] = merged
    return out


def _is_modern_zonal_layout(columns: list[str]) -> bool:
    return (
        "Name" in columns
        and "PTID" in columns
        and _CANON_SPIN in columns
        and _CANON_NONSYNC in columns
        and _CANON_OPER in columns
        and _CANON_REGCAP in columns
    )


def _legacy_wide_prefixes(columns: list[str]) -> list[tuple[str, str]]:
    """Return (column prefix with trailing space, canonical zone label) pairs."""
    out: list[tuple[str, str]] = []
    if any(c.startswith("SENY ") for c in columns):
        out.append(("SENY ", "SENY"))
    if any(c.startswith("East ") for c in columns):
        out.append(("East ", "EAST"))
    if any(c.startswith("West ") for c in columns):
        out.append(("West ", "WEST"))
    return out


def _find_zone_metric_columns(
    columns: list[str], zone_prefix: str
) -> dict[str, str | None]:
    """Map logical metric → raw column name for one legacy hub (East/West/SENY)."""
    spin = nonsync = oper = regcap = None
    for c in columns:
        if not c.startswith(zone_prefix):
            continue
        if "Movement" in c:
            continue
        if "10 Min Spinning" in c or ("Spinning" in c and "10" in c and "Reserve" in c):
            spin = c
        elif "Non-Synchronous" in c or "Non Synchronous" in c:
            nonsync = c
        elif "30 Min Operating" in c or ("Operating" in c and "30" in c):
            oper = c
        elif "Regulation" in c:
            regcap = c
    return {
        "spin": spin,
        "nonsync": nonsync,
        "oper": oper,
        "regcap": regcap,
    }


def _zone_metrics_non_null(zm: dict[str, str | None]) -> bool:
    return any(zm[k] is not None for k in zm)


def _infer_time_zone_abbrev(ts: pd.Timestamp) -> str:
    if isinstance(ts, pd.Timestamp) and ts.tzinfo is not None:
        return str(ts.strftime("%Z"))
    return ""


def _mis_extra_price_columns(columns: Iterable[str], consumed: set[str]) -> list[str]:
    out: list[str] = []
    for c in columns:
        if c in consumed:
            continue
        if "($/MWHr)" in c or "($/MW)" in c or "($/MWH)" in c:
            out.append(c)
    return out


def _mis_header_to_extra_snake(col: str) -> str:
    base = col.strip()
    for suf in (" ($/MWHr)", " ($/MW)", " ($/MWH)"):
        if base.endswith(suf):
            base = base[: -len(suf)]
            break
    slug = re.sub(r"[^A-Za-z0-9]+", "_", base).strip("_").lower()
    if col.strip().endswith("($/MW)") and not col.strip().endswith("($/MWHr)"):
        return f"{slug}_usd_per_mw"
    return f"{slug}_usd_per_mwhr"


def _legacy_east_west_to_canonical(
    pdf: pd.DataFrame, *, market: str
) -> tuple[pd.DataFrame, set[str]]:
    """Explode legacy hub-wide columns into one row per (interval, hub zone)."""
    cols = list(pdf.columns)
    zones = _legacy_wide_prefixes(cols)
    if not zones:
        raise ValueError(
            "NYISO AS archive: expected East/West (or SENY) hub columns; "
            f"got columns {cols!r}",
        )

    consumed: set[str] = set(_INTERVAL_CORE)
    movement_col: str | None = _MIS_MOVEMENT if _MIS_MOVEMENT in cols else None
    if movement_col:
        consumed.add(movement_col)
    tz_col = "Time Zone" if "Time Zone" in cols else None
    if tz_col:
        consumed.add(tz_col)

    zone_maps: dict[str, dict[str, str | None]] = {}
    for zpref, _zlabel in zones:
        zm = _find_zone_metric_columns(cols, zpref)
        zone_maps[zpref] = zm
        for _k, v in zm.items():
            if v is not None:
                consumed.add(v)

    for c in _MODERN_PRICE_COLS:
        if c in cols:
            consumed.add(c)
    for c in ("Name", "PTID"):
        if c in cols:
            consumed.add(c)

    extras_src = _mis_extra_price_columns(cols, consumed)
    for c in extras_src:
        consumed.add(c)

    records: list[dict[str, object]] = []

    for idx in range(len(pdf)):
        row = pdf.iloc[idx]
        ts = row["Time"]
        tz_val = row[tz_col] if tz_col else _infer_time_zone_abbrev(ts)
        mov = (
            row[movement_col] if market == "rt" and movement_col is not None else pd.NA
        )
        for zpref, zlabel in zones:
            zm = zone_maps[zpref]
            if not _zone_metrics_non_null(zm):
                continue
            rec: dict[str, object] = {
                "Time": ts,
                "Time Zone": tz_val,
                "Name": zlabel,
                "PTID": pd.NA,
                _CANON_SPIN: row[zm["spin"]] if zm["spin"] is not None else pd.NA,
                _CANON_NONSYNC: row[zm["nonsync"]]
                if zm["nonsync"] is not None
                else pd.NA,
                _CANON_OPER: row[zm["oper"]] if zm["oper"] is not None else pd.NA,
                _CANON_REGCAP: row[zm["regcap"]] if zm["regcap"] is not None else pd.NA,
                _MIS_MOVEMENT: mov,
                "Interval Start": row["Interval Start"],
                "Interval End": row["Interval End"],
            }
            for xc in extras_src:
                rec[xc] = row[xc]
            records.append(rec)

    out = pd.DataFrame.from_records(records)
    return out, consumed


def _consume_modern_layout_noise(columns: list[str]) -> set[str]:
    """Columns present in hybrid months that we ignore when reading zonal rows."""
    noise: set[str] = set()
    for c in columns:
        s = c.strip()
        if s.startswith("East ") or s.startswith("West ") or s.startswith("SENY "):
            noise.add(c)
    return noise


def _modern_to_canonical(
    pdf: pd.DataFrame, *, market: str
) -> tuple[pd.DataFrame, set[str]]:
    """Subset modern MIS zonal layout to canonical column names (+ extras)."""
    cols = list(pdf.columns)
    need = [
        "Time",
        "Time Zone",
        "Name",
        "PTID",
        _CANON_SPIN,
        _CANON_NONSYNC,
        _CANON_OPER,
        _CANON_REGCAP,
        "Interval Start",
        "Interval End",
    ]
    missing = [c for c in need if c not in cols]
    if missing:
        raise ValueError(f"missing expected modern MIS columns: {missing}")

    consumed = set(need)
    consumed |= _consume_modern_layout_noise(cols)
    if _MIS_MOVEMENT in cols:
        consumed.add(_MIS_MOVEMENT)

    extras_src = _mis_extra_price_columns(cols, consumed)
    for c in extras_src:
        consumed.add(c)

    take = need.copy()
    if _MIS_MOVEMENT in cols:
        take.append(_MIS_MOVEMENT)
    out = pdf[take + extras_src].copy()
    if _MIS_MOVEMENT not in out.columns:
        out[_MIS_MOVEMENT] = pd.NA
    if market != "rt":
        out[_MIS_MOVEMENT] = pd.NA
    return out, consumed


def _normalize_nyiso_as_archive_pdf(pdf: pd.DataFrame, *, market: str) -> pd.DataFrame:
    """Map historical MIS shapes (hub-wide, hybrid, zonal) to one canonical wide schema."""
    pdf = _strip_column_names(pdf)
    pdf = _coerce_regulation_movement_column(pdf)
    pdf = _coalesce_duplicate_column_names(pdf)
    cols = list(pdf.columns)

    if _is_modern_zonal_layout(cols):
        has_name = "Name" in pdf.columns
        if has_name and pdf["Name"].notna().any() and pdf["Name"].isna().any():
            leg = pdf[pdf["Name"].isna()].copy()
            for drop_c in ("Name", "PTID"):
                if drop_c in leg.columns:
                    leg = leg.drop(columns=[drop_c])
            mod = pdf[pdf["Name"].notna()].copy()
            out_l, _ = _legacy_east_west_to_canonical(leg, market=market)
            out_m, _ = _modern_to_canonical(mod, market=market)
            combined = pd.concat([out_l, out_m], ignore_index=True, sort=False)
            return combined.sort_values(
                ["Interval Start", "Name"],
                kind="mergesort",
            )

        if has_name and pdf["Name"].notna().all():
            out, _ = _modern_to_canonical(pdf, market=market)
            return out.sort_values(["Interval Start", "Name"], kind="mergesort")

        leg_only = pdf.drop(columns=[c for c in ("Name", "PTID") if c in pdf.columns])
        out, _ = _legacy_east_west_to_canonical(leg_only, market=market)
        return out.sort_values(["Interval Start", "Name"], kind="mergesort")

    out, _ = _legacy_east_west_to_canonical(pdf, market=market)
    return out.sort_values(["Interval Start", "Name"], kind="mergesort")


def _raw_archive_pdf_to_polars(
    pdf: pd.DataFrame, *, market: str, year: int, month: int
) -> pl.DataFrame:
    """Wide rows: one row per (interval, zone) with all MIS-derived measures."""
    pdf = _normalize_nyiso_as_archive_pdf(pdf, market=market)

    required = [
        "Time",
        "Time Zone",
        "Name",
        "PTID",
        _CANON_SPIN,
        _CANON_NONSYNC,
        _CANON_OPER,
        _CANON_REGCAP,
        "Interval Start",
        "Interval End",
    ]
    missing = [c for c in required if c not in pdf.columns]
    if missing:
        raise ValueError(f"missing expected MIS columns after normalize: {missing}")

    pdf = _apply_handle_as_prices_intervals(
        pdf, rt_or_dam="rt" if market == "rt" else "dam"
    )
    if _MIS_MOVEMENT not in pdf.columns:
        pdf[_MIS_MOVEMENT] = pd.NA

    core_exprs = [
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
        pl.col(_CANON_SPIN).cast(pl.Float64).alias("spin_10min_usd_per_mwhr"),
        pl.col(_CANON_NONSYNC).cast(pl.Float64).alias("non_sync_10min_usd_per_mwhr"),
        pl.col(_CANON_OPER).cast(pl.Float64).alias("operating_30min_usd_per_mwhr"),
        pl.col(_CANON_REGCAP)
        .cast(pl.Float64)
        .alias("nyca_regulation_capacity_usd_per_mwhr"),
        pl.col(_MIS_MOVEMENT)
        .cast(pl.Float64)
        .alias("nyca_regulation_movement_usd_per_mw"),
    ]

    plf = pl.from_pandas(pdf, nan_to_null=True)
    extra_cols = [c for c in plf.columns if c not in required and c != _MIS_MOVEMENT]
    extra_exprs = []
    extra_snakes: list[str] = []
    for c in sorted(extra_cols):
        snake = _mis_header_to_extra_snake(c)
        extra_snakes.append(snake)
        extra_exprs.append(pl.col(c).cast(pl.Float64).alias(snake))

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

    out = plf.select(
        *core_exprs,
        *extra_exprs,
    ).with_columns(
        pl.lit(market).cast(pl.Categorical).alias("market"),
        pl.lit(year).cast(pl.Int16).alias("year"),
        pl.lit(month).cast(pl.UInt8).alias("month"),
    )
    tail = sorted(extra_snakes)
    return out.select([*ordered, *tail]).sort(
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
        default=5.0,
        help="Pause between months (default: 5)",
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
