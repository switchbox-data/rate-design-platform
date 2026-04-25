"""Compare two ``all_utilities`` master parquet batches at the same run pair.

For each electric utility (Hive partition under ``comb_bills_year_target`` /
``cross_subsidization_BAT_values``), loads both batches, inner-joins on natural
keys, and checks that every column present in both frames matches within
tolerance (numeric) or exactly (integer, boolean, string).

Natural keys:

- Master bills: ``bldg_id``, ``month``
- Master BAT: ``bldg_id``

CLI example (requires AWS credentials for S3)::

  uv run python utils/post/compare_master_batches.py \\
    --state ny \\
    --batch-a ny_20260416a_r1-36 \\
    --batch-b ny_20260416a_with_passthrough_r1-36 \\
    --run-delivery 1 --run-supply 2
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import polars as pl

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.post.build_master_bat import VALID_RUN_PAIRS
from utils.post.io import BLDG_ID, path_or_s3

COMB_REL = "comb_bills_year_target"
BAT_REL = "cross_subsidization_BAT_values"
PARTITION_PREFIX = "sb.electric_utility="
_COMPARE_SKIP = "__compare_master_batches_skip__"


def _master_paths(
    *,
    path_output_base: str,
    state_lower: str,
    batch: str,
    run_delivery: int,
    run_supply: int,
) -> tuple[str, str]:
    root = (
        f"{path_output_base.rstrip('/')}/hp_rates/{state_lower}/"
        f"all_utilities/{batch}/run_{run_delivery}+{run_supply}/"
    )
    return f"{root}{COMB_REL}/", f"{root}{BAT_REL}/"


def _list_utilities_from_hive_root(comb_root: str) -> list[str]:
    """Return sorted utility codes from ``sb.electric_utility=<code>/`` dirs."""
    p = path_or_s3(comb_root.rstrip("/") + "/")
    if isinstance(p, Path):
        if not p.is_dir():
            msg = f"Master comb path is not a directory: {comb_root}"
            raise FileNotFoundError(msg)
        names: list[str] = []
        for child in p.iterdir():
            if child.is_dir() and child.name.startswith(PARTITION_PREFIX):
                names.append(child.name.removeprefix(PARTITION_PREFIX))
        if not names:
            msg = f"No {PARTITION_PREFIX}* partitions under {comb_root}"
            raise FileNotFoundError(msg)
        return sorted(names)
    # S3 (cloudpathlib)
    if not p.exists():
        msg = f"Master comb path not found: {comb_root}"
        raise FileNotFoundError(msg)
    names = []
    for child in p.iterdir():
        if child.is_dir() and child.name.startswith(PARTITION_PREFIX):
            names.append(child.name.removeprefix(PARTITION_PREFIX))
    if not names:
        msg = f"No {PARTITION_PREFIX}* partitions under {comb_root}"
        raise FileNotFoundError(msg)
    return sorted(names)


def _load_utility_table(
    *,
    root: str,
    utility: str,
    storage_options: dict[str, str] | None,
) -> pl.DataFrame:
    path = f"{root.rstrip('/')}/{PARTITION_PREFIX}{utility}/data.parquet"
    return pl.read_parquet(path, storage_options=storage_options)


def _validate_run_pair(run_delivery: int, run_supply: int) -> None:
    if (run_delivery, run_supply) not in VALID_RUN_PAIRS:
        pairs = ", ".join(f"{d}+{s}" for d, s in sorted(VALID_RUN_PAIRS))
        msg = f"Invalid run pair {run_delivery}+{run_supply}. Expected one of: {pairs}"
        raise ValueError(msg)


def _is_exact_dtype(dt: pl.DataType) -> bool:
    if dt == pl.Boolean:
        return True
    if dt.is_integer():
        return True
    if dt == pl.String or dt == pl.Utf8:
        return True
    if isinstance(dt, pl.Categorical):
        return True
    if dt.is_temporal():
        return True
    return False


def _is_float_dtype(dt: pl.DataType) -> bool:
    return dt.is_float()


def _compare_column(
    *,
    col: str,
    left: pl.Series,
    right: pl.Series,
    abs_tol: float,
    rel_tol: float,
) -> str | None:
    """Return an error message or None if the column matches."""
    if left.dtype != right.dtype:
        if left.dtype.is_numeric() and right.dtype.is_numeric():
            left_f = left.cast(pl.Float64)
            right_f = right.cast(pl.Float64)
            return _compare_float_series(
                col=col, left=left_f, right=right_f, abs_tol=abs_tol, rel_tol=rel_tol
            )
        return (
            f"Column {col!r}: dtype mismatch {left.dtype} vs {right.dtype} "
            "(cannot compare)"
        )

    one_null = left.is_null() ^ right.is_null()
    if bool(one_null.any()):
        n = int(one_null.sum())
        return f"Column {col!r}: {n} rows have null on exactly one side"

    if _is_exact_dtype(left.dtype):
        neq = left != right
        if bool(neq.any()):
            n = int(neq.sum())
            return f"Column {col!r}: {n} rows differ (exact equality required)"
        return None

    if _is_float_dtype(left.dtype):
        return _compare_float_series(
            col=col,
            left=left.cast(pl.Float64),
            right=right.cast(pl.Float64),
            abs_tol=abs_tol,
            rel_tol=rel_tol,
        )

    return _COMPARE_SKIP


def _compare_float_series(
    *,
    col: str,
    left: pl.Series,
    right: pl.Series,
    abs_tol: float,
    rel_tol: float,
) -> str | None:
    both_null = left.is_null() & right.is_null()
    one_null = left.is_null() ^ right.is_null()
    if bool(one_null.any()):
        n = int(one_null.sum())
        return f"Column {col!r}: {n} rows have null on exactly one side"
    diff = (left - right).abs().to_numpy()
    scale = np.maximum(left.abs().to_numpy(), right.abs().to_numpy())
    tol = abs_tol + rel_tol * scale
    bn = both_null.to_numpy()
    ok = bn | (~bn & (diff <= tol))
    bad = ~ok
    if bool(bad.any()):
        n = int(bad.sum())
        max_diff = float(np.nanmax(diff))
        return (
            f"Column {col!r}: {n} rows exceed tol (abs_tol={abs_tol}, "
            f"rel_tol={rel_tol}), max_abs_diff={max_diff:.6e}"
        )
    return None


def compare_frames(
    *,
    left: pl.DataFrame,
    right: pl.DataFrame,
    join_keys: list[str],
    label: str,
    utility: str,
    abs_tol: float,
    rel_tol: float,
) -> list[str]:
    """Inner-join on ``join_keys`` and compare shared non-key columns."""
    errors: list[str] = []
    missing_keys = [
        k for k in join_keys if k not in left.columns or k not in right.columns
    ]
    if missing_keys:
        return [
            f"[{utility} {label}] join keys missing in frame(s): {missing_keys} "
            f"(left cols={left.columns}, right cols={right.columns})"
        ]

    left_n = left.select(join_keys).n_unique()
    right_n = right.select(join_keys).n_unique()
    if left_n != left.height:
        errors.append(
            f"[{utility} {label}] left frame has duplicate keys "
            f"({left.height} rows, {left_n} unique key tuples)"
        )
    if right_n != right.height:
        errors.append(
            f"[{utility} {label}] right frame has duplicate keys "
            f"({right.height} rows, {right_n} unique key tuples)"
        )

    if left.height != right.height:
        errors.append(
            f"[{utility} {label}] row count differs before join: "
            f"{left.height} vs {right.height}"
        )

    common = sorted(
        (set(left.columns) & set(right.columns)) - set(join_keys),
    )
    only_l = sorted(set(left.columns) - set(right.columns))
    only_r = sorted(set(right.columns) - set(left.columns))
    if only_l:
        print(
            f"[{utility} {label}] note: {len(only_l)} column(s) only in batch-a "
            f"(not compared): {only_l[:12]}{'...' if len(only_l) > 12 else ''}",
            file=sys.stderr,
        )
    if only_r:
        print(
            f"[{utility} {label}] note: {len(only_r)} column(s) only in batch-b "
            f"(not compared): {only_r[:12]}{'...' if len(only_r) > 12 else ''}",
            file=sys.stderr,
        )

    joined = left.join(right, on=join_keys, how="inner", suffix="_b")
    if joined.height != left.height or joined.height != right.height:
        errors.append(
            f"[{utility} {label}] inner join row count {joined.height} != "
            f"left {left.height} / right {right.height} (key mismatch between batches)"
        )
        return errors

    for col in common:
        rhs = f"{col}_b"
        if rhs not in joined.columns:
            errors.append(f"[{utility} {label}] internal: missing {rhs!r} after join")
            continue
        msg = _compare_column(
            col=col,
            left=joined[col],
            right=joined[rhs],
            abs_tol=abs_tol,
            rel_tol=rel_tol,
        )
        if msg == _COMPARE_SKIP:
            print(
                f"[{utility} {label}] skip column {col!r} (dtype {joined[col].dtype})",
                file=sys.stderr,
            )
        elif msg:
            errors.append(f"[{utility} {label}] {msg}")

    return errors


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Compare master bills and BAT between two all_utilities batches.",
    )
    p.add_argument("--state", required=True, help="State code, e.g. ny")
    p.add_argument("--batch-a", required=True, help="First batch name (baseline).")
    p.add_argument("--batch-b", required=True, help="Second batch name (comparison).")
    p.add_argument(
        "--path-output-base",
        default="s3://data.sb/switchbox/cairo/outputs",
        help="Root containing hp_rates/<state>/all_utilities/...",
    )
    p.add_argument("--run-delivery", type=int, required=True)
    p.add_argument("--run-supply", type=int, required=True)
    p.add_argument(
        "--utilities",
        default=None,
        help="Comma-separated utilities; default = partitions under batch-a comb path.",
    )
    p.add_argument(
        "--abs-tol",
        type=float,
        default=1e-5,
        help="Absolute tolerance for float columns (default 1e-5).",
    )
    p.add_argument(
        "--rel-tol",
        type=float,
        default=0.0,
        help="Relative tolerance for float columns: tol += rel_tol * max(|a|,|b|).",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    state_lower = args.state.lower()
    _validate_run_pair(args.run_delivery, args.run_supply)

    comb_a, bat_a = _master_paths(
        path_output_base=args.path_output_base,
        state_lower=state_lower,
        batch=args.batch_a,
        run_delivery=args.run_delivery,
        run_supply=args.run_supply,
    )
    comb_b, bat_b = _master_paths(
        path_output_base=args.path_output_base,
        state_lower=state_lower,
        batch=args.batch_b,
        run_delivery=args.run_delivery,
        run_supply=args.run_supply,
    )

    utilities = (
        [u.strip() for u in args.utilities.split(",") if u.strip()]
        if args.utilities
        else _list_utilities_from_hive_root(comb_a)
    )

    opts = get_aws_storage_options()
    all_errors: list[str] = []

    print(
        f"Comparing batch-a={args.batch_a!r} vs batch-b={args.batch_b!r} "
        f"state={state_lower} run={args.run_delivery}+{args.run_supply} "
        f"utilities={utilities}",
        file=sys.stderr,
    )

    join_bills = [BLDG_ID, "month"]
    join_bat = [BLDG_ID]

    for u in utilities:
        try:
            bills_l = _load_utility_table(root=comb_a, utility=u, storage_options=opts)
            bills_r = _load_utility_table(root=comb_b, utility=u, storage_options=opts)
        except FileNotFoundError as e:
            all_errors.append(f"[{u} comb_bills] {e}")
            continue

        try:
            bat_l = _load_utility_table(root=bat_a, utility=u, storage_options=opts)
            bat_r = _load_utility_table(root=bat_b, utility=u, storage_options=opts)
        except FileNotFoundError as e:
            all_errors.append(f"[{u} BAT] {e}")
            continue

        bills_l = bills_l.sort(join_bills)
        bills_r = bills_r.sort(join_bills)
        bat_l = bat_l.sort(join_bat)
        bat_r = bat_r.sort(join_bat)

        all_errors.extend(
            compare_frames(
                left=bills_l,
                right=bills_r,
                join_keys=join_bills,
                label="comb_bills",
                utility=u,
                abs_tol=args.abs_tol,
                rel_tol=args.rel_tol,
            )
        )
        all_errors.extend(
            compare_frames(
                left=bat_l,
                right=bat_r,
                join_keys=join_bat,
                label="BAT",
                utility=u,
                abs_tol=args.abs_tol,
                rel_tol=args.rel_tol,
            )
        )

    if all_errors:
        print(f"\n{len(all_errors)} issue(s):", file=sys.stderr)
        for line in all_errors:
            print(line, file=sys.stderr)
        return 1

    print("All comparisons passed.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
