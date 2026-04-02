#!/usr/bin/env python3
"""Per-utility and worst-row diagnostics for NY master BAT/bills batch diffs."""

from __future__ import annotations

import argparse
import glob
import pathlib
import sys

import polars as pl

_POST_DIR = pathlib.Path(__file__).resolve().parent
if str(_POST_DIR) not in sys.path:
    sys.path.insert(0, str(_POST_DIR))

from verify_ny_epmc_master_batch import (  # noqa: E402
    BAT_KEYS,
    BILL_KEYS,
    _numeric_shared_cols,
    scan_bat,
    scan_bills,
)

TOL = 1e-6


def _join_old_new(
    df_old: pl.DataFrame, df_new: pl.DataFrame, keys: list[str], common: list[str]
) -> pl.DataFrame:
    ln = df_new.select(keys + [pl.col(c).alias(f"__n_{c}") for c in common])
    return df_old.select(keys + common).join(ln, on=keys, how="inner")


def per_utility_max_abs(
    j: pl.DataFrame, common: list[str], util_col: str = "sb.electric_utility"
) -> pl.DataFrame:
    diff_exprs = [
        (pl.col(c) - pl.col(f"__n_{c}")).abs().alias(f"_d_{c}") for c in common
    ]
    wide = j.select([util_col] + diff_exprs)
    agg_cols = [pl.max(f"_d_{c}").alias(c) for c in common]
    return wide.group_by(util_col).agg(agg_cols).sort(util_col)


def worst_buildings(
    j: pl.DataFrame, col: str, n: int = 5
) -> pl.DataFrame:
    dcol = f"__d_{col}"
    out = j.with_columns((pl.col(col) - pl.col(f"__n_{col}")).abs().alias(dcol))
    return (
        out.sort(dcol, descending=True)
        .head(n)
        .select(
            [
                "bldg_id",
                "sb.electric_utility",
                pl.col(col),
                pl.col(f"__n_{col}"),
                pl.col(dcol).alias("abs_diff"),
            ]
        )
    )


def scan_log_git_commit(pattern: str) -> list[str]:
    lines: list[str] = []
    for path in sorted(glob.glob(pattern))[:12]:
        try:
            with open(path) as f:
                for _ in range(4):
                    line = f.readline().rstrip()
                    if line.startswith("git_commit:") or line.startswith("timestamp:"):
                        lines.append(f"{path}: {line}")
        except OSError:
            continue
    return lines


def _utilities_over_tol(by_u: pl.DataFrame, cols: list[str], tol: float) -> pl.DataFrame:
    use = [c for c in cols if c in by_u.columns]
    if not use:
        return pl.DataFrame()
    return (
        by_u.with_columns(pl.max_horizontal([pl.col(c) for c in use]).alias("_mx"))
        .filter(pl.col("_mx") > tol)
        .sort("_mx", descending=True)
        .drop("_mx")
    )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--new-batch", default="ny_20260401_all_epmc_r1-24")
    p.add_argument("--old-batch", default="ny_20260327_r1-16_epmc")
    p.add_argument("--base", default="s3://data.sb/switchbox/cairo/outputs/hp_rates/ny/all_utilities")
    p.add_argument("--aws-region", default="us-west-2")
    p.add_argument("--log-glob", default="", help="e.g. $HOME/rdp_run_logs/*_run9_*NEW_BATCH*.log")
    p.add_argument(
        "--run-pairs",
        default="run_9+10,run_11+12,run_13+14,run_15+16",
        help="Comma-separated master run pair dirs, e.g. run_11+12,run_13+14",
    )
    args = p.parse_args()

    so = {"aws_region": args.aws_region}
    run_pairs = [x.strip() for x in args.run_pairs.split(",") if x.strip()]

    print("=" * 72)
    print("NY EPMC batch diff diagnostics")
    print(f"  OLD: {args.old_batch}")
    print(f"  NEW: {args.new_batch}")
    print("=" * 72)

    # 1) Log git lines (optional)
    print("\n## A) Git / run timestamps from local logs (if glob provided)")
    if args.log_glob:
        found = scan_log_git_commit(args.log_glob)
        if found:
            for x in found[:20]:
                print(x)
            if len(found) > 20:
                print(f"  ... ({len(found)} lines total)")
        else:
            print("  No files matched --log-glob")
    else:
        default_glob = f"{__import__('os').environ.get('HOME', '')}/rdp_run_logs/or_run9_{args.new_batch}.log"
        if __import__('os').path.isfile(default_glob):
            found = scan_log_git_commit(default_glob)
            for x in found:
                print(x)
        else:
            print("  (Pass --log-glob to scan rdp_run_logs for git_commit / timestamp.)")
            print("  No default or_run9 log found for NEW batch.")

    focus_cols_bat = [
        "BAT_vol_delivery",
        "BAT_vol_total",
        "BAT_vol_supply",
        "BAT_epmc_delivery",
        "BAT_epmc_supply",
        "BAT_epmc_total",
        "annual_bill_delivery",
        "annual_bill_supply",
        "annual_bill_total",
    ]
    focus_cols_bill = ["elec_delivery_bill", "elec_supply_bill", "elec_total_bill", "energy_total_bill"]

    summary_bat: list[dict] = []
    summary_bill: list[dict] = []

    for rp in run_pairs:
        nw = 3 if rp in ("run_13+14", "run_15+16") else 8
        print(f"\n{'#' * 72}\n# {rp}\n{'#' * 72}")

        lo = scan_bat(args.base, args.old_batch, rp, so).collect()
        ln = scan_bat(args.base, args.new_batch, rp, so).collect()
        common = _numeric_shared_cols(lo.schema, ln.schema, BAT_KEYS)
        j = _join_old_new(lo, ln, BAT_KEYS, common)
        by_u = per_utility_max_abs(j, common)

        use_bat = [c for c in focus_cols_bat if c in by_u.columns]
        over = _utilities_over_tol(by_u, use_bat, TOL)
        max_mx = 0.0
        if over.height and use_bat:
            mx_series = over.with_columns(
                pl.max_horizontal([pl.col(c) for c in use_bat]).alias("_m")
            ).select(pl.max("_m"))
            max_mx = float(mx_series.to_series()[0])
        summary_bat.append(
            {
                "run_pair": rp,
                "join_rows": j.height,
                "utilities_over_tol": over.height,
                "max_focus_abs": max_mx,
                "utilities": over.select("sb.electric_utility").to_series().to_list() if over.height else [],
            }
        )

        print(f"\n## B) BAT per-utility max |old-new| — {rp}")
        print(f"  Inner-join rows: {j.height} (old rows: {lo.height}, new rows: {ln.height})")
        show_cols = ["sb.electric_utility"] + [c for c in focus_cols_bat if c in by_u.columns]
        print(by_u.select(show_cols))
        if over.height:
            print("  Utilities with |diff| > 1e-6 on any focus BAT col (subset):")
            print(over.select(show_cols))

        print(f"\n## C) Worst buildings by BAT_vol_delivery — {rp}")
        if "BAT_vol_delivery" in common:
            print(worst_buildings(j, "BAT_vol_delivery", nw))
        else:
            print("  BAT_vol_delivery not in shared cols")

        if rp in ("run_13+14", "run_15+16") and "BAT_vol_supply" in common:
            print(f"\n## C2) Worst buildings by BAT_vol_supply — {rp}")
            print(worst_buildings(j, "BAT_vol_supply", nw))

        print(f"\n## D) Bills per-utility max |old-new| — {rp}")
        bo = scan_bills(args.base, args.old_batch, rp, so).collect()
        bn = scan_bills(args.base, args.new_batch, rp, so).collect()
        c2 = _numeric_shared_cols(bo.schema, bn.schema, BILL_KEYS)
        jb = _join_old_new(bo, bn, BILL_KEYS, c2)
        print(f"  Inner-join rows: {jb.height}")
        by_ub = per_utility_max_abs(jb, c2)
        show_b = ["sb.electric_utility"] + [c for c in focus_cols_bill if c in by_ub.columns]
        print(by_ub.select(show_b))
        over_b = _utilities_over_tol(by_ub, [c for c in focus_cols_bill if c in by_ub.columns], TOL)
        max_b = 0.0
        ub_use = [c for c in focus_cols_bill if c in by_ub.columns]
        if over_b.height and ub_use:
            max_b = float(
                over_b.with_columns(pl.max_horizontal([pl.col(c) for c in ub_use]).alias("_m"))
                .select(pl.max("_m"))
                .to_series()[0]
            )
        summary_bill.append(
            {
                "run_pair": rp,
                "join_rows": jb.height,
                "utilities_over_tol": over_b.height,
                "max_focus_abs": max_b,
                "utilities": over_b.select("sb.electric_utility").to_series().to_list()
                if over_b.height
                else [],
            }
        )
        if over_b.height:
            print("  Utilities with |diff| > 1e-6 on bill focus cols (subset):")
            print(over_b.select(show_b))

        jba: pl.DataFrame | None = None
        if "elec_total_bill" in c2:
            jba = _join_old_new(
                bo.filter(pl.col("month") == "Annual") if "month" in bo.columns else bo,
                bn.filter(pl.col("month") == "Annual") if "month" in bn.columns else bn,
                BILL_KEYS,
                c2,
            )
            print(f"\n## E) Worst rows by elec_total_bill (month=Annual) — {rp}")
            if jba.height:
                print(worst_buildings(jba, "elec_total_bill", nw))

        if (
            rp in ("run_13+14", "run_15+16")
            and jba is not None
            and jba.height
            and "elec_supply_bill" in c2
        ):
            print(f"\n## E2) Worst rows by elec_supply_bill (month=Annual) — {rp}")
            print(worst_buildings(jba, "elec_supply_bill", nw))

    print("\n" + "=" * 72)
    print("SUMMARY: BAT focus cols (max |old-new| within utility, then max across utils)")
    print("=" * 72)
    for row in summary_bat:
        print(
            f"  {row['run_pair']}: join_rows={row['join_rows']}  "
            f"utils_with_diff>{TOL}={row['utilities_over_tol']}  "
            f"max_focus_abs≈{row['max_focus_abs']:.6g}  utilities={row['utilities']}"
        )
    print("\nSUMMARY: Bills focus cols")
    for row in summary_bill:
        print(
            f"  {row['run_pair']}: join_rows={row['join_rows']}  "
            f"utils_with_diff>{TOL}={row['utilities_over_tol']}  "
            f"max_focus_abs≈{row['max_focus_abs']:.6g}  utilities={row['utilities']}"
        )

    print("\n" + "=" * 72)
    print("Done. If one utility dominates, inspect that utility's run outputs and tariffs.")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
