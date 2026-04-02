#!/usr/bin/env python3
"""Verify NY master BAT/bills between two all_utilities batches (EPMC guideline checks)."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable

import polars as pl

NUMERIC_TYPES = (
    pl.Float32,
    pl.Float64,
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
)

RUN_PAIRS = [
    "run_1+2",
    "run_3+4",
    "run_5+6",
    "run_7+8",
    "run_9+10",
    "run_11+12",
    "run_13+14",
    "run_15+16",
]

PAIRS_1_12 = RUN_PAIRS[:6]
PAIRS_13_16 = RUN_PAIRS[6:]

BAT_KEYS = ["bldg_id", "sb.electric_utility"]
BILL_KEYS = ["bldg_id", "sb.electric_utility", "month"]

TOL = 1e-6


def _is_numeric(dt: pl.DataType) -> bool:
    return isinstance(dt, NUMERIC_TYPES)


def _numeric_shared_cols(
    s_old: pl.Schema, s_new: pl.Schema, exclude: Iterable[str]
) -> list[str]:
    ex = set(exclude)
    out: list[str] = []
    for name, dt in s_old.items():
        if name in ex or name not in s_new:
            continue
        if _is_numeric(dt) and _is_numeric(s_new[name]):
            out.append(name)
    return sorted(out)


def scan_bat(base: str, batch: str, run_pair: str, storage_options: dict) -> pl.LazyFrame:
    path = f"{base}/{batch}/{run_pair}/cross_subsidization_BAT_values/"
    return pl.scan_parquet(path, hive_partitioning=True, storage_options=storage_options)


def scan_bills(base: str, batch: str, run_pair: str, storage_options: dict) -> pl.LazyFrame:
    path = f"{base}/{batch}/{run_pair}/comb_bills_year_target/"
    return pl.scan_parquet(path, hive_partitioning=True, storage_options=storage_options)


def check1_new_batch_schema(
    base: str, new_batch: str, storage_options: dict
) -> tuple[bool, list[str]]:
    """Check 1 on NEW batch only."""
    messages: list[str] = []
    ok = True
    rp = RUN_PAIRS[0]
    bat = scan_bat(base, new_batch, rp, storage_options)
    bills = scan_bills(base, new_batch, rp, storage_options)
    bat_cols = set(bat.collect_schema().names())
    bill_cols = set(bills.collect_schema().names())

    need_bat = {"BAT_epmc_delivery", "residual_share_epmc_delivery"}
    forbid_bat = {"BAT_peak_delivery"}
    for c in need_bat:
        if c not in bat_cols:
            messages.append(f"  MISSING on BAT ({rp}): {c}")
            ok = False
    for c in forbid_bat:
        if c in bat_cols:
            messages.append(f"  UNEXPECTED on BAT ({rp}, should be absent): {c}")
            ok = False

    lmi_ok = any(
        x in bill_cols
        for x in (
            "elec_total_bill_lmi_100",
            "elec_total_bill_lmi_32",
            "elec_total_bill_lmi_40",
        )
    )
    if not lmi_ok:
        messages.append(
            f"  No expected LMI elec bill cols on bills ({rp}); have similar: "
            + ", ".join(sorted(c for c in bill_cols if "lmi" in c.lower()))[:200]
        )
        ok = False

    if ok:
        messages.append(f"  BAT ({rp}): BAT_epmc_delivery, residual_share_epmc_delivery present; BAT_peak_delivery absent")
        messages.append(f"  Bills ({rp}): LMI-related columns present")
    return ok, messages


def compare_numeric_max_abs_diff(
    lf_old: pl.LazyFrame,
    lf_new: pl.LazyFrame,
    keys: list[str],
    label: str,
) -> tuple[bool, list[str]]:
    """Join on keys; for each shared numeric column (excl keys), max |old - new|."""
    s_old = lf_old.collect_schema()
    s_new = lf_new.collect_schema()
    common = _numeric_shared_cols(s_old, s_new, keys)
    if not common:
        return False, [f"  {label}: no shared numeric columns to compare"]

    # Rename new side for join
    rename_new = {c: f"__new_{c}" for c in common}
    lf_n = lf_new.select(keys + [pl.col(c).alias(rename_new[c]) for c in common])

    j = lf_old.select(keys + common).join(lf_n, on=keys, how="inner")

    exprs = [
        (pl.col(c) - pl.col(f"__new_{c}")).abs().max().alias(f"maxdiff_{c}") for c in common
    ]
    row = j.select(exprs).collect().row(0)
    bad: list[str] = []
    for c, mx in zip(common, row, strict=True):
        if mx is None:
            continue
        if float(mx) > TOL:
            bad.append(f"    {c}: max_abs_diff={mx:.6g}")

    msgs: list[str] = []
    if bad:
        msgs.append(f"  {label}: FAIL (> {TOL}):")
        msgs.extend(bad)
        return False, msgs
    msgs.append(f"  {label}: OK (all {len(common)} shared numeric cols max |diff| <= {TOL})")
    return True, msgs


def check5_building_counts(
    base: str, batch: str, storage_options: dict, label: str
) -> tuple[bool, list[str]]:
    """Per utility, distinct bldg_id count must match across all 8 run pairs (BAT)."""
    msgs: list[str] = []
    counts_by_pair: dict[str, pl.DataFrame] = {}
    for rp in RUN_PAIRS:
        df = (
            scan_bat(base, batch, rp, storage_options)
            .group_by("sb.electric_utility")
            .agg(pl.col("bldg_id").n_unique().alias("n_bldg"))
            .collect()
        )
        counts_by_pair[rp] = df.sort("sb.electric_utility")

    ref = counts_by_pair[RUN_PAIRS[0]]
    ok = True
    for rp in RUN_PAIRS[1:]:
        cmp_df = counts_by_pair[rp]
        if not ref.equals(cmp_df):
            ok = False
            msgs.append(f"  {label} {rp} vs {RUN_PAIRS[0]}: mismatch")
            # detail
            m = ref.join(cmp_df, on="sb.electric_utility", suffix="_other", how="outer")
            msgs.append(f"    joined:\n{m}")
    if ok:
        msgs.append(f"  {label}: OK — same per-utility bldg counts across all {len(RUN_PAIRS)} BAT run pairs")
    return ok, msgs


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--new-batch", required=True)
    p.add_argument("--old-batch", required=True)
    p.add_argument("--base", default="s3://data.sb/switchbox/cairo/outputs/hp_rates/ny/all_utilities")
    p.add_argument("--aws-region", default="us-west-2")
    args = p.parse_args()

    storage_options = {"aws_region": args.aws_region}

    print("=" * 72)
    print("NY master batch verification (EPMC guideline)")
    print(f"  NEW: {args.new_batch}")
    print(f"  OLD: {args.old_batch}")
    print(f"  BASE: {args.base}")
    print("=" * 72)

    results: dict[str, bool] = {}

    # Check 1
    print("\n## Check 1: New batch columns (EPMC present, peak absent, LMI bills)")
    ok1, lines = check1_new_batch_schema(args.base, args.new_batch, storage_options)
    results["check1"] = ok1
    for line in lines:
        print(line)
    if not ok1:
        print("Check 1: FAILED")
    else:
        print("Check 1: SUCCESS")

    # Check 2 — BAT pairs 1–12
    print("\n## Check 2: BAT shared numeric vs old batch (run pairs 1+2 … 11+12)")
    ok2_all = True
    for rp in PAIRS_1_12:
        lo = scan_bat(args.base, args.old_batch, rp, storage_options)
        ln = scan_bat(args.base, args.new_batch, rp, storage_options)
        ok, lines = compare_numeric_max_abs_diff(lo, ln, BAT_KEYS, f"BAT {rp}")
        results[f"check2_{rp}"] = ok
        ok2_all = ok2_all and ok
        for line in lines:
            print(line)
    results["check2"] = ok2_all
    print(f"Check 2 (aggregate): {'SUCCESS' if ok2_all else 'FAILED'}")

    # Check 3 — Bills pairs 1–12
    print("\n## Check 3: Master bills shared numeric vs old batch (run pairs 1+2 … 11+12)")
    ok3_all = True
    for rp in PAIRS_1_12:
        lo = scan_bills(args.base, args.old_batch, rp, storage_options)
        ln = scan_bills(args.base, args.new_batch, rp, storage_options)
        ok, lines = compare_numeric_max_abs_diff(lo, ln, BILL_KEYS, f"Bills {rp}")
        results[f"check3_{rp}"] = ok
        ok3_all = ok3_all and ok
        for line in lines:
            print(line)
    results["check3"] = ok3_all
    print(f"Check 3 (aggregate): {'SUCCESS' if ok3_all else 'FAILED'}")

    # Check 4 — informational (13–16 may differ)
    print("\n## Check 4: BAT/bills pairs 13+14 and 15+16 (diffs reported, not failures)")
    ok4 = True
    results["check4"] = True
    for rp in PAIRS_13_16:
        for name, scanner, keys in (
            ("BAT", scan_bat, BAT_KEYS),
            ("Bills", scan_bills, BILL_KEYS),
        ):
            lo = scanner(args.base, args.old_batch, rp, storage_options)
            ln = scanner(args.base, args.new_batch, rp, storage_options)
            ok, lines = compare_numeric_max_abs_diff(lo, ln, keys, f"{name} {rp}")
            # Report max diffs even if "ok" — print first line summary + any bad lines
            for line in lines:
                print(line)
    print("Check 4: INFORMATIONAL (expected to differ) — not scored PASS/FAIL")

    # Check 5 — both batches
    print("\n## Check 5: Building counts consistent across run pairs (per batch)")
    ok5a, la = check5_building_counts(args.base, args.old_batch, storage_options, "OLD")
    ok5b, lb = check5_building_counts(args.base, args.new_batch, storage_options, "NEW")
    for line in la:
        print(line)
    for line in lb:
        print(line)
    results["check5"] = ok5a and ok5b
    print(f"Check 5: {'SUCCESS' if results['check5'] else 'FAILED'}")

    # Summary
    print("\n" + "=" * 72)
    print("SUMMARY (scored checks)")
    print("=" * 72)
    for k in ("check1", "check2", "check3", "check5"):
        status = "SUCCESS" if results[k] else "FAILED"
        print(f"  {k}: {status}")
    print("\nCheck 4 was informational only (run pairs 13–16).")
    any_fail = not all(results[k] for k in ("check1", "check2", "check3", "check5"))
    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main())
