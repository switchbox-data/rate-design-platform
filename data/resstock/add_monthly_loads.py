"""Aggregate hourly ResStock load curves into monthly load curves.

Reads per-building hourly parquet files, aggregates to monthly using the same
column-level rules as buildstock-fetch (sum for energy/emissions, mean for
load/temperature), and writes one monthly parquet per building.

Usage (from project root):
    uv run python data/resstock/add_monthly_loads.py \
        --path-input /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \
        --path-output /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \
        --state NY --upgrade-ids "00 02" \
        --bsf-release res_2024_amy2018_2 --workers 50
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import cast

import polars as pl

from buildstock_fetch.constants import LOAD_CURVE_COLUMN_AGGREGATION


def load_aggregation_rules(release: str) -> list[pl.Expr]:
    """Build polars aggregation expressions from bsf's column map CSV."""
    csv_path = LOAD_CURVE_COLUMN_AGGREGATION / f"{release}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"bsf column map not found: {csv_path}. "
            f"Available: {[p.stem for p in LOAD_CURVE_COLUMN_AGGREGATION.glob('*.csv')]}"
        )
    rules_df = pl.read_csv(csv_path)
    rules = dict(
        zip(
            rules_df["name"].to_list(),
            rules_df["Aggregate_function"].to_list(),
            strict=True,
        )
    )

    exprs: list[pl.Expr] = []
    for col, agg in rules.items():
        if col == "timestamp":
            continue
        match agg:
            case "sum":
                exprs.append(pl.col(col).sum())
            case "mean":
                exprs.append(pl.col(col).mean())
            case "first":
                exprs.append(pl.col(col).first())
            case _:
                raise ValueError(
                    f"Unknown aggregation function '{agg}' for column '{col}'"
                )
    return exprs


def aggregate_one_building(
    input_path: Path,
    output_path: Path,
    agg_rules: list[pl.Expr],
) -> None:
    """Read one hourly parquet, aggregate to monthly, write output."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # bsf's column map covers out.* and bldg_id but not year/day/hour/timestamp
    # (those are added by bsf after aggregation). We add year.first() here and
    # reconstruct timestamp from the result.
    all_rules = [*agg_rules, pl.col("year").first()]

    df = cast(
        pl.DataFrame,
        pl.scan_parquet(input_path)
        .group_by("month")
        .agg(all_rules)
        .sort("month")
        .collect(),
    )

    year = df["year"][0]

    df = df.with_columns(
        pl.datetime(year=year, month=pl.col("month"), day=1).alias("timestamp"),
        pl.col("year").cast(pl.Int32),
        pl.col("month").cast(pl.Int8),
    )

    out_cols = [c for c in df.columns if c.startswith("out.")]
    col_order = ["timestamp", *out_cols, "bldg_id", "year", "month"]
    df = df.select(col_order)

    df.write_parquet(output_path)


def process_upgrade(
    path_input: Path,
    path_output: Path,
    state: str,
    upgrade: str,
    agg_rules: list[pl.Expr],
    workers: int,
) -> None:
    """Process all building files for one upgrade."""
    input_dir = path_input / f"load_curve_hourly/state={state}/upgrade={upgrade}"
    output_dir = path_output / f"load_curve_monthly/state={state}/upgrade={upgrade}"

    if not input_dir.exists():
        print(f"  Input directory does not exist, skipping: {input_dir}")
        return

    files = sorted(input_dir.glob("*.parquet"))
    n_files = len(files)
    if n_files == 0:
        print(f"  No parquet files found in {input_dir}")
        return

    print(f"  Found {n_files:,} hourly files in {input_dir}")
    print(f"  Output: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    done = 0
    errors = 0

    def _process(src: Path) -> str | None:
        dst = output_dir / src.name
        try:
            aggregate_one_building(src, dst, agg_rules)
            return None
        except Exception as e:
            return f"{src.name}: {e}"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process, f): f for f in files}
        for future in as_completed(futures):
            result = future.result()
            done += 1
            if result is not None:
                errors += 1
                print(f"  ERROR {result}")
            if done % 5000 == 0 or done == n_files:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                print(
                    f"  {done:,}/{n_files:,} ({rate:.0f} files/s, {elapsed:.1f}s elapsed)"
                )

    elapsed = time.time() - t0
    print(f"  Done: {done:,} files in {elapsed:.1f}s ({errors} errors)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate hourly ResStock load curves to monthly."
    )
    parser.add_argument(
        "--path-input",
        required=True,
        help="Root of the ResStock release (local), e.g. /ebs/data/nrel/resstock/res_2024_amy2018_2_sb",
    )
    parser.add_argument(
        "--path-output",
        required=True,
        help="Root to write monthly load curves into (local), e.g. /ebs/data/nrel/resstock/res_2024_amy2018_2_sb",
    )
    parser.add_argument("--state", required=True, help="Two-letter state code, e.g. NY")
    parser.add_argument(
        "--upgrade-ids",
        required=True,
        help='Space-separated upgrade IDs, e.g. "00 02"',
    )
    parser.add_argument(
        "--bsf-release",
        required=True,
        help="bsf release key for column aggregation rules, e.g. res_2024_amy2018_2",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=50,
        help="Number of parallel workers (default: 50)",
    )
    args = parser.parse_args()

    path_input = Path(args.path_input)
    path_output = Path(args.path_output)
    upgrade_ids = args.upgrade_ids.split()

    if not path_input.exists():
        print(f"Error: input path does not exist: {path_input}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading bsf aggregation rules for release '{args.bsf_release}'...")
    agg_rules = load_aggregation_rules(args.bsf_release)
    print(f"  {len(agg_rules)} column rules loaded")

    for upgrade in upgrade_ids:
        print(f"\n{'=' * 60}")
        print(f"Processing state={args.state}, upgrade={upgrade}")
        print(f"{'=' * 60}")
        process_upgrade(
            path_input, path_output, args.state, upgrade, agg_rules, args.workers
        )

    print("\nAll done.")


if __name__ == "__main__":
    main()
