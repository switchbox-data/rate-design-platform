"""Aggregate sub-TX and distribution BAT inputs from per-utility levelized CSVs.

Reads each utility's incremental diluted levelized CSV and extracts the
sub_tx_and_dist value into a single summary CSV — one row per utility.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

UTILITIES = ["cenhud", "coned", "nimo", "nyseg", "or", "psegli", "rge"]
BUCKET = "sub_tx_and_dist"
VALUE_COL = "levelized_mc_kw_yr"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path-output",
        type=Path,
        required=True,
        help="Path to write the summary CSV",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    rows: list[dict[str, object]] = []

    for utility in UTILITIES:
        csv_path = (
            script_dir
            / utility
            / "outputs"
            / f"{utility}_incremental_diluted_levelized.csv"
        )
        df = pl.read_csv(csv_path)
        value = df.filter(pl.col("bucket") == BUCKET)[VALUE_COL].item()
        rows.append({"utility": utility, "sub_tx_and_dist_mc_kw_yr": round(value, 2)})

    out = pl.DataFrame(rows)
    args.path_output.parent.mkdir(parents=True, exist_ok=True)
    out.write_csv(args.path_output)

    print(f"Wrote {len(rows)} utilities to {args.path_output}")
    print(out)


if __name__ == "__main__":
    main()
