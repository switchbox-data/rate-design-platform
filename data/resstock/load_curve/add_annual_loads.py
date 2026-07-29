"""Aggregate hourly ResStock load curves into annual load curves for the _sb release.

For each building, aggregates selected columns from ``_sb`` hourly load curves
into a single annual row, then joins onto a slim slice of the raw NREL
``load_curve_annual`` file (``bldg_id``, ``upgrade``, ``weight``, ``out.params.*``,
and ``upgrade_name`` when present on upgrades 01–05).

Aggregations from hourly:
  - Sum ``*.energy_consumption`` → rename to ``*.energy_consumption.kwh``
  - Sum ``out.load.*.energy_delivered.kbtu`` (same names in annual; already kBtu)

Dropped from raw annual (not re-derived): ``*.savings``, emissions /
``emissions_reduction``, peaks, bills, unmet hours, hot-water gallons,
energy burden, and other annual-only metrics.

Usage (from project root):
    uv run python data/resstock/load_curve/add_annual_loads.py \\
        --path-hourly /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \\
        --path-annual-raw /ebs/data/nrel/resstock/res_2024_amy2018_2 \\
        --path-output /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \\
        --state CT --upgrade-ids "00 02" --workers 256
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import cast

import polars as pl

# Hourly energy columns look like:
#   out.electricity.heating.energy_consumption
# Annual equivalents:
#   out.electricity.heating.energy_consumption.kwh
_HOURLY_ENERGY_SUFFIX = ".energy_consumption"
_INTENSITY_SUFFIX = ".energy_consumption_intensity"

LOAD_HEATING = "out.load.heating.energy_delivered.kbtu"
LOAD_COOLING = "out.load.cooling.energy_delivered.kbtu"
LOAD_HOT_WATER = "out.load.hot_water.energy_delivered.kbtu"

LOAD_ENERGY_DELIVERED_COLS: tuple[str, ...] = (
    LOAD_HEATING,
    LOAD_COOLING,
    LOAD_HOT_WATER,
)


def hourly_energy_col_to_annual(hourly_col: str) -> str | None:
    """Map an hourly energy column name to its annual ``.kwh`` counterpart.

    Returns ``None`` if *hourly_col* is not a plain energy-consumption column
    (e.g. intensity columns are excluded).
    """
    if hourly_col.endswith(_INTENSITY_SUFFIX):
        return None
    if not hourly_col.endswith(_HOURLY_ENERGY_SUFFIX):
        return None
    return hourly_col + ".kwh"


def energy_consumption_columns(schema_names: list[str]) -> list[str]:
    """Return hourly schema columns that are energy consumption (not intensity)."""
    return [c for c in schema_names if hourly_energy_col_to_annual(c) is not None]


def aggregate_hourly_energy_to_annual(hourly_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Aggregate one building's hourly load curve to a single annual row.

    Args:
        hourly_lf: LazyFrame for a single ``bldg_id`` (typically 8760 rows)
            with hourly ResStock load-curve columns.

    Returns:
        LazyFrame with one row: ``bldg_id``, annual-named energy totals, and
        summed load-delivered columns. Emissions and peaks are not included.
    """
    schema_names = hourly_lf.collect_schema().names()
    if "bldg_id" not in schema_names:
        raise ValueError("hourly LazyFrame must contain 'bldg_id'")

    energy_cols = energy_consumption_columns(schema_names)
    if not energy_cols:
        raise ValueError(
            "hourly LazyFrame has no '*.energy_consumption' columns to aggregate"
        )

    agg_exprs: list[pl.Expr] = [pl.col("bldg_id").first().alias("bldg_id")]

    for hourly_col in energy_cols:
        annual_col = hourly_energy_col_to_annual(hourly_col)
        assert annual_col is not None  # filtered above
        agg_exprs.append(pl.col(hourly_col).sum().alias(annual_col))

    # Load delivered: same column names (already kBtu) in hourly and annual.
    # Heating/cooling are rewritten by non-HP approximation on upgrade 02;
    # hot_water is not. No kWh→kBtu conversion — both schemas label these .kbtu.
    for load_col in LOAD_ENERGY_DELIVERED_COLS:
        if load_col in schema_names:
            agg_exprs.append(pl.col(load_col).sum().alias(load_col))

    return hourly_lf.select(agg_exprs)


def select_annual_params_weight_upgrade(annual_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Keep identity/params columns from raw annual; drop metrics rebuilt from hourly.

    Always keeps ``bldg_id``, ``upgrade``, ``weight``, and all ``out.params.*``.
    Also keeps ``upgrade_name`` when present (NREL upgrades 01–05 only).

    Drops absolute and ``*.savings`` energy/load/bill/peak/hot-water/unmet-hour/
    energy-burden columns, plus ``out.emissions*`` / ``out.emissions_reduction.*``,
    so the joined result uses freshly aggregated values from ``_sb`` hourly.
    """
    schema_names = annual_lf.collect_schema().names()
    required = ("bldg_id", "upgrade", "weight")
    missing = [c for c in required if c not in schema_names]
    if missing:
        raise ValueError(
            f"load_curve_annual missing required columns {missing}; "
            f"found: {schema_names[:20]}..."
        )

    keep: list[str] = ["bldg_id", "upgrade", "weight"]
    if "upgrade_name" in schema_names:
        keep.append("upgrade_name")
    keep.extend(c for c in schema_names if c.startswith("out.params."))
    return annual_lf.select(keep)


def join_aggregated_energy_to_annual(
    aggregated_energy_lf: pl.LazyFrame,
    annual_params_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Left-join aggregated ``_sb`` metrics onto slim raw annual params/weight/upgrade.

    *annual_params_lf* should already be filtered via
    :func:`select_annual_params_weight_upgrade`. Buildings present in the
    aggregated energy side but missing from annual params keep energy totals
    with null params/weight/upgrade (and null ``upgrade_name`` when applicable).
    """
    return aggregated_energy_lf.join(annual_params_lf, on="bldg_id", how="left")


def aggregate_one_building(input_path: Path) -> pl.DataFrame:
    """Read one hourly parquet; return a 1-row DataFrame of annual aggregates."""
    return cast(
        pl.DataFrame,
        aggregate_hourly_energy_to_annual(pl.scan_parquet(input_path)).collect(),
    )


def process_upgrade(
    path_hourly: Path,
    path_annual_raw: Path,
    path_output: Path,
    state: str,
    upgrade: str,
    workers: int,
) -> None:
    """Aggregate all buildings for one state/upgrade; write consolidated annual parquet."""
    hourly_dir = path_hourly / f"load_curve_hourly/state={state}/upgrade={upgrade}"
    annual_raw_dir = (
        path_annual_raw / f"load_curve_annual/state={state}/upgrade={upgrade}"
    )
    output_dir = path_output / f"load_curve_annual/state={state}/upgrade={upgrade}"

    if not hourly_dir.exists():
        print(f"  Input directory does not exist, skipping: {hourly_dir}")
        return
    if not annual_raw_dir.exists():
        print(f"  Raw annual directory does not exist, skipping: {annual_raw_dir}")
        return

    files = sorted(hourly_dir.glob("*.parquet"))
    n_files = len(files)
    if n_files == 0:
        print(f"  No parquet files found in {hourly_dir}")
        return

    annual_raw_files = sorted(annual_raw_dir.glob("*.parquet"))
    if not annual_raw_files:
        print(f"  No annual parquet files in {annual_raw_dir}")
        return

    annual_params_lf = select_annual_params_weight_upgrade(
        pl.scan_parquet(str(annual_raw_dir))
    )

    print(f"  Found {n_files:,} hourly files in {hourly_dir}")
    print(f"  Raw annual params from: {annual_raw_dir}")
    print(f"  Output: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    frames: list[pl.DataFrame] = []
    done = 0
    errors = 0

    def _process(src: Path) -> pl.DataFrame | str:
        try:
            return aggregate_one_building(src)
        except Exception as e:
            return f"{src.name}: {e}"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process, f): f for f in files}
        for future in as_completed(futures):
            result = future.result()
            done += 1
            if isinstance(result, str):
                errors += 1
                print(f"  ERROR {result}")
            else:
                frames.append(result)
            if done % 5000 == 0 or done == n_files:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                print(
                    f"  {done:,}/{n_files:,} ({rate:.0f} files/s, {elapsed:.1f}s elapsed)"
                )

    if not frames:
        print(f"  No successful aggregations for upgrade={upgrade}; skipping write")
        return

    aggregated = cast(pl.DataFrame, pl.concat(frames, how="vertical_relaxed"))
    joined = cast(
        pl.DataFrame,
        join_aggregated_energy_to_annual(aggregated.lazy(), annual_params_lf).collect(),
    )

    if len(annual_raw_files) == 1:
        out_name = annual_raw_files[0].name
    else:
        out_name = f"{state}_upgrade{upgrade}_metadata_and_annual_results.parquet"

    out_path = output_dir / out_name
    joined.write_parquet(out_path)

    elapsed = time.time() - t0
    print(
        f"  Done: wrote {joined.height:,} rows × {joined.width} cols to {out_path} "
        f"({errors} errors, {elapsed:.1f}s)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate _sb hourly load curves to annual energy/load totals and join "
            "onto raw annual params/weight/upgrade."
        )
    )
    parser.add_argument(
        "--path-hourly",
        required=True,
        help=(
            "Root of the _sb ResStock release with load_curve_hourly "
            "(e.g. .../res_2024_amy2018_2_sb)"
        ),
    )
    parser.add_argument(
        "--path-annual-raw",
        required=True,
        help=(
            "Root of the raw NREL release with load_curve_annual "
            "(e.g. .../res_2024_amy2018_2)"
        ),
    )
    parser.add_argument(
        "--path-output",
        required=True,
        help=(
            "Root to write _sb load_curve_annual into "
            "(typically the same as --path-hourly)"
        ),
    )
    parser.add_argument("--state", required=True, help="Two-letter state code, e.g. CT")
    parser.add_argument(
        "--upgrade-ids",
        required=True,
        help='Space-separated upgrade IDs, e.g. "00 02"',
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=256,
        help="Number of parallel workers (default: 256)",
    )
    args = parser.parse_args()

    path_hourly = Path(args.path_hourly)
    path_annual_raw = Path(args.path_annual_raw)
    path_output = Path(args.path_output)
    upgrade_ids = args.upgrade_ids.split()

    for path, label in (
        (path_hourly, "--path-hourly"),
        (path_annual_raw, "--path-annual-raw"),
    ):
        if not path.exists():
            print(f"Error: {label} does not exist: {path}", file=sys.stderr)
            sys.exit(1)

    for upgrade in upgrade_ids:
        print(f"\n{'=' * 60}")
        print(f"Processing state={args.state}, upgrade={upgrade}")
        print(f"{'=' * 60}")
        process_upgrade(
            path_hourly=path_hourly,
            path_annual_raw=path_annual_raw,
            path_output=path_output,
            state=args.state,
            upgrade=upgrade,
            workers=args.workers,
        )

    print("\nAll done.")


if __name__ == "__main__":
    main()
