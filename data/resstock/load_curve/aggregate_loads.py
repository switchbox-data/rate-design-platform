"""Aggregate hourly ResStock load curves into coarser time resolutions.

Reads each per-building hourly parquet **once** in a ThreadPoolExecutor, then
conditionally performs one or both of:

- **Monthly**: group by month using the full bsf column-level rules (sum for
  energy / emissions / energy_delivered, mean for temperatures); writes one
  monthly parquet per building.
- **Annual**: sum only the ``*.energy_consumption`` (not intensity) and
  ``out.load.*.energy_delivered.kbtu`` columns into a single row per building;
  after all buildings finish, concatenate and join onto slim raw-NREL annual
  params (``bldg_id``, ``upgrade``, ``weight``, ``out.params.*``,
  ``upgrade_name``); write one consolidated annual parquet per upgrade.

Which aggregations are performed is controlled by the ``add_monthly`` and
``add_annual`` flags in :func:`process_upgrade`. The expensive hourly read
happens only once regardless of which outputs are enabled.

Annual column selection from hourly (bsf aggregation rules, subset only):
  - ``*.energy_consumption`` (not intensity) -> sum -> rename to ``.kwh``
  - ``out.load.*.energy_delivered.kbtu`` -> sum (same names)

From raw NREL annual: ``bldg_id``, ``upgrade``, ``weight``, ``out.params.*``,
and ``upgrade_name`` when present. All ``*.savings``, emissions reductions,
peaks, bills, etc. are dropped.

Usage (from project root)::

    uv run python data/resstock/load_curve/aggregate_loads.py \\
        --path-input /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \\
        --path-output /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \\
        --path-annual-raw /ebs/data/nrel/resstock/res_2024_amy2018_2 \\
        --state NY --upgrade-ids "00 02" \\
        --bsf-release res_2024_amy2018_2 --workers 50 \\
        --add-monthly --add-annual
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Mapping, Sequence
from pathlib import Path

import polars as pl
from buildstock_fetch.constants import LOAD_CURVE_COLUMN_AGGREGATION

# ── Constants ────────────────────────────────────────────────────────────────

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

# ── BSF rule loading ─────────────────────────────────────────────────────────


def load_bsf_aggregation_map(release: str) -> dict[str, str]:
    """Load bsf column -> Aggregate_function map for a release key.

    Args:
        release: e.g. ``res_2024_amy2018_2`` (matches CSV stem under
            ``buildstock_fetch/data/load_curve_column_map/``).
    """
    csv_path = LOAD_CURVE_COLUMN_AGGREGATION / f"{release}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"bsf column map not found: {csv_path}. "
            f"Available: {[p.stem for p in LOAD_CURVE_COLUMN_AGGREGATION.glob('*.csv')]}"
        )
    rules_df = pl.read_csv(csv_path)
    return dict(
        zip(
            rules_df["name"].to_list(),
            rules_df["Aggregate_function"].to_list(),
            strict=True,
        )
    )


# ── Annual helpers ───────────────────────────────────────────────────────────


def is_annual_metric_column(name: str) -> bool:
    """True if this hourly column should be rolled into ``_sb`` annual.

    Includes plain ``*.energy_consumption`` and any ``energy_delivered`` column;
    excludes intensities and everything else (temps, emissions, etc.).
    """
    if name.endswith(_INTENSITY_SUFFIX):
        return False
    if name.endswith(_HOURLY_ENERGY_SUFFIX):
        return True
    return "energy_delivered" in name


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


def annual_column_name(hourly_col: str) -> str:
    """Annual output name for a metric column (``.kwh`` suffix for energy)."""
    renamed = hourly_energy_col_to_annual(hourly_col)
    return renamed if renamed is not None else hourly_col


def build_annual_agg_exprs(
    rules: Mapping[str, str],
    schema_names: Sequence[str],
) -> list[pl.Expr]:
    """Build annual aggregation exprs from a subset of bsf rules.

    Only columns in both *rules* and *schema_names* that pass
    :func:`is_annual_metric_column` are included. Aggregation function comes
    from the CSV (``sum`` for energy and delivered under bsf >= 1.6.6).
    """
    schema_set = set(schema_names)
    if "bldg_id" not in schema_set:
        raise ValueError("hourly schema must contain 'bldg_id'")

    exprs: list[pl.Expr] = [pl.col("bldg_id").first().alias("bldg_id")]
    n_metrics = 0
    for col, agg in rules.items():
        if col not in schema_set or not is_annual_metric_column(col):
            continue
        out_name = annual_column_name(col)
        match agg:
            case "sum":
                exprs.append(pl.col(col).sum().alias(out_name))
            case "mean":
                exprs.append(pl.col(col).mean().alias(out_name))
            case "first":
                exprs.append(pl.col(col).first().alias(out_name))
            case _:
                raise ValueError(
                    f"Unknown aggregation function '{agg}' for column '{col}'"
                )
        n_metrics += 1

    if n_metrics == 0:
        raise ValueError(
            "no annual metric columns found in hourly schema "
            "(expected *.energy_consumption and/or energy_delivered)"
        )
    return exprs


def aggregate_hourly_df_to_annual_row(
    hourly_df: pl.DataFrame,
    rules: Mapping[str, str],
) -> pl.DataFrame:
    """Aggregate an in-memory hourly building DataFrame to one annual row."""
    exprs = build_annual_agg_exprs(rules, hourly_df.columns)
    return hourly_df.select(exprs)


def select_annual_params_weight_upgrade(annual_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Keep identity/params columns from raw annual; drop metrics rebuilt from hourly.

    Always keeps ``bldg_id``, ``upgrade``, ``weight``, and all ``out.params.*``.
    Also keeps ``upgrade_name`` when present (NREL upgrades 01-05 only).
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
    """Left-join aggregated ``_sb`` metrics onto slim raw annual params/weight/upgrade."""
    return aggregated_energy_lf.join(annual_params_lf, on="bldg_id", how="left")


def write_consolidated_annual(
    annual_rows: list[pl.DataFrame],
    path_annual_raw: Path,
    path_output: Path,
    state: str,
    upgrade: str,
) -> Path | None:
    """Concat annual rows, join raw params, write one ResStock-style annual parquet.

    Returns the output path, or ``None`` if there is nothing to write.
    """
    if not annual_rows:
        return None

    annual_raw_dir = (
        path_annual_raw / f"load_curve_annual/state={state}/upgrade={upgrade}"
    )
    if not annual_raw_dir.exists():
        raise FileNotFoundError(
            f"Raw annual directory does not exist: {annual_raw_dir}"
        )
    annual_raw_files = sorted(annual_raw_dir.glob("*.parquet"))
    if not annual_raw_files:
        raise FileNotFoundError(f"No annual parquet files in {annual_raw_dir}")

    output_dir = path_output / f"load_curve_annual/state={state}/upgrade={upgrade}"
    output_dir.mkdir(parents=True, exist_ok=True)

    aggregated = pl.concat(annual_rows, how="vertical_relaxed")
    annual_params_lf = select_annual_params_weight_upgrade(
        pl.scan_parquet(str(annual_raw_dir))
    )
    joined = join_aggregated_energy_to_annual(
        aggregated.lazy(), annual_params_lf
    ).collect()

    if len(annual_raw_files) == 1:
        out_name = annual_raw_files[0].name
    else:
        out_name = f"{state}_upgrade{upgrade}_metadata_and_annual_results.parquet"

    out_path = output_dir / out_name
    joined.write_parquet(out_path)
    return out_path


# ── Monthly helpers ──────────────────────────────────────────────────────────


def monthly_aggregation_exprs(rules: Mapping[str, str]) -> list[pl.Expr]:
    """Build polars monthly aggregation expressions from a bsf rule map."""
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


def _write_monthly_from_hourly_df(
    hourly_df: pl.DataFrame,
    output_path: Path,
    monthly_exprs: Sequence[pl.Expr],
) -> None:
    """Aggregate an in-memory hourly DataFrame to monthly and write parquet."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # bsf's column map covers out.* and bldg_id but not year/day/hour/timestamp
    # (those are added by bsf after aggregation). We add year.first() here and
    # reconstruct timestamp from the result.
    all_rules = [*monthly_exprs, pl.col("year").first()]

    df = hourly_df.group_by("month").agg(all_rules).sort("month")

    year = df["year"][0]

    df = df.with_columns(
        pl.datetime(year=year, month=pl.col("month"), day=1).alias("timestamp"),
        pl.col("year").cast(pl.Int32),
        pl.col("month").cast(pl.Int8),
    )

    out_cols = [c for c in df.columns if c.startswith("out.")]
    col_order = ["timestamp", *out_cols, "bldg_id", "year", "month"]
    df.select(col_order).write_parquet(output_path)


# ── Combined processing ──────────────────────────────────────────────────────


def aggregate_one_building(
    input_path: Path,
    *,
    add_monthly: bool,
    monthly_output_path: Path | None,
    monthly_exprs: Sequence[pl.Expr] | None,
    add_annual: bool,
    annual_rules: Mapping[str, str] | None,
) -> pl.DataFrame | None:
    """Read one hourly parquet once; conditionally write monthly and/or return annual row."""
    hourly_df = pl.read_parquet(input_path)
    if add_monthly:
        assert monthly_output_path is not None
        assert monthly_exprs is not None
        _write_monthly_from_hourly_df(hourly_df, monthly_output_path, monthly_exprs)
    if add_annual:
        assert annual_rules is not None
        return aggregate_hourly_df_to_annual_row(hourly_df, annual_rules)
    return None


def process_upgrade(
    path_input: Path,
    path_output: Path,
    state: str,
    upgrade: str,
    rules: Mapping[str, str],
    workers: int,
    *,
    add_monthly: bool = True,
    add_annual: bool = False,
    path_annual_raw: Path | None = None,
) -> None:
    """Process all building files for one upgrade.

    Reads each hourly parquet once, then conditionally:
    - Writes monthly aggregation (when *add_monthly* is True)
    - Accumulates annual rows and writes a consolidated annual parquet
      (when *add_annual* is True; requires *path_annual_raw*)

    Raises rather than silently skipping or writing partial output whenever
    that could leave a caller's ``aws s3 sync`` uploading missing, stale, or
    incomplete monthly/annual data: missing/empty *input_dir*, any per-building
    processing failure, or (when *add_annual*) zero successfully aggregated
    annual rows. Callers that want to legitimately skip a (state, upgrade)
    with no hourly data (e.g. an upgrade not applicable to a state) must check
    for that themselves before calling, as ``main.py`` does.
    """
    if not add_monthly and not add_annual:
        raise ValueError("At least one of add_monthly or add_annual must be True")
    if add_annual and path_annual_raw is None:
        raise ValueError("path_annual_raw is required when add_annual=True")

    input_dir = path_input / f"load_curve_hourly/state={state}/upgrade={upgrade}"
    monthly_dir = path_output / f"load_curve_monthly/state={state}/upgrade={upgrade}"

    # Fail hard rather than silently skip: a soft skip here would return without
    # writing anything, leaving any pre-existing monthly/annual output in place
    # for aws sync to silently re-upload as if it were current.
    if not input_dir.exists():
        raise FileNotFoundError(
            f"Hourly input directory does not exist: {input_dir} "
            f"(state={state} upgrade={upgrade})."
        )

    files = sorted(input_dir.glob("*.parquet"))
    n_files = len(files)
    if n_files == 0:
        raise FileNotFoundError(
            f"No hourly parquet files found in {input_dir} "
            f"(state={state} upgrade={upgrade})."
        )

    monthly_exprs = monthly_aggregation_exprs(rules) if add_monthly else None

    outputs: list[str] = []
    if add_monthly:
        outputs.append(f"monthly -> {monthly_dir}")
    if add_annual:
        annual_out_dir = (
            path_output / f"load_curve_annual/state={state}/upgrade={upgrade}"
        )
        outputs.append(f"annual -> {annual_out_dir}")

    print(f"  Found {n_files:,} hourly files in {input_dir}")
    for o in outputs:
        print(f"  {o}")

    if add_monthly:
        monthly_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    annual_frames: list[pl.DataFrame] = []
    done = 0
    errors = 0
    error_messages: list[str] = []

    def _process(src: Path) -> pl.DataFrame | str | None:
        try:
            return aggregate_one_building(
                src,
                add_monthly=add_monthly,
                monthly_output_path=monthly_dir / src.name if add_monthly else None,
                monthly_exprs=monthly_exprs,
                add_annual=add_annual,
                annual_rules=rules if add_annual else None,
            )
        except Exception as e:
            return f"{src.name}: {e}"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process, f): f for f in files}
        for future in as_completed(futures):
            result = future.result()
            done += 1
            if isinstance(result, str):
                errors += 1
                error_messages.append(result)
                print(f"  ERROR {result}")
            elif isinstance(result, pl.DataFrame):
                annual_frames.append(result)
            if done % 5000 == 0 or done == n_files:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                print(
                    f"  {done:,}/{n_files:,} ({rate:.0f} files/s, {elapsed:.1f}s elapsed)"
                )

    if errors:
        # Fail hard rather than write/sync partial output: a monthly or annual
        # file that silently drops the failed buildings would look complete but
        # isn't, and aws sync would happily upload it.
        shown = "\n".join(f"  {m}" for m in error_messages[:20])
        more = f"\n  ... and {errors - 20} more" if errors > 20 else ""
        raise RuntimeError(
            f"{errors:,} of {n_files:,} hourly files failed to aggregate for "
            f"state={state} upgrade={upgrade}. Refusing to write/sync partial "
            f"monthly/annual output.\n{shown}{more}"
        )

    if add_annual:
        assert path_annual_raw is not None
        # Fail hard: a soft skip would leave any pre-existing annual parquet in
        # place, and the caller's aws s3 sync would silently re-upload stale data.
        if not annual_frames:
            raise RuntimeError(
                f"add_annual=True but no annual rows were produced for "
                f"state={state} upgrade={upgrade} "
                f"(processed={done}, errors={errors}). Refusing to continue so "
                f"aws sync cannot upload stale load_curve_annual data."
            )
        out_path = write_consolidated_annual(
            annual_frames, path_annual_raw, path_output, state, upgrade
        )
        assert out_path is not None
        print(f"  Wrote annual: {out_path} ({len(annual_frames):,} buildings)")

    elapsed = time.time() - t0
    print(f"  Done: {done:,} files in {elapsed:.1f}s ({errors} errors)")


# ── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate hourly ResStock load curves to coarser time resolutions "
            "(monthly and/or annual). Use --add-monthly and/or --add-annual."
        )
    )
    parser.add_argument(
        "--path-input",
        required=True,
        help="Root of the ResStock release (local), e.g. .../res_2024_amy2018_2_sb",
    )
    parser.add_argument(
        "--path-output",
        required=True,
        help="Root to write monthly/annual load curves into (local)",
    )
    parser.add_argument(
        "--path-annual-raw",
        default=None,
        help=(
            "Root of the raw NREL release with load_curve_annual. Required when "
            "--add-annual is set (provides params/weight/upgrade_name)."
        ),
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
    parser.add_argument(
        "--add-monthly",
        action="store_true",
        default=False,
        help="Produce monthly load curves (default: False unless neither flag is set).",
    )
    parser.add_argument(
        "--add-annual",
        action="store_true",
        default=False,
        help="Produce annual load curves (default: False unless neither flag is set).",
    )
    args = parser.parse_args()

    add_monthly: bool = args.add_monthly
    add_annual: bool = args.add_annual
    if not add_monthly and not add_annual:
        add_monthly = True
        add_annual = True
        print("Neither --add-monthly nor --add-annual specified; producing both.")

    path_input = Path(args.path_input)
    path_output = Path(args.path_output)
    path_annual_raw = (
        Path(args.path_annual_raw) if args.path_annual_raw is not None else None
    )
    upgrade_ids = args.upgrade_ids.split()

    if not path_input.exists():
        print(f"Error: input path does not exist: {path_input}", file=sys.stderr)
        sys.exit(1)
    if add_annual and path_annual_raw is None:
        print(
            "Error: --path-annual-raw is required when --add-annual is set.",
            file=sys.stderr,
        )
        sys.exit(1)
    if path_annual_raw is not None and not path_annual_raw.exists():
        print(
            f"Error: --path-annual-raw does not exist: {path_annual_raw}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Loading bsf aggregation rules for release '{args.bsf_release}'...")
    rules = load_bsf_aggregation_map(args.bsf_release)
    annual_n = sum(1 for c in rules if is_annual_metric_column(c))
    print(f"  {len(rules)} column rules ({annual_n} used for annual subset)")
    print(
        f"  Producing: {'monthly' if add_monthly else ''}"
        f"{'+ ' if add_monthly and add_annual else ''}"
        f"{'annual' if add_annual else ''}"
    )

    for upgrade in upgrade_ids:
        print(f"\n{'=' * 60}")
        print(f"Processing state={args.state}, upgrade={upgrade}")
        print(f"{'=' * 60}")
        process_upgrade(
            path_input=path_input,
            path_output=path_output,
            state=args.state,
            upgrade=upgrade,
            rules=rules,
            workers=args.workers,
            add_monthly=add_monthly,
            add_annual=add_annual,
            path_annual_raw=path_annual_raw,
        )

    print("\nAll done.")


if __name__ == "__main__":
    main()
