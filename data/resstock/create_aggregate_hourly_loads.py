"""Build aggregate hourly electricity profiles (MWh) from ResStock loads.

Filters buildings using ``utility_assignment.parquet`` (``sb.electric_utility``,
``postprocess_group.has_hp``, ``weight``), then sums hourly loads per upgrade.

**``--customer-class``** (required): ``true`` or ``false``.

- **true:** Split aggregation into **HP** vs **non-HP** cohorts (two outputs per
  upgrade). Optional ``--electric-utility`` narrows both cohorts to that utility;
  if omitted, cohorts are **state-wide**.
- **false:** Single aggregate **by utility only** (all buildings in that utility,
  ignoring HP). **Requires** ``--electric-utility`` (use ``all`` to run once per
  unique ``sb.electric_utility`` in the metadata file).

kWh values are converted to MWh by dividing by 1000 after the weighted or
unweighted sum.

**``--electric-utility all``** runs the same aggregation for **each** distinct
``sb.electric_utility`` in ``utility_assignment.parquet`` (sorted). With
``--customer-class true``, each utility gets separate HP and non-HP outputs.

Usage::

    # Utility total (HP + non-HP combined)
    uv run python data/resstock/create_aggregate_hourly_loads.py \\
        --nrel-root s3://data.sb/nrel/resstock \\
        --release res_2024_amy2018_2_sb \\
        --state NY \\
        --upgrades 00 02 \\
        --electric-utility coned \\
        --customer-class false \\
        --path-output-dir /tmp/agg_loads

    # Separate HP vs non-HP for one utility
    uv run python data/resstock/create_aggregate_hourly_loads.py \\
        --resstock-base s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb \\
        --state NY --upgrades 02 \\
        --electric-utility coned \\
        --customer-class true \\
        --path-output-dir /tmp/agg_loads

    # State-wide HP vs non-HP (no utility filter)
    uv run python data/resstock/create_aggregate_hourly_loads.py \\
        --resstock-base s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb \\
        --state RI --upgrades 02 \\
        --customer-class true \\
        --path-output-dir /tmp/agg_loads

    # Every utility in the state (utility totals, one file per utility per upgrade)
    uv run python data/resstock/create_aggregate_hourly_loads.py \\
        --state NY --upgrades 00 02 \\
        --electric-utility all \\
        --customer-class false \\
        --path-output-dir /tmp/agg_loads \\
        --nrel-root s3://data.sb/nrel/resstock \\
        --release res_2024_amy2018_2_sb
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import cast

import polars as pl
import s3fs

from utils import get_aws_region
from utils.loads import (
    BLDG_ID_COL,
    ELECTRIC_LOAD_COL,
    LOAD_CURVE_HOURLY_SUBDIR,
)

log = logging.getLogger(__name__)

_UA_UTILITY_COL = "sb.electric_utility"
_UA_HP_COL = "postprocess_group.has_hp"
_WEIGHT_COL = "weight"

# Cap parallel parquet readers (I/O bound; avoids huge thread counts on wide folders).
_MAX_PARQUET_READ_WORKERS = 256


def _normalize_upgrade(u: str) -> str:
    s = u.strip()
    if s.isdigit():
        return s.zfill(2)
    return s


def _parse_upgrades(s: str) -> list[str]:
    return [_normalize_upgrade(x) for x in s.split()]


def _upgrade_partition_dir(resstock_base: str, state: str, upgrade: str) -> str:
    base = resstock_base.rstrip("/")
    st = state.strip().upper()
    up = _normalize_upgrade(upgrade)
    return f"{base}/{LOAD_CURVE_HOURLY_SUBDIR}state={st}/upgrade={up}"


def _glob_parquet_local(partition_dir: str) -> list[str]:
    p = Path(partition_dir)
    if not p.is_dir():
        return []
    return sorted(str(x) for x in p.glob("*.parquet"))


def _glob_parquet_s3(partition_dir: str, storage_options: dict[str, str]) -> list[str]:
    """List ``*.parquet`` under an ``s3://`` partition directory."""
    rest = partition_dir.removeprefix("s3://")
    bucket, _, key_prefix = rest.partition("/")
    key_prefix = key_prefix.rstrip("/")
    fs = _s3_fs(storage_options)
    pattern = f"{bucket}/{key_prefix}/*.parquet"
    keys = fs.glob(pattern)
    return sorted(f"s3://{k}" for k in keys)


def _s3_fs(storage_options: dict[str, str]) -> s3fs.S3FileSystem:
    region = storage_options.get("aws_region")
    kwargs: dict = {}
    if region:
        kwargs["client_kwargs"] = {"region_name": region}
    return s3fs.S3FileSystem(**kwargs)


def _constructed_parquet_paths(
    partition_dir: str, bldg_ids: list[int], upgrade: str
) -> list[str]:
    """One path per building: ``{bldg_id}-{int(upgrade)}.parquet`` (ResStock on-disk layout)."""
    up_int = int(_normalize_upgrade(upgrade))
    root = partition_dir.rstrip("/")
    return [f"{root}/{bid}-{up_int}.parquet" for bid in bldg_ids]


def _parquet_paths_for_partition(
    partition_dir: str,
    bldg_ids: list[int],
    upgrade: str,
    storage_options: dict[str, str],
) -> list[str]:
    """Prefer directory glob (chunk files, tests); else per-building filenames (typical S3 layout)."""
    if partition_dir.startswith("s3://"):
        paths = _glob_parquet_s3(partition_dir, storage_options)
    else:
        paths = _glob_parquet_local(partition_dir)
    if paths:
        return paths
    return _constructed_parquet_paths(partition_dir, bldg_ids, upgrade)


def _storage_options_for_path(
    path: str, storage_options: dict[str, str]
) -> dict[str, str] | None:
    return storage_options if path.startswith("s3://") else None


def _partial_aggregate_one_parquet(
    path: str,
    *,
    allowed_bldg_ids: frozenset[int],
    load_col: str,
    weighted: bool,
    weights_df: pl.DataFrame,
    storage_options: dict[str, str],
) -> tuple[pl.DataFrame | None, int]:
    """Read one parquet, filter to cohort, return partial ``load_mwh`` and cohort building count.

    The int is the number of distinct ``bldg_id`` values from the cohort present in
    this file (for progress reporting).
    """
    opts = _storage_options_for_path(path, storage_options)
    try:
        df = pl.read_parquet(
            path,
            columns=[BLDG_ID_COL, "timestamp", load_col],
            storage_options=opts,
        )
    except OSError:
        log.debug("Skipping unreadable or missing parquet: %s", path)
        return None, 0

    df = df.filter(pl.col(BLDG_ID_COL).cast(pl.Int64).is_in(list(allowed_bldg_ids)))
    if df.height == 0:
        return None, 0

    n_bldg_in_file = int(df[BLDG_ID_COL].n_unique())

    if weighted:
        df = df.join(weights_df, on=BLDG_ID_COL, how="inner")
        expr_load = pl.col(load_col).cast(pl.Float64) * pl.col(_WEIGHT_COL).cast(
            pl.Float64
        )
    else:
        expr_load = pl.col(load_col).cast(pl.Float64)

    out = (
        df.with_columns((expr_load / 1000.0).alias("_mwh"))
        .group_by("timestamp")
        .agg(pl.col("_mwh").sum().alias("load_mwh"))
    )
    return out, n_bldg_in_file


def _resstock_base(args: argparse.Namespace) -> str:
    if args.resstock_base:
        return str(args.resstock_base).rstrip("/")
    if not args.release:
        msg = "Provide --resstock-base or --release (with --nrel-root)"
        raise ValueError(msg)
    root = str(args.nrel_root).rstrip("/")
    release = str(args.release).strip()
    return f"{root}/{release}"


def _default_utility_assignment_path(resstock_base: str, state: str) -> str:
    st = state.strip().upper()
    return f"{resstock_base}/metadata_utility/state={st}/utility_assignment.parquet"


def list_unique_electric_utilities(
    path_utility_assignment: str,
    storage_options: dict[str, str],
) -> list[str]:
    """Distinct ``sb.electric_utility`` values in the file, lowercased and sorted."""
    df = pl.read_parquet(
        path_utility_assignment,
        columns=[_UA_UTILITY_COL],
        storage_options=storage_options,
    )
    if _UA_UTILITY_COL not in df.columns:
        msg = f"utility_assignment missing column {_UA_UTILITY_COL!r}"
        raise ValueError(msg)
    raw = df[_UA_UTILITY_COL].drop_nulls().unique().to_list()
    out = sorted(
        {str(v).strip().lower() for v in raw if str(v).strip()},
    )
    if not out:
        msg = "No electric utility values found in utility_assignment"
        raise ValueError(msg)
    return out


def electric_utility_passes(
    electric_utility: str | None,
    *,
    path_utility_assignment: str,
    storage_options: dict[str, str],
) -> list[str | None]:
    """Return one pass per utility, or ``[None]`` when no utility filter (state-wide)."""
    if electric_utility is None:
        return [None]
    s = electric_utility.strip().lower()
    if s == "all":
        return cast(
            list[str | None],
            list_unique_electric_utilities(
                path_utility_assignment,
                storage_options=storage_options,
            ),
        )
    return [s]


def load_filtered_buildings(
    path_utility_assignment: str,
    *,
    electric_utility: str | None,
    has_hp_filter: bool | None,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Return ``bldg_id``, ``weight`` filtered by utility and/or HP cohort.

    ``has_hp_filter``: ``None`` = all buildings (subject to utility filter);
    ``True`` = HP only; ``False`` = non-HP only.
    """
    if electric_utility is None and has_hp_filter is None:
        msg = "Provide electric_utility and/or has_hp_filter"
        raise ValueError(msg)

    ua = pl.read_parquet(path_utility_assignment, storage_options=storage_options)
    for col in (BLDG_ID_COL, _WEIGHT_COL, _UA_UTILITY_COL, _UA_HP_COL):
        if col not in ua.columns:
            msg = f"utility_assignment missing column {col!r}; have {ua.columns}"
            raise ValueError(msg)

    lf = ua.lazy()
    if electric_utility is not None:
        lf = lf.filter(pl.col(_UA_UTILITY_COL) == electric_utility.strip().lower())
    if has_hp_filter is True:
        lf = lf.filter(pl.col(_UA_HP_COL))
    elif has_hp_filter is False:
        lf = lf.filter(~pl.col(_UA_HP_COL))

    out = cast(
        pl.DataFrame,
        lf.select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col(_WEIGHT_COL).cast(pl.Float64),
        ).collect(),
    )
    return out


def aggregate_hourly_load_mwh(
    resstock_base: str,
    state: str,
    upgrade: str,
    buildings: pl.DataFrame,
    *,
    load_col: str,
    weighted: bool,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """One row per timestamp: ``timestamp``, ``load_mwh``.

    Reads each hourly parquet under the state/upgrade partition in parallel
    (threads). Each worker returns a partial per-timestamp sum; the main thread
    concatenates and sums with no shared mutable state (safe for concurrency).
    """
    bldg_ids = buildings[BLDG_ID_COL].to_list()
    if not bldg_ids:
        msg = "No buildings after filters; check utility / cohort"
        raise ValueError(msg)

    up = _normalize_upgrade(upgrade)
    partition_dir = _upgrade_partition_dir(resstock_base, state, up)
    paths = _parquet_paths_for_partition(
        partition_dir,
        bldg_ids,
        up,
        storage_options,
    )
    if not paths:
        msg = f"No parquet paths resolved under {partition_dir}"
        raise ValueError(msg)

    allowed = frozenset(bldg_ids)
    weights_df = buildings.select(
        pl.col(BLDG_ID_COL).cast(pl.Int64),
        pl.col(_WEIGHT_COL).cast(pl.Float64),
    )

    n_workers = min(_MAX_PARQUET_READ_WORKERS, max(1, len(paths)))
    total_bldgs = len(bldg_ids)
    # Log roughly every 2% of the cohort so large runs do not flood INFO.
    log_step = max(1, total_bldgs // 50)

    def _worker(path: str) -> tuple[pl.DataFrame | None, int]:
        return _partial_aggregate_one_parquet(
            path,
            allowed_bldg_ids=allowed,
            load_col=load_col,
            weighted=weighted,
            weights_df=weights_df,
            storage_options=storage_options,
        )

    partials: list[pl.DataFrame] = []
    processed_bldgs = 0
    next_milestone = log_step
    last_logged = 0
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futures = {ex.submit(_worker, p): p for p in paths}
        for fut in as_completed(futures):
            part, n_bldg = fut.result()
            processed_bldgs += n_bldg
            if part is not None and part.height > 0:
                partials.append(part)
            shown = min(processed_bldgs, total_bldgs)
            while next_milestone <= total_bldgs and shown >= next_milestone:
                log.info(
                    "Progress: %d out of %d buildings processed",
                    next_milestone,
                    total_bldgs,
                )
                last_logged = next_milestone
                next_milestone += log_step
    final_shown = min(processed_bldgs, total_bldgs)
    if final_shown != last_logged:
        log.info(
            "Progress: %d out of %d buildings processed",
            final_shown,
            total_bldgs,
        )

    if not partials:
        msg = "No load data after reading parquet files; check paths / cohort / load column"
        raise ValueError(msg)

    out = (
        pl.concat(partials)
        .group_by("timestamp")
        .agg(pl.col("load_mwh").sum().alias("load_mwh"))
        .sort("timestamp")
    )
    return cast(pl.DataFrame, out)


def _safe_label(s: str | None) -> str:
    if s is None:
        return "all"
    return re.sub(r"[^\w.\-]+", "_", s)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Aggregate ResStock hourly electric load to MWh per timestamp.",
    )
    p.add_argument(
        "--resstock-base",
        type=str,
        default=None,
        help="Full path to ResStock release root (contains load_curve_hourly/).",
    )
    p.add_argument(
        "--nrel-root",
        type=str,
        default="s3://data.sb/nrel/resstock",
        help="Parent of release folder (used with --release if --resstock-base unset).",
    )
    p.add_argument(
        "--release",
        type=str,
        default=None,
        help="Release directory name, e.g. res_2024_amy2018_2_sb (required if no --resstock-base).",
    )
    p.add_argument("--state", type=str, required=True, help="State, e.g. NY or RI.")
    p.add_argument(
        "--upgrades",
        type=str,
        required=True,
        help='Space-separated upgrade IDs, e.g. "00 02".',
    )
    p.add_argument(
        "--electric-utility",
        type=str,
        default=None,
        help=(
            "``sb.electric_utility`` code (e.g. coned, rie), or ``all`` to run once "
            "per distinct utility in the metadata file. Omit only when "
            "--customer-class is true (state-wide HP vs non-HP)."
        ),
    )
    p.add_argument(
        "--customer-class",
        type=str,
        choices=("true", "false"),
        required=True,
        help=(
            "If true, aggregate separately for HP vs non-HP (optional utility, or "
            "``all`` for every utility). If false, one aggregate per utility "
            "(requires --electric-utility or ``all``)."
        ),
    )
    p.add_argument(
        "--path-utility-assignment",
        type=str,
        default=None,
        help="Override path to utility_assignment.parquet.",
    )
    p.add_argument(
        "--path-output-dir",
        type=Path,
        required=True,
        help="Directory for output Parquet files (created if missing).",
    )
    p.add_argument(
        "--load-column",
        type=str,
        default=ELECTRIC_LOAD_COL,
        help=f"Hourly kWh column (default: {ELECTRIC_LOAD_COL}).",
    )
    p.add_argument(
        "--unweighted",
        action="store_true",
        help="Sum building kWh without sample weights (default: weighted).",
    )
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()

    split_by_hp = args.customer_class == "true"
    if not split_by_hp and not args.electric_utility:
        log.error(
            "When --customer-class is false, --electric-utility is required "
            "(use a code or ``all`` for every utility in the metadata).",
        )
        sys.exit(2)

    try:
        resstock_base = _resstock_base(args)
    except ValueError as e:
        log.error("%s", e)
        sys.exit(2)
    if args.resstock_base and args.release:
        log.info("Using --resstock-base; --release/--nrel-root ignored")
    state_u = args.state.strip().upper()
    upgrades = _parse_upgrades(args.upgrades)
    if not upgrades:
        log.error("No upgrades parsed from --upgrades")
        sys.exit(2)

    path_ua = args.path_utility_assignment or _default_utility_assignment_path(
        resstock_base,
        state_u,
    )
    storage_options = {"aws_region": get_aws_region()}

    log.info("ResStock base: %s", resstock_base)
    log.info("Utility assignment: %s", path_ua)
    log.info("Split by HP cohorts: %s", split_by_hp)

    out_dir: Path = args.path_output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    weighted = not args.unweighted

    try:
        util_passes = electric_utility_passes(
            args.electric_utility,
            path_utility_assignment=path_ua,
            storage_options=storage_options,
        )
    except ValueError as e:
        log.error("%s", e)
        sys.exit(2)

    if args.electric_utility and args.electric_utility.strip().lower() == "all":
        log.info("Running %d electric-utility passes (all)", len(util_passes))

    cohorts: list[tuple[str, bool | None]]
    if split_by_hp:
        cohorts = [("hp", True), ("non_hp", False)]
    else:
        cohorts = [("all", None)]

    for util_code in util_passes:
        util_label = _safe_label(util_code)
        for cohort_name, has_hp_filter in cohorts:
            buildings = load_filtered_buildings(
                path_ua,
                electric_utility=util_code,
                has_hp_filter=has_hp_filter,
                storage_options=storage_options,
            )
            log.info(
                "Cohort %s: %d buildings (utility=%s, has_hp_filter=%s)",
                cohort_name,
                buildings.height,
                util_code,
                has_hp_filter,
            )
            for up in upgrades:
                df = aggregate_hourly_load_mwh(
                    resstock_base,
                    state_u,
                    up,
                    buildings,
                    load_col=args.load_column,
                    weighted=weighted,
                    storage_options=storage_options,
                )
                if split_by_hp:
                    name = (
                        f"aggregate_hourly_load_{state_u}_{util_label}_{cohort_name}"
                        f"_upgrade{up}.parquet"
                    )
                else:
                    name = f"aggregate_hourly_load_{state_u}_{util_label}_upgrade{up}.parquet"
                path_out = out_dir / name
                df.write_parquet(path_out, storage_options=storage_options)
                log.info("Wrote %s (%d rows)", path_out, df.height)


if __name__ == "__main__":
    main()
