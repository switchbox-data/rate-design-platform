"""Compare ResStock total load per utility to EIA-861 residential sales.

Uses ResStock hourly loads (load_curve_hourly: one parquet per building). Pre-aggregates
net electricity kWh per building, joins to utility assignment, then sums by utility.
Joins to EIA-861 residential_sales_mwh (converted to kWh) on utility shortcode and
outputs a comparison table (ratio, pct difference).

Per AGENTS.md: hourly loads live at
s3://data.sb/nrel/resstock/<release>/load_curve_hourly/state=<state>/upgrade=<upgrade>/<bldg_id>_<upgrade_id>.parquet
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import cast

import polars as pl

from utils import get_aws_region

BLDG_ID_COL = "bldg_id"
ELECTRIC_UTILITY_COL = "sb.electric_utility"
# Hourly load parquet: per-hour electricity (kWh). Sum = annual kWh.
LOAD_COL_PREFERRED = "out.electricity.total.energy_consumption"
LOAD_COL_FALLBACK = "total_fuel_electricity"
RESSTOCK_TOTAL_KWH = "resstock_total_kwh"
EIA_RESIDENTIAL_KWH = "eia_residential_kwh"
MWH_TO_KWH = 1000

DEFAULT_RESSTOCK_RELEASE = "res_2024_amy2018_2"
S3_BASE_RESSTOCK = "s3://data.sb/nrel/resstock"
S3_BASE_EIA861 = "s3://data.sb/eia/861/electric_utility_stats"


def _storage_options() -> dict[str, str]:
    return {"aws_region": get_aws_region()}


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _default_loads_path(release: str, state: str, upgrade: str) -> str:
    return f"{S3_BASE_RESSTOCK}/{release}/load_curve_hourly/state={state}/upgrade={upgrade}/"


def _default_utility_assignment_path(release: str, state: str) -> str:
    return f"{S3_BASE_RESSTOCK}/{release}/metadata_utility/state={state}/utility_assignment.parquet"


def _default_eia861_path(state: str) -> str:
    return f"{S3_BASE_EIA861}/state={state}/data.parquet"


def _normalize_s3_path(path: str) -> str:
    """Ensure path is a full s3:// URI (s3fs may return bucket/key)."""
    if path.startswith("s3://"):
        return path
    return "s3://" + path


def _list_load_parquet_paths(
    path_loads: str, storage_options: dict[str, str] | None
) -> list[tuple[int, str]]:
    """List parquet paths under path_loads and parse bldg_id from each filename. Returns [(bldg_id, path), ...]."""
    if _is_s3(path_loads):
        import s3fs

        fs = s3fs.S3FileSystem(default_fill_cache=False)
        raw = fs.glob(path_loads.rstrip("/") + "/*.parquet")
        full_paths = [_normalize_s3_path(p) for p in raw]
    else:
        p = Path(path_loads)
        if not p.is_dir():
            raise FileNotFoundError(f"Loads directory not found: {path_loads}")
        full_paths = [str(f) for f in p.glob("*.parquet")]

    result: list[tuple[int, str]] = []
    # Filename: <bldg_id>-<upgrade_id>.parquet or <bldg_id>_<upgrade_id>.parquet (AGENTS.md)
    pattern = re.compile(r"^(\d+)[-_]\d+\.parquet$", re.IGNORECASE)
    for full_path in full_paths:
        name = Path(full_path).name
        m = pattern.match(name)
        if m:
            result.append((int(m.group(1)), full_path))
    return result


def _electricity_column_for_load(
    schema_names: list[str], load_column_override: str | None
) -> str:
    if load_column_override is not None:
        if load_column_override not in schema_names:
            raise ValueError(
                f"Load parquet is missing requested column {load_column_override!r}. "
                f"Available (first 20): {schema_names[:20]!r}"
            )
        return load_column_override
    if LOAD_COL_PREFERRED in schema_names:
        return LOAD_COL_PREFERRED
    if LOAD_COL_FALLBACK in schema_names:
        return LOAD_COL_FALLBACK
    raise ValueError(
        f"Load parquet is missing column {LOAD_COL_PREFERRED!r} or {LOAD_COL_FALLBACK!r}. "
        f"Use --load-column to specify. Available (first 20): {schema_names[:20]!r}"
    )


def _annual_kwh_from_load_path(
    path: str,
    load_col: str,
    storage_options: dict[str, str] | None,
) -> float:
    opts = storage_options if _is_s3(path) else None
    df = pl.read_parquet(path, storage_options=opts)
    return float(df.select(pl.col(load_col).sum()).item())


def _load_resstock_total_kwh_per_utility_from_loads(
    path_loads: str,
    path_utility_assignment: str,
    storage_options: dict[str, str] | None,
    load_column_override: str | None = None,
) -> pl.DataFrame:
    """Pre-aggregate annual kWh per building from hourly load parquets, join to utility assignment, sum by utility."""
    bldg_paths = _list_load_parquet_paths(path_loads, storage_options)
    if not bldg_paths:
        raise ValueError(
            f"No parquet files found under {path_loads!r} (expected <bldg_id>-<upgrade>.parquet)"
        )

    # Infer electricity column from first file
    first_path = bldg_paths[0][1]
    opts = storage_options if _is_s3(first_path) else None
    schema = pl.scan_parquet(first_path, storage_options=opts).collect_schema().names()
    load_col = _electricity_column_for_load(schema, load_column_override)

    rows: list[tuple[int, float]] = []
    for bldg_id, p in bldg_paths:
        annual = _annual_kwh_from_load_path(p, load_col, storage_options)
        rows.append((bldg_id, annual))
    loads_df = pl.DataFrame(
        {BLDG_ID_COL: [r[0] for r in rows], "annual_kwh": [r[1] for r in rows]}
    )

    opts_ua = storage_options if _is_s3(path_utility_assignment) else None
    ua = pl.scan_parquet(path_utility_assignment, storage_options=opts_ua)
    if ELECTRIC_UTILITY_COL not in ua.collect_schema().names():
        raise ValueError(
            f"Utility assignment at {path_utility_assignment!r} is missing "
            f"required column {ELECTRIC_UTILITY_COL!r}"
        )
    ua = ua.select([BLDG_ID_COL, ELECTRIC_UTILITY_COL]).collect()

    joined = loads_df.join(cast(pl.DataFrame, ua), on=BLDG_ID_COL, how="inner")
    out = joined.group_by(ELECTRIC_UTILITY_COL).agg(
        pl.col("annual_kwh").sum().alias(RESSTOCK_TOTAL_KWH)
    )
    return cast(pl.DataFrame, out)


def _load_eia861_residential_kwh(
    path_eia861: str, storage_options: dict[str, str] | None
) -> pl.DataFrame:
    """Load EIA-861 state parquet and convert residential_sales_mwh to kWh."""
    opts = storage_options if _is_s3(path_eia861) else None
    out = (
        pl.scan_parquet(path_eia861, storage_options=opts)
        .select(
            pl.col("utility_code"),
            (pl.col("residential_sales_mwh") * MWH_TO_KWH).alias(EIA_RESIDENTIAL_KWH),
        )
        .collect()
    )
    return cast(pl.DataFrame, out)


def compare_resstock_eia861(
    path_loads: str,
    path_utility_assignment: str,
    path_eia861: str,
    storage_options: dict[str, str] | None = None,
    load_column: str | None = None,
) -> pl.DataFrame:
    """Compute comparison table: utility_code, resstock_total_kwh, eia_residential_kwh, ratio, pct_diff."""
    resstock = _load_resstock_total_kwh_per_utility_from_loads(
        path_loads,
        path_utility_assignment,
        storage_options=storage_options,
        load_column_override=load_column,
    )
    eia = _load_eia861_residential_kwh(path_eia861, storage_options)

    comparison = resstock.join(
        eia,
        left_on=ELECTRIC_UTILITY_COL,
        right_on="utility_code",
        how="inner",
    ).select(
        pl.col(ELECTRIC_UTILITY_COL).alias("utility_code"),
        pl.col(RESSTOCK_TOTAL_KWH),
        pl.col(EIA_RESIDENTIAL_KWH),
        (pl.col(RESSTOCK_TOTAL_KWH) / pl.col(EIA_RESIDENTIAL_KWH)).alias("ratio"),
        (
            (pl.col(RESSTOCK_TOTAL_KWH) - pl.col(EIA_RESIDENTIAL_KWH))
            / pl.col(EIA_RESIDENTIAL_KWH)
            * 100
        ).alias("pct_diff"),
    )
    return comparison


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare ResStock total load per utility to EIA-861 residential sales (uses hourly loads)."
    )
    parser.add_argument(
        "--state",
        required=True,
        help="State abbreviation (e.g. RI, NY)",
    )
    parser.add_argument(
        "--resstock-release",
        default=DEFAULT_RESSTOCK_RELEASE,
        help="ResStock release name",
    )
    parser.add_argument(
        "--upgrade",
        default="00",
        help="ResStock upgrade ID (zero-padded)",
    )
    parser.add_argument(
        "--path-loads",
        default=None,
        help="ResStock load_curve_hourly directory (default: S3 from release/state/upgrade)",
    )
    parser.add_argument(
        "--path-utility-assignment",
        default=None,
        help="ResStock utility assignment parquet path (default: S3 from release/state)",
    )
    parser.add_argument(
        "--path-eia861",
        default=None,
        help="EIA-861 state parquet path (default: S3 state partition)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Write comparison CSV here; if omitted, print to stdout",
    )
    parser.add_argument(
        "--load-column",
        default=None,
        help="Hourly load parquet column for electricity (default: %s or %s)"
        % (LOAD_COL_PREFERRED, LOAD_COL_FALLBACK),
    )
    args = parser.parse_args()

    state = args.state.strip().upper()
    path_loads = args.path_loads or _default_loads_path(
        args.resstock_release, state, args.upgrade
    )
    path_utility_assignment = (
        args.path_utility_assignment
        or _default_utility_assignment_path(args.resstock_release, state)
    )
    path_eia861 = args.path_eia861 or _default_eia861_path(state)

    opts = (
        _storage_options()
        if (
            _is_s3(path_loads) or _is_s3(path_utility_assignment) or _is_s3(path_eia861)
        )
        else None
    )

    comparison = compare_resstock_eia861(
        path_loads=path_loads,
        path_utility_assignment=path_utility_assignment,
        path_eia861=path_eia861,
        storage_options=opts,
        load_column=args.load_column or None,
    )

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        comparison.write_csv(args.output)
        print(f"Wrote {args.output}")
    else:
        print(comparison.write_csv())


if __name__ == "__main__":
    main()
