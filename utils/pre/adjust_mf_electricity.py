"""Apply multifamily non-HVAC column-by-column adjustment to ResStock load curve annual and hourly parquet.

Reads metadata and the single annual parquet file, computes MF/SF ratios (mean kWh/sqft,
non-zero only), adjusts multifamily rows in the annual file, then for each multifamily
bldg_id adjusts the corresponding hourly parquet in the load_curve_hourly directory
({bldg_id}-{upgrade}.parquet) by scaling non-HVAC columns and recomputing total. Writes
back to the same paths (local or S3).
"""

from __future__ import annotations

import math
from pathlib import Path

import polars as pl
from cloudpathlib import S3Path
from scipy import stats as scipy_stats

from utils import get_aws_region

BLDG_ID_COL = "bldg_id"
ANNUAL_ELECTRICITY_COL = "out.electricity.total.energy_consumption.kwh"
HOURLY_TOTAL_ELECTRICITY_COL = "out.electricity.total.energy_consumption"

BUILDING_TYPE_RECS_COL = "in.geometry_building_type_recs"
FLOOR_AREA_COL = "in.geometry_floor_area"

HVAC_RELATED_ELECTRICITY_COLS = (
    "out.electricity.cooling.energy_consumption.kwh",
    "out.electricity.cooling_fans_pumps.energy_consumption.kwh",
    "out.electricity.heating.energy_consumption.kwh",
    "out.electricity.heating_fans_pumps.energy_consumption.kwh",
    "out.electricity.heating_hp_bkup.energy_consumption.kwh",
    "out.electricity.heating_hp_bkup_fa.energy_consumption.kwh",
    "out.electricity.mech_vent.energy_consumption.kwh",
)
NON_HVAC_RELATED_ELECTRICITY_COLS = (
    "out.electricity.ceiling_fan.energy_consumption.kwh",
    "out.electricity.clothes_dryer.energy_consumption.kwh",
    "out.electricity.clothes_washer.energy_consumption.kwh",
    "out.electricity.dishwasher.energy_consumption.kwh",
    "out.electricity.freezer.energy_consumption.kwh",
    "out.electricity.hot_water.energy_consumption.kwh",
    "out.electricity.lighting_exterior.energy_consumption.kwh",
    "out.electricity.lighting_garage.energy_consumption.kwh",
    "out.electricity.lighting_interior.energy_consumption.kwh",
    "out.electricity.permanent_spa_heat.energy_consumption.kwh",
    "out.electricity.permanent_spa_pump.energy_consumption.kwh",
    "out.electricity.plug_loads.energy_consumption.kwh",
    "out.electricity.pool_heater.energy_consumption.kwh",
    "out.electricity.pool_pump.energy_consumption.kwh",
    "out.electricity.pv.energy_consumption.kwh",
    "out.electricity.range_oven.energy_consumption.kwh",
    "out.electricity.refrigerator.energy_consumption.kwh",
    "out.electricity.well_pump.energy_consumption.kwh",
)


def _storage_options(path: str) -> dict[str, str] | None:
    if path.startswith("s3://"):
        return {"aws_region": get_aws_region()}
    return None


def _annual_to_hourly_col(annual_col: str) -> str:
    """Map annual electricity column name to hourly (strip .kwh)."""
    if annual_col.endswith(".energy_consumption.kwh"):
        return annual_col.replace(".energy_consumption.kwh", ".energy_consumption")
    return annual_col


def _hourly_path(
    path_hourly_dir: str,
    bldg_id: int,
    upgrade: str,
) -> str:
    """Path to one building's hourly parquet: {dir}/{bldg_id}-{upgrade_int}.parquet."""
    upgrade_int = int(upgrade)
    if path_hourly_dir.startswith("s3://"):
        return str(S3Path(path_hourly_dir) / f"{bldg_id}-{upgrade_int}.parquet")
    return str(Path(path_hourly_dir) / f"{bldg_id}-{upgrade_int}.parquet")


def _parse_floor_area_sqft(val: str | None) -> float:
    """Parse one floor area value: '4000+' -> 5000, '750-999' -> midpoint, else float."""
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return float("nan")
    s = str(val).strip()
    if s.endswith("+"):
        return 5000.0
    if "-" in s:
        parts = s.split("-", 1)
        if len(parts) == 2:
            try:
                lo = float(parts[0].strip())
                hi = float(parts[1].strip())
                return (lo + hi) / 2.0
            except ValueError:
                return float("nan")
    try:
        return float(s.replace("+", ""))
    except ValueError:
        return float("nan")


def _two_sample_difference_of_means_test(
    group1: pl.Series,
    group2: pl.Series,
) -> dict[str, float | int]:
    """Welch's t-test for difference of two independent means. Returns mean1, mean2, diff, p_value, etc."""
    import numpy as np

    a1 = group1.to_numpy().astype(np.float64, copy=False)
    a2 = group2.to_numpy().astype(np.float64)
    a1 = a1[~(np.isnan(a1) | np.isinf(a1))]
    a2 = a2[~(np.isnan(a2) | np.isinf(a2))]
    n1, n2 = len(a1), len(a2)
    if n1 < 2 or n2 < 2:
        return {
            "mean1": float(np.mean(a1)) if n1 else float("nan"),
            "mean2": float(np.mean(a2)) if n2 else float("nan"),
            "diff": float("nan"),
            "p_value": 1.0,
        }
    mean1 = float(np.mean(a1))
    mean2 = float(np.mean(a2))
    std1 = float(np.std(a1, ddof=1))
    std2 = float(np.std(a2, ddof=1))
    se1_sq = (std1**2) / n1
    se2_sq = (std2**2) / n2
    se_diff = math.sqrt(se1_sq + se2_sq)
    t_stat = (mean1 - mean2) / se_diff if se_diff > 0 else 0.0
    num = (se1_sq + se2_sq) ** 2
    denom = (se1_sq**2 / (n1 - 1)) + (se2_sq**2 / (n2 - 1))
    welch_df = num / denom if denom > 0 else 0.0
    p_value = float(2 * scipy_stats.t.sf(abs(t_stat), welch_df))
    return {
        "mean1": mean1,
        "mean2": mean2,
        "diff": mean1 - mean2,
        "p_value": p_value,
    }


def _get_non_hvac_mf_to_sf_ratios(
    annual_df: pl.DataFrame,
    metadata_df: pl.DataFrame,
) -> dict[str, float]:
    """Compute MF/SF ratio (mean kWh/sqft, non-zero only) for each non-HVAC electricity column."""
    ratios: dict[str, float] = {}
    if (
        BUILDING_TYPE_RECS_COL not in metadata_df.columns
        or FLOOR_AREA_COL not in metadata_df.columns
    ):
        return ratios
    meta = (
        metadata_df.with_columns(
            pl.col(FLOOR_AREA_COL)
            .map_batches(
                lambda s: pl.Series([_parse_floor_area_sqft(x) for x in s]),
                return_dtype=pl.Float64,
            )
            .alias("floor_area_sqft")
        )
        .with_columns(
            pl.col(BUILDING_TYPE_RECS_COL)
            .str.contains("Single-Family", literal=True)
            .alias("_is_sf"),
            pl.col(BUILDING_TYPE_RECS_COL)
            .str.contains("Multi-Family", literal=True)
            .alias("_is_mf"),
        )
        .select(
            pl.col(BLDG_ID_COL),
            pl.col("floor_area_sqft"),
            pl.col("_is_sf"),
            pl.col("_is_mf"),
        )
    )
    non_hvac_present = [
        c for c in NON_HVAC_RELATED_ELECTRICITY_COLS if c in annual_df.columns
    ]
    if not non_hvac_present:
        return ratios
    merged = annual_df.select(
        [pl.col(BLDG_ID_COL)] + [pl.col(c) for c in non_hvac_present]
    ).join(meta, on=BLDG_ID_COL, how="inner")
    merged = merged.filter(
        pl.col("floor_area_sqft").is_finite() & (pl.col("floor_area_sqft") > 0)
    )
    sf_df = merged.filter(pl.col("_is_sf"))
    mf_df = merged.filter(pl.col("_is_mf"))
    for col in non_hvac_present:
        by_sqft = pl.col(col) / pl.col("floor_area_sqft")
        sf_vals = (
            sf_df.filter(pl.col(col) > 0)
            .with_columns(by_sqft.alias("_kwh_sqft"))
            .filter(pl.col("_kwh_sqft").is_finite())
            .get_column("_kwh_sqft")
        )
        mf_vals = (
            mf_df.filter(pl.col(col) > 0)
            .with_columns(by_sqft.alias("_kwh_sqft"))
            .filter(pl.col("_kwh_sqft").is_finite())
            .get_column("_kwh_sqft")
        )
        if sf_vals.len() < 2 or mf_vals.len() < 2:
            ratios[col] = 1.0
            continue
        test = _two_sample_difference_of_means_test(mf_vals, sf_vals)
        mean_sf = test["mean2"]
        mean_mf = test["mean1"]
        ratios[col] = mean_mf / mean_sf if mean_sf != 0 else 1.0
    return ratios


def _apply_hourly_mf_adjustment(
    path_hourly: str,
    ratios: dict[str, float],
    storage_options: dict[str, str] | None,
) -> None:
    """Scale non-HVAC columns in one hourly parquet by 1/ratio and recompute total; sink back."""
    hvac_hourly = [_annual_to_hourly_col(c) for c in HVAC_RELATED_ELECTRICITY_COLS]
    non_hvac_annual_to_hourly = {
        _annual_to_hourly_col(c): c for c in NON_HVAC_RELATED_ELECTRICITY_COLS
    }
    lf = pl.scan_parquet(path_hourly, storage_options=storage_options)
    schema = lf.collect_schema().names()
    if HOURLY_TOTAL_ELECTRICITY_COL not in schema:
        return
    non_hvac_in_schema = [
        h for h in non_hvac_annual_to_hourly if h in schema
    ]
    if not non_hvac_in_schema:
        return
    sum_parts: list[pl.Expr] = []
    update_exprs: list[pl.Expr] = []
    for h_col in non_hvac_in_schema:
        annual_col = non_hvac_annual_to_hourly[h_col]
        ratio = ratios.get(annual_col, 1.0)
        if ratio > 0:
            sum_parts.append(pl.col(h_col) / ratio)
            update_exprs.append((pl.col(h_col) / ratio).alias(h_col))
        else:
            sum_parts.append(pl.col(h_col))
    hvac_cols_present = [c for c in hvac_hourly if c in schema]
    adjusted_non_hvac = pl.sum_horizontal(sum_parts)
    sum_hvac = (
        pl.sum_horizontal([pl.col(c) for c in hvac_cols_present])
        if hvac_cols_present
        else pl.lit(0.0)
    )
    new_total = sum_hvac + adjusted_non_hvac
    update_exprs.append(new_total.alias(HOURLY_TOTAL_ELECTRICITY_COL))
    lf.with_columns(update_exprs).sink_parquet(path_hourly, storage_options=storage_options)


def adjust_mf_electricity_parquet(
    path_metadata: str,
    path_annual: str,
    path_load_curve_hourly: str,
    *,
    upgrade: str = "0",
    storage_options: dict[str, str] | None = None,
) -> None:
    """Load metadata and single annual parquet, apply MF non-HVAC adjustment, write back; then adjust each MF bldg_id's hourly parquet in path_load_curve_hourly.

    path_annual is the path to the single load curve annual parquet file.
    path_load_curve_hourly is a directory (local or s3://) containing one parquet per
    bldg_id, named {bldg_id}-{upgrade}.parquet. Only multifamily bldg_ids are adjusted.

    S3: storage_options are inferred from get_aws_region() when path starts with s3://.
    """
    opts_meta = storage_options or _storage_options(path_metadata)
    opts_annual = storage_options or _storage_options(path_annual)
    opts_hourly = storage_options or _storage_options(path_load_curve_hourly)

    metadata_df = pl.read_parquet(path_metadata, storage_options=opts_meta)
    if BLDG_ID_COL not in metadata_df.columns:
        raise ValueError(
            f"Metadata at {path_metadata!r} missing column {BLDG_ID_COL!r}"
        )

    lf = pl.scan_parquet(path_annual, storage_options=opts_annual)
    schema = lf.collect_schema().names()
    if BLDG_ID_COL not in schema or ANNUAL_ELECTRICITY_COL not in schema:
        return
    non_hvac_present = [
        c for c in NON_HVAC_RELATED_ELECTRICITY_COLS if c in schema
    ]
    if not non_hvac_present:
        return
    annual_for_ratio = lf.select(
        [pl.col(BLDG_ID_COL)] + [pl.col(c) for c in non_hvac_present]
    ).collect()
    meta_subset = metadata_df.filter(
        pl.col(BLDG_ID_COL).is_in(annual_for_ratio.get_column(BLDG_ID_COL))
    )
    if meta_subset.height == 0:
        return
    ratios = _get_non_hvac_mf_to_sf_ratios(annual_for_ratio, meta_subset)
    multifamily_bldg_ids = (
        meta_subset.filter(
            pl.col(BUILDING_TYPE_RECS_COL).str.contains(
                "Multi-Family", literal=True
            )
        )
        .get_column(BLDG_ID_COL)
        .to_list()
    )
    if not multifamily_bldg_ids or not ratios:
        return

    non_hvac_in_df = [
        c for c in NON_HVAC_RELATED_ELECTRICITY_COLS if c in schema
    ]
    is_mf = pl.col(BLDG_ID_COL).is_in(multifamily_bldg_ids)
    hvac_cols = [c for c in HVAC_RELATED_ELECTRICITY_COLS if c in schema]
    sum_parts: list[pl.Expr] = []
    for c in non_hvac_in_df:
        ratio = ratios.get(c, 1.0)
        if ratio > 0:
            sum_parts.append(
                pl.when(is_mf).then(pl.col(c) / ratio).otherwise(pl.col(c))
            )
        else:
            sum_parts.append(pl.col(c))
    adjusted_non_hvac = pl.sum_horizontal(sum_parts)
    sum_hvac = (
        pl.sum_horizontal([pl.col(c) for c in hvac_cols])
        if hvac_cols
        else pl.lit(0.0)
    )
    new_total = sum_hvac + adjusted_non_hvac
    update_exprs: list[pl.Expr] = [
        new_total.alias(ANNUAL_ELECTRICITY_COL),
    ]
    for c in non_hvac_in_df:
        ratio = ratios.get(c, 1.0)
        if ratio > 0:
            update_exprs.append(
                pl.when(is_mf)
                .then(pl.col(c) / ratio)
                .otherwise(pl.col(c))
                .alias(c)
            )
    lf.with_columns(update_exprs).sink_parquet(path_annual, storage_options=opts_annual)

    for bldg_id in multifamily_bldg_ids:
        bldg_id_int = int(bldg_id) if not isinstance(bldg_id, int) else bldg_id
        path_hourly = _hourly_path(path_load_curve_hourly, bldg_id_int, upgrade)
        if not path_load_curve_hourly.startswith("s3://") and not Path(
            path_hourly
        ).exists():
            continue
        _apply_hourly_mf_adjustment(path_hourly, ratios, opts_hourly)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Apply multifamily non-HVAC adjustment to ResStock load curve annual and hourly parquet."
    )
    parser.add_argument(
        "path_metadata",
        type=str,
        help="Path to metadata parquet (local or s3://).",
    )
    parser.add_argument(
        "path_annual",
        type=str,
        help="Path to the single load curve annual parquet file.",
    )
    parser.add_argument(
        "path_load_curve_hourly",
        type=str,
        help="Path to load_curve_hourly directory containing one parquet per bldg_id ({bldg_id}-{upgrade}.parquet).",
    )
    parser.add_argument(
        "--upgrade",
        type=str,
        default="0",
        help="Upgrade id used in hourly filenames (default: 0).",
    )
    args = parser.parse_args()
    adjust_mf_electricity_parquet(
        args.path_metadata,
        args.path_annual,
        args.path_load_curve_hourly,
        upgrade=args.upgrade,
    )
