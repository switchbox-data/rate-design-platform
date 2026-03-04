"""Apply multifamily non-HVAC column-by-column adjustment to ResStock load curve annual and hourly parquet.

Reads metadata and the single annual parquet file, computes MF/SF ratios (mean kWh/sqft,
non-zero only), adjusts multifamily rows in the annual file, then for each multifamily
bldg_id adjusts the corresponding hourly parquet in the load_curve_hourly directory
({bldg_id}-{upgrade}.parquet) by scaling non-HVAC columns and recomputing total. Writes
back to the same paths (local or S3).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region

STORAGE_OPTIONS = {"aws_region": get_aws_region()}


def _storage_options_for_path(path: Path | S3Path) -> dict[str, str]:
    """Return STORAGE_OPTIONS for S3 paths, empty dict for local Path."""
    return STORAGE_OPTIONS if isinstance(path, S3Path) else {}


BLDG_ID_COL = "bldg_id"
ANNUAL_ELECTRICITY_COL = "out.electricity.total.energy_consumption.kwh"
HOURLY_TOTAL_ELECTRICITY_CONSUMPTION_COL = "out.electricity.total.energy_consumption"
HOURLY_TOTAL_ELECTRICITY_INTENSITY_COL = (
    "out.electricity.total.energy_consumption_intensity"
)
MF_NON_HVAC_ELECTRICITY_ADJUSTED_COL = "mf_non_hvac_electricity_adjusted"
BUILDING_TYPE_RECS_COL = "in.geometry_building_type_recs"
FLOOR_AREA_COL = "in.geometry_floor_area"

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


def annual_to_hourly_cols(annual_col: str) -> list[str]:
    """Return [consumption_col, consumption_intensity_col] for the hourly load curve.

    Expects annual column name like "out.electricity.<enduse>.energy_consumption.kwh".
    Returns empty list if the name does not match that pattern.
    """
    if not annual_col.endswith(".energy_consumption.kwh"):
        return []
    consumption = annual_col.replace(".energy_consumption.kwh", ".energy_consumption")
    intensity = annual_col.replace(
        ".energy_consumption.kwh", ".energy_consumption_intensity"
    )
    return [consumption, intensity]


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


def _get_non_hvac_mf_to_sf_ratios(
    load_curve_annual: pl.LazyFrame,
    metadata: pl.LazyFrame,
) -> dict[str, float]:
    """Compute MF/SF ratio (mean kWh/sqft, non-zero only) for each non-HVAC electricity column."""
    ratios: dict[str, float] = {}
    meta_schema = metadata.collect_schema().names()
    if BUILDING_TYPE_RECS_COL not in meta_schema or FLOOR_AREA_COL not in meta_schema:
        return ratios
    meta = (
        metadata.with_columns(
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
    annual_schema = load_curve_annual.collect_schema().names()
    non_hvac_present = [
        c for c in NON_HVAC_RELATED_ELECTRICITY_COLS if c in annual_schema
    ]
    if not non_hvac_present:
        return ratios
    merged = cast(
        pl.DataFrame,
        (
            load_curve_annual.select(
                [pl.col(BLDG_ID_COL)] + [pl.col(c) for c in non_hvac_present]
            )
            .join(meta, on=BLDG_ID_COL, how="inner")
            .filter(
                pl.col("floor_area_sqft").is_finite() & (pl.col("floor_area_sqft") > 0)
            )
            .collect()
        ),
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
        mean_sf = cast(float, sf_vals.mean())
        mean_mf = cast(float, mf_vals.mean())
        ratios[col] = mean_mf / mean_sf if mean_sf != 0 else 1.0
    return ratios


def _adjust_mf_electricity_hourly_one_bldg(
    load_curve_hourly: pl.LazyFrame,
    ratios: dict[str, float],
) -> pl.LazyFrame:
    """Adjust multifamily electricity in hourly load curve: scale non-HVAC consumption and intensity by dividing by MF/SF ratio. Returns adjusted LazyFrame."""
    load_curve_hourly_schema = load_curve_hourly.collect_schema().names()
    if HOURLY_TOTAL_ELECTRICITY_CONSUMPTION_COL not in load_curve_hourly_schema:
        raise ValueError(
            f"Hourly load curve missing required column {HOURLY_TOTAL_ELECTRICITY_CONSUMPTION_COL!r}"
        )
    if HOURLY_TOTAL_ELECTRICITY_INTENSITY_COL not in load_curve_hourly_schema:
        raise ValueError(
            f"Hourly load curve missing required column {HOURLY_TOTAL_ELECTRICITY_INTENSITY_COL!r}"
        )

    # Non-HVAC: scale both consumption and consumption_intensity using annual_to_hourly_cols
    sum_parts: list[pl.Expr] = []
    sum_parts_intensity: list[pl.Expr] = []
    non_hvac_consumption_cols: list[str] = []
    non_hvac_intensity_cols: list[str] = []
    update_exprs: list[pl.Expr] = []
    for annual_col in NON_HVAC_RELATED_ELECTRICITY_COLS:
        hourly_cols = annual_to_hourly_cols(annual_col)
        if not hourly_cols:
            raise ValueError(
                f"Annual column {annual_col} does not have corresponding hourly columns"
            )
        consumption_col, intensity_col = hourly_cols[0], hourly_cols[1]
        if (
            consumption_col not in load_curve_hourly_schema
            or intensity_col not in load_curve_hourly_schema
        ):
            raise ValueError(
                f"Load curve hourly schema missing columns {consumption_col} or {intensity_col}"
            )
        non_hvac_consumption_cols.append(consumption_col)
        non_hvac_intensity_cols.append(intensity_col)
        ratio = ratios.get(annual_col, 1.0)
        divisor = ratio if ratio > 0 else 1.0
        # Adjust consumption column: divide by MF/SF ratio
        update_exprs.append((pl.col(consumption_col) / divisor).alias(consumption_col))
        sum_parts.append(pl.col(consumption_col) / divisor)
        # Adjust intensity column
        update_exprs.append((pl.col(intensity_col) / divisor).alias(intensity_col))
        sum_parts_intensity.append(pl.col(intensity_col) / divisor)

    old_non_hvac = pl.sum_horizontal([pl.col(c) for c in non_hvac_consumption_cols])
    adjusted_non_hvac = pl.sum_horizontal(sum_parts)
    new_total_consumption = (
        pl.col(HOURLY_TOTAL_ELECTRICITY_CONSUMPTION_COL)
        - old_non_hvac
        + adjusted_non_hvac
    )
    update_exprs.append(
        new_total_consumption.alias(HOURLY_TOTAL_ELECTRICITY_CONSUMPTION_COL)
    )

    old_non_hvac_intensity = pl.sum_horizontal(
        [pl.col(c) for c in non_hvac_intensity_cols]
    )
    adjusted_non_hvac_intensity = pl.sum_horizontal(sum_parts_intensity)
    new_total_intensity = (
        pl.col(HOURLY_TOTAL_ELECTRICITY_INTENSITY_COL)
        - old_non_hvac_intensity
        + adjusted_non_hvac_intensity
    )
    update_exprs.append(
        new_total_intensity.alias(HOURLY_TOTAL_ELECTRICITY_INTENSITY_COL)
    )

    adjusted_hourly = load_curve_hourly.with_columns(update_exprs)
    return adjusted_hourly


def adjust_mf_electricity_parquet(
    metadata: pl.LazyFrame,
    input_load_curve_annual: pl.LazyFrame,
    load_curve_hourly_dir: Path | S3Path,
    path_metadata: Path | S3Path,
    upgrade_id: str = "00",
    storage_options: dict[str, str] | None = None,
) -> None:
    """Load metadata and single annual parquet, apply MF non-HVAC adjustment, write back; then adjust each MF bldg_id's hourly parquet in path_load_curve_hourly.

    path_metadata and load_curve_hourly_dir may be Path (local) or S3Path (s3://).
    storage_options are set automatically from path type (S3 vs local); pass explicitly to override.
    """
    opts = (
        storage_options
        if storage_options is not None
        else _storage_options_for_path(load_curve_hourly_dir)
    )

    meta_schema = metadata.collect_schema().names()
    if BLDG_ID_COL not in meta_schema:
        raise ValueError(f"Metadata LazyFrame missing column {BLDG_ID_COL!r}")
    if MF_NON_HVAC_ELECTRICITY_ADJUSTED_COL not in meta_schema:
        metadata = metadata.with_columns(
            pl.lit(False).alias(MF_NON_HVAC_ELECTRICITY_ADJUSTED_COL)
        )

    schema = input_load_curve_annual.collect_schema().names()
    if (
        BLDG_ID_COL not in schema
        or ANNUAL_ELECTRICITY_COL not in schema
        or any(c not in schema for c in NON_HVAC_RELATED_ELECTRICITY_COLS)
    ):
        raise ValueError(
            f"Load curve annual LazyFrame missing columns {BLDG_ID_COL!r} or {ANNUAL_ELECTRICITY_COL!r} or {NON_HVAC_RELATED_ELECTRICITY_COLS!r}"
        )
    non_hvac_present = [c for c in NON_HVAC_RELATED_ELECTRICITY_COLS if c in schema]
    if not non_hvac_present:
        raise ValueError(
            "Load curve annual LazyFrame has no non-HVAC electricity columns"
        )
    annual_for_ratio = input_load_curve_annual.select(
        [pl.col(BLDG_ID_COL)] + [pl.col(c) for c in non_hvac_present]
    )
    bldg_ids_in_annual = (
        cast(pl.DataFrame, annual_for_ratio.collect()).get_column(BLDG_ID_COL).to_list()
    )
    meta_subset = metadata.filter(pl.col(BLDG_ID_COL).is_in(bldg_ids_in_annual))
    meta_collected = cast(pl.DataFrame, meta_subset.collect())
    if meta_collected.height != len(bldg_ids_in_annual):
        raise ValueError(
            f"Number of metadata rows ({meta_collected.height}) does not match number of bldg_ids in the load curve annual ({len(bldg_ids_in_annual)})"
        )
    ratios = _get_non_hvac_mf_to_sf_ratios(annual_for_ratio, meta_subset)
    print(ratios)
    unadjusted_multifamily_bldg_ids = (
        cast(
            pl.DataFrame,
            meta_subset.filter(
                pl.col(BUILDING_TYPE_RECS_COL).str.contains(
                    "Multi-Family", literal=True
                )
                & ~pl.col(MF_NON_HVAC_ELECTRICITY_ADJUSTED_COL)
            )
            .select(pl.col(BLDG_ID_COL))
            .collect(),
        )
        .get_column(BLDG_ID_COL)
        .to_list()
    )

    def _adjust_and_sink_one_bldg(bldg_id: int) -> int:
        path_hourly = load_curve_hourly_dir / f"{bldg_id}-{int(upgrade_id)}.parquet"
        lf = pl.scan_parquet(str(path_hourly), storage_options=opts)
        adjusted = _adjust_mf_electricity_hourly_one_bldg(lf, ratios)
        adjusted.sink_parquet(str(path_hourly), storage_options=opts)
        return bldg_id

    with ThreadPoolExecutor() as executor:
        adjusted_bldg_ids = list(
            executor.map(_adjust_and_sink_one_bldg, unadjusted_multifamily_bldg_ids)
        )

    if adjusted_bldg_ids:
        updated_metadata = metadata.with_columns(
            pl.when(pl.col(BLDG_ID_COL).is_in(adjusted_bldg_ids))
            .then(True)
            .otherwise(pl.col(MF_NON_HVAC_ELECTRICITY_ADJUSTED_COL))
            .alias(MF_NON_HVAC_ELECTRICITY_ADJUSTED_COL)
        )
        updated_metadata.sink_parquet(str(path_metadata), storage_options=opts)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Apply multifamily non-HVAC adjustment to ResStock load curve annual and hourly parquet."
    )
    parser.add_argument(
        "--path_s3",
        type=str,
        required=True,
        help="S3 base path for ResStock data (e.g. s3://data.sb/nrel/resstock)",
    )
    parser.add_argument(
        "--state", type=str, required=True, help="State abbreviation (e.g. NY)"
    )
    parser.add_argument(
        "--input_release",
        type=str,
        required=True,
        help="Input release name (e.g. res_2024_amy2018_2)",
    )
    parser.add_argument(
        "--output_release",
        type=str,
        required=True,
        help="Output release name for approximated curves (e.g. res_2024_amy2018_2_sb)",
    )
    parser.add_argument(
        "--upgrade_ids",
        type=str,
        required=True,
        help="Space-separated upgrade IDs (e.g. 00 01 02)",
    )
    args = parser.parse_args()

    path_s3 = S3Path(args.path_s3)
    upgrade_ids = args.upgrade_ids.split(" ")
    for upgrade_id in upgrade_ids:
        input_load_curve_annual_path = (
            path_s3
            / args.input_release
            / "load_curve_annual"
            / f"state={args.state}"
            / f"upgrade={upgrade_id}"
            / f"{args.state}_upgrade{upgrade_id}_metadata_and_annual_results.parquet"
        )
        input_load_curve_annual = pl.scan_parquet(
            str(input_load_curve_annual_path), storage_options=STORAGE_OPTIONS
        )

        metadata_path = (
            path_s3
            / args.output_release
            / "metadata"
            / f"state={args.state}"
            / f"upgrade={upgrade_id}"
            / "metadata-sb.parquet"
        )
        metadata = pl.scan_parquet(str(metadata_path), storage_options=STORAGE_OPTIONS)
        load_curve_hourly_dir = (
            path_s3
            / args.output_release
            / "load_curve_hourly"
            / f"state={args.state}"
            / f"upgrade={upgrade_id}"
        )

        adjust_mf_electricity_parquet(
            metadata=metadata,
            input_load_curve_annual=input_load_curve_annual,
            load_curve_hourly_dir=load_curve_hourly_dir,
            path_metadata=metadata_path,
            upgrade_id=upgrade_id,
            storage_options=STORAGE_OPTIONS,
        )
