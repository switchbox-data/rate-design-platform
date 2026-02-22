"""
Validate gas tariff maps for RI and NY against source metadata and utility assignment.

For each electric utility with a gas tariff map CSV we check:
1. Coverage: every bldg_id assigned to that electric utility is in the map; no extra bldg_ids.
2. Correctness: tariff_key matches the mapper logic from sb.gas_utility, heats_with_natgas,
   in.geometry_building_type_recs, and in.geometry_stories_low_rise (coned: SF/MF/low/high;
   kedny/kedli: SF/MF + heating/nonheating; nyseg/rie: heating/nonheating only; nimo/rge/cenhud/or/nfg: passthrough;
   null/small → null_gas_tariff).

Reads from S3 (utility assignment, metadata). Run from project root:
  uv run python scripts/validate_gas_tariff_maps.py
"""

from pathlib import Path
from typing import cast

import polars as pl

from utils import get_aws_region

STORAGE_OPTIONS = {"aws_region": get_aws_region()}
RELEASE = "res_2024_amy2018_2"
UPGRADE = "00"
S3_BASE = f"s3://data.sb/nrel/resstock/{RELEASE}"

# Mirror gas_tariff_mapper.py
SMALL_GAS_UTILITIES = frozenset(
    {"bath", "chautauqua", "corning", "fillmore", "reserve", "stlaw"}
)


def expected_tariff_key_expr() -> pl.Expr:
    """Replicate _tariff_key_expr from utils/pre/gas_tariff_mapper.py (same when/then order)."""
    building_type_column = pl.col("in.geometry_building_type_recs")
    stories_column = pl.col("in.geometry_stories_low_rise")
    heats_with_natgas_column = pl.col("heats_with_natgas")
    gas_utility_col = pl.col("sb.gas_utility")

    return (
        pl.when(
            (gas_utility_col == "coned")
            & building_type_column.str.contains("Single-Family", literal=True)
        )
        .then(pl.concat_str([gas_utility_col, pl.lit("_sf")]))
        .when(
            (gas_utility_col == "coned")
            & building_type_column.str.contains("Multi-Family", literal=True)
            & stories_column.str.contains("4+", literal=True)
        )
        .then(pl.concat_str([gas_utility_col, pl.lit("_mf_highrise")]))
        .when(
            (gas_utility_col == "coned")
            & building_type_column.str.contains("Multi-Family", literal=True)
        )
        .then(pl.concat_str([gas_utility_col, pl.lit("_mf_lowrise")]))
        .when(
            (gas_utility_col == "kedny")
            & building_type_column.str.contains("Single-Family", literal=True)
            & heats_with_natgas_column.eq(True)
        )
        .then(pl.concat_str([gas_utility_col, pl.lit("_sf_heating")]))
        .when(
            (gas_utility_col == "kedny")
            & building_type_column.str.contains("Single-Family", literal=True)
            & heats_with_natgas_column.eq(False)
        )
        .then(pl.concat_str([gas_utility_col, pl.lit("_sf_nonheating")]))
        .when(
            (gas_utility_col == "kedny")
            & building_type_column.str.contains("Multi-Family", literal=True)
        )
        .then(pl.concat_str([gas_utility_col, pl.lit("_mf_heating")]))
        .when(
            (gas_utility_col == "kedli")
            & building_type_column.str.contains("Single-Family", literal=True)
            & heats_with_natgas_column.eq(True)
        )
        .then(pl.concat_str([gas_utility_col, pl.lit("_sf_heating")]))
        .when(
            (gas_utility_col == "kedli")
            & building_type_column.str.contains("Single-Family", literal=True)
            & heats_with_natgas_column.eq(False)
        )
        .then(pl.concat_str([gas_utility_col, pl.lit("_sf_nonheating")]))
        .when(
            (gas_utility_col == "kedli")
            & building_type_column.str.contains("Multi-Family", literal=True)
            & heats_with_natgas_column.eq(True)
        )
        .then(pl.concat_str([gas_utility_col, pl.lit("_mf_heating")]))
        .when(
            (gas_utility_col == "kedli")
            & building_type_column.str.contains("Multi-Family", literal=True)
            & heats_with_natgas_column.eq(False)
        )
        .then(pl.concat_str([gas_utility_col, pl.lit("_mf_nonheating")]))
        .when((gas_utility_col == "nyseg") & heats_with_natgas_column.eq(True))
        .then(pl.concat_str([gas_utility_col, pl.lit("_heating")]))
        .when((gas_utility_col == "nyseg") & heats_with_natgas_column.eq(False))
        .then(pl.concat_str([gas_utility_col, pl.lit("_nonheating")]))
        .when((gas_utility_col == "rie") & heats_with_natgas_column.eq(True))
        .then(pl.concat_str([gas_utility_col, pl.lit("_heating")]))
        .when((gas_utility_col == "rie") & heats_with_natgas_column.eq(False))
        .then(pl.concat_str([gas_utility_col, pl.lit("_nonheating")]))
        .when(
            (gas_utility_col == "nimo")
            | (gas_utility_col == "rge")
            | (gas_utility_col == "cenhud")
            | (gas_utility_col == "or")
            | (gas_utility_col == "nfg")
        )
        .then(gas_utility_col)
        .when(gas_utility_col.is_null())
        .then(pl.lit("null_gas_tariff"))
        .when(gas_utility_col.is_in(list(SMALL_GAS_UTILITIES)))
        .then(pl.lit("null_gas_tariff"))
        .otherwise(gas_utility_col)
        .fill_null(gas_utility_col)
        .alias("expected_tariff_key")
    )


def path_utility_assignment(state: str) -> str:
    return f"{S3_BASE}/metadata_utility/state={state}/utility_assignment.parquet"


def path_metadata(state: str) -> str:
    return f"{S3_BASE}/metadata/state={state}/upgrade={UPGRADE}/metadata-sb.parquet"


def load_joined(state: str) -> pl.DataFrame:
    """Load utility assignment + metadata joined on bldg_id; columns needed for expected_tariff_key."""
    ua = pl.scan_parquet(
        path_utility_assignment(state), storage_options=STORAGE_OPTIONS
    ).select("bldg_id", "sb.electric_utility", "sb.gas_utility")
    meta = pl.scan_parquet(
        path_metadata(state), storage_options=STORAGE_OPTIONS
    ).select(
        "bldg_id",
        "heats_with_natgas",
        "in.geometry_building_type_recs",
        "in.geometry_stories_low_rise",
    )
    joined = ua.join(meta, on="bldg_id", how="inner")
    return cast(pl.DataFrame, joined.with_columns(expected_tariff_key_expr()).collect())


def validate_one(
    electric_utility: str,
    path_csv: Path,
    joined: pl.DataFrame,
) -> bool:
    """Validate one gas tariff map CSV. Returns True if all checks pass."""
    electric_bldgs = joined.filter(
        pl.col("sb.electric_utility") == electric_utility
    ).select("bldg_id", "expected_tariff_key")
    expected_ids = set(electric_bldgs["bldg_id"].to_list())

    if not path_csv.exists():
        print(f"  SKIP: {path_csv} not found")
        return True

    map_df = pl.read_csv(path_csv)
    map_ids = set(map_df["bldg_id"].to_list())

    missing = expected_ids - map_ids
    extra = map_ids - expected_ids
    ok_coverage = len(missing) == 0 and len(extra) == 0

    print(
        f"  Coverage: expected {len(expected_ids)} bldg_ids in map, got {len(map_ids)}"
    )
    if missing:
        print(f"    FAIL: {len(missing)} missing (sample: {list(missing)[:3]})")
    if extra:
        print(f"    FAIL: {len(extra)} extra (sample: {list(extra)[:3]})")
    if ok_coverage:
        print("    OK: no missing or extra bldg_ids")

    # Correctness: join map to joined data for this electric utility; compare tariff_key to expected_tariff_key
    combined = map_df.join(electric_bldgs, on="bldg_id", how="inner")
    mismatches = combined.filter(pl.col("tariff_key") != pl.col("expected_tariff_key"))
    n_mismatch = mismatches.height
    ok_correct = n_mismatch == 0

    if n_mismatch > 0:
        print(f"  Correctness: FAIL — {n_mismatch} rows with wrong tariff_key")
        print(
            mismatches.select("bldg_id", "tariff_key", "expected_tariff_key").head(10)
        )
    else:
        print(
            "  Correctness: OK — all tariff_key match expected (gas_utility + heating/geometry rules)"
        )

    return ok_coverage and ok_correct


def main() -> None:
    path_repo = Path(__file__).resolve().parent.parent

    # RI: single utility rie
    ri_gas_dir = path_repo / "rate_design/ri/hp_rates/config/tariff_maps/gas"
    # NY: utilities that have gas map CSVs
    ny_gas_dir = path_repo / "rate_design/ny/hp_rates/config/tariff_maps/gas"
    ny_utilities = ["coned", "cenhud", "nimo", "nyseg", "or", "psegli", "rge"]

    all_pass = True

    # --- RI ---
    print("=" * 60)
    print("RI (electric_utility = rie)")
    print("=" * 60)
    joined_ri = load_joined("RI")
    ok_ri = validate_one("rie", ri_gas_dir / "rie.csv", joined_ri)
    if not ok_ri:
        all_pass = False
    print()

    # --- NY ---
    print("=" * 60)
    print("NY (electric utilities with gas map CSVs)")
    print("=" * 60)
    joined_ny = load_joined("NY")
    for util in ny_utilities:
        print(f"--- {util} ---")
        ok = validate_one(util, ny_gas_dir / f"{util}.csv", joined_ny)
        if not ok:
            all_pass = False
        print()

    print("=" * 60)
    print("Overall:", "PASS" if all_pass else "FAIL")
    if not all_pass:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
