import argparse
import logging
import warnings
from pathlib import Path
from typing import cast

import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region
from utils.types import ElectricUtility

STORAGE_OPTIONS = {"aws_region": get_aws_region()}

# Small gas utilities we do not model: no tariffs, exclude from analysis. We handle
# them here in the mapper by assigning null_gas_tariff rather than changing utility
# assignment (e.g. re-running assign_utility_* to exclude or reassign them), which is
# simpler and avoids touching polygon/overlap logic. CAIRO then uses the null tariff
# for these buildings.
# NY: bath, chautauqua, corning, fillmore, reserve, stlaw
# MD: easton_muni (also listed under excluded_gas_utilities in state_configs.yaml)
EXCLUDED_GAS_UTILITIES = frozenset(
    {
        "bath",
        "chautauqua",
        "corning",
        "fillmore",
        "reserve",
        "stlaw",
        "easton_muni",
    }
)
# Gas utilities we expect in assignment (IOUs we model + excluded + electric-only that may appear).
# If we see any other gas_utility value, we log a warning so new polygon data or new utilities
# don't slip through unnoticed.
EXPECTED_GAS_UTILITIES = EXCLUDED_GAS_UTILITIES | {
    "coned",
    "kedny",
    "kedli",
    "nimo",
    "nyseg",
    "rie",
    "rge",
    "cenhud",
    "or",
    "nfg",
    "psegli",
    "none",
    # MD
    "bge",
    "columbia_gas_md",
    "ugi_central_penn",
    "washington_gas",
    "chesapeake_utilities",
    "elkton_gas",
    "sandpiper",
}

# Post-merger Chesapeake territory: county group from sb.gas_utility, RES-1/RES-2 from
# annual therms (≤150 → res1, >150 → res2). See context/code/data/md_tariff_fetch.md.
CHESAPEAKE_GAS_UTILITIES = frozenset(
    {"chesapeake_utilities", "elkton_gas", "sandpiper"}
)
CHESAPEAKE_RES1_MAX_THERMS = 150.0
# Match tariff $/therm → $/kWh conversion in split_chesapeake_gas_tariffs / add_pgc_to_gas_urdb.
KWH_PER_THERM = 29.3001
NATGAS_ANNUAL_KWH_COL = "out.natural_gas.total.energy_consumption.kwh"

log = logging.getLogger(__name__)


def load_annual_gas_therms(
    path_load_curve_annual: str | Path | S3Path,
    *,
    storage_options: dict[str, str] | None = None,
) -> pl.LazyFrame:
    """Load bldg_id → annual natural gas use in therms from ResStock annual results.

    ``path_load_curve_annual`` may be a parquet file or a directory of parquets
    (e.g. ``.../load_curve_annual/state=MD/upgrade=00/``). Pass an ``S3Path`` (not
    a bare ``s3://`` string wrapped in ``Path``) for S3 locations — wrapping an
    ``S3Path`` in ``Path(...)`` mangles the URI.
    """
    uri = str(path_load_curve_annual)
    lf = (
        pl.scan_parquet(uri, storage_options=storage_options)
        if storage_options
        else pl.scan_parquet(uri)
    )
    schema_names = lf.collect_schema().names()
    if "bldg_id" not in schema_names or NATGAS_ANNUAL_KWH_COL not in schema_names:
        raise ValueError(
            f"load_curve_annual at {uri} must contain 'bldg_id' and "
            f"'{NATGAS_ANNUAL_KWH_COL}'; found columns: {schema_names[:20]}..."
        )
    return lf.select(
        "bldg_id",
        (pl.col(NATGAS_ANNUAL_KWH_COL) / KWH_PER_THERM).alias("annual_gas_therms"),
    )


def _tariff_key_expr() -> pl.Expr:
    building_type_column = pl.col("in.geometry_building_type_recs")
    is_mf = building_type_column.str.contains("5+", literal=True)
    heats_with_natgas_column = pl.col("heats_with_natgas")
    gas_utility_col = pl.col("sb.gas_utility")

    # Chesapeake: RES-1 (≤150 therms/yr) vs RES-2 (>150 therms/yr)
    chesapeake_is_res1 = pl.col("annual_gas_therms") <= CHESAPEAKE_RES1_MAX_THERMS
    chesapeake_is_res2 = pl.col("annual_gas_therms") > CHESAPEAKE_RES1_MAX_THERMS

    return (
        #### coned ####
        # coned: single non-heating rate; separate heating rates for SF vs MF
        # Non-heating (any building type)
        pl.when((gas_utility_col == "coned") & heats_with_natgas_column.eq(False))
        .then(pl.lit("coned_nonheating"))
        # Heating + SF (1-4 units)
        .when((gas_utility_col == "coned") & heats_with_natgas_column.eq(True) & ~is_mf)
        .then(pl.lit("coned_sf_heating"))
        # Heating + MF (5+ units)
        .when((gas_utility_col == "coned") & heats_with_natgas_column.eq(True) & is_mf)
        .then(pl.lit("coned_mf_heating"))
        #### coned ####
        #### kedny ####
        # kedny: SF gets heating/non-heating; MF gets single rate
        .when((gas_utility_col == "kedny") & is_mf)
        .then(pl.lit("kedny_mf"))
        .when((gas_utility_col == "kedny") & ~is_mf & heats_with_natgas_column.eq(True))
        .then(pl.lit("kedny_sf_heating"))
        .when(
            (gas_utility_col == "kedny") & ~is_mf & heats_with_natgas_column.eq(False)
        )
        .then(pl.lit("kedny_sf_nonheating"))
        #### kedny ####
        #### kedli ####
        # kedli: same as kedny — SF gets heating/non-heating; MF gets single rate
        .when((gas_utility_col == "kedli") & is_mf)
        .then(pl.lit("kedli_mf"))
        .when((gas_utility_col == "kedli") & ~is_mf & heats_with_natgas_column.eq(True))
        .then(pl.lit("kedli_sf_heating"))
        .when(
            (gas_utility_col == "kedli") & ~is_mf & heats_with_natgas_column.eq(False)
        )
        .then(pl.lit("kedli_sf_nonheating"))
        #### kedli ####
        #### nyseg ####
        # nyseg: Heating with natural gas
        .when((gas_utility_col == "nyseg") & heats_with_natgas_column.eq(True))
        .then(pl.concat_str([gas_utility_col, pl.lit("_heating")]))
        # nyseg: Does not heat with natural gas
        .when((gas_utility_col == "nyseg") & heats_with_natgas_column.eq(False))
        .then(pl.concat_str([gas_utility_col, pl.lit("_nonheating")]))
        #### nyseg ####
        #### rie ####
        # rie: Heating with natural gas
        .when((gas_utility_col == "rie") & heats_with_natgas_column.eq(True))
        .then(pl.concat_str([gas_utility_col, pl.lit("_heating")]))
        # rie: Does not heat with natural gas
        .when((gas_utility_col == "rie") & heats_with_natgas_column.eq(False))
        .then(pl.concat_str([gas_utility_col, pl.lit("_nonheating")]))
        #### rie ####
        #### nimo | rge | cenhud | or | nfg ####
        .when(
            (gas_utility_col == "nimo")
            | (gas_utility_col == "rge")
            | (gas_utility_col == "cenhud")
            | (gas_utility_col == "or")
            | (gas_utility_col == "nfg")
        )
        .then(gas_utility_col)
        #### nimo | rge | cenhud | or | nfg ####
        #### MD: bge | columbia_gas_md | ugi_central_penn ####
        # Single residential tariff JSON per utility (stem matches tariff filename).
        .when(gas_utility_col == "bge")
        .then(pl.lit("bge_residential"))
        .when(gas_utility_col == "columbia_gas_md")
        .then(pl.lit("columbia_gas_md_residential"))
        .when(gas_utility_col == "ugi_central_penn")
        .then(pl.lit("ugi_central_penn_residential"))
        #### MD: bge | columbia_gas_md | ugi_central_penn ####
        #### MD: washington_gas ####
        # Heating vs non-heating subclasses (matches RateAcuity / URDB JSON stems).
        .when((gas_utility_col == "washington_gas") & heats_with_natgas_column.eq(True))
        .then(pl.lit("washington_gas_heating"))
        .when(
            (gas_utility_col == "washington_gas") & heats_with_natgas_column.eq(False)
        )
        .then(pl.lit("washington_gas_nonheating"))
        #### MD: washington_gas ####
        #### MD: Chesapeake territory (county + RES-1/RES-2 by annual therms) ####
        .when((gas_utility_col == "chesapeake_utilities") & chesapeake_is_res1)
        .then(pl.lit("chesapeake_main_res1"))
        .when((gas_utility_col == "chesapeake_utilities") & chesapeake_is_res2)
        .then(pl.lit("chesapeake_main_res2"))
        .when((gas_utility_col == "elkton_gas") & chesapeake_is_res1)
        .then(pl.lit("chesapeake_cecil_res1"))
        .when((gas_utility_col == "elkton_gas") & chesapeake_is_res2)
        .then(pl.lit("chesapeake_cecil_res2"))
        .when((gas_utility_col == "sandpiper") & chesapeake_is_res1)
        .then(pl.lit("chesapeake_worcester_res1"))
        .when((gas_utility_col == "sandpiper") & chesapeake_is_res2)
        .then(pl.lit("chesapeake_worcester_res2"))
        #### MD: Chesapeake territory ####
        ### Null value in the gas_utility column gets assigned to "null_gas_tariff" ####
        .when(gas_utility_col.is_null())
        .then(pl.lit("null_gas_tariff"))
        ### Null value in the gas_utility column gets assigned to "null_gas_tariff" ####
        ### Small / excluded utilities: assign null_gas_tariff (no placeholder tariffs). ###
        .when(gas_utility_col.is_in(list(EXCLUDED_GAS_UTILITIES)))
        .then(pl.lit("null_gas_tariff"))
        ### Small utilities ###
        # Default: passthrough utility code for any other gas utility
        .otherwise(gas_utility_col)
        .fill_null(gas_utility_col)
        .alias("tariff_key")
    )


def map_gas_tariff(
    SB_metadata: pl.LazyFrame,
    electric_utility_name: ElectricUtility,
    annual_gas_therms: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """Map buildings for one electric utility to gas ``tariff_key`` values.

    Args:
        SB_metadata: Metadata with ``bldg_id``, ``sb.electric_utility``,
            ``sb.gas_utility``, ``in.geometry_building_type_recs``,
            ``heats_with_natgas``.
        electric_utility_name: Keep only buildings with this electric utility.
        annual_gas_therms: Optional LazyFrame with ``bldg_id`` and
            ``annual_gas_therms``. Required when any retained building has
            ``sb.gas_utility`` in :data:`CHESAPEAKE_GAS_UTILITIES` (RES-1/RES-2
            split). Use :func:`load_annual_gas_therms` to build it from
            ResStock ``load_curve_annual``.
    """
    # Filter metadata by electric utility name
    utility_metadata = SB_metadata.filter(
        pl.col("sb.electric_utility") == electric_utility_name
    )

    # Check if there are any rows in the filtered dataframe
    test_sample = cast(pl.DataFrame, utility_metadata.head(1).collect())
    if test_sample.is_empty():
        return pl.LazyFrame()

    # Log if we see any gas_utility not in expected set (IOUs + small + none/psegli)
    distinct_gas = cast(
        pl.DataFrame,
        utility_metadata.select("sb.gas_utility").unique().collect(),
    )
    distinct_gas_vals = {
        row["sb.gas_utility"] for row in distinct_gas.iter_rows(named=True)
    }
    for val in distinct_gas_vals:
        if val is not None and val not in EXPECTED_GAS_UTILITIES:
            log.warning(
                "Gas tariff mapper saw unexpected gas_utility %r (electric_utility=%s); "
                "expected only utilities in EXPECTED_GAS_UTILITIES "
                "(modeled IOUs + EXCLUDED_GAS_UTILITIES + none/psegli).",
                val,
                electric_utility_name,
            )

    needs_chesapeake_therms = bool(distinct_gas_vals & CHESAPEAKE_GAS_UTILITIES)
    if needs_chesapeake_therms and annual_gas_therms is None:
        raise ValueError(
            "Chesapeake-territory gas utilities "
            f"({sorted(distinct_gas_vals & CHESAPEAKE_GAS_UTILITIES)}) require "
            "annual_gas_therms (from load_curve_annual) for RES-1/RES-2 mapping. "
            "Pass --path_load_curve_annual or annual_gas_therms=..."
        )

    selected = utility_metadata.select(
        pl.col(
            "bldg_id",
            "sb.gas_utility",
            "in.geometry_building_type_recs",
            "heats_with_natgas",
        )
    )
    if annual_gas_therms is not None:
        selected = selected.join(annual_gas_therms, on="bldg_id", how="left")
    else:
        selected = selected.with_columns(
            pl.lit(None).cast(pl.Float64).alias("annual_gas_therms")
        )

    if needs_chesapeake_therms:
        missing = cast(
            pl.DataFrame,
            selected.filter(
                pl.col("sb.gas_utility").is_in(list(CHESAPEAKE_GAS_UTILITIES))
                & pl.col("annual_gas_therms").is_null()
            )
            .select(pl.len())
            .collect(),
        ).item()
        if missing > 0:
            raise ValueError(
                f"{missing} Chesapeake-territory building(s) lack annual_gas_therms "
                "after joining load_curve_annual; check path and upgrade_id."
            )

    gas_tariff_mapping_df = selected.with_columns(_tariff_key_expr()).drop(
        "sb.gas_utility",
        "in.geometry_building_type_recs",
        "heats_with_natgas",
        "annual_gas_therms",
    )

    return gas_tariff_mapping_df


def _default_path_load_curve_annual(
    metadata_path: str | Path, state: str, upgrade_id: str
) -> Path | None:
    """Prefer raw-release annual loads next to an ``*_sb`` metadata root.

    ``load_curve_annual`` is often incomplete under ``*_sb`` (sample-sized);
    the full population lives under the matching raw release directory.
    """
    meta = Path(metadata_path)
    # metadata_path is typically .../res_..._sb/metadata
    release = meta.parent if meta.name == "metadata" else meta
    candidates: list[Path] = []
    if release.name.endswith("_sb"):
        candidates.append(Path(str(release)[: -len("_sb")]))
    candidates.append(release)
    for root in candidates:
        annual_dir = (
            root / "load_curve_annual" / f"state={state}" / f"upgrade={upgrade_id}"
        )
        if annual_dir.exists():
            return annual_dir
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    warnings.simplefilter("always", DeprecationWarning)
    parser = argparse.ArgumentParser(
        description="Utility to help assign gas tariffs to utility customers."
    )
    parser.add_argument(
        "--metadata_path",
        required=True,
        help="Absolute or s3 path to ResStock metadata",
    )
    parser.add_argument(
        "--utility_assignment_path",
        required=True,
        help="Absolute or s3 path to ResStock utility assignment",
    )
    parser.add_argument("--state", required=True, help="State code (e.g. NY, RI)")
    parser.add_argument("--upgrade_id", required=True, help="Upgrade id (e.g. 00)")
    parser.add_argument(
        "--electric_utility",
        required=True,
        help="Electric utility std_name (e.g. coned, nyseg, nimo)",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Output directory for output CSV",
    )
    parser.add_argument(
        "--path_load_curve_annual",
        default=None,
        help=(
            "Path to ResStock load_curve_annual parquet file or directory "
            "(state=/upgrade= hive folder). Required for Chesapeake RES-1/RES-2 "
            "mapping when those gas utilities appear. If omitted, tries the raw "
            "release sibling of an *_sb metadata root."
        ),
    )
    args = parser.parse_args()

    try:  # If the metadata path is an S3 path, use the S3Path class.
        base_path = S3Path(args.metadata_path)
        use_s3_metadata = True
    except ValueError:
        base_path = Path(args.metadata_path)
        use_s3_metadata = False

    try:  # If the utility assignment path is an S3 path, use the S3Path class.
        utility_assignment_base_path = S3Path(args.utility_assignment_path)
        use_s3_utility_assignment = True
    except ValueError:
        utility_assignment_base_path = Path(args.utility_assignment_path)
        use_s3_utility_assignment = False

    # Support metadata_utility path (utility_assignment.parquet) or metadata path (metadata-sb.parquet)
    if "metadata_utility" in str(args.utility_assignment_path):
        utility_assignment_path = (
            utility_assignment_base_path
            / f"state={args.state}"
            / "utility_assignment.parquet"
        )
    else:
        utility_assignment_path = (
            utility_assignment_base_path
            / f"state={args.state}"
            / f"upgrade={args.upgrade_id}"
            / "metadata-sb.parquet"
        )
    metadata_path = (
        base_path
        / f"state={args.state}"
        / f"upgrade={args.upgrade_id}"
        / "metadata-sb.parquet"
    )

    if use_s3_metadata and not metadata_path.exists():
        raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")
    if not use_s3_metadata and not Path(metadata_path).exists():
        raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")
    if use_s3_utility_assignment and not utility_assignment_path.exists():
        raise FileNotFoundError(
            f"Utility assignment path {utility_assignment_path} does not exist"
        )
    if not use_s3_utility_assignment and not Path(utility_assignment_path).exists():
        raise FileNotFoundError(
            f"Utility assignment path {utility_assignment_path} does not exist"
        )

    storage_opts_metadata = STORAGE_OPTIONS if use_s3_metadata else None
    storage_opts_utility_assignment = (
        STORAGE_OPTIONS if use_s3_utility_assignment else None
    )
    SB_metadata = (
        pl.scan_parquet(str(metadata_path), storage_options=storage_opts_metadata)
        if storage_opts_metadata
        else pl.scan_parquet(str(metadata_path))
    )
    utility_assignment = (
        pl.scan_parquet(
            str(utility_assignment_path),
            storage_options=storage_opts_utility_assignment,
        )
        if storage_opts_utility_assignment
        else pl.scan_parquet(str(utility_assignment_path))
    )

    # Use real sb.electric_utility and sb.gas_utility if present; else fall back to synthetic (deprecated)
    utility_assignment_schema_cols = utility_assignment.collect_schema().names()
    if (
        "sb.electric_utility" in utility_assignment_schema_cols
        and "sb.gas_utility" in utility_assignment_schema_cols
    ):
        # Verify all metadata bldg_ids are covered by the utility assignment.
        # The assignment is always derived from upgrade=00, so for non-zero upgrades
        # (e.g. upgrade=02) the metadata may be a strict subset of the assignment
        # (buildings without a valid HP scenario are dropped). Extra bldg_ids in the
        # assignment that have no matching metadata row are harmlessly filtered by
        # the inner join below.
        metadata_bldg_ids_df = cast(
            pl.DataFrame,
            SB_metadata.select("bldg_id").unique().collect(),
        )
        metadata_bldg_ids = set(metadata_bldg_ids_df["bldg_id"].to_list())

        utility_bldg_ids_df = cast(
            pl.DataFrame,
            utility_assignment.select("bldg_id").unique().collect(),
        )
        utility_bldg_ids = set(utility_bldg_ids_df["bldg_id"].to_list())

        missing_from_assignment = metadata_bldg_ids - utility_bldg_ids
        if missing_from_assignment:
            raise ValueError(
                f"{len(missing_from_assignment)} metadata bldg_id(s) have no entry in "
                f"utility_assignment (first 5: {sorted(missing_from_assignment)[:5]}). "
                "Re-run assign_utility to include all buildings."
            )

        # Inner join ensures one-to-one matching; will fail if bldg_ids don't match
        SB_metadata_with_utilities = SB_metadata.join(
            utility_assignment.select(
                "bldg_id", "sb.electric_utility", "sb.gas_utility"
            ),
            on="bldg_id",
            how="inner",
        )

        # Verify join preserved all rows (catches duplicates or other issues)
        metadata_count_df = cast(
            pl.DataFrame,
            SB_metadata.select(pl.len()).collect(),
        )
        metadata_count = metadata_count_df.row(0)[0]

        joined_count_df = cast(
            pl.DataFrame,
            SB_metadata_with_utilities.select(pl.len()).collect(),
        )
        joined_count = joined_count_df.row(0)[0]
        if metadata_count != joined_count:
            raise ValueError(
                f"Join failed: metadata has {metadata_count} rows but inner join produced {joined_count} rows. "
                f"This indicates duplicate bldg_ids or non-matching values."
            )
    else:
        warnings.warn(
            "metadata has no sb.electric_utility/sb.gas_utility columns; using synthetic data. "
            "Run assign_utility_ny (data/resstock/utility/) and point --metadata_path to metadata_utility for real data.",
            DeprecationWarning,
            stacklevel=2,
        )
        SB_metadata_with_utilities = SB_metadata.with_columns(
            pl.when(pl.col("bldg_id").hash() % 3 == 0)
            .then(pl.lit("coned"))
            .when(pl.col("bldg_id").hash() % 3 == 1)
            .then(pl.lit("nimo"))
            .otherwise(pl.lit("nyseg"))
            .alias("sb.electric_utility"),
            pl.when((pl.col("bldg_id").hash() % 2) == 0)
            .then(pl.lit("nimo"))
            .otherwise(pl.lit("nyseg"))
            .alias("sb.gas_utility"),
        )

    annual_gas_therms_lf: pl.LazyFrame | None = None
    annual_path_arg = args.path_load_curve_annual
    if annual_path_arg is None:
        derived = _default_path_load_curve_annual(
            args.metadata_path, args.state, args.upgrade_id
        )
        if derived is not None:
            annual_path_arg = str(derived)
            log.info("Using load_curve_annual at %s", annual_path_arg)
    if annual_path_arg is not None:
        try:
            annual_s3 = S3Path(annual_path_arg)
            use_s3_annual = True
            annual_path_resolved = annual_s3
        except ValueError:
            use_s3_annual = False
            annual_path_resolved = Path(annual_path_arg)
        if use_s3_annual and not annual_path_resolved.exists():
            raise FileNotFoundError(
                f"load_curve_annual path {annual_path_resolved} does not exist"
            )
        if not use_s3_annual and not Path(annual_path_resolved).exists():
            raise FileNotFoundError(
                f"load_curve_annual path {annual_path_resolved} does not exist"
            )
        annual_gas_therms_lf = load_annual_gas_therms(
            annual_path_resolved,
            storage_options=STORAGE_OPTIONS if use_s3_annual else None,
        )

    gas_tariff_mapping_df = map_gas_tariff(
        SB_metadata=SB_metadata_with_utilities,
        electric_utility_name=args.electric_utility,
        annual_gas_therms=annual_gas_therms_lf,
    )
    # Check if the result has any rows before writing. If there are no rows assigned to the electric utility, empty lazyframe is returned.
    row_count_df = cast(
        pl.DataFrame,
        gas_tariff_mapping_df.select(pl.len()).collect(),
    )
    row_count = row_count_df.row(0)[0]
    if row_count == 0:
        warnings.warn(
            f"No rows found for electric utility {args.electric_utility}.",
            DeprecationWarning,
            stacklevel=2,
        )
    else:
        output_filename = f"{args.electric_utility}_u{args.upgrade_id}.csv"
        try:
            out_base = S3Path(args.output_dir)
            output_path = out_base / output_filename
            if not output_path.parent.exists():
                output_path.parent.mkdir(parents=True)
            gas_tariff_mapping_df.sink_csv(
                str(output_path), storage_options=STORAGE_OPTIONS
            )
        except ValueError:
            out_base = Path(args.output_dir)
            output_path = out_base / output_filename
            if not output_path.parent.exists():
                out_base.mkdir(parents=True, exist_ok=True)
            gas_tariff_mapping_df.sink_csv(str(output_path))
