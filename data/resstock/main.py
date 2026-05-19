"""Orchestration entry point for the ResStock data pipeline.

Runs three phases for each requested state:
  1. Fetch  — download raw ResStock parquet files to the local EBS mount via bsf.
  2. Modify — apply Switchbox-specific metadata transformations (HP customers,
              heating type, natural gas connection, vulnerability columns).
              All four transforms are on by default; pass False to skip any.
  3. Upload — sync the modified local files to the corresponding S3 path.

Metadata transforms run in dependency order:
  identify_hp_customers → identify_heating_type → identify_natgas_connection
  → add_vulnerability_columns (NY only)

Usage::

    uv run python -m data.resstock.main --state NY
    uv run python -m data.resstock.main --state NY RI
    uv run python -m data.resstock.main --state RI --path-output-dir /data.sb/nrel/resstock
    uv run python -m data.resstock.main --state NY --file-types metadata load_curve_hourly load_curve_annual
    uv run python -m data.resstock.main --state NY --upgrade-ids 0 2
    uv run python -m data.resstock.main --state NY --identify-hp-customers False
    uv run python -m data.resstock.main --state NY --identify-natgas-connection False
    uv run python -m data.resstock.main --state RI --add-vulnerability-columns False
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import cast

import polars as pl
import yaml

from data.resstock import fetch_resstock_data
from data.resstock.adjust_mf_electricity import (
    BUILDING_TYPE_RECS_COL,
    adjust_mf_electricity_parquet,
)
from data.resstock.assign_utility import (
    SUPPORTED_UTILITY_STATES,
    assign_utility_ny,
    assign_utility_ri,
    read_csv_to_gdf_from_s3,
)
from data.resstock.approximate_non_hp_load import (
    _find_nearest_neighbors,
    _identify_non_hp_mf,
    _identify_other_fuel_types,
    update_load_curve_hourly,
    update_metadata as update_non_hp_metadata,
)
from data.resstock.add_monthly_loads import load_aggregation_rules, process_upgrade
from data.resstock.add_vulnerability_columns import (
    add_vulnerability_columns,
    load_puma_conditional_probs,
)
from data.resstock.constants import (
    HEATING_TYPE_COLS,
    HP_CUSTOMERS_COLS,
    NATGAS_CONNECTION_COLS,
    VULNERABILITY_COLS,
)
from data.resstock.copy_resstock_data import copy_dir
from data.resstock.identify_heating_type import identify_heating_type
from data.resstock.identify_hp_customers import identify_hp_customers
from data.resstock.identify_natgas_connection import identify_natgas_connection
from data.resstock.manifest import (
    fail_run,
    finish_run,
    new_run_record,
    record_step,
    upsert_run,
    write_manifest,
)
from data.resstock.validations import (
    validate_local_files,
    validate_metadata_columns,
    validate_metadata_output,
    validate_metadata_readable,
    validate_s3_objects,
)


def _parse_bool(v: str) -> bool:
    if v.lower() in ("true", "1", "yes"):
        return True
    if v.lower() in ("false", "0", "no"):
        return False
    raise argparse.ArgumentTypeError(f"Expected a boolean (True/False), got '{v}'")


# ── Defaults from data/resstock/config.yaml ───────────────────────────────────

_CONFIG_PATH = Path(__file__).parent / "config.yaml"

with _CONFIG_PATH.open() as _f:
    _cfg = yaml.safe_load(_f)

_DEFAULT_OUTPUT_DIR: str = _cfg["paths"]["output_dir"]
_DEFAULT_S3_DIR: str = _cfg["paths"]["s3_dir"]
_DEFAULT_S3_PUMS_DIR: str = _cfg["paths"]["s3_pums_dir"]
_DEFAULT_RELEASE_YEAR: int = _cfg["resstock"]["release_year"]
_DEFAULT_WEATHER_FILE: str = _cfg["resstock"]["weather_file"]
_DEFAULT_RELEASE_VERSION: int = _cfg["resstock"]["release_version"]
_DEFAULT_UPGRADE_IDS: list[str] = _cfg["resstock"]["upgrade_ids"]
_DEFAULT_FILE_TYPES: list[str] = _cfg["resstock"]["file_types"]
_DEFAULT_PUMS_SURVEY: str = _cfg["pums"]["survey"]
_DEFAULT_PUMS_YEAR: str = str(_cfg["pums"]["year"])


def _approximate_non_hp_load(
    *,
    states: list[str],
    path_sb: Path,
    sample: int,
) -> None:
    """Approximate non-HP load curves for upgrade 02 via k-nearest-neighbor HVAC
    substitution.  For each state, identifies non-HP multifamily and other-fuel-type
    buildings, finds their k nearest HP neighbors by heating-load RMSE, replaces their
    HVAC hourly columns with the neighbor average, and updates metadata accordingly.

    When ``sample > 0`` only a subset of buildings is available locally (bsf filters
    both metadata and load curves to the sampled set).  The neighbor search is limited
    to those sampled buildings.  This is acceptable because ``--sample`` is a
    development/testing feature; run without ``--sample`` for production use.
    """
    upgrade = "02"
    for s in states:
        loc = f"state={s} upgrade={upgrade}"
        local_lc_dir = (
            path_sb / "load_curve_hourly" / f"state={s}" / f"upgrade={upgrade}"
        )
        metadata_path = (
            path_sb
            / "metadata"
            / f"state={s}"
            / f"upgrade={upgrade}"
            / "metadata-sb.parquet"
        )
        if not metadata_path.exists():
            print(f"  WARNING: {metadata_path} not found, skipping {loc}.", flush=True)
            continue
        if not local_lc_dir.exists():
            print(f"  WARNING: {local_lc_dir} not found, skipping {loc}.", flush=True)
            continue

        print(f"  Processing {loc}...", flush=True)
        metadata = pl.scan_parquet(str(metadata_path))

        # When --sample > 0, bsf filters both metadata and load curves to the
        # sampled subset.  Restrict targets to buildings whose parquet exists
        # locally (defensive — in practice bsf keeps them in sync).
        local_bldg_ids: set[int] | None = None
        if sample > 0:
            local_bldg_ids = {
                int(p.stem.split("-")[0])
                for p in local_lc_dir.iterdir()
                if p.suffix == ".parquet"
            }
            print(
                f"    WARNING: --sample active — neighbor pool is limited to the "
                f"{len(local_bldg_ids)} locally downloaded buildings. "
                f"Run without --sample for production use.",
                flush=True,
            )

        non_hp_parts = [
            _identify_non_hp_mf(metadata),
            _identify_other_fuel_types(metadata),
        ]
        non_hp_bldg_metadata = pl.concat(non_hp_parts).unique("bldg_id")

        if local_bldg_ids is not None:
            non_hp_bldg_metadata = non_hp_bldg_metadata.filter(
                pl.col("bldg_id").is_in(list(local_bldg_ids))
            )

        n_targets = cast(pl.DataFrame, non_hp_bldg_metadata.collect()).height
        if n_targets == 0:
            print("    No non-HP target buildings, skipping.", flush=True)
            continue
        print(f"    {n_targets} non-HP target buildings.", flush=True)

        neighbor_map = _find_nearest_neighbors(
            metadata,
            non_hp_bldg_metadata,
            local_lc_dir,
            upgrade,
            k=15,
            include_cooling=False,
        )

        # Log neighbor-search summary.
        neighbor_counts = [len(v) for v in neighbor_map.values()]
        n_no_neighbors = sum(1 for c in neighbor_counts if c == 0)
        if neighbor_counts:
            avg_n = sum(neighbor_counts) / len(neighbor_counts)
            min_n = min(neighbor_counts)
            max_n = max(neighbor_counts)
            print(
                f"    Neighbor search complete: {len(neighbor_map)} targets, "
                f"neighbors per target: min={min_n} avg={avg_n:.1f} max={max_n}.",
                flush=True,
            )
        if n_no_neighbors:
            print(
                f"    WARNING: {n_no_neighbors} target(s) found no neighbors "
                f"and will be skipped.",
                flush=True,
            )

        natural_gas_usage = update_load_curve_hourly(
            neighbor_map,
            local_lc_dir,
            local_lc_dir,
            upgrade,
        )

        # Collect + write_parquet (not sink_parquet) because the scan source and
        # output target are the same file.
        updated_metadata = update_non_hp_metadata(
            non_hp_bldg_metadata,
            metadata,
            natural_gas_usage=natural_gas_usage,
        )
        cast(pl.DataFrame, updated_metadata.collect()).write_parquet(str(metadata_path))
        print(
            f"    Done: updated {n_targets} buildings in {metadata_path.name}.",
            flush=True,
        )


# Upgrades that the MF electricity adjustment applies to.
_MF_ADJ_UPGRADES: list[str] = ["00", "02"]

# Upgrade used as input for utility assignment (assignment is per-state, not per-upgrade).
_UTILITY_ASSIGN_UPGRADE: str = "00"

# Default S3 directory for NY utility polygon CSVs.
_DEFAULT_S3_GIS_DIR: str = "s3://data.sb/gis/utility_boundaries/"


def _adjust_mf_electricity(
    *,
    states: list[str],
    path_raw: Path,
    path_sb: Path,
    upgrade_ids: list[str],
    sample: int,
) -> list[str]:
    """Apply MF non-HVAC electricity adjustment to the _sb release.

    For each (state, upgrade) combination in _MF_ADJ_UPGRADES, reads the
    full-population MF/SF electricity ratios from the raw ``load_curve_annual``
    (which is never copied to ``_sb``), then scales non-HVAC columns in each
    multifamily building's hourly parquet in ``path_sb``.

    Must run AFTER ``_approximate_non_hp_load`` so that approximated buildings'
    load curves are already present in ``_sb`` before the ratio is computed.

    Returns a list of zero-padded upgrade IDs that were actually processed.

    When ``--sample > 0``:
    - The MF/SF ratio is derived from only the sampled buildings, which may not
      represent the full state population.  A warning is printed.
    - If the sample contains no MF buildings, the step is skipped for that
      state/upgrade (nothing to adjust).
    - If the sample contains fewer than 2 SF buildings, the ratio computation
      defaults to 1.0 for all columns (effectively a no-op).  A warning is
      printed and the step proceeds so the pipeline does not halt.
    """
    padded_requested = [u.zfill(2) for u in upgrade_ids]
    active_upgrades = [uid for uid in _MF_ADJ_UPGRADES if uid in padded_requested]
    processed_upgrades: list[str] = []

    for s in states:
        for uid in active_upgrades:
            loc = f"state={s} upgrade={uid}"
            metadata_path = (
                path_sb
                / "metadata"
                / f"state={s}"
                / f"upgrade={uid}"
                / "metadata-sb.parquet"
            )
            lc_hourly_dir = (
                path_sb / "load_curve_hourly" / f"state={s}" / f"upgrade={uid}"
            )
            lca_dir = path_raw / "load_curve_annual" / f"state={s}" / f"upgrade={uid}"

            if not metadata_path.exists():
                print(
                    f"  WARNING: {metadata_path} not found, skipping {loc}.",
                    flush=True,
                )
                continue
            if not lc_hourly_dir.exists():
                print(
                    f"  WARNING: {lc_hourly_dir} not found, skipping {loc}.",
                    flush=True,
                )
                continue
            if not lca_dir.exists():
                print(
                    f"  WARNING: load_curve_annual not found at {lca_dir}, "
                    f"skipping {loc}. "
                    f"Ensure 'load_curve_annual' is in --file-types.",
                    flush=True,
                )
                continue

            print(f"  Processing {loc}...", flush=True)
            metadata = pl.scan_parquet(str(metadata_path))

            if sample > 0:
                print(
                    "    WARNING: --sample active — MF/SF electricity ratios are "
                    "computed from only the sampled buildings and may not represent "
                    "the full state population. "
                    "Run without --sample for production use.",
                    flush=True,
                )
                n_mf = cast(
                    pl.DataFrame,
                    metadata.filter(
                        pl.col(BUILDING_TYPE_RECS_COL).str.contains(
                            "Multi-Family", literal=True
                        )
                    )
                    .select(pl.len())
                    .collect(),
                ).item()
                n_sf = cast(
                    pl.DataFrame,
                    metadata.filter(
                        pl.col(BUILDING_TYPE_RECS_COL).str.contains(
                            "Single-Family", literal=True
                        )
                    )
                    .select(pl.len())
                    .collect(),
                ).item()
                if n_mf == 0:
                    print(
                        f"    WARNING: No MF buildings in sample for {loc} — "
                        f"skipping MF adjustment (nothing to adjust).",
                        flush=True,
                    )
                    continue
                if n_sf < 2:
                    print(
                        f"    WARNING: Fewer than 2 SF buildings in sample for "
                        f"{loc} ({n_sf} found) — MF/SF ratios will default to 1.0 "
                        f"(no scaling applied). Proceeding.",
                        flush=True,
                    )

            input_lca = pl.scan_parquet(str(lca_dir))
            adjust_mf_electricity_parquet(
                metadata=metadata,
                input_load_curve_annual=input_lca,
                load_curve_hourly_dir=lc_hourly_dir,
                path_metadata=metadata_path,
                upgrade_id=uid,
                storage_options={},
            )
            if uid not in processed_upgrades:
                processed_upgrades.append(uid)
            print(f"    Done: MF adjustment complete for {loc}.", flush=True)

    return processed_upgrades


def _assign_utility(
    *,
    states: list[str],
    path_sb: Path,
    upgrade_ids: list[str],
    sample: int,
    s3_base_sb: str,
    path_s3_gis_dir: str,
    ny_electric_poly_filename: str | None,
    ny_gas_poly_filename: str | None,
) -> None:
    """Write metadata_utility/state=<s>/utility_assignment.parquet for each state.

    Reads upgrade-00 metadata-sb.parquet from the _sb release, runs the
    state-specific utility assignment, and writes the result locally.
    Supported states: NY (GIS-based probabilistic assignment) and RI (single
    utility). Any other state is skipped with an ERROR message.

    For NY, electric and gas polygon CSVs must be provided via
    ``ny_electric_poly_filename`` and ``ny_gas_poly_filename``; they are
    loaded from ``path_s3_gis_dir`` (default ``s3://data.sb/gis/utility_boundaries/``).
    PUMAs are fetched via pygris (requires network access).

    The output is uploaded to S3 via ``aws s3 cp`` immediately after each state
    is processed.

    When ``--sample > 0``:
    - For RI: no effect — the rule-based assignment is deterministic and complete.
    - For NY: the probability distributions come from GIS overlaps (not from the
      sample), so the assignment quality is unaffected.  A note is printed to
      set expectations.
    """
    padded = [u.zfill(2) for u in upgrade_ids]
    if _UTILITY_ASSIGN_UPGRADE not in padded:
        print(
            f"  WARNING: Utility assignment uses upgrade {_UTILITY_ASSIGN_UPGRADE} "
            f"metadata, but that upgrade is not in --upgrade-ids "
            f"({upgrade_ids}). Skipping utility assignment.",
            flush=True,
        )
        return

    for s in states:
        loc = f"state={s}"
        if s not in SUPPORTED_UTILITY_STATES:
            print(
                f"  ERROR: Utility assignment is not implemented for state {s!r}. "
                f"Only {sorted(SUPPORTED_UTILITY_STATES)} are supported. Skipping.",
                flush=True,
            )
            continue

        metadata_path = (
            path_sb
            / "metadata"
            / f"state={s}"
            / f"upgrade={_UTILITY_ASSIGN_UPGRADE}"
            / "metadata-sb.parquet"
        )
        if not metadata_path.exists():
            print(
                f"  WARNING: {metadata_path} not found, skipping {loc}.",
                flush=True,
            )
            continue

        out_dir = path_sb / "metadata_utility" / f"state={s}"
        out_path = out_dir / "utility_assignment.parquet"
        out_dir.mkdir(parents=True, exist_ok=True)

        print(f"  Processing {loc}...", flush=True)
        metadata = pl.scan_parquet(str(metadata_path))

        if sample > 0:
            n_bldgs = cast(pl.DataFrame, metadata.select(pl.len()).collect()).item()
            print(
                f"    NOTE: --sample active ({n_bldgs} buildings in local metadata). "
                f"Utility assignment probabilities are derived from GIS data, not "
                f"from the sample, so assignment quality is unaffected.",
                flush=True,
            )

        if s == "RI":
            result = assign_utility_ri(metadata)

        elif s == "NY":
            if not ny_electric_poly_filename:
                print(
                    f"  ERROR: --ny-electric-poly-filename is required for NY "
                    f"utility assignment. Skipping {loc}.",
                    flush=True,
                )
                continue
            if not ny_gas_poly_filename:
                print(
                    f"  ERROR: --ny-gas-poly-filename is required for NY "
                    f"utility assignment. Skipping {loc}.",
                    flush=True,
                )
                continue

            from cloudpathlib import S3Path
            from pygris import pumas as get_pumas

            from data.resstock.assign_utility_ny import CONFIGS

            gis_base = S3Path(path_s3_gis_dir.rstrip("/"))
            electric_polygons = read_csv_to_gdf_from_s3(
                gis_base / ny_electric_poly_filename,
                utility_type="electric",
            )
            gas_polygons = read_csv_to_gdf_from_s3(
                gis_base / ny_gas_poly_filename,
                utility_type="gas",
            )
            print("    Loading Census PUMA shapefiles via pygris...", flush=True)
            import geopandas as gpd
            from typing import cast as typing_cast

            pumas = typing_cast(
                gpd.GeoDataFrame,
                get_pumas(state=CONFIGS["state_code"], year=2019, cb=True),
            )
            pumas = pumas.to_crs(epsg=CONFIGS["state_crs"])

            result = assign_utility_ny(
                input_metadata=metadata,
                electric_polygons=electric_polygons,
                gas_polygons=gas_polygons,
                pumas=pumas,
                config=CONFIGS,
            )

        else:
            # Unreachable given the SUPPORTED_UTILITY_STATES guard above.
            continue

        # Keep only the assignment columns — the full metadata belongs in
        # metadata-sb.parquet (per-upgrade), not in the utility assignment file.
        result_slim = result.select("bldg_id", "sb.electric_utility", "sb.gas_utility")
        cast(pl.DataFrame, result_slim.collect()).write_parquet(str(out_path))
        print(f"    Written: {out_path}", flush=True)

        # Upload immediately so the file is on S3 even if a later state fails.
        s3_dest = f"{s3_base_sb.rstrip('/')}/metadata_utility/state={s}/utility_assignment.parquet"
        print(f"    Uploading → {s3_dest}", flush=True)
        upload_rc = subprocess.run(
            ["aws", "s3", "cp", str(out_path), s3_dest],
            check=False,
        ).returncode
        if upload_rc != 0:
            print(
                f"    WARNING: aws s3 cp exited with code {upload_rc} for {loc}.",
                flush=True,
            )

        print(f"    Done: utility assignment complete for {loc}.", flush=True)


def _add_monthly_loads(
    *,
    states: list[str],
    path_sb: Path,
    upgrade_ids: list[str],
    release: str,
    s3_base_sb: str,
    sample: int,
    workers: int,
) -> list[str]:
    """Aggregate _sb hourly load curves into monthly load curves and upload them.

    Reads per-building hourly parquets from ``path_sb/load_curve_hourly/`` and
    writes one monthly parquet per building to ``path_sb/load_curve_monthly/``.
    Aggregation rules (sum vs mean vs first) come from bsf's column-aggregation
    CSV for ``release`` (the raw release name, not the _sb variant).

    After each state is processed, the ``load_curve_monthly/state=<s>/``
    directory is synced to S3 via ``aws s3 sync``.

    When ``--sample > 0`` only N hourly files exist locally.  The aggregation
    proceeds on whatever files are present and N monthly files are written.
    This is expected behaviour for development/testing; run without ``--sample``
    for production.

    Returns the list of (state, upgrade) pairs that were actually processed,
    in ``"state=<s> upgrade=<uid>"`` format, for manifest recording.
    """
    print(f"  Loading bsf aggregation rules for release '{release}'...", flush=True)
    agg_rules = load_aggregation_rules(release)
    print(f"    {len(agg_rules)} column rules loaded.", flush=True)

    processed: list[str] = []

    for s in states:
        if sample > 0:
            print(
                f"  NOTE: --sample active for state={s}. "
                f"Monthly load curves will be generated only for the sampled buildings. "
                f"Run without --sample for production.",
                flush=True,
            )

        for uid in [u.zfill(2) for u in upgrade_ids]:
            loc = f"state={s} upgrade={uid}"
            hourly_dir = path_sb / "load_curve_hourly" / f"state={s}" / f"upgrade={uid}"

            if not hourly_dir.exists():
                print(
                    f"  WARNING: Hourly directory not found, skipping {loc}: "
                    f"{hourly_dir}",
                    flush=True,
                )
                continue

            n_files = len(list(hourly_dir.glob("*.parquet")))
            if n_files == 0:
                print(
                    f"  WARNING: No hourly parquets in {hourly_dir}, skipping {loc}.",
                    flush=True,
                )
                continue

            print(
                f"  Aggregating {n_files:,} hourly files → monthly for {loc}...",
                flush=True,
            )
            process_upgrade(
                path_input=path_sb,
                path_output=path_sb,
                state=s,
                upgrade=uid,
                agg_rules=agg_rules,
                workers=workers,
            )
            processed.append(loc)

        # Upload the full load_curve_monthly/state=<s>/ tree for this state once
        # all upgrades are done.
        monthly_state_dir = path_sb / "load_curve_monthly" / f"state={s}"
        if monthly_state_dir.exists():
            s3_dest = f"{s3_base_sb.rstrip('/')}/load_curve_monthly/state={s}/"
            print(f"  Uploading {monthly_state_dir} → {s3_dest}", flush=True)
            upload_rc = subprocess.run(
                ["aws", "s3", "sync", str(monthly_state_dir), s3_dest],
                check=False,
            ).returncode
            if upload_rc != 0:
                print(
                    f"  WARNING: aws s3 sync exited with code {upload_rc} "
                    f"for load_curve_monthly/state={s}/.",
                    flush=True,
                )
        else:
            print(
                f"  WARNING: No monthly output directory found for state={s} — "
                f"nothing to upload.",
                flush=True,
            )

    return processed


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ResStock data pipeline: fetch, modify, and upload.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--state",
        nargs="+",
        required=True,
        metavar="STATE",
        help="One or more 2-letter state codes (e.g. NY RI).",
    )
    parser.add_argument(
        "--path-output-dir",
        default=_DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help="Local EBS directory where ResStock parquet files are written.",
    )
    parser.add_argument(
        "--path-s3-dir",
        default=_DEFAULT_S3_DIR,
        metavar="S3_URI",
        help="S3 base URI mirroring the local output directory.",
    )
    parser.add_argument(
        "--release-year",
        type=int,
        default=_DEFAULT_RELEASE_YEAR,
        help="ResStock release year.",
    )
    parser.add_argument(
        "--weather-file",
        default=_DEFAULT_WEATHER_FILE,
        help="AMY weather file identifier.",
    )
    parser.add_argument(
        "--release-version",
        type=int,
        default=_DEFAULT_RELEASE_VERSION,
        help="ResStock release version number.",
    )
    parser.add_argument(
        "--upgrade-ids",
        nargs="+",
        default=_DEFAULT_UPGRADE_IDS,
        metavar="ID",
        help="Upgrade IDs to download (space-separated integers).",
    )
    parser.add_argument(
        "--file-types",
        nargs="+",
        default=_DEFAULT_FILE_TYPES,
        metavar="TYPE",
        help="File types to download (e.g. metadata load_curve_hourly load_curve_annual).",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=0,
        help="Number of buildings to sample (0 = all).",
    )
    parser.add_argument(
        "--identify-hp-customers",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Add postprocess_group.has_hp to metadata (default: True). "
            "Must be True for identify-heating-type to work correctly."
        ),
    )
    parser.add_argument(
        "--identify-heating-type",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Add heating-type columns to metadata (default: True). "
            "Requires identify-hp-customers."
        ),
    )
    parser.add_argument(
        "--identify-natgas-connection",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Add has_natgas_connection to metadata from load_curve_annual (default: True). "
            "Requires identify-heating-type and 'load_curve_annual' in --file-types."
        ),
    )
    parser.add_argument(
        "--add-vulnerability-columns",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Add LMI vulnerability columns from PUMS (default: True). "
            "NY only; pass False for RI."
        ),
    )
    parser.add_argument(
        "--approximate-non-hp-load",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Approximate non-HP load curves for upgrade 02 via k-nearest-neighbor "
            "HVAC substitution (default: True). Only runs when upgrade 02 is in "
            "--upgrade-ids and load_curve_hourly is in --file-types."
        ),
    )
    parser.add_argument(
        "--adjust-mf-electricity",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Apply MF non-HVAC electricity adjustment for upgrades 00 and 02 "
            "(default: True). Runs after approximate-non-hp-load. Requires "
            "load_curve_hourly and load_curve_annual in --file-types."
        ),
    )
    parser.add_argument(
        "--assign-utility",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Assign electric/gas utilities to buildings and write "
            "metadata_utility/state=<s>/utility_assignment.parquet (default: True). "
            "Supported states: NY, RI. Runs after all metadata transforms. "
            "Requires 'metadata' in --file-types and upgrade 00 in --upgrade-ids."
        ),
    )
    parser.add_argument(
        "--path-s3-gis-dir",
        default=_DEFAULT_S3_GIS_DIR,
        metavar="S3_URI",
        help=(
            "S3 directory containing NY utility polygon CSV files "
            f"(default: {_DEFAULT_S3_GIS_DIR})."
        ),
    )
    parser.add_argument(
        "--ny-electric-poly-filename",
        default=None,
        metavar="FILENAME",
        help=(
            "Filename of the NY electric utility polygon CSV in --path-s3-gis-dir "
            "(e.g. ny_electric_utilities_20260309.csv). Required when state=NY."
        ),
    )
    parser.add_argument(
        "--ny-gas-poly-filename",
        default=None,
        metavar="FILENAME",
        help=(
            "Filename of the NY gas utility polygon CSV in --path-s3-gis-dir "
            "(e.g. ny_gas_utilities_20260309.csv). Required when state=NY."
        ),
    )
    parser.add_argument(
        "--path-s3-pums-dir",
        default=_DEFAULT_S3_PUMS_DIR,
        metavar="S3_URI",
        help="S3 base URI for Census PUMS person data (used by add-vulnerability-columns).",
    )
    parser.add_argument(
        "--pums-survey",
        default=_DEFAULT_PUMS_SURVEY,
        help="PUMS survey type (e.g. acs5) — selects PUMS vintage.",
    )
    parser.add_argument(
        "--pums-year",
        default=_DEFAULT_PUMS_YEAR,
        help="PUMS end year (e.g. 2021) — selects PUMS vintage.",
    )
    parser.add_argument(
        "--add-monthly-loads",
        type=_parse_bool,
        default=True,
        metavar="BOOL",
        help=(
            "Aggregate _sb hourly load curves into monthly load curves and upload to S3 "
            "(default: True). Only runs when load_curve_hourly is in --file-types."
        ),
    )
    parser.add_argument(
        "--monthly-workers",
        type=int,
        default=50,
        metavar="N",
        help="Number of parallel workers for monthly aggregation (default: 50).",
    )
    return parser.parse_args(argv)


def _upload(
    state: list[str],
    file_types: list[str],
    release: str,
    path_output_dir: str | Path,
    path_s3_dir: str,
) -> None:
    """Sync fetched/modified local files to S3 (data only, not the manifest)."""
    local_base = Path(path_output_dir) / release
    s3_base = f"{path_s3_dir.rstrip('/')}/{release}"

    for file_type in file_types:
        for s in state:
            local_path = local_base / file_type / f"state={s}"
            s3_path = f"{s3_base}/{file_type}/state={s}/"
            print(f"Uploading {local_path} → {s3_path}", flush=True)
            result = subprocess.run(
                ["aws", "s3", "sync", str(local_path), s3_path],
                check=False,
            )
            if result.returncode != 0:
                print(
                    f"  WARNING: aws s3 sync exited with code {result.returncode}",
                    flush=True,
                )


def _upload_manifest(local_dir: Path, s3_base: str) -> None:
    """Push the manifest to S3 after the run record has been finalized on disk."""
    local_manifest = local_dir / "manifest.yaml"
    s3_manifest = f"{s3_base}/manifest.yaml"
    if not local_manifest.exists():
        return
    print(f"Uploading manifest {local_manifest} → {s3_manifest}", flush=True)
    result = subprocess.run(
        ["aws", "s3", "cp", str(local_manifest), s3_manifest],
        check=False,
    )
    if result.returncode != 0:
        print(
            f"  WARNING: manifest upload exited with code {result.returncode}",
            flush=True,
        )


def _modify_metadata(
    metadata: pl.LazyFrame,
    upgrade_id: str,
    *,
    run_identify_hp_customers: bool,
    run_identify_heating_type: bool,
    run_identify_natgas_connection: bool,
    path_raw: Path,
    state: str,
    run_add_vulnerability_columns: bool,
    pums_base_dir: str | None = None,
    pums_survey: str | None = None,
    pums_year: str | None = None,
) -> pl.LazyFrame:
    """Chain all metadata transformations in dependency order.

    Steps and their dependencies:
      1. identify_hp_customers       → adds postprocess_group.has_hp
      2. identify_heating_type       → adds heating_type columns (requires has_hp)
      3. identify_natgas_connection  → adds has_natgas_connection (requires heats_with_natgas;
                                       loads load_curve_annual from the raw release at path_raw,
                                       not from _sb, because load_curve_annual is never copied
                                       to _sb — it has no post-approximation equivalent)
      4. add_vulnerability_columns   → adds LMI vulnerability columns (NY only;
                                       loads PUMS conditional probs from S3)

    Each step receives and returns a LazyFrame. Steps 3 and 4 have internal collects for
    validation; step 4 always materialises the full frame. I/O (scan_parquet / sink_parquet)
    and iteration over states/upgrades is handled by the caller.

    Note: has_natgas_connection is re-derived from _sb load_curve_hourly by
    update_non_hp_metadata (step 2b-i) for any buildings whose HVAC load curves are
    approximated, so the raw-annual baseline set here is only the final value for
    non-approximated buildings.
    """
    if run_identify_hp_customers:
        metadata = identify_hp_customers(metadata=metadata, upgrade_id=upgrade_id)
    if run_identify_heating_type:
        metadata = identify_heating_type(metadata=metadata, upgrade_id=upgrade_id)
    if run_identify_natgas_connection:
        lca_dir = (
            path_raw / "load_curve_annual" / f"state={state}" / f"upgrade={upgrade_id}"
        )
        if not lca_dir.exists():
            raise RuntimeError(
                f"[state={state} upgrade={upgrade_id}] load_curve_annual not found "
                f"in the raw release at {lca_dir}. "
                f"Ensure 'load_curve_annual' is in --file-types so it is fetched."
            )
        load_curve_annual = pl.scan_parquet(str(lca_dir))
        metadata = identify_natgas_connection(
            metadata=metadata, load_curve_annual=load_curve_annual
        )
    if run_add_vulnerability_columns:
        if pums_base_dir is None or pums_survey is None or pums_year is None:
            raise ValueError(
                "pums_base_dir, pums_survey, and pums_year are required "
                "when --add-vulnerability-columns True."
            )
        puma_conditional_probs = load_puma_conditional_probs(
            pums_base_dir=pums_base_dir,
            survey=pums_survey,
            year=pums_year,
            state=state,
        )
        metadata = add_vulnerability_columns(
            metadata=metadata,
            puma_conditional_probs=puma_conditional_probs,
        )
    return metadata


# File types that belong only to the raw NREL release and must never be copied
# to the _sb release, uploaded under _sb, or validated against _sb.
# load_curve_annual has no post-approximation equivalent: the only valid
# aggregation of the modified _sb load curves is load_curve_monthly (derived
# from load_curve_hourly by add_monthly_loads after all modifications are done).
_SB_EXCLUDED_FILE_TYPES: frozenset[str] = frozenset({"load_curve_annual"})


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    release = f"res_{args.release_year}_{args.weather_file}_{args.release_version}"
    release_sb = f"{release}_sb"
    path_raw = Path(args.path_output_dir) / release
    path_sb = Path(args.path_output_dir) / release_sb
    s3_base_raw = f"{args.path_s3_dir.rstrip('/')}/{release}"
    s3_base_sb = f"{args.path_s3_dir.rstrip('/')}/{release_sb}"

    # File types that will actually appear in _sb (excludes raw-only types).
    sb_file_types = [ft for ft in args.file_types if ft not in _SB_EXCLUDED_FILE_TYPES]

    # ── Manifest: start a run record ──────────────────────────────────────────
    run = new_run_record(
        release=release,
        release_sb=release_sb,
        states=args.state,
        upgrade_ids=args.upgrade_ids,
        file_types=args.file_types,
        flags={
            "identify_hp_customers": args.identify_hp_customers,
            "identify_heating_type": args.identify_heating_type,
            "identify_natgas_connection": args.identify_natgas_connection,
            "add_vulnerability_columns": args.add_vulnerability_columns,
            "approximate_non_hp_load": args.approximate_non_hp_load,
            "adjust_mf_electricity": args.adjust_mf_electricity,
            "assign_utility": args.assign_utility,
            "add_monthly_loads": args.add_monthly_loads,
            "monthly_workers": args.monthly_workers,
            "sample": args.sample,
        },
    )

    # Warn early about file types that are excluded from _sb so the user sees
    # it at the top of the run log rather than discovering it mid-pipeline.
    run_warnings: list[str] = []
    for ft in _SB_EXCLUDED_FILE_TYPES:
        if ft in args.file_types:
            msg = (
                f"'{ft}' will be fetched for the raw release but is NOT copied to "
                f"the _sb release. The _sb release has no post-modification annual "
                f"equivalent; use load_curve_monthly (derived from load_curve_hourly) "
                f"for month-level aggregations of the modified _sb data."
            )
            print(f"WARNING: {msg}", flush=True)
            run_warnings.append(msg)
    if run_warnings:
        run["warnings"] = run_warnings

    try:
        # ── 1. Fetch ──────────────────────────────────────────────────────────
        # ── 1a. Fetch raw ResStock data ────────────────────────────────────────
        print("Fetching raw ResStock data...", flush=True)
        rc = fetch_resstock_data.run(
            state=args.state,
            path_output_dir=args.path_output_dir,
            release_year=args.release_year,
            weather_file=args.weather_file,
            release_version=args.release_version,
            upgrade_ids=args.upgrade_ids,
            file_types=args.file_types,
            sample=args.sample,
        )
        if rc != 0:
            raise RuntimeError(f"bsf exited with code {rc}")
        print("Validating fetch...", flush=True)
        validate_local_files(
            label="fetch (step 1a)",
            state=args.state,
            upgrade_ids=args.upgrade_ids,
            file_types=args.file_types,
            base_path=path_raw,
        )
        record_step(run, "fetch", path=str(path_raw))
        upsert_run(path_raw, run)

        # ── 1b. Clone raw release to _sb (only the fetched states/file_types) ──
        # load_curve_annual is intentionally excluded: it has no post-modification
        # equivalent in _sb (no mechanism to re-derive it from modified hourly).
        print(
            f"Cloning {path_raw} → {path_sb} "
            f"(states={args.state}, file_types={sb_file_types})...",
            flush=True,
        )
        n_copied = 0
        for file_type in sb_file_types:
            for s in args.state:
                for uid in args.upgrade_ids:
                    upgrade_id_padded = uid.zfill(2)
                    src = (
                        path_raw
                        / file_type
                        / f"state={s}"
                        / f"upgrade={upgrade_id_padded}"
                    )
                    dst = (
                        path_sb
                        / file_type
                        / f"state={s}"
                        / f"upgrade={upgrade_id_padded}"
                    )
                    if src.is_dir():
                        n_copied += copy_dir(src, dst)
        print(f"  Cloned {n_copied} files.", flush=True)
        path_sb.mkdir(parents=True, exist_ok=True)
        write_manifest(path_sb, {"runs": []})
        print("Validating clone...", flush=True)
        validate_local_files(
            label="clone (step 1b)",
            state=args.state,
            upgrade_ids=args.upgrade_ids,
            file_types=sb_file_types,
            base_path=path_sb,
        )
        record_step(run, "clone", files_copied=n_copied, path=str(path_sb))
        upsert_run(path_sb, run)

        # ── 2. Modify ─────────────────────────────────────────────────────────

        # ── 2a. Modify metadata ────────────────────────────────────────────────
        metadata_transforms_applied: list[str] = []
        print("Modifying metadata...", flush=True)
        for s in args.state:
            for uid in args.upgrade_ids:
                upgrade_id_padded = uid.zfill(2)
                loc = f"state={s} upgrade={upgrade_id_padded}"
                metadata_dir = (
                    path_sb / "metadata" / f"state={s}" / f"upgrade={upgrade_id_padded}"
                )
                input_path = metadata_dir / "metadata.parquet"
                output_path = metadata_dir / "metadata-sb.parquet"

                if not input_path.exists():
                    print(f"  WARNING: {input_path} not found, skipping.", flush=True)
                    continue
                print(f"  {loc}", flush=True)

                # Read metadata
                input_metadata = pl.scan_parquet(str(input_path))
                validate_metadata_readable(input_metadata, input_path, loc)

                output_metadata = _modify_metadata(
                    metadata=input_metadata,
                    upgrade_id=upgrade_id_padded,
                    run_identify_hp_customers=args.identify_hp_customers,
                    run_identify_heating_type=args.identify_heating_type,
                    run_identify_natgas_connection=args.identify_natgas_connection,
                    path_raw=path_raw,
                    state=s,
                    run_add_vulnerability_columns=args.add_vulnerability_columns,
                    pums_base_dir=args.path_s3_pums_dir,
                    pums_survey=args.pums_survey,
                    pums_year=args.pums_year,
                )

                # Validate output schema for each active transformation.
                if args.identify_hp_customers:
                    validate_metadata_columns(
                        output_metadata, HP_CUSTOMERS_COLS, "identify_hp_customers", loc
                    )
                    if "identify_hp_customers" not in metadata_transforms_applied:
                        metadata_transforms_applied.append("identify_hp_customers")
                if args.identify_heating_type:
                    validate_metadata_columns(
                        output_metadata, HEATING_TYPE_COLS, "identify_heating_type", loc
                    )
                    if "identify_heating_type" not in metadata_transforms_applied:
                        metadata_transforms_applied.append("identify_heating_type")
                if args.identify_natgas_connection:
                    validate_metadata_columns(
                        output_metadata,
                        NATGAS_CONNECTION_COLS,
                        "identify_natgas_connection",
                        loc,
                    )
                    if "identify_natgas_connection" not in metadata_transforms_applied:
                        metadata_transforms_applied.append("identify_natgas_connection")
                if args.add_vulnerability_columns:
                    validate_metadata_columns(
                        output_metadata,
                        VULNERABILITY_COLS,
                        "add_vulnerability_columns",
                        loc,
                    )
                    if "add_vulnerability_columns" not in metadata_transforms_applied:
                        metadata_transforms_applied.append("add_vulnerability_columns")

                # Single sink at the end of the full transformation chain.
                output_metadata.sink_parquet(str(output_path))
                validate_metadata_output(output_path, loc)

        record_step(run, "modify_metadata", transforms=metadata_transforms_applied)
        upsert_run(path_sb, run)

        # ── 2b. Modify load curves ─────────────────────────────────────────────

        # ── 2b-i. Approximate non-HP load for upgrade 02 ──────────────────────
        _APPROX_UPGRADE = "02"
        if (
            args.approximate_non_hp_load
            and _APPROX_UPGRADE in [u.zfill(2) for u in args.upgrade_ids]
            and "load_curve_hourly" in args.file_types
        ):
            print("Approximating non-HP load curves (upgrade 02)...", flush=True)
            _approximate_non_hp_load(
                states=args.state,
                path_sb=path_sb,
                sample=args.sample,
            )
            record_step(run, "approximate_non_hp_load", upgrades=[_APPROX_UPGRADE])
            upsert_run(path_sb, run)

        # ── 2b-ii. Adjust MF non-HVAC electricity for upgrades 00 and 02 ──────
        if (
            args.adjust_mf_electricity
            and any(u.zfill(2) in _MF_ADJ_UPGRADES for u in args.upgrade_ids)
            and "load_curve_hourly" in args.file_types
        ):
            print("Adjusting MF non-HVAC electricity...", flush=True)
            processed_upgrades = _adjust_mf_electricity(
                states=args.state,
                path_raw=path_raw,
                path_sb=path_sb,
                upgrade_ids=args.upgrade_ids,
                sample=args.sample,
            )
            record_step(run, "adjust_mf_electricity", upgrades=processed_upgrades)
            upsert_run(path_sb, run)

        # ── 2b-iii. Assign electric/gas utilities ─────────────────────────────
        if args.assign_utility and "metadata" in args.file_types:
            print("Assigning utilities...", flush=True)
            _assign_utility(
                states=args.state,
                path_sb=path_sb,
                upgrade_ids=args.upgrade_ids,
                sample=args.sample,
                s3_base_sb=s3_base_sb,
                path_s3_gis_dir=args.path_s3_gis_dir,
                ny_electric_poly_filename=args.ny_electric_poly_filename,
                ny_gas_poly_filename=args.ny_gas_poly_filename,
            )
            record_step(run, "assign_utility")
            upsert_run(path_sb, run)

        # ── 2b-iv. Add monthly load curves ────────────────────────────────────
        if args.add_monthly_loads and "load_curve_hourly" in args.file_types:
            print("Adding monthly load curves...", flush=True)
            processed_monthly = _add_monthly_loads(
                states=args.state,
                path_sb=path_sb,
                upgrade_ids=args.upgrade_ids,
                release=release,
                s3_base_sb=s3_base_sb,
                sample=args.sample,
                workers=args.monthly_workers,
            )
            record_step(run, "add_monthly_loads", processed=processed_monthly)
            upsert_run(path_sb, run)

        # ── 3. Upload ─────────────────────────────────────────────────────────
        print("Uploading raw ResStock data to S3...", flush=True)
        _upload(
            state=args.state,
            file_types=args.file_types,
            release=release,
            path_output_dir=args.path_output_dir,
            path_s3_dir=args.path_s3_dir,
        )
        record_step(run, "upload_raw", s3_base=s3_base_raw)
        upsert_run(path_raw, run)
        _upload_manifest(path_raw, s3_base_raw)

        print("Uploading modified sb ResStock data to S3...", flush=True)
        _upload(
            state=args.state,
            file_types=sb_file_types,
            release=release_sb,
            path_output_dir=args.path_output_dir,
            path_s3_dir=args.path_s3_dir,
        )
        record_step(run, "upload_sb", s3_base=s3_base_sb)
        upsert_run(path_sb, run)
        _upload_manifest(path_sb, s3_base_sb)

        print("Validating S3 uploads...", flush=True)
        validate_s3_objects(
            label="upload raw (step 3)",
            state=args.state,
            upgrade_ids=args.upgrade_ids,
            file_types=args.file_types,
            s3_base=s3_base_raw,
            local_base=path_raw,
        )
        validate_s3_objects(
            label="upload sb (step 3)",
            state=args.state,
            upgrade_ids=args.upgrade_ids,
            file_types=sb_file_types,
            s3_base=s3_base_sb,
            local_base=path_sb,
        )

        # ── Done ──────────────────────────────────────────────────────────────
        finish_run(run)
        upsert_run(path_raw, run)
        upsert_run(path_sb, run)
        print("Pipeline complete.", flush=True)

    except Exception as e:
        error_msg = str(e)
        print(f"ERROR: {error_msg}", flush=True)
        fail_run(run, error_msg)
        # Best-effort: record the failure in whichever manifests exist on disk.
        for path in (path_raw, path_sb):
            if path.exists():
                upsert_run(path, run)
        sys.exit(1)


if __name__ == "__main__":
    main()
