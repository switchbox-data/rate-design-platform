"""Read CAIRO run outputs from S3 for validation.

CSVs are scanned lazily via ``pl.scan_csv``; JSON files are fetched with ``boto3``.
Local config reads (input tariffs, RR YAMLs) use standard file I/O.

Run directory layout::

    bills/{elec,gas,comb}_bills_year_target.csv
    cross_subsidization/cross_subsidization_BAT_values.csv
    customer_metadata.csv
    tariff_final_config.json
"""

from __future__ import annotations

import json
from typing import Any, Literal

import boto3
import polars as pl
import yaml

from utils import get_project_root
from utils.loads import ELECTRIC_LOAD_COL
from utils.post.validate.config import RunConfig

BillType = Literal["elec", "gas", "comb"]

_VALID_BILL_TYPES: frozenset[str] = frozenset({"elec", "gas", "comb"})
_REL_BAT = "cross_subsidization/cross_subsidization_BAT_values.csv"
_REL_METADATA = "customer_metadata.csv"
_REL_TARIFF_CONFIG = "tariff_final_config.json"


def _s3_join(s3_dir: str, relative: str) -> str:
    return f"{s3_dir.rstrip('/')}/{relative}"


def _s3_get_bytes(s3_uri: str) -> bytes:
    """Fetch raw bytes from an S3 URI."""
    without_scheme = s3_uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()


def load_bills(s3_dir: str, bill_type: BillType = "elec") -> pl.LazyFrame:
    """Lazily scan ``bills/{bill_type}_bills_year_target.csv`` from a run directory.

    ``bill_type`` is one of ``"elec"`` (default), ``"gas"``, or ``"comb"``.
    """
    if bill_type not in _VALID_BILL_TYPES:
        raise ValueError(
            f"bill_type must be one of {sorted(_VALID_BILL_TYPES)!r}, got {bill_type!r}"
        )
    return pl.scan_csv(_s3_join(s3_dir, f"bills/{bill_type}_bills_year_target.csv"))


def load_bat(s3_dir: str) -> pl.LazyFrame:
    """Lazily scan ``cross_subsidization/cross_subsidization_BAT_values.csv``."""
    return pl.scan_csv(_s3_join(s3_dir, _REL_BAT))


def load_metadata(s3_dir: str) -> pl.LazyFrame:
    """Lazily scan ``customer_metadata.csv`` (ResStock metadata with ``bldg_id``, ``weight``).

    The ``in.occupants`` column contains values like ``"10+"`` which cannot be parsed as integers,
    so it is read as a string (Utf8) to avoid parsing errors.
    """
    return pl.scan_csv(
        _s3_join(s3_dir, _REL_METADATA),
        schema_overrides={"in.occupants": pl.Utf8},
    )


def load_tariff_config(s3_dir: str) -> dict[str, Any]:
    """Fetch and parse ``tariff_final_config.json`` from a run directory via boto3."""
    return json.loads(_s3_get_bytes(_s3_join(s3_dir, _REL_TARIFF_CONFIG)))


def load_input_tariff(state: str, utility: str, tariff_filename: str) -> dict[str, Any]:
    """Read a URDB tariff JSON from ``rate_design/hp_rates/{state}/config/tariffs/electric/``.

    ``utility`` is accepted for call-site clarity but not used in the path.
    """
    path = (
        get_project_root()
        / "rate_design"
        / "hp_rates"
        / state.lower()
        / "config"
        / "tariffs"
        / "electric"
        / tariff_filename
    )
    if not path.exists():
        raise FileNotFoundError(
            f"Input tariff not found for state={state!r} utility={utility!r}: {path}"
        )
    return json.loads(path.read_text(encoding="utf-8"))


def load_revenue_requirement(
    state: str,
    utility: str,
    rr_filename: str | None = None,
) -> dict[str, Any]:
    """Read a revenue requirement YAML from ``rate_design/hp_rates/{state}/config/rev_requirement/``.

    Defaults to ``{utility}.yaml``; pass ``rr_filename`` to load the subclass variant
    (e.g. ``"{utility}_hp_vs_nonhp.yaml"``).
    """
    filename = rr_filename if rr_filename is not None else f"{utility.lower()}.yaml"
    path = (
        get_project_root()
        / "rate_design"
        / "hp_rates"
        / state.lower()
        / "config"
        / "rev_requirement"
        / filename
    )
    if not path.exists():
        raise FileNotFoundError(
            f"Revenue requirement YAML not found for state={state!r} utility={utility!r}: {path}"
        )
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def scan_utility_loads(path_resstock_loads: str) -> pl.LazyFrame:
    """Scan ResStock load curves from a local directory path.

    The path should point to the upgrade-00 partition directory
    (e.g. /ebs/data/.../load_curve_hourly/state=NY/upgrade=00/).

    Args:
        path_resstock_loads: Local path to the ResStock load curves directory
            for upgrade 00 (already partitioned, no hive kwargs needed).

    Returns:
        LazyFrame of hourly load data with columns including ``bldg_id``,
        ``timestamp``, and ``out.electricity.net.energy_consumption``.
    """
    return pl.scan_parquet(path_resstock_loads)


def compute_weighted_loads_by_subclass_from_collected(
    loads_df: pl.DataFrame,
    metadata_df: pl.DataFrame,
) -> pl.DataFrame:
    """Compute weighted hourly loads by HP/non-HP subclass from pre-collected loads.

    Optimized version that works with a pre-collected DataFrame instead of a LazyFrame,
    avoiding repeated parquet scans. Use this when loads have already been collected
    for a block of runs.

    Args:
        loads_df: Pre-collected DataFrame of ResStock load curves (upgrade 00),
            filtered to buildings that appear in any run in the block.
        metadata_df: Collected DataFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns for a specific run.

    Returns:
        DataFrame with columns: ``hour`` (int, 0–8759), ``subclass``
        (``"HP"`` / ``"Non-HP"``), ``total_weighted_load_kwh`` (sum, for
        cost-of-service), and ``load_kwh`` (weighted mean, for the load plot).
    """
    _HP = "postprocess_group.has_hp"
    _BLDG = "bldg_id"
    _WEIGHT = "weight"

    frames = []
    for hp_val, label in [(True, "HP"), (False, "Non-HP")]:
        group_meta = metadata_df.filter(pl.col(_HP) == hp_val)
        if group_meta.is_empty():
            continue
        bldg_ids = group_meta[_BLDG].cast(pl.Int64).to_list()
        weights_df = group_meta.select(
            pl.col(_BLDG).cast(pl.Int64),
            pl.col(_WEIGHT).cast(pl.Float64),
        )
        group_df = (
            loads_df.filter(pl.col(_BLDG).cast(pl.Int64).is_in(bldg_ids))
            .join(weights_df, on=_BLDG, how="inner")
            .select(
                pl.col("timestamp")
                .cast(pl.String, strict=False)
                .str.to_datetime(strict=False)
                .alias("_ts"),
                (pl.col(ELECTRIC_LOAD_COL).cast(pl.Float64) * pl.col(_WEIGHT)).alias(
                    "_wload"
                ),
                pl.col(_WEIGHT).cast(pl.Float64),
            )
            .group_by("_ts")
            .agg(
                pl.col("_wload").sum().alias("total_weighted_load_kwh"),
                pl.col(_WEIGHT).sum().alias("_weight_sum"),
            )
            .with_columns(
                (pl.col("total_weighted_load_kwh") / pl.col("_weight_sum")).alias(
                    "load_kwh"
                )
            )
            .sort("_ts")
            .with_row_index("hour")
            .select(["hour", "total_weighted_load_kwh", "load_kwh"])
            .with_columns(pl.lit(label).alias("subclass"))
        )
        frames.append(group_df)

    return pl.concat(frames)


def load_all_mc_components(run2_config: RunConfig) -> dict[str, pl.Series]:
    """Load all four marginal cost components from run 2 config paths.

    Run 2 is used because it is the first run with ``run_includes_supply: true``,
    giving real (non-zero) supply MC paths.

    Args:
        run2_config: RunConfig for run 2 (has real supply MCs).

    Returns:
        Dict with keys: ``"dist_sub_tx"``, ``"bulk_tx"``, ``"supply_energy"``,
        ``"supply_capacity"``. Each value is a Series of 8760 floats ($/kWh)
        aligned to hour index (0–8759). Supply components are converted from
        $/MWh to $/kWh (÷1000).
    """
    # Load dist+sub-tx MC
    dist_sub_tx_df = pl.read_parquet(run2_config.path_dist_and_sub_tx_mc)
    dist_sub_tx_df = dist_sub_tx_df.sort("timestamp").with_row_index("hour")
    dist_sub_tx_series = dist_sub_tx_df["mc_total_per_kwh"].cast(pl.Float64)

    # Load bulk TX MC (optional, may be None)
    if run2_config.path_bulk_tx_mc:
        bulk_tx_df = pl.read_parquet(run2_config.path_bulk_tx_mc)
        bulk_tx_df = bulk_tx_df.sort("timestamp").with_row_index("hour")
        bulk_tx_series = bulk_tx_df["bulk_tx_cost_enduse"].cast(pl.Float64)
    else:
        # Create zero series if bulk_tx_mc is not provided
        bulk_tx_series = pl.Series("bulk_tx", [0.0] * 8760)

    # Load supply energy MC (convert $/MWh → $/kWh)
    supply_energy_df = pl.read_parquet(run2_config.path_supply_energy_mc)
    supply_energy_df = supply_energy_df.sort("timestamp").with_row_index("hour")
    supply_energy_series = (
        supply_energy_df["energy_cost_enduse"].cast(pl.Float64) / 1000.0
    )

    # Load supply capacity MC (convert $/MWh → $/kWh)
    supply_capacity_df = pl.read_parquet(run2_config.path_supply_capacity_mc)
    supply_capacity_df = supply_capacity_df.sort("timestamp").with_row_index("hour")
    supply_capacity_series = (
        supply_capacity_df["capacity_cost_enduse"].cast(pl.Float64) / 1000.0
    )

    return {
        "dist_sub_tx": dist_sub_tx_series,
        "bulk_tx": bulk_tx_series,
        "supply_energy": supply_energy_series,
        "supply_capacity": supply_capacity_series,
    }
