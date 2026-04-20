"""Utility functions for Cairo-related operations."""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import polars as pl
from cairo.rates_tool import config
from cairo.rates_tool.loads import __timeshift__
from cloudpathlib import S3Path

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.types import ElectricUtility

CambiumPathLike = str | Path | S3Path
log = logging.getLogger(__name__)


def _current_rss_gb() -> float:
    """Read current RSS from /proc/self/status (Linux only)."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1e6  # kB → GB
    except OSError:
        pass
    return 0.0


def _log_rss(label: str) -> None:
    log.info("RSS [%.2f GB] %s", _current_rss_gb(), label)


# ---------------------------------------------------------------------------
# Low-level I/O helpers
# ---------------------------------------------------------------------------


def _normalize_mc_path(path: CambiumPathLike):  # noqa: ANN201
    """Return a single path-like (Path or S3Path) for an MC CSV or Parquet.

    Accepts:
    - S3Path / Path: returned as-is
    - str starting with "s3://": wrapped in S3Path
    - str with "/" or explicit extension: wrapped in Path
    - bare scenario name (str): resolved to config.MARGINALCOST_DIR/<name>.csv
    """
    if isinstance(path, (S3Path, Path)):
        return path
    if isinstance(path, str):
        if path.startswith("s3://"):
            return S3Path(path)
        if "/" in path or path.endswith((".csv", ".parquet")):
            return Path(path)
        return config.MARGINALCOST_DIR / f"{path}.csv"
    raise TypeError(f"path must be str, Path, or S3Path; got {type(path)}")


def _read_parquet_to_pandas(path: Path | S3Path) -> pd.DataFrame:
    """Read a single parquet file (local or S3) into a pandas DataFrame.

    For S3, streams bytes through BytesIO so PyArrow reads a single file rather
    than inferring a partitioned dataset from Hive-style path segments like
    ``scenario=MidCase/...`` (which would raise ArrowTypeError on schema merge).
    """
    if isinstance(path, S3Path):
        return pd.read_parquet(io.BytesIO(path.read_bytes()), engine="pyarrow")
    return pd.read_parquet(path)


def _set_timestamp_index(df: pd.DataFrame) -> pd.DataFrame:
    """Move a 'timestamp' column to the index if needed, and name the index 'time'."""
    if "timestamp" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
        df = df.set_index("timestamp")
    if df.index.name != "time":
        df.index.name = "time"
    return df


def _scan_parquet_pl(path_str: str) -> pl.LazyFrame:
    """Scan a parquet path (local or S3) as a Polars LazyFrame."""
    if path_str.startswith("s3://"):
        return pl.scan_parquet(path_str, storage_options=get_aws_storage_options())
    return pl.scan_parquet(path_str)


def _is_cambium_path(path: CambiumPathLike) -> bool:
    """Return True if the path string contains 'cambium' (case-insensitive)."""
    return "cambium" in str(path).lower()


# ---------------------------------------------------------------------------
# Supply-side marginal cost loaders
# ---------------------------------------------------------------------------


def _load_cambium_marginal_costs(
    cambium_scenario: CambiumPathLike, target_year: int
) -> pd.DataFrame:
    """
    Load Cambium marginal costs from CSV or Parquet (local or S3). Returns costs in $/kWh.

    Accepts: scenario name (str → CSV under config dir), local path (str or Path),
    or S3 URI (str or S3Path). Example S3 Parquet:
    s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet

    Assumptions (verified against S3 Parquet and repo CSV example_marginal_costs.csv):
    - CSV: first 5 rows are metadata; row 6 is header; data has columns timestamp,
      energy_cost_enduse, capacity_cost_enduse. Costs are in $/MWh.
    - Parquet: columns timestamp (datetime), energy_cost_enduse, capacity_cost_enduse
      (float). Costs are in $/MWh. Exactly 8760 rows (hourly). No partition columns
      in the DataFrame (single-file read).
    - Both: we divide cost columns by 1000 to get $/kWh; then common_year alignment,
      __timeshift__ to target_year, and tz_localize("EST") so output matches CAIRO.
    """
    path = _normalize_mc_path(cambium_scenario)
    if not path.exists():
        raise FileNotFoundError(f"Cambium marginal cost file {path} does not exist")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        raw = path.read_bytes() if isinstance(path, S3Path) else None
        df = pd.read_csv(
            io.BytesIO(raw) if raw is not None else path,
            skiprows=5,
            index_col="timestamp",
            parse_dates=True,
        )
    elif suffix == ".parquet":
        df = _read_parquet_to_pandas(path)
        df = _set_timestamp_index(df)
    else:
        raise ValueError(
            f"Cambium file must be .csv or .parquet; got {path} (suffix {suffix})"
        )

    keep_cols = {
        "energy_cost_enduse": "Marginal Energy Costs ($/kWh)",
        "capacity_cost_enduse": "Marginal Capacity Costs ($/kWh)",
    }
    numeric_input_cols = list(keep_cols.keys())
    for col in numeric_input_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=numeric_input_cols, how="any")
    if df.empty:
        raise ValueError(
            f"Cambium marginal cost file {path} has no valid numeric rows in required "
            f"columns: {numeric_input_cols}"
        )

    df = df.loc[:, list(keep_cols.keys())].rename(columns=keep_cols)
    df.loc[:, [c for c in df.columns if "/kWh" in c]] /= 1000  # $/MWh → $/kWh

    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.loc[~df.index.isna()].copy()
    if df.empty:
        raise ValueError(f"Cambium marginal cost file {path} has no valid timestamps")

    common_years = [2017, 2023, 2034, 2045, 2051]
    year_diff = [abs(y - target_year) for y in common_years]
    common_year = common_years[year_diff.index(min(year_diff))]
    df.index = pd.DatetimeIndex(
        [t.replace(year=common_year) for t in df.index],
        name="time",
    )
    df = __timeshift__(df, target_year)
    df.index = df.index.tz_localize("EST")
    df.index.name = "time"
    return df


def _load_supply_mc_column(
    path: CambiumPathLike,
    col: str,
    label: str,
    target_year: int,
    context: str = "Supply MC",
) -> pd.DataFrame:
    """Load a single supply MC column from a parquet file (local or S3).

    Returns a one-column DataFrame named ``label`` in $/kWh, indexed by a
    tz-aware DatetimeIndex (EST), time-shifted to ``target_year``.

    Args:
        path: Local or S3 path to the supply MC parquet file.
        col: Source column name in the parquet (e.g. ``"energy_cost_enduse"``).
        label: Output column name (e.g. ``"Marginal Energy Costs ($/kWh)"``).
        target_year: Target year for timeshifting.
        context: Human-readable label used in error messages.
    """
    normalized = _normalize_mc_path(path)
    if not normalized.exists():
        raise FileNotFoundError(f"{context} file {normalized} does not exist")
    if normalized.suffix.lower() != ".parquet":
        raise ValueError(
            f"{context} file must be .parquet; got {normalized} (suffix {normalized.suffix})"
        )

    df = _read_parquet_to_pandas(normalized)
    df = _set_timestamp_index(df)

    if col not in df.columns:
        raise ValueError(f"{context} file {normalized} missing required column: {col}")

    df = df.loc[:, [col]].copy()
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[col])
    if df.empty:
        raise ValueError(
            f"{context} file {normalized} has no valid numeric rows in {col}"
        )

    df = df.rename(columns={col: label})
    df.loc[:, label] /= 1000  # $/MWh → $/kWh

    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.loc[~df.index.isna()].copy()
    if df.empty:
        raise ValueError(f"{context} file {normalized} has no valid timestamps")

    common_year = df.index[0].year
    if common_year != target_year:
        df = __timeshift__(df, target_year)

    if df.index.tz is None:
        df.index = df.index.tz_localize("EST")
    else:
        df.index = df.index.tz_convert("EST")
    df.index.name = "time"
    return df


def _load_supply_marginal_costs(
    energy_path: CambiumPathLike | None,
    capacity_path: CambiumPathLike | None,
    target_year: int,
    ancillary_path: CambiumPathLike | None = None,
) -> pd.DataFrame:
    """
    Load supply marginal costs: Energy + Capacity (bulk supply), plus optional Ancillary.

    Architecture:
    - Supply MCs = Energy + Capacity (bulk supply, from Cambium or NYISO)
    - Delivery MCs = Bulk Tx + Dist+Sub-Tx (handled separately)
    All MCs are index-aligned to a common 8760-hour DatetimeIndex.

    If both energy_path and capacity_path are provided:
    - If either path contains "cambium", treat it as a Cambium file (combined energy+capacity)
      and use _load_cambium_marginal_costs with the cambium path.
    - Otherwise, load them separately and combine.

    If only one is provided, raises ValueError.
    If both are None, raises ValueError (caller should use _load_cambium_marginal_costs instead).

    For delivery-only runs (add_supply_revenue_requirement=false), supply MCs may be set to
    zero but are still loaded to provide the alignment target index.

    Returns DataFrame with columns matching CAIRO convention:
    - Marginal Energy Costs ($/kWh)
    - Marginal Capacity Costs ($/kWh)
    - Marginal Ancillary Costs ($/kWh)  [optional, only when ancillary_path is provided]

    CAIRO's _calculate_system_revenue_target sums all columns via
    ``marginal_system_prices.sum(axis=1)``, so the ancillary column is automatically
    included in the total marginal cost when present.

    Args:
        energy_path: Path to energy MC parquet (or None). If path contains "cambium",
                     treated as combined Cambium file.
        capacity_path: Path to capacity MC parquet (or None). If path contains "cambium",
                       treated as combined Cambium file.
        target_year: Target year for timeshifting.
        ancillary_path: Optional path to ancillary MC parquet. When provided, loads
                        ``ancillary_cost_enduse`` ($/MWh) and appends it as a third
                        column ``"Marginal Ancillary Costs ($/kWh)"``.

    Note — delivery-only RR top-up excludes ancillary:
        CAIRO's ``_return_revenue_requirement_target`` has a ``delivery_only_rev_req_passed``
        flag that tops up a delivery-only revenue requirement with estimated supply costs
        before decomposition. That top-up filters columns by
        ``"Energy" in col or "Capacity" in col``, which does NOT match
        ``"Marginal Ancillary Costs ($/kWh)"``. Ancillary costs are therefore excluded
        from the top-up amount. In practice this codebase never sets
        ``delivery_only_rev_req_passed=True`` (RRs are pre-topped-up in the YAML), so
        this has no runtime effect — but it would matter if that flag were ever used.

    Returns:
        Combined DataFrame with energy and capacity costs (and ancillary when
        ancillary_path is provided), indexed by DatetimeIndex (EST-localized, 8760 rows).
    """
    if energy_path is None and capacity_path is None:
        raise ValueError(
            "Both energy_path and capacity_path are None. "
            "Use _load_cambium_marginal_costs for combined files."
        )
    if energy_path is None:
        raise ValueError("energy_path is required when using separate supply MC files")
    if capacity_path is None:
        raise ValueError(
            "capacity_path is required when using separate supply MC files"
        )

    # Backward compatibility: if either path is a Cambium file (combined energy+capacity),
    # route to the Cambium loader and let it produce both columns.
    if _is_cambium_path(energy_path) or _is_cambium_path(capacity_path):
        cambium_path = energy_path if _is_cambium_path(energy_path) else capacity_path
        log.info(
            "Detected Cambium path in supply MC: %s. Using _load_cambium_marginal_costs for backward compatibility.",
            cambium_path,
        )
        combined = _load_cambium_marginal_costs(cambium_path, target_year)
    else:
        # Separate energy and capacity parquets (e.g. NYISO LBMP + ICAP).
        energy_df = _load_supply_mc_column(
            energy_path,
            col="energy_cost_enduse",
            label="Marginal Energy Costs ($/kWh)",
            target_year=target_year,
            context="Supply energy MC",
        )
        capacity_df = _load_supply_mc_column(
            capacity_path,
            col="capacity_cost_enduse",
            label="Marginal Capacity Costs ($/kWh)",
            target_year=target_year,
            context="Supply capacity MC",
        )

        combined = pd.concat([energy_df, capacity_df], axis=1)
        if len(combined) != 8760:
            raise ValueError(
                f"Combined supply MC has {len(combined)} rows, expected 8760"
            )

    if ancillary_path is not None and str(ancillary_path).strip():
        ancillary_df = _load_supply_mc_column(
            ancillary_path,
            col="ancillary_cost_enduse",
            label="Marginal Ancillary Costs ($/kWh)",
            target_year=target_year,
            context="Supply ancillary MC",
        )
        combined = pd.concat([combined, ancillary_df], axis=1)
        log.info(
            "Loaded ancillary supply MC: %d rows, avg=%.6f $/kWh",
            len(ancillary_df),
            float(ancillary_df["Marginal Ancillary Costs ($/kWh)"].mean()),
        )

    return combined


# ---------------------------------------------------------------------------
# Building ID / load-file helpers
# ---------------------------------------------------------------------------


def build_bldg_id_to_load_filepath(
    path_resstock_loads: Path,
    building_ids: list[int] | None = None,
    return_path_base: Path | None = None,
) -> dict[int, Path]:
    """
    Build a dictionary mapping building IDs to their load file paths.

    Args:
        path_resstock_loads: Directory containing parquet load files to scan
        building_ids: Optional list of building IDs to include. If None, includes all.
        return_path_base: Base directory for returned paths.
            If None, returns actual file paths from path_resstock_loads.
            If Path, returns paths as return_path_base / filename.

    Returns:
        Dictionary mapping building ID (int) to full file path (Path)

    Raises:
        FileNotFoundError: If path_resstock_loads does not exist
    """
    if not path_resstock_loads.exists():
        raise FileNotFoundError(f"Load directory not found: {path_resstock_loads}")

    building_ids_set = set(building_ids) if building_ids is not None else None

    bldg_id_to_load_filepath = {}
    for parquet_file in path_resstock_loads.glob("*.parquet"):
        try:
            bldg_id = int(parquet_file.stem.split("-")[0])
        except ValueError:
            continue  # Skip files that don't match expected pattern

        if building_ids_set is not None and bldg_id not in building_ids_set:
            continue

        filepath = (
            parquet_file
            if return_path_base is None
            else return_path_base / parquet_file.name
        )
        bldg_id_to_load_filepath[bldg_id] = filepath

    return bldg_id_to_load_filepath


def _fetch_prototype_ids_by_electric_util(
    electric_utility: ElectricUtility, utility_assignment: pl.LazyFrame
) -> list[int]:
    """
    Fetch all building ID's assigned to the given electric utility.

    Args:
        electric_utility: The electric utility to fetch prototype IDs for.
        utility_assignment: The utility assignment LazyFrame.

    Returns:
        A list of building IDs assigned to the given electric utility.
    """
    if "sb.electric_utility" not in utility_assignment.collect_schema().names():
        raise ValueError("sb.electric_utility column not found in utility assignment")
    utility_assignment = utility_assignment.filter(
        pl.col("sb.electric_utility") == electric_utility
    )
    bldg_ids = cast(
        pl.DataFrame,
        utility_assignment.select("bldg_id").collect(),
    )
    if bldg_ids.height == 0:
        raise ValueError(f"No buildings assigned to {electric_utility}")
    return cast(list[int], bldg_ids["bldg_id"].to_list())


# ---------------------------------------------------------------------------
# Delivery-side marginal cost loaders
# ---------------------------------------------------------------------------


def load_dist_and_sub_tx_marginal_costs(
    path: str | Path,
) -> pd.Series:
    """Load *distribution + upstream (Tx/Sub-Tx)* marginal costs and return a tz-aware Series.

    The parquet this function loads is produced by
    `utils/pre/marginal_costs/generate_utility_tx_dx_mc.py`
    and contains:
    - `mc_upstream_per_kwh` (Tx/Sub-Tx component, $/kWh)
    - `mc_dist_per_kwh` (distribution component, $/kWh)
    - `mc_total_per_kwh` (= upstream + distribution, $/kWh)

    We load `mc_total_per_kwh` and pass it to CAIRO as the delivery-side MC trace.
    Bulk transmission is handled separately via `load_bulk_tx_marginal_costs`.
    """
    df = cast(pl.DataFrame, _scan_parquet_pl(str(path)).collect()).to_pandas()
    required_cols = {"timestamp", "mc_total_per_kwh"}
    missing_cols = required_cols.difference(df.columns)
    if missing_cols:
        raise ValueError(
            "Dist+sub-tx marginal costs parquet is missing required columns "
            f"{sorted(required_cols)}. Missing: {sorted(missing_cols)}"
        )
    series = df.set_index("timestamp")["mc_total_per_kwh"]
    series.index = pd.DatetimeIndex(series.index).tz_localize("EST")
    series.index.name = "time"
    series.name = "Marginal Dist+Sub-Tx Costs ($/kWh)"
    return series


def load_bulk_tx_marginal_costs(
    path: str | Path,
) -> pd.Series:
    """Load bulk transmission marginal costs from a parquet path and return a tz-aware Series.

    Reads parquet with columns ``timestamp`` and ``bulk_tx_cost_enduse`` ($/kWh),
    TZ-localizes to EST, and returns a Series named
    ``"Marginal Bulk Transmission Costs ($/kWh)"``.

    The output mirrors the shape of :func:`load_dist_and_sub_tx_marginal_costs` so the
    caller can sum the two into a single delivery MC trace for CAIRO.

    Args:
        path: Local or S3 path to the bulk transmission MC parquet file.
            Expected schema: ``timestamp`` (datetime), ``bulk_tx_cost_enduse``
            (float, $/MWh), 8760 rows.

    Returns:
        ``pd.Series`` indexed by ``DatetimeIndex`` (EST-localized, name ``"time"``),
        values in $/kWh, series name ``"Marginal Bulk Transmission Costs ($/kWh)"``.

    Raises:
        FileNotFoundError: If the path does not exist.
        ValueError: If required columns are missing, data has nulls, or row count
            is not 8760.
    """
    df = cast(pl.DataFrame, _scan_parquet_pl(str(path)).collect()).to_pandas()

    required_cols = {"timestamp", "bulk_tx_cost_enduse"}
    missing_cols = required_cols.difference(df.columns)
    if missing_cols:
        raise ValueError(
            "Bulk transmission marginal costs parquet is missing required columns "
            f"{sorted(required_cols)}. Missing: {sorted(missing_cols)}"
        )

    if df["bulk_tx_cost_enduse"].isna().any():
        raise ValueError(
            "Bulk transmission marginal costs contain null values in bulk_tx_cost_enduse"
        )

    if len(df) != 8760:
        log.warning(
            "Bulk transmission MC has %d rows (expected 8760); proceeding anyway.",
            len(df),
        )

    series = df.set_index("timestamp")["bulk_tx_cost_enduse"]  # Already in $/kWh
    series.index = pd.DatetimeIndex(series.index).tz_localize("EST")
    series.index.name = "time"
    series.name = "Marginal Bulk Transmission Costs ($/kWh)"

    if (series < 0).any():
        log.warning(
            "Bulk transmission MC has %d negative values; check input data.",
            int((series < 0).sum()),
        )

    log.info(
        "Loaded bulk transmission MC: %d rows, annual sum = %.4f $/kW-yr, "
        "avg = %.6f $/kWh, max = %.6f $/kWh",
        len(series),
        float(series.sum()),
        float(series.mean()),
        float(series.max()),
    )
    return series


def _align_mc_to_index(
    mc_series: pd.Series,
    target_index: pd.DatetimeIndex,
    mc_type: str = "MC",
) -> pd.Series:
    """Align a marginal cost Series to a target DatetimeIndex.

    Handles index alignment when MC has a different year or length than the target.
    Preserves values by position when lengths match (common when MC file year differs
    from run year), otherwise reindexes.

    Args:
        mc_series: MC Series with DatetimeIndex to align.
        target_index: Target DatetimeIndex (typically from bulk MC).
        mc_type: Label for logging (e.g., "dist_and_sub_tx", "bulk_tx").

    Returns:
        MC Series aligned to target_index, preserving original name.
    """
    if mc_series.index.equals(target_index):
        return mc_series

    if len(mc_series) == len(target_index):
        # Same length but different timestamps: align by position
        log.info(
            "Aligned %s MC index to target (mc_year=%s, target_year=%s)",
            mc_type,
            mc_series.index[0].year,
            target_index[0].year,
        )
        return pd.Series(mc_series.values, index=target_index, name=mc_series.name)

    # Different lengths: reindex
    aligned = mc_series.reindex(target_index)
    log.info(
        "Reindexed %s MC to target (mc_rows=%s, target_rows=%s)",
        mc_type,
        len(mc_series),
        len(target_index),
    )
    return aligned


def add_bulk_tx_and_dist_and_sub_tx_marginal_cost(
    path_dist_and_sub_tx_mc: str | Path,
    path_bulk_tx_mc: str | Path | None,
    target_index: pd.DatetimeIndex,
) -> pd.Series:
    """Load and combine delivery marginal costs: Bulk Tx + Dist+Sub-Tx.

    Architecture:
    - Delivery MCs = Bulk Tx + Dist+Sub-Tx (both are delivery charges)
    - Supply MCs = Energy + Capacity (bulk supply, handled separately)
    All MCs must share the same DatetimeIndex for CAIRO compatibility.

    This function loads dist+sub-tx MC (required) and bulk transmission MC (optional),
    aligns both to the target_index, and sums them into a single delivery MC Series.

    Args:
        path_dist_and_sub_tx_mc: Path to dist+sub-tx MC parquet file.
        path_bulk_tx_mc: Optional path to bulk transmission MC parquet file.
        target_index: Target DatetimeIndex to align both MCs to (typically from supply MC index
            to ensure all MCs share the same 8760-hour index).

    Returns:
        Combined delivery MC Series (dist+sub-tx + bulk transmission) aligned to target_index,
        named "Marginal Distribution Costs ($/kWh)".

    Raises:
        ValueError: If combined series contains nulls after alignment.
    """
    dist_and_sub_tx_mc = _align_mc_to_index(
        load_dist_and_sub_tx_marginal_costs(path_dist_and_sub_tx_mc),
        target_index,
        "dist_and_sub_tx",
    )

    if path_bulk_tx_mc is not None and str(path_bulk_tx_mc).strip():
        bulk_tx_mc = _align_mc_to_index(
            load_bulk_tx_marginal_costs(path_bulk_tx_mc), target_index, "bulk_tx"
        )
        dist_sum = float(dist_and_sub_tx_mc.sum())
        bulk_tx_sum = float(bulk_tx_mc.sum())
        log.info(
            "Pre-merge delivery MC: Dist+Sub-Tx=%.4f $/kW-yr, Bulk Tx=%.4f $/kW-yr",
            dist_sum,
            bulk_tx_sum,
        )
        delivery_mc = dist_and_sub_tx_mc + bulk_tx_mc
        if delivery_mc.isna().any():
            raise ValueError(
                "Combined delivery MC (dist+sub-tx + bulk transmission) contains null values"
            )
        log.info(
            "Combined delivery MC: Total=%.4f $/kW-yr (Dist+Sub-Tx=%.4f + Bulk Tx=%.4f)",
            float(delivery_mc.sum()),
            dist_sum,
            bulk_tx_sum,
        )
    else:
        delivery_mc = dist_and_sub_tx_mc

    delivery_mc.name = "Marginal Distribution Costs ($/kWh)"
    return delivery_mc


# ---------------------------------------------------------------------------
# TOU / demand-response helpers
# ---------------------------------------------------------------------------

# Skip proportional allocation of period-level shifts when the building-period
# sum of `electricity_net` is non-positive or below this threshold (kWh). PV and
# near-net-zero TOU periods otherwise blow up `hour_share = kWh / Q_period`.
FLEX_SHIFT_MIN_PERIOD_ABS_KWH: float = 1.0


def _flex_shift_hour_share_from_groups(
    electricity_net: np.ndarray,
    q_orig: np.ndarray,
    n_in_group: np.ndarray,
    *,
    min_abs_kwh: float,
) -> np.ndarray:
    """Hour weights for spreading ``load_shift`` across rows in a building×period.

    When the period total is positive and at least ``min_abs_kwh``, use
    ``hourly_kWh / period_total`` (proportional to net). Otherwise use a uniform
    ``1/n_hours`` split so PV-dominated or near-zero net periods never divide by
    a tiny denominator, while weights still sum to 1 within the group (so
    period-level ``load_shift`` is fully allocated).
    """
    q = np.asarray(q_orig, dtype=np.float64)
    x = np.asarray(electricity_net, dtype=np.float64)
    n = np.maximum(np.asarray(n_in_group, dtype=np.float64), 1.0)
    safe_net = (q > 0.0) & (np.abs(q) >= float(min_abs_kwh))
    with np.errstate(divide="ignore", invalid="ignore"):
        out = np.where(safe_net, x / q, 1.0 / n)
    return np.where(np.isfinite(out), out, 0.0)


def _zero_unsafe_period_shifts_and_rebalance(
    targets: pd.DataFrame,
    *,
    receiver_period: int,
    min_abs_kwh: float,
) -> pd.DataFrame:
    """Drop shifts for unsafe building-periods; set receiver shift to close the sum per building."""
    out = targets.copy()
    unsafe = (out["Q_orig"] <= 0) | (out["Q_orig"].abs() < float(min_abs_kwh))
    out.loc[unsafe, "load_shift"] = 0.0
    recv_mask = out["energy_period"] == receiver_period
    donor_sum = out.loc[~recv_mask].groupby("bldg_id", sort=False)["load_shift"].sum()
    out.loc[recv_mask, "load_shift"] = (
        out.loc[recv_mask, "bldg_id"].map(-donor_sum).fillna(0.0).astype(np.float64)
    )
    return out


def extract_tou_period_rates(tou_tariff: dict) -> pd.DataFrame:
    """Extract period-level TOU rates from a URDB-style tariff.

    Args:
        tou_tariff: URDB v7 tariff dictionary with `energyratestructure`.

    Returns:
        DataFrame with columns:
        - `energy_period` (int)
        - `tier` (1-based int)
        - `rate` ($/kWh, including `adj`)
    """
    tariff_item = tou_tariff["items"][0]
    rate_structure = tariff_item["energyratestructure"]
    rows: list[dict[str, object]] = []
    for period_idx, tiers in enumerate(rate_structure):
        for tier_idx, tier_data in enumerate(tiers):
            rate = float(tier_data["rate"]) + float(tier_data.get("adj", 0.0))
            rows.append(
                {
                    "energy_period": period_idx,
                    "tier": tier_idx + 1,
                    "rate": rate,
                }
            )
    return pd.DataFrame(rows)


def assign_hourly_periods(
    hourly_index: pd.DatetimeIndex,
    tou_tariff: dict,
) -> pd.Series:
    """Map hourly timestamps to TOU `energy_period` values.

    Args:
        hourly_index: Hourly DatetimeIndex (typically one full year).
        tou_tariff: URDB v7 tariff dictionary with weekday/weekend schedules.

    Returns:
        Series indexed by `hourly_index` containing integer `energy_period`.
    """
    tariff_item = tou_tariff["items"][0]
    weekday_schedule = np.array(tariff_item["energyweekdayschedule"])
    weekend_schedule = np.array(tariff_item["energyweekendschedule"])

    months = np.asarray(hourly_index.month) - 1  # type: ignore[attr-defined]
    hours = np.asarray(hourly_index.hour)  # type: ignore[attr-defined]
    is_weekday = np.asarray(hourly_index.dayofweek) < 5  # type: ignore[attr-defined]

    periods = np.where(
        is_weekday, weekday_schedule[months, hours], weekend_schedule[months, hours]
    )
    return pd.Series(periods, index=hourly_index, name="energy_period", dtype=int)


def _build_period_consumption(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate baseline consumption by building and tariff period.

    Args:
        hourly_df: DataFrame with `bldg_id`, `energy_period`, `electricity_net`.

    Returns:
        DataFrame with one row per `(bldg_id, energy_period)` and `Q_orig`.
        Missing period combinations are zero-filled for stable downstream math.
    """
    observed_periods = sorted(hourly_df["energy_period"].dropna().unique())
    bldg_ids = pd.Index(hourly_df["bldg_id"].unique(), name="bldg_id")
    scaffold = pd.MultiIndex.from_product(
        [bldg_ids, pd.Index(observed_periods, name="energy_period")],
        names=["bldg_id", "energy_period"],
    )
    return (
        hourly_df.groupby(["bldg_id", "energy_period"])["electricity_net"]
        .sum()
        .reindex(scaffold, fill_value=0.0)
        .rename("Q_orig")
        .reset_index()
    )


def _compute_equivalent_flat_tariff(
    period_consumption: pd.DataFrame,
    period_rate: pd.Series,
) -> float:
    """Compute endogenous equivalent flat rate for one customer-class slice.

    Args:
        period_consumption: Building-period baseline demand with `Q_orig`.
        period_rate: Series mapping `energy_period -> rate`.

    Returns:
        Endogenous flat comparator price for the active customer slice:
        `sum_t(Q_t * P_t) / sum_t(Q_t)`.
    """
    rates = period_rate.rename("rate").reset_index()
    class_period = (
        period_consumption.groupby("energy_period", as_index=False)["Q_orig"].sum()
    ).merge(rates, on="energy_period", how="left")
    total_demand = float(class_period["Q_orig"].sum())
    if total_demand <= 0:
        raise ValueError("Cannot compute equivalent flat tariff with zero demand.")
    return float((class_period["Q_orig"] * class_period["rate"]).sum() / total_demand)


def _build_period_shift_targets(
    period_consumption: pd.DataFrame,
    period_rate: pd.Series,
    demand_elasticity: float,
    equivalent_flat_tariff: float,
    receiver_period: int | None,
) -> pd.DataFrame:
    """Build period-level shift targets under constant elasticity.

    Args:
        period_consumption: Building-period demand with `Q_orig`.
        period_rate: Series mapping `energy_period -> rate`.
        demand_elasticity: Constant demand elasticity parameter.
        equivalent_flat_tariff: Comparator flat rate for elasticity response.
        receiver_period: Optional sink period; if omitted, lowest-rate period used.

    Returns:
        DataFrame with per-building/period target demand and `load_shift`.
    """
    targets = period_consumption.merge(
        period_rate.rename("rate").reset_index(), on="energy_period", how="left"
    )
    targets["Q_target"] = targets["Q_orig"] * (
        (targets["rate"] / equivalent_flat_tariff) ** demand_elasticity
    )
    targets["load_shift"] = targets["Q_target"] - targets["Q_orig"]
    # Zero-sum: receiver period absorbs the negated sum of donor shifts.
    recv_period = (
        int(targets.loc[targets["rate"].idxmin(), "energy_period"])
        if receiver_period is None
        else receiver_period
    )
    donor_shift = (
        targets[targets["energy_period"] != recv_period]
        .groupby("bldg_id")["load_shift"]
        .sum()
    )
    recv_mask = targets["energy_period"] == recv_period
    targets.loc[recv_mask, "load_shift"] = (
        targets.loc[recv_mask, "bldg_id"].map(-donor_shift).fillna(0.0)
    )
    return targets


def _shift_building_hourly_demand(
    bldg_hourly_df: pd.DataFrame,
    period_targets: pd.DataFrame,
    equivalent_flat_tariff: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """CAIRO-style worker: allocate period shifts to hourly rows for one building.

    Args:
        bldg_hourly_df: One building's hourly load rows with `energy_period`.
        period_targets: Building-level period shift targets.
        equivalent_flat_tariff: Comparator flat rate used in elasticity math.

    Returns:
        Tuple of:
        - shifted hourly DataFrame (`shifted_net`, `hourly_shift`)
        - period-level elasticity tracker DataFrame
    """
    shifted = bldg_hourly_df.merge(
        period_targets[["energy_period", "Q_orig", "load_shift", "rate"]],
        on="energy_period",
        how="left",
    )
    _n = shifted.groupby(["bldg_id", "energy_period"], sort=False)[
        "electricity_net"
    ].transform("size")
    shifted["hour_share"] = _flex_shift_hour_share_from_groups(
        shifted["electricity_net"].to_numpy(),
        shifted["Q_orig"].to_numpy(),
        _n.to_numpy(),
        min_abs_kwh=FLEX_SHIFT_MIN_PERIOD_ABS_KWH,
    )
    shifted["hourly_shift"] = shifted["load_shift"].fillna(0.0) * shifted["hour_share"]
    shifted["shifted_net"] = shifted["electricity_net"] + shifted["hourly_shift"]

    tracker = shifted.groupby(["bldg_id", "energy_period"], as_index=False).agg(
        Q_orig=("electricity_net", "sum"),
        Q_new=("shifted_net", "sum"),
        rate=("rate", "first"),
    )
    valid = (
        (tracker["Q_new"] > 0)
        & (tracker["Q_orig"] > 0)
        & (tracker["rate"] != equivalent_flat_tariff)
    )
    tracker["epsilon"] = np.nan
    tracker.loc[valid, "epsilon"] = np.log(
        tracker.loc[valid, "Q_new"] / tracker.loc[valid, "Q_orig"]
    ) / np.log(tracker.loc[valid, "rate"] / equivalent_flat_tariff)
    return shifted, tracker


def process_residential_hourly_demand_response_shift(
    hourly_load_df: pd.DataFrame,
    period_rate: pd.Series,
    demand_elasticity: float,
    equivalent_flat_tariff: float | None = None,
    receiver_period: int | None = None,
    *,
    min_period_abs_kwh: float | None = None,
) -> tuple[np.ndarray, np.ndarray, pd.DataFrame]:
    """CAIRO-style parent function for demand-response load shifting.

    Args:
        hourly_load_df: Hourly TOU-cohort load with `bldg_id`, `energy_period`,
            and `electricity_net`.
        period_rate: Series mapping `energy_period -> rate`.
        demand_elasticity: Constant demand elasticity parameter.
        equivalent_flat_tariff: Optional comparator flat rate. If omitted,
            computed endogenously from the active slice.
        receiver_period: Optional sink period for zero-sum balancing.
        min_period_abs_kwh: Minimum positive period sum (kWh) required before
            applying shifts for that building-period; defaults to
            ``FLEX_SHIFT_MIN_PERIOD_ABS_KWH``.

    Returns:
        Tuple of:
        - shifted_net values (numpy array, same length as hourly_load_df)
        - hourly_shift values (numpy array)
        - period-level elasticity tracker DataFrame
    """
    if hourly_load_df.empty:
        return np.array([]), np.array([]), pd.DataFrame()

    min_k = (
        float(min_period_abs_kwh)
        if min_period_abs_kwh is not None
        else float(FLEX_SHIFT_MIN_PERIOD_ABS_KWH)
    )

    period_consumption = _build_period_consumption(hourly_load_df)
    flat_tariff = (
        _compute_equivalent_flat_tariff(period_consumption, period_rate)
        if equivalent_flat_tariff is None
        else equivalent_flat_tariff
    )
    period_targets = _build_period_shift_targets(
        period_consumption=period_consumption,
        period_rate=period_rate,
        demand_elasticity=demand_elasticity,
        equivalent_flat_tariff=flat_tariff,
        receiver_period=receiver_period,
    )
    recv_period = (
        int(period_targets.loc[period_targets["rate"].idxmin(), "energy_period"])
        if receiver_period is None
        else int(receiver_period)
    )
    period_targets = _zero_unsafe_period_shifts_and_rebalance(
        period_targets,
        receiver_period=recv_period,
        min_abs_kwh=min_k,
    )

    # Broadcast period-level targets to hourly rows without a full merge.
    # period_targets is small (~buildings × periods); hourly_load_df can be
    # tens of millions of rows.  A merge would double memory; instead we use
    # groupby.transform for Q_orig and a keyed lookup for load_shift.
    pt_idx = period_targets.set_index(["bldg_id", "energy_period"])
    load_shift_map = pt_idx["load_shift"].to_dict()

    grp = hourly_load_df.groupby(["bldg_id", "energy_period"], sort=False)
    q_orig = grp["electricity_net"].transform("sum")
    n_in_group = grp["electricity_net"].transform("size")

    hourly_keys = tuple(
        zip(hourly_load_df["bldg_id"], hourly_load_df["energy_period"], strict=False)
    )
    load_shift_arr = np.array(
        [load_shift_map.get(k, 0.0) for k in hourly_keys], dtype=np.float64
    )
    del hourly_keys

    hour_share = _flex_shift_hour_share_from_groups(
        hourly_load_df["electricity_net"].to_numpy(),
        q_orig.to_numpy(),
        n_in_group.to_numpy(),
        min_abs_kwh=min_k,
    )
    hourly_shift = load_shift_arr * hour_share
    shifted_net = hourly_load_df["electricity_net"].values + hourly_shift

    # Tracker: period-level elasticity diagnostics (tiny).
    tracker = (
        pd.DataFrame(
            {
                "bldg_id": hourly_load_df["bldg_id"],
                "energy_period": hourly_load_df["energy_period"],
                "electricity_net": hourly_load_df["electricity_net"],
                "shifted_net": shifted_net,
            }
        )
        .groupby(["bldg_id", "energy_period"], as_index=False)
        .agg(Q_orig=("electricity_net", "sum"), Q_new=("shifted_net", "sum"))
    )
    rate_map = period_rate.to_dict()
    tracker["rate"] = tracker["energy_period"].map(rate_map)
    valid = (
        (tracker["Q_new"] > 0)
        & (tracker["Q_orig"] > 0)
        & (tracker["rate"] != flat_tariff)
    )
    tracker["epsilon"] = np.nan
    tracker.loc[valid, "epsilon"] = np.log(
        tracker.loc[valid, "Q_new"] / tracker.loc[valid, "Q_orig"]
    ) / np.log(tracker.loc[valid, "rate"] / flat_tariff)

    return shifted_net, hourly_shift, tracker


def _infer_season_groups_from_tariff(
    period_map: pd.Series,
) -> list[dict[str, object]]:
    """Infer season groups from month-specific period signatures in the tariff.

    Args:
        period_map: Series indexed by time with integer `energy_period`.

    Returns:
        List of season group dictionaries (`name`, `months`). Returns an empty
        list when tariff structure is effectively full-year.
    """
    month_periods = (
        period_map.to_frame(name="energy_period")
        .assign(month=lambda frame: frame.index.month)
        .groupby("month")["energy_period"]
        .unique()
        .apply(lambda values: tuple(sorted(int(v) for v in values)))
    )
    grouped: dict[tuple[int, ...], list[int]] = {}
    for month, period_tuple in month_periods.items():
        key = cast(tuple[int, ...], period_tuple)
        grouped.setdefault(key, []).append(int(cast(int, month)))
    if len(grouped) <= 1:
        return []

    groups: list[dict[str, object]] = []
    for idx, (periods, months) in enumerate(
        sorted(grouped.items(), key=lambda item: min(item[1]))
    ):
        if not periods:
            continue
        groups.append({"name": f"season_{idx + 1}", "months": sorted(months)})
    return groups


def apply_runtime_tou_demand_response(
    raw_load_elec: pd.DataFrame,
    tou_bldg_ids: list[int],
    tou_tariff: dict,
    demand_elasticity: float | dict[str, float],
    season_specs: list | None = None,
    *,
    inplace: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply runtime TOU demand response to the assigned TOU customer cohort.

    Args:
        raw_load_elec: Full electric load DataFrame indexed by `(bldg_id, time)`.
        tou_bldg_ids: Building IDs assigned to the TOU tariff.
        tou_tariff: URDB v7 tariff dictionary.
        demand_elasticity: Constant demand elasticity parameter, or a
            ``{season_name: elasticity}`` dict for per-season values.
        season_specs: Optional season definitions for seasonal slicing.
        inplace: If True, mutate *raw_load_elec* directly instead of copying.
            The caller is responsible for passing an already-copied DataFrame.

    Returns:
        Tuple of:
        - shifted load DataFrame (same object as input when *inplace=True*)
        - pivoted elasticity tracker DataFrame by building
    """
    _log_rss("apply_runtime_tou_demand_response entry")
    if not tou_bldg_ids:
        if inplace:
            return raw_load_elec, pd.DataFrame()
        return raw_load_elec.copy(), pd.DataFrame()

    bldg_level = raw_load_elec.index.get_level_values("bldg_id")
    tou_set = set(tou_bldg_ids)
    tou_mask = bldg_level.isin(tou_set)
    if not tou_mask.any():
        log.warning("No TOU buildings found in load data; skipping demand response.")
        if inplace:
            return raw_load_elec, pd.DataFrame()
        return raw_load_elec.copy(), pd.DataFrame()

    # Capture original total before any mutation (needed for logging).
    original_total = raw_load_elec.loc[tou_mask, "electricity_net"].sum()

    rate_df = extract_tou_period_rates(tou_tariff)
    period_rate = cast(pd.Series, rate_df.groupby("energy_period")["rate"].first())
    time_idx = pd.DatetimeIndex(
        raw_load_elec.index.get_level_values("time").unique().sort_values()
    )
    period_map = assign_hourly_periods(time_idx, tou_tariff)

    # Period lookup: 8760 rows (tiny), used to tag each season slice.
    period_lookup = period_map.reset_index()
    period_lookup.columns = pd.Index(["time", "energy_period"])

    shifted_load_elec = raw_load_elec if inplace else raw_load_elec.copy()
    _log_rss("after shifted_load_elec " + ("(inplace)" if inplace else "copy"))

    # Determine season groups from explicit specs, tariff-inferred, or full-year.
    if season_specs:
        season_groups: list[dict[str, object]] = [
            {"name": str(spec.season.name), "months": list(spec.season.months)}
            for spec in season_specs
        ]
    else:
        season_groups = _infer_season_groups_from_tariff(period_map)

    trackers: list[pd.DataFrame] = []
    time_level = pd.DatetimeIndex(shifted_load_elec.index.get_level_values("time"))
    month_level = time_level.month  # type: ignore[attr-defined]
    has_load_data = "load_data" in shifted_load_elec.columns

    def _shift_season(season_name: str, season_months: set[int]) -> None:
        """Shift one season's TOU rows in-place on shifted_load_elec."""
        season_eps = (
            demand_elasticity.get(season_name, 0.0)
            if isinstance(demand_elasticity, dict)
            else demand_elasticity
        )
        if season_eps == 0.0:
            return

        mask = bldg_level.isin(tou_set) & month_level.isin(season_months)
        season_df = (
            shifted_load_elec.loc[mask, ["electricity_net"]].copy().reset_index()
        )
        if season_df.empty:
            return
        season_df = season_df.merge(period_lookup, on="time", how="left")
        if season_df["energy_period"].dropna().empty:
            return
        _log_rss(f"  season '{season_name}' slice ready ({len(season_df)} rows)")

        shifted_net, hourly_shift_arr, tracker = (
            process_residential_hourly_demand_response_shift(
                hourly_load_df=season_df,
                period_rate=period_rate,
                demand_elasticity=season_eps,
            )
        )
        tracker["season"] = season_name
        trackers.append(tracker)

        # Write shifted values back in-place via the MultiIndex.
        idx = pd.MultiIndex.from_arrays(
            [season_df["bldg_id"], season_df["time"]],
            names=["bldg_id", "time"],
        )
        del season_df
        shifted_load_elec.loc[idx, "electricity_net"] = shifted_net
        if has_load_data:
            shifted_load_elec.loc[idx, "load_data"] += hourly_shift_arr

        del shifted_net, hourly_shift_arr, idx
        _log_rss(f"  season '{season_name}' writeback done")

    if season_groups:
        for season_group in season_groups:
            _shift_season(
                str(season_group["name"]),
                set(cast(list[int], season_group["months"])),
            )
    else:
        all_months = set(range(1, 13))
        _shift_season("all_year", all_months)

    # Build elasticity tracker pivot.
    if trackers:
        tracker_df = pd.concat(trackers, ignore_index=True)
        tracker_df["period_label"] = tracker_df.apply(
            lambda row: f"{row['season']}_period_{int(row['energy_period'])}", axis=1
        )
        elasticity_tracker = tracker_df.pivot(
            index="bldg_id", columns="period_label", values="epsilon"
        )
    else:
        elasticity_tracker = pd.DataFrame()

    shifted_total = shifted_load_elec.loc[tou_mask, "electricity_net"].sum()
    log.info(
        "Runtime demand response complete: bldgs=%d, elasticity=%s, "
        "original=%.0f, shifted=%.0f, diff=%.2f",
        len(tou_bldg_ids),
        demand_elasticity,
        original_total,
        shifted_total,
        shifted_total - original_total,
    )
    _log_rss("apply_runtime_tou_demand_response exit")
    return shifted_load_elec, elasticity_tracker
