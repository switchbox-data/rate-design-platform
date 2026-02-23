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


def _normalize_cambium_path(cambium_scenario: CambiumPathLike):  # noqa: ANN201
    """Return a single path-like (Path or S3Path) for Cambium CSV or Parquet."""
    if isinstance(cambium_scenario, S3Path):
        return cambium_scenario
    if isinstance(cambium_scenario, Path):
        return cambium_scenario
    if isinstance(cambium_scenario, str):
        if cambium_scenario.startswith("s3://"):
            return S3Path(cambium_scenario)
        if "/" in cambium_scenario or cambium_scenario.endswith((".csv", ".parquet")):
            return Path(cambium_scenario)
        return config.MARGINALCOST_DIR / f"{cambium_scenario}.csv"
    raise TypeError(
        f"cambium_scenario must be str, Path, or S3Path; got {type(cambium_scenario)}"
    )


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
    path = _normalize_cambium_path(cambium_scenario)
    if not path.exists():
        raise FileNotFoundError(f"Cambium marginal cost file {path} does not exist")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        if isinstance(path, S3Path):
            raw = path.read_bytes()
            df = pd.read_csv(
                io.BytesIO(raw),
                skiprows=5,
                index_col="timestamp",
                parse_dates=True,
            )
        else:
            df = pd.read_csv(
                path,
                skiprows=5,
                index_col="timestamp",
                parse_dates=True,
            )
    elif suffix == ".parquet":
        if isinstance(path, S3Path):
            # Read as bytes and pass BytesIO so PyArrow reads a single file. If we pass
            # the S3 path (even with explicit S3FileSystem), PyArrow infers a partitioned
            # dataset from path segments like scenario=MidCase/... and raises
            # ArrowTypeError when merging partition schemas.
            raw = path.read_bytes()
            df = pd.read_parquet(io.BytesIO(raw), engine="pyarrow")
        else:
            df = pd.read_parquet(path)
        if "timestamp" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
            df = df.set_index("timestamp")
        if df.index.name != "time":
            df.index.name = "time"
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

        if return_path_base is None:
            filepath = parquet_file
        else:
            filepath = return_path_base / parquet_file.name

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


def load_distribution_marginal_costs(
    path: str | Path,
) -> pd.Series:
    """Load distribution marginal costs from a parquet path and return a tz-aware Series."""
    path_str = str(path)
    if path_str.startswith("s3://"):
        distribution_mc_scan: pl.LazyFrame = pl.scan_parquet(
            path_str,
            storage_options=get_aws_storage_options(),
        )
    else:
        distribution_mc_scan = pl.scan_parquet(path_str)
    distribution_mc_df = cast(pl.DataFrame, distribution_mc_scan.collect())
    distribution_marginal_costs = distribution_mc_df.to_pandas()
    required_cols = {"timestamp", "mc_total_per_kwh"}
    missing_cols = required_cols.difference(distribution_marginal_costs.columns)
    if missing_cols:
        raise ValueError(
            "Distribution marginal costs parquet is missing required columns "
            f"{sorted(required_cols)}. Missing: {sorted(missing_cols)}"
        )
    distribution_marginal_costs = distribution_marginal_costs.set_index("timestamp")[
        "mc_total_per_kwh"
    ]
    distribution_marginal_costs.index = pd.DatetimeIndex(
        distribution_marginal_costs.index
    ).tz_localize("EST")
    distribution_marginal_costs.index.name = "time"
    distribution_marginal_costs.name = "Marginal Distribution Costs ($/kWh)"
    return distribution_marginal_costs


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
    shifted["hour_share"] = np.where(
        shifted["Q_orig"].abs() > 0,
        shifted["electricity_net"] / shifted["Q_orig"],
        0.0,
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
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """CAIRO-style parent function for demand-response load shifting.

    Args:
        hourly_load_df: Hourly TOU-cohort load with `bldg_id`, `energy_period`,
            and `electricity_net`.
        period_rate: Series mapping `energy_period -> rate`.
        demand_elasticity: Constant demand elasticity parameter.
        equivalent_flat_tariff: Optional comparator flat rate. If omitted,
            computed endogenously from the active slice.
        receiver_period: Optional sink period for zero-sum balancing.

    Returns:
        Tuple of:
        - shifted hourly DataFrame for the slice
        - period-level elasticity tracker DataFrame
    """
    if hourly_load_df.empty:
        return hourly_load_df.copy(), pd.DataFrame()

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

    shifted_chunks: list[pd.DataFrame] = []
    tracker_chunks: list[pd.DataFrame] = []
    for bldg_id, bldg_hourly in hourly_load_df.groupby("bldg_id", sort=False):
        bldg_targets = period_targets[period_targets["bldg_id"] == bldg_id]
        shifted_bldg, tracker_bldg = _shift_building_hourly_demand(
            bldg_hourly_df=bldg_hourly,
            period_targets=bldg_targets,
            equivalent_flat_tariff=flat_tariff,
        )
        shifted_chunks.append(shifted_bldg)
        tracker_chunks.append(tracker_bldg)

    return (
        pd.concat(shifted_chunks, ignore_index=True),
        pd.concat(tracker_chunks, ignore_index=True),
    )


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
    ordered_groups = sorted(grouped.items(), key=lambda item: min(item[1]))
    for idx, (periods, months) in enumerate(ordered_groups):
        if not periods:
            continue
        groups.append(
            {
                "name": f"season_{idx + 1}",
                "months": sorted(months),
            }
        )
    return groups


def apply_runtime_tou_demand_response(
    raw_load_elec: pd.DataFrame,
    tou_bldg_ids: list[int],
    tou_tariff: dict,
    demand_elasticity: float,
    season_specs: list | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply runtime TOU demand response to the assigned TOU customer cohort.

    Args:
        raw_load_elec: Full electric load DataFrame indexed by `(bldg_id, time)`.
        tou_bldg_ids: Building IDs assigned to the TOU tariff.
        tou_tariff: URDB v7 tariff dictionary.
        demand_elasticity: Constant demand elasticity parameter.
        season_specs: Optional season definitions for seasonal slicing.

    Returns:
        Tuple of:
        - full shifted load DataFrame (`raw_load_elec` shape preserved)
        - pivoted elasticity tracker DataFrame by building
    """
    if not tou_bldg_ids:
        return raw_load_elec.copy(), pd.DataFrame()

    # Only TOU-assigned buildings are shifted; others pass through unchanged.
    bldg_level = raw_load_elec.index.get_level_values("bldg_id")
    tou_mask = bldg_level.isin(set(tou_bldg_ids))
    if not tou_mask.any():
        log.warning("No TOU buildings found in load data; skipping demand response.")
        return raw_load_elec.copy(), pd.DataFrame()

    rate_df = extract_tou_period_rates(tou_tariff)
    period_rate = cast(pd.Series, rate_df.groupby("energy_period")["rate"].first())
    time_idx = pd.DatetimeIndex(
        raw_load_elec.index.get_level_values("time").unique().sort_values()
    )
    period_map = assign_hourly_periods(time_idx, tou_tariff)

    tou_df = raw_load_elec.loc[tou_mask, ["electricity_net"]].copy().reset_index()
    period_df = period_map.reset_index()
    period_df.columns = ["time", "energy_period"]
    tou_df = tou_df.merge(period_df, on="time", how="left")
    tou_df["month"] = tou_df["time"].dt.month

    # Shift per-season so energy conservation holds within each slice.
    # Season groups come from explicit specs, tariff-inferred months, or
    # fall back to full-year as a single group.
    shifted_chunks: list[pd.DataFrame] = []
    trackers: list[pd.DataFrame] = []
    season_groups: list[dict[str, object]] = []
    if season_specs:
        for spec in season_specs:
            season_groups.append(
                {
                    "name": str(spec.season.name),
                    "months": list(spec.season.months),
                }
            )
    else:
        # For seasonal+TOU tariffs without explicit derivation specs, infer
        # month groups directly from tariff month->period structure.
        season_groups = _infer_season_groups_from_tariff(period_map)

    if season_groups:
        for season_group in season_groups:
            season_name = str(season_group["name"])
            season_months = set(cast(list[int], season_group["months"]))
            season_df = tou_df[tou_df["month"].isin(season_months)].copy()
            if season_df.empty:
                continue
            season_periods = sorted(season_df["energy_period"].dropna().unique())
            if not season_periods:
                continue
            shifted_season, tracker = process_residential_hourly_demand_response_shift(
                hourly_load_df=season_df,
                period_rate=period_rate,
                demand_elasticity=demand_elasticity,
            )
            tracker["season"] = season_name
            shifted_chunks.append(shifted_season)
            trackers.append(tracker)
    else:
        # Non-seasonal tariff: shift across the full year as one group.
        if not tou_df["energy_period"].dropna().empty:
            shifted_year, tracker = process_residential_hourly_demand_response_shift(
                hourly_load_df=tou_df,
                period_rate=period_rate,
                demand_elasticity=demand_elasticity,
            )
            tracker["season"] = "all_year"
            shifted_chunks.append(shifted_year)
            trackers.append(tracker)

    # Merge shifted TOU rows back; non-TOU buildings are untouched.
    shifted_load_elec = raw_load_elec.copy()
    if shifted_chunks:
        shifted = pd.concat(shifted_chunks, ignore_index=True).set_index(
            ["bldg_id", "time"]
        )
        shifted = shifted.sort_index()
        shifted_load_elec.loc[shifted.index, "electricity_net"] = shifted[
            "shifted_net"
        ].to_numpy()
        if "load_data" in shifted_load_elec.columns:
            shifted_load_elec.loc[shifted.index, "load_data"] = (
                shifted_load_elec.loc[shifted.index, "load_data"]
                + shifted["hourly_shift"].to_numpy()
            )

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

    original_total = raw_load_elec.loc[tou_mask, "electricity_net"].sum()
    shifted_total = shifted_load_elec.loc[tou_mask, "electricity_net"].sum()
    log.info(
        "Runtime demand response complete: bldgs=%d, elasticity=%.3f, original=%.0f, shifted=%.0f, diff=%.2f",
        len(tou_bldg_ids),
        demand_elasticity,
        original_total,
        shifted_total,
        shifted_total - original_total,
    )
    return shifted_load_elec, elasticity_tracker
