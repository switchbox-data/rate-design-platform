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

from utils.types import ElectricUtility
from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

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
    """Extract period-level energy rates from a URDB v7 tariff JSON."""
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
    """Map each timestamp to its TOU energy period using URDB schedules."""
    tariff_item = tou_tariff["items"][0]
    weekday_schedule = np.array(tariff_item["energyweekdayschedule"])
    weekend_schedule = np.array(tariff_item["energyweekendschedule"])

    months = np.asarray(hourly_index.month) - 1
    hours = np.asarray(hourly_index.hour)
    is_weekday = np.asarray(hourly_index.dayofweek) < 5

    periods = np.where(
        is_weekday, weekday_schedule[months, hours], weekend_schedule[months, hours]
    )
    return pd.Series(periods, index=hourly_index, name="energy_period", dtype=int)


def _apply_period_shift_core(
    hourly_df: pd.DataFrame,
    period_rate: pd.Series,
    demand_elasticity: float,
    equivalent_flat_rate: float,
    receiver_period: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Core load-shifting engine over a single timeslice."""
    if hourly_df.empty:
        return hourly_df.copy(), pd.DataFrame()

    df = hourly_df.copy()
    observed_periods = sorted(df["energy_period"].unique())
    bldg_ids = pd.Index(df["bldg_id"].unique(), name="bldg_id")
    period_idx = pd.Index(observed_periods, name="energy_period")
    scaffold = pd.MultiIndex.from_product(
        [bldg_ids, period_idx], names=["bldg_id", "energy_period"]
    )

    period_consumption = (
        df.groupby(["bldg_id", "energy_period"])["electricity_net"]
        .sum()
        .reindex(scaffold, fill_value=0.0)
        .rename("Q_orig")
        .reset_index()
    )
    period_rates = period_rate.loc[observed_periods].rename("rate").reset_index()
    period_consumption = period_consumption.merge(period_rates, on="energy_period")
    period_consumption["Q_target"] = period_consumption["Q_orig"] * (
        (period_consumption["rate"] / equivalent_flat_rate) ** demand_elasticity
    )
    period_consumption["load_shift"] = (
        period_consumption["Q_target"] - period_consumption["Q_orig"]
    )

    recv_period = (
        int(period_rates.loc[period_rates["rate"].idxmin(), "energy_period"])
        if receiver_period is None
        else receiver_period
    )
    donor_mask = period_consumption["energy_period"] != recv_period
    donor_shift = period_consumption.loc[donor_mask].groupby("bldg_id")[
        "load_shift"
    ].sum()
    receiver_mask = period_consumption["energy_period"] == recv_period
    period_consumption.loc[receiver_mask, "load_shift"] = (
        period_consumption.loc[receiver_mask, "bldg_id"].map(-donor_shift).fillna(0.0)
    )

    df = df.merge(
        period_consumption[["bldg_id", "energy_period", "Q_orig", "load_shift"]],
        on=["bldg_id", "energy_period"],
        how="left",
    )
    df["hour_share"] = np.where(
        df["Q_orig"].abs() > 0,
        df["electricity_net"] / df["Q_orig"],
        0.0,
    )
    df["hourly_shift"] = df["load_shift"].fillna(0.0) * df["hour_share"]
    df["shifted_net"] = df["electricity_net"] + df["hourly_shift"]

    tracker = df.groupby(["bldg_id", "energy_period"], as_index=False).agg(
        Q_new=("shifted_net", "sum")
    )
    tracker = tracker.merge(
        period_consumption[["bldg_id", "energy_period", "Q_orig", "rate"]],
        on=["bldg_id", "energy_period"],
        how="left",
    )
    valid = (
        (tracker["Q_new"] > 0)
        & (tracker["Q_orig"] > 0)
        & (tracker["rate"] != equivalent_flat_rate)
    )
    tracker["epsilon"] = np.nan
    tracker.loc[valid, "epsilon"] = np.log(
        tracker.loc[valid, "Q_new"] / tracker.loc[valid, "Q_orig"]
    ) / np.log(tracker.loc[valid, "rate"] / equivalent_flat_rate)

    return df, tracker


def _infer_season_groups_from_tariff(
    period_map: pd.Series, period_rate: pd.Series
) -> list[dict[str, object]]:
    """Infer seasonal month groups from month->period assignments."""
    month_periods = (
        period_map.to_frame(name="energy_period")
        .assign(month=lambda frame: frame.index.month)
        .groupby("month")["energy_period"]
        .unique()
        .apply(lambda values: tuple(sorted(int(v) for v in values)))
    )
    grouped: dict[tuple[int, ...], list[int]] = {}
    for month, period_tuple in month_periods.items():
        grouped.setdefault(period_tuple, []).append(int(month))
    if len(grouped) <= 1:
        return []

    groups: list[dict[str, object]] = []
    ordered_groups = sorted(grouped.items(), key=lambda item: min(item[1]))
    for idx, (periods, months) in enumerate(ordered_groups):
        if not periods:
            continue
        base_rate = min(float(period_rate.loc[p]) for p in periods)
        groups.append(
            {
                "name": f"season_{idx + 1}",
                "months": sorted(months),
                "base_rate": base_rate,
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
    """Apply runtime demand response for TOU cohort (full-year or seasonal wrapper)."""
    if not tou_bldg_ids:
        return raw_load_elec.copy(), pd.DataFrame()

    bldg_level = raw_load_elec.index.get_level_values("bldg_id")
    tou_mask = bldg_level.isin(set(tou_bldg_ids))
    if not tou_mask.any():
        log.warning("No TOU buildings found in load data; skipping demand response.")
        return raw_load_elec.copy(), pd.DataFrame()

    rate_df = extract_tou_period_rates(tou_tariff)
    period_rate = rate_df.groupby("energy_period")["rate"].first()
    time_idx = pd.DatetimeIndex(
        raw_load_elec.index.get_level_values("time").unique().sort_values()
    )
    period_map = assign_hourly_periods(time_idx, tou_tariff)

    tou_df = raw_load_elec.loc[tou_mask, ["electricity_net"]].copy().reset_index()
    period_df = period_map.reset_index()
    period_df.columns = ["time", "energy_period"]
    tou_df = tou_df.merge(period_df, on="time", how="left")
    tou_df["month"] = tou_df["time"].dt.month

    shifted_chunks: list[pd.DataFrame] = []
    trackers: list[pd.DataFrame] = []
    season_groups: list[dict[str, object]] = []
    if season_specs:
        for spec in season_specs:
            season_groups.append(
                {
                    "name": str(spec.season.name),
                    "months": list(spec.season.months),
                    "base_rate": float(spec.base_rate),
                }
            )
    else:
        season_groups = _infer_season_groups_from_tariff(period_map, period_rate)

    if season_groups:
        for season_group in season_groups:
            season_name = str(season_group["name"])
            season_months = set(cast(list[int], season_group["months"]))
            base_rate = float(season_group["base_rate"])
            season_df = tou_df[tou_df["month"].isin(season_months)].copy()
            if season_df.empty:
                continue
            season_periods = sorted(season_df["energy_period"].dropna().unique())
            if not season_periods:
                continue
            receiver_period = int(
                min(
                    season_periods,
                    key=lambda period: abs(float(period_rate.loc[period]) - base_rate),
                )
            )
            shifted_season, tracker = _apply_period_shift_core(
                season_df,
                period_rate=period_rate,
                demand_elasticity=demand_elasticity,
                equivalent_flat_rate=base_rate,
                receiver_period=receiver_period,
            )
            tracker["season"] = season_name
            shifted_chunks.append(shifted_season)
            trackers.append(tracker)
    else:
        observed_periods = sorted(tou_df["energy_period"].dropna().unique())
        if observed_periods:
            receiver_period = int(
                min(observed_periods, key=lambda period: float(period_rate.loc[period]))
            )
            equivalent_flat = float(period_rate.loc[receiver_period])
            shifted_year, tracker = _apply_period_shift_core(
                tou_df,
                period_rate=period_rate,
                demand_elasticity=demand_elasticity,
                equivalent_flat_rate=equivalent_flat,
                receiver_period=receiver_period,
            )
            tracker["season"] = "all_year"
            shifted_chunks.append(shifted_year)
            trackers.append(tracker)

    shifted_load_elec = raw_load_elec.copy()
    if shifted_chunks:
        shifted = pd.concat(shifted_chunks, ignore_index=True).set_index(["bldg_id", "time"])
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
