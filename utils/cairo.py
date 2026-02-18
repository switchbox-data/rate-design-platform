"""Utility functions for Cairo-related operations."""

from __future__ import annotations

import io
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from cairo.rates_tool import config
from cairo.rates_tool.loads import __timeshift__
from cloudpathlib import S3Path

log = logging.getLogger(__name__)

CambiumPathLike = str | Path | S3Path


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


def load_distribution_marginal_costs(
    state: str,
    region: str,
    utility: str,
    year_run: int,
) -> pd.Series:
    """Load distribution marginal costs from S3 and return as a tz-aware Series.

    Reads Hive-partitioned parquet from
    ``s3://data.sb/switchbox/marginal_costs/{state}/`` and filters to
    the requested *region*, *utility*, and *year_run*.

    Args:
        state: Two-letter state code (e.g. ``"RI"``).
        region: ISO region (e.g. ``"isone"``).
        utility: Utility short name (e.g. ``"rie"``).
        year_run: Target year for the marginal-cost data.

    Returns:
        Series of ``mc_total_per_kwh`` indexed by a tz-aware
        ``DatetimeIndex`` (EST, name ``"time"``).
    """
    import polars as pl

    from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

    distribution_mc_root = (
        f"s3://data.sb/switchbox/marginal_costs/{state.lower().strip('/')}/"
    )
    distribution_mc_scan: pl.LazyFrame = pl.scan_parquet(
        distribution_mc_root,
        hive_partitioning=True,
        storage_options=get_aws_storage_options(),
    )
    distribution_mc_scan = (
        distribution_mc_scan.filter(pl.col("region").cast(pl.Utf8) == region)
        .filter(pl.col("utility").cast(pl.Utf8) == utility)
        .filter(pl.col("year").cast(pl.Utf8) == str(year_run))
    )
    distribution_mc_df = distribution_mc_scan.collect()
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


# ---------------------------------------------------------------------------
# Demand shifting / demand flexibility
# ---------------------------------------------------------------------------


def extract_tou_period_rates(tou_tariff: dict) -> pd.DataFrame:
    """Extract period-level energy rates from a URDB v7 tariff JSON.

    Reads ``items[0].energyratestructure`` and returns a DataFrame with one row
    per (energy_period, tier) containing the effective rate (rate + adj).

    Args:
        tou_tariff: URDB v7 tariff dict (e.g. output of
            :func:`utils.pre.compute_tou.make_seasonal_tou_tariff`).

    Returns:
        DataFrame with columns ``energy_period`` (int), ``tier`` (int, 1-based),
        ``rate`` (float, $/kWh).
    """
    tariff_item = tou_tariff["items"][0]
    rate_structure = tariff_item["energyratestructure"]
    rows: list[dict[str, object]] = []
    for period_idx, tiers in enumerate(rate_structure):
        for tier_idx, tier_data in enumerate(tiers):
            rate = tier_data["rate"] + tier_data.get("adj", 0.0)
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
    """Map each timestamp in an 8760-hour index to its TOU energy period.

    Uses the ``energyweekdayschedule`` / ``energyweekendschedule`` matrices
    from the URDB v7 tariff definition (12 months × 24 hours, 0-indexed).

    Args:
        hourly_index: DatetimeIndex with 8760 hourly timestamps.
        tou_tariff: URDB v7 tariff dict.

    Returns:
        Series indexed by *hourly_index* with integer energy-period values.
    """
    tariff_item = tou_tariff["items"][0]
    wd = np.array(tariff_item["energyweekdayschedule"])  # (12, 24)
    we = np.array(tariff_item["energyweekendschedule"])  # (12, 24)

    months = np.asarray(hourly_index.month) - 1  # type: ignore[union-attr]  # 0-indexed
    hours = np.asarray(hourly_index.hour)  # type: ignore[union-attr]
    is_weekday = np.asarray(hourly_index.dayofweek) < 5  # type: ignore[union-attr]

    periods = np.where(is_weekday, wd[months, hours], we[months, hours])
    return pd.Series(periods, index=hourly_index, name="energy_period", dtype=int)


def apply_seasonal_demand_response(
    raw_load_elec: pd.DataFrame,
    tou_bldg_ids: list[int],
    tou_tariff: dict,
    demand_elasticity: float,
    season_specs: list,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply demand shifting independently within each season.

    For each ``SeasonTouSpec`` at index *i*, the tariff has two energy
    periods (as built by
    :func:`utils.pre.compute_tou.make_seasonal_tou_tariff`):

    * ``2·i``   — season off-peak (equiv flat rate = ``spec.base_rate``)
    * ``2·i+1`` — season peak

    Load shifting is applied separately within each season's months and the
    results are stitched into a single 8760 output.

    Args:
        raw_load_elec: Building loads with MultiIndex ``[bldg_id, time]``
            and column ``electricity_net``.
        tou_bldg_ids: Building IDs assigned to the seasonal TOU tariff.
        tou_tariff: URDB v7 tariff dict (as produced by
            :func:`utils.pre.compute_tou.make_seasonal_tou_tariff`).
        demand_elasticity: Price-elasticity coefficient (negative).
        season_specs: List of
            :class:`~utils.pre.compute_tou.SeasonTouSpec` — one per season,
            in the same order used to build the tariff.

    Returns:
        ``(shifted_load_elec, elasticity_tracker)`` tuple.
    """
    # -- 1. Extract period rates and build hour→period mapping ----------------
    rate_df = extract_tou_period_rates(tou_tariff)
    period_rate = rate_df.groupby("energy_period")["rate"].first()

    time_idx = pd.DatetimeIndex(
        raw_load_elec.index.get_level_values("time").unique().sort_values()
    )
    period_map = assign_hourly_periods(time_idx, tou_tariff)

    # -- 2. Isolate TOU-building loads ----------------------------------------
    tou_bldg_set = set(tou_bldg_ids)
    bldg_level = raw_load_elec.index.get_level_values("bldg_id")
    tou_mask = bldg_level.isin(tou_bldg_set)

    if not tou_mask.any():
        log.warning("No TOU buildings found in load data; skipping demand response")
        return raw_load_elec.copy(), pd.DataFrame()

    tou_df = raw_load_elec.loc[tou_mask, ["electricity_net"]].copy()
    tou_df = tou_df.reset_index()

    # Merge period assignment
    period_series = period_map.reset_index()
    period_series.columns = ["time", "energy_period"]
    tou_df = tou_df.merge(period_series, on="time")

    # Tag each row with its month for season filtering
    tou_df["month"] = tou_df["time"].dt.month

    # -- 3. Process each season independently ---------------------------------
    all_season_dfs: list[pd.DataFrame] = []
    all_tracker_dfs: list[pd.DataFrame] = []

    for i, spec in enumerate(season_specs):
        offpeak_period = 2 * i
        peak_period = 2 * i + 1
        month_set = set(spec.season.months)

        season_df = tou_df[tou_df["month"].isin(month_set)].copy()
        if season_df.empty:
            log.info("No %s hours found; skipping", spec.season.name)
            continue

        # Equivalent flat rate = the seasonal off-peak (base) rate
        equiv_flat = float(spec.base_rate)

        log.info(
            "Seasonal DR %s: equiv_flat=$%.6f/kWh, periods=[%d(offpeak), %d(peak)]",
            spec.season.name,
            equiv_flat,
            offpeak_period,
            peak_period,
        )

        # Period-level shift targets
        season_periods = [offpeak_period, peak_period]
        sc = (
            season_df.groupby(["bldg_id", "energy_period"], as_index=False)[
                "electricity_net"
            ]
            .sum()
            .rename(columns={"electricity_net": "Q_orig"})
        )
        sc = sc[sc["energy_period"].isin(season_periods)]
        pr_df = period_rate.loc[season_periods].reset_index()
        pr_df.columns = pd.Index(["energy_period", "rate"])
        sc = sc.merge(pr_df, on="energy_period")

        sc["Q_target"] = sc["Q_orig"] * ((sc["rate"] / equiv_flat) ** demand_elasticity)
        sc["load_shift"] = sc["Q_target"] - sc["Q_orig"]

        # Receiver = off-peak period (rate == equiv_flat)
        receiver_period = offpeak_period
        donor_mask = sc["energy_period"] != receiver_period
        donor_shifts = sc.loc[donor_mask].groupby("bldg_id")["load_shift"].sum()
        recv_idx = sc["energy_period"] == receiver_period
        sc.loc[recv_idx, "load_shift"] = (
            sc.loc[recv_idx, "bldg_id"].map(-donor_shifts).values
        )

        # Hourly proportional distribution
        season_df = season_df[season_df["energy_period"].isin(season_periods)].copy()
        season_df = season_df.merge(
            sc[["bldg_id", "energy_period", "Q_orig", "load_shift"]],
            on=["bldg_id", "energy_period"],
        )
        season_df["hour_share"] = np.where(
            season_df["Q_orig"].abs() > 0,
            season_df["electricity_net"] / season_df["Q_orig"],
            0.0,
        )
        season_df["hourly_shift"] = season_df["load_shift"] * season_df["hour_share"]
        season_df["shifted_net"] = (
            season_df["electricity_net"] + season_df["hourly_shift"]
        )

        all_season_dfs.append(season_df)

        # Elasticity tracker for this season
        sp = season_df.groupby(["bldg_id", "energy_period"], as_index=False).agg(
            Q_new=("shifted_net", "sum")
        )
        sp = sp.merge(
            sc[["bldg_id", "energy_period", "Q_orig", "rate"]],
            on=["bldg_id", "energy_period"],
        )
        valid = (sp["Q_new"] > 0) & (sp["Q_orig"] > 0) & (sp["rate"] != equiv_flat)
        sp["epsilon"] = np.nan
        sp.loc[valid, "epsilon"] = np.log(
            sp.loc[valid, "Q_new"] / sp.loc[valid, "Q_orig"]
        ) / np.log(sp.loc[valid, "rate"] / equiv_flat)
        all_tracker_dfs.append(sp)

    # -- 4. Build output with shifted loads -----------------------------------
    shifted_load_elec = raw_load_elec.copy()

    if all_season_dfs:
        combined = pd.concat(all_season_dfs, ignore_index=True)
        combined_idx = combined.set_index(["bldg_id", "time"]).sort_index()
        shifted_load_elec.loc[combined_idx.index, "electricity_net"] = combined_idx[
            "shifted_net"
        ].values

        if "load_data" in shifted_load_elec.columns:
            shifted_load_elec.loc[combined_idx.index, "load_data"] = (
                shifted_load_elec.loc[combined_idx.index, "load_data"]
                + combined_idx["hourly_shift"].values
            )

    # -- 5. Elasticity tracker ------------------------------------------------
    if all_tracker_dfs:
        tracker = pd.concat(all_tracker_dfs, ignore_index=True)
        elasticity_tracker = tracker.pivot(
            index="bldg_id", columns="energy_period", values="epsilon"
        )
        elasticity_tracker.columns = [f"period_{p}" for p in elasticity_tracker.columns]
    else:
        elasticity_tracker = pd.DataFrame()

    # -- 6. Validation --------------------------------------------------------
    orig_total = raw_load_elec.loc[tou_mask, "electricity_net"].sum()
    shifted_total = shifted_load_elec.loc[tou_mask, "electricity_net"].sum()
    log.info(
        "Seasonal demand response applied: %d buildings, elasticity=%.3f, "
        "original=%.0f kWh, shifted=%.0f kWh, diff=%.2f kWh",
        len(tou_bldg_ids),
        demand_elasticity,
        orig_total,
        shifted_total,
        shifted_total - orig_total,
    )
    if not elasticity_tracker.empty:
        mean_eps = elasticity_tracker.mean()
        for col in mean_eps.index:
            log.info("  Mean achieved elasticity %s: %.4f", col, mean_eps[col])

    return shifted_load_elec, elasticity_tracker
