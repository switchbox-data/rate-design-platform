"""Compute subclass revenue requirements from CAIRO outputs.

For each customer subclass in a selected metadata grouping column:
  RR_subclass = sum(annual_target_bills) - sum(selected_BAT_metric)

Inputs (under --run-dir):
  - bills/elec_bills_year_target.csv
  - cross_subsidization/cross_subsidization_BAT_values.csv
  - customer_metadata.csv
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from time import perf_counter
from typing import cast

import numpy as np
import pandas as pd
import polars as pl
import yaml
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.cairo import _load_supply_marginal_costs
from utils.loads import scan_resstock_loads
from utils.pre.season_config import (
    DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
    get_utility_periods_yaml_path,
    load_winter_months_from_periods,
)

# CAIRO output column names
BLDG_ID_COL = "bldg_id"
WEIGHT_COL = "weight"
DEFAULT_GROUP_COL = "has_hp"
BAT_METRIC_CHOICES = ("BAT_vol", "BAT_peak", "BAT_percustomer")
DEFAULT_BAT_METRIC = "BAT_percustomer"

# Output constants
GROUP_VALUE_COL = "subclass"
ANNUAL_MONTH_VALUE = "Annual"
DEFAULT_SEASONAL_OUTPUT_FILENAME = "seasonal_discount_rate_inputs.csv"
ELECTRIC_LOAD_COL = "out.electricity.net.energy_consumption"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCENARIO_CONFIG_PATH = (
    PROJECT_ROOT / "rate_design/hp_rates/ri/config/scenarios.yaml"
)
DEFAULT_DIFFERENTIATED_YAML_PATH = (
    PROJECT_ROOT / "rate_design/hp_rates/ri/config/rev_requirement/rie_hp_vs_nonhp.yaml"
)
DEFAULT_RIE_YAML_PATH = (
    PROJECT_ROOT / "rate_design/hp_rates/ri/config/rev_requirement/rie.yaml"
)
LOGGER = logging.getLogger(__name__)


def parse_group_value_to_subclass(raw: str) -> dict[str, str]:
    """Parse 'true=hp,false=non-hp' into {'true': 'hp', 'false': 'non-hp'}."""
    result: dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if "=" not in pair:
            raise ValueError(
                f"Invalid group-value-to-subclass pair {pair!r}; expected 'value=alias'"
            )
        value, alias = pair.split("=", 1)
        value = value.strip()
        alias = alias.strip()
        if not value or not alias:
            raise ValueError(
                f"Empty key or value in group-value-to-subclass pair {pair!r}"
            )
        if value in result:
            raise ValueError(
                f"Duplicate group value {value!r} in group-value-to-subclass"
            )
        result[value] = alias
    if not result:
        raise ValueError("group-value-to-subclass must contain at least one mapping")
    return result


def compute_per_subclass_supply_mc(
    supply_mc_df: pd.DataFrame,
    loads_lf: pl.LazyFrame,
    metadata_with_subclass: pl.DataFrame,
) -> dict[str, float]:
    """Compute per-subclass supply MC from hourly prices and loads.

    Mirrors the computation in run_scenario.py lines 669-702:
      supply_MC_k = sum_h(supply_price_h * weighted_load_k_h)

    Args:
        supply_mc_df: DataFrame from _load_supply_marginal_costs (8760 rows,
            columns contain 'Energy' and/or 'Capacity' in names).
        loads_lf: ResStock loads LazyFrame (bldg_id, timestamp, demand col).
        metadata_with_subclass: DataFrame with bldg_id, weight, subclass columns.
    """
    supply_cols = [c for c in supply_mc_df.columns if "Energy" in c or "Capacity" in c]
    if not supply_cols:
        raise ValueError(
            "Supply MC DataFrame has no Energy/Capacity columns: "
            f"{supply_mc_df.columns.tolist()}"
        )
    supply_prices_arr = supply_mc_df[supply_cols].sum(axis=1).values

    building_ids = metadata_with_subclass[BLDG_ID_COL].to_list()
    weighted_loads: pl.DataFrame = cast(
        pl.DataFrame,
        loads_lf.filter(pl.col(BLDG_ID_COL).is_in(building_ids))
        .join(
            metadata_with_subclass.select(
                BLDG_ID_COL, WEIGHT_COL, GROUP_VALUE_COL
            ).lazy(),
            on=BLDG_ID_COL,
            how="inner",
        )
        .with_columns(
            (pl.col(ELECTRIC_LOAD_COL) * pl.col(WEIGHT_COL)).alias("weighted_kwh")
        )
        .group_by(GROUP_VALUE_COL, "timestamp")
        .agg(pl.col("weighted_kwh").sum())
        .sort(GROUP_VALUE_COL, "timestamp")
        .collect(),
    )

    result: dict[str, float] = {}
    for subclass_val in weighted_loads[GROUP_VALUE_COL].unique().sort().to_list():
        sub = weighted_loads.filter(pl.col(GROUP_VALUE_COL) == subclass_val).sort(
            "timestamp"
        )
        hourly_load = sub["weighted_kwh"].to_numpy()
        if len(hourly_load) != len(supply_prices_arr):
            raise ValueError(
                f"Subclass {subclass_val!r}: expected {len(supply_prices_arr)} "
                f"hourly values, got {len(hourly_load)}"
            )
        result[str(subclass_val)] = float(np.dot(supply_prices_arr, hourly_load))
    return result


def _resolve_winter_months(
    *,
    state: str,
    utility: str,
    periods_yaml_path: Path | None = None,
) -> tuple[int, ...]:
    resolved_periods_path = (
        periods_yaml_path
        if periods_yaml_path is not None
        else get_utility_periods_yaml_path(
            project_root=PROJECT_ROOT,
            state=state,
            utility=utility,
        )
    )
    if resolved_periods_path.exists():
        return tuple(
            load_winter_months_from_periods(
                resolved_periods_path,
                default_winter_months=DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
            )
        )
    return tuple(DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS)


def _csv_path(run_dir: S3Path | Path, relative: str) -> str:
    return str(run_dir / relative)


def _json_path(run_dir: S3Path | Path, relative: str) -> str:
    return str(run_dir / relative)


def _load_group_values(
    run_dir: S3Path | Path,
    group_col: str,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    metadata = pl.scan_csv(
        _csv_path(run_dir, "customer_metadata.csv"),
        storage_options=storage_options,
    )
    schema = metadata.collect_schema().names()
    group_col_candidates = (
        [group_col, f"postprocess_group.{group_col}"]
        if "." not in group_col
        else [group_col]
    )
    resolved_group_col = next((cn for cn in group_col_candidates if cn in schema), None)
    if resolved_group_col is None:
        msg = (
            f"Grouping column '{group_col}' not found in customer_metadata.csv. "
            f"Tried: {group_col_candidates}"
        )
        raise ValueError(msg)

    return (
        metadata.select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col(resolved_group_col)
            .cast(pl.String, strict=False)
            .alias(GROUP_VALUE_COL),
            pl.col(WEIGHT_COL).cast(pl.Float64).alias(WEIGHT_COL),
        )
        .with_columns(pl.col(GROUP_VALUE_COL).fill_null("Unknown"))
        .unique(subset=[BLDG_ID_COL], keep="first")
    )


def _load_metadata_for_group(
    run_dir: S3Path | Path,
    group_col: str,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return _load_group_values(
        run_dir=run_dir,
        group_col=group_col,
        storage_options=storage_options,
    )


def _load_annual_target_bills(
    run_dir: S3Path | Path,
    annual_month: str,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            _csv_path(run_dir, "bills/elec_bills_year_target.csv"),
            storage_options=storage_options,
        )
        .filter(pl.col("month") == annual_month)
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col("bill_level").cast(pl.Float64).alias("annual_bill"),
        )
        .group_by(BLDG_ID_COL)
        .agg(pl.col("annual_bill").sum())
    )


def _load_cross_subsidy(
    run_dir: S3Path | Path,
    cross_subsidy_col: str,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            _csv_path(
                run_dir, "cross_subsidization/cross_subsidization_BAT_values.csv"
            ),
            storage_options=storage_options,
        )
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col(cross_subsidy_col).cast(pl.Float64).alias("cross_subsidy"),
        )
        .group_by(BLDG_ID_COL)
        .agg(pl.col("cross_subsidy").sum())
    )


def _resolve_path_or_s3(path_value: str) -> S3Path | Path:
    return S3Path(path_value) if path_value.startswith("s3://") else Path(path_value)


def _extract_default_rate_from_tariff_config(
    tariff_final_config_path: S3Path | Path,
) -> float:
    if isinstance(tariff_final_config_path, S3Path):
        payload = tariff_final_config_path.read_text()
    else:
        payload = Path(tariff_final_config_path).read_text(encoding="utf-8")
    tariff = json.loads(payload)
    # Support CAIRO internal tariff shape where top-level key is tariff_key and
    # rates are in `ur_ec_tou_mat` rows: [period, tier, max_usage, units, buy, sell, adj].
    if isinstance(tariff, dict) and tariff:
        first_key = next(iter(tariff))
        first_tariff = tariff.get(first_key, {})
        if isinstance(first_tariff, dict) and "ur_ec_tou_mat" in first_tariff:
            tou_mat = first_tariff.get("ur_ec_tou_mat", [])
            if not tou_mat:
                raise ValueError("tariff_final_config.json has empty `ur_ec_tou_mat`")
            # Pick period=1,tier=1 row when present; otherwise first row.
            row = next(
                (
                    r
                    for r in tou_mat
                    if isinstance(r, list)
                    and len(r) >= 5
                    and int(r[0]) == 1
                    and int(r[1]) == 1
                ),
                tou_mat[0],
            )
            if not isinstance(row, list) or len(row) < 5:
                raise ValueError("Invalid `ur_ec_tou_mat` row format in tariff config")
            return float(row[4])

    raise ValueError(
        "tariff_final_config.json does not match expected CAIRO internal "
        "`ur_ec_tou_mat` format."
    )


def compute_hp_seasonal_discount_inputs(
    run_dir: S3Path | Path,
    resstock_base: str,
    state: str,
    upgrade: str,
    cross_subsidy_col: str = DEFAULT_BAT_METRIC,
    storage_options: dict[str, str] | None = None,
    tariff_final_config_path: S3Path | Path | None = None,
    winter_months: tuple[int, ...] | None = None,
) -> pl.DataFrame:
    """Compute HP-only seasonal discount inputs from run outputs + ResStock loads.

    Uses hive partitions (state, upgrade) and building IDs from the run so the
    load scan aligns with the CAIRO sample and avoids globbing.
    """
    resolved_winter_months = (
        winter_months
        if winter_months is not None
        else tuple(DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS)
    )
    t0 = perf_counter()
    metadata = _load_metadata_for_group(
        run_dir=run_dir,
        group_col=DEFAULT_GROUP_COL,
        storage_options=storage_options,
    )
    cross_sub = _load_cross_subsidy(run_dir, cross_subsidy_col, storage_options)

    hp_cross_subsidy = (
        metadata.filter(pl.col(GROUP_VALUE_COL) == "true")
        .join(cross_sub, on=BLDG_ID_COL, how="left")
        .collect()
    )
    LOGGER.info(
        "seasonal_inputs: loaded metadata + cross-subsidy in %.2fs",
        perf_counter() - t0,
    )
    hp_cross_subsidy = cast(pl.DataFrame, hp_cross_subsidy)
    if hp_cross_subsidy.is_empty():
        raise ValueError("No HP customers found in customer_metadata.csv.")

    nulls_cs = hp_cross_subsidy.filter(pl.col("cross_subsidy").is_null()).height
    if nulls_cs:
        raise ValueError(f"Missing cross-subsidy values for {nulls_cs} HP buildings.")
    nulls_weight = hp_cross_subsidy.filter(pl.col(WEIGHT_COL).is_null()).height
    if nulls_weight:
        raise ValueError(f"Missing sample weights for {nulls_weight} HP buildings.")

    hp_weights = hp_cross_subsidy.select(pl.col(BLDG_ID_COL), pl.col(WEIGHT_COL))
    building_ids = hp_cross_subsidy[BLDG_ID_COL].to_list()
    t1 = perf_counter()
    loads = scan_resstock_loads(
        resstock_base,
        state,
        upgrade,
        building_ids=building_ids,
        storage_options=storage_options,
    )
    LOGGER.info(
        "seasonal_inputs: prepared loads scan for %d HP buildings in %.2fs",
        len(building_ids),
        perf_counter() - t1,
    )
    t2 = perf_counter()
    winter_kwh_hp = (
        loads.join(hp_weights.lazy(), on=BLDG_ID_COL, how="inner")
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col("timestamp")
            .cast(pl.String, strict=False)
            .str.to_datetime(strict=False)
            .alias("timestamp"),
            pl.col(ELECTRIC_LOAD_COL).cast(pl.Float64).alias("demand_kwh"),
            pl.col(WEIGHT_COL).cast(pl.Float64),
        )
        .with_columns(pl.col("timestamp").dt.month().alias("month_num"))
        .filter(pl.col("month_num").is_in(resolved_winter_months))
        .with_columns((pl.col("demand_kwh") * pl.col(WEIGHT_COL)).alias("weighted_kwh"))
        .select(pl.col("weighted_kwh").sum().alias("winter_kwh_hp"))
        .collect(engine="streaming")
    )
    LOGGER.info(
        "seasonal_inputs: collected winter kWh aggregate in %.2fs",
        perf_counter() - t2,
    )
    winter_kwh_hp = cast(pl.DataFrame, winter_kwh_hp)

    winter_kwh = float(winter_kwh_hp["winter_kwh_hp"][0] or 0.0)
    if winter_kwh <= 0:
        raise ValueError(
            "Winter kWh for HP customers is zero; cannot compute winter rate."
        )

    total_cross_subsidy_hp = float(
        hp_cross_subsidy.select(
            (pl.col("cross_subsidy") * pl.col(WEIGHT_COL)).sum().alias("weighted_cs")
        )["weighted_cs"][0]
    )
    tariff_path = (
        tariff_final_config_path
        if tariff_final_config_path is not None
        else (run_dir / "tariff_final_config.json")
    )
    default_rate = _extract_default_rate_from_tariff_config(tariff_path)
    winter_rate_raw = default_rate - (total_cross_subsidy_hp / winter_kwh)
    winter_rate_hp = winter_rate_raw
    if winter_rate_hp < 0:
        raise ValueError(
            "Computed winter_rate_hp is negative. "
            "Check formula inputs: "
            f"default_rate={default_rate}, "
            f"total_cross_subsidy_hp={total_cross_subsidy_hp}, "
            f"winter_kwh_hp={winter_kwh}, "
            f"winter_rate_hp={winter_rate_hp}"
        )

    t3 = perf_counter()
    result = pl.DataFrame(
        {
            "subclass": ["true"],
            "cross_subsidy_col": [cross_subsidy_col],
            "default_rate": [default_rate],
            "total_cross_subsidy_hp": [total_cross_subsidy_hp],
            "winter_kwh_hp": [winter_kwh],
            "winter_rate_hp": [winter_rate_hp],
            "winter_rate_raw": [winter_rate_raw],
            "winter_months": [",".join(str(m) for m in resolved_winter_months)],
        }
    )
    LOGGER.info(
        "seasonal_inputs: finalized result frame in %.2fs",
        perf_counter() - t3,
    )
    return result


def compute_subclass_rr(
    run_dir: S3Path | Path,
    group_col: str = DEFAULT_GROUP_COL,
    cross_subsidy_col: str = DEFAULT_BAT_METRIC,
    annual_month: str = ANNUAL_MONTH_VALUE,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Return subclass revenue requirement breakdown for the selected grouping.

    Columns: subclass, sum_bills, sum_cross_subsidy, revenue_requirement
    """
    group_values = _load_group_values(run_dir, group_col, storage_options)
    bills = _load_annual_target_bills(run_dir, annual_month, storage_options)
    cross_sub = _load_cross_subsidy(run_dir, cross_subsidy_col, storage_options)

    joined = cast(
        pl.DataFrame,
        group_values.join(bills, on=BLDG_ID_COL, how="left")
        .join(cross_sub, on=BLDG_ID_COL, how="left")
        .collect(),
    )
    if joined.is_empty():
        msg = "No customers found in customer_metadata.csv."
        raise ValueError(msg)

    nulls_bills = joined.filter(pl.col("annual_bill").is_null()).height
    if nulls_bills:
        msg = (
            f"Missing annual target bills for {nulls_bills} buildings "
            f"(month={annual_month})."
        )
        raise ValueError(msg)

    nulls_cs = joined.filter(pl.col("cross_subsidy").is_null()).height
    if nulls_cs:
        msg = f"Missing cross-subsidy values for {nulls_cs} buildings."
        raise ValueError(msg)

    nulls_weight = joined.filter(pl.col(WEIGHT_COL).is_null()).height
    if nulls_weight:
        msg = f"Missing sample weights for {nulls_weight} buildings."
        raise ValueError(msg)

    return (
        joined.with_columns(
            (pl.col("annual_bill") * pl.col(WEIGHT_COL)).alias("weighted_annual_bill"),
            (pl.col("cross_subsidy") * pl.col(WEIGHT_COL)).alias(
                "weighted_cross_subsidy"
            ),
        )
        .group_by(GROUP_VALUE_COL)
        .agg(
            pl.col("weighted_annual_bill").sum().alias("sum_bills"),
            pl.col("weighted_cross_subsidy").sum().alias("sum_cross_subsidy"),
        )
        .with_columns(
            (pl.col("sum_bills") - pl.col("sum_cross_subsidy")).alias(
                "revenue_requirement"
            )
        )
        .sort(GROUP_VALUE_COL)
    )


def _load_run_from_scenario_config(
    scenario_config_path: Path,
    run_num: int,
) -> dict[str, object]:
    data = yaml.safe_load(scenario_config_path.read_text(encoding="utf-8")) or {}
    runs = data.get("runs", {})
    run = runs.get(run_num) or runs.get(str(run_num))
    if run is None:
        msg = f"Run {run_num} not found in scenario config: {scenario_config_path}"
        raise ValueError(msg)
    if not isinstance(run, dict):
        msg = (
            f"Run {run_num} is not a mapping in scenario config: {scenario_config_path}"
        )
        raise ValueError(msg)
    return cast(dict[str, object], run)


def _load_run_fields(
    scenario_config_path: Path,
    run_num: int,
) -> tuple[str, str, float]:
    """Read state, utility, and delivery revenue requirement from a scenario run.

    Raises KeyError if any required field is missing.
    """
    run = _load_run_from_scenario_config(scenario_config_path, run_num)
    for field in ("state", "utility", "utility_delivery_revenue_requirement"):
        if field not in run:
            raise KeyError(
                f"Run {run_num} in {scenario_config_path} is missing "
                f"required field '{field}'"
            )
    state = str(run["state"]).upper()
    utility = str(run["utility"]).lower()
    raw_rr = run["utility_delivery_revenue_requirement"]
    if isinstance(raw_rr, str) and raw_rr.endswith(".yaml"):
        rr_path = scenario_config_path.parent.parent / raw_rr
        with open(rr_path) as f:
            rr_data = yaml.safe_load(f)
        revenue_requirement = float(rr_data["revenue_requirement"])
    else:
        revenue_requirement = float(cast(float | int | str, raw_rr))
    return state, utility, revenue_requirement


def _write_revenue_requirement_yamls(
    breakdown: pl.DataFrame,
    run_dir: S3Path | Path,
    group_col: str,
    cross_subsidy_col: str,
    utility: str,
    default_revenue_requirement: float,
    differentiated_yaml_path: Path,
    default_yaml_path: Path,
    *,
    group_value_to_subclass: dict[str, str] | None = None,
    supply_mc_by_subclass: dict[str, float] | None = None,
    total_delivery_rr: float | None = None,
    total_delivery_and_supply_rr: float | None = None,
) -> tuple[Path, Path]:
    differentiated_yaml_path.parent.mkdir(parents=True, exist_ok=True)
    default_yaml_path.parent.mkdir(parents=True, exist_ok=True)

    gv_map = group_value_to_subclass or {}
    supply_mc = supply_mc_by_subclass or {}

    subclass_rr: dict[str, dict[str, float]] = {}
    for row in breakdown.to_dicts():
        raw_val = str(row["subclass"])
        alias = gv_map.get(raw_val, raw_val)
        delivery = float(row["revenue_requirement"])
        supply = supply_mc.get(raw_val, 0.0)
        subclass_rr[alias] = {
            "delivery": delivery,
            "supply": supply,
            "total": delivery + supply,
        }

    differentiated_data: dict[str, object] = {
        "utility": utility,
        "group_col": group_col,
        "cross_subsidy_col": cross_subsidy_col,
        "source_run_dir": str(run_dir),
    }
    if total_delivery_rr is not None:
        differentiated_data["total_delivery_revenue_requirement"] = total_delivery_rr
    if total_delivery_and_supply_rr is not None:
        differentiated_data["total_delivery_and_supply_revenue_requirement"] = (
            total_delivery_and_supply_rr
        )
    differentiated_data["subclass_revenue_requirements"] = subclass_rr

    differentiated_yaml_path.write_text(
        yaml.safe_dump(differentiated_data, sort_keys=False),
        encoding="utf-8",
    )
    return differentiated_yaml_path, default_yaml_path


def _write_seasonal_inputs_csv(
    seasonal_inputs: pl.DataFrame,
    run_dir: S3Path | Path,
    output_dir: S3Path | Path | None = None,
) -> str:
    target_dir = output_dir if output_dir is not None else run_dir
    output_path = str(target_dir / DEFAULT_SEASONAL_OUTPUT_FILENAME)
    csv_text = seasonal_inputs.write_csv(None)
    if isinstance(csv_text, str):
        if isinstance(target_dir, S3Path):
            S3Path(output_path).write_text(csv_text)
        else:
            Path(output_path).write_text(csv_text, encoding="utf-8")
        return output_path

    msg = "Failed to render seasonal discount input CSV text."
    raise ValueError(msg)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Compute subclass revenue requirements from CAIRO outputs."
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to CAIRO output directory (local or s3://...)",
    )
    parser.add_argument(
        "--group-col",
        default=DEFAULT_GROUP_COL,
        help=(
            "Grouping column from customer_metadata.csv "
            "(default: has_hp; utility will also resolve postprocess_group.has_hp)"
        ),
    )
    parser.add_argument(
        "--cross-subsidy-col",
        default=DEFAULT_BAT_METRIC,
        choices=BAT_METRIC_CHOICES,
        help="BAT column in cross_subsidization_BAT_values.csv to use.",
    )
    parser.add_argument(
        "--annual-month",
        default=ANNUAL_MONTH_VALUE,
        help="Month label for annual bill (default: Annual)",
    )
    parser.add_argument(
        "--scenario-config",
        type=Path,
        default=DEFAULT_SCENARIO_CONFIG_PATH,
        help=(
            "Path to RI scenarios YAML used to read default utility delivery revenue "
            "requirement."
        ),
    )
    parser.add_argument(
        "--run-num",
        type=int,
        default=1,
        help="Run number in scenarios.yaml to read default revenue requirement from.",
    )
    parser.add_argument(
        "--differentiated-yaml-path",
        type=Path,
        default=DEFAULT_DIFFERENTIATED_YAML_PATH,
        help="Path to write differentiated subclass revenue requirements YAML.",
    )
    parser.add_argument(
        "--default-yaml-path",
        type=Path,
        default=DEFAULT_RIE_YAML_PATH,
        help="Path to write default RIE revenue requirement YAML.",
    )
    parser.add_argument(
        "--write-revenue-requirement-yamls",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Whether to write differentiated/default revenue requirement YAML outputs "
            "(default: true)."
        ),
    )
    parser.add_argument(
        "--resstock-base",
        help=(
            "Optional base path to ResStock release (e.g. s3://.../res_2024_amy2018_2). "
            "If provided with --upgrade, writes seasonal discount inputs for has_hp=true."
        ),
    )
    parser.add_argument(
        "--upgrade",
        default="00",
        help="Upgrade partition for loads (e.g. 00). Used when --resstock-base is set.",
    )
    parser.add_argument(
        "--tariff-final-config-path",
        help=(
            "Optional override for tariff_final_config.json (local or s3://...). "
            "Defaults to <run-dir>/tariff_final_config.json."
        ),
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Optional output directory for seasonal discount CSV. "
            "If omitted, writes to --run-dir."
        ),
    )
    parser.add_argument(
        "--periods-yaml",
        type=Path,
        help=(
            "Optional override for utility periods YAML path. "
            "If omitted, resolves from state and utility in the scenario config."
        ),
    )
    parser.add_argument(
        "--group-value-to-subclass",
        help=(
            "Mapping of raw group values to subclass aliases, e.g. "
            "'true=hp,false=non-hp'. Used for YAML output keys."
        ),
    )
    parser.add_argument(
        "--base-rr-yaml",
        type=Path,
        help=(
            "Path to base revenue requirement YAML (e.g. cenhud.yaml). "
            "Copies total_delivery_revenue_requirement and "
            "total_delivery_and_supply_revenue_requirement into output."
        ),
    )
    parser.add_argument(
        "--supply-energy-mc",
        help="Path to supply energy MC parquet (for per-subclass supply MC).",
    )
    parser.add_argument(
        "--supply-capacity-mc",
        help="Path to supply capacity MC parquet (for per-subclass supply MC).",
    )
    parser.add_argument(
        "--year-run",
        type=int,
        help="Target year for MC time-shifting (required when computing supply MC).",
    )
    args = parser.parse_args()

    run_dir: S3Path | Path = (
        S3Path(args.run_dir) if args.run_dir.startswith("s3://") else Path(args.run_dir)
    )
    output_dir: S3Path | Path | None = (
        _resolve_path_or_s3(args.output_dir) if args.output_dir else None
    )
    storage_options = get_aws_storage_options() if isinstance(run_dir, S3Path) else None

    breakdown = compute_subclass_rr(
        run_dir=run_dir,
        group_col=args.group_col,
        cross_subsidy_col=args.cross_subsidy_col,
        annual_month=args.annual_month,
        storage_options=storage_options,
    )
    print(breakdown)

    run_state, run_utility, default_revenue_requirement = _load_run_fields(
        scenario_config_path=args.scenario_config,
        run_num=args.run_num,
    )

    gv_map: dict[str, str] | None = None
    if args.group_value_to_subclass:
        gv_map = parse_group_value_to_subclass(args.group_value_to_subclass)

    # Read top-level totals from base RR YAML if provided.
    total_delivery_rr: float | None = None
    total_delivery_and_supply_rr: float | None = None
    if args.base_rr_yaml:
        with args.base_rr_yaml.open(encoding="utf-8") as f:
            base_rr_data = yaml.safe_load(f)
        total_delivery_rr = float(base_rr_data["total_delivery_revenue_requirement"])
        total_delivery_and_supply_rr = float(
            base_rr_data["total_delivery_and_supply_revenue_requirement"]
        )

    # Compute per-subclass supply MC if paths provided.
    supply_mc_by_subclass: dict[str, float] | None = None
    if args.supply_energy_mc and args.supply_capacity_mc:
        if args.year_run is None:
            parser.error("--year-run is required when computing supply MC")
        if not args.resstock_base:
            parser.error("--resstock-base is required when computing supply MC")

        t_supply = perf_counter()
        supply_mc_df = _load_supply_marginal_costs(
            args.supply_energy_mc,
            args.supply_capacity_mc,
            args.year_run,
        )
        LOGGER.info(
            "Loaded supply MC prices (%d rows) in %.2fs",
            len(supply_mc_df),
            perf_counter() - t_supply,
        )

        metadata_with_subclass = _load_group_values(
            run_dir, args.group_col, storage_options
        ).collect()
        metadata_with_subclass = cast(pl.DataFrame, metadata_with_subclass)

        building_ids = metadata_with_subclass[BLDG_ID_COL].to_list()
        loads_lf = scan_resstock_loads(
            args.resstock_base,
            run_state,
            args.upgrade,
            building_ids=building_ids,
            storage_options=storage_options,
        )

        t_mc = perf_counter()
        supply_mc_by_subclass = compute_per_subclass_supply_mc(
            supply_mc_df=supply_mc_df,
            loads_lf=loads_lf,
            metadata_with_subclass=metadata_with_subclass,
        )
        LOGGER.info(
            "Per-subclass supply MC: %s (%.2fs)",
            {k: f"${v:,.0f}" for k, v in supply_mc_by_subclass.items()},
            perf_counter() - t_mc,
        )

    if args.write_revenue_requirement_yamls:
        differentiated_yaml_path, default_yaml_path = _write_revenue_requirement_yamls(
            breakdown=breakdown,
            run_dir=run_dir,
            group_col=args.group_col,
            cross_subsidy_col=args.cross_subsidy_col,
            utility=run_utility,
            default_revenue_requirement=default_revenue_requirement,
            differentiated_yaml_path=args.differentiated_yaml_path,
            default_yaml_path=args.default_yaml_path,
            group_value_to_subclass=gv_map,
            supply_mc_by_subclass=supply_mc_by_subclass,
            total_delivery_rr=total_delivery_rr,
            total_delivery_and_supply_rr=total_delivery_and_supply_rr,
        )
        print(f"Wrote differentiated YAML: {differentiated_yaml_path}")
        print(f"Wrote default YAML: {default_yaml_path}")

    if args.resstock_base:
        winter_months = _resolve_winter_months(
            state=run_state,
            utility=run_utility,
            periods_yaml_path=args.periods_yaml,
        )
        tariff_final_config_path = (
            _resolve_path_or_s3(args.tariff_final_config_path)
            if args.tariff_final_config_path
            else None
        )
        seasonal_inputs = compute_hp_seasonal_discount_inputs(
            run_dir=run_dir,
            resstock_base=args.resstock_base,
            state=run_state,
            upgrade=args.upgrade,
            cross_subsidy_col=args.cross_subsidy_col,
            storage_options=storage_options,
            tariff_final_config_path=tariff_final_config_path,
            winter_months=winter_months,
        )
        print(seasonal_inputs)
        seasonal_output_path = _write_seasonal_inputs_csv(
            seasonal_inputs=seasonal_inputs,
            run_dir=run_dir,
            output_dir=output_dir,
        )
        print(f"Wrote seasonal discount inputs CSV: {seasonal_output_path}")


if __name__ == "__main__":
    main()
