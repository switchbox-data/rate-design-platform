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

import polars as pl
import yaml
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.loads import ELECTRIC_PV_COL, grid_consumption_expr, scan_resstock_loads
from utils.pre.season_config import (
    DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
    get_utility_periods_yaml_path,
    load_winter_months_from_periods,
)

# CAIRO output column names
BLDG_ID_COL = "bldg_id"
WEIGHT_COL = "weight"
DEFAULT_GROUP_COL = "has_hp"
BAT_METRIC_CHOICES = ("BAT_vol", "BAT_peak", "BAT_percustomer", "BAT_epmc")
DEFAULT_BAT_METRIC = "BAT_percustomer"
SUBCLASS_RR_ALLOCATION_METHODS: tuple[str, ...] = (
    "BAT_percustomer",
    "BAT_epmc",
    "BAT_vol",
)
BAT_COL_TO_ALLOCATION_KEY: dict[str, str] = {
    "BAT_percustomer": "percustomer",
    "BAT_epmc": "epmc",
    "BAT_vol": "volumetric",
    "BAT_peak": "peak",
}

# Output constants
GROUP_VALUE_COL = "subclass"
ANNUAL_MONTH_VALUE = "Annual"
DEFAULT_SEASONAL_OUTPUT_FILENAME = "seasonal_discount_rate_inputs.csv"
DEFAULT_FLAT_OUTPUT_FILENAME = "flat_discount_rate_inputs.csv"


def seasonal_discount_filename(group_col: str, subclass_value: str) -> str:
    """Return the seasonal discount CSV filename for a specific subclass.

    Encodes the grouping column and subclass value so that multiple subclass
    seasonal-discount computations sharing the same CAIRO run directory do not
    overwrite each other.

    Examples
    --------
    >>> seasonal_discount_filename("has_hp", "true")
    'seasonal_discount_rate_inputs_has_hp_true.csv'
    >>> seasonal_discount_filename("heating_type_v2", "electric_heating")
    'seasonal_discount_rate_inputs_heating_type_v2_electric_heating.csv'
    """
    safe_col = group_col.replace(".", "_")
    safe_val = subclass_value.replace(" ", "_")
    return f"seasonal_discount_rate_inputs_{safe_col}_{safe_val}.csv"


def flat_discount_filename(group_col: str, subclass_value: str) -> str:
    """Return the flat discount CSV filename for a specific subclass.

    Encodes the grouping column and subclass value so multiple subclass
    flat-discount computations sharing the same run directory do not collide.

    Examples
    --------
    >>> flat_discount_filename("has_hp", "true")
    'flat_discount_rate_inputs_has_hp_true.csv'
    """
    safe_col = group_col.replace(".", "_")
    safe_val = subclass_value.replace(" ", "_")
    return f"flat_discount_rate_inputs_{safe_col}_{safe_val}.csv"


ELECTRIC_LOAD_COL = "out.electricity.total.energy_consumption"
MONTH_ABBREV_TO_NUM: dict[str, int] = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}
NUM_TO_MONTH_ABBREV: dict[int, str] = {v: k for k, v in MONTH_ABBREV_TO_NUM.items()}
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


def _extract_fixed_charge_from_urdb(
    base_tariff_json_path: S3Path | Path,
) -> float:
    """Read ``fixedchargefirstmeter`` from a URDB v7 tariff JSON."""
    if isinstance(base_tariff_json_path, S3Path):
        payload = base_tariff_json_path.read_text()
    else:
        payload = Path(base_tariff_json_path).read_text(encoding="utf-8")
    tariff = json.loads(payload)
    items = tariff.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError(
            "Base tariff JSON must contain a non-empty 'items' list: "
            f"{base_tariff_json_path}"
        )
    fixed_charge = items[0].get("fixedchargefirstmeter")
    if fixed_charge is None:
        raise ValueError(
            "Base tariff JSON is missing 'fixedchargefirstmeter': "
            f"{base_tariff_json_path}"
        )
    return float(fixed_charge)


def _resolve_selector_group_values(
    subclass_value: str,
    group_value_to_subclass: dict[str, str] | None,
) -> tuple[str, ...]:
    """Resolve a requested subclass label to raw group values to match.

    If ``group_value_to_subclass`` is provided, callers may pass either a raw
    group value (for example ``"true"``) or a subclass alias (for example
    ``"electric_heating"``). Alias values expand to all matching raw values
    from the scenario selector config.
    """
    if group_value_to_subclass is None:
        return (subclass_value,)

    matched_values = tuple(
        group_value
        for group_value, subclass_alias in group_value_to_subclass.items()
        if subclass_alias == subclass_value
    )
    if matched_values:
        return matched_values
    return (subclass_value,)


def _load_subclass_cross_subsidy_inputs(
    run_dir: S3Path | Path,
    group_col: str,
    subclass_value: str,
    cross_subsidy_col: str,
    storage_options: dict[str, str] | None,
    group_value_to_subclass: dict[str, str] | None = None,
    *,
    log_prefix: str,
) -> tuple[pl.DataFrame, float]:
    """Load subclass metadata joined to the requested BAT metric.

    Returns the joined subclass frame plus the weighted total cross-subsidy.
    """
    t0 = perf_counter()
    metadata = _load_metadata_for_group(
        run_dir=run_dir,
        group_col=group_col,
        storage_options=storage_options,
    )
    cross_sub = _load_cross_subsidy(run_dir, cross_subsidy_col, storage_options)
    group_values = _resolve_selector_group_values(
        subclass_value,
        group_value_to_subclass,
    )

    subclass_cross_subsidy = cast(
        pl.DataFrame,
        metadata.filter(pl.col(GROUP_VALUE_COL).is_in(group_values))
        .join(cross_sub, on=BLDG_ID_COL, how="left")
        .collect(),
    )
    LOGGER.info(
        "%s [%s=%s]: loaded metadata + cross-subsidy in %.2fs",
        log_prefix,
        group_col,
        subclass_value,
        perf_counter() - t0,
    )
    if subclass_cross_subsidy.is_empty():
        raise ValueError(
            f"No customers with {group_col}={subclass_value!r} found in customer_metadata.csv."
        )

    nulls_cs = subclass_cross_subsidy.filter(pl.col("cross_subsidy").is_null()).height
    if nulls_cs:
        raise ValueError(f"Missing cross-subsidy values for {nulls_cs} buildings.")
    nulls_weight = subclass_cross_subsidy.filter(pl.col(WEIGHT_COL).is_null()).height
    if nulls_weight:
        raise ValueError(f"Missing sample weights for {nulls_weight} buildings.")

    total_cross_subsidy = float(
        subclass_cross_subsidy.select(
            (pl.col("cross_subsidy") * pl.col(WEIGHT_COL)).sum().alias("weighted_cs")
        )["weighted_cs"][0]
    )
    return subclass_cross_subsidy, total_cross_subsidy


def _compute_annual_energy_revenue(
    run_dir: S3Path | Path,
    subclass_cross_subsidy: pl.DataFrame,
    fixed_charge: float,
    storage_options: dict[str, str] | None,
    *,
    group_col: str,
    subclass_value: str,
    log_prefix: str,
) -> float:
    """Compute subclass annual energy revenue from annual bills and fixed charges."""
    t1 = perf_counter()
    ids_weights = subclass_cross_subsidy.select(BLDG_ID_COL, WEIGHT_COL)

    annual_energy_rev_row = cast(
        pl.DataFrame,
        _load_annual_target_bills(run_dir, ANNUAL_MONTH_VALUE, storage_options)
        .join(ids_weights.lazy(), on=BLDG_ID_COL, how="inner")
        .with_columns(
            ((pl.col("annual_bill") - 12.0 * fixed_charge) * pl.col(WEIGHT_COL)).alias(
                "weighted_energy_rev"
            )
        )
        .select(pl.col("weighted_energy_rev").sum().alias("annual_energy_rev"))
        .collect(),
    )
    annual_energy_rev = float(annual_energy_rev_row["annual_energy_rev"][0] or 0.0)
    LOGGER.info(
        "%s [%s=%s]: computed annual energy revenue in %.2fs",
        log_prefix,
        group_col,
        subclass_value,
        perf_counter() - t1,
    )
    return annual_energy_rev


def compute_subclass_seasonal_discount_inputs(
    run_dir: S3Path | Path,
    resstock_base: str,
    state: str,
    upgrade: str,
    group_col: str = DEFAULT_GROUP_COL,
    subclass_value: str = "true",
    cross_subsidy_col: str = DEFAULT_BAT_METRIC,
    storage_options: dict[str, str] | None = None,
    group_value_to_subclass: dict[str, str] | None = None,
    base_tariff_json_path: S3Path | Path | None = None,
    winter_months: tuple[int, ...] | None = None,
) -> pl.DataFrame:
    """Compute seasonal discount inputs for one subclass from run outputs + ResStock loads.

    Derives effective flat seasonal rates from the run's actual bill revenue,
    so the seasonal discount works correctly regardless of whether the base
    tariff is flat or structured (tiered, seasonal, TOU).

    Parameters
    ----------
    group_col:
        Column in ``customer_metadata.csv`` that defines subclass membership
        (e.g. ``"has_hp"`` or ``"heating_type_v2"``).
    subclass_value:
        The value of *group_col* that identifies the target subclass
        (e.g. ``"true"`` for HP customers, ``"electric_heating"``).
        When ``group_value_to_subclass`` is provided, this may be either a raw
        group value or a subclass alias from scenario ``subclass_config``.
    """
    if base_tariff_json_path is None:
        raise ValueError(
            "base_tariff_json_path is required: pass the URDB-format calibrated "
            "tariff JSON (e.g. <utility>_default_calibrated.json) to extract the "
            "fixed charge."
        )

    resolved_winter_months = (
        winter_months
        if winter_months is not None
        else tuple(DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS)
    )

    # --- Subclass metadata, weights, and cross-subsidy ---
    subclass_cross_subsidy, total_cross_subsidy = _load_subclass_cross_subsidy_inputs(
        run_dir=run_dir,
        group_col=group_col,
        subclass_value=subclass_value,
        cross_subsidy_col=cross_subsidy_col,
        storage_options=storage_options,
        group_value_to_subclass=group_value_to_subclass,
        log_prefix="seasonal_inputs",
    )

    # --- Fixed charge from URDB base tariff ---
    fixed_charge = _extract_fixed_charge_from_urdb(base_tariff_json_path)

    # --- Annual energy revenue from annual bills ---
    # Use the Annual row (same source as compute_subclass_rr) so the flat rate
    # is derived from exactly the same bills that produced the subclass RR.
    # Subtracting 12*FC converts the annual total bill into the annual energy-only revenue.
    annual_energy_rev = _compute_annual_energy_revenue(
        run_dir=run_dir,
        subclass_cross_subsidy=subclass_cross_subsidy,
        fixed_charge=fixed_charge,
        storage_options=storage_options,
        group_col=group_col,
        subclass_value=subclass_value,
        log_prefix="seasonal_inputs",
    )

    # --- Seasonal kWh from ResStock loads ---
    sub_weights = subclass_cross_subsidy.select(pl.col(BLDG_ID_COL), pl.col(WEIGHT_COL))
    building_ids = subclass_cross_subsidy[BLDG_ID_COL].to_list()
    t2 = perf_counter()
    loads = scan_resstock_loads(
        resstock_base,
        state,
        upgrade,
        building_ids=building_ids,
        storage_options=storage_options,
    )
    LOGGER.info(
        "seasonal_inputs [%s=%s]: prepared loads scan for %d buildings in %.2fs",
        group_col,
        subclass_value,
        len(building_ids),
        perf_counter() - t2,
    )
    t3 = perf_counter()
    seasonal_kwh = cast(
        pl.DataFrame,
        loads.join(sub_weights.lazy(), on=BLDG_ID_COL, how="inner")
        .select(
            pl.col("timestamp")
            .cast(pl.String, strict=False)
            .str.to_datetime(strict=False)
            .alias("timestamp"),
            grid_consumption_expr(ELECTRIC_LOAD_COL, ELECTRIC_PV_COL).alias(
                "demand_kwh"
            ),
            pl.col(WEIGHT_COL).cast(pl.Float64),
        )
        .with_columns(pl.col("timestamp").dt.month().alias("month_num"))
        .with_columns(
            (pl.col("demand_kwh") * pl.col(WEIGHT_COL)).alias("weighted_kwh"),
            pl.col("month_num").is_in(resolved_winter_months).alias("is_winter"),
        )
        .select(
            pl.col("weighted_kwh").sum().alias("annual_kwh"),
            pl.when(pl.col("is_winter"))
            .then(pl.col("weighted_kwh"))
            .otherwise(0.0)
            .sum()
            .alias("winter_kwh"),
        )
        .collect(engine="streaming"),
    )
    LOGGER.info(
        "seasonal_inputs [%s=%s]: collected seasonal kWh aggregates in %.2fs",
        group_col,
        subclass_value,
        perf_counter() - t3,
    )

    annual_kwh = float(seasonal_kwh["annual_kwh"][0] or 0.0)
    winter_kwh = float(seasonal_kwh["winter_kwh"][0] or 0.0)
    summer_kwh = annual_kwh - winter_kwh

    if winter_kwh <= 0:
        raise ValueError(
            f"Winter kWh for {group_col}={subclass_value!r} is zero; cannot compute winter rate."
        )
    if summer_kwh <= 0:
        raise ValueError(
            f"Summer kWh for {group_col}={subclass_value!r} is zero; cannot compute summer rate."
        )

    # --- Derive effective seasonal rates ---
    # Equivalent flat rate = subclass annual energy revenue / subclass annual kWh.
    # This uses the same Annual row bills that produced the subclass RR, ensuring
    # the pre-calibrated seasonal tariff is exactly subclass revenue neutral
    # (rate_unity = 1.0 at CAIRO precalc, up to floating-point precision).
    # Summer rate = flat rate; winter rate = flat rate - winter discount,
    # where winter_discount = CS / winter_kWh eliminates the subclass cross-subsidy.
    total_kwh = summer_kwh + winter_kwh
    equivalent_flat_rate = annual_energy_rev / total_kwh
    winter_discount = total_cross_subsidy / winter_kwh
    summer_rate = equivalent_flat_rate
    winter_rate_raw = equivalent_flat_rate - winter_discount
    winter_rate = winter_rate_raw
    if winter_rate < 0:
        raise ValueError(
            f"Computed winter_rate for {group_col}={subclass_value!r} is negative. "
            "Check formula inputs: "
            f"annual_energy_rev={annual_energy_rev}, "
            f"total_cross_subsidy={total_cross_subsidy}, "
            f"winter_kwh={winter_kwh}, "
            f"annual_kwh={total_kwh}, "
            f"equivalent_flat_rate={equivalent_flat_rate}, "
            f"winter_discount={winter_discount}, "
            f"winter_rate={winter_rate}"
        )

    t4 = perf_counter()
    result = pl.DataFrame(
        {
            "subclass": [subclass_value],
            "group_col": [group_col],
            "cross_subsidy_col": [cross_subsidy_col],
            "equivalent_flat_rate": [equivalent_flat_rate],
            "winter_discount": [winter_discount],
            "summer_rate": [summer_rate],
            "total_cross_subsidy": [total_cross_subsidy],
            "winter_kwh": [winter_kwh],
            "summer_kwh": [summer_kwh],
            "annual_kwh": [total_kwh],
            "annual_energy_rev": [annual_energy_rev],
            "winter_rate": [winter_rate],
            "winter_rate_raw": [winter_rate_raw],
            "winter_months": [",".join(str(m) for m in resolved_winter_months)],
        }
    )
    LOGGER.info(
        "seasonal_inputs [%s=%s]: finalized result frame in %.2fs",
        group_col,
        subclass_value,
        perf_counter() - t4,
    )
    return result


def compute_subclass_flat_discount_inputs(
    run_dir: S3Path | Path,
    resstock_base: str,
    state: str,
    upgrade: str,
    group_col: str = DEFAULT_GROUP_COL,
    subclass_value: str = "true",
    cross_subsidy_col: str = DEFAULT_BAT_METRIC,
    storage_options: dict[str, str] | None = None,
    group_value_to_subclass: dict[str, str] | None = None,
    base_tariff_json_path: S3Path | Path | None = None,
) -> pl.DataFrame:
    """Compute a fair flat volumetric rate for one customer subclass.

    The flat rate eliminates the subclass cross-subsidy uniformly across all hours::

        equivalent_flat_rate = annual_energy_rev / annual_kwh
        flat_discount        = total_cross_subsidy / annual_kwh
        flat_rate            = equivalent_flat_rate - flat_discount

    Parameters
    ----------
    group_col:
        Column in ``customer_metadata.csv`` that defines subclass membership
        (e.g. ``"has_hp"`` or ``"heating_type_v2"``).
    subclass_value:
        The value of *group_col* that identifies the target subclass
        (e.g. ``"true"`` for HP customers, ``"electric_heating"``).
        When ``group_value_to_subclass`` is provided, this may be either a raw
        group value or a subclass alias from scenario ``subclass_config``.
    """
    if base_tariff_json_path is None:
        raise ValueError(
            "base_tariff_json_path is required: pass the URDB-format calibrated "
            "tariff JSON to extract the fixed charge."
        )

    # --- Subclass metadata, weights, and cross-subsidy ---
    subclass_cross_subsidy, total_cross_subsidy = _load_subclass_cross_subsidy_inputs(
        run_dir=run_dir,
        group_col=group_col,
        subclass_value=subclass_value,
        cross_subsidy_col=cross_subsidy_col,
        storage_options=storage_options,
        group_value_to_subclass=group_value_to_subclass,
        log_prefix="flat_inputs",
    )

    # --- Fixed charge from URDB base tariff ---
    fixed_charge = _extract_fixed_charge_from_urdb(base_tariff_json_path)

    # --- Annual energy revenue from annual bills ---
    annual_energy_rev = _compute_annual_energy_revenue(
        run_dir=run_dir,
        subclass_cross_subsidy=subclass_cross_subsidy,
        fixed_charge=fixed_charge,
        storage_options=storage_options,
        group_col=group_col,
        subclass_value=subclass_value,
        log_prefix="flat_inputs",
    )

    # --- Annual kWh from ResStock loads ---
    building_ids = subclass_cross_subsidy[BLDG_ID_COL].to_list()
    sub_weights = subclass_cross_subsidy.select(pl.col(BLDG_ID_COL), pl.col(WEIGHT_COL))
    t2 = perf_counter()
    loads = scan_resstock_loads(
        resstock_base,
        state,
        upgrade,
        building_ids=building_ids,
        storage_options=storage_options,
    )
    LOGGER.info(
        "flat_inputs [%s=%s]: prepared loads scan for %d buildings in %.2fs",
        group_col,
        subclass_value,
        len(building_ids),
        perf_counter() - t2,
    )
    t3 = perf_counter()
    kwh_agg = cast(
        pl.DataFrame,
        loads.join(sub_weights.lazy(), on=BLDG_ID_COL, how="inner")
        .select(
            grid_consumption_expr(ELECTRIC_LOAD_COL, ELECTRIC_PV_COL).alias(
                "demand_kwh"
            ),
            pl.col(WEIGHT_COL).cast(pl.Float64),
        )
        .with_columns(
            (pl.col("demand_kwh") * pl.col(WEIGHT_COL)).alias("weighted_kwh"),
        )
        .select(pl.col("weighted_kwh").sum().alias("annual_kwh"))
        .collect(engine="streaming"),
    )
    LOGGER.info(
        "flat_inputs [%s=%s]: collected annual kWh aggregate in %.2fs",
        group_col,
        subclass_value,
        perf_counter() - t3,
    )

    annual_kwh = float(kwh_agg["annual_kwh"][0] or 0.0)
    if annual_kwh <= 0:
        raise ValueError(
            f"Annual kWh for {group_col}={subclass_value!r} is zero; cannot compute flat rate."
        )

    # --- Derive fair flat rate ---
    equivalent_flat_rate = annual_energy_rev / annual_kwh
    flat_discount = total_cross_subsidy / annual_kwh
    flat_rate = equivalent_flat_rate - flat_discount

    if flat_rate < 0:
        raise ValueError(
            f"Computed flat_rate for {group_col}={subclass_value!r} is negative. "
            f"annual_energy_rev={annual_energy_rev}, "
            f"total_cross_subsidy={total_cross_subsidy}, "
            f"annual_kwh={annual_kwh}, "
            f"equivalent_flat_rate={equivalent_flat_rate}, "
            f"flat_discount={flat_discount}, "
            f"flat_rate={flat_rate}"
        )

    result = pl.DataFrame(
        {
            "subclass": [subclass_value],
            "group_col": [group_col],
            "cross_subsidy_col": [cross_subsidy_col],
            "flat_rate": [flat_rate],
            "equivalent_flat_rate": [equivalent_flat_rate],
            "flat_discount": [flat_discount],
            "total_cross_subsidy": [total_cross_subsidy],
            "annual_kwh": [annual_kwh],
            "annual_energy_rev": [annual_energy_rev],
            "fixed_charge": [fixed_charge],
        }
    )
    LOGGER.info("flat_inputs [%s=%s]: done", group_col, subclass_value)
    return result


def compute_subclass_rr(
    run_dir: S3Path | Path,
    group_col: str = DEFAULT_GROUP_COL,
    cross_subsidy_cols: str | tuple[str, ...] = SUBCLASS_RR_ALLOCATION_METHODS,
    annual_month: str = ANNUAL_MONTH_VALUE,
    storage_options: dict[str, str] | None = None,
) -> dict[str, pl.DataFrame]:
    """Return subclass revenue requirement breakdowns for one or more BAT columns.

    Loads bills and BAT CSV once, joins once, then computes a separate
    breakdown per BAT column.  Returns ``{bat_col: DataFrame}`` where each
    DataFrame has columns: subclass, sum_bills, sum_cross_subsidy,
    revenue_requirement.

    For backward compat, *cross_subsidy_cols* may be a single string.
    """
    if isinstance(cross_subsidy_cols, str):
        cross_subsidy_cols = (cross_subsidy_cols,)

    group_values = _load_group_values(run_dir, group_col, storage_options)
    bills = _load_annual_target_bills(run_dir, annual_month, storage_options)

    bat_select = [pl.col(BLDG_ID_COL).cast(pl.Int64)] + [
        pl.col(col).cast(pl.Float64) for col in cross_subsidy_cols
    ]
    cross_sub_all = (
        pl.scan_csv(
            _csv_path(
                run_dir, "cross_subsidization/cross_subsidization_BAT_values.csv"
            ),
            storage_options=storage_options,
        )
        .select(bat_select)
        .group_by(BLDG_ID_COL)
        .agg([pl.col(col).sum() for col in cross_subsidy_cols])
    )

    joined = cast(
        pl.DataFrame,
        group_values.join(bills, on=BLDG_ID_COL, how="left")
        .join(cross_sub_all, on=BLDG_ID_COL, how="left")
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

    for col in cross_subsidy_cols:
        nulls_cs = joined.filter(pl.col(col).is_null()).height
        if nulls_cs:
            msg = f"Missing cross-subsidy values for {nulls_cs} buildings in {col}."
            raise ValueError(msg)

    nulls_weight = joined.filter(pl.col(WEIGHT_COL).is_null()).height
    if nulls_weight:
        msg = f"Missing sample weights for {nulls_weight} buildings."
        raise ValueError(msg)

    results: dict[str, pl.DataFrame] = {}
    for col in cross_subsidy_cols:
        results[col] = (
            joined.with_columns(
                (pl.col("annual_bill") * pl.col(WEIGHT_COL)).alias(
                    "weighted_annual_bill"
                ),
                (pl.col(col) * pl.col(WEIGHT_COL)).alias("weighted_cross_subsidy"),
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

    return results


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
    """Read state, utility, and revenue requirement from a scenario run.

    Raises KeyError if any required field is missing.
    """
    run = _load_run_from_scenario_config(scenario_config_path, run_num)
    for field in ("state", "utility", "utility_revenue_requirement"):
        if field not in run:
            raise KeyError(
                f"Run {run_num} in {scenario_config_path} is missing "
                f"required field '{field}'"
            )
    state = str(run["state"]).upper()
    utility = str(run["utility"]).lower()
    raw_rr = run["utility_revenue_requirement"]
    if isinstance(raw_rr, str) and raw_rr.endswith(".yaml"):
        rr_path = scenario_config_path.parent.parent / raw_rr
        with open(rr_path) as f:
            rr_data = yaml.safe_load(f)
        if "total_delivery_revenue_requirement" in rr_data:
            revenue_requirement = float(rr_data["total_delivery_revenue_requirement"])
        elif "revenue_requirement" in rr_data:
            revenue_requirement = float(rr_data["revenue_requirement"])
        else:
            raise KeyError(
                f"RR YAML at {rr_path} is missing both "
                "'total_delivery_revenue_requirement' and 'revenue_requirement'"
            )
    else:
        revenue_requirement = float(cast(float | int | str, raw_rr))
    return state, utility, revenue_requirement


def _write_revenue_requirement_yamls(
    delivery_breakdowns: dict[str, pl.DataFrame],
    run_dir: S3Path | Path,
    group_col: str,
    utility: str,
    default_revenue_requirement: float,
    differentiated_yaml_path: Path,
    default_yaml_path: Path,
    *,
    group_value_to_subclass: dict[str, str] | None = None,
    total_breakdowns: dict[str, pl.DataFrame] | None = None,
    total_delivery_rr: float | None = None,
    total_delivery_and_supply_rr: float | None = None,
    heating_type_breakdown: dict[str, dict[str, dict[str, float]]] | None = None,
    customer_count_override: float | None = None,
    kwh_scale_factor: float | None = None,
) -> tuple[Path, Path]:
    """Write per-subclass revenue requirement YAML with separate delivery/supply blocks.

    Delivery and supply allocation methods are independent.  Each run picks
    one delivery method and one supply method via ``residual_allocation_delivery``
    and ``residual_allocation_supply`` in its scenario YAML.

    Delivery methods: passthrough, percustomer, epmc, volumetric.
    Supply methods: passthrough, percustomer, volumetric.
    (Supply EPMC is omitted — broken by the run 1/run 2 subtraction architecture;
    volumetric gives a nearly identical result.)

    Supply pass-through = actual supply bills per subclass (no BAT adjustment).
    Supply percustomer/volumetric = supply bills - supply BAT (clean subtraction
    because per-customer and volumetric weights are constant across runs).
    """
    differentiated_yaml_path.parent.mkdir(parents=True, exist_ok=True)
    default_yaml_path.parent.mkdir(parents=True, exist_ok=True)

    gv_map = group_value_to_subclass or {}

    # --- Delivery block: BAT-adjusted RR per allocation method ---
    delivery_block: dict[str, dict[str, float]] = {}

    # Build passthrough_delivery from a single dedicated pass (sum_bills is the same
    # across all BAT columns for a given subclass, but multiple raw heating_type_v2
    # values may map to the same alias via group_value_to_subclass, so we sum).
    passthrough_delivery: dict[str, float] = {}
    _first_del_breakdown = next(iter(delivery_breakdowns.values()))
    for row in _first_del_breakdown.to_dicts():
        raw_val = str(row["subclass"])
        alias = gv_map.get(raw_val, raw_val)
        passthrough_delivery[alias] = passthrough_delivery.get(alias, 0.0) + float(
            row["sum_bills"]
        )

    for bat_col, delivery_breakdown in delivery_breakdowns.items():
        method_key = BAT_COL_TO_ALLOCATION_KEY.get(bat_col, bat_col)
        method_vals: dict[str, float] = {}
        for row in delivery_breakdown.to_dicts():
            raw_val = str(row["subclass"])
            alias = gv_map.get(raw_val, raw_val)
            method_vals[alias] = method_vals.get(alias, 0.0) + float(
                row["revenue_requirement"]
            )
        delivery_block[method_key] = method_vals

    delivery_block["passthrough"] = passthrough_delivery

    # --- Supply block: pass-through + BAT-adjusted methods ---
    supply_block: dict[str, dict[str, float]] = {}

    if total_breakdowns is not None:
        # Pass-through supply: actual supply bills per subclass (no BAT)
        any_col = next(iter(delivery_breakdowns))
        del_bills: dict[str, float] = {}
        for row in delivery_breakdowns[any_col].sort("subclass").to_dicts():
            raw_val = str(row["subclass"])
            alias = gv_map.get(raw_val, raw_val)
            del_bills[alias] = del_bills.get(alias, 0.0) + float(row["sum_bills"])

        tot_bills: dict[str, float] = {}
        for row in total_breakdowns[any_col].sort("subclass").to_dicts():
            raw_val = str(row["subclass"])
            alias = gv_map.get(raw_val, raw_val)
            tot_bills[alias] = tot_bills.get(alias, 0.0) + float(row["sum_bills"])

        passthrough_supply = {
            alias: tot_bills[alias] - del_bills[alias] for alias in del_bills
        }
        supply_block["passthrough"] = passthrough_supply

        # BAT-adjusted supply for methods with clean subtraction
        # (percustomer and volumetric weights don't change between runs)
        for bat_col in ("BAT_percustomer", "BAT_vol"):
            method_key = BAT_COL_TO_ALLOCATION_KEY[bat_col]
            if bat_col not in delivery_breakdowns or bat_col not in total_breakdowns:
                continue
            del_rr: dict[str, float] = {}
            for row in delivery_breakdowns[bat_col].to_dicts():
                raw_val = str(row["subclass"])
                alias = gv_map.get(raw_val, raw_val)
                del_rr[alias] = del_rr.get(alias, 0.0) + float(
                    row["revenue_requirement"]
                )
            tot_rr: dict[str, float] = {}
            for row in total_breakdowns[bat_col].to_dicts():
                raw_val = str(row["subclass"])
                alias = gv_map.get(raw_val, raw_val)
                tot_rr[alias] = tot_rr.get(alias, 0.0) + float(
                    row["revenue_requirement"]
                )
            supply_block[method_key] = {
                alias: tot_rr[alias] - del_rr[alias] for alias in del_rr
            }

    differentiated_data: dict[str, object] = {
        "utility": utility,
        "group_col": group_col,
        "source_run_dir": str(run_dir),
    }
    if customer_count_override is not None:
        differentiated_data["test_year_customer_count"] = customer_count_override
    if kwh_scale_factor is not None:
        differentiated_data["resstock_kwh_scale_factor"] = kwh_scale_factor
    if total_delivery_rr is not None:
        differentiated_data["total_delivery_revenue_requirement"] = total_delivery_rr
    if total_delivery_and_supply_rr is not None:
        differentiated_data["total_delivery_and_supply_revenue_requirement"] = (
            total_delivery_and_supply_rr
        )
    differentiated_data["subclass_revenue_requirements"] = {
        "delivery": delivery_block,
        "supply": supply_block,
    }
    if heating_type_breakdown is not None:
        differentiated_data["heating_type_breakdown"] = heating_type_breakdown

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
    """Write seasonal discount inputs CSV with a subclass-specific filename.

    The filename encodes ``group_col`` and ``subclass`` so that multiple
    subclass seasonal-discount computations sharing the same run directory do
    not overwrite each other (e.g. HP-seasonal and electric-heating-seasonal
    can both live under run-1's output directory).
    """
    target_dir = output_dir if output_dir is not None else run_dir
    # Derive filename from the DataFrame's group_col/subclass columns when present;
    # fall back to the legacy generic name for DataFrames that pre-date this change.
    if "group_col" in seasonal_inputs.columns and "subclass" in seasonal_inputs.columns:
        gc = str(seasonal_inputs["group_col"][0])
        sv = str(seasonal_inputs["subclass"][0])
        filename = seasonal_discount_filename(gc, sv)
    else:
        filename = DEFAULT_SEASONAL_OUTPUT_FILENAME
    output_path = str(target_dir / filename)
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
        default=",".join(SUBCLASS_RR_ALLOCATION_METHODS),
        help=(
            "Comma-separated BAT columns to compute subclass RR for. "
            f"Choices: {', '.join(BAT_METRIC_CHOICES)}. "
            f"Default: {','.join(SUBCLASS_RR_ALLOCATION_METHODS)}"
        ),
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
            "Path to scenario YAML used to read state, utility, and base "
            "revenue requirement for the run."
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
            "If provided with --upgrade, writes seasonal discount inputs for the subclass "
            "selected by --group-col / --subclass-value."
        ),
    )
    parser.add_argument(
        "--upgrade",
        default="00",
        help="Upgrade partition for loads (e.g. 00). Used when --resstock-base is set.",
    )
    parser.add_argument(
        "--subclass-value",
        default="true",
        help=(
            "Value of --group-col that identifies the target subclass for seasonal "
            "discount computation (default: 'true', i.e. HP customers when "
            "group-col=has_hp). Only used when --resstock-base is set."
        ),
    )
    parser.add_argument(
        "--base-tariff-json",
        help=(
            "Path to URDB-format base tariff JSON (e.g. <utility>_default_calibrated.json). "
            "Used to extract fixedchargefirstmeter for seasonal discount computation."
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
        "--run-dir-supply",
        help=(
            "Path to CAIRO output directory for the delivery+supply run (run 2). "
            "When provided, per-subclass supply RR is derived as total (run 2) "
            "minus delivery (run 1)."
        ),
    )
    args = parser.parse_args()

    run_dir: S3Path | Path = (
        S3Path(args.run_dir) if args.run_dir.startswith("s3://") else Path(args.run_dir)
    )
    output_dir: S3Path | Path | None = (
        _resolve_path_or_s3(args.output_dir) if args.output_dir else None
    )
    storage_options = get_aws_storage_options() if isinstance(run_dir, S3Path) else None

    cross_subsidy_cols = tuple(
        c.strip() for c in args.cross_subsidy_col.split(",") if c.strip()
    )

    delivery_breakdowns = compute_subclass_rr(
        run_dir=run_dir,
        group_col=args.group_col,
        cross_subsidy_cols=cross_subsidy_cols,
        annual_month=args.annual_month,
        storage_options=storage_options,
    )
    for col, breakdown in delivery_breakdowns.items():
        print(f"Delivery breakdown ({col}):")
        print(breakdown)

    run_state, run_utility, default_revenue_requirement = _load_run_fields(
        scenario_config_path=args.scenario_config,
        run_num=args.run_num,
    )

    gv_map: dict[str, str] | None = None
    if args.group_value_to_subclass:
        gv_map = parse_group_value_to_subclass(args.group_value_to_subclass)

    total_delivery_rr: float | None = None
    total_delivery_and_supply_rr: float | None = None
    base_rr_customer_count: float | None = None
    base_rr_kwh_scale_factor: float | None = None
    if args.base_rr_yaml:
        base_rr_path = args.scenario_config.parent.parent / args.base_rr_yaml
        with base_rr_path.open(encoding="utf-8") as f:
            base_rr_data = yaml.safe_load(f)
        total_delivery_rr = float(base_rr_data["total_delivery_revenue_requirement"])
        total_delivery_and_supply_rr = float(
            base_rr_data["total_delivery_and_supply_revenue_requirement"]
        )
        if "test_year_customer_count" in base_rr_data:
            base_rr_customer_count = float(base_rr_data["test_year_customer_count"])
        if "resstock_kwh_scale_factor" in base_rr_data:
            base_rr_kwh_scale_factor = float(base_rr_data["resstock_kwh_scale_factor"])

    total_breakdowns: dict[str, pl.DataFrame] | None = None
    if args.run_dir_supply:
        run_dir_supply: S3Path | Path = (
            S3Path(args.run_dir_supply)
            if args.run_dir_supply.startswith("s3://")
            else Path(args.run_dir_supply)
        )
        storage_options_supply = (
            get_aws_storage_options() if isinstance(run_dir_supply, S3Path) else None
        )
        total_breakdowns = compute_subclass_rr(
            run_dir=run_dir_supply,
            group_col=args.group_col,
            cross_subsidy_cols=cross_subsidy_cols,
            annual_month=args.annual_month,
            storage_options=storage_options_supply,
        )
        for col, tb in total_breakdowns.items():
            LOGGER.info("Run-2 (delivery+supply) breakdown (%s):\n%s", col, tb)

    # Informational breakdown by heating_type_v2 (if available in metadata).
    heating_type_breakdown: dict[str, dict[str, dict[str, float]]] | None = None
    try:
        ht_delivery = compute_subclass_rr(
            run_dir=run_dir,
            group_col="heating_type_v2",
            cross_subsidy_cols=cross_subsidy_cols,
            annual_month=args.annual_month,
            storage_options=storage_options,
        )
        ht_total: dict[str, pl.DataFrame] | None = None
        if args.run_dir_supply:
            ht_total = compute_subclass_rr(
                run_dir=run_dir_supply,
                group_col="heating_type_v2",
                cross_subsidy_cols=cross_subsidy_cols,
                annual_month=args.annual_month,
                storage_options=storage_options_supply,
            )
        heating_type_breakdown = {}
        for bat_col in cross_subsidy_cols:
            method_key = BAT_COL_TO_ALLOCATION_KEY.get(bat_col, bat_col)
            ht_del_df = ht_delivery[bat_col]
            ht_tot_by_sub: dict[str, float] = {}
            if ht_total is not None and bat_col in ht_total:
                for row in ht_total[bat_col].to_dicts():
                    ht_tot_by_sub[str(row["subclass"])] = float(
                        row["revenue_requirement"]
                    )
            method_block: dict[str, dict[str, float]] = {}
            for row in ht_del_df.to_dicts():
                sub = str(row["subclass"])
                delivery = float(row["revenue_requirement"])
                total = ht_tot_by_sub.get(sub, delivery)
                method_block[sub] = {
                    "delivery": delivery,
                    "supply": total - delivery,
                    "total": total,
                }
            heating_type_breakdown[method_key] = method_block
        for method, block in heating_type_breakdown.items():
            print(f"Heating type breakdown ({method}):")
            for sub, vals in block.items():
                print(f"  {sub}: {vals}")
    except (ValueError, KeyError) as exc:
        LOGGER.info(
            "Skipping heating_type_v2 breakdown (column may not exist): %s", exc
        )

    if args.write_revenue_requirement_yamls:
        differentiated_yaml_path, default_yaml_path = _write_revenue_requirement_yamls(
            delivery_breakdowns=delivery_breakdowns,
            run_dir=run_dir,
            group_col=args.group_col,
            utility=run_utility,
            default_revenue_requirement=default_revenue_requirement,
            differentiated_yaml_path=args.differentiated_yaml_path,
            default_yaml_path=args.default_yaml_path,
            group_value_to_subclass=gv_map,
            total_breakdowns=total_breakdowns,
            total_delivery_rr=total_delivery_rr,
            total_delivery_and_supply_rr=total_delivery_and_supply_rr,
            heating_type_breakdown=heating_type_breakdown,
            customer_count_override=base_rr_customer_count,
            kwh_scale_factor=base_rr_kwh_scale_factor,
        )
        print(f"Wrote differentiated YAML: {differentiated_yaml_path}")
        print(f"Wrote default YAML: {default_yaml_path}")

    if args.resstock_base:
        winter_months = _resolve_winter_months(
            state=run_state,
            utility=run_utility,
            periods_yaml_path=args.periods_yaml,
        )
        base_tariff_json_path = (
            _resolve_path_or_s3(args.base_tariff_json)
            if args.base_tariff_json
            else None
        )
        # Use the single cross-subsidy column for seasonal discount (first col if multiple).
        seasonal_cross_subsidy_col = (
            cross_subsidy_cols[0] if cross_subsidy_cols else DEFAULT_BAT_METRIC
        )
        seasonal_inputs = compute_subclass_seasonal_discount_inputs(
            run_dir=run_dir,
            resstock_base=args.resstock_base,
            state=run_state,
            upgrade=args.upgrade,
            group_col=args.group_col,
            subclass_value=args.subclass_value,
            cross_subsidy_col=seasonal_cross_subsidy_col,
            storage_options=storage_options,
            base_tariff_json_path=base_tariff_json_path,
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
