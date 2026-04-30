"""Prepare 12 uncalibrated fair-default tariff JSONs for one utility.

For each of 6 (subclass × strategy) combos and 2 (delivery, supply) variants:
1. Loads shared fair-default inputs once per subclass and delivery/supply variant.
2. Derives strategy-specific fair-default parameters.
3. Calls create_fair_default_tariff to emit the uncalibrated URDB tariff JSON.

The tariff_key embedded in each JSON matches the filename stem so that
copy_calibrated_tariff_from_run produces correctly-named _calibrated outputs.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import cast

import pandas as pd
import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils import get_aws_region
from utils.cairo import (
    add_bulk_tx_and_dist_and_sub_tx_marginal_cost,
    load_dist_and_sub_tx_marginal_costs,
)
from utils.loads import (
    ELECTRIC_LOAD_COL,
    ELECTRIC_PV_COL,
    grid_consumption_expr,
    hourly_resstock_load_from_parquet,
    scan_resstock_loads,
)
from utils.mid.compute_fair_default_inputs import (
    CustomerGroupTotals,
    FairDefaultInputs,
    derive_fair_default_rate_designs,
    fair_default_inputs_frame,
    fair_default_rate_design_modules,
    fixed_charge_feasibility,
    _load_bill_totals,
    _load_metadata_and_cross_subsidy,
    _resolve_winter_months,
)
from utils.mid.compute_subclass_rr import (
    BLDG_ID_COL,
    DEFAULT_BAT_METRIC,
    WEIGHT_COL,
    _extract_fixed_charge_from_urdb,
    _resolve_path_or_s3,
    parse_group_value_to_subclass,
)
from utils.mid.create_fair_default_tariff import (
    FairDefaultStrategy,
    _read_json,
    _write_json,
    create_fair_default_tariff,
)
from utils.pre.compute_tou import (
    compute_mc_seasonal_ratio,
)
from utils.scenario_config import get_residential_customer_count_from_utility_stats

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SubclassConfig:
    """Subclass identification for fair-default design."""

    shorthand: str
    group_col: str
    group_value: str
    group_value_to_subclass: str | None


@dataclass(frozen=True)
class FairDefaultCombo:
    """One (subclass, strategy) combination."""

    subclass: SubclassConfig
    strategy: FairDefaultStrategy
    strategy_key: str


@dataclass(frozen=True)
class KwhTotals:
    """Weighted load totals reused across delivery/supply fair-default designs."""

    building_ids: frozenset[int]
    class_annual_kwh: float
    class_winter_kwh: float
    subclass_annual_kwh: dict[str, float]
    subclass_winter_kwh: dict[str, float]


SUBCLASSES: list[SubclassConfig] = [
    SubclassConfig(
        shorthand="hp",
        group_col="has_hp",
        group_value="true",
        group_value_to_subclass=None,
    ),
    SubclassConfig(
        shorthand="eheat",
        group_col="heating_type_v2",
        group_value="electric_heating",
        group_value_to_subclass=(
            "heat_pump=electric_heating,electrical_resistance=electric_heating"
        ),
    ),
]

STRATEGY_KEYS: list[tuple[FairDefaultStrategy, str]] = [
    ("fixed_charge_only", "default_fixed"),
    ("seasonal_rates_only", "default_seasonal"),
    ("fixed_plus_seasonal_mc", "default_fixed_seasonal_mc"),
]

ALL_COMBOS: list[FairDefaultCombo] = [
    FairDefaultCombo(subclass=sub, strategy=strat, strategy_key=key)
    for sub in SUBCLASSES
    for strat, key in STRATEGY_KEYS
]


def _align_load_to_mc(hourly_load: pd.Series, mc_index: pd.DatetimeIndex) -> pd.Series:
    """Align an hourly load series to an MC index by position when lengths match.

    Mirrors :func:`utils.pre.derive_seasonal_tou.derive_seasonal_tou`'s alignment
    so the demand-weighted MC averages line up the same way both workflows do.
    ResStock loads can come from a different calendar year than MC; if they have
    the same length we trust the positional alignment, otherwise we forward-fill
    onto the MC index.
    """
    if hourly_load.index.equals(mc_index):
        return hourly_load
    if len(hourly_load) == len(mc_index):
        return pd.Series(hourly_load.values, index=mc_index, name=hourly_load.name)
    return hourly_load.reindex(mc_index, method="ffill")


def _load_utility_hourly_load(
    *,
    utility: str,
    state: str,
    upgrade: str,
    resstock_base: str,
    path_utility_assignment: str,
    path_electric_utility_stats: str,
    storage_options: dict[str, str] | None,
) -> pd.Series:
    """Load utility-aggregate hourly ResStock load for demand-weighting MC.

    Mirrors the load-loading pattern in
    :func:`utils.pre.derive_seasonal_tou.load_tou_inputs`: pull all buildings
    assigned to ``utility`` from ``utility_assignment.parquet``, rescale weights
    to the EIA residential customer count, then sum weighted hourly load.
    """
    aws_storage_options = {"aws_region": get_aws_region()}
    customer_count = get_residential_customer_count_from_utility_stats(
        path_electric_utility_stats,
        utility,
        storage_options=aws_storage_options,
    )
    LOGGER.info("Residential customer count for %s: %d", utility, customer_count)

    ua_df = cast(
        pl.DataFrame,
        pl.scan_parquet(path_utility_assignment)
        .filter(pl.col("sb.electric_utility") == utility)
        .select("bldg_id", "weight")
        .collect(),
    )
    raw_weight_sum = float(ua_df["weight"].sum())
    if raw_weight_sum > 0:
        ua_df = ua_df.with_columns(
            (pl.col("weight") / raw_weight_sum * customer_count).alias("weight")
        )
    weights_df = ua_df.select(
        pl.col("bldg_id").cast(pl.Int64),
        pl.col("weight").cast(pl.Float64),
    )
    building_ids = ua_df["bldg_id"].to_list()
    LOGGER.info(
        "Loading %d buildings of ResStock load for %s utility-aggregate demand "
        "(used to demand-weight rho_MC)",
        len(building_ids),
        utility,
    )
    loads_lf = scan_resstock_loads(
        resstock_base,
        state,
        upgrade,
        building_ids=building_ids,
        storage_options=storage_options,
    )
    return hourly_resstock_load_from_parquet(
        loads_lf,
        weights_df,
        load_col=ELECTRIC_LOAD_COL,
    )


def _compute_delivery_mc_seasonal_ratio(
    path_dist_and_sub_tx_mc: str,
    path_bulk_tx_mc: str,
    winter_months: list[int] | None,
    hourly_load: pd.Series,
) -> float:
    """Compute demand-weighted MC seasonal ratio from delivery-side marginal costs."""
    dist_series = load_dist_and_sub_tx_marginal_costs(path_dist_and_sub_tx_mc)
    combined = add_bulk_tx_and_dist_and_sub_tx_marginal_cost(
        path_dist_and_sub_tx_mc=path_dist_and_sub_tx_mc,
        path_bulk_tx_mc=path_bulk_tx_mc,
        target_index=pd.DatetimeIndex(dist_series.index),
    )
    aligned_load = _align_load_to_mc(hourly_load, pd.DatetimeIndex(combined.index))
    return compute_mc_seasonal_ratio(combined, aligned_load, winter_months)


def _compute_supply_mc_seasonal_ratio(
    path_supply_energy_mc: str,
    path_supply_capacity_mc: str,
    storage_options: dict | None,
    winter_months: list[int] | None,
    hourly_load: pd.Series,
) -> float:
    """Compute demand-weighted MC seasonal ratio from supply-side marginal costs.

    Sums energy and capacity MC columns to get total supply MC per hour.
    """
    energy_df = cast(
        pl.DataFrame,
        pl.scan_parquet(
            path_supply_energy_mc, storage_options=storage_options or {}
        ).collect(),
    ).to_pandas()
    cap_df = cast(
        pl.DataFrame,
        pl.scan_parquet(
            path_supply_capacity_mc, storage_options=storage_options or {}
        ).collect(),
    ).to_pandas()

    for df in [energy_df, cap_df]:
        for col in ["timestamp", "datetime", "time"]:
            if col in df.columns:
                df.index = pd.DatetimeIndex(df[col])
                break

    numeric_energy = [
        c for c in energy_df.columns if energy_df[c].dtype in (float, "float64")
    ]
    numeric_cap = [c for c in cap_df.columns if cap_df[c].dtype in (float, "float64")]
    combined = energy_df[numeric_energy[0]] + cap_df[numeric_cap[0]]
    combined.name = "total_supply_mc"
    aligned_load = _align_load_to_mc(hourly_load, pd.DatetimeIndex(combined.index))
    return compute_mc_seasonal_ratio(combined, aligned_load, winter_months)


def _patch_tariff_key(tariff: dict, tariff_key: str) -> dict:
    """Set the tariff_key field (used by copy_calibrated_tariff_from_run)."""
    tariff["tariff_key"] = tariff_key
    return tariff


def _derive_fair_default_inputs_frame(
    *,
    inputs: FairDefaultInputs,
    winter_months: tuple[int, ...],
    group_col: str,
    subclass_value: str,
    state: str,
    upgrade: str,
    mc_seasonal_ratio: float | None,
) -> pl.DataFrame:
    """Derive one fair-default inputs row from cached shared inputs."""
    designs = derive_fair_default_rate_designs(
        inputs,
        fair_default_rate_design_modules(mc_seasonal_ratio),
    )
    seasonal = designs["seasonal_rates_only"]
    if not seasonal.feasible:
        LOGGER.warning(
            "seasonal_rates_only fair-default module produced a negative rate "
            "(winter=%s, summer=%s); clipped residual cross-subsidy is %s.",
            seasonal.winter_rate,
            seasonal.summer_rate,
            seasonal.residual_cross_subsidy,
        )

    LOGGER.info("fair_default_inputs [%s=%s]: done", group_col, subclass_value)
    return fair_default_inputs_frame(
        inputs=inputs,
        designs=designs,
        feasibility=fixed_charge_feasibility(inputs),
        group_col=group_col,
        subclass_value=subclass_value,
        cross_subsidy_col=DEFAULT_BAT_METRIC,
        state=state,
        upgrade=upgrade,
        winter_months=winter_months,
        mc_seasonal_ratio=mc_seasonal_ratio,
    )


def _load_kwh_totals_for_subclasses(
    *,
    resstock_base: str,
    state: str,
    upgrade: str,
    metadata_by_subclass: dict[str, pl.DataFrame],
    winter_months: tuple[int, ...],
    storage_options: dict[str, str] | None,
) -> KwhTotals:
    """Scan ResStock loads once and aggregate kWh for all fair-default subclasses."""
    first_shorthand = next(iter(metadata_by_subclass))
    weights = metadata_by_subclass[first_shorthand].select(BLDG_ID_COL, WEIGHT_COL)
    building_ids = weights[BLDG_ID_COL].to_list()
    building_id_set = frozenset(int(bldg_id) for bldg_id in building_ids)

    weights_with_flags = weights
    for shorthand, metadata in metadata_by_subclass.items():
        metadata_building_ids = frozenset(
            int(bldg_id) for bldg_id in metadata[BLDG_ID_COL].to_list()
        )
        if metadata_building_ids != building_id_set:
            raise ValueError(
                "Fair-default subclass metadata must use the same building sample "
                f"for all subclasses; {shorthand} differs."
            )
        weights_with_flags = weights_with_flags.join(
            metadata.select(
                BLDG_ID_COL,
                pl.col("_is_subclass").alias(f"_is_{shorthand}"),
            ),
            on=BLDG_ID_COL,
            how="inner",
        )

    t0 = perf_counter()
    loads = scan_resstock_loads(
        resstock_base,
        state,
        upgrade,
        building_ids=building_ids,
        storage_options=storage_options,
    )
    LOGGER.info(
        "fair_default_inputs: prepared one loads scan for %d buildings in %.2fs",
        len(building_ids),
        perf_counter() - t0,
    )

    t1 = perf_counter()
    aggregate_exprs: list[pl.Expr] = [
        pl.col("weighted_kwh").sum().alias("class_annual_kwh"),
        pl.when(pl.col("is_winter"))
        .then(pl.col("weighted_kwh"))
        .otherwise(0.0)
        .sum()
        .alias("class_winter_kwh"),
    ]
    for shorthand in metadata_by_subclass:
        aggregate_exprs.extend(
            [
                pl.when(pl.col(f"_is_{shorthand}"))
                .then(pl.col("weighted_kwh"))
                .otherwise(0.0)
                .sum()
                .alias(f"{shorthand}_annual_kwh"),
                pl.when(pl.col(f"_is_{shorthand}") & pl.col("is_winter"))
                .then(pl.col("weighted_kwh"))
                .otherwise(0.0)
                .sum()
                .alias(f"{shorthand}_winter_kwh"),
            ]
        )

    kwh = cast(
        pl.DataFrame,
        loads.join(weights_with_flags.lazy(), on=BLDG_ID_COL, how="inner")
        .select(
            pl.col("timestamp")
            .cast(pl.String, strict=False)
            .str.to_datetime(strict=False)
            .dt.month()
            .alias("month_num"),
            (
                grid_consumption_expr(ELECTRIC_LOAD_COL, ELECTRIC_PV_COL)
                * pl.col(WEIGHT_COL).cast(pl.Float64)
            ).alias("weighted_kwh"),
            *[pl.col(f"_is_{shorthand}") for shorthand in metadata_by_subclass],
        )
        .with_columns(pl.col("month_num").is_in(winter_months).alias("is_winter"))
        .select(*aggregate_exprs)
        .collect(engine="streaming"),
    )
    LOGGER.info(
        "fair_default_inputs: collected all subclass load totals in %.2fs",
        perf_counter() - t1,
    )
    return KwhTotals(
        building_ids=building_id_set,
        class_annual_kwh=float(kwh["class_annual_kwh"][0] or 0.0),
        class_winter_kwh=float(kwh["class_winter_kwh"][0] or 0.0),
        subclass_annual_kwh={
            shorthand: float(kwh[f"{shorthand}_annual_kwh"][0] or 0.0)
            for shorthand in metadata_by_subclass
        },
        subclass_winter_kwh={
            shorthand: float(kwh[f"{shorthand}_winter_kwh"][0] or 0.0)
            for shorthand in metadata_by_subclass
        },
    )


def prepare_fair_default_tariffs(
    *,
    utility: str,
    state: str,
    run_dir_delivery: str | Path | S3Path,
    run_dir_supply: str | Path | S3Path,
    output_dir: Path,
    resstock_base: str,
    upgrade: str,
    path_dist_and_sub_tx_mc: str,
    path_bulk_tx_mc: str,
    path_supply_energy_mc: str,
    path_supply_capacity_mc: str,
    path_base_tariff_delivery: str | Path,
    path_base_tariff_supply: str | Path,
    path_utility_assignment: str,
    path_electric_utility_stats: str,
    path_periods_yaml: Path | None = None,
    allow_infeasible: bool = False,
) -> list[str]:
    """Build 12 uncalibrated fair-default tariff JSONs.

    Returns:
        List of paths to written tariff JSON files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    storage_options = (
        get_aws_storage_options()
        if isinstance(run_dir_delivery, S3Path)
        or str(run_dir_delivery).startswith("s3://")
        or resstock_base.startswith("s3://")
        else None
    )

    run_dir_delivery_resolved = _resolve_path_or_s3(str(run_dir_delivery))
    run_dir_supply_resolved = _resolve_path_or_s3(str(run_dir_supply))

    resolved_winter_months = _resolve_winter_months(path_periods_yaml)
    winter_months = list(resolved_winter_months)

    LOGGER.info(
        "Loading utility-aggregate hourly ResStock load for %s "
        "(used to demand-weight rho_MC) ...",
        utility,
    )
    utility_hourly_load = _load_utility_hourly_load(
        utility=utility,
        state=state,
        upgrade=upgrade,
        resstock_base=resstock_base,
        path_utility_assignment=path_utility_assignment,
        path_electric_utility_stats=path_electric_utility_stats,
        storage_options=storage_options,
    )

    LOGGER.info("Computing delivery MC seasonal ratio for %s ...", utility)
    delivery_mc_ratio = _compute_delivery_mc_seasonal_ratio(
        path_dist_and_sub_tx_mc=path_dist_and_sub_tx_mc,
        path_bulk_tx_mc=path_bulk_tx_mc,
        winter_months=winter_months,
        hourly_load=utility_hourly_load,
    )
    LOGGER.info("Delivery MC seasonal ratio: %.4f", delivery_mc_ratio)

    LOGGER.info("Computing supply MC seasonal ratio for %s ...", utility)
    supply_mc_ratio = _compute_supply_mc_seasonal_ratio(
        path_supply_energy_mc=path_supply_energy_mc,
        path_supply_capacity_mc=path_supply_capacity_mc,
        storage_options=storage_options,
        winter_months=winter_months,
        hourly_load=utility_hourly_load,
    )
    LOGGER.info("Supply MC seasonal ratio: %.4f", supply_mc_ratio)

    base_tariff_delivery = _read_json(
        Path(path_base_tariff_delivery)
        if not isinstance(path_base_tariff_delivery, Path)
        else path_base_tariff_delivery
    )
    base_tariff_supply = _read_json(
        Path(path_base_tariff_supply)
        if not isinstance(path_base_tariff_supply, Path)
        else path_base_tariff_supply
    )

    written_paths: list[str] = []
    metadata_cache: dict[tuple[bool, str], tuple[pl.DataFrame, float]] = {}
    kwh_totals_cache: KwhTotals | None = None
    shared_inputs_cache: dict[tuple[bool, str], FairDefaultInputs] = {}

    def get_metadata_and_cross_subsidy(
        *,
        is_supply: bool,
        subclass: SubclassConfig,
    ) -> tuple[pl.DataFrame, float]:
        cache_key = (is_supply, subclass.shorthand)
        if cache_key not in metadata_cache:
            run_dir = (
                run_dir_supply_resolved if is_supply else run_dir_delivery_resolved
            )
            metadata_cache[cache_key] = _load_metadata_and_cross_subsidy(
                run_dir=run_dir,
                group_col=subclass.group_col,
                subclass_value=subclass.group_value,
                cross_subsidy_col=DEFAULT_BAT_METRIC,
                storage_options=storage_options,
                group_value_to_subclass=(
                    parse_group_value_to_subclass(subclass.group_value_to_subclass)
                    if subclass.group_value_to_subclass
                    else None
                ),
            )
        return metadata_cache[cache_key]

    def get_kwh_totals() -> KwhTotals:
        nonlocal kwh_totals_cache
        if kwh_totals_cache is None:
            delivery_metadata_by_subclass = {
                subclass.shorthand: get_metadata_and_cross_subsidy(
                    is_supply=False,
                    subclass=subclass,
                )[0]
                for subclass in SUBCLASSES
            }
            kwh_totals_cache = _load_kwh_totals_for_subclasses(
                resstock_base=resstock_base,
                state=state,
                upgrade=upgrade,
                metadata_by_subclass=delivery_metadata_by_subclass,
                winter_months=resolved_winter_months,
                storage_options=storage_options,
            )
        return kwh_totals_cache

    def get_shared_inputs(
        *,
        is_supply: bool,
        subclass: SubclassConfig,
    ) -> FairDefaultInputs:
        cache_key = (is_supply, subclass.shorthand)
        if cache_key not in shared_inputs_cache:
            run_dir = (
                run_dir_supply_resolved if is_supply else run_dir_delivery_resolved
            )
            base_tariff_json_path = (
                Path(path_base_tariff_supply)
                if is_supply
                else Path(path_base_tariff_delivery)
            )
            LOGGER.info(
                "Loading shared fair-default inputs for %s %s ...",
                subclass.shorthand,
                "supply" if is_supply else "delivery",
            )
            metadata, subclass_cross_subsidy = get_metadata_and_cross_subsidy(
                is_supply=is_supply,
                subclass=subclass,
            )
            metadata_building_ids = frozenset(
                int(bldg_id) for bldg_id in metadata[BLDG_ID_COL].to_list()
            )
            kwh_totals = get_kwh_totals()
            if metadata_building_ids != kwh_totals.building_ids:
                raise ValueError(
                    "Fair-default delivery and supply runs must use the same "
                    f"building sample; {subclass.shorthand} "
                    f"{'supply' if is_supply else 'delivery'} differs."
                )

            class_bill, subclass_bill = _load_bill_totals(
                run_dir,
                metadata,
                storage_options,
            )
            class_annual_kwh = kwh_totals.class_annual_kwh
            class_winter_kwh = kwh_totals.class_winter_kwh
            subclass_annual_kwh = kwh_totals.subclass_annual_kwh[subclass.shorthand]
            subclass_winter_kwh = kwh_totals.subclass_winter_kwh[subclass.shorthand]
            class_totals = CustomerGroupTotals(
                customer_count=float(metadata[WEIGHT_COL].sum() or 0.0),
                current_bill=class_bill,
                annual_kwh=class_annual_kwh,
                winter_kwh=class_winter_kwh,
                summer_kwh=class_annual_kwh - class_winter_kwh,
            )
            subclass_totals = CustomerGroupTotals(
                customer_count=float(
                    metadata.filter(pl.col("_is_subclass"))[WEIGHT_COL].sum() or 0.0
                ),
                current_bill=subclass_bill,
                annual_kwh=subclass_annual_kwh,
                winter_kwh=subclass_winter_kwh,
                summer_kwh=subclass_annual_kwh - subclass_winter_kwh,
            )
            class_totals.validate("class")
            subclass_totals.validate("subclass")
            shared_inputs_cache[cache_key] = FairDefaultInputs(
                class_totals=class_totals,
                subclass_totals=subclass_totals,
                subclass_cross_subsidy=subclass_cross_subsidy,
                base_fixed_charge=_extract_fixed_charge_from_urdb(
                    base_tariff_json_path
                ),
                fixed_charge_floor=0.0,
            )
        return shared_inputs_cache[cache_key]

    for combo in ALL_COMBOS:
        sub = combo.subclass
        strategy = combo.strategy
        key = combo.strategy_key

        mc_ratio_delivery = (
            delivery_mc_ratio if strategy == "fixed_plus_seasonal_mc" else None
        )
        mc_ratio_supply = (
            supply_mc_ratio if strategy == "fixed_plus_seasonal_mc" else None
        )

        for is_supply in (False, True):
            base_tariff = base_tariff_supply if is_supply else base_tariff_delivery
            mc_ratio = mc_ratio_supply if is_supply else mc_ratio_delivery

            if is_supply:
                tariff_stem = f"{utility}_{sub.shorthand}_{key}_supply"
            else:
                tariff_stem = f"{utility}_{sub.shorthand}_{key}"

            LOGGER.info("Processing %s ...", tariff_stem)

            inputs_output_dir = output_dir / ".fair_default_inputs"
            inputs_output_dir.mkdir(parents=True, exist_ok=True)

            shared_inputs = get_shared_inputs(
                is_supply=is_supply,
                subclass=sub,
            )
            inputs_df = _derive_fair_default_inputs_frame(
                inputs=shared_inputs,
                winter_months=resolved_winter_months,
                group_col=sub.group_col,
                subclass_value=sub.group_value,
                state=state,
                upgrade=upgrade,
                mc_seasonal_ratio=mc_ratio,
            )

            inputs_csv_path = inputs_output_dir / f"{tariff_stem}_inputs.csv"
            csv_text = inputs_df.write_csv(None)
            if not isinstance(csv_text, str):
                raise ValueError("Failed to render fair default input CSV text.")
            inputs_csv_path.write_text(csv_text, encoding="utf-8")
            LOGGER.info("Wrote inputs CSV: %s", inputs_csv_path)

            row = inputs_df.row(0, named=True)

            tariff = create_fair_default_tariff(
                base_tariff=base_tariff,
                inputs_row=row,
                strategy=strategy,
                label=tariff_stem,
                periods_yaml_path=path_periods_yaml,
                allow_infeasible=allow_infeasible,
            )
            tariff = _patch_tariff_key(tariff, tariff_stem)

            output_path = output_dir / f"{tariff_stem}.json"
            written = _write_json(output_path, tariff)
            LOGGER.info("Wrote tariff JSON: %s", written)
            written_paths.append(written)

    return written_paths


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description=(
            "Build 12 uncalibrated fair-default tariff JSONs for one utility. "
            "Loops 6 (subclass × strategy) combos × 2 (delivery, supply) variants."
        )
    )
    parser.add_argument("--utility", required=True, help="Utility name (e.g. nyseg).")
    parser.add_argument("--state", required=True, help="State abbreviation (e.g. NY).")
    parser.add_argument(
        "--run-dir-delivery",
        required=True,
        help="CAIRO output directory for baseline delivery run (run-1).",
    )
    parser.add_argument(
        "--run-dir-supply",
        required=True,
        help="CAIRO output directory for baseline supply run (run-2).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Directory to write uncalibrated tariff JSONs (e.g. config/tariffs/electric).",
    )
    parser.add_argument(
        "--resstock-base",
        required=True,
        help="Base path to ResStock release (e.g. s3://.../res_2024_amy2018_2_sb).",
    )
    parser.add_argument(
        "--upgrade",
        default="00",
        help="ResStock upgrade partition to use (default: 00).",
    )
    parser.add_argument(
        "--path-dist-and-sub-tx-mc",
        required=True,
        help="Parquet path for dist+sub-tx marginal costs.",
    )
    parser.add_argument(
        "--path-bulk-tx-mc",
        required=True,
        help="Parquet path for bulk-tx marginal costs.",
    )
    parser.add_argument(
        "--path-supply-energy-mc",
        required=True,
        help="Parquet path for supply energy marginal costs.",
    )
    parser.add_argument(
        "--path-supply-capacity-mc",
        required=True,
        help="Parquet path for supply capacity marginal costs.",
    )
    parser.add_argument(
        "--base-tariff-delivery",
        required=True,
        help="Calibrated delivery base tariff JSON (e.g. nyseg_default_calibrated.json).",
    )
    parser.add_argument(
        "--base-tariff-supply",
        required=True,
        help=(
            "Calibrated supply base tariff JSON "
            "(e.g. nyseg_default_supply_calibrated.json)."
        ),
    )
    parser.add_argument(
        "--path-utility-assignment",
        required=True,
        help=(
            "Parquet path for ResStock utility_assignment metadata "
            "(used to load utility-aggregate hourly load for demand-weighting "
            "rho_MC)."
        ),
    )
    parser.add_argument(
        "--path-electric-utility-stats",
        required=True,
        help=(
            "Parquet path for EIA-861 electric utility stats "
            "(used to rescale ResStock weights to the EIA residential "
            "customer count for the utility-aggregate hourly load)."
        ),
    )
    parser.add_argument(
        "--periods-yaml",
        type=Path,
        default=None,
        help="Optional periods YAML for winter months configuration.",
    )
    parser.add_argument(
        "--allow-infeasible",
        action="store_true",
        help="Clip negative charges/rates at zero instead of raising.",
    )
    args = parser.parse_args()

    written = prepare_fair_default_tariffs(
        utility=args.utility,
        state=args.state,
        run_dir_delivery=args.run_dir_delivery,
        run_dir_supply=args.run_dir_supply,
        output_dir=args.output_dir,
        resstock_base=args.resstock_base,
        upgrade=args.upgrade,
        path_dist_and_sub_tx_mc=args.path_dist_and_sub_tx_mc,
        path_bulk_tx_mc=args.path_bulk_tx_mc,
        path_supply_energy_mc=args.path_supply_energy_mc,
        path_supply_capacity_mc=args.path_supply_capacity_mc,
        path_base_tariff_delivery=args.base_tariff_delivery,
        path_base_tariff_supply=args.base_tariff_supply,
        path_utility_assignment=args.path_utility_assignment,
        path_electric_utility_stats=args.path_electric_utility_stats,
        path_periods_yaml=args.periods_yaml,
        allow_infeasible=args.allow_infeasible,
    )
    print(f"Wrote {len(written)} tariff JSON files:")
    for path in written:
        print(f"  {path}")


if __name__ == "__main__":
    main()
