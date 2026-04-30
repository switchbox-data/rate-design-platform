"""Compute fair-default rate design inputs from CAIRO outputs and ResStock loads."""

from __future__ import annotations

import argparse
import logging
import math
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import cast

import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.loads import ELECTRIC_LOAD_COL, ELECTRIC_PV_COL, grid_consumption_expr
from utils.loads import scan_resstock_loads, scan_resstock_loads_monthly
from utils.mid.compute_subclass_rr import (
    ANNUAL_MONTH_VALUE,
    BAT_METRIC_CHOICES,
    BLDG_ID_COL,
    DEFAULT_BAT_METRIC,
    DEFAULT_GROUP_COL,
    GROUP_VALUE_COL,
    WEIGHT_COL,
    _extract_fixed_charge_from_urdb,
    _load_annual_target_bills,
    _load_group_values,
    _load_subclass_cross_subsidy_inputs,
    _resolve_path_or_s3,
    _resolve_selector_group_values,
    parse_group_value_to_subclass,
)
from utils.pre.season_config import (
    DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
    load_winter_months_from_periods,
)

LOGGER = logging.getLogger(__name__)
DEFAULT_OUTPUT_FILENAME = "fair_default_inputs.csv"
MONTHS_PER_YEAR = 12.0
ZERO_TOLERANCE = 1e-12


@dataclass(frozen=True, slots=True)
class CustomerGroupTotals:
    """Weighted customer count, bills, and seasonal kWh for a group."""

    customer_count: float
    current_bill: float
    annual_kwh: float
    winter_kwh: float
    summer_kwh: float

    def validate(self, label: str) -> None:
        for name, value in (
            ("customer_count", self.customer_count),
            ("annual_kwh", self.annual_kwh),
            ("winter_kwh", self.winter_kwh),
            ("summer_kwh", self.summer_kwh),
        ):
            if value <= 0.0:
                raise ValueError(f"{label}_{name} must be positive; got {value}.")


@dataclass(frozen=True, slots=True)
class FairDefaultInputs:
    """All inputs shared by fair-default rate design modules."""

    class_totals: CustomerGroupTotals
    subclass_totals: CustomerGroupTotals
    subclass_cross_subsidy: float
    base_fixed_charge: float
    fixed_charge_floor: float

    @property
    def subclass_fair_bill(self) -> float:
        return self.subclass_totals.current_bill - self.subclass_cross_subsidy

    @property
    def class_energy_revenue(self) -> float:
        return energy_revenue(self.class_totals, self.base_fixed_charge)

    @property
    def subclass_energy_revenue(self) -> float:
        return energy_revenue(self.subclass_totals, self.base_fixed_charge)

    @property
    def base_flat_rate(self) -> float:
        return self.class_energy_revenue / self.class_totals.annual_kwh


@dataclass(frozen=True, slots=True)
class FairDefaultRateDesign:
    """One callable rate design module's output."""

    module: str
    fixed_charge: float
    flat_rate: float | None
    winter_rate: float | None
    summer_rate: float | None
    discriminant: float
    feasible: bool
    clipped_winter_rate: float | None = None
    clipped_summer_rate: float | None = None
    residual_cross_subsidy: float = 0.0


RateDesignModule = Callable[[FairDefaultInputs], FairDefaultRateDesign]


def energy_revenue(group: CustomerGroupTotals, fixed_charge: float) -> float:
    return group.current_bill - MONTHS_PER_YEAR * fixed_charge * group.customer_count


def seasonal_bill(
    group: CustomerGroupTotals,
    fixed_charge: float,
    winter_rate: float,
    summer_rate: float,
) -> float:
    return (
        MONTHS_PER_YEAR * fixed_charge * group.customer_count
        + winter_rate * group.winter_kwh
        + summer_rate * group.summer_kwh
    )


def _require_nonzero(value: float, label: str) -> None:
    if math.isclose(value, 0.0, abs_tol=ZERO_TOLERANCE):
        raise ValueError(f"{label} is degenerate; denominator is zero.")


def fixed_charge_only_rate_design(inputs: FairDefaultInputs) -> FairDefaultRateDesign:
    """Solve Strategy A: fixed-charge-only fair-default design."""
    class_totals = inputs.class_totals
    subclass_totals = inputs.subclass_totals
    denominator = (
        class_totals.customer_count * subclass_totals.annual_kwh
        - subclass_totals.customer_count * class_totals.annual_kwh
    )
    _require_nonzero(denominator, "fixed rate design")
    fixed_charge = (
        class_totals.current_bill * subclass_totals.annual_kwh
        - inputs.subclass_fair_bill * class_totals.annual_kwh
    ) / (MONTHS_PER_YEAR * denominator)
    return FairDefaultRateDesign(
        module="fixed_charge_only",
        fixed_charge=fixed_charge,
        flat_rate=None,
        winter_rate=None,
        summer_rate=None,
        discriminant=MONTHS_PER_YEAR * denominator,
        feasible=fixed_charge >= inputs.fixed_charge_floor,
    )


def _seasonal_denominator(inputs: FairDefaultInputs) -> float:
    class_totals = inputs.class_totals
    subclass_totals = inputs.subclass_totals
    denominator = (
        class_totals.winter_kwh * subclass_totals.summer_kwh
        - subclass_totals.winter_kwh * class_totals.summer_kwh
    )
    _require_nonzero(denominator, "seasonal rate design")
    return denominator


def _seasonal_rates_at_fixed_charge(
    inputs: FairDefaultInputs,
    fixed_charge: float,
) -> tuple[float, float, float]:
    class_totals = inputs.class_totals
    subclass_totals = inputs.subclass_totals
    denominator = _seasonal_denominator(inputs)
    class_energy_target = energy_revenue(class_totals, fixed_charge)
    subclass_energy_target = (
        inputs.subclass_fair_bill
        - MONTHS_PER_YEAR * fixed_charge * subclass_totals.customer_count
    )
    winter_rate = (
        class_energy_target * subclass_totals.summer_kwh
        - subclass_energy_target * class_totals.summer_kwh
    ) / denominator
    summer_rate = (
        subclass_energy_target * class_totals.winter_kwh
        - class_energy_target * subclass_totals.winter_kwh
    ) / denominator
    return winter_rate, summer_rate, denominator


def _clip_seasonal_design(
    inputs: FairDefaultInputs,
    fixed_charge: float,
    winter_rate: float,
    summer_rate: float,
) -> tuple[float, float, float]:
    if winter_rate >= 0.0 and summer_rate >= 0.0:
        return winter_rate, summer_rate, 0.0

    class_energy_target = energy_revenue(inputs.class_totals, fixed_charge)
    if winter_rate < 0.0 <= summer_rate:
        clipped_winter_rate = 0.0
        clipped_summer_rate = class_energy_target / inputs.class_totals.summer_kwh
    elif summer_rate < 0.0 <= winter_rate:
        clipped_summer_rate = 0.0
        clipped_winter_rate = class_energy_target / inputs.class_totals.winter_kwh
    else:
        clipped_winter_rate = 0.0
        clipped_summer_rate = 0.0

    clipped_subclass_bill = seasonal_bill(
        inputs.subclass_totals,
        fixed_charge,
        clipped_winter_rate,
        clipped_summer_rate,
    )
    return (
        clipped_winter_rate,
        clipped_summer_rate,
        clipped_subclass_bill - inputs.subclass_fair_bill,
    )


def seasonal_rates_only_rate_design(inputs: FairDefaultInputs) -> FairDefaultRateDesign:
    """Solve the default tariff by preserving the calibrated fixed charge."""
    fixed_charge = inputs.base_fixed_charge
    winter_rate, summer_rate, denominator = _seasonal_rates_at_fixed_charge(
        inputs,
        fixed_charge,
    )
    clipped_winter_rate, clipped_summer_rate, residual = _clip_seasonal_design(
        inputs,
        fixed_charge,
        winter_rate,
        summer_rate,
    )
    feasible = (
        fixed_charge >= inputs.fixed_charge_floor
        and winter_rate >= 0.0
        and summer_rate >= 0.0
    )
    return FairDefaultRateDesign(
        module="seasonal_rates_only",
        fixed_charge=fixed_charge,
        flat_rate=None,
        winter_rate=winter_rate,
        summer_rate=summer_rate,
        discriminant=denominator,
        feasible=feasible,
        clipped_winter_rate=clipped_winter_rate,
        clipped_summer_rate=clipped_summer_rate,
        residual_cross_subsidy=residual,
    )


def fixed_plus_seasonal_mc_rate_design(mc_seasonal_ratio: float) -> RateDesignModule:
    """Return a design module constrained by winter_rate / summer_rate."""
    if mc_seasonal_ratio <= 0.0:
        raise ValueError(
            f"mc_seasonal_ratio must be positive; got {mc_seasonal_ratio}."
        )

    def solve(inputs: FairDefaultInputs) -> FairDefaultRateDesign:
        class_totals = inputs.class_totals
        subclass_totals = inputs.subclass_totals
        class_ratio_kwh = (
            mc_seasonal_ratio * class_totals.winter_kwh + class_totals.summer_kwh
        )
        subclass_ratio_kwh = (
            mc_seasonal_ratio * subclass_totals.winter_kwh + subclass_totals.summer_kwh
        )
        denominator = (
            class_totals.customer_count * subclass_ratio_kwh
            - subclass_totals.customer_count * class_ratio_kwh
        )
        _require_nonzero(denominator, "combined rate design")
        fixed_charge = (
            subclass_ratio_kwh * class_totals.current_bill
            - class_ratio_kwh * inputs.subclass_fair_bill
        ) / (MONTHS_PER_YEAR * denominator)
        summer_rate = (
            class_totals.customer_count * inputs.subclass_fair_bill
            - subclass_totals.customer_count * class_totals.current_bill
        ) / denominator
        winter_rate = mc_seasonal_ratio * summer_rate
        feasible = (
            fixed_charge >= inputs.fixed_charge_floor
            and winter_rate >= 0.0
            and summer_rate >= 0.0
        )
        return FairDefaultRateDesign(
            module="fixed_plus_seasonal_mc",
            fixed_charge=fixed_charge,
            flat_rate=None,
            winter_rate=winter_rate,
            summer_rate=summer_rate,
            discriminant=MONTHS_PER_YEAR * denominator,
            feasible=feasible,
            clipped_winter_rate=winter_rate,
            clipped_summer_rate=summer_rate,
        )

    return solve


def fair_default_rate_design_modules(
    mc_seasonal_ratio: float | None,
) -> dict[str, RateDesignModule]:
    """Return enabled fair-default modules keyed by tariff strategy name."""
    modules: dict[str, RateDesignModule] = {
        "fixed_charge_only": fixed_charge_only_rate_design,
        "seasonal_rates_only": seasonal_rates_only_rate_design,
    }
    if mc_seasonal_ratio is not None:
        modules["fixed_plus_seasonal_mc"] = fixed_plus_seasonal_mc_rate_design(
            mc_seasonal_ratio
        )
    return modules


def derive_fair_default_rate_designs(
    inputs: FairDefaultInputs,
    modules: dict[str, RateDesignModule],
) -> dict[str, FairDefaultRateDesign]:
    """Run each configured rate design module against the shared inputs."""
    return {name: module(inputs) for name, module in modules.items()}


@dataclass(frozen=True, slots=True)
class FixedChargeFeasibility:
    minimum: float
    maximum: float
    exists: bool
    winter_rate_at_zero_fixed_charge: float
    winter_rate_per_fixed_charge_dollar: float
    summer_rate_at_zero_fixed_charge: float
    summer_rate_per_fixed_charge_dollar: float


def _nonnegative_interval(intercept: float, slope: float) -> tuple[float, float, bool]:
    if math.isclose(slope, 0.0, abs_tol=ZERO_TOLERANCE):
        return -math.inf, math.inf, intercept >= 0.0
    bound = -intercept / slope
    if slope > 0.0:
        return bound, math.inf, True
    return -math.inf, bound, True


def fixed_charge_feasibility(inputs: FairDefaultInputs) -> FixedChargeFeasibility:
    denominator = _seasonal_denominator(inputs)
    class_totals = inputs.class_totals
    subclass_totals = inputs.subclass_totals
    winter_intercept = (
        class_totals.current_bill * subclass_totals.summer_kwh
        - inputs.subclass_fair_bill * class_totals.summer_kwh
    ) / denominator
    winter_slope = (
        MONTHS_PER_YEAR
        * (
            subclass_totals.customer_count * class_totals.summer_kwh
            - class_totals.customer_count * subclass_totals.summer_kwh
        )
        / denominator
    )
    summer_intercept = (
        inputs.subclass_fair_bill * class_totals.winter_kwh
        - class_totals.current_bill * subclass_totals.winter_kwh
    ) / denominator
    summer_slope = (
        MONTHS_PER_YEAR
        * (
            class_totals.customer_count * subclass_totals.winter_kwh
            - subclass_totals.customer_count * class_totals.winter_kwh
        )
        / denominator
    )

    lower = inputs.fixed_charge_floor
    upper = math.inf
    for intercept, slope in (
        (winter_intercept, winter_slope),
        (summer_intercept, summer_slope),
    ):
        rate_lower, rate_upper, possible = _nonnegative_interval(intercept, slope)
        if not possible:
            return FixedChargeFeasibility(
                minimum=math.nan,
                maximum=math.nan,
                exists=False,
                winter_rate_at_zero_fixed_charge=winter_intercept,
                winter_rate_per_fixed_charge_dollar=winter_slope,
                summer_rate_at_zero_fixed_charge=summer_intercept,
                summer_rate_per_fixed_charge_dollar=summer_slope,
            )
        lower = max(lower, rate_lower)
        upper = min(upper, rate_upper)

    return FixedChargeFeasibility(
        minimum=lower,
        maximum=upper,
        exists=lower <= upper,
        winter_rate_at_zero_fixed_charge=winter_intercept,
        winter_rate_per_fixed_charge_dollar=winter_slope,
        summer_rate_at_zero_fixed_charge=summer_intercept,
        summer_rate_per_fixed_charge_dollar=summer_slope,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Feasible-line data structures (used by plot_fair_default_feasible_line)
# ──────────────────────────────────────────────────────────────────────────────

DEFAULT_CROSS_SUBSIDY_COL = "BAT_percustomer"
DEFAULT_SUBCLASS_VALUE = "true"

_INF = math.inf


@dataclass(frozen=True, slots=True)
class AffineLine:
    """Affine parametrisation r(F) = intercept + slope * F for one rate."""

    intercept: float
    slope: float

    def at(self, f: float) -> float:
        return self.intercept + self.slope * f

    def zero_crossing(self) -> float:
        """F value where this rate hits zero; +/-inf if slope is zero."""
        if math.isclose(self.slope, 0.0, abs_tol=1e-12):
            return _INF if self.intercept >= 0.0 else -_INF
        return -self.intercept / self.slope


@dataclass(frozen=True, slots=True)
class StrategyPoint:
    """One named strategy's (F, r_win, r_sum) point."""

    label: str
    fixed_charge: float
    winter_rate: float | None
    summer_rate: float | None
    feasible: bool
    variant: str = ""


@dataclass(frozen=True, slots=True)
class FeasibleLineData:
    """Everything needed to render one feasible-line plot."""

    title: str
    r_win: AffineLine
    r_sum: AffineLine
    feasible_min: float
    feasible_max: float
    feasible_exists: bool
    strategies: list[StrategyPoint]
    base_fixed_charge: float
    fixed_charge_floor: float
    mc_seasonal_ratio: float | None


# ──────────────────────────────────────────────────────────────────────────────
# Feasible-line compute path (reads load_curve_monthly, not hourly loads)
# ──────────────────────────────────────────────────────────────────────────────


def _load_kwh_totals_monthly(
    *,
    resstock_base: str,
    state: str,
    upgrade: str,
    metadata: pl.DataFrame,
    winter_months: tuple[int, ...],
    storage_options: dict[str, str] | None,
) -> tuple[tuple[float, float], tuple[float, float]]:
    """Scan load_curve_monthly and return class and subclass kWh totals.

    Mirrors :func:`_load_kwh_totals` but reads monthly parquets (~12 rows/building)
    instead of 8760 hourly rows, making it cheap to run after every batch.

    Returns ``((class_annual, class_winter), (subclass_annual, subclass_winter))``.
    """
    building_ids = cast(list[int], metadata[BLDG_ID_COL].cast(pl.Int64).to_list())
    t0 = perf_counter()
    lf = scan_resstock_loads_monthly(
        resstock_base,
        state,
        upgrade,
        building_ids=building_ids,
        storage_options=storage_options,
    )
    LOGGER.info(
        "fair_default_inputs: prepared monthly loads scan for %d buildings in %.2fs",
        len(building_ids),
        perf_counter() - t0,
    )

    weights = metadata.select(
        pl.col(BLDG_ID_COL).cast(pl.Int64),
        pl.col(WEIGHT_COL).cast(pl.Float64),
        pl.col("_is_subclass"),
    )

    schema_names = lf.collect_schema().names()
    pv_col = ELECTRIC_PV_COL if ELECTRIC_PV_COL in schema_names else None

    if pv_col is not None:
        kwh_expr = grid_consumption_expr(ELECTRIC_LOAD_COL, pv_col)
    else:
        kwh_expr = pl.col(ELECTRIC_LOAD_COL).cast(pl.Float64).clip(lower_bound=0.0)

    t1 = perf_counter()
    kwh = cast(
        pl.DataFrame,
        lf.join(weights.lazy(), on=BLDG_ID_COL, how="inner")
        .select(
            pl.col("month").cast(pl.Int8),
            (kwh_expr * pl.col(WEIGHT_COL)).alias("weighted_kwh"),
            pl.col("_is_subclass"),
        )
        .with_columns(pl.col("month").is_in(list(winter_months)).alias("is_winter"))
        .select(
            pl.col("weighted_kwh").sum().alias("class_annual_kwh"),
            pl.when(pl.col("is_winter"))
            .then(pl.col("weighted_kwh"))
            .otherwise(0.0)
            .sum()
            .alias("class_winter_kwh"),
            pl.when(pl.col("_is_subclass"))
            .then(pl.col("weighted_kwh"))
            .otherwise(0.0)
            .sum()
            .alias("subclass_annual_kwh"),
            pl.when(pl.col("_is_subclass") & pl.col("is_winter"))
            .then(pl.col("weighted_kwh"))
            .otherwise(0.0)
            .sum()
            .alias("subclass_winter_kwh"),
        )
        .collect(engine="streaming"),
    )
    LOGGER.info(
        "fair_default_inputs: collected monthly kWh totals in %.2fs",
        perf_counter() - t1,
    )
    return (
        (
            float(kwh["class_annual_kwh"][0] or 0.0),
            float(kwh["class_winter_kwh"][0] or 0.0),
        ),
        (
            float(kwh["subclass_annual_kwh"][0] or 0.0),
            float(kwh["subclass_winter_kwh"][0] or 0.0),
        ),
    )


def _build_feasible_line_data(
    *,
    inputs: FairDefaultInputs,
    mc_seasonal_ratio: float | None,
    title: str,
    fixed_charge_floor: float,
) -> FeasibleLineData:
    """Build FeasibleLineData from pre-assembled FairDefaultInputs."""
    feasibility = fixed_charge_feasibility(inputs)
    designs = derive_fair_default_rate_designs(
        inputs,
        fair_default_rate_design_modules(mc_seasonal_ratio),
    )

    r_win = AffineLine(
        intercept=feasibility.winter_rate_at_zero_fixed_charge,
        slope=feasibility.winter_rate_per_fixed_charge_dollar,
    )
    r_sum = AffineLine(
        intercept=feasibility.summer_rate_at_zero_fixed_charge,
        slope=feasibility.summer_rate_per_fixed_charge_dollar,
    )

    strategies: list[StrategyPoint] = []
    if "fixed_charge_only" in designs:
        d = designs["fixed_charge_only"]
        strategies.append(
            StrategyPoint(
                label="A",
                fixed_charge=d.fixed_charge,
                winter_rate=r_win.at(d.fixed_charge),
                summer_rate=r_sum.at(d.fixed_charge),
                feasible=d.feasible,
            )
        )
    if "seasonal_rates_only" in designs:
        d = designs["seasonal_rates_only"]
        strategies.append(
            StrategyPoint(
                label="B",
                fixed_charge=d.fixed_charge,
                winter_rate=d.winter_rate,
                summer_rate=d.summer_rate,
                feasible=d.feasible,
            )
        )
    if "fixed_plus_seasonal_mc" in designs:
        d = designs["fixed_plus_seasonal_mc"]
        strategies.append(
            StrategyPoint(
                label="C",
                fixed_charge=d.fixed_charge,
                winter_rate=d.winter_rate,
                summer_rate=d.summer_rate,
                feasible=d.feasible,
            )
        )

    return FeasibleLineData(
        title=title,
        r_win=r_win,
        r_sum=r_sum,
        feasible_min=feasibility.minimum,
        feasible_max=feasibility.maximum,
        feasible_exists=feasibility.exists,
        strategies=strategies,
        base_fixed_charge=inputs.base_fixed_charge,
        fixed_charge_floor=fixed_charge_floor,
        mc_seasonal_ratio=mc_seasonal_ratio,
    )


def compute_feasible_line_from_runs(
    *,
    run_dir_delivery: str | Path | S3Path,
    run_dir_supply: str | Path | S3Path,
    resstock_base: str,
    state: str,
    upgrade: str,
    path_base_tariff_delivery: str | Path,
    path_base_tariff_supply: str | Path,
    group_col: str = DEFAULT_GROUP_COL,
    subclass_value: str = DEFAULT_SUBCLASS_VALUE,
    group_value_to_subclass: dict[str, str] | None = None,
    cross_subsidy_col: str = DEFAULT_CROSS_SUBSIDY_COL,
    path_periods_yaml: Path | None = None,
    mc_seasonal_ratio_delivery: float | None = None,
    mc_seasonal_ratio_supply: float | None = None,
    fixed_charge_floor: float = 0.0,
    title_delivery: str | None = None,
    title_supply: str | None = None,
) -> dict[str, FeasibleLineData]:
    """Build ``{'delivery': ..., 'supply': ...}`` feasible-line data from run dirs.

    Uses ``load_curve_monthly`` (~12 rows/building) instead of hourly loads so
    this is cheap to run after every batch.  When both
    ``mc_seasonal_ratio_delivery`` and ``mc_seasonal_ratio_supply`` are ``None``,
    strategy C is omitted from both results.

    Args:
        run_dir_delivery: CAIRO output directory for the delivery (run-1) scenario.
        run_dir_supply: CAIRO output directory for the delivery+supply (run-2) scenario.
        resstock_base: Base path to the ResStock release (local or S3).
        state: State abbreviation (e.g. "NY").
        upgrade: ResStock upgrade partition (e.g. "00").
        path_base_tariff_delivery: Calibrated delivery base tariff JSON.
        path_base_tariff_supply: Calibrated supply base tariff JSON.
        group_col: Metadata column that identifies the subclass (default: "has_hp").
        subclass_value: Value of ``group_col`` for the subclass (default: "true").
        group_value_to_subclass: Optional mapping for multi-value groups.
        cross_subsidy_col: BAT column for cross-subsidy (default: "BAT_percustomer").
        path_periods_yaml: Optional periods YAML for winter-month configuration.
        mc_seasonal_ratio_delivery: Delivery rho_MC; ``None`` skips strategy C.
        mc_seasonal_ratio_supply: Supply rho_MC; ``None`` skips strategy C.
        fixed_charge_floor: Minimum allowed fixed charge (default: 0.0).
        title_delivery: Override title for the delivery FeasibleLineData.
        title_supply: Override title for the supply FeasibleLineData.

    Returns:
        ``{'delivery': FeasibleLineData, 'supply': FeasibleLineData}``
    """
    group_value_to_subclass_map = (
        parse_group_value_to_subclass(group_value_to_subclass)
        if isinstance(group_value_to_subclass, str)
        else group_value_to_subclass
    )

    run_dir_del = _resolve_path_or_s3(str(run_dir_delivery))
    run_dir_sup = _resolve_path_or_s3(str(run_dir_supply))

    storage_options = (
        get_aws_storage_options()
        if isinstance(run_dir_del, S3Path)
        or str(run_dir_delivery).startswith("s3://")
        or resstock_base.startswith("s3://")
        else None
    )

    winter_months = _resolve_winter_months(path_periods_yaml)

    result: dict[str, FeasibleLineData] = {}

    for variant, run_dir, base_tariff_path, mc_ratio, title_override in (
        (
            "delivery",
            run_dir_del,
            Path(path_base_tariff_delivery),
            mc_seasonal_ratio_delivery,
            title_delivery,
        ),
        (
            "supply",
            run_dir_sup,
            Path(path_base_tariff_supply),
            mc_seasonal_ratio_supply,
            title_supply,
        ),
    ):
        LOGGER.info("compute_feasible_line_from_runs: loading %s run dir ...", variant)

        metadata, subclass_cross_subsidy = _load_metadata_and_cross_subsidy(
            run_dir=run_dir,
            group_col=group_col,
            subclass_value=subclass_value,
            cross_subsidy_col=cross_subsidy_col,
            storage_options=storage_options,
            group_value_to_subclass=group_value_to_subclass_map,
        )

        class_bill, subclass_bill = _load_bill_totals(
            run_dir, metadata, storage_options
        )

        (
            (class_annual_kwh, class_winter_kwh),
            (subclass_annual_kwh, subclass_winter_kwh),
        ) = _load_kwh_totals_monthly(
            resstock_base=resstock_base,
            state=state,
            upgrade=upgrade,
            metadata=metadata,
            winter_months=winter_months,
            storage_options=storage_options,
        )

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

        inputs = FairDefaultInputs(
            class_totals=class_totals,
            subclass_totals=subclass_totals,
            subclass_cross_subsidy=subclass_cross_subsidy,
            base_fixed_charge=_extract_fixed_charge_from_urdb(base_tariff_path),
            fixed_charge_floor=fixed_charge_floor,
        )

        default_title = f"fair-default feasible (C1∧C2) line — {group_col}={subclass_value}, {variant}"
        title = title_override if title_override else default_title

        result[variant] = _build_feasible_line_data(
            inputs=inputs,
            mc_seasonal_ratio=mc_ratio,
            title=title,
            fixed_charge_floor=fixed_charge_floor,
        )
        LOGGER.info("compute_feasible_line_from_runs: %s done", variant)

    return result


def _resolve_winter_months(path_periods_yaml: Path | None) -> tuple[int, ...]:
    if path_periods_yaml is None:
        return tuple(DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS)
    return tuple(
        load_winter_months_from_periods(
            path_periods_yaml,
            default_winter_months=DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
        )
    )


def _load_metadata_and_cross_subsidy(
    run_dir: S3Path | Path,
    group_col: str,
    subclass_value: str,
    cross_subsidy_col: str,
    storage_options: dict[str, str] | None,
    group_value_to_subclass: dict[str, str] | None,
) -> tuple[pl.DataFrame, float]:
    subclass_rows, subclass_cross_subsidy = _load_subclass_cross_subsidy_inputs(
        run_dir=run_dir,
        group_col=group_col,
        subclass_value=subclass_value,
        cross_subsidy_col=cross_subsidy_col,
        storage_options=storage_options,
        group_value_to_subclass=group_value_to_subclass,
        log_prefix="fair_default_inputs",
    )
    subclass_ids = set(subclass_rows[BLDG_ID_COL].to_list())
    raw_subclass_values = _resolve_selector_group_values(
        subclass_value,
        group_value_to_subclass,
    )
    metadata = cast(
        pl.DataFrame,
        _load_group_values(run_dir, group_col, storage_options)
        .with_columns(
            pl.col(GROUP_VALUE_COL).is_in(raw_subclass_values).alias("_is_subclass")
        )
        .collect(),
    )
    if metadata.is_empty():
        raise ValueError("No customers found in customer_metadata.csv.")
    if (
        set(metadata.filter(pl.col("_is_subclass"))[BLDG_ID_COL].to_list())
        != subclass_ids
    ):
        raise ValueError("Subclass metadata and BAT inputs do not match.")
    return metadata, subclass_cross_subsidy


def _load_bill_totals(
    run_dir: S3Path | Path,
    metadata: pl.DataFrame,
    storage_options: dict[str, str] | None,
) -> tuple[float, float]:
    bills = cast(
        pl.DataFrame,
        _load_annual_target_bills(run_dir, ANNUAL_MONTH_VALUE, storage_options)
        .join(
            metadata.select(BLDG_ID_COL, WEIGHT_COL, "_is_subclass").lazy(),
            on=BLDG_ID_COL,
            how="right",
        )
        .collect(),
    )
    missing_bills = bills.filter(pl.col("annual_bill").is_null()).height
    if missing_bills:
        raise ValueError(
            f"Missing annual target bills for {missing_bills} buildings "
            f"(month={ANNUAL_MONTH_VALUE})."
        )
    weighted_bill = pl.col("annual_bill") * pl.col(WEIGHT_COL)
    totals = bills.select(
        weighted_bill.sum().alias("class_current_bill"),
        pl.when(pl.col("_is_subclass"))
        .then(weighted_bill)
        .otherwise(0.0)
        .sum()
        .alias("subclass_current_bill"),
    )
    return (
        float(totals["class_current_bill"][0] or 0.0),
        float(totals["subclass_current_bill"][0] or 0.0),
    )


def _load_kwh_totals(
    *,
    resstock_base: str,
    state: str,
    upgrade: str,
    metadata: pl.DataFrame,
    winter_months: tuple[int, ...],
    storage_options: dict[str, str] | None,
) -> tuple[tuple[float, float], tuple[float, float]]:
    building_ids = metadata[BLDG_ID_COL].to_list()
    t0 = perf_counter()
    loads = scan_resstock_loads(
        resstock_base,
        state,
        upgrade,
        building_ids=building_ids,
        storage_options=storage_options,
    )
    LOGGER.info(
        "fair_default_inputs: prepared loads scan for %d buildings in %.2fs",
        len(building_ids),
        perf_counter() - t0,
    )

    weights = metadata.select(BLDG_ID_COL, WEIGHT_COL, "_is_subclass")
    t1 = perf_counter()
    kwh = cast(
        pl.DataFrame,
        loads.join(weights.lazy(), on=BLDG_ID_COL, how="inner")
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
            pl.col("_is_subclass"),
        )
        .with_columns(pl.col("month_num").is_in(winter_months).alias("is_winter"))
        .select(
            pl.col("weighted_kwh").sum().alias("class_annual_kwh"),
            pl.when(pl.col("is_winter"))
            .then(pl.col("weighted_kwh"))
            .otherwise(0.0)
            .sum()
            .alias("class_winter_kwh"),
            pl.when(pl.col("_is_subclass"))
            .then(pl.col("weighted_kwh"))
            .otherwise(0.0)
            .sum()
            .alias("subclass_annual_kwh"),
            pl.when(pl.col("_is_subclass") & pl.col("is_winter"))
            .then(pl.col("weighted_kwh"))
            .otherwise(0.0)
            .sum()
            .alias("subclass_winter_kwh"),
        )
        .collect(engine="streaming"),
    )
    LOGGER.info(
        "fair_default_inputs: collected class and subclass load totals in %.2fs",
        perf_counter() - t1,
    )
    return (
        (
            float(kwh["class_annual_kwh"][0] or 0.0),
            float(kwh["class_winter_kwh"][0] or 0.0),
        ),
        (
            float(kwh["subclass_annual_kwh"][0] or 0.0),
            float(kwh["subclass_winter_kwh"][0] or 0.0),
        ),
    )


def load_fair_default_inputs(
    *,
    run_dir: S3Path | Path,
    resstock_base: str,
    state: str,
    upgrade: str,
    group_col: str,
    subclass_value: str,
    cross_subsidy_col: str,
    storage_options: dict[str, str] | None,
    group_value_to_subclass: dict[str, str] | None,
    base_tariff_json_path: S3Path | Path,
    periods_yaml_path: Path | None,
    fixed_charge_floor: float,
) -> tuple[FairDefaultInputs, tuple[int, ...]]:
    """Load run outputs and ResStock loads into shared rate design inputs."""
    winter_months = _resolve_winter_months(periods_yaml_path)
    metadata, subclass_cross_subsidy = _load_metadata_and_cross_subsidy(
        run_dir,
        group_col,
        subclass_value,
        cross_subsidy_col,
        storage_options,
        group_value_to_subclass,
    )
    class_bill, subclass_bill = _load_bill_totals(
        run_dir,
        metadata,
        storage_options,
    )
    (class_annual_kwh, class_winter_kwh), (subclass_annual_kwh, subclass_winter_kwh) = (
        _load_kwh_totals(
            resstock_base=resstock_base,
            state=state,
            upgrade=upgrade,
            metadata=metadata,
            winter_months=winter_months,
            storage_options=storage_options,
        )
    )

    class_customers = float(metadata[WEIGHT_COL].sum() or 0.0)
    subclass_customers = float(
        metadata.filter(pl.col("_is_subclass"))[WEIGHT_COL].sum() or 0.0
    )
    class_totals = CustomerGroupTotals(
        customer_count=class_customers,
        current_bill=class_bill,
        annual_kwh=class_annual_kwh,
        winter_kwh=class_winter_kwh,
        summer_kwh=class_annual_kwh - class_winter_kwh,
    )
    subclass_totals = CustomerGroupTotals(
        customer_count=subclass_customers,
        current_bill=subclass_bill,
        annual_kwh=subclass_annual_kwh,
        winter_kwh=subclass_winter_kwh,
        summer_kwh=subclass_annual_kwh - subclass_winter_kwh,
    )
    class_totals.validate("class")
    subclass_totals.validate("subclass")

    inputs = FairDefaultInputs(
        class_totals=class_totals,
        subclass_totals=subclass_totals,
        subclass_cross_subsidy=subclass_cross_subsidy,
        base_fixed_charge=_extract_fixed_charge_from_urdb(base_tariff_json_path),
        fixed_charge_floor=fixed_charge_floor,
    )
    return inputs, winter_months


def _design_columns(prefix: str, design: FairDefaultRateDesign) -> dict[str, object]:
    columns: dict[str, object] = {
        f"{prefix}_discriminant": design.discriminant,
        f"{prefix}_fixed_charge": design.fixed_charge,
        f"{prefix}_feasible": design.feasible,
    }
    if design.flat_rate is not None:
        columns[f"{prefix}_flat_rate"] = design.flat_rate
    if design.winter_rate is not None:
        columns[f"{prefix}_winter_rate"] = design.winter_rate
    if design.summer_rate is not None:
        columns[f"{prefix}_summer_rate"] = design.summer_rate
    if design.clipped_winter_rate is not None:
        columns[f"{prefix}_clipped_winter_rate"] = design.clipped_winter_rate
    if design.clipped_summer_rate is not None:
        columns[f"{prefix}_clipped_summer_rate"] = design.clipped_summer_rate
    columns[f"{prefix}_residual_cross_subsidy_after_clipping"] = (
        design.residual_cross_subsidy
    )
    return columns


def fair_default_inputs_frame(
    *,
    inputs: FairDefaultInputs,
    designs: dict[str, FairDefaultRateDesign],
    feasibility: FixedChargeFeasibility,
    group_col: str,
    subclass_value: str,
    cross_subsidy_col: str,
    state: str,
    upgrade: str,
    winter_months: tuple[int, ...],
    mc_seasonal_ratio: float | None,
) -> pl.DataFrame:
    class_totals = inputs.class_totals
    subclass_totals = inputs.subclass_totals
    row: dict[str, object] = {
        "subclass": subclass_value,
        "group_col": group_col,
        "cross_subsidy_col": cross_subsidy_col,
        "state": state,
        "upgrade": upgrade,
        "winter_months": ",".join(str(m) for m in winter_months),
        "base_fixed_charge": inputs.base_fixed_charge,
        "fixed_charge_floor": inputs.fixed_charge_floor,
        "mc_seasonal_ratio": mc_seasonal_ratio,
        "class_customer_count": class_totals.customer_count,
        "subclass_customer_count": subclass_totals.customer_count,
        "class_current_bill": class_totals.current_bill,
        "subclass_current_bill": subclass_totals.current_bill,
        "subclass_cross_subsidy": inputs.subclass_cross_subsidy,
        "subclass_fair_bill": inputs.subclass_fair_bill,
        "class_annual_kwh": class_totals.annual_kwh,
        "class_winter_kwh": class_totals.winter_kwh,
        "class_summer_kwh": class_totals.summer_kwh,
        "subclass_annual_kwh": subclass_totals.annual_kwh,
        "subclass_winter_kwh": subclass_totals.winter_kwh,
        "subclass_summer_kwh": subclass_totals.summer_kwh,
        "class_energy_revenue": inputs.class_energy_revenue,
        "subclass_energy_revenue": inputs.subclass_energy_revenue,
        "base_flat_rate": inputs.base_flat_rate,
        "feasible_fixed_charge_min": feasibility.minimum,
        "feasible_fixed_charge_max": feasibility.maximum,
        "feasible_fixed_charge_range_exists": feasibility.exists,
        "winter_rate_at_zero_fixed_charge": (
            feasibility.winter_rate_at_zero_fixed_charge
        ),
        "winter_rate_per_fixed_charge_dollar": (
            feasibility.winter_rate_per_fixed_charge_dollar
        ),
        "summer_rate_at_zero_fixed_charge": (
            feasibility.summer_rate_at_zero_fixed_charge
        ),
        "summer_rate_per_fixed_charge_dollar": (
            feasibility.summer_rate_per_fixed_charge_dollar
        ),
    }
    for name, design in designs.items():
        row.update(_design_columns(name, design))
    return pl.DataFrame(row)


def compute_fair_default_inputs(
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
    periods_yaml_path: Path | None = None,
    mc_seasonal_ratio: float | None = None,
    fixed_charge_floor: float = 0.0,
) -> pl.DataFrame:
    """Load shared inputs, run fair-default modules, and return one CSV row."""
    if base_tariff_json_path is None:
        raise ValueError(
            "base_tariff_json_path is required: pass a calibrated URDB tariff JSON."
        )

    inputs, winter_months = load_fair_default_inputs(
        run_dir=run_dir,
        resstock_base=resstock_base,
        state=state,
        upgrade=upgrade,
        group_col=group_col,
        subclass_value=subclass_value,
        cross_subsidy_col=cross_subsidy_col,
        storage_options=storage_options,
        group_value_to_subclass=group_value_to_subclass,
        base_tariff_json_path=base_tariff_json_path,
        periods_yaml_path=periods_yaml_path,
        fixed_charge_floor=fixed_charge_floor,
    )
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
        cross_subsidy_col=cross_subsidy_col,
        state=state,
        upgrade=upgrade,
        winter_months=winter_months,
        mc_seasonal_ratio=mc_seasonal_ratio,
    )


def _write_fair_default_inputs_csv(path: S3Path | Path, csv_text: str) -> str:
    if isinstance(path, S3Path):
        path.write_text(csv_text)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(csv_text, encoding="utf-8")
    return str(path)


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Compute fair-default rate designs from CAIRO outputs and loads."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--resstock-base", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--upgrade", required=True)
    parser.add_argument("--group-col", default=DEFAULT_GROUP_COL)
    parser.add_argument("--subclass-value", default="true")
    parser.add_argument("--group-value-to-subclass")
    parser.add_argument(
        "--cross-subsidy-col",
        default=DEFAULT_BAT_METRIC,
        choices=BAT_METRIC_CHOICES,
    )
    parser.add_argument("--base-tariff-json", required=True)
    parser.add_argument("--periods-yaml", type=Path)
    parser.add_argument("--mc-seasonal-ratio", type=float)
    parser.add_argument("--fixed-charge-floor", type=float, default=0.0)
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    run_dir = _resolve_path_or_s3(args.run_dir)
    output_dir = _resolve_path_or_s3(args.output_dir) if args.output_dir else run_dir
    base_tariff_json_path = _resolve_path_or_s3(args.base_tariff_json)
    storage_options = (
        get_aws_storage_options()
        if isinstance(run_dir, S3Path) or args.resstock_base.startswith("s3://")
        else None
    )
    group_value_to_subclass = (
        parse_group_value_to_subclass(args.group_value_to_subclass)
        if args.group_value_to_subclass
        else None
    )
    fair_default_inputs = compute_fair_default_inputs(
        run_dir=run_dir,
        resstock_base=args.resstock_base,
        state=args.state,
        upgrade=args.upgrade,
        group_col=args.group_col,
        subclass_value=args.subclass_value,
        cross_subsidy_col=args.cross_subsidy_col,
        storage_options=storage_options,
        group_value_to_subclass=group_value_to_subclass,
        base_tariff_json_path=base_tariff_json_path,
        periods_yaml_path=args.periods_yaml,
        mc_seasonal_ratio=args.mc_seasonal_ratio,
        fixed_charge_floor=args.fixed_charge_floor,
    )
    print(fair_default_inputs)

    output_path = output_dir / DEFAULT_OUTPUT_FILENAME
    csv_text = fair_default_inputs.write_csv(None)
    if not isinstance(csv_text, str):
        raise ValueError("Failed to render fair default input CSV text.")
    written_path = _write_fair_default_inputs_csv(output_path, csv_text)
    print(f"Wrote fair default inputs CSV: {written_path}")


if __name__ == "__main__":
    main()
