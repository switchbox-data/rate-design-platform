"""Advisory end-of-run checks of a ResStock release against real-world data.

Compares the baseline stock (upgrade 00) of a built release to:

- EIA-861 residential customers and sales, statewide and by electric utility.
- EIA-176 residential gas deliveries (sales + transport), statewide and by gas
  utility, plus an estimated gas *meter* count against EIA-176 consumers.
- ACS PUMS occupied housing units: heating fuel, single-family vs multifamily,
  year built, and the share of gas households whose gas is included in rent
  (master metering).

It also sanity-checks the heat-pump approximation on the approximated upgrades.

Every check only warns. Each warning names the section of the reports2 notebook
``reports/templates/data_prep/investigate_resstock.qmd`` that shows the full
comparison, so the user can see what's driving the gap.

Usage::

    uv run python -m data.resstock.validations.real_world_checks --state CT
    uv run python -m data.resstock.validations.real_world_checks --state CT --location s3
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import polars as pl
import yaml

from data.resstock.constants import CONFIG_PATH
from data.resstock.load_curve.adjust_gas_usage import (
    EIA176_BY_CONSUMER,
    EIA176_DISPOSITION,
    KWH_PER_MMBTU,
)
from data.resstock.utils import load_state_configs
from utils import get_aws_region
from utils.post.compare_resstock_eia176_gas import (
    GAS_OPERATORS_BY_STATE,
    eia_year_for_resstock_release,
)
from utils.utility_codes import get_eia_utility_id_to_std_name

BLDG_ID = "bldg_id"
WEIGHT = "weight"
ELEC_UTILITY = "sb.electric_utility"
GAS_UTILITY = "sb.gas_utility"
ANNUAL_ELEC = "out.electricity.total.energy_consumption.kwh"
ANNUAL_GAS = "out.natural_gas.total.energy_consumption.kwh"
MWH_TO_KWH = 1000

BASELINE_UPGRADE = "00"
STATEWIDE = "statewide"

# Gas end uses that sit inside the dwelling unless the building's heating or
# water heating is shared. Heating and hot water are handled separately.
_IN_UNIT_GAS_END_USES = (
    "range_oven",
    "clothes_dryer",
    "fireplace",
    "grill",
    "lighting",
    "pool_heater",
    "permanent_spa_heat",
)
_SHARED_HEATING_VALUES = ("Heating Only", "Heating and Cooling")

Status = Literal["ok", "warn", "skipped", "ignored"]


# ── Notebook pointers ─────────────────────────────────────────────────────────

NOTEBOOK_PATH = "reports2/reports/templates/data_prep/investigate_resstock.qmd"


@dataclass(frozen=True)
class NotebookSection:
    section: str
    subsection: str
    anchor: str

    def pointer(self) -> str:
        return (
            f'{NOTEBOOK_PATH} → "{self.section}" → "{self.subsection}" (#{self.anchor})'
        )


_SEC_ELEC = "Section 2: Electric customers and electricity use"
_SEC_GAS = "Section 3: Gas customers and gas use"
_SEC_HP = "Section 4: Heat-pump approximation"
_SEC_HEAT = "Section 5: Heating-system mix"
_SEC_STOCK = "Section 6: Other stock characteristics"

NOTEBOOK_SECTIONS: dict[str, NotebookSection] = {
    "electric_customers_statewide": NotebookSection(
        _SEC_ELEC, "Statewide electricity", "sec-electricity-statewide"
    ),
    "electric_customers_utility": NotebookSection(
        _SEC_ELEC,
        "Customer counts by electric utility",
        "sec-electric-customers-by-utility",
    ),
    "electric_kwh_statewide": NotebookSection(
        _SEC_ELEC, "Statewide electricity", "sec-electricity-statewide"
    ),
    "electric_kwh_sb_drift": NotebookSection(
        _SEC_ELEC, "Statewide electricity", "sec-electricity-statewide"
    ),
    "electric_kwh_utility": NotebookSection(
        _SEC_ELEC,
        "Electricity use by electric utility",
        "sec-electricity-by-utility",
    ),
    "gas_kwh_statewide": NotebookSection(
        _SEC_GAS, "Statewide gas", "sec-gas-statewide"
    ),
    "gas_kwh_utility": NotebookSection(
        _SEC_GAS, "Gas use by gas utility", "sec-gas-by-utility"
    ),
    "gas_meters": NotebookSection(
        _SEC_GAS, "Customer counts by gas utility", "sec-gas-customers-by-utility"
    ),
    "hp_approx_share": NotebookSection(
        _SEC_HP,
        "Approximated buildings by upgrade",
        "sec-hp-approximation-by-upgrade",
    ),
    "hp_approx_unclassified": NotebookSection(
        _SEC_HP,
        "Approximated buildings by upgrade",
        "sec-hp-approximation-by-upgrade",
    ),
    "acs_heating_fuel": NotebookSection(
        _SEC_HEAT, "Heating fuel: ResStock vs ACS and RECS", "sec-heating-fuel"
    ),
    "acs_building_type": NotebookSection(
        _SEC_STOCK, "Single-family vs multifamily", "sec-sf-mf"
    ),
    "acs_year_built": NotebookSection(_SEC_STOCK, "Year built", "sec-year-built"),
    "acs_master_metering": NotebookSection(
        _SEC_GAS, "Customer counts by gas utility", "sec-gas-customers-by-utility"
    ),
}


# ── Config ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RealWorldCheckConfig:
    """Tolerances for each check. Percentages are in percent, not fractions.

    Defaults mirror ``real_world_checks.tolerances`` in ``config.yaml``; per-state
    overrides live under ``real_world_checks`` in ``state_configs.yaml``.
    """

    electric_customers_statewide_pct: float = 10.0
    electric_customers_utility_pct: float = 20.0
    electric_kwh_statewide_pct: float = 10.0
    electric_kwh_utility_pct: float = 20.0
    electric_kwh_sb_drift_pp: float = 5.0
    gas_kwh_statewide_scaled_pct: float = 1.0
    gas_kwh_statewide_unscaled_pct: float = 15.0
    gas_kwh_utility_pct: float = 25.0
    gas_meters_band_pct: float = 10.0
    hp_approx_max_share_pct: float = 35.0
    acs_share_pp: float = 5.0
    acs_master_metering_pp: float = 5.0
    # Per-utility checks are skipped below this many EIA residential customers:
    # a utility that small has too few ResStock samples to compare.
    min_utility_customers: float = 5000.0
    # Entries are "<check>" or "<check>:<scope>", e.g. "electric_kwh_utility:utility=frp".
    ignore: tuple[str, ...] = field(default_factory=tuple)


def _merge_config(
    base: RealWorldCheckConfig, overrides: Mapping[str, Any] | None
) -> RealWorldCheckConfig:
    if not overrides:
        return base
    known = {f.name for f in fields(RealWorldCheckConfig)}
    unknown = sorted(set(overrides) - known)
    if unknown:
        raise ValueError(f"Unknown real_world_checks tolerance key(s): {unknown}")
    values = dict(overrides)
    if "ignore" in values:
        values["ignore"] = tuple(base.ignore) + tuple(values["ignore"] or ())
    return replace(base, **values)


def load_check_config(state: str) -> RealWorldCheckConfig:
    """Tolerances from ``config.yaml``, overridden by the state's ``state_configs.yaml`` block."""
    with CONFIG_PATH.open() as f:
        cfg = yaml.safe_load(f) or {}
    rw = cfg.get("real_world_checks") or {}
    config = _merge_config(RealWorldCheckConfig(), rw.get("tolerances"))
    config = _merge_config(config, {"ignore": rw.get("ignore") or []})
    state_rw = (load_state_configs().get(state) or {}).get("real_world_checks") or {}
    config = _merge_config(config, state_rw.get("tolerances"))
    return _merge_config(config, {"ignore": state_rw.get("ignore") or []})


# ── Results ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CheckResult:
    """One comparison. ``diff`` is in percent (``diff_unit="pct"``) or percentage points."""

    check: str
    label: str
    scope: str
    release: str
    unit: str
    resstock_value: float | None
    benchmark_name: str
    benchmark_value: float | None
    diff: float | None
    diff_unit: Literal["pct", "pp"]
    tolerance: float
    status: Status
    resstock_value_high: float | None = None
    note: str | None = None

    @property
    def notebook_section(self) -> NotebookSection:
        return NOTEBOOK_SECTIONS[self.check]

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["notebook_section"] = self.notebook_section.pointer()
        return out


def _fmt_value(value: float | None, unit: str) -> str:
    if value is None:
        return "n/a"
    if unit == "%":
        return f"{value:.1f}%"
    return f"{value:,.0f} {unit}"


def format_warning(state: str, result: CheckResult) -> str:
    if result.resstock_value_high is not None:
        resstock = (
            f"{_fmt_value(result.resstock_value, result.unit)} to "
            f"{_fmt_value(result.resstock_value_high, result.unit)}"
        )
    else:
        resstock = _fmt_value(result.resstock_value, result.unit)
    sign = "%" if result.diff_unit == "pct" else " pp"
    diff = "n/a" if result.diff is None else f"{result.diff:+.1f}{sign}"
    msg = (
        f"[real-world] {state} {result.label}, {result.scope} ({result.release}): "
        f"ResStock {resstock} vs {result.benchmark_name} "
        f"{_fmt_value(result.benchmark_value, result.unit)} "
        f"({diff}, tolerance ±{result.tolerance:g}{sign})."
    )
    if result.note:
        msg += f" {result.note}."
    return f"{msg} Investigate in {result.notebook_section.pointer()}."


def _pct_diff(resstock: float, benchmark: float) -> float:
    return (resstock - benchmark) / benchmark * 100


def _pct_result(
    *,
    check: str,
    label: str,
    scope: str,
    release: str,
    unit: str,
    resstock: float,
    benchmark_name: str,
    benchmark: float,
    tolerance: float,
    note: str | None = None,
) -> CheckResult:
    if benchmark == 0:
        return CheckResult(
            check=check,
            label=label,
            scope=scope,
            release=release,
            unit=unit,
            resstock_value=resstock,
            benchmark_name=benchmark_name,
            benchmark_value=benchmark,
            diff=None,
            diff_unit="pct",
            tolerance=tolerance,
            status="skipped",
            note=f"{benchmark_name} is zero",
        )
    diff = _pct_diff(resstock, benchmark)
    return CheckResult(
        check=check,
        label=label,
        scope=scope,
        release=release,
        unit=unit,
        resstock_value=resstock,
        benchmark_name=benchmark_name,
        benchmark_value=benchmark,
        diff=diff,
        diff_unit="pct",
        tolerance=tolerance,
        status="warn" if abs(diff) > tolerance else "ok",
        note=note,
    )


def _skipped(
    check: str, label: str, scope: str, release: str, note: str
) -> CheckResult:
    return CheckResult(
        check=check,
        label=label,
        scope=scope,
        release=release,
        unit="",
        resstock_value=None,
        benchmark_name="",
        benchmark_value=None,
        diff=None,
        diff_unit="pct",
        tolerance=0.0,
        status="skipped",
        note=note,
    )


# ── Inputs ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ReleaseInputs:
    """Upgrade-00 tables for one state, plus metadata for the approximated upgrades.

    ``annual_sb`` is None when the ``_sb`` annual load curve hasn't been built.
    ``approx_metadata`` maps upgrade id → (``_sb`` metadata, raw metadata).
    """

    state: str
    release_raw: str
    release_sb: str
    metadata_raw: pl.DataFrame
    metadata_sb: pl.DataFrame
    annual_raw: pl.DataFrame
    annual_sb: pl.DataFrame | None
    utility_assignment: pl.DataFrame | None
    approx_metadata: dict[str, tuple[pl.DataFrame, pl.DataFrame]] = field(
        default_factory=dict
    )


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _exists(path: str) -> bool:
    if _is_s3(path):
        try:
            pl.scan_parquet(
                path, storage_options={"aws_region": get_aws_region()}
            ).collect_schema()
        except Exception:
            return False
        return True
    return Path(path).exists()


def _read(path: str, columns: Sequence[str] | None = None) -> pl.DataFrame:
    opts = {"aws_region": get_aws_region()} if _is_s3(path) else None
    lf = pl.scan_parquet(path, storage_options=opts)
    if columns is not None:
        available = set(lf.collect_schema().names())
        lf = lf.select([c for c in columns if c in available])
    df = lf.collect()
    if not isinstance(df, pl.DataFrame):
        raise TypeError(f"Expected DataFrame from {path}")
    return df


_METADATA_COLS = (
    BLDG_ID,
    WEIGHT,
    "in.vacancy_status",
    "in.heating_fuel",
    "in.geometry_building_type_recs",
    "in.geometry_building_type_height",
    "in.geometry_building_number_units_mf",
    "in.hvac_has_shared_system",
    "in.water_heater_in_unit",
    "in.water_heater_fuel",
    "in.vintage",
    "postprocess_group.has_hp",
    "approximated_hp_load",
)
_ANNUAL_COLS = (
    BLDG_ID,
    ANNUAL_ELEC,
    ANNUAL_GAS,
    *(
        f"out.natural_gas.{e}.energy_consumption.kwh"
        for e in ("heating", "heating_hp_bkup", "hot_water", *_IN_UNIT_GAS_END_USES)
    ),
)


def _metadata_path(base: str, state: str, upgrade: str, filename: str) -> str:
    return f"{base}/metadata/state={state}/upgrade={upgrade}/{filename}"


def _annual_path(base: str, state: str) -> str:
    return (
        f"{base}/load_curve_annual/state={state}/upgrade={BASELINE_UPGRADE}/"
        f"{state}_upgrade{BASELINE_UPGRADE}_metadata_and_annual_results.parquet"
    )


def load_release_inputs(
    *,
    state: str,
    base_raw: str,
    base_sb: str,
    approx_upgrades: Sequence[str],
) -> ReleaseInputs:
    """Load the upgrade-00 inputs from a local directory or S3 prefix per release.

    ``base_raw`` / ``base_sb`` are release roots, e.g.
    ``/ebs/data/nrel/resstock/res_2024_amy2018_2`` or the matching ``s3://`` URI.
    """
    base_raw = base_raw.rstrip("/")
    base_sb = base_sb.rstrip("/")
    metadata_raw = _read(
        _metadata_path(base_raw, state, BASELINE_UPGRADE, "metadata.parquet"),
        _METADATA_COLS,
    )
    metadata_sb = _read(
        _metadata_path(base_sb, state, BASELINE_UPGRADE, "metadata-sb.parquet"),
        _METADATA_COLS,
    )
    annual_raw = _read(_annual_path(base_raw, state), _ANNUAL_COLS)
    path_annual_sb = _annual_path(base_sb, state)
    annual_sb = (
        _read(path_annual_sb, (BLDG_ID, ANNUAL_ELEC, ANNUAL_GAS))
        if _exists(path_annual_sb)
        else None
    )
    path_assignment = (
        f"{base_sb}/metadata_utility/state={state}/utility_assignment.parquet"
    )
    assignment = (
        _read(path_assignment, (BLDG_ID, ELEC_UTILITY, GAS_UTILITY))
        if _exists(path_assignment)
        else None
    )
    approx: dict[str, tuple[pl.DataFrame, pl.DataFrame]] = {}
    for upgrade in approx_upgrades:
        uid = upgrade.zfill(2)
        p_sb = _metadata_path(base_sb, state, uid, "metadata-sb.parquet")
        p_raw = _metadata_path(base_raw, state, uid, "metadata.parquet")
        if _exists(p_sb) and _exists(p_raw):
            approx[uid] = (_read(p_sb, _METADATA_COLS), _read(p_raw, _METADATA_COLS))
    return ReleaseInputs(
        state=state,
        release_raw=Path(base_raw).name,
        release_sb=Path(base_sb).name,
        metadata_raw=metadata_raw,
        metadata_sb=metadata_sb,
        annual_raw=annual_raw,
        annual_sb=annual_sb,
        utility_assignment=assignment,
        approx_metadata=approx,
    )


def load_eia861_residential(path_eia861: str, state: str) -> pl.DataFrame:
    """EIA-861 residential kWh and customers per platform utility.

    Rows are matched by ``utility_code`` when present, else by ``utility_id_eia``
    through ``utils.utility_codes``. Unmatched rows (competitive suppliers,
    adjustment rows) are dropped: adding them to distribution-utility totals
    double-counts sales in retail-choice states.
    """
    id_map = get_eia_utility_id_to_std_name(state)
    df = _read(
        path_eia861,
        (
            "utility_id_eia",
            "utility_code",
            "residential_sales_mwh",
            "residential_customers",
        ),
    )
    return (
        df.with_columns(
            pl.coalesce(
                pl.col("utility_code"),
                pl.col("utility_id_eia").replace_strict(
                    id_map, default=None, return_dtype=pl.String
                ),
            ).alias("utility_code")
        )
        .filter(pl.col("utility_code").is_not_null())
        .group_by("utility_code")
        .agg(
            (pl.col("residential_sales_mwh").cast(pl.Float64).sum() * MWH_TO_KWH).alias(
                "eia_kwh"
            ),
            pl.col("residential_customers")
            .cast(pl.Float64)
            .sum()
            .alias("eia_customers"),
        )
    )


def load_eia176_by_operator(
    state: str,
    year: int,
    path_by_consumer: str = EIA176_BY_CONSUMER,
    path_disposition: str = EIA176_DISPOSITION,
) -> pl.DataFrame:
    """EIA-176 residential sales + transport per operator: kWh and consumers (meters).

    An operator with deliveries but no delivered-gas heat content gets the state
    median, flagged in ``heat_content_imputed``.
    """
    deliveries = (
        pl.scan_parquet(path_by_consumer)
        .filter(
            (pl.col("operating_state") == state)
            & (pl.col("report_year") == year)
            & (pl.col("customer_class") == "residential")
            & pl.col("revenue_class").cast(pl.String).is_in(["sales", "transport"])
        )
        .group_by("operator_id_eia")
        .agg(
            pl.col("volume_mcf").cast(pl.Float64).sum().alias("volume_mcf"),
            pl.col("consumers").cast(pl.Float64).sum().alias("eia_customers"),
        )
    )
    heat = (
        pl.scan_parquet(path_disposition)
        .filter((pl.col("operating_state") == state) & (pl.col("report_year") == year))
        .select(
            "operator_id_eia",
            pl.col("delivered_gas_heat_content_mmbtu_per_mcf").cast(pl.Float64),
        )
    )
    joined = deliveries.join(heat, on="operator_id_eia", how="left").collect()
    if not isinstance(joined, pl.DataFrame) or joined.height == 0:
        raise ValueError(f"EIA-176 has no residential deliveries for {state} {year}.")
    heat_col = pl.col("delivered_gas_heat_content_mmbtu_per_mcf")
    if joined["delivered_gas_heat_content_mmbtu_per_mcf"].drop_nulls().len() == 0:
        raise ValueError(
            f"EIA-176 has no delivered-gas heat content for {state} {year}."
        )
    return joined.select(
        "operator_id_eia",
        (
            pl.col("volume_mcf") * heat_col.fill_null(heat_col.median()) * KWH_PER_MMBTU
        ).alias("eia_kwh"),
        "eia_customers",
        heat_col.is_null().alias("heat_content_imputed"),
    )


def load_acs_housing(path_pums_housing: str) -> pl.DataFrame:
    """Occupied, non-group-quarters ACS PUMS housing units with the columns we compare."""
    df = _read(path_pums_housing, ("typehugq", "hfl", "bld", "yrblt", "gasfp", "wgtp"))
    return df.filter((pl.col("typehugq") == "1") & pl.col("hfl").is_not_null())


# ── ResStock aggregations (pure) ──────────────────────────────────────────────


def _occupied(metadata: pl.DataFrame) -> pl.DataFrame:
    if "in.vacancy_status" not in metadata.columns:
        return metadata
    return metadata.filter(pl.col("in.vacancy_status") == "Occupied")


def weighted_by_utility(
    metadata: pl.DataFrame,
    annual: pl.DataFrame | None,
    assignment: pl.DataFrame,
    utility_col: str,
    kwh_col: str,
) -> pl.DataFrame:
    """Weighted ResStock dwellings and kWh per utility. ``resstock_kwh`` is null without annual."""
    df = metadata.select(BLDG_ID, WEIGHT).join(
        assignment.select(BLDG_ID, utility_col), on=BLDG_ID, how="inner"
    )
    if annual is not None:
        df = df.join(annual.select(BLDG_ID, kwh_col), on=BLDG_ID, how="left")
        kwh = (pl.col(kwh_col).fill_null(0) * pl.col(WEIGHT)).sum()
    else:
        kwh = pl.lit(None, dtype=pl.Float64)
    return (
        df.filter(pl.col(utility_col).is_not_null())
        .group_by(utility_col)
        .agg(
            pl.col(WEIGHT).sum().alias("resstock_customers"), kwh.alias("resstock_kwh")
        )
        .rename({utility_col: "utility_code"})
    )


def weighted_total_kwh(
    metadata: pl.DataFrame, annual: pl.DataFrame, kwh_col: str
) -> float:
    df = metadata.select(BLDG_ID, WEIGHT).join(
        annual.select(BLDG_ID, kwh_col), on=BLDG_ID, how="inner"
    )
    return float(
        df.select((pl.col(kwh_col).fill_null(0) * pl.col(WEIGHT)).sum()).item()
    )


def estimate_gas_meters(
    metadata: pl.DataFrame,
    annual: pl.DataFrame,
    assignment: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Estimate residential gas meters from ResStock dwellings, as a min/max band.

    EIA-176 counts meters; ResStock counts dwellings. A multifamily building whose
    heating or hot water comes from a shared gas system has a building (master)
    meter, which each of its dwellings holds ``1 / n_units`` of. Whether that
    building's in-unit gas (cooking, drying) is on the master meter too is not in
    ResStock, so:

    - ``meters_max`` / ``master_share_min``: in-unit gas use gets its own meter.
    - ``meters_min`` / ``master_share_max``: a shared-gas building is fully
      master-metered.

    Only dwellings with positive annual gas are counted. Returns one ``statewide``
    row, plus one row per ``sb.gas_utility`` when ``assignment`` is given.
    """
    end_use = {
        e: f"out.natural_gas.{e}.energy_consumption.kwh"
        for e in ("heating", "heating_hp_bkup", "hot_water", *_IN_UNIT_GAS_END_USES)
    }
    present = [c for c in end_use.values() if c in annual.columns]
    df = metadata.join(
        annual.select(BLDG_ID, ANNUAL_GAS, *present), on=BLDG_ID, how="inner"
    ).filter(pl.col(ANNUAL_GAS).fill_null(0) > 0)

    def gas(e: str) -> pl.Expr:
        col = end_use[e]
        return pl.col(col).fill_null(0) if col in present else pl.lit(0.0)

    is_mf = pl.col("in.geometry_building_type_recs").str.contains("Multi-Family")
    shared_heat = pl.col("in.hvac_has_shared_system").is_in(_SHARED_HEATING_VALUES) & (
        pl.col("in.heating_fuel") == "Natural Gas"
    )
    shared_hw = (pl.col("in.water_heater_in_unit") == "No") & (
        pl.col("in.water_heater_fuel") == "Natural Gas"
    )
    in_unit_gas = (
        pl.sum_horizontal(*(gas(e) for e in _IN_UNIT_GAS_END_USES))
        + pl.when(shared_heat)
        .then(0.0)
        .otherwise(gas("heating") + gas("heating_hp_bkup"))
        + pl.when(shared_hw).then(0.0).otherwise(gas("hot_water"))
    )
    n_units = (
        pl.col("in.geometry_building_number_units_mf")
        .cast(pl.Float64, strict=False)
        .fill_null(1.0)
        .clip(lower_bound=1.0)
    )
    df = df.with_columns(
        (is_mf & (shared_heat | shared_hw)).fill_null(False).alias("_shared"),
        in_unit_gas.alias("_in_unit_gas"),
        n_units.alias("_n_units"),
    ).with_columns(
        # Most meters: own meter unless a shared-gas unit has no in-unit gas.
        (
            pl.when(pl.col("_shared") & (pl.col("_in_unit_gas") <= 0))
            .then(0.0)
            .otherwise(1.0)
            + pl.when(pl.col("_shared")).then(1 / pl.col("_n_units")).otherwise(0.0)
        ).alias("_meters_max"),
        # Fewest meters: a shared-gas building is one meter.
        pl.when(pl.col("_shared"))
        .then(1 / pl.col("_n_units"))
        .otherwise(1.0)
        .alias("_meters_min"),
        (pl.col("_shared") & (pl.col("_in_unit_gas") <= 0)).alias("_master_min"),
    )
    w = pl.col(WEIGHT)
    aggs = [
        w.sum().alias("gas_dwellings"),
        (w * pl.col("_meters_min")).sum().alias("meters_min"),
        (w * pl.col("_meters_max")).sum().alias("meters_max"),
        ((w * pl.col("_master_min")).sum() / w.sum() * 100).alias("master_share_min"),
        ((w * pl.col("_shared")).sum() / w.sum() * 100).alias("master_share_max"),
    ]
    statewide = df.select(pl.lit(STATEWIDE).alias("utility_code"), *aggs)
    if assignment is None:
        return statewide
    by_utility = (
        df.join(assignment.select(BLDG_ID, GAS_UTILITY), on=BLDG_ID, how="inner")
        .filter(pl.col(GAS_UTILITY).is_not_null())
        .group_by(pl.col(GAS_UTILITY).alias("utility_code"))
        .agg(*aggs)
    )
    return pl.concat(
        [statewide, by_utility.sort("utility_code")], how="vertical_relaxed"
    )


# ── Category mappings shared by ResStock and ACS ──────────────────────────────


def heating_fuel_resstock(col: pl.Expr) -> pl.Expr:
    return col.replace_strict(
        {
            "Natural Gas": "Natural gas",
            "Electricity": "Electricity",
            "Fuel Oil": "Fuel oil",
            "Propane": "Propane",
        },
        default="Other / none",
        return_dtype=pl.String,
    )


def heating_fuel_acs(col: pl.Expr) -> pl.Expr:
    # HFL: 1 utility gas, 2 bottled/tank/LP, 3 electricity, 4 fuel oil.
    return col.replace_strict(
        {"1": "Natural gas", "2": "Propane", "3": "Electricity", "4": "Fuel oil"},
        default="Other / none",
        return_dtype=pl.String,
    )


def building_class_resstock(col: pl.Expr) -> pl.Expr:
    return (
        pl.when(col.str.starts_with("Single-Family"))
        .then(pl.lit("Single-family"))
        .when(col == "Mobile Home")
        .then(pl.lit("Mobile home"))
        .otherwise(pl.lit("Multifamily"))
    )


def building_class_acs(col: pl.Expr) -> pl.Expr:
    # BLD: 01 mobile home, 02/03 one-family detached/attached, 04-09 apartments,
    # 10 boat/RV/van (dropped).
    return (
        pl.when(col.is_in(["02", "03"]))
        .then(pl.lit("Single-family"))
        .when(col == "01")
        .then(pl.lit("Mobile home"))
        .when(col.is_in(["04", "05", "06", "07", "08", "09"]))
        .then(pl.lit("Multifamily"))
        .otherwise(pl.lit(None, dtype=pl.String))
    )


def vintage_resstock(col: pl.Expr) -> pl.Expr:
    return (
        pl.when(col.is_in(["<1940", "1940s"]))
        .then(pl.lit("Before 1950"))
        .when(col == "1950s")
        .then(pl.lit("1950-59"))
        .when(col.is_in(["1960s", "1970s"]))
        .then(pl.lit("1960-79"))
        .when(col.is_in(["1980s", "1990s"]))
        .then(pl.lit("1980-99"))
        .when(col == "2000s")
        .then(pl.lit("2000-09"))
        .otherwise(pl.lit("2010+"))
    )


def vintage_acs(col: pl.Expr) -> pl.Expr:
    # YRBLT is a year (1939 means 1939 or earlier).
    y = col.cast(pl.Int32, strict=False)
    return (
        pl.when(y.is_null())
        .then(pl.lit(None, dtype=pl.String))
        .when(y < 1950)
        .then(pl.lit("Before 1950"))
        .when(y < 1960)
        .then(pl.lit("1950-59"))
        .when(y < 1980)
        .then(pl.lit("1960-79"))
        .when(y < 2000)
        .then(pl.lit("1980-99"))
        .when(y < 2010)
        .then(pl.lit("2000-09"))
        .otherwise(pl.lit("2010+"))
    )


def weighted_shares(
    df: pl.DataFrame, category: pl.Expr, weight_col: str
) -> pl.DataFrame:
    """Weighted percent share per category, ignoring null categories."""
    return (
        df.select(category.alias("category"), pl.col(weight_col).cast(pl.Float64))
        .filter(pl.col("category").is_not_null())
        .group_by("category")
        .agg(pl.col(weight_col).sum().alias("weighted"))
        .with_columns(
            (pl.col("weighted") / pl.col("weighted").sum() * 100).alias("pct")
        )
    )


def acs_master_metered_share(pums: pl.DataFrame) -> float:
    """Percent of gas-using ACS households whose gas is included in rent or condo fee.

    GASFP: 1 included in rent/condo fee, 2 included in electricity payment,
    3 no charge or gas not used, 4 valid monthly gas cost.
    """
    gas = pums.filter(pl.col("gasfp").is_in(["1", "2", "4"]))
    total = float(gas["wgtp"].cast(pl.Float64).sum())
    if total == 0:
        raise ValueError("ACS PUMS has no gas-using households (GASFP in 1, 2, 4).")
    included = float(gas.filter(pl.col("gasfp") == "1")["wgtp"].cast(pl.Float64).sum())
    return included / total * 100


# ── Checks ────────────────────────────────────────────────────────────────────


def _utility_skip_note(eia_customers: float, cfg: RealWorldCheckConfig) -> str | None:
    if eia_customers <= 0:
        return "EIA reports zero residential customers (industrial- or commercial-only utility)"
    if eia_customers < cfg.min_utility_customers:
        return (
            f"fewer than {cfg.min_utility_customers:,.0f} EIA residential customers; "
            "too few ResStock samples to compare"
        )
    return None


def check_electricity(
    inputs: ReleaseInputs, eia861: pl.DataFrame, cfg: RealWorldCheckConfig
) -> list[CheckResult]:
    """Customer counts and customer-normalized kWh against EIA-861."""
    results: list[CheckResult] = []
    rs_customers = float(inputs.metadata_sb[WEIGHT].sum())
    eia_customers = float(eia861["eia_customers"].sum())
    eia_kwh = float(eia861["eia_kwh"].sum())
    results.append(
        _pct_result(
            check="electric_customers_statewide",
            label="electric customers",
            scope=STATEWIDE,
            release=inputs.release_sb,
            unit="dwellings",
            resstock=rs_customers,
            benchmark_name="EIA-861",
            benchmark=eia_customers,
            tolerance=cfg.electric_customers_statewide_pct,
        )
    )

    raw_kwh = weighted_total_kwh(inputs.metadata_raw, inputs.annual_raw, ANNUAL_ELEC)
    raw_norm = raw_kwh * eia_customers / rs_customers
    raw_result = _pct_result(
        check="electric_kwh_statewide",
        label="electricity, customer-normalized",
        scope=STATEWIDE,
        release=inputs.release_raw,
        unit="kWh",
        resstock=raw_norm,
        benchmark_name="EIA-861",
        benchmark=eia_kwh,
        tolerance=cfg.electric_kwh_statewide_pct,
    )
    # Raw is reported for context; only _sb is held to the tolerance.
    results.append(
        replace(
            raw_result,
            status="ok" if raw_result.status == "warn" else raw_result.status,
        )
    )
    if inputs.annual_sb is None:
        results.append(
            _skipped(
                "electric_kwh_statewide",
                "electricity, customer-normalized",
                STATEWIDE,
                inputs.release_sb,
                "_sb load_curve_annual not found",
            )
        )
    else:
        sb_kwh = weighted_total_kwh(inputs.metadata_sb, inputs.annual_sb, ANNUAL_ELEC)
        sb_result = _pct_result(
            check="electric_kwh_statewide",
            label="electricity, customer-normalized",
            scope=STATEWIDE,
            release=inputs.release_sb,
            unit="kWh",
            resstock=sb_kwh * eia_customers / rs_customers,
            benchmark_name="EIA-861",
            benchmark=eia_kwh,
            tolerance=cfg.electric_kwh_statewide_pct,
        )
        results.append(sb_result)
        if sb_result.diff is not None and raw_result.diff is not None:
            drift = abs(sb_result.diff) - abs(raw_result.diff)
            results.append(
                CheckResult(
                    check="electric_kwh_sb_drift",
                    label="electricity, _sb gap to EIA minus raw gap",
                    scope=STATEWIDE,
                    release=inputs.release_sb,
                    unit="%",
                    resstock_value=sb_result.diff,
                    benchmark_name="raw gap",
                    benchmark_value=raw_result.diff,
                    diff=drift,
                    diff_unit="pp",
                    tolerance=cfg.electric_kwh_sb_drift_pp,
                    status="warn" if drift > cfg.electric_kwh_sb_drift_pp else "ok",
                    note="_sb post-processing moved electricity further from EIA-861 than raw NREL"
                    if drift > cfg.electric_kwh_sb_drift_pp
                    else None,
                )
            )

    if inputs.utility_assignment is None:
        results.append(
            _skipped(
                "electric_customers_utility",
                "electric customers",
                "by utility",
                inputs.release_sb,
                "utility_assignment.parquet not found",
            )
        )
        return results
    by_utility = weighted_by_utility(
        inputs.metadata_sb,
        inputs.annual_sb,
        inputs.utility_assignment,
        ELEC_UTILITY,
        ANNUAL_ELEC,
    ).join(eia861, on="utility_code", how="left")
    for row in by_utility.sort("utility_code").iter_rows(named=True):
        scope = f"utility={row['utility_code']}"
        if row["eia_customers"] is None:
            for check in ("electric_customers_utility", "electric_kwh_utility"):
                results.append(
                    _skipped(
                        check,
                        "electricity",
                        scope,
                        inputs.release_sb,
                        "no EIA-861 match",
                    )
                )
            continue
        skip = _utility_skip_note(row["eia_customers"], cfg)
        if skip is not None:
            for check in ("electric_customers_utility", "electric_kwh_utility"):
                results.append(
                    _skipped(check, "electricity", scope, inputs.release_sb, skip)
                )
            continue
        results.append(
            _pct_result(
                check="electric_customers_utility",
                label="electric customers",
                scope=scope,
                release=inputs.release_sb,
                unit="dwellings",
                resstock=row["resstock_customers"],
                benchmark_name="EIA-861",
                benchmark=row["eia_customers"],
                tolerance=cfg.electric_customers_utility_pct,
            )
        )
        if row["resstock_kwh"] is None:
            continue
        results.append(
            _pct_result(
                check="electric_kwh_utility",
                label="electricity, customer-normalized",
                scope=scope,
                release=inputs.release_sb,
                unit="kWh",
                resstock=row["resstock_kwh"]
                * row["eia_customers"]
                / row["resstock_customers"],
                benchmark_name="EIA-861",
                benchmark=row["eia_kwh"],
                tolerance=cfg.electric_kwh_utility_pct,
            )
        )
    return results


def _meter_band_result(
    *,
    scope: str,
    release: str,
    row: Mapping[str, Any],
    eia_customers: float,
    cfg: RealWorldCheckConfig,
) -> CheckResult:
    lo, hi = float(row["meters_min"]), float(row["meters_max"])
    tol = cfg.gas_meters_band_pct
    if eia_customers < lo:
        diff = _pct_diff(eia_customers, lo)
    elif eia_customers > hi:
        diff = _pct_diff(eia_customers, hi)
    else:
        diff = 0.0
    inside = lo * (1 - tol / 100) <= eia_customers <= hi * (1 + tol / 100)
    share_lo, share_hi = float(row["master_share_min"]), float(row["master_share_max"])
    note = (
        f"{row['gas_dwellings']:,.0f} gas dwellings; ResStock implies "
        f"{share_lo:.0f}% to {share_hi:.0f}% of them are master-metered"
    )
    if hi > lo:
        implied = share_lo + (hi - eia_customers) / (hi - lo) * (share_hi - share_lo)
        note += f"; matching EIA-176 needs about {implied:.0f}%"
    return CheckResult(
        check="gas_meters",
        label="gas meters (estimated from dwellings)",
        scope=scope,
        release=release,
        unit="meters",
        resstock_value=lo,
        resstock_value_high=hi,
        benchmark_name="EIA-176 consumers",
        benchmark_value=eia_customers,
        diff=diff,
        diff_unit="pct",
        tolerance=tol,
        status="ok" if inside else "warn",
        note=note,
    )


def infer_gas_scaled(inputs: ReleaseInputs) -> bool | None:
    """True if ``_sb`` gas differs from raw (the EIA-176 scaler ran). None without ``_sb`` annual."""
    if inputs.annual_sb is None:
        return None
    raw = weighted_total_kwh(inputs.metadata_raw, inputs.annual_raw, ANNUAL_GAS)
    sb = weighted_total_kwh(inputs.metadata_sb, inputs.annual_sb, ANNUAL_GAS)
    return raw > 0 and abs(sb / raw - 1) > 1e-3


def check_gas(
    inputs: ReleaseInputs,
    eia176: pl.DataFrame,
    operator_map: Mapping[str, str] | None,
    cfg: RealWorldCheckConfig,
    gas_scaled: bool | None = None,
) -> list[CheckResult]:
    """Statewide and per-utility gas kWh and meters against EIA-176.

    Gas kWh is never divided by a customer count: EIA-176 consumers are meters.
    """
    results: list[CheckResult] = []
    eia_kwh = float(eia176["eia_kwh"].sum())
    eia_customers = float(eia176["eia_customers"].sum())
    imputed: list[str] = (
        eia176.filter(pl.col("heat_content_imputed"))["operator_id_eia"].to_list()
        if "heat_content_imputed" in eia176.columns
        else []
    )
    notes = []
    if imputed:
        notes.append(
            f"EIA-176 heat content imputed with the state median for {imputed}"
        )

    if gas_scaled is None:
        gas_scaled = infer_gas_scaled(inputs)
    sb_kwh: float | None = None
    if inputs.annual_sb is None:
        results.append(
            _skipped(
                "gas_kwh_statewide",
                "gas",
                STATEWIDE,
                inputs.release_sb,
                "_sb load_curve_annual not found",
            )
        )
    else:
        sb_kwh = weighted_total_kwh(inputs.metadata_sb, inputs.annual_sb, ANNUAL_GAS)
        if gas_scaled:
            notes.append(
                "the EIA-176 gas scaler ran, so this total should match almost exactly"
            )
        results.append(
            _pct_result(
                check="gas_kwh_statewide",
                label="gas" + (" (scaled to EIA-176)" if gas_scaled else ""),
                scope=STATEWIDE,
                release=inputs.release_sb,
                unit="kWh",
                resstock=sb_kwh,
                benchmark_name="EIA-176",
                benchmark=eia_kwh,
                tolerance=cfg.gas_kwh_statewide_scaled_pct
                if gas_scaled
                else cfg.gas_kwh_statewide_unscaled_pct,
                note="; ".join(notes) or None,
            )
        )

    meters = estimate_gas_meters(
        inputs.metadata_raw, inputs.annual_raw, inputs.utility_assignment
    )
    statewide_meters = meters.filter(pl.col("utility_code") == STATEWIDE).row(
        0, named=True
    )
    results.append(
        _meter_band_result(
            scope=STATEWIDE,
            release=inputs.release_raw,
            row=statewide_meters,
            eia_customers=eia_customers,
            cfg=cfg,
        )
    )

    if not operator_map:
        for check in ("gas_kwh_utility", "gas_meters"):
            results.append(
                _skipped(
                    check,
                    "gas",
                    "by utility",
                    inputs.release_sb,
                    f"no EIA-176 operator map for {inputs.state} in GAS_OPERATORS_BY_STATE",
                )
            )
        return results
    if inputs.utility_assignment is None:
        results.append(
            _skipped(
                "gas_meters",
                "gas",
                "by utility",
                inputs.release_sb,
                "utility_assignment.parquet not found",
            )
        )
        return results

    eia_by_utility = (
        eia176.filter(pl.col("operator_id_eia").is_in(list(operator_map)))
        .with_columns(
            pl.col("operator_id_eia")
            .replace_strict(dict(operator_map), return_dtype=pl.String)
            .alias("utility_code")
        )
        .group_by("utility_code")
        .agg(pl.col("eia_kwh").sum(), pl.col("eia_customers").sum())
    )
    meters_by_utility = meters.filter(pl.col("utility_code") != STATEWIDE)
    rs_by_utility = weighted_by_utility(
        inputs.metadata_sb,
        inputs.annual_sb,
        inputs.utility_assignment,
        GAS_UTILITY,
        ANNUAL_GAS,
    )
    scale = eia_kwh / sb_kwh if sb_kwh else None
    joined = eia_by_utility.join(meters_by_utility, on="utility_code", how="left").join(
        rs_by_utility.select("utility_code", "resstock_kwh"),
        on="utility_code",
        how="left",
    )
    for row in joined.sort("utility_code").iter_rows(named=True):
        scope = f"utility={row['utility_code']}"
        if row["gas_dwellings"] is None:
            for check in ("gas_meters", "gas_kwh_utility"):
                results.append(
                    _skipped(
                        check,
                        "gas",
                        scope,
                        inputs.release_sb,
                        "no ResStock dwellings assigned",
                    )
                )
            continue
        skip = _utility_skip_note(row["eia_customers"], cfg)
        if skip is not None:
            for check in ("gas_meters", "gas_kwh_utility"):
                results.append(_skipped(check, "gas", scope, inputs.release_sb, skip))
            continue
        results.append(
            _meter_band_result(
                scope=scope,
                release=inputs.release_raw,
                row=row,
                eia_customers=row["eia_customers"],
                cfg=cfg,
            )
        )
        if scale is None or row["resstock_kwh"] is None:
            continue
        results.append(
            _pct_result(
                check="gas_kwh_utility",
                label="gas, after one statewide kWh scaling",
                scope=scope,
                release=inputs.release_sb,
                unit="kWh",
                resstock=row["resstock_kwh"] * scale,
                benchmark_name="EIA-176",
                benchmark=row["eia_kwh"],
                tolerance=cfg.gas_kwh_utility_pct,
            )
        )
    return results


def check_hp_approximation(
    inputs: ReleaseInputs, cfg: RealWorldCheckConfig
) -> list[CheckResult]:
    """Share of each approximated upgrade's stock that received the HP approximation.

    Also flags approximated buildings that match neither targeting rule in
    ``approximate_non_hp_load`` (non-HP multifamily, or other/no heating fuel).
    """
    results: list[CheckResult] = []
    for upgrade, (sb, raw) in sorted(inputs.approx_metadata.items()):
        scope = f"upgrade={upgrade}"
        if "approximated_hp_load" not in sb.columns:
            results.append(
                _skipped(
                    "hp_approx_share",
                    "HP approximation",
                    scope,
                    inputs.release_sb,
                    "no approximated_hp_load column",
                )
            )
            continue
        df = sb.select(
            BLDG_ID, WEIGHT, pl.col("approximated_hp_load").fill_null(False)
        ).join(
            raw.select(BLDG_ID, "in.geometry_building_type_height", "in.heating_fuel"),
            on=BLDG_ID,
            how="left",
        )
        total = float(df[WEIGHT].sum())
        flagged = df.filter(pl.col("approximated_hp_load"))
        share = float(flagged[WEIGHT].sum()) / total * 100 if total else 0.0
        results.append(
            CheckResult(
                check="hp_approx_share",
                label="share of weighted stock with approximated HP load",
                scope=scope,
                release=inputs.release_sb,
                unit="%",
                resstock_value=share,
                benchmark_name="maximum",
                benchmark_value=cfg.hp_approx_max_share_pct,
                diff=share - cfg.hp_approx_max_share_pct,
                diff_unit="pp",
                tolerance=0.0,
                status="warn" if share > cfg.hp_approx_max_share_pct else "ok",
            )
        )
        unclassified = flagged.filter(
            ~pl.col("in.geometry_building_type_height")
            .str.contains("Multifamily")
            .fill_null(False)
            & ~pl.col("in.heating_fuel").is_in(["Other Fuel", "None"]).fill_null(False)
        ).height
        results.append(
            CheckResult(
                check="hp_approx_unclassified",
                label="approximated buildings matching neither targeting rule",
                scope=scope,
                release=inputs.release_sb,
                unit="buildings",
                resstock_value=float(unclassified),
                benchmark_name="expected",
                benchmark_value=0.0,
                diff=None,
                diff_unit="pct",
                tolerance=0.0,
                status="warn" if unclassified else "ok",
            )
        )
    return results


def _share_results(
    *,
    check: str,
    label: str,
    release: str,
    resstock: pl.DataFrame,
    acs: pl.DataFrame,
    tolerance: float,
) -> list[CheckResult]:
    joined = resstock.select("category", pl.col("pct").alias("rs_pct")).join(
        acs.select("category", pl.col("pct").alias("acs_pct")),
        on="category",
        how="full",
        coalesce=True,
    )
    results = []
    for row in joined.sort("category").iter_rows(named=True):
        rs = row["rs_pct"] or 0.0
        benchmark = row["acs_pct"] or 0.0
        diff = rs - benchmark
        results.append(
            CheckResult(
                check=check,
                label=label,
                scope=f"statewide, {row['category']}",
                release=release,
                unit="%",
                resstock_value=rs,
                benchmark_name="ACS",
                benchmark_value=benchmark,
                diff=diff,
                diff_unit="pp",
                tolerance=tolerance,
                status="warn" if abs(diff) > tolerance else "ok",
            )
        )
    return results


def check_acs(
    inputs: ReleaseInputs, pums: pl.DataFrame, cfg: RealWorldCheckConfig
) -> list[CheckResult]:
    """Occupied-stock shares against ACS PUMS: fuel, building type, vintage, master metering."""
    occupied = _occupied(inputs.metadata_raw)
    release = inputs.release_raw
    results = _share_results(
        check="acs_heating_fuel",
        label="heating-fuel share",
        release=release,
        resstock=weighted_shares(
            occupied, heating_fuel_resstock(pl.col("in.heating_fuel")), WEIGHT
        ),
        acs=weighted_shares(pums, heating_fuel_acs(pl.col("hfl")), "wgtp"),
        tolerance=cfg.acs_share_pp,
    )
    results += _share_results(
        check="acs_building_type",
        label="building-type share",
        release=release,
        resstock=weighted_shares(
            occupied,
            building_class_resstock(pl.col("in.geometry_building_type_recs")),
            WEIGHT,
        ),
        acs=weighted_shares(pums, building_class_acs(pl.col("bld")), "wgtp"),
        tolerance=cfg.acs_share_pp,
    )
    results += _share_results(
        check="acs_year_built",
        label="year-built share",
        release=release,
        resstock=weighted_shares(
            occupied, vintage_resstock(pl.col("in.vintage")), WEIGHT
        ),
        acs=weighted_shares(pums, vintage_acs(pl.col("yrblt")), "wgtp"),
        tolerance=cfg.acs_share_pp,
    )

    acs_share = acs_master_metered_share(pums)
    meters = estimate_gas_meters(occupied, inputs.annual_raw).row(0, named=True)
    lo, hi = float(meters["master_share_min"]), float(meters["master_share_max"])
    tol = cfg.acs_master_metering_pp
    if acs_share < lo:
        diff = acs_share - lo
    elif acs_share > hi:
        diff = acs_share - hi
    else:
        diff = 0.0
    results.append(
        CheckResult(
            check="acs_master_metering",
            label="master-metered share of gas households",
            scope=STATEWIDE,
            release=release,
            unit="%",
            resstock_value=lo,
            resstock_value_high=hi,
            benchmark_name="ACS gas included in rent (GASFP=1)",
            benchmark_value=acs_share,
            diff=diff,
            diff_unit="pp",
            tolerance=tol,
            status="warn" if abs(diff) > tol else "ok",
            note="ACS 'included in rent' is an upper bound on master metering",
        )
    )
    return results


# ── Runner ────────────────────────────────────────────────────────────────────


def _guard(
    check: str, label: str, release: str, fn: Callable[[], list[CheckResult]]
) -> list[CheckResult]:
    """Run one group of checks; turn any exception into a skipped result."""
    try:
        return fn()
    except Exception as exc:
        return [
            _skipped(
                check,
                label,
                STATEWIDE,
                release,
                f"check failed: {type(exc).__name__}: {exc}",
            )
        ]


def _apply_ignore(
    results: list[CheckResult], cfg: RealWorldCheckConfig
) -> list[CheckResult]:
    ignored = set(cfg.ignore)
    return [
        replace(r, status="ignored")
        if r.status == "warn"
        and (r.check in ignored or f"{r.check}:{r.scope}" in ignored)
        else r
        for r in results
    ]


def run_real_world_checks(
    *,
    inputs: ReleaseInputs,
    eia_year: int,
    path_eia861: str,
    path_pums_housing: str,
    cfg: RealWorldCheckConfig,
    gas_scaled: bool | None = None,
    load_eia861: Callable[[str, str], pl.DataFrame] = load_eia861_residential,
    load_eia176: Callable[[str, int], pl.DataFrame] = load_eia176_by_operator,
    load_acs: Callable[[str], pl.DataFrame] = load_acs_housing,
) -> list[CheckResult]:
    """Run every check for one state. Never raises: failures become ``skipped`` results."""
    state = inputs.state
    results: list[CheckResult] = []
    results += _guard(
        "electric_customers_statewide",
        "electricity vs EIA-861",
        inputs.release_sb,
        lambda: check_electricity(inputs, load_eia861(path_eia861, state), cfg),
    )
    results += _guard(
        "gas_kwh_statewide",
        "gas vs EIA-176",
        inputs.release_sb,
        lambda: check_gas(
            inputs,
            load_eia176(state, eia_year),
            GAS_OPERATORS_BY_STATE.get(state),
            cfg,
            gas_scaled=gas_scaled,
        ),
    )
    results += _guard(
        "hp_approx_share",
        "HP approximation",
        inputs.release_sb,
        lambda: check_hp_approximation(inputs, cfg),
    )
    results += _guard(
        "acs_heating_fuel",
        "stock vs ACS",
        inputs.release_raw,
        lambda: check_acs(inputs, load_acs(path_pums_housing), cfg),
    )
    return _apply_ignore(results, cfg)


def summarize(state: str, results: Sequence[CheckResult]) -> list[str]:
    """Print a one-line count summary and each warning; return the warning messages."""
    counts = {
        s: sum(r.status == s for r in results)
        for s in ("ok", "warn", "skipped", "ignored")
    }
    print(
        f"  Real-world checks for {state}: {counts['ok']} ok, {counts['warn']} warn, "
        f"{counts['skipped']} skipped, {counts['ignored']} ignored.",
        flush=True,
    )
    warnings = [format_warning(state, r) for r in results if r.status == "warn"]
    for msg in warnings:
        print(f"WARNING {msg}", flush=True)
    for r in results:
        if r.status == "skipped" and r.note and r.note.startswith("check failed"):
            print(f"  NOTE: {state} {r.label} skipped: {r.note}", flush=True)
    return warnings


def write_report(path: Path, state: str, results: Sequence[CheckResult]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "state": state,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "results": [r.to_dict() for r in results],
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return path


@dataclass
class PostPipelineChecks:
    """Outcome of the end-of-pipeline checks across states, for the manifest."""

    results: dict[str, list[CheckResult]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    reports: dict[str, str] = field(default_factory=dict)
    skipped_reason: str | None = None

    def manifest_details(self) -> dict[str, Any]:
        if self.skipped_reason is not None:
            return {"skipped": self.skipped_reason}
        return {
            "counts": {
                state: {
                    s: sum(r.status == s for r in rs)
                    for s in ("ok", "warn", "skipped", "ignored")
                }
                for state, rs in self.results.items()
            },
            "reports": self.reports,
            "warned": [
                r.to_dict()
                for rs in self.results.values()
                for r in rs
                if r.status == "warn"
            ],
        }


def pums_housing_path(path_s3_pums_dir: str, survey: str, year: str, state: str) -> str:
    return f"{path_s3_pums_dir.rstrip('/')}/{survey}/{year}/housing/state={state}/data.parquet"


def eia861_path(path_s3_eia861_dir: str, year: int, state: str) -> str:
    return f"{path_s3_eia861_dir.rstrip('/')}/year={year}/state={state}/data.parquet"


def run_post_pipeline_checks(
    *,
    states: Sequence[str],
    upgrade_ids: Sequence[str],
    sample: int,
    release: str,
    path_raw: Path,
    path_sb: Path,
    eia_year: int | None,
    path_s3_eia861_dir: str,
    path_s3_pums_dir: str,
    pums_survey: str,
    pums_year: str,
    approx_upgrades: Sequence[str],
    gas_scaled: bool | None = None,
) -> PostPipelineChecks:
    """End-of-pipeline entry point for ``main.py``. Never raises."""
    out = PostPipelineChecks()
    if sample > 0:
        out.skipped_reason = "--sample is set; weights do not add up to the state"
    elif BASELINE_UPGRADE not in [u.zfill(2) for u in upgrade_ids]:
        out.skipped_reason = "upgrade 00 is not in --upgrade-ids"
    if out.skipped_reason is None and eia_year is None:
        try:
            eia_year = eia_year_for_resstock_release(release)
        except ValueError as exc:
            out.skipped_reason = str(exc)
    if out.skipped_reason is not None or eia_year is None:
        print(f"  Skipping real-world checks: {out.skipped_reason}", flush=True)
        return out

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for state in states:
        try:
            cfg = load_check_config(state)
            inputs = load_release_inputs(
                state=state,
                base_raw=str(path_raw),
                base_sb=str(path_sb),
                approx_upgrades=[
                    u
                    for u in approx_upgrades
                    if u.zfill(2) in {x.zfill(2) for x in upgrade_ids}
                ],
            )
            results = run_real_world_checks(
                inputs=inputs,
                eia_year=eia_year,
                path_eia861=eia861_path(path_s3_eia861_dir, eia_year, state),
                path_pums_housing=pums_housing_path(
                    path_s3_pums_dir, pums_survey, pums_year, state
                ),
                cfg=cfg,
                gas_scaled=gas_scaled,
            )
            out.results[state] = results
            out.warnings += summarize(state, results)
            report = write_report(
                path_sb / f"real_world_checks_{state}_{ts}.yaml", state, results
            )
            out.reports[state] = str(report)
            print(f"  Report: {report}", flush=True)
        except Exception as exc:
            msg = f"[real-world] {state} checks could not run: {type(exc).__name__}: {exc}"
            print(f"WARNING {msg}", flush=True)
            out.warnings.append(msg)
    return out


# ── CLI ───────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    with CONFIG_PATH.open() as f:
        cfg = yaml.safe_load(f)
    r = cfg["resstock"]
    default_release = (
        f"res_{r['release_year']}_{r['weather_file']}_{r['release_version']}"
    )
    parser = argparse.ArgumentParser(
        description="Compare a built ResStock release to EIA-861, EIA-176, and ACS PUMS.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--state", nargs="+", required=True, help="2-letter state code(s)."
    )
    parser.add_argument(
        "--release", default=default_release, help="Raw (non-_sb) release name."
    )
    parser.add_argument("--location", choices=["ebs", "s3"], default="ebs")
    parser.add_argument("--path-output-dir", default=cfg["paths"]["output_dir"])
    parser.add_argument("--path-s3-dir", default=cfg["paths"]["s3_dir"])
    parser.add_argument("--path-s3-eia861-dir", default=cfg["paths"]["s3_eia861_dir"])
    parser.add_argument("--path-s3-pums-dir", default=cfg["paths"]["s3_pums_dir"])
    parser.add_argument("--pums-survey", default=cfg["pums"]["survey"])
    parser.add_argument("--pums-year", default=str(cfg["pums"]["year"]))
    parser.add_argument(
        "--eia-year",
        type=int,
        default=None,
        help="Default: AMY year in the release name.",
    )
    parser.add_argument(
        "--path-report",
        type=Path,
        default=None,
        help="Write the YAML report here (one state only). Default: print only.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    states = [s.upper() for chunk in args.state for s in chunk.split()]
    base = args.path_output_dir if args.location == "ebs" else args.path_s3_dir
    base = base.rstrip("/")
    eia_year = args.eia_year or eia_year_for_resstock_release(args.release)
    with CONFIG_PATH.open() as f:
        approx_upgrades = yaml.safe_load(f)["resstock"]["approx_upgrade_ids"]
    n_warn = 0
    for state in states:
        inputs = load_release_inputs(
            state=state,
            base_raw=f"{base}/{args.release}",
            base_sb=f"{base}/{args.release}_sb",
            approx_upgrades=approx_upgrades,
        )
        results = run_real_world_checks(
            inputs=inputs,
            eia_year=eia_year,
            path_eia861=eia861_path(args.path_s3_eia861_dir, eia_year, state),
            path_pums_housing=pums_housing_path(
                args.path_s3_pums_dir, args.pums_survey, args.pums_year, state
            ),
            cfg=load_check_config(state),
        )
        n_warn += len(summarize(state, results))
        if args.path_report is not None and len(states) == 1:
            print(f"  Report: {write_report(args.path_report, state, results)}")
    print(f"Done: {n_warn} warning(s).")


if __name__ == "__main__":
    main(sys.argv[1:])
