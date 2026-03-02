"""Investigate ResStock vs EIA load discrepancy by electric utility.

Reads ResStock annual loads and metadata, joins to utility assignment, and groups
by electric utility code. Loads EIA-861 residential sales by utility. Produces
ResStock annual load results by utility, metadata grouped by utility, and EIA
loads for comparison.
"""

from __future__ import annotations

from typing import cast

import math

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend so plot works without a display
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from scipy import stats as scipy_stats

from utils import get_aws_region

BLDG_ID_COL = "bldg_id"
ELECTRIC_UTILITY_COL = "sb.electric_utility"
WEIGHT_COL = "weight"
ANNUAL_ELECTRICITY_COL = "out.electricity.total.energy_consumption.kwh"
MWH_TO_KWH = 1000

DEFAULT_RESSTOCK_RELEASE = "res_2024_amy2018_2"
S3_BASE_RESSTOCK = "s3://data.sb/nrel/resstock"
S3_BASE_EIA861 = "s3://data.sb/eia/861/electric_utility_stats"

# Metadata column names
BUILDING_TYPE_RECS_COL = "in.geometry_building_type_recs"
FLOOR_AREA_COL = "in.geometry_floor_area"


# Load curve annual column names
HVAC_RELATED_ELECTRICITY_COLS = (
    "out.electricity.cooling_fans_pumps.energy_consumption.kwh",
    "out.electricity.heating.energy_consumption.kwh",
    "out.electricity.heating_fans_pumps.energy_consumption.kwh",
    "out.electricity.heating_hp_bkup.energy_consumption.kwh",
    "out.electricity.heating_hp_bkup_fa.energy_consumption.kwh",
    "out.electricity.mech_vent.energy_consumption.kwh"
)
NON_HVAC_RELATED_ELECTRICITY_COLS = (
    "out.electricity.ceiling_fan.energy_consumption.kwh",
    "out.electricity.clothes_dryer.energy_consumption.kwh",
    "out.electricity.clothes_washer.energy_consumption.kwh",
    "out.electricity.dishwasher.energy_consumption.kwh",
    "out.electricity.freezer.energy_consumption.kwh",
    "out.electricity.hot_water.energy_consumption.kwh",
    "out.electricity.lighting_exterior.energy_consumption.kwh",
    "out.electricity.lighting_garage.energy_consumption.kwh",
    "out.electricity.lighting_interior.energy_consumption.kwh",
    "out.electricity.permanent_spa_heat.energy_consumption.kwh",
    "out.electricity.permanent_spa_pump.energy_consumption.kwh",
    "out.electricity.plug_loads.energy_consumption.kwh",
    "out.electricity.pool_heater.energy_consumption.kwh",
    "out.electricity.pool_pump.energy_consumption.kwh",
    "out.electricity.range_oven.energy_consumption.kwh",
    "out.electricity.refrigerator.energy_consumption.kwh",
    "out.electricity.well_pump.energy_consumption.kwh",
    "out.electricity.pv.energy_consumption.kwh"
)

def _storage_options() -> dict[str, str]:
    return {"aws_region": get_aws_region()}


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _default_annual_path(release: str, state: str, upgrade: str) -> str:
    base = f"{S3_BASE_RESSTOCK}/{release}/load_curve_annual/state={state}/upgrade={upgrade}"
    return f"{base}/{state}_upgrade{upgrade}_metadata_and_annual_results.parquet"


def _default_utility_assignment_path(release: str, state: str) -> str:
    return f"{S3_BASE_RESSTOCK}/{release}/metadata_utility/state={state}/utility_assignment.parquet"


def _default_metadata_path(release: str, state: str, upgrade: str) -> str:
    return f"{S3_BASE_RESSTOCK}/{release}/metadata/state={state}/upgrade={upgrade}/metadata-sb.parquet"


def _default_eia861_path(state: str, year: int = 2018) -> str:
    return f"{S3_BASE_EIA861}/year={year}/state={state}/data.parquet"


def load_resstock_annual_by_utility(
    path_annual: str,
    path_utility_assignment: str,
    storage_options: dict[str, str] | None,
    load_column: str | None = None,
) -> pl.DataFrame:
    """Read annual parquet, join utility assignment, group by electric utility. Returns one row per utility with aggregated load results."""
    opts_annual = storage_options if _is_s3(path_annual) else None
    elec_col = load_column or ANNUAL_ELECTRICITY_COL
    annual_lf = pl.scan_parquet(path_annual, storage_options=opts_annual)
    schema = annual_lf.collect_schema().names()
    if elec_col not in schema or BLDG_ID_COL not in schema or WEIGHT_COL not in schema:
        raise ValueError(
            f"Annual parquet at {path_annual!r} must have {BLDG_ID_COL!r}, {elec_col!r}, {WEIGHT_COL!r}. Found: {schema[:40]!r}"
        )

    annual_df = cast(
        pl.DataFrame,
        annual_lf.select(
            pl.col(BLDG_ID_COL),
            pl.col(elec_col).alias("annual_kwh"),
            pl.col(WEIGHT_COL),
        ).collect(),
    )

    opts_ua = storage_options if _is_s3(path_utility_assignment) else None
    ua_df = cast(
        pl.DataFrame,
        pl.scan_parquet(path_utility_assignment, storage_options=opts_ua)
        .select(BLDG_ID_COL, ELECTRIC_UTILITY_COL)
        .collect(),
    )
    if ELECTRIC_UTILITY_COL not in ua_df.columns:
        raise ValueError(
            f"Utility assignment at {path_utility_assignment!r} missing {ELECTRIC_UTILITY_COL!r}"
        )

    joined = annual_df.join(ua_df, on=BLDG_ID_COL, how="inner").with_columns(
        (pl.col("annual_kwh") * pl.col(WEIGHT_COL)).alias("weighted_kwh")
    )
    by_utility = joined.group_by(ELECTRIC_UTILITY_COL).agg(
        pl.col("weighted_kwh").sum().alias("resstock_total_kwh"),
        pl.col(WEIGHT_COL).sum().alias("resstock_customers"),
        pl.len().alias("n_bldgs"),
        pl.col("annual_kwh").mean().alias("mean_annual_kwh_per_bldg"),
    )
    return by_utility.rename({ELECTRIC_UTILITY_COL: "utility_code"})


def load_metadata_by_utility(
    path_metadata: str,
    path_utility_assignment: str,
    storage_options: dict[str, str] | None,
) -> tuple[pl.DataFrame, dict[str, pl.DataFrame]]:
    """Load metadata parquet, join utility assignment. Returns (metadata with utility_code column, dict of utility_code -> metadata DataFrame)."""
    opts_meta = storage_options if _is_s3(path_metadata) else None
    opts_ua = storage_options if _is_s3(path_utility_assignment) else None

    meta_df = cast(
        pl.DataFrame,
        pl.scan_parquet(path_metadata, storage_options=opts_meta).collect(),
    )
    if BLDG_ID_COL not in meta_df.columns:
        raise ValueError(f"Metadata at {path_metadata!r} missing {BLDG_ID_COL!r}")

    ua_df = cast(
        pl.DataFrame,
        pl.scan_parquet(path_utility_assignment, storage_options=opts_ua)
        .select(BLDG_ID_COL, ELECTRIC_UTILITY_COL)
        .collect(),
    )

    metadata_with_utility = meta_df.join(ua_df, on=BLDG_ID_COL, how="inner").rename(
        {ELECTRIC_UTILITY_COL: "utility_code"}
    )
    by_utility: dict[str, pl.DataFrame] = {}
    for code in metadata_with_utility["utility_code"].unique().to_list():
        by_utility[code] = metadata_with_utility.filter(pl.col("utility_code") == code)
    return metadata_with_utility, by_utility


def load_eia_by_utility(
    path_eia861: str,
    storage_options: dict[str, str] | None,
    *,
    utility_codes: list[int] | None = None,
) -> pl.DataFrame:
    """Load EIA-861 state parquet; returns one row per utility with residential sales and customer counts.

    When utility_codes is provided, only rows with matching utility_code are loaded.
    """
    opts = storage_options if _is_s3(path_eia861) else None
    lf = (
        pl.scan_parquet(path_eia861, storage_options=opts)
        .select(
            pl.col("utility_code"),
            pl.col("residential_sales_mwh"),
            (pl.col("residential_sales_mwh") * MWH_TO_KWH).alias("eia_residential_kwh"),
            pl.col("residential_customers").alias("eia_residential_customers"),
        )
    )
    if utility_codes is not None:
        lf = lf.filter(pl.col("utility_code").is_in(utility_codes))
    df = lf.collect()
    return cast(pl.DataFrame, df)


def building_type_share_by_utility(
    metadata_by_utility: dict[str | int, pl.DataFrame],
) -> dict[str | int, dict[str, float]]:
    """Compute each utility's share of multifamily and single-family buildings.

    Multifamily: BUILDING_TYPE_RECS_COL contains "Multi-Family".
    Single-family: BUILDING_TYPE_RECS_COL contains "Single-Family".
    Percentages are count-based (share of buildings in that utility).
    Returns dict mapping utility_code -> {"multifamily_pct": float, "single_family_pct": float}.
    """
    out: dict[str | int, dict[str, float]] = {}
    for utility_code, df in metadata_by_utility.items():
        if BUILDING_TYPE_RECS_COL not in df.columns:
            raise ValueError(
                f"Metadata for utility {utility_code!r} missing column {BUILDING_TYPE_RECS_COL!r}"
            )
        col = pl.col(BUILDING_TYPE_RECS_COL)
        n_total = len(df)
        if n_total == 0:
            out[utility_code] = {"multifamily_pct": 0.0, "single_family_pct": 0.0}
            continue
        n_multifamily = df.filter(col.str.contains("Multi-Family", literal=True)).height
        n_single_family = df.filter(
            col.str.contains("Single-Family", literal=True)
        ).height
        n_mobile_home = df.filter(
            col.str.contains("Mobile Home", literal=True)
        ).height
        out[utility_code] = {
            "multifamily_pct": n_multifamily / n_total * 100.0,
            "single_family_pct": n_single_family / n_total * 100.0,
            "mobile_home_pct": n_mobile_home / n_total * 100.0,
        }
    return out


def compare_resstock_eia_by_utility(
    resstock_annual_by_utility: pl.DataFrame,
    eia_by_utility: pl.DataFrame,
) -> pl.DataFrame:
    """Join ResStock and EIA by utility_code and compute comparison metrics.

    ResStock total kWh is normalized by customer count before comparison: we scale
    ResStock total kWh by (eia_customers / resstock_customers) so kwh_ratio and
    kwh_pct_diff reflect per-customer load (consumption intensity).
    """
    joined = resstock_annual_by_utility.join(
        eia_by_utility.select(
            pl.col("utility_code"),
            pl.col("eia_residential_kwh"),
            pl.col("eia_residential_customers"),
        ),
        on="utility_code",
        how="inner",
    )
    resstock_normalized_kwh = (
        pl.col("resstock_total_kwh")
        * pl.col("eia_residential_customers")
        / pl.col("resstock_customers")
    )
    return joined.with_columns(
        resstock_normalized_kwh.alias("resstock_total_kwh_normalized_to_eia")
    ).select(
        pl.col("utility_code"),
        pl.col("resstock_total_kwh"),
        pl.col("resstock_total_kwh_normalized_to_eia"),
        pl.col("eia_residential_kwh"),
        (pl.col("resstock_total_kwh_normalized_to_eia") / pl.col("eia_residential_kwh")).alias(
            "kwh_ratio"
        ),
        (
            (pl.col("resstock_total_kwh_normalized_to_eia") - pl.col("eia_residential_kwh"))
            / pl.col("eia_residential_kwh")
            * 100
        ).alias("kwh_pct_diff"),
        pl.col("resstock_customers"),
        pl.col("eia_residential_customers"),
        (pl.col("resstock_customers") / pl.col("eia_residential_customers")).alias(
            "customers_ratio"
        ),
        (
            (pl.col("resstock_customers") - pl.col("eia_residential_customers"))
            / pl.col("eia_residential_customers")
            * 100
        ).alias("customers_pct_diff"),
    )

def fit_kwh_pct_diff_vs_multifamily_pct(
    comparison: pl.DataFrame,
    building_type_shares: dict[str | int, dict[str, float]],
) -> dict[str, float | int | np.ndarray]:
    """Fit a linear model: kwh_pct_diff ~ multifamily_pct.

    Returns dict with fit params, R², error distribution, F-test (overall significance),
    t-tests and p-values for slope/intercept, and AIC/BIC for model comparison.
    """
    shares_df = pl.DataFrame([
        {
            "utility_code": uc,
            "multifamily_pct": vals["multifamily_pct"],
        }
        for uc, vals in building_type_shares.items()
    ])
    plot_df = (
        comparison.select("utility_code", "kwh_pct_diff")
        .join(shares_df, on="utility_code", how="inner")
    )
    x = plot_df["multifamily_pct"].to_numpy()
    y = plot_df["kwh_pct_diff"].to_numpy()
    n = len(x)
    if n < 2:
        raise ValueError("Need at least 2 points to fit a linear model")

    coefs = np.polyfit(x, y, 1)
    slope, intercept = float(coefs[0]), float(coefs[1])
    y_pred = slope * x + intercept
    residuals = y - y_pred

    ss_res = np.sum(residuals**2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
    ss_reg = np.sum((y_pred - np.mean(y)) ** 2)

    df_residual = n - 2
    df_reg = 1
    rmse = np.sqrt(ss_res / df_residual) if df_residual > 0 else 0.0
    x_mean = np.mean(x)
    x_var = np.sum((x - x_mean) ** 2)
    if x_var > 0:
        se_slope = float(rmse / np.sqrt(x_var))
        se_intercept = float(rmse * np.sqrt(1 / n + x_mean**2 / x_var))
    else:
        se_slope = se_intercept = 0.0

    # F-test: H0 that slope = 0 (no linear relationship)
    ms_reg = ss_reg / df_reg if df_reg else 0.0
    ms_res = ss_res / df_residual if df_residual else 0.0
    f_stat = (ms_reg / ms_res) if ms_res > 0 else 0.0
    f_pvalue = float(scipy_stats.f.sf(f_stat, df_reg, df_residual))

    # t-tests for slope and intercept
    t_slope = (slope / se_slope) if se_slope > 0 else 0.0
    slope_pvalue = float(2 * scipy_stats.t.sf(abs(t_slope), df_residual))
    t_intercept = (intercept / se_intercept) if se_intercept > 0 else 0.0
    intercept_pvalue = float(2 * scipy_stats.t.sf(abs(t_intercept), df_residual))

    # AIC/BIC for model comparison (e.g. when adding predictors)
    k = 2  # slope + intercept
    ss_res_safe = ss_res if ss_res > 0 else 1e-10
    aic = float(n * math.log(ss_res_safe / n) + 2 * k)
    bic = float(n * math.log(ss_res_safe / n) + k * math.log(n))

    return {
        "slope": slope,
        "intercept": intercept,
        "r_squared": r_squared,
        "n_obs": n,
        "residuals": residuals,
        "residual_mean": float(np.mean(residuals)),
        "residual_std": float(np.std(residuals, ddof=2)) if n > 2 else 0.0,
        "rmse": float(rmse),
        "se_slope": se_slope,
        "se_intercept": se_intercept,
        "f_stat": f_stat,
        "f_pvalue": f_pvalue,
        "slope_t": t_slope,
        "slope_pvalue": slope_pvalue,
        "intercept_t": t_intercept,
        "intercept_pvalue": intercept_pvalue,
        "aic": aic,
        "bic": bic,
    }


def fit_kwh_pct_diff_vs_multifamily_and_mobile_pct(
    comparison: pl.DataFrame,
    building_type_shares: dict[str | int, dict[str, float]],
    fit_single_predictor: dict[str, float | int | np.ndarray] | None = None,
) -> dict[str, float | int | np.ndarray]:
    """Fit linear model: kwh_pct_diff ~ multifamily_pct + mobile_home_pct.

    Returns same stat structure as fit_kwh_pct_diff_vs_multifamily_pct, with two slopes.
    If fit_single_predictor (from fit_kwh_pct_diff_vs_multifamily_pct) is provided,
    also returns partial F-test and comparison stats to test whether adding
    mobile_home_pct improves the fit significantly.
    """
    shares_df = pl.DataFrame([
        {
            "utility_code": uc,
            "multifamily_pct": vals["multifamily_pct"],
            "mobile_home_pct": vals.get("mobile_home_pct", 0.0),
        }
        for uc, vals in building_type_shares.items()
    ])
    plot_df = (
        comparison.select("utility_code", "kwh_pct_diff")
        .join(shares_df, on="utility_code", how="inner")
    )
    x_mf = plot_df["multifamily_pct"].to_numpy()
    x_mobile = plot_df["mobile_home_pct"].to_numpy()
    y = plot_df["kwh_pct_diff"].to_numpy()
    n = len(y)
    if n < 3:
        raise ValueError("Need at least 3 points to fit a two-predictor linear model")

    X = np.column_stack([np.ones(n), x_mf, x_mobile])
    beta, ss_res_arr, rank, _ = np.linalg.lstsq(X, y, rcond=None)
    ss_res_full = float(np.sum((y - X @ beta) ** 2))
    residuals = y - X @ beta
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1.0 - (ss_res_full / ss_tot) if ss_tot > 0 else 0.0
    ss_reg = ss_tot - ss_res_full

    intercept = float(beta[0])
    slope_multifamily = float(beta[1])
    slope_mobile_home = float(beta[2])

    df_residual = n - 3
    df_reg = 2
    rmse = np.sqrt(ss_res_full / df_residual) if df_residual > 0 else 0.0
    sigma_sq = ss_res_full / df_residual if df_residual > 0 else 0.0
    try:
        cov_beta = sigma_sq * np.linalg.inv(X.T @ X)
        se = np.sqrt(np.diag(cov_beta))
        se_intercept = float(se[0])
        se_slope_multifamily = float(se[1])
        se_slope_mobile_home = float(se[2])
    except np.linalg.LinAlgError:
        se_intercept = se_slope_multifamily = se_slope_mobile_home = 0.0

    ms_reg = ss_reg / df_reg if df_reg else 0.0
    ms_res = ss_res_full / df_residual if df_residual else 0.0
    f_stat = (ms_reg / ms_res) if ms_res > 0 else 0.0
    f_pvalue = float(scipy_stats.f.sf(f_stat, df_reg, df_residual))

    t_mf = (slope_multifamily / se_slope_multifamily) if se_slope_multifamily > 0 else 0.0
    t_mobile = (slope_mobile_home / se_slope_mobile_home) if se_slope_mobile_home > 0 else 0.0
    t_intercept = (intercept / se_intercept) if se_intercept > 0 else 0.0
    slope_multifamily_pvalue = float(2 * scipy_stats.t.sf(abs(t_mf), df_residual))
    slope_mobile_home_pvalue = float(2 * scipy_stats.t.sf(abs(t_mobile), df_residual))
    intercept_pvalue = float(2 * scipy_stats.t.sf(abs(t_intercept), df_residual))

    k = 3
    ss_res_safe = ss_res_full if ss_res_full > 0 else 1e-10
    aic = float(n * math.log(ss_res_safe / n) + 2 * k)
    bic = float(n * math.log(ss_res_safe / n) + k * math.log(n))

    out: dict[str, float | int | np.ndarray] = {
        "slope_multifamily": slope_multifamily,
        "slope_mobile_home": slope_mobile_home,
        "intercept": intercept,
        "r_squared": r_squared,
        "n_obs": n,
        "residuals": residuals,
        "residual_mean": float(np.mean(residuals)),
        "residual_std": float(np.std(residuals, ddof=3)) if n > 3 else 0.0,
        "rmse": float(rmse),
        "se_slope_multifamily": se_slope_multifamily,
        "se_slope_mobile_home": se_slope_mobile_home,
        "se_intercept": se_intercept,
        "f_stat": f_stat,
        "f_pvalue": f_pvalue,
        "slope_multifamily_t": t_mf,
        "slope_multifamily_pvalue": slope_multifamily_pvalue,
        "slope_mobile_home_t": t_mobile,
        "slope_mobile_home_pvalue": slope_mobile_home_pvalue,
        "intercept_t": t_intercept,
        "intercept_pvalue": intercept_pvalue,
        "aic": aic,
        "bic": bic,
    }

    if fit_single_predictor is not None:
        r_sq_single = float(fit_single_predictor["r_squared"])
        ss_res_single = np.sum(np.asarray(fit_single_predictor["residuals"]) ** 2)
        delta_r_squared = r_squared - r_sq_single
        # Partial F-test: does adding mobile_home_pct explain significantly more variance?
        # F = (SS_res_single - SS_res_full) / 1 / (SS_res_full / (n-3))
        df_extra = 1
        partial_f = (
            (ss_res_single - ss_res_full) / df_extra / (ss_res_full / df_residual)
            if ss_res_full > 0
            else 0.0
        )
        partial_f_pvalue = float(scipy_stats.f.sf(partial_f, df_extra, df_residual))
        out["r_squared_single_predictor"] = r_sq_single
        out["delta_r_squared"] = delta_r_squared
        out["partial_f_stat"] = partial_f
        out["partial_f_pvalue"] = partial_f_pvalue
        out["aic_single_predictor"] = fit_single_predictor["aic"]
        out["bic_single_predictor"] = fit_single_predictor["bic"]

    return out


def plot_kwh_pct_diff_vs_multifamily_pct(
    comparison: pl.DataFrame,
    building_type_shares: dict[str | int, dict[str, float]],
    fit_result: dict[str, float | int | np.ndarray],
    *,
    path_output: str | None = None,
) -> None:
    """Plot percent difference (ResStock vs EIA load) per utility vs share of multifamily homes.

    Uses fit_result from fit_kwh_pct_diff_vs_multifamily_pct for the regression line and stats.
    comparison: DataFrame from compare_resstock_eia_by_utility (utility_code, kwh_pct_diff).
    building_type_shares: dict from building_type_share_by_utility (utility_code -> multifamily_pct, single_family_pct).
    """
    shares_df = pl.DataFrame([
        {
            "utility_code": uc,
            "multifamily_pct": vals["multifamily_pct"],
        }
        for uc, vals in building_type_shares.items()
    ])
    plot_df = (
        comparison.select("utility_code", "kwh_pct_diff")
        .join(shares_df, on="utility_code", how="inner")
    )
    x = plot_df["multifamily_pct"].to_numpy()
    y = plot_df["kwh_pct_diff"].to_numpy()
    utility_codes = plot_df["utility_code"].to_list()

    fig, ax = plt.subplots()
    ax.scatter(x, y, alpha=0.7)
    for xi, yi, uc in zip(x, y, utility_codes, strict=True):
        ax.annotate(
            str(uc),
            (xi, yi),
            fontsize=8,
            alpha=0.9,
            xytext=(4, 4),
            textcoords="offset points",
        )
    if len(x) > 1:
        slope = float(fit_result["slope"])
        intercept = float(fit_result["intercept"])
        x_line = np.linspace(x.min(), x.max(), 100)
        ax.plot(x_line, slope * x_line + intercept, "r-", alpha=0.8, label="Linear fit")
        r_sq = fit_result["r_squared"]
        f_pval = fit_result["f_pvalue"]
        ax.legend()
        ax.text(
            0.05,
            0.95,
            f"$R^2$ = {r_sq:.3f}\nF p-value = {f_pval:.4f}",
            transform=ax.transAxes,
            fontsize=9,
            verticalalignment="top",
        )
    ax.set_xlabel("Multifamily share (%)")
    ax.set_ylabel(
        "Load % difference (ResStock − EIA) / EIA"
    )
    ax.set_title(
        "ResStock vs EIA load % difference (normalized by customer count) by utility multifamily share"
    )
    out_path = path_output or "kwh_pct_diff_vs_multifamily.png"
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Plot saved to {out_path}")


# Map building_type argument to substring in BUILDING_TYPE_RECS_COL for filtering
BUILDING_TYPE_FILTER: dict[str, str] = {
    "multifamily": "Multi-Family",
    "single_family": "Single-Family",
    "mobile_home": "Mobile Home",
}


def _bldg_ids_for_building_type(
    resstock_metadata: pl.DataFrame,
    building_type: str,
) -> list:
    """Return list of bldg_id values whose BUILDING_TYPE_RECS_COL matches building_type."""
    substring = BUILDING_TYPE_FILTER.get(building_type)
    filtered = resstock_metadata.filter(
        pl.col(BUILDING_TYPE_RECS_COL).str.contains(substring, literal=True)
    )
    return filtered.get_column(BLDG_ID_COL).to_list()


def _parse_floor_area_sqft(val: str | None) -> float:
    """Parse one floor area value: '4000+' -> 5500, '750-999' -> midpoint, else float.
    No Polars list.get; avoids out-of-bounds and cast errors."""
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return float("nan")
    s = str(val).strip()
    if s.endswith("+"):
        return 5500.0
    if "-" in s:
        parts = s.split("-", 1)
        if len(parts) == 2:
            try:
                lo = float(parts[0].strip())
                hi = float(parts[1].strip())
                return (lo + hi) / 2.0
            except ValueError:
                return float("nan")
    try:
        return float(s.replace("+", ""))
    except ValueError:
        return float("nan")


def calculate_total_hvac_related_electricity_kwh(
    resstock_annual: pl.DataFrame,
    resstock_metadata: pl.DataFrame | dict[str | int, pl.DataFrame],
    building_type: str,
) -> pl.DataFrame:
    """Filter by building type, sum HVAC-related electricity columns per building.

    Returns bldg_id, total_hvac_kwh, and total_hvac_kwh_by_floor_area (total_hvac_kwh
    divided by floor area). Floor area values like '750-999' are parsed as the midpoint.
    resstock_metadata may be a single DataFrame or a dict of utility_code -> DataFrame
    (e.g. metadata_by_utility); if dict, all values are concatenated.
    """
    if isinstance(resstock_metadata, dict):
        resstock_metadata = pl.concat(resstock_metadata.values())
    bldg_ids = _bldg_ids_for_building_type(resstock_metadata, building_type)
    filtered = resstock_annual.filter(pl.col(BLDG_ID_COL).is_in(bldg_ids))
    cols_present = [c for c in HVAC_RELATED_ELECTRICITY_COLS if c in filtered.columns]
    if not cols_present:
        raise ValueError(
            f"No HVAC-related electricity columns found for building type: {building_type}"
        )

    floor_area_sqft = (
        resstock_metadata.filter(pl.col(BLDG_ID_COL).is_in(bldg_ids))
        .with_columns(
            pl.col(FLOOR_AREA_COL)
            .map_batches(
                lambda s: pl.Series([_parse_floor_area_sqft(x) for x in s]),
                return_dtype=pl.Float64,
            )
            .alias("floor_area_sqft")
        )
        .select(pl.col(BLDG_ID_COL), pl.col("floor_area_sqft"))
    )
    result = filtered.select(
        pl.col(BLDG_ID_COL),
        pl.sum_horizontal([pl.col(c) for c in cols_present]).alias("total_hvac_kwh"),
    ).join(floor_area_sqft, on=BLDG_ID_COL, how="left")
    return result.with_columns(
        (pl.col("total_hvac_kwh") / pl.col("floor_area_sqft")).alias(
            "total_hvac_kwh_by_floor_area"
        )
    ).select(
        pl.col(BLDG_ID_COL),
        pl.col("total_hvac_kwh"),
        pl.col("total_hvac_kwh_by_floor_area"),
    )


def calculate_total_non_hvac_related_electricity_kwh(
    resstock_annual: pl.DataFrame,
    resstock_metadata: pl.DataFrame | dict[str | int, pl.DataFrame],
    building_type: str,
) -> pl.DataFrame:
    """Filter by building type, sum non-HVAC electricity columns per building.

    Returns bldg_id, total_non_hvac_kwh, and total_non_hvac_kwh_by_floor_area
    (total_non_hvac_kwh divided by floor area). Floor area values like '750-999'
    are parsed as the midpoint.
    resstock_metadata may be a single DataFrame or a dict of utility_code -> DataFrame;
    if dict, all values are concatenated.
    """
    if isinstance(resstock_metadata, dict):
        resstock_metadata = pl.concat(resstock_metadata.values())
    bldg_ids = _bldg_ids_for_building_type(resstock_metadata, building_type)
    filtered = resstock_annual.filter(pl.col(BLDG_ID_COL).is_in(bldg_ids))
    cols_present = [
        c for c in NON_HVAC_RELATED_ELECTRICITY_COLS if c in filtered.columns
    ]
    if not cols_present:
        raise ValueError(
            f"No non-HVAC-related electricity columns found for building type: {building_type}"
        )

    meta_subset = (
        resstock_metadata.filter(pl.col(BLDG_ID_COL).is_in(bldg_ids))
        .with_columns(
            pl.col(FLOOR_AREA_COL)
            .map_batches(
                lambda s: pl.Series([_parse_floor_area_sqft(x) for x in s]),
                return_dtype=pl.Float64,
            )
            .alias("floor_area_sqft")
        )
        .select(pl.col(BLDG_ID_COL), pl.col("floor_area_sqft"))
    )
    result = filtered.select(
        pl.col(BLDG_ID_COL),
        pl.sum_horizontal([pl.col(c) for c in cols_present]).alias(
            "total_non_hvac_kwh"
        ),
    ).join(meta_subset, on=BLDG_ID_COL, how="left")
    return result.with_columns(
        (pl.col("total_non_hvac_kwh") / pl.col("floor_area_sqft")).alias(
            "total_non_hvac_kwh_by_floor_area"
        )
    ).select(
        pl.col(BLDG_ID_COL),
        pl.col("total_non_hvac_kwh"),
        pl.col("total_non_hvac_kwh_by_floor_area"),
    )


def two_sample_difference_of_means_test(
    group1: np.ndarray | pl.Series,
    group2: np.ndarray | pl.Series,
) -> dict[str, float | int]:
    """Welch's t-test for difference of two independent means (unequal variances).

    Returns dict with mean1, mean2, diff (mean1 - mean2), std1, std2, n1, n2,
    t_stat, welch_df, p_value (two-tailed). Use p_value < 0.05 for significance.
    """
    a1 = np.asarray(group1, dtype=np.float64)
    a2 = np.asarray(group2, dtype=np.float64)
    a1 = a1[~(np.isnan(a1) | np.isinf(a1))]
    a2 = a2[~(np.isnan(a2) | np.isinf(a2))]
    n1, n2 = len(a1), len(a2)
    if n1 < 2 or n2 < 2:
        return {
            "mean1": float(np.mean(a1)) if n1 else float("nan"),
            "mean2": float(np.mean(a2)) if n2 else float("nan"),
            "diff": float("nan"),
            "std1": float(np.std(a1, ddof=1)) if n1 > 1 else 0.0,
            "std2": float(np.std(a2, ddof=1)) if n2 > 1 else 0.0,
            "n1": n1,
            "n2": n2,
            "t_stat": float("nan"),
            "welch_df": float("nan"),
            "p_value": 1.0,
        }
    mean1 = float(np.mean(a1))
    mean2 = float(np.mean(a2))
    std1 = float(np.std(a1, ddof=1))
    std2 = float(np.std(a2, ddof=1))
    se1_sq = (std1**2) / n1
    se2_sq = (std2**2) / n2
    se_diff = math.sqrt(se1_sq + se2_sq)
    t_stat = (mean1 - mean2) / se_diff if se_diff > 0 else 0.0
    # Welch–Satterthwaite df
    num = (se1_sq + se2_sq) ** 2
    denom = (se1_sq**2 / (n1 - 1)) + (se2_sq**2 / (n2 - 1))
    welch_df = num / denom if denom > 0 else 0.0
    p_value = float(2 * scipy_stats.t.sf(abs(t_stat), welch_df))
    return {
        "mean1": mean1,
        "mean2": mean2,
        "diff": mean1 - mean2,
        "std1": std1,
        "std2": std2,
        "n1": n1,
        "n2": n2,
        "t_stat": t_stat,
        "welch_df": welch_df,
        "p_value": p_value,
    }


def load_data(
    path_annual: str,
    path_utility_assignment: str,
    path_metadata: str,
    path_eia861: str,
    storage_options: dict[str, str] | None = None,
    load_column: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, dict[str, pl.DataFrame], pl.DataFrame]:
    """Load ResStock annual by utility, metadata by utility, and EIA by utility.

    Returns:
        resstock_annual_by_utility: one row per utility (resstock_total_kwh, resstock_customers, n_bldgs, mean_annual_kwh_per_bldg).
        metadata_with_utility: full metadata with utility_code column.
        metadata_by_utility: dict of utility_code -> metadata DataFrame for that utility.
        eia_by_utility: one row per utility (utility_code, residential_sales_mwh, eia_residential_kwh, eia_residential_customers).
    """
    resstock_annual_by_utility = load_resstock_annual_by_utility(
        path_annual,
        path_utility_assignment,
        storage_options=storage_options,
        load_column=load_column,
    )
    resstock_annual = pl.scan_parquet(path_annual, storage_options=storage_options).collect()
    metadata_with_utility, metadata_by_utility = load_metadata_by_utility(
        path_metadata,
        path_utility_assignment,
        storage_options=storage_options,
    )
    utility_codes = resstock_annual_by_utility["utility_code"].unique().to_list()
    eia = load_eia_by_utility(
        path_eia861, storage_options=storage_options, utility_codes=utility_codes
    )
    return resstock_annual_by_utility, resstock_annual, metadata_with_utility, metadata_by_utility, eia


if __name__ == "__main__":
    state = "NY"
    resstock_release = DEFAULT_RESSTOCK_RELEASE
    upgrade = "00"
    eia_year = 2018
    load_column = None

    path_annual = _default_annual_path(resstock_release, state, upgrade)
    path_utility_assignment = _default_utility_assignment_path(resstock_release, state)
    path_metadata = _default_metadata_path(resstock_release, state, upgrade)
    path_eia861 = _default_eia861_path(state, eia_year)

    opts = (
        _storage_options()
        if (
            _is_s3(path_annual)
            or _is_s3(path_utility_assignment)
            or _is_s3(path_metadata)
            or _is_s3(path_eia861)
        )
        else None
    )

    resstock_annual_by_utility, resstock_annual, metadata_with_utility, metadata_by_utility, eia = load_data(
        path_annual=path_annual,
        path_utility_assignment=path_utility_assignment,
        path_metadata=path_metadata,
        path_eia861=path_eia861,
        storage_options=opts,
        load_column=load_column,
    )

    # Overall comparison. Perecent difference normalized by EIA residential customers.
    comparison = compare_resstock_eia_by_utility(resstock_annual_by_utility, eia)

    # Plot per-utility percent difference vs multifamily share and some stats.
    building_type_shares = building_type_share_by_utility(metadata_by_utility)
    multifamily_fit_result = fit_kwh_pct_diff_vs_multifamily_pct(comparison, building_type_shares)
    plot_kwh_pct_diff_vs_multifamily_pct(
        comparison, building_type_shares, multifamily_fit_result
    )

    # Two-predictor fit (multifamily + mobile home) and comparison to single-predictor.
    mobile_multifamily_fit_result = fit_kwh_pct_diff_vs_multifamily_and_mobile_pct(
        comparison, building_type_shares, fit_single_predictor=multifamily_fit_result
    )
    print("\nLinear fit: kwh_pct_diff ~ multifamily_pct + mobile_home_pct")
    print(f"  R² = {mobile_multifamily_fit_result['r_squared']:.4f}, AIC = {mobile_multifamily_fit_result['aic']:.2f}, BIC = {mobile_multifamily_fit_result['bic']:.2f}")
    print(f"  slope_multifamily = {mobile_multifamily_fit_result['slope_multifamily']:.4f} (p = {mobile_multifamily_fit_result['slope_multifamily_pvalue']:.4f})")
    print(f"  slope_mobile_home = {mobile_multifamily_fit_result['slope_mobile_home']:.4f} (p = {mobile_multifamily_fit_result['slope_mobile_home_pvalue']:.4f})")
    print("  Comparison to single-predictor (multifamily only):")
    print(f"    ΔR² = {mobile_multifamily_fit_result['delta_r_squared']:.4f}")
    print(f"    Partial F (adding mobile_home) = {mobile_multifamily_fit_result['partial_f_stat']:.4f}, p = {mobile_multifamily_fit_result['partial_f_pvalue']:.4f}")
    print(f"    Single-predictor AIC/BIC: {mobile_multifamily_fit_result['aic_single_predictor']:.2f} / {mobile_multifamily_fit_result['bic_single_predictor']:.2f}")
    p_partial = mobile_multifamily_fit_result["partial_f_pvalue"]
    if p_partial < 0.05:
        print(f"  Adding mobile_home_pct is statistically significant (p = {p_partial:.4f} < 0.05).")
    else:
        print(f"  Adding mobile_home_pct is not statistically significant (p = {p_partial:.4f} >= 0.05).")

    # Calculate total non-HVAC-related electricity kWh for multifamily and single-family buildings.
    multifamily_non_hvac_related_electricity_kwh = calculate_total_non_hvac_related_electricity_kwh(
        resstock_annual, metadata_with_utility, "multifamily"
    )
    single_family_non_hvac_related_electricity_kwh = calculate_total_non_hvac_related_electricity_kwh(
        resstock_annual, metadata_with_utility, "single_family"
    )

    # Difference-of-means test: total_non_hvac_kwh_by_floor_area (multifamily vs single-family)
    mf_vals = multifamily_non_hvac_related_electricity_kwh.filter(
        pl.col("total_non_hvac_kwh_by_floor_area").is_finite()
    ).get_column("total_non_hvac_kwh_by_floor_area")
    sf_vals = single_family_non_hvac_related_electricity_kwh.filter(
        pl.col("total_non_hvac_kwh_by_floor_area").is_finite()
    ).get_column("total_non_hvac_kwh_by_floor_area")
    diff_test = two_sample_difference_of_means_test(mf_vals, sf_vals)
    print("\nDifference-of-means test: total_non_hvac_kwh_by_floor_area (multifamily vs single-family)")
    print(f"  Multifamily:   n={diff_test['n1']}, mean={diff_test['mean1']:.2f}, std={diff_test['std1']:.2f}")
    print(f"  Single-family: n={diff_test['n2']}, mean={diff_test['mean2']:.2f}, std={diff_test['std2']:.2f}")
    print(f"  Difference (MF − SF) = {diff_test['diff']:.2f}")
    print(f"  Difference (MF − SF) as % of SF = {diff_test['diff'] / diff_test['mean2'] * 100:.2f}%")
    print(f"  Welch t = {diff_test['t_stat']:.4f}, df ≈ {diff_test['welch_df']:.1f}, p = {diff_test['p_value']:.4f}")
    if diff_test["p_value"] < 0.05:
        print("  The difference is statistically significant (p < 0.05).")
    else:
        print("  The difference is not statistically significant (p >= 0.05).")


    # Normalize non-HVAC related electricity consumption by floor area for multifamily and recalculate the difference with eia.
    MF_to_SF_non_hvac_ratio = diff_test['mean1'] / diff_test['mean2']



    """print("\nResStock annual load results by utility:")
    print(resstock_annual)
    print("\nEIA-861 by utility:")
    print(eia)
    print("\nComparison (ResStock vs EIA by utility):")
    print(comparison)
    print(
        f"\nMetadata: {metadata_with_utility.shape[0]} rows, {metadata_with_utility.shape[1]} columns; utilities: {list(metadata_by_utility.keys())}"
    )"""
