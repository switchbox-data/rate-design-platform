"""Apply Maryland FY26 OHEP MEAP and EUSP benefits to master bills.

The source schedules are encoded in ``utils/post/data/md_ohep_benefits.yaml``
from:

* ``context/sources/FY26-MEAP-Benefit-Matrix.md``
* ``context/sources/FY26-EUSP-Benefit-Matrix.md``

Modeled OHEP Poverty Levels 1-5 are assigned from household income as a
percentage of the Federal Poverty Level. Levels 6 and 7 are intentionally not
assigned because ResStock lacks the required housing/metering and categorical
enrollment fields. MEAP is keyed by level and primary heating fuel. EUSP is
keyed by level, primary heating fuel, and annual electric kWh.

Annual grants are allocated to months proportionally to each month's bill for
the relevant fuel (not in equal 12ths). This ensures the full annual grant is
consumed up to the annual bill total, matching real-world account crediting
where excess credit carries forward. EUSP is applied to the electric bill. MEAP
is applied to the bill for the primary heating fuel (electric, gas, oil, or
propane). The Annual row is rebuilt as the sum of the twelve discounted monthly
rows.

This implements current OHEP benefits only. It does not implement the
forthcoming Maryland Low-Income Mechanism (LIM).
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
import warnings
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv

from utils.file_io import get_aws_storage_options
from utils.post.lmi_common import (
    assign_md_eusp_kwh_band_expr,
    assign_md_ohep_level_expr,
    fpl_pct_expr,
    fpl_threshold_expr,
    get_md_eusp_benefits_df,
    get_md_meap_benefits_df,
    inflate_income_expr,
    load_cpi_ratio,
    load_fpl_guidelines,
    load_md_ohep_config,
    parse_occupants_expr,
    participation_uniform_expr,
    select_participants_weighted,
)

ANNUAL_MONTH = "Annual"
BLDG_ID = "bldg_id"
ANNUAL_ELECTRIC_KWH = "out.electricity.total.energy_consumption.kwh"

SHARED_OUTPUT_COLS = [
    "ohep_poverty_level",
    "primary_heating_fuel",
    "annual_electric_kwh",
    "eusp_kwh_band",
    "elec_lmi_tier",
    "gas_lmi_tier",
    "oil_lmi_tier",
    "propane_lmi_tier",
    "is_lmi_elec",
    "is_lmi_gas",
    "is_lmi_oil",
    "is_lmi_propane",
    "is_lmi_any",
    "has_unmodeled_meap_fuel",
]


def primary_heating_fuel_expr() -> pl.Expr:
    """Map ResStock heating fields to the canonical OHEP fuel keys.

    Heat-pump homes are electric-heated even though ``in.heating_fuel`` retains
    the pre-retrofit fuel in upgrade 02. ``Other Fuel`` is the available
    ResStock proxy for the OHEP matrix's Wood/Coal category.
    """
    return (
        pl.when(pl.col("postprocess_group.has_hp").fill_null(False))
        .then(pl.lit("electric"))
        .when(pl.col("in.heating_fuel") == "Electricity")
        .then(pl.lit("electric"))
        .when(pl.col("in.heating_fuel") == "Natural Gas")
        .then(pl.lit("gas"))
        .when(pl.col("in.heating_fuel") == "Fuel Oil")
        .then(pl.lit("oil_kerosene"))
        .when(pl.col("in.heating_fuel") == "Propane")
        .then(pl.lit("propane"))
        .when(pl.col("in.heating_fuel") == "Other Fuel")
        .then(pl.lit("wood_coal"))
        .otherwise(pl.lit(None, dtype=pl.String))
    )


def _scan_parquet(path: str, opts: dict[str, str]) -> pl.LazyFrame:
    storage_options = opts if path.startswith("s3://") else None
    return pl.scan_parquet(path, storage_options=storage_options)


def _annual_load_path(path_resstock_release: str, state: str, upgrade: str) -> str:
    base = path_resstock_release.rstrip("/")
    return (
        f"{base}/load_curve_annual/state={state}/upgrade={upgrade}/"
        f"{state}_upgrade{upgrade}_metadata_and_annual_results.parquet"
    )


def _build_md_ohep_profiles(
    master_bldg_ids: pl.DataFrame,
    *,
    state: str,
    upgrade: str,
    path_resstock_release: str,
    fpl_year: int,
    cpi_ratio: float,
    opts: dict[str, str],
    config: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Build one OHEP eligibility and scheduled-benefit row per master building."""
    if config is None:
        config = load_md_ohep_config()
    fpl = load_fpl_guidelines(fpl_year)
    base = path_resstock_release.rstrip("/")
    path_metadata = (
        f"{base}/metadata/state={state}/upgrade={upgrade}/metadata-sb.parquet"
    )
    path_annual_load = _annual_load_path(base, state, upgrade)
    ids = master_bldg_ids.select(BLDG_ID).unique().lazy()

    metadata = (
        _scan_parquet(path_metadata, opts)
        .join(ids, on=BLDG_ID, how="inner")
        .select(
            BLDG_ID,
            "in.occupants",
            "in.representative_income",
            "in.vacancy_status",
            "in.heating_fuel",
            "postprocess_group.has_hp",
        )
    )
    annual_load = (
        _scan_parquet(path_annual_load, opts)
        .join(ids, on=BLDG_ID, how="inner")
        .select(
            BLDG_ID,
            pl.col(ANNUAL_ELECTRIC_KWH).alias("annual_electric_kwh"),
        )
    )
    profiles = (
        metadata.join(annual_load, on=BLDG_ID, how="inner")
        .filter(pl.col("in.vacancy_status") != "Vacant")
        .with_columns(
            parse_occupants_expr("in.occupants").alias("_occupants"),
            inflate_income_expr("in.representative_income", cpi_ratio).alias(
                "_income_inflated"
            ),
            primary_heating_fuel_expr().alias("primary_heating_fuel"),
        )
        .with_columns(
            fpl_threshold_expr("_occupants", fpl["base"], fpl["increment"]).alias(
                "_fpl_threshold"
            )
        )
        .with_columns(
            fpl_pct_expr("_income_inflated", pl.col("_fpl_threshold")).alias("fpl_pct")
        )
        .with_columns(
            assign_md_ohep_level_expr("fpl_pct", config).alias("ohep_poverty_level"),
            assign_md_eusp_kwh_band_expr("annual_electric_kwh", config).alias(
                "eusp_kwh_band"
            ),
        )
        .select(
            BLDG_ID,
            "ohep_poverty_level",
            "primary_heating_fuel",
            "annual_electric_kwh",
            "eusp_kwh_band",
            "fpl_pct",
        )
        .collect()
    )

    if profiles[BLDG_ID].n_unique() != profiles.height:
        raise AssertionError("MD OHEP profile contains duplicate bldg_id rows")

    profiles = (
        profiles.join(
            get_md_meap_benefits_df(config),
            on=["ohep_poverty_level", "primary_heating_fuel"],
            how="left",
        )
        .join(
            get_md_eusp_benefits_df(config),
            on=[
                "ohep_poverty_level",
                "primary_heating_fuel",
                "eusp_kwh_band",
            ],
            how="left",
        )
        .with_columns(
            pl.col("meap_annual_benefit").fill_null(0.0),
            pl.col("eusp_annual_benefit").fill_null(0.0),
        )
        .with_columns(
            pl.when(
                (pl.col("eusp_annual_benefit") > 0)
                | (
                    (pl.col("primary_heating_fuel") == "electric")
                    & (pl.col("meap_annual_benefit") > 0)
                )
            )
            .then(pl.col("ohep_poverty_level"))
            .otherwise(pl.lit(0))
            .cast(pl.Int32)
            .alias("elec_lmi_tier"),
            pl.when(
                (pl.col("primary_heating_fuel") == "gas")
                & (pl.col("meap_annual_benefit") > 0)
            )
            .then(pl.col("ohep_poverty_level"))
            .otherwise(pl.lit(0))
            .cast(pl.Int32)
            .alias("gas_lmi_tier"),
            pl.when(
                (pl.col("primary_heating_fuel") == "oil_kerosene")
                & (pl.col("meap_annual_benefit") > 0)
            )
            .then(pl.col("ohep_poverty_level"))
            .otherwise(pl.lit(0))
            .cast(pl.Int32)
            .alias("oil_lmi_tier"),
            pl.when(
                (pl.col("primary_heating_fuel") == "propane")
                & (pl.col("meap_annual_benefit") > 0)
            )
            .then(pl.col("ohep_poverty_level"))
            .otherwise(pl.lit(0))
            .cast(pl.Int32)
            .alias("propane_lmi_tier"),
            (
                (pl.col("primary_heating_fuel") == "wood_coal")
                & (pl.col("meap_annual_benefit") > 0)
            ).alias("has_unmodeled_meap_fuel"),
        )
        .with_columns(
            (pl.col("elec_lmi_tier") > 0).alias("is_lmi_elec"),
            (pl.col("gas_lmi_tier") > 0).alias("is_lmi_gas"),
            (pl.col("oil_lmi_tier") > 0).alias("is_lmi_oil"),
            (pl.col("propane_lmi_tier") > 0).alias("is_lmi_propane"),
        )
        .with_columns(
            (
                pl.col("is_lmi_elec")
                | pl.col("is_lmi_gas")
                | pl.col("is_lmi_oil")
                | pl.col("is_lmi_propane")
            ).alias("is_lmi_any")
        )
    )
    return profiles


def _sample_md_participation(
    profiles: pl.DataFrame,
    participation_rate: float,
    participation_mode: str,
    seed: int,
) -> pl.DataFrame:
    """Add a deterministic participation flag to one-row-per-building profiles."""
    if participation_mode not in {"uniform", "weighted"}:
        raise ValueError(
            f"participation_mode must be 'uniform' or 'weighted'; got {participation_mode!r}"
        )
    eligible = pl.col("is_lmi_any")
    if participation_rate >= 1.0:
        return profiles.with_columns(eligible.alias("participates"))
    if participation_mode == "uniform":
        return profiles.with_columns(
            participation_uniform_expr(
                BLDG_ID, participation_rate, seed, eligible
            ).alias("participates")
        )

    eligible_df = (
        profiles.filter(eligible)
        .with_columns((1.0 / pl.col("fpl_pct").clip(lower_bound=1.0)).alias("_weight"))
        .select(BLDG_ID, "_weight")
    )
    if eligible_df.is_empty():
        return profiles.with_columns(pl.lit(False).alias("participates"))
    sampled = select_participants_weighted(
        eligible_df, participation_rate, seed, "_weight", BLDG_ID
    )
    return profiles.join(sampled, on=BLDG_ID, how="left").with_columns(
        pl.col("participates").fill_null(False)
    )


def _enrich_master_with_profiles(
    master: pl.DataFrame, profiles: pl.DataFrame
) -> pl.DataFrame:
    n_rows = master.height
    joined = master.join(profiles, on=BLDG_ID, how="left")
    if joined.height != n_rows:
        raise AssertionError(
            f"MD OHEP profile join changed row count: {n_rows} -> {joined.height}"
        )
    return joined.with_columns(
        pl.col("ohep_poverty_level").fill_null(0).cast(pl.Int32),
        pl.col("eusp_kwh_band").fill_null(0).cast(pl.Int32),
        pl.col("elec_lmi_tier").fill_null(0).cast(pl.Int32),
        pl.col("gas_lmi_tier").fill_null(0).cast(pl.Int32),
        pl.col("oil_lmi_tier").fill_null(0).cast(pl.Int32),
        pl.col("propane_lmi_tier").fill_null(0).cast(pl.Int32),
        pl.col("is_lmi_elec").fill_null(False),
        pl.col("is_lmi_gas").fill_null(False),
        pl.col("is_lmi_oil").fill_null(False),
        pl.col("is_lmi_propane").fill_null(False),
        pl.col("is_lmi_any").fill_null(False),
        pl.col("has_unmodeled_meap_fuel").fill_null(False),
        pl.col("meap_annual_benefit").fill_null(0.0),
        pl.col("eusp_annual_benefit").fill_null(0.0),
        pl.col("participates").fill_null(False),
    )


def _apply_md_ohep_benefits(
    enriched: pl.DataFrame,
    pct_label: int,
    *,
    keep_component_columns: bool,
    include_meap: bool = True,
    include_eusp: bool = True,
) -> pl.DataFrame:
    """Apply one participation scenario using proportional monthly allocation.

    Annual grants are distributed to months proportionally to each month's bill
    (for the relevant fuel). This ensures the full grant is consumed up to the
    annual bill total without any "lost credit" from floor-clipping that would
    occur with equal-12ths allocation.

    For each fuel:
        fraction_remaining = max(0, 1 - annual_credit / annual_bill)
        discounted_month_bill = month_bill × fraction_remaining

    If the annual bill is zero, the discounted bill is also zero (no credit to
    apply). If the annual credit exceeds the annual bill, every month goes to
    zero (maximum possible relief).
    """
    suffix = str(pct_label)
    base_cols = {
        "elec": "elec_total_bill",
        "gas": "gas_total_bill",
        "oil": "oil_total_bill",
        "propane": "propane_total_bill",
    }
    lmi_cols = {fuel: f"{fuel}_total_bill_lmi_{suffix}" for fuel in base_cols}

    meap_credit_expr = (
        pl.when(pl.col("participates"))
        .then(pl.col("meap_annual_benefit") if include_meap else pl.lit(0.0))
        .otherwise(0.0)
    )
    eusp_credit_expr = (
        pl.when(pl.col("participates"))
        .then(pl.col("eusp_annual_benefit") if include_eusp else pl.lit(0.0))
        .otherwise(0.0)
    )

    fuel_annual_credit_exprs = {
        "elec": eusp_credit_expr
        + pl.when(pl.col("primary_heating_fuel") == "electric")
        .then(meap_credit_expr)
        .otherwise(0.0),
        "gas": pl.when(pl.col("primary_heating_fuel") == "gas")
        .then(meap_credit_expr)
        .otherwise(0.0),
        "oil": pl.when(pl.col("primary_heating_fuel") == "oil_kerosene")
        .then(meap_credit_expr)
        .otherwise(0.0),
        "propane": pl.when(pl.col("primary_heating_fuel") == "propane")
        .then(meap_credit_expr)
        .otherwise(0.0),
    }

    # Compute fraction_remaining from the sum of monthly bills, not the Annual
    # row. The discounted Annual row is rebuilt from those months, so the
    # denominator has to be the same total we actually scale.
    annual_fractions = (
        enriched.group_by(BLDG_ID)
        .agg(
            pl.col("participates").first(),
            pl.col("primary_heating_fuel").first(),
            pl.col("meap_annual_benefit").first(),
            pl.col("eusp_annual_benefit").first(),
            *[
                pl.col(base_cols[fuel])
                .filter(pl.col("month") != ANNUAL_MONTH)
                .sum()
                .alias(base_cols[fuel])
                for fuel in base_cols
            ],
        )
        .with_columns(
            *[
                fuel_annual_credit_exprs[fuel].alias(f"_credit_{fuel}")
                for fuel in base_cols
            ]
        )
        .select(
            BLDG_ID,
            *[
                pl.when(pl.col(base_cols[fuel]) > 0)
                .then(
                    (1.0 - pl.col(f"_credit_{fuel}") / pl.col(base_cols[fuel])).clip(
                        lower_bound=0.0
                    )
                )
                .otherwise(
                    pl.when(pl.col(f"_credit_{fuel}") > 0)
                    .then(pl.lit(0.0))
                    .otherwise(pl.lit(1.0))
                )
                .alias(f"_frac_{fuel}")
                for fuel in base_cols
            ],
        )
    )

    result = enriched.join(annual_fractions, on=BLDG_ID, how="left")
    if result.height != enriched.height:
        raise AssertionError(
            "MD OHEP fraction join changed row count: "
            f"{enriched.height} -> {result.height}"
        )
    result = result.with_columns(
        *[pl.col(f"_frac_{fuel}").fill_null(1.0) for fuel in base_cols]
    )

    # Monthly rows: discounted = original × fraction_remaining
    # Annual row: placeholder (rebuilt below from monthly sum)
    result = result.with_columns(
        *[
            pl.when(pl.col("month") != ANNUAL_MONTH)
            .then(pl.col(base_cols[fuel]) * pl.col(f"_frac_{fuel}"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias(lmi_cols[fuel])
            for fuel in base_cols
        ],
        (
            pl.col("participates")
            & (
                (pl.lit(include_eusp) & (pl.col("eusp_annual_benefit") > 0))
                | (
                    pl.lit(include_meap)
                    & (pl.col("primary_heating_fuel") == "electric")
                    & (pl.col("meap_annual_benefit") > 0)
                )
            )
        ).alias(f"applied_discount_elec_{suffix}"),
        (
            pl.col("participates")
            & pl.lit(include_meap)
            & (pl.col("primary_heating_fuel") == "gas")
            & (pl.col("meap_annual_benefit") > 0)
        ).alias(f"applied_discount_gas_{suffix}"),
        (
            pl.col("participates")
            & pl.lit(include_meap)
            & (pl.col("primary_heating_fuel") == "oil_kerosene")
            & (pl.col("meap_annual_benefit") > 0)
        ).alias(f"applied_discount_oil_{suffix}"),
        (
            pl.col("participates")
            & pl.lit(include_meap)
            & (pl.col("primary_heating_fuel") == "propane")
            & (pl.col("meap_annual_benefit") > 0)
        ).alias(f"applied_discount_propane_{suffix}"),
    )

    # Rebuild Annual row as sum of discounted monthly rows
    sum_aliases = {fuel: f"_annual_{fuel}_lmi_{suffix}" for fuel in base_cols}
    monthly_sums = (
        result.filter(pl.col("month") != ANNUAL_MONTH)
        .group_by(BLDG_ID)
        .agg(
            *[
                pl.col(lmi_cols[fuel]).sum().alias(sum_aliases[fuel])
                for fuel in base_cols
            ]
        )
    )
    n_before_sum_join = result.height
    result = result.join(monthly_sums, on=BLDG_ID, how="left")
    if result.height != n_before_sum_join:
        raise AssertionError(
            "MD OHEP annual-sum join changed row count: "
            f"{n_before_sum_join} -> {result.height}"
        )
    result = result.with_columns(
        *[
            pl.when(pl.col("month") == ANNUAL_MONTH)
            .then(pl.col(sum_aliases[fuel]))
            .otherwise(pl.col(lmi_cols[fuel]))
            .alias(lmi_cols[fuel])
            for fuel in base_cols
        ]
    ).with_columns(
        pl.sum_horizontal([pl.col(lmi_cols[fuel]) for fuel in base_cols]).alias(
            f"energy_total_bill_lmi_{suffix}"
        )
    )

    if keep_component_columns:
        result = result.with_columns(
            meap_credit_expr.alias(f"meap_annual_credit_{suffix}"),
            eusp_credit_expr.alias(f"eusp_annual_credit_{suffix}"),
        )

    drop_cols = [
        *sum_aliases.values(),
        *[f"_frac_{fuel}" for fuel in base_cols],
    ]
    return result.drop(*drop_cols)


def _validate_md_ohep(
    df: pl.DataFrame,
    pct_label: int,
    participation_rate: float,
    *,
    include_eusp: bool = True,
) -> None:
    suffix = str(pct_label)
    fuels = ("elec", "gas", "oil", "propane")
    bill_pairs = [
        (f"{fuel}_total_bill", f"{fuel}_total_bill_lmi_{suffix}") for fuel in fuels
    ]
    for base_col, lmi_col in bill_pairs:
        if df[lmi_col].null_count() > 0:
            raise AssertionError(f"{lmi_col} contains nulls")
        if df.filter(pl.col(lmi_col) < -1e-6).height:
            raise AssertionError(f"{lmi_col} contains negative bills")
        if (
            df.filter(pl.col("month") == ANNUAL_MONTH)
            .filter(pl.col(lmi_col) > pl.col(base_col) + 1e-6)
            .height
        ):
            raise AssertionError(f"{lmi_col} exceeds {base_col} on Annual rows")

    monthly_sums = (
        df.filter(pl.col("month") != ANNUAL_MONTH)
        .group_by(BLDG_ID)
        .agg(
            *[
                pl.col(lmi_col).sum().alias(f"_check_{lmi_col}")
                for _, lmi_col in bill_pairs
            ]
        )
    )
    annual = df.filter(pl.col("month") == ANNUAL_MONTH).join(
        monthly_sums, on=BLDG_ID, how="left"
    )
    for _, lmi_col in bill_pairs:
        max_diff = (
            annual.select(
                (pl.col(lmi_col) - pl.col(f"_check_{lmi_col}")).abs().max()
            ).item()
            or 0.0
        )
        if float(max_diff) > 1e-6:
            raise AssertionError(
                f"{lmi_col} Annual row differs from monthly sum by {max_diff}"
            )

    energy_col = f"energy_total_bill_lmi_{suffix}"
    energy_diff = df.select(
        (
            pl.col(energy_col)
            - pl.sum_horizontal(
                [pl.col(f"{fuel}_total_bill_lmi_{suffix}") for fuel in fuels]
            )
        )
        .abs()
        .max()
    ).item()
    if energy_diff is not None and float(energy_diff) > 1e-6:
        raise AssertionError(f"{energy_col} does not equal discounted fuel bills")

    if participation_rate >= 1.0 and include_eusp:
        mismatches = annual.filter(
            pl.col(f"applied_discount_elec_{suffix}") != pl.col("is_lmi_elec")
        ).height
        if mismatches:
            raise AssertionError(
                f"At 100% participation, electric applied flag mismatches "
                f"eligibility for {mismatches} buildings"
            )


def apply_md_ohep_to_master(
    master: pl.DataFrame,
    *,
    state: str,
    upgrade: str,
    path_resstock_release: str,
    fpl_year: int,
    cpi_s3_path: str,
    participation_rates: list[float],
    participation_mode: str,
    seed: int,
    opts: dict[str, str],
    keep_component_columns: bool = False,
    include_meap: bool = True,
    include_eusp: bool = True,
) -> pl.DataFrame:
    """Append MD OHEP benefit and net LMI bill columns to master bills."""
    config = load_md_ohep_config()
    configured_fpl_year = int(config["fpl_guideline_year"])
    if fpl_year != configured_fpl_year:
        warnings.warn(
            f"OHEP config specifies fpl_guideline_year={configured_fpl_year} "
            f"but --fpl-year={fpl_year} was passed. Using {fpl_year} for income "
            f"inflation. This is fine for sensitivity analysis but may misalign "
            f"FPL thresholds with the intended program year.",
            stacklevel=2,
        )
    cpi_ratio = load_cpi_ratio(cpi_s3_path, fpl_year, opts)
    master_ids = master.select(BLDG_ID).unique()
    raw_profiles = _build_md_ohep_profiles(
        master_ids,
        state=state.upper(),
        upgrade=upgrade,
        path_resstock_release=path_resstock_release,
        fpl_year=fpl_year,
        cpi_ratio=cpi_ratio,
        opts=opts,
        config=config,
    )

    result = master
    for rate_index, rate in enumerate(participation_rates):
        if not 0.0 <= rate <= 1.0:
            raise ValueError(f"participation rate must be between 0 and 1; got {rate}")
        pct_label = round(rate * 100)
        sampled = _sample_md_participation(raw_profiles, rate, participation_mode, seed)
        if rate_index == 0:
            result = _enrich_master_with_profiles(result, sampled)
        else:
            result = result.join(
                sampled.select(BLDG_ID, "participates"),
                on=BLDG_ID,
                how="left",
            ).with_columns(pl.col("participates").fill_null(False))
        result = _apply_md_ohep_benefits(
            result,
            pct_label,
            keep_component_columns=keep_component_columns,
            include_meap=include_meap,
            include_eusp=include_eusp,
        )
        _validate_md_ohep(result, pct_label, rate, include_eusp=include_eusp)
        result = result.drop("participates")

    if not keep_component_columns:
        result = result.drop("meap_annual_benefit", "eusp_annual_benefit", "fpl_pct")
    return result


def _infer_upgrade(master: pl.DataFrame, explicit_upgrade: str | None) -> str:
    if explicit_upgrade is not None:
        return explicit_upgrade.zfill(2)
    if "upgrade" not in master.columns:
        raise ValueError(
            "--upgrade is required when master bills lack an upgrade column"
        )
    upgrades = master["upgrade"].drop_nulls().unique().to_list()
    if len(upgrades) != 1:
        raise ValueError(
            f"Cannot infer one ResStock upgrade from master bills: {upgrades}"
        )
    return str(int(upgrades[0])).zfill(2)


def _write_hive_partitioned(
    df: pl.DataFrame,
    output_path: str,
    partition_col: str = "sb.electric_utility",
) -> None:
    tmp_dir = Path(tempfile.mkdtemp(prefix="md_ohep_master_"))
    try:
        for key, utility_df in df.group_by(partition_col):
            partition_dir = tmp_dir / f"{partition_col}={key[0]}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            utility_df.drop(partition_col).write_parquet(partition_dir / "data.parquet")
        if output_path.startswith("s3://"):
            subprocess.run(
                ["aws", "s3", "sync", str(tmp_dir), output_path],
                check=True,
                capture_output=True,
            )
        else:
            destination = Path(output_path)
            destination.mkdir(parents=True, exist_ok=True)
            shutil.copytree(tmp_dir, destination, dirs_exist_ok=True)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _rate_specific_columns(pct_label: int) -> list[str]:
    suffix = str(pct_label)
    return [
        *[
            f"{fuel}_total_bill_lmi_{suffix}"
            for fuel in ("elec", "gas", "oil", "propane", "energy")
        ],
        *[
            f"applied_discount_{fuel}_{suffix}"
            for fuel in ("elec", "gas", "oil", "propane")
        ],
        f"meap_annual_credit_{suffix}",
        f"eusp_annual_credit_{suffix}",
    ]


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Apply Maryland FY26 OHEP MEAP/EUSP benefits to master bills."
    )
    parser.add_argument(
        "--master-bills-path",
        required=True,
        help="Path to Hive-partitioned comb_bills_year_target parquet",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Output path; defaults to an in-place update of master bills",
    )
    parser.add_argument("--state", default="MD")
    parser.add_argument(
        "--upgrade",
        default=None,
        help="ResStock upgrade (00 or 02); inferred from master upgrade column",
    )
    parser.add_argument(
        "--path-resstock-release",
        default="s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb",
    )
    parser.add_argument("--fpl-year", type=int, default=2025)
    parser.add_argument("--cpi-s3-path", required=True)
    parser.add_argument("--participation-rate", type=float, default=1.0)
    parser.add_argument(
        "--participation-mode",
        choices=["uniform", "weighted"],
        default="weighted",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--keep-component-columns",
        action="store_true",
        help="Retain rate-specific MEAP/EUSP scheduled-credit columns",
    )
    parser.add_argument(
        "--exclude-meap",
        action="store_false",
        dest="include_meap",
        help="Do not apply MEAP; useful for component sensitivity runs",
    )
    parser.add_argument(
        "--exclude-eusp",
        action="store_false",
        dest="include_eusp",
        help="Do not apply EUSP; useful for component sensitivity runs",
    )
    args = parser.parse_args()

    opts = get_aws_storage_options()
    storage_options = opts if args.master_bills_path.startswith("s3://") else None
    master = pl.scan_parquet(
        args.master_bills_path,
        hive_partitioning=True,
        storage_options=storage_options,
    ).collect()
    upgrade = _infer_upgrade(master, args.upgrade)
    pct_label = round(args.participation_rate * 100)

    drop_cols = [
        *SHARED_OUTPUT_COLS,
        "meap_annual_benefit",
        "eusp_annual_benefit",
        "fpl_pct",
        *_rate_specific_columns(pct_label),
    ]
    existing = [column for column in drop_cols if column in master.columns]
    if existing:
        master = master.drop(existing)

    result = apply_md_ohep_to_master(
        master,
        state=args.state,
        upgrade=upgrade,
        path_resstock_release=args.path_resstock_release,
        fpl_year=args.fpl_year,
        cpi_s3_path=args.cpi_s3_path,
        participation_rates=[args.participation_rate],
        participation_mode=args.participation_mode,
        seed=args.seed,
        opts=opts,
        keep_component_columns=args.keep_component_columns,
        include_meap=args.include_meap,
        include_eusp=args.include_eusp,
    )
    output_path = args.output_path or args.master_bills_path
    _write_hive_partitioned(result, output_path)
    annual = result.filter(pl.col("month") == ANNUAL_MONTH)
    participants = annual.filter(pl.col(f"applied_discount_elec_{pct_label}")).height
    print(
        f"Applied MD FY26 OHEP benefits to {result.height} rows; "
        f"{participants} participating buildings; wrote {output_path}"
    )


if __name__ == "__main__":
    main()
