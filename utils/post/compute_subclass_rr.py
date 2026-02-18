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
from pathlib import Path
from typing import cast

import polars as pl
import yaml
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

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
WINTER_MONTHS = (12, 1, 2)
ELECTRIC_LOAD_COL = "out.electricity.net.energy_consumption"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCENARIO_CONFIG_PATH = (
    PROJECT_ROOT / "rate_design/ri/hp_rates/config/scenarios.yaml"
)
DEFAULT_DIFFERENTIATED_YAML_PATH = (
    PROJECT_ROOT / "rate_design/ri/hp_rates/config/rev_requirement/rie_hp_vs_nonhp.yaml"
)
DEFAULT_RIE_YAML_PATH = (
    PROJECT_ROOT / "rate_design/ri/hp_rates/config/rev_requirement/rie.yaml"
)


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


def _scan_loads_parquet(
    loads_path: S3Path | Path,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    base = str(loads_path).rstrip("/")
    parquet_glob = f"{base}/*.parquet"
    if isinstance(loads_path, S3Path):
        return pl.scan_parquet(parquet_glob, storage_options=storage_options)
    return pl.scan_parquet(parquet_glob)


def _resolve_electric_load_column(loads: pl.LazyFrame) -> str:
    schema_cols = loads.collect_schema().names()
    if ELECTRIC_LOAD_COL in schema_cols:
        return ELECTRIC_LOAD_COL
    available_preview = ", ".join(schema_cols[:10])
    raise ValueError(
        f"Required electric load column '{ELECTRIC_LOAD_COL}' not found. "
        f"Available columns (first 10): {available_preview}"
    )


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
    resstock_loads_path: S3Path | Path,
    cross_subsidy_col: str = DEFAULT_BAT_METRIC,
    storage_options: dict[str, str] | None = None,
    tariff_final_config_path: S3Path | Path | None = None,
) -> pl.DataFrame:
    """Compute HP-only seasonal discount inputs from run outputs + ResStock loads."""
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
    loads = _scan_loads_parquet(resstock_loads_path, storage_options)
    electric_load_col = _resolve_electric_load_column(loads)
    winter_kwh_hp = (
        loads.join(hp_weights.lazy(), on=BLDG_ID_COL, how="inner")
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col("timestamp")
            .cast(pl.String, strict=False)
            .str.to_datetime(strict=False)
            .alias("timestamp"),
            pl.col(electric_load_col).cast(pl.Float64).alias("demand_kwh"),
            pl.col(WEIGHT_COL).cast(pl.Float64),
        )
        .with_columns(pl.col("timestamp").dt.month().alias("month_num"))
        .filter(pl.col("month_num").is_in(WINTER_MONTHS))
        .with_columns((pl.col("demand_kwh") * pl.col(WEIGHT_COL)).alias("weighted_kwh"))
        .select(pl.col("weighted_kwh").sum().alias("winter_kwh_hp"))
        .collect()
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
    winter_rate_hp = default_rate - (total_cross_subsidy_hp / winter_kwh)

    return pl.DataFrame(
        {
            "subclass": ["true"],
            "cross_subsidy_col": [cross_subsidy_col],
            "default_rate": [default_rate],
            "total_cross_subsidy_hp": [total_cross_subsidy_hp],
            "winter_kwh_hp": [winter_kwh],
            "winter_rate_hp": [winter_rate_hp],
            "winter_months": [",".join(str(m) for m in WINTER_MONTHS)],
        }
    )


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


def _load_default_revenue_requirement(
    scenario_config_path: Path,
    run_num: int,
) -> tuple[str, float]:
    data = yaml.safe_load(scenario_config_path.read_text(encoding="utf-8")) or {}
    runs = data.get("runs", {})
    run = runs.get(run_num) or runs.get(str(run_num))
    if run is None:
        msg = f"Run {run_num} not found in scenario config: {scenario_config_path}"
        raise ValueError(msg)

    utility = str(run.get("utility", "rie"))
    revenue_requirement = float(run["utility_delivery_revenue_requirement"])
    return utility, revenue_requirement


def _write_revenue_requirement_yamls(
    breakdown: pl.DataFrame,
    run_dir: S3Path | Path,
    group_col: str,
    cross_subsidy_col: str,
    utility: str,
    default_revenue_requirement: float,
    differentiated_yaml_path: Path,
    default_yaml_path: Path,
) -> tuple[Path, Path]:
    differentiated_yaml_path.parent.mkdir(parents=True, exist_ok=True)
    default_yaml_path.parent.mkdir(parents=True, exist_ok=True)

    differentiated_data = {
        "utility": utility,
        "group_col": group_col,
        "cross_subsidy_col": cross_subsidy_col,
        "run_dir": str(run_dir),
        "subclass_revenue_requirements": {
            str(row["subclass"]): float(row["revenue_requirement"])
            for row in breakdown.to_dicts()
        },
    }
    default_data = {
        "utility": utility,
        "revenue_requirement": float(default_revenue_requirement),
        "source": "scenarios.yaml.utility_delivery_revenue_requirement",
    }

    differentiated_yaml_path.write_text(
        yaml.safe_dump(differentiated_data, sort_keys=False),
        encoding="utf-8",
    )
    default_yaml_path.write_text(
        yaml.safe_dump(default_data, sort_keys=False),
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
        "--resstock-loads-path",
        help=(
            "Optional ResStock hourly electric loads directory (local or s3://...). "
            "If provided, writes seasonal discount inputs for has_hp=true."
        ),
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

    if args.write_revenue_requirement_yamls:
        utility, default_revenue_requirement = _load_default_revenue_requirement(
            scenario_config_path=args.scenario_config,
            run_num=args.run_num,
        )
        differentiated_yaml_path, default_yaml_path = _write_revenue_requirement_yamls(
            breakdown=breakdown,
            run_dir=run_dir,
            group_col=args.group_col,
            cross_subsidy_col=args.cross_subsidy_col,
            utility=utility,
            default_revenue_requirement=default_revenue_requirement,
            differentiated_yaml_path=args.differentiated_yaml_path,
            default_yaml_path=args.default_yaml_path,
        )
        print(f"Wrote differentiated YAML: {differentiated_yaml_path}")
        print(f"Wrote default YAML: {default_yaml_path}")

    if args.resstock_loads_path:
        resstock_loads_path = _resolve_path_or_s3(args.resstock_loads_path)
        tariff_final_config_path = (
            _resolve_path_or_s3(args.tariff_final_config_path)
            if args.tariff_final_config_path
            else None
        )
        seasonal_inputs = compute_hp_seasonal_discount_inputs(
            run_dir=run_dir,
            resstock_loads_path=resstock_loads_path,
            cross_subsidy_col=args.cross_subsidy_col,
            storage_options=storage_options,
            tariff_final_config_path=tariff_final_config_path,
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
