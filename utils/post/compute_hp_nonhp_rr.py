"""Compute HP and non-HP revenue requirements from CAIRO outputs.

For each customer group (HP vs non-HP), this script computes:
  RR_group = sum(annual electric target bills) - sum(cross_subsidy_per_customer)

Inputs (under --run-dir):
  - bills/elec_bills_year_target.csv
  - cross_subsidization/cross_subsidization_BAT_values.csv
  - customer_metadata.csv
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import polars as pl
from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

BLDG_ID_COL = "bldg_id"
HAS_HP_COL = "postprocess_group.has_hp"
HAS_HP_FLAG_COL = "has_hp"
ANNUAL_BILL_COL = "annual_bill"
CROSS_SUBSIDY_COL = "customer_level_residual_share_percustomer"
CROSS_SUBSIDY_PER_CUSTOMER_COL = "cross_subsidy_per_customer"
GROUP_COL = "customer_group"
SUM_BILLS_COL = "sum_bills"
SUM_CROSS_SUBSIDY_COL = "sum_cross_subsidy_per_customer"
RR_COL = "revenue_requirement"

HP_GROUP = "HP"
NONHP_GROUP = "NonHP"
ANNUAL_MONTH_VALUE = "Annual"
WINTER_MONTHS_DEFAULT = ("Nov", "Dec", "Jan", "Feb", "Mar")
TARIFF_RATE_COL_IDX = 4


def _storage_opts() -> dict[str, str]:
    return get_aws_storage_options()


def _run_file(run_dir: S3Path | Path, relative_path: str) -> str:
    return str(run_dir / relative_path)


def _run_path(run_dir: S3Path | Path, relative_path: str) -> S3Path | Path:
    return run_dir / relative_path


def _read_json(path: S3Path | Path) -> dict[str, Any]:
    text = path.read_text() if isinstance(path, S3Path) else path.read_text(encoding="utf-8")
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return obj


def _load_customer_metadata(
    run_dir: S3Path | Path,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            _run_file(run_dir, "customer_metadata.csv"),
            storage_options=storage_options,
        )
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col(HAS_HP_COL).cast(pl.Boolean, strict=False).alias(HAS_HP_FLAG_COL),
        )
        .with_columns(pl.col(HAS_HP_FLAG_COL).fill_null(False))
        .unique(subset=[BLDG_ID_COL], keep="first")
    )


def _load_annual_target_bills(
    run_dir: S3Path | Path,
    annual_month: str,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            _run_file(run_dir, "bills/elec_bills_year_target.csv"),
            storage_options=storage_options,
        )
        .filter(pl.col("month") == annual_month)
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col("bill_level").cast(pl.Float64).alias(ANNUAL_BILL_COL),
        )
        .group_by(BLDG_ID_COL)
        .agg(pl.col(ANNUAL_BILL_COL).sum().alias(ANNUAL_BILL_COL))
        )


def _load_target_bills_monthly(
    run_dir: S3Path | Path,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return pl.scan_csv(
        _run_file(run_dir, "bills/elec_bills_year_target.csv"),
        storage_options=storage_options,
    ).select(
        pl.col(BLDG_ID_COL).cast(pl.Int64),
        pl.col("month").cast(pl.Utf8),
        pl.col("bill_level").cast(pl.Float64).alias(ANNUAL_BILL_COL),
    )


def _load_cross_subsidy_per_customer(
    run_dir: S3Path | Path,
    storage_options: dict[str, str] | None,
) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            _run_file(
                run_dir,
                "cross_subsidization/cross_subsidization_BAT_values.csv",
            ),
            storage_options=storage_options,
        )
        .select(
            pl.col(BLDG_ID_COL).cast(pl.Int64),
            pl.col(CROSS_SUBSIDY_COL)
            .cast(pl.Float64)
            .alias(CROSS_SUBSIDY_PER_CUSTOMER_COL),
        )
        .group_by(BLDG_ID_COL)
        .agg(
            pl.col(CROSS_SUBSIDY_PER_CUSTOMER_COL)
            .sum()
            .alias(CROSS_SUBSIDY_PER_CUSTOMER_COL)
        )
    )


def _validate_joined_inputs(joined: pl.DataFrame, annual_month: str) -> None:
    missing_bills = joined.filter(pl.col(ANNUAL_BILL_COL).is_null()).height
    if missing_bills > 0:
        raise ValueError(
            f"Missing annual target bills for {missing_bills} buildings "
            f"(month={annual_month})."
        )

    missing_cross_subsidy = joined.filter(
        pl.col(CROSS_SUBSIDY_PER_CUSTOMER_COL).is_null()
    ).height
    if missing_cross_subsidy > 0:
        raise ValueError(
            "Missing per-customer cross-subsidy values for "
            f"{missing_cross_subsidy} buildings."
        )


def compute_hp_nonhp_rr(
    run_dir: S3Path | Path,
    annual_month: str = ANNUAL_MONTH_VALUE,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Return HP/non-HP sums and revenue requirement by group."""
    metadata = _load_customer_metadata(run_dir, storage_options)
    annual_bills = _load_annual_target_bills(run_dir, annual_month, storage_options)
    cross_subsidy = _load_cross_subsidy_per_customer(run_dir, storage_options)

    joined = (
        metadata.join(annual_bills, on=BLDG_ID_COL, how="left")
        .join(cross_subsidy, on=BLDG_ID_COL, how="left")
        .collect()
    )
    if joined.is_empty():
        raise ValueError("No customers found in customer_metadata.csv.")

    _validate_joined_inputs(joined, annual_month)

    breakdown = (
        joined.with_columns(
            pl.when(pl.col(HAS_HP_FLAG_COL))
            .then(pl.lit(HP_GROUP))
            .otherwise(pl.lit(NONHP_GROUP))
            .alias(GROUP_COL)
        )
        .group_by(GROUP_COL)
        .agg(
            pl.col(ANNUAL_BILL_COL).sum().alias(SUM_BILLS_COL),
            pl.col(CROSS_SUBSIDY_PER_CUSTOMER_COL).sum().alias(SUM_CROSS_SUBSIDY_COL),
        )
        .with_columns((pl.col(SUM_BILLS_COL) - pl.col(SUM_CROSS_SUBSIDY_COL)).alias(RR_COL))
    )

    groups = pl.DataFrame({GROUP_COL: [HP_GROUP, NONHP_GROUP]})
    return groups.join(breakdown, on=GROUP_COL, how="left").with_columns(
        pl.col(SUM_BILLS_COL).fill_null(0.0).cast(pl.Float64),
        pl.col(SUM_CROSS_SUBSIDY_COL).fill_null(0.0).cast(pl.Float64),
        pl.col(RR_COL).fill_null(0.0).cast(pl.Float64),
    )


def _load_tariff_from_run_dir(
    run_dir: S3Path | Path,
    tariff_key: str | None = None,
) -> tuple[str, dict[str, Any]]:
    tariff_config = _read_json(_run_path(run_dir, "tariff_final_config.json"))
    keys = list(tariff_config.keys())
    if not keys:
        raise ValueError("tariff_final_config.json is empty")
    if tariff_key is None:
        if len(keys) != 1:
            raise ValueError(
                "tariff_final_config.json contains multiple tariffs; pass `tariff_key`."
            )
        tariff_key = keys[0]
    if tariff_key not in tariff_config:
        raise ValueError(
            f"Tariff key `{tariff_key}` not found in tariff_final_config.json. "
            f"Available: {keys}"
        )
    tariff = tariff_config[tariff_key]
    if not isinstance(tariff, dict):
        raise ValueError(f"Tariff entry `{tariff_key}` is not a JSON object")
    return tariff_key, tariff


def _extract_flat_default_rate_per_kwh(tariff: dict[str, Any]) -> float:
    tou_mat = tariff.get("ur_ec_tou_mat")
    if not isinstance(tou_mat, list) or not tou_mat:
        raise ValueError("Missing or invalid `ur_ec_tou_mat` in tariff config")

    rates: list[float] = []
    for row in tou_mat:
        if not isinstance(row, list) or len(row) <= TARIFF_RATE_COL_IDX:
            raise ValueError("Unexpected row format in `ur_ec_tou_mat`")
        rates.append(float(row[TARIFF_RATE_COL_IDX]))

    first_rate = rates[0]
    if any(abs(r - first_rate) > 1e-12 for r in rates[1:]):
        raise ValueError(
            "Tariff is not flat across TOU rows; cannot hold summer at a single flat rate."
        )
    return first_rate


def _extract_monthly_fixed_charge(tariff: dict[str, Any]) -> float:
    return float(tariff.get("ur_monthly_fixed_charge", 0.0))


def _hp_cross_subsidy_total_from_breakdown(breakdown: pl.DataFrame) -> float:
    hp = breakdown.filter(pl.col(GROUP_COL) == HP_GROUP)
    if hp.is_empty():
        return 0.0
    return float(hp.get_column(SUM_CROSS_SUBSIDY_COL)[0])


def _estimate_hp_winter_demand_kwh_from_bills(
    run_dir: S3Path | Path,
    default_rate_per_kwh: float,
    monthly_fixed_charge: float,
    winter_months: tuple[str, ...],
    storage_options: dict[str, str] | None,
) -> float:
    if default_rate_per_kwh <= 0:
        raise ValueError("Default flat rate must be > 0 to infer demand from bills.")

    hp_winter_bills = (
        _load_customer_metadata(run_dir, storage_options)
        .filter(pl.col(HAS_HP_FLAG_COL))
        .join(_load_target_bills_monthly(run_dir, storage_options), on=BLDG_ID_COL, how="inner")
        .filter(pl.col("month").is_in(list(winter_months)))
        .collect()
    )
    if hp_winter_bills.is_empty():
        raise ValueError(
            "No HP winter bills found in elec_bills_year_target.csv for selected winter months."
        )

    inferred_kwh = (
        hp_winter_bills.with_columns(
            (
                (pl.col(ANNUAL_BILL_COL) - monthly_fixed_charge)
                .clip(lower_bound=0.0)
                / default_rate_per_kwh
            ).alias("inferred_kwh")
        )
        .select(pl.col("inferred_kwh").sum().alias("winter_hp_demand_kwh"))
        .item()
    )
    inferred_kwh_float = float(inferred_kwh)
    if inferred_kwh_float <= 0:
        raise ValueError("Estimated HP winter demand is <= 0; cannot compute winter rate.")
    return inferred_kwh_float


def calculate_winter_rate_from_cross_subsidy(
    default_rate_per_kwh: float,
    cross_subsidy_total: float,
    winter_hp_demand_kwh: float,
) -> float:
    """Compute winter rate with summer flat rate held constant."""
    if winter_hp_demand_kwh <= 0:
        raise ValueError("winter_hp_demand_kwh must be > 0")
    return default_rate_per_kwh - (cross_subsidy_total / winter_hp_demand_kwh)


def calculate_winter_discount_holding_summer_flat(
    run_dir: S3Path | Path,
    winter_months: tuple[str, ...] = WINTER_MONTHS_DEFAULT,
    annual_month: str = ANNUAL_MONTH_VALUE,
    storage_options: dict[str, str] | None = None,
    tariff_key: str | None = None,
) -> pl.DataFrame:
    """Compute winter discount rate that removes HP cross-subsidy.

    Formula:
      winter_rate = default_rate - cross_subsidy_total / winter_hp_demand_kwh

    `default_rate` is read from `tariff_final_config.json` and used as the summer rate.
    `winter_hp_demand_kwh` is inferred from HP winter target bills assuming a flat rate with
    a monthly fixed charge.
    """

    breakdown = compute_hp_nonhp_rr(
        run_dir=run_dir,
        annual_month=annual_month,
        storage_options=storage_options,
    )
    cross_subsidy_total = _hp_cross_subsidy_total_from_breakdown(breakdown)
    resolved_tariff_key, tariff = _load_tariff_from_run_dir(run_dir, tariff_key)
    default_rate = _extract_flat_default_rate_per_kwh(tariff)
    monthly_fixed_charge = _extract_monthly_fixed_charge(tariff)
    winter_hp_demand_kwh = _estimate_hp_winter_demand_kwh_from_bills(
        run_dir=run_dir,
        default_rate_per_kwh=default_rate,
        monthly_fixed_charge=monthly_fixed_charge,
        winter_months=winter_months,
        storage_options=storage_options,
    )
    winter_rate = calculate_winter_rate_from_cross_subsidy(
        default_rate_per_kwh=default_rate,
        cross_subsidy_total=cross_subsidy_total,
        winter_hp_demand_kwh=winter_hp_demand_kwh,
    )
    winter_discount_per_kwh = default_rate - winter_rate

    return pl.DataFrame(
        {
            "tariff_key": [resolved_tariff_key],
            "summer_rate_per_kwh": [default_rate],
            "winter_rate_per_kwh": [winter_rate],
            "winter_discount_per_kwh": [winter_discount_per_kwh],
            "cross_subsidy_total_hp": [cross_subsidy_total],
            "winter_hp_demand_kwh": [winter_hp_demand_kwh],
            "winter_months": [",".join(winter_months)],
        }
    )


def compute_rr_wide(breakdown: pl.DataFrame) -> pl.DataFrame:
    """Return single-row summary with RR_HP and RR_NonHP columns."""
    return breakdown.select(
        pl.col(SUM_BILLS_COL)
        .filter(pl.col(GROUP_COL) == HP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("sum_bills_hp"),
        pl.col(SUM_CROSS_SUBSIDY_COL)
        .filter(pl.col(GROUP_COL) == HP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("sum_cross_subsidy_hp"),
        pl.col(RR_COL)
        .filter(pl.col(GROUP_COL) == HP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("RR_HP"),
        pl.col(SUM_BILLS_COL)
        .filter(pl.col(GROUP_COL) == NONHP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("sum_bills_nonhp"),
        pl.col(SUM_CROSS_SUBSIDY_COL)
        .filter(pl.col(GROUP_COL) == NONHP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("sum_cross_subsidy_nonhp"),
        pl.col(RR_COL)
        .filter(pl.col(GROUP_COL) == NONHP_GROUP)
        .first()
        .fill_null(0.0)
        .alias("RR_NonHP"),
    )


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description=(
            "Compute RR_HP and RR_NonHP from CAIRO outputs using annual target "
            "electric bills and per-customer cross-subsidy."
        )
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to CAIRO output directory (local path or s3://...)",
    )
    parser.add_argument(
        "--annual-month",
        default=ANNUAL_MONTH_VALUE,
        help="Month label in bills file to treat as annual bill (default: Annual)",
    )
    parser.add_argument(
        "--output-csv",
        help="Optional path to write single-row RR summary CSV",
    )
    parser.add_argument(
        "--output-breakdown-csv",
        help="Optional path to write HP/non-HP breakdown CSV",
    )
    parser.add_argument(
        "--output-winter-discount-csv",
        help="Optional path to write winter discount summary CSV",
    )
    parser.add_argument(
        "--winter-months",
        default="Nov,Dec,Jan,Feb,Mar",
        help="Comma-separated month labels to treat as winter in bills file.",
    )
    parser.add_argument(
        "--tariff-key",
        help="Optional tariff key in tariff_final_config.json.",
    )
    args = parser.parse_args()

    run_dir = (
        S3Path(args.run_dir) if args.run_dir.startswith("s3://") else Path(args.run_dir)
    )
    storage_options = _storage_opts() if isinstance(run_dir, S3Path) else None

    breakdown = compute_hp_nonhp_rr(
        run_dir=run_dir,
        annual_month=args.annual_month,
        storage_options=storage_options,
    )
    rr_summary = compute_rr_wide(breakdown)

    if args.output_breakdown_csv:
        breakdown.write_csv(args.output_breakdown_csv)
    if args.output_csv:
        rr_summary.write_csv(args.output_csv)

    winter_months = tuple(m.strip() for m in args.winter_months.split(",") if m.strip())
    winter_discount_summary = calculate_winter_discount_holding_summer_flat(
        run_dir=run_dir,
        winter_months=winter_months,
        annual_month=args.annual_month,
        storage_options=storage_options,
        tariff_key=args.tariff_key,
    )
    if args.output_winter_discount_csv:
        winter_discount_summary.write_csv(args.output_winter_discount_csv)

    print("HP/non-HP breakdown:")
    print(breakdown)
    print("\nRR summary:")
    print(rr_summary)
    print("\nWinter discount summary:")
    print(winter_discount_summary)


if __name__ == "__main__":
    main()
