"""Compute seasonal discount inputs without running subclass RR or YAML rewrites."""

from __future__ import annotations

import argparse
from pathlib import Path

from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.mid.compute_subclass_rr import (
    DEFAULT_BAT_METRIC,
    DEFAULT_SEASONAL_OUTPUT_FILENAME,
    _resolve_path_or_s3,
    compute_hp_seasonal_discount_inputs,
)


def _write_seasonal_inputs_csv(
    seasonal_inputs_path: S3Path | Path,
    csv_text: str,
) -> str:
    if isinstance(seasonal_inputs_path, S3Path):
        seasonal_inputs_path.write_text(csv_text)
    else:
        seasonal_inputs_path.parent.mkdir(parents=True, exist_ok=True)
        seasonal_inputs_path.write_text(csv_text, encoding="utf-8")
    return str(seasonal_inputs_path)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description=(
            "Compute HP seasonal discount inputs from run outputs and ResStock loads "
            "without subclass RR aggregation or RR YAML writes."
        )
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to CAIRO output directory (local or s3://...).",
    )
    parser.add_argument(
        "--resstock-loads-path",
        required=True,
        help="Path to ResStock hourly electric loads directory (local or s3://...).",
    )
    parser.add_argument(
        "--cross-subsidy-col",
        default=DEFAULT_BAT_METRIC,
        choices=("BAT_vol", "BAT_peak", "BAT_percustomer"),
        help="BAT column in cross_subsidization_BAT_values.csv to use.",
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

    run_dir = _resolve_path_or_s3(args.run_dir)
    resstock_loads_path = _resolve_path_or_s3(args.resstock_loads_path)
    tariff_final_config_path = (
        _resolve_path_or_s3(args.tariff_final_config_path)
        if args.tariff_final_config_path
        else None
    )
    output_dir = _resolve_path_or_s3(args.output_dir) if args.output_dir else run_dir

    storage_options = get_aws_storage_options() if isinstance(run_dir, S3Path) else None
    seasonal_inputs = compute_hp_seasonal_discount_inputs(
        run_dir=run_dir,
        resstock_loads_path=resstock_loads_path,
        cross_subsidy_col=args.cross_subsidy_col,
        storage_options=storage_options,
        tariff_final_config_path=tariff_final_config_path,
    )
    print(seasonal_inputs)

    output_path = output_dir / DEFAULT_SEASONAL_OUTPUT_FILENAME
    csv_text = seasonal_inputs.write_csv(None)
    if not isinstance(csv_text, str):
        raise ValueError("Failed to render seasonal discount input CSV text.")
    written_path = _write_seasonal_inputs_csv(output_path, csv_text)
    print(f"Wrote seasonal discount inputs CSV: {written_path}")


if __name__ == "__main__":
    main()
