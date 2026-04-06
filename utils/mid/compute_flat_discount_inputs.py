"""Compute flat discount inputs without running subclass RR or YAML rewrites."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from cloudpathlib import S3Path
from dotenv import load_dotenv

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.mid.compute_subclass_rr import (
    BAT_METRIC_CHOICES,
    DEFAULT_BAT_METRIC,
    DEFAULT_GROUP_COL,
    _resolve_path_or_s3,
    compute_subclass_flat_discount_inputs,
    flat_discount_filename,
)


def _write_flat_inputs_csv(
    path: S3Path | Path,
    csv_text: str,
) -> str:
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
        description=(
            "Compute a fair flat volumetric rate for a customer subclass from run "
            "outputs and ResStock loads."
        )
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to CAIRO output directory (local or s3://...).",
    )
    parser.add_argument(
        "--resstock-base",
        required=True,
        help="Base path to ResStock release (e.g. s3://.../res_2024_amy2018_2).",
    )
    parser.add_argument(
        "--state",
        required=True,
        help="State partition for loads (e.g. NY, RI).",
    )
    parser.add_argument(
        "--upgrade",
        required=True,
        help="Upgrade partition for loads (e.g. 00).",
    )
    parser.add_argument(
        "--group-col",
        default=DEFAULT_GROUP_COL,
        help=(
            "Column in customer_metadata.csv that defines subclass membership "
            "(default: has_hp). The resolved column name (with postprocess_group. "
            "prefix if needed) is used automatically."
        ),
    )
    parser.add_argument(
        "--subclass-value",
        default="true",
        help=(
            "Value of --group-col that identifies the target subclass "
            "(default: 'true', i.e. HP customers when group-col=has_hp). "
            "For electric-heating subclass use 'electric_heating'."
        ),
    )
    parser.add_argument(
        "--cross-subsidy-col",
        default=DEFAULT_BAT_METRIC,
        choices=BAT_METRIC_CHOICES,
        help="BAT column in cross_subsidization_BAT_values.csv to use.",
    )
    parser.add_argument(
        "--base-tariff-json",
        required=True,
        help=(
            "Path to URDB-format base tariff JSON (e.g. <utility>_default_calibrated.json). "
            "Used to extract fixedchargefirstmeter."
        ),
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Optional output directory for flat discount CSV. "
            "If omitted, writes to --run-dir."
        ),
    )
    args = parser.parse_args()

    run_dir = _resolve_path_or_s3(args.run_dir)
    base_tariff_json_path = _resolve_path_or_s3(args.base_tariff_json)
    output_dir = _resolve_path_or_s3(args.output_dir) if args.output_dir else run_dir

    storage_options = get_aws_storage_options() if isinstance(run_dir, S3Path) else None
    flat_inputs = compute_subclass_flat_discount_inputs(
        run_dir=run_dir,
        resstock_base=args.resstock_base,
        state=args.state,
        upgrade=args.upgrade,
        group_col=args.group_col,
        subclass_value=args.subclass_value,
        cross_subsidy_col=args.cross_subsidy_col,
        storage_options=storage_options,
        base_tariff_json_path=base_tariff_json_path,
    )
    print(flat_inputs)

    filename = flat_discount_filename(args.group_col, args.subclass_value)
    output_path = output_dir / filename
    csv_text = flat_inputs.write_csv(None)
    if not isinstance(csv_text, str):
        raise ValueError("Failed to render flat discount input CSV text.")
    written_path = _write_flat_inputs_csv(output_path, csv_text)
    print(f"Wrote flat discount inputs CSV: {written_path}")


if __name__ == "__main__":
    main()
