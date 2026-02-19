"""Create a seasonal-discount tariff JSON from computed seasonal inputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import polars as pl
from cloudpathlib import S3Path

from utils.pre.create_tariff import create_seasonal_rate


def _resolve_path(path_value: str) -> S3Path | Path:
    return S3Path(path_value) if path_value.startswith("s3://") else Path(path_value)


def _read_json(path: S3Path | Path) -> dict[str, Any]:
    if isinstance(path, S3Path):
        return json.loads(path.read_text())
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: S3Path | Path) -> pl.DataFrame:
    if isinstance(path, S3Path):
        return pl.read_csv(str(path))
    return pl.read_csv(path)


def _write_json(path: S3Path | Path, payload: dict[str, Any]) -> str:
    text = json.dumps(payload, indent=2)
    if isinstance(path, S3Path):
        path.write_text(text)
        return str(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return str(path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a seasonal-discount tariff JSON from seasonal input metrics."
    )
    parser.add_argument(
        "--base-tariff-json",
        required=True,
        help="Path to baseline tariff JSON (typically <run_dir>/tariff_final_config.json).",
    )
    parser.add_argument(
        "--seasonal-inputs-csv",
        required=True,
        help=(
            "Path to seasonal_discount_rate_inputs.csv "
            "(typically from compute_subclass_rr --resstock-loads-path)."
        ),
    )
    parser.add_argument(
        "--label",
        required=True,
        help="Tariff label/name for created seasonal-discount tariff.",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Output path for created tariff JSON (local or s3://...).",
    )
    args = parser.parse_args()

    base_tariff_path = _resolve_path(args.base_tariff_json)
    seasonal_inputs_path = _resolve_path(args.seasonal_inputs_csv)
    output_path = _resolve_path(args.output_path)

    base_tariff = _read_json(base_tariff_path)
    seasonal_inputs = _read_csv(seasonal_inputs_path)
    if seasonal_inputs.is_empty():
        raise ValueError("Seasonal inputs CSV is empty.")

    row = seasonal_inputs.row(0, named=True)
    summer_rate = float(row["default_rate"])
    winter_rate = float(row["winter_rate_hp"])

    seasonal_tariff = create_seasonal_rate(
        base_tariff=base_tariff,
        label=args.label,
        winter_rate=winter_rate,
        summer_rate=summer_rate,
    )
    written_path = _write_json(output_path, seasonal_tariff)
    print(f"Created seasonal-discount tariff file: {written_path}")


if __name__ == "__main__":
    main()
