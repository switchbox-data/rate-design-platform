"""Create a flat-discount tariff JSON from computed flat discount inputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import polars as pl
from cloudpathlib import S3Path

from utils.pre.create_tariff import create_flat_rate, write_tariff_json


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
    if isinstance(path, S3Path):
        path.write_text(json.dumps(payload, indent=2))
        return str(path)
    written = write_tariff_json(payload, path)
    return str(written)


def _extract_flat_rate(row: dict[str, Any]) -> float:
    if "flat_rate" not in row or row["flat_rate"] is None:
        raise ValueError("Flat discount inputs CSV must contain a 'flat_rate' column.")

    return float(row["flat_rate"])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a flat-discount tariff JSON from flat discount input metrics."
    )
    parser.add_argument(
        "--base-tariff-json",
        required=True,
        help="Path to baseline tariff JSON (e.g. <utility>_default_calibrated.json).",
    )
    parser.add_argument(
        "--flat-inputs-csv",
        required=True,
        help="Path to flat_discount_rate_inputs.csv.",
    )
    parser.add_argument(
        "--label",
        required=True,
        help="Tariff label/name for the created flat-discount tariff.",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Output path for created tariff JSON (local or s3://...).",
    )
    args = parser.parse_args()

    base_tariff_path = _resolve_path(args.base_tariff_json)
    flat_inputs_path = _resolve_path(args.flat_inputs_csv)
    output_path = _resolve_path(args.output_path)

    base_tariff = _read_json(base_tariff_path)
    flat_inputs = _read_csv(flat_inputs_path)
    if flat_inputs.is_empty():
        raise ValueError("Flat discount inputs CSV is empty.")

    row = flat_inputs.row(0, named=True)
    flat_rate = _extract_flat_rate(row)

    flat_tariff = create_flat_rate(
        base_tariff=base_tariff,
        label=args.label,
        volumetric_rate=flat_rate,
    )
    written_path = _write_json(output_path, flat_tariff)
    print(f"Created flat-discount tariff file: {written_path}")


if __name__ == "__main__":
    main()
