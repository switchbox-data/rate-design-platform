"""Copy calibrated tariff_final_config from a run directory into state config."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from cloudpathlib import S3Path

from utils import get_project_root
from utils.pre.tariff_naming import parse_ri_run_name, parse_tariff_key_from_ri_run_name


def _resolve_path_or_s3(path_value: str) -> Path | S3Path:
    return S3Path(path_value) if path_value.startswith("s3://") else Path(path_value)


def _read_json(path: Path | S3Path) -> dict[str, Any]:
    if isinstance(path, S3Path):
        return json.loads(path.read_text())
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def copy_calibrated_tariff_from_run_dir(
    run_dir: Path | S3Path,
    *,
    destination_dir: Path | None = None,
) -> Path:
    run_name = run_dir.name
    state = parse_ri_run_name(run_name)["state"]
    tariff_key = parse_tariff_key_from_ri_run_name(run_name)

    tariff_final_config_path = run_dir / "tariff_final_config.json"
    payload = _read_json(tariff_final_config_path)

    target_dir = (
        destination_dir
        if destination_dir is not None
        else get_project_root()
        / "rate_design"
        / state
        / "hp_rates"
        / "config"
        / "tariffs"
        / "electric"
    )
    output_path = target_dir / f"{tariff_key}_calibrated.json"
    return _write_json(output_path, payload)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Copy <run_dir>/tariff_final_config.json into state tariff config as "
            "<tariff_key>_calibrated.json using the RI run-directory naming convention."
        )
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="RI run output directory path (local or s3://...).",
    )
    parser.add_argument(
        "--destination-dir",
        default="",
        help=(
            "Optional local directory override for output tariff file. "
            "Defaults to rate_design/<state>/hp_rates/config/tariffs/electric."
        ),
    )
    args = parser.parse_args()

    run_dir = _resolve_path_or_s3(args.run_dir)
    destination_dir = Path(args.destination_dir) if args.destination_dir else None
    output_path = copy_calibrated_tariff_from_run_dir(
        run_dir,
        destination_dir=destination_dir,
    )
    print(f"Copied calibrated tariff to: {output_path}")


if __name__ == "__main__":
    main()

