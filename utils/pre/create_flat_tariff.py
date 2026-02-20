"""Create flat-rate URDB v7 tariff JSON files."""

from __future__ import annotations

import argparse
from pathlib import Path

from utils.pre.create_tariff import create_default_flat_tariff, write_tariff_json


def _write_json(path: Path, payload: dict) -> str:
    written = write_tariff_json(payload, path)
    return str(written)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a flat-rate URDB v7 format tariff JSON file"
    )
    parser.add_argument("--label", required=True, help="Tariff label/identifier")
    parser.add_argument(
        "--volumetric-rate",
        type=float,
        required=True,
        help="Volumetric rate in $/kWh",
    )
    parser.add_argument(
        "--fixed-charge",
        type=float,
        required=True,
        help="Fixed monthly charge in $",
    )
    parser.add_argument(
        "--adjustment",
        type=float,
        default=0.0,
        help="Rate adjustment in $/kWh (default: 0.0)",
    )
    parser.add_argument(
        "--utility",
        default="GenericUtility",
        help="Utility name (default: GenericUtility)",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Output JSON file path.",
    )

    args = parser.parse_args()

    tariff = create_default_flat_tariff(
        label=args.label,
        volumetric_rate=args.volumetric_rate,
        fixed_charge=args.fixed_charge,
        adjustment=args.adjustment,
        utility=args.utility,
    )

    output_path = Path(args.output_path)
    written_path = _write_json(output_path, tariff)
    print(f"Created tariff file: {written_path}")


if __name__ == "__main__":
    main()
