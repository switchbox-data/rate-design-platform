"""Utility to create flat-rate URDB v7 format tariff JSON files."""

import argparse
import json
from pathlib import Path


def create_default_flat_tariff(
    label: str,
    volumetric_rate: float,
    fixed_charge: float,
    adjustment: float = 0.0,
    utility: str = "GenericUtility",
) -> dict:
    """
    Generate URDB v7 format flat tariff JSON structure.

    Args:
        label: Unique identifier for the tariff
        volumetric_rate: Energy rate in $/kWh
        fixed_charge: Fixed monthly charge in $
        adjustment: Rate adjustment in $/kWh (default: 0.0)
        utility: Utility name (default: "GenericUtility")

    Returns:
        Dictionary in URDB v7 format
    """
    # Single period (0) for all hours, all months (flat rate)
    schedule = [[0] * 24 for _ in range(12)]

    return {
        "items": [
            {
                "label": label,
                "uri": "",
                "sector": "Residential",
                "energyweekdayschedule": schedule,
                "energyweekendschedule": schedule,
                "energyratestructure": [
                    [{"rate": volumetric_rate, "adj": adjustment, "unit": "kWh"}]
                ],
                "fixedchargefirstmeter": fixed_charge,
                "fixedchargeunits": "$/month",
                "mincharge": 0.0,
                "minchargeunits": "$/month",
                "utility": utility,
                "servicetype": "Bundled",
                "name": label,
                "is_default": False,
                "country": "USA",
                "demandunits": "kW",
                "demandrateunit": "kW",
            }
        ]
    }


def main():
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
    parser.add_argument("--output-path", required=True, help="Output JSON file path")

    args = parser.parse_args()

    tariff = create_default_flat_tariff(
        label=args.label,
        volumetric_rate=args.volumetric_rate,
        fixed_charge=args.fixed_charge,
        adjustment=args.adjustment,
        utility=args.utility,
    )

    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(tariff, f, indent=2)

    print(f"Created tariff file: {output_path}")


if __name__ == "__main__":
    main()
