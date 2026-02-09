"""Utility to generate precalc mapping from a tariff structure JSON file."""

import argparse
import json
from pathlib import Path

import pandas as pd


def generate_default_precalc_mapping(
    tariff_path: Path,
    tariff_key: str,
) -> pd.DataFrame:
    """
    Generate precalc mapping from tariff structure.

    Extracts unique (period, tier) combinations from energyratestructure
    and derives rel_value proportionally from rates (normalized to min rate = 1.0).

    Args:
        tariff_path: Path to the tariff structure JSON file
        tariff_key: Key name for the tariff

    Returns:
        DataFrame with columns: period, tier, rel_value, tariff
    """
    with open(tariff_path) as f:
        tariff = json.load(f)

    tariff_item = tariff["items"][0]
    rate_structure = tariff_item["energyratestructure"]

    # First pass: collect all effective rates to find minimum
    all_rates = []
    for period_tiers in rate_structure:
        for tier in period_tiers:
            effective_rate = tier["rate"] + tier.get("adj", 0.0)
            all_rates.append(effective_rate)

    min_rate = min(all_rates) if all_rates else 1.0

    # Second pass: build mapping with rel_value = rate / min_rate
    mappings = []
    for period_idx, period_tiers in enumerate(rate_structure):
        for tier_idx, tier in enumerate(period_tiers):
            effective_rate = tier["rate"] + tier.get("adj", 0.0)
            rel_value = effective_rate / min_rate if min_rate > 0 else 1.0
            mappings.append(
                {
                    "period": period_idx,
                    "tier": tier_idx,
                    "rel_value": round(rel_value, 4),
                    "tariff": tariff_key,
                }
            )

    return pd.DataFrame(mappings)


def main():
    parser = argparse.ArgumentParser(
        description="Generate precalc mapping from a tariff structure JSON file"
    )
    parser.add_argument(
        "--tariff-path",
        required=True,
        help="Path to tariff structure JSON file",
    )
    parser.add_argument(
        "--tariff-key",
        required=True,
        help="Key name for the tariff",
    )
    parser.add_argument(
        "--output-path",
        help="Output CSV path (prints to stdout if not provided)",
    )

    args = parser.parse_args()

    tariff_path = Path(args.tariff_path)
    if not tariff_path.exists():
        raise FileNotFoundError(f"Tariff file not found: {tariff_path}")

    mapping_df = generate_default_precalc_mapping(
        tariff_path=tariff_path,
        tariff_key=args.tariff_key,
    )

    if args.output_path:
        output_path = Path(args.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        mapping_df.to_csv(output_path, index=False)
        print(f"Created precalc mapping: {output_path}")
    else:
        print(mapping_df.to_string(index=False))


if __name__ == "__main__":
    main()
