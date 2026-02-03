"""CLI tool to generate customer count reweighting CSV files for utilities.

This script reweights ResStock building samples to match utility-specific customer
counts from rate cases.
"""

import argparse
from pathlib import Path


def main():
    """Parse CLI arguments and generate reweighted customer CSV."""
    parser = argparse.ArgumentParser(
        description="Generate customer count reweighting CSV for a utility territory"
    )
    parser.add_argument(
        "--customer-count",
        type=int,
        required=True,
        help="Target customer count for the utility territory",
    )
    parser.add_argument(
        "--metadata-path",
        type=Path,
        required=True,
        help="Path to ResStock metadata parquet file",
    )
    parser.add_argument(
        "--utility-name",
        type=str,
        required=True,
        help="Utility name to filter buildings on",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        required=True,
        help="Output path for the reweighted customer CSV",
    )

    args = parser.parse_args()

    print(f"Customer count: {args.customer_count}")
    print(f"Metadata path: {args.metadata_path}")
    print(f"Utility name: {args.utility_name}")
    print(f"Output path: {args.output_path}")


if __name__ == "__main__":
    main()
