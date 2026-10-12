"""Write zero-filled supply energy and capacity 8760s (``zero.parquet``).

Delivery-only CAIRO runs read ``zero.parquet`` for supply energy and capacity.
The real supply generators write it alongside ``data.parquet``; this script
writes only the placeholders, for utilities that have no real supply MC yet
(e.g. CT). Each write appends a ``key: zero`` entry to the partition's
``manifest.yaml``.

Usage:
    uv run python utils/data_prep/marginal_costs/generate_zero_supply_mc.py \\
        --utility ct_eversource --year 2025 \\
        --output-s3-base s3://data.sb/switchbox/marginal_costs/ct/supply/ --upload
"""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

from utils.data_prep.marginal_costs.mc_manifest import require_published_code
from utils.data_prep.marginal_costs.supply_utils import (
    generate_zero_capacity_mc,
    generate_zero_energy_mc,
    save_zero_capacity_mc,
    save_zero_energy_mc,
)
from utils.file_io import get_aws_storage_options


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--utility", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--output-s3-base",
        required=True,
        help="Supply root holding energy/ and capacity/, e.g. .../ct/supply/",
    )
    parser.add_argument(
        "--component",
        choices=["energy", "capacity", "both"],
        default="both",
    )
    parser.add_argument("--upload", action="store_true", help="Write to S3.")
    args = parser.parse_args()
    load_dotenv()
    storage_options = get_aws_storage_options()

    if not args.upload:
        print("No --upload: nothing written.")
        return
    require_published_code()
    if args.component in ("energy", "both"):
        save_zero_energy_mc(
            generate_zero_energy_mc(args.year),
            args.utility,
            args.year,
            args.output_s3_base,
            storage_options,
        )
    if args.component in ("capacity", "both"):
        save_zero_capacity_mc(
            generate_zero_capacity_mc(args.year),
            args.utility,
            args.year,
            args.output_s3_base,
            storage_options,
        )


if __name__ == "__main__":
    main()
