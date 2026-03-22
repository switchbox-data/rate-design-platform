"""Materialize per-year ResStock data for mixed-upgrade HP adoption trajectories.

Reads an adoption config YAML (scenario fractions per upgrade per year), assigns
buildings to upgrades using ``buildstock_fetch.scenarios.MixedUpgradeScenario``,
and writes one directory per year containing:

- ``metadata-sb.parquet``: combined metadata rows from the assigned upgrades.
- ``loads/``: directory of symlinks pointing each building to the correct
  upgrade's load parquet (``{bldg_id}-{upgrade_id}.parquet``).

The output mirrors the layout that ``run_scenario.py`` already expects for a
single-upgrade run, so no changes are needed to the scenario runner.

Usage
-----
::

    uv run python utils/pre/materialize_mixed_upgrade.py \\
        --state ri \\
        --utility rie \\
        --adoption-config rate_design/hp_rates/ny/config/adoption/nyca_electrification.yaml \\
        --path-resstock-release /ebs/data/nrel/resstock/res_2024_amy2018_2_sb \\
        --output-dir /ebs/data/nrel/resstock/res_2024_amy2018_2_sb/adoption/nyca_electrification

TODO: implement body — this is a skeleton stub.
"""

from __future__ import annotations

import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Materialize per-year mixed-upgrade ResStock data for adoption trajectories.",
    )
    p.add_argument(
        "--state", required=True, help="Two-letter state abbreviation (e.g. ny, ri)."
    )
    p.add_argument("--utility", required=True, help="Utility slug (e.g. rie, nyseg).")
    p.add_argument(
        "--adoption-config",
        required=True,
        metavar="PATH",
        dest="path_adoption_config",
        help="Path to adoption trajectory YAML.",
    )
    p.add_argument(
        "--path-resstock-release",
        required=True,
        help="Root path of the processed ResStock _sb release (local or s3://).",
    )
    p.add_argument(
        "--output-dir",
        required=True,
        metavar="PATH",
        dest="path_output_dir",
        help="Directory to write per-year materialized data.",
    )
    return p


def main(argv: list[str] | None = None) -> None:
    build_parser().parse_args(argv)
    # TODO: implement
    # 1. Load adoption config YAML and validate with buildstock_fetch.scenarios.validate_scenario()
    # 2. Discover upgrade directories under args.path_resstock_release; error if any are missing.
    # 3. Use MixedUpgradeScenario to assign buildings → upgrades per year.
    # 4. For each year:
    #    a. Read metadata-sb.parquet from each required upgrade directory.
    #    b. Filter to correct buildings per upgrade.
    #    c. Combine and write to <output_dir>/year=<N>/metadata-sb.parquet.
    #    d. Create <output_dir>/year=<N>/loads/ with symlinks per building.
    # 5. Write scenario CSV (bldg_id, year_0, year_1, ...) for reference.
    raise NotImplementedError("materialize_mixed_upgrade is not yet implemented")


if __name__ == "__main__":
    sys.exit(main())
