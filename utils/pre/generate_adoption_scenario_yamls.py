"""Generate per-year scenario YAML entries for mixed-upgrade adoption runs.

Reads a base scenario YAML, extracts selected run configs, and emits a new
YAML file (``scenarios_<utility>_adoption.yaml``) with one entry per
(year × run) combination.  The per-year ``path_resstock_metadata`` and
``path_resstock_loads`` are rewritten to point at the materialized data
produced by ``materialize_mixed_upgrade.py``.  ``run_name`` is also extended
with the year index and calendar year label.

Usage
-----
::

    uv run python utils/pre/generate_adoption_scenario_yamls.py \\
        --base-scenario rate_design/hp_rates/ri/config/scenarios/scenarios_rie.yaml \\
        --runs 1,2,5,6 \\
        --adoption-config rate_design/hp_rates/ny/config/adoption/nyca_electrification.yaml \\
        --materialized-dir /ebs/data/nrel/resstock/res_2024_amy2018_2_sb/adoption/nyca_electrification \\
        --output rate_design/hp_rates/ri/config/scenarios/scenarios_rie_adoption.yaml

TODO: implement body — this is a skeleton stub.
"""

from __future__ import annotations

import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generate per-year scenario YAMLs for mixed-upgrade adoption runs.",
    )
    p.add_argument(
        "--base-scenario",
        required=True,
        metavar="PATH",
        dest="path_base_scenario",
        help="Existing scenario YAML to use as the run config template.",
    )
    p.add_argument(
        "--runs",
        required=True,
        help="Comma-separated run numbers to include (e.g. 1,2,5,6).",
    )
    p.add_argument(
        "--adoption-config",
        required=True,
        metavar="PATH",
        dest="path_adoption_config",
        help="Path to adoption trajectory YAML (for year_labels and scenario_name).",
    )
    p.add_argument(
        "--materialized-dir",
        required=True,
        metavar="PATH",
        dest="path_materialized_dir",
        help="Root of materialized per-year data (output of materialize_mixed_upgrade).",
    )
    p.add_argument(
        "--output",
        required=True,
        metavar="PATH",
        dest="path_output",
        help="Path to write the generated adoption scenario YAML.",
    )
    return p


def main(argv: list[str] | None = None) -> None:
    build_parser().parse_args(argv)
    # TODO: implement
    # 1. Load adoption config YAML for year_labels and scenario_name.
    # 2. Load base scenario YAML and extract the specified run configs.
    # 3. For each year index and each run number:
    #    a. Copy the run config.
    #    b. Replace path_resstock_metadata → <materialized_dir>/year=<N>/metadata-sb.parquet
    #    c. Replace path_resstock_loads   → <materialized_dir>/year=<N>/loads/
    #    d. Update run_name to include year index and label.
    # 4. Write combined YAML to args.path_output.
    raise NotImplementedError("generate_adoption_scenario_yamls is not yet implemented")


if __name__ == "__main__":
    sys.exit(main())
