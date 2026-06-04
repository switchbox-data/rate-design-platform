"""Shared constants for the ResStock data pipeline.

Also serves as a CLI for Justfiles to read config values cleanly::

    uv run python -m data.resstock.constants resstock.release_year
    uv run python -m data.resstock.constants resstock_release
    uv run python -m data.resstock.constants paths.output_dir

See ``_get_config_value`` for all supported key formats.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

# ── Module-level constants ────────────────────────────────────────────────────

# Absolute paths to the shared pipeline configuration files.  Use these instead
# of re-deriving the path via Path(__file__).parent in individual scripts —
# the constants are always correct regardless of how deeply a module is nested
# under data/resstock/.
CONFIG_PATH: Path = Path(__file__).parent / "config.yaml"
STATE_CONFIGS_PATH: Path = Path(__file__).parent / "state_configs.yaml"

# Root of the local EBS ResStock data directory.  All released and modified
# (_sb) parquet trees live under this path on EBS instances.
PATH_EBS_PARQUET: Path = Path("/ebs/data/nrel/resstock")

# Columns added by identify_hp_customers.
HP_CUSTOMERS_COLS: frozenset[str] = frozenset({"postprocess_group.has_hp"})

# Columns added by identify_heating_type (requires has_hp from identify_hp_customers).
HEATING_TYPE_COLS: frozenset[str] = frozenset(
    {
        "postprocess_group.heating_type",
        "postprocess_group.heating_type_v2",
        "heats_with_electricity",
        "heats_with_natgas",
        "heats_with_oil",
        "heats_with_propane",
    }
)

# Columns added by identify_natgas_connection (requires heats_with_natgas from heating_type).
NATGAS_CONNECTION_COLS: frozenset[str] = frozenset({"has_natgas_connection"})

# Columns added by add_vulnerability_columns (NY only).
VULNERABILITY_COLS: frozenset[str] = frozenset(
    {
        "has_child_under_6",
        "has_person_over_60",
        "has_disabled_person",
        "is_vulnerable",
    }
)

# File types that belong only to the raw NREL release and must never be copied
# to the _sb release, uploaded under _sb, or validated against _sb.
# load_curve_annual has no post-approximation equivalent: the only valid
# aggregation of the modified _sb load curves is load_curve_monthly (derived
# from load_curve_hourly by add_monthly_loads after all modifications are done).
SB_EXCLUDED_FILE_TYPES: frozenset[str] = frozenset({"load_curve_annual"})


# ── CLI for Justfile config access ────────────────────────────────────────────


def _get_config_value(key: str) -> str:
    """Resolve a config key and return its string representation.

    Supported key formats:

    **config.yaml dotted paths** — e.g. ``resstock.release_year``,
    ``paths.s3_dir``, ``pums.survey``.  Lists are returned as
    space-separated strings.

    **Derived keys** (computed from config.yaml):

    - ``resstock_release`` — e.g. ``res_2024_amy2018_2``
    - ``resstock_release_sb`` — e.g. ``res_2024_amy2018_2_sb``
    - ``upgrade_ids_padded`` — e.g. ``00 01 02 03 04 05``

    **Module constants** — ``path_ebs_parquet``.

    **state_configs.yaml paths** — prefix with ``state_config.``, e.g.
    ``state_config.NY.utility_assignment.kwargs.electric_poly_filename``.
    """
    import yaml

    _module_constants: dict[str, str] = {
        "path_ebs_parquet": str(PATH_EBS_PARQUET),
    }
    if key in _module_constants:
        return _module_constants[key]

    config = yaml.safe_load(CONFIG_PATH.open())
    rs: dict[str, Any] = config["resstock"]
    release = f"res_{rs['release_year']}_{rs['weather_file']}_{rs['release_version']}"

    _derived: dict[str, str] = {
        "resstock_release": release,
        "resstock_release_sb": f"{release}_sb",
        "upgrade_ids_padded": " ".join(str(u).zfill(2) for u in rs["upgrade_ids"]),
    }
    if key in _derived:
        return _derived[key]

    if key.startswith("state_config."):
        state_config = yaml.safe_load(STATE_CONFIGS_PATH.open())
        parts = key.split(".")[1:]
        val: Any = state_config
        for part in parts:
            val = val[part]
    else:
        val = config
        for part in key.split("."):
            val = val[part]

    if isinstance(val, list):
        return " ".join(str(v) for v in val)
    return str(val)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:  # noqa: PLR2004
        print(
            "Usage: uv run python -m data.resstock.constants <key>",
            file=sys.stderr,
        )
        sys.exit(1)
    print(_get_config_value(sys.argv[1]))
