"""Shared constants for the EIA-861 data pipeline.

Single source of truth for PUDL upstream version, S3 paths, and state code
validation.  Imported by ``fetch_electric_utility_stat_parquets.py``,
``fetch_service_territory.py``, ``utils.py``, and downstream consumers like
``data.resstock.utility.assign_utility_md``.
"""

from __future__ import annotations

# ── PUDL upstream ─────────────────────────────────────────────────────────────
# Catalyst Coop stable release used for all EIA-861 tables.
# See https://github.com/catalyst-cooperative/pudl/releases
PUDL_STABLE_VERSION: str = "v2026.2.0"

_PUDL_S3_BASE: str = (
    f"https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/{PUDL_STABLE_VERSION}"
)

PUDL_YEARLY_SALES_URL: str = f"{_PUDL_S3_BASE}/core_eia861__yearly_sales.parquet"

PUDL_SERVICE_TERRITORY_URL: str = (
    f"{_PUDL_S3_BASE}/core_eia861__yearly_service_territory.parquet"
)

# ── Our S3 outputs ────────────────────────────────────────────────────────────

S3_UTILITY_STATS_BASE: str = "s3://data.sb/eia/861/electric_utility_stats/"
"""Hive-partitioned root: ``year=<year>/state=<state>/data.parquet``."""

S3_SERVICE_TERRITORY_BASE: str = "s3://data.sb/eia/861/service_territory/"
"""Hive-partitioned root: ``state=<STATE>/data.parquet``."""


def service_territory_s3_path(state: str) -> str:
    """Build the full S3 path for a state's county service territory parquet."""
    return f"{S3_SERVICE_TERRITORY_BASE}state={state.upper()}/data.parquet"


# ── Entity type filter ────────────────────────────────────────────────────────

DISTRIBUTION_ENTITY_TYPES: frozenset[str] = frozenset(
    {"Investor Owned", "Cooperative", "Municipal", "Political Subdivision", "State"}
)
"""EIA entity_type values (PUDL title-case convention) for physical distribution
utilities.  Excludes retail/power/wholesale marketers, behind-the-meter, and
other entities that don't own distribution infrastructure.
"""

# ── State code validation ─────────────────────────────────────────────────────

VALID_STATE_CODES: frozenset[str] = frozenset(
    {
        "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga",
        "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
        "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj",
        "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc",
        "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy",
        "dc",
    }
)  # fmt: skip
"""Lowercase two-letter US state and DC codes."""
