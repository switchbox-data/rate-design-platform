"""
Fetch electric tariffs from Genability/Arcadia using a state-specific YAML config.

Reads genability_tariffs.yaml (utility_shortcode -> tariff_key -> identifier). State is passed as an argument.
Resolves utility shortcodes to Arcadia LSE via EIA ID from utils/utility_codes.py.
Identifier can be:
  - "default" : residential default tariff (RESIDENTIAL + DEFAULT in Arcadia)
  - <integer> : masterTariffId (use that tariff)
  - "<name>"  : tariff name to match (first match in LSE's tariff list)

Takes an --effective-date (YYYY-MM-DD) that controls which tariff version you get —
all charges effective on that date will be included. Defaults to today (UTC).

Writes one <key>_<identifier>_<date>.json per entry (e.g. rie_default_2025-01-01.json)
to the output directory (Genability/Arcadia JSON).

Requires ARCADIA_APP_ID and ARCADIA_APP_KEY in the environment.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import yaml
from tariff_fetch.genability.lse import get_lses_page
from tariff_fetch.genability.tariffs import tariffs_paginate

from utils.utility_codes import get_utilities_for_state

# Load .env from project root (same as utils/__init__.py)
_env_path = Path(__file__).resolve().parents[2] / ".env"
if _env_path.exists():
    from dotenv import load_dotenv

    load_dotenv(_env_path)

log = logging.getLogger(__name__)


def _get_arcadia_credentials() -> tuple[str, str]:
    import os

    app_id = os.environ.get("ARCADIA_APP_ID")
    app_key = os.environ.get("ARCADIA_APP_KEY")
    if not app_id or not app_key:
        raise RuntimeError("Set ARCADIA_APP_ID and ARCADIA_APP_KEY in the environment")
    return (app_id, app_key)


def _eia_id_for_utility(state: str, std_name: str) -> int:
    """Return first EIA utility ID for this state and std_name. Raises if missing."""
    from utils.utility_codes import UTILITIES

    state_upper = state.upper()
    for u in UTILITIES:
        if u.get("state") != state_upper or u["std_name"] != std_name:
            continue
        eia_ids = u.get("eia_utility_ids")
        if not eia_ids:
            raise ValueError(
                f"Utility {std_name!r} in {state} has no eia_utility_ids in utils/utility_codes.py; "
                "add EIA IDs for Genability LSE lookup"
            )
        return int(eia_ids[0])
    raise ValueError(
        f"Utility {std_name!r} not found for state {state}. "
        f"Valid electric std_names: {get_utilities_for_state(state_upper, 'electric')}"
    )


def _lse_id_for_eia(auth: tuple[str, str], eia_id: int) -> int:
    """Resolve EIA ID to Arcadia LSE id. Raises if not found or ambiguous."""
    result = get_lses_page(
        auth,
        fields="min",
        searchOn=["code"],
        search=str(eia_id),
        startsWith=True,
        endsWith=True,
    )
    results = result.get("results") or []
    if len(results) == 0:
        raise ValueError(
            f"EIA ID {eia_id} not found in Arcadia. "
            "Check that the utility exists in Genability/Arcadia."
        )
    if len(results) > 1:
        names = [r.get("name", r.get("lseId")) for r in results]
        raise ValueError(
            f"EIA ID {eia_id} matches multiple LSEs in Arcadia: {names}. "
            "Use masterTariffId in genability_tariffs.yaml to pick one."
        )
    return int(results[0]["lseId"])


def _resolve_identifier_to_master_tariff_id(
    auth: tuple[str, str],
    lse_id: int,
    identifier: str | int,
    effective_on: datetime,
) -> int:
    """Resolve YAML identifier to Arcadia masterTariffId."""
    if isinstance(identifier, int):
        return identifier
    if str(identifier).strip().lower() == "default":
        tariffs = list(
            tariffs_paginate(
                auth,
                lseId=lse_id,
                fields="min",
                effectiveOn=effective_on,
                customerClasses=["RESIDENTIAL"],
                tariffTypes=["DEFAULT"],
            )
        )
        if not tariffs:
            raise ValueError(
                f"LSE {lse_id}: no RESIDENTIAL DEFAULT tariff found in Arcadia"
            )
        chosen = tariffs[0]
        chosen_id = int(chosen["masterTariffId"])
        chosen_name = chosen.get("tariffName") or "(no name)"
        if len(tariffs) > 1:
            all_names = [t.get("tariffName") or "(no name)" for t in tariffs]
            log.warning(
                "LSE %s: multiple RESIDENTIAL DEFAULT tariffs found: %s; picking first: %s (masterTariffId=%s)",
                lse_id,
                all_names,
                chosen_name,
                chosen_id,
            )
        else:
            log.info(
                "LSE %s: selected RESIDENTIAL DEFAULT tariff: %s (masterTariffId=%s)",
                lse_id,
                chosen_name,
                chosen_id,
            )
        return chosen_id
    # Match by tariff name (substring)
    name_substring = str(identifier).strip()
    all_tariffs = list(
        tariffs_paginate(
            auth,
            lseId=lse_id,
            fields="min",
            effectiveOn=effective_on,
        )
    )
    for t in all_tariffs:
        if name_substring.lower() in (t.get("tariffName") or "").lower():
            tid = int(t["masterTariffId"])
            log.info(
                "LSE %s: matched tariff by name: %s (masterTariffId=%s)",
                lse_id,
                t.get("tariffName") or "(no name)",
                tid,
            )
            return tid
    available = [t.get("tariffName") for t in all_tariffs[:20]]
    raise ValueError(
        f"No tariff name containing {name_substring!r} for LSE {lse_id}. "
        f"Available (first 20): {available}"
    )


def load_config(yaml_path: Path) -> dict[str, dict[str, str | int]]:
    """Load genability_tariffs.yaml; return { std_name: { tariff_key: identifier } }.

    Supports flat form (utility: default) and nested form (utility: { tariff_key: identifier }).
    """
    data = yaml.safe_load(yaml_path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"{yaml_path}: expected a YAML mapping")
    utilities: dict[str, dict[str, str | int]] = {}
    for u, v in data.items():
        if not isinstance(u, str):
            raise ValueError(f"{yaml_path}: utility key must be string, got {u!r}")
        if isinstance(v, dict):
            # Nested: utility -> { tariff_key: identifier }
            for tk, ident in v.items():
                if not isinstance(tk, str):
                    raise ValueError(
                        f"{yaml_path}: tariff_key must be string, got {tk!r}"
                    )
                if not (isinstance(ident, str) or isinstance(ident, int)):
                    raise ValueError(
                        f"{yaml_path}: identifier for {u}.{tk} must be 'default', an integer, "
                        f"or a tariff name string; got {type(ident).__name__}"
                    )
            utilities[u] = v
        else:
            # Flat: utility -> identifier (output file = <utility>.json)
            if not (isinstance(v, str) or isinstance(v, int)):
                raise ValueError(
                    f"{yaml_path}: value for {u!r} must be 'default', an integer (masterTariffId), "
                    f"or a tariff name string; got {type(v).__name__}"
                )
            utilities[u] = {u: v}
    return utilities


def _filename_stem(tariff_key: str, identifier: str | int, effective_date: date) -> str:
    """Return the output filename stem, e.g. rie_default_2025-01-01."""
    date_str = effective_date.isoformat()
    if isinstance(identifier, int):
        return f"{tariff_key}_{identifier}_{date_str}"
    ident_str = str(identifier).strip().lower()
    if ident_str == "default":
        return f"{tariff_key}_default_{date_str}"
    slug = (
        "".join(c if c.isalnum() or c in "._-" else "_" for c in identifier)
        .strip("_")
        .lower()
    )
    if not slug:
        slug = "default"
    return f"{tariff_key}_{slug}_{date_str}"


def fetch_genability_tariffs(
    yaml_path: Path,
    output_dir: Path,
    state: str,
    effective_date: date | None = None,
) -> None:
    """Load YAML config and fetch all listed Genability tariffs.

    Writes <key>_<identifier>_<date>.json per entry to output_dir.
    """
    if effective_date is None:
        effective_date = datetime.now(timezone.utc).date()
    effective_on = datetime(
        effective_date.year,
        effective_date.month,
        effective_date.day,
        tzinfo=timezone.utc,
    )

    log.info("Loading config from %s", yaml_path)
    log.info("Effective date: %s", effective_date.isoformat())
    state_upper = state.upper()
    utilities_config = load_config(yaml_path)
    total_tariffs = sum(len(m) for m in utilities_config.values())
    log.info(
        "Config: state=%s, %d utilit(ies), %d tariff(s) to fetch",
        state_upper,
        len(utilities_config),
        total_tariffs,
    )
    valid_std = set(get_utilities_for_state(state_upper, "electric"))
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    auth = _get_arcadia_credentials()

    for std_name, tariff_map in utilities_config.items():
        if std_name not in valid_std:
            raise ValueError(
                f"Utility {std_name!r} in YAML is not an electric utility for {state_upper}. "
                f"Valid std_names: {sorted(valid_std)}"
            )
        eia_id = _eia_id_for_utility(state_upper, std_name)
        log.info("Resolving %r (EIA %s) to Arcadia LSE...", std_name, eia_id)
        lse_id = _lse_id_for_eia(auth, eia_id)
        log.info("LSE id %s for %r", lse_id, std_name)

        for tariff_key, identifier in tariff_map.items():
            if isinstance(identifier, str) and identifier.isdigit():
                identifier = int(identifier)
            log.info("Resolving tariff %r -> %s...", tariff_key, identifier)
            master_tariff_id = _resolve_identifier_to_master_tariff_id(
                auth, lse_id, identifier, effective_on
            )
            log.info("Fetching full tariff masterTariffId=%s", master_tariff_id)
            results = list(
                tariffs_paginate(
                    auth,
                    masterTariffId=master_tariff_id,
                    effectiveOn=effective_on,
                    fields="ext",
                    populateProperties=True,
                    populateRates=True,
                )
            )
            if not results:
                raise ValueError(
                    f"masterTariffId {master_tariff_id} returned no tariff data"
                )
            stem = _filename_stem(tariff_key, identifier, effective_date)
            out_path = output_dir / f"{stem}.json"
            out_path.write_text(json.dumps(results, indent=2))
            log.info("Wrote %s", out_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch electric tariffs from Genability/Arcadia using genability_tariffs.yaml"
    )
    parser.add_argument(
        "path_yaml",
        type=Path,
        help="Path to genability_tariffs.yaml",
    )
    parser.add_argument(
        "path_output_dir",
        type=Path,
        help="Output directory for <utility>_<identifier>.json files (e.g. rie_default.json)",
    )
    parser.add_argument(
        "--state",
        "-s",
        required=True,
        help="Two-letter state (e.g. NY, RI)",
    )
    parser.add_argument(
        "--effective-date",
        type=date.fromisoformat,
        default=None,
        help="Effective date (YYYY-MM-DD) for tariff version. Defaults to today (UTC).",
    )
    parser.add_argument(
        "--list-utilities",
        action="store_true",
        help="Load YAML and list utilities (no fetch)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )
    if args.list_utilities:
        utilities = load_config(args.path_yaml)
        print(f"State: {args.state.upper()}")
        for u, m in utilities.items():
            print(f"  {u}: {list(m.keys())}")
        return
    fetch_genability_tariffs(
        args.path_yaml, args.path_output_dir, args.state, args.effective_date
    )


if __name__ == "__main__":
    main()
