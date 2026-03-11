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

With --urdb, also converts each tariff to URDB v7 JSON (delivery-only and delivery+supply
variants) using the tariff-fetch Arcadia-to-URDB converter.

Requires ARCADIA_APP_ID and ARCADIA_APP_KEY in the environment.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Compatibility patches for tariff_fetch.urdb.arcadia (must precede imports)
# ---------------------------------------------------------------------------
import sys
import types as _types

# Python 3.12+ removed xdrlib; tariff_fetch.urdb.arcadia.library imports
# xdrlib.ConversionError — stub it so the import chain works.
if "xdrlib" not in sys.modules:
    _xdrlib = _types.ModuleType("xdrlib")
    _xdrlib.ConversionError = type("ConversionError", (Exception,), {})  # type: ignore[attr-defined]
    sys.modules["xdrlib"] = _xdrlib

import argparse
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import yaml
from pydantic import TypeAdapter
from tariff_fetch.arcadia.api import ArcadiaSignalAPI
from tariff_fetch.arcadia.schema.common import RateChargeClass
from tariff_fetch.arcadia.schema.tariff import TariffExtended

from utils.utility_codes import get_utilities_for_state

# tariff-fetch 0.4.1 bug: fixedcharge.py has ``from t import Library, Scenario``
# which is a broken import. Patch the ``t`` module before importing build helpers.
from tariff_fetch.urdb.arcadia.library import Library as _Library
from tariff_fetch.urdb.arcadia.library import PropertyValue as _PropertyValue
from tariff_fetch.urdb.arcadia.scenario import Scenario as _Scenario

if "t" not in sys.modules:
    _t_mod = _types.ModuleType("t")
    _t_mod.Library = _Library  # type: ignore[attr-defined]
    _t_mod.Scenario = _Scenario  # type: ignore[attr-defined]
    sys.modules["t"] = _t_mod

from tariff_fetch.urdb.arcadia.energyschedule import build_energy_schedule
from tariff_fetch.urdb.arcadia.fixedcharge import build_fixed_charge
import tariff_fetch.urdb.arcadia.library as _lib_mod
from tariff_fetch.urdb.arcadia.metadata import build_metadata

# ---------------------------------------------------------------------------
# Relax tariff-fetch Pydantic schemas
# ---------------------------------------------------------------------------
# The Arcadia API returns enum values and extra fields that tariff-fetch's
# strict schemas don't recognize (ONE_TIME charge period, SELL_EXPORT
# transaction type, NET_EXCESS charge class, SUPER_OFF_PEAK property period,
# edgePredominance, prorationRules).  Widen annotations and allow extras so
# rider-tariff fetches during URDB conversion don't fail validation.
#
# TypedDict inheritance copies parent annotations into child __annotations__,
# so patches must target the leaf classes Pydantic actually validates against.
from typing import Annotated, NotRequired

from pydantic import BeforeValidator

from tariff_fetch.arcadia.schema import tariff as _tariff_schema
from tariff_fetch.arcadia.schema import tariffproperty as _prop_schema
from tariff_fetch.arcadia.schema import tariffrate as _rate_schema
from tariff_fetch.arcadia.schema import timeofuse as _tou_schema
from tariff_fetch.arcadia.schema.validators import comma_separated_str

_relaxed_charge_class = NotRequired[
    Annotated[list[str], BeforeValidator(comma_separated_str)]
]
for _cls in (_tariff_schema.TariffExtended, _tariff_schema.TariffStandard):
    _cls.__annotations__["charge_period"] = str

for _cls in (_rate_schema.TariffRateExtended, _rate_schema.TariffRateStandard):
    _cls.__annotations__["charge_period"] = str
    _cls.__annotations__["charge_class"] = _relaxed_charge_class

_rate_schema.TariffRateExtended.__annotations__["transaction_type"] = str
_prop_schema.TariffPropertyStandard.__annotations__["period"] = NotRequired[str]

for _cls in (_tou_schema.TimeOfUseExtended, _tou_schema.TimeOfUseStandard):
    _cls.__annotations__["tou_type"] = str

for _cls in (
    _tariff_schema.TariffExtended,
    _tariff_schema.TariffStandard,
    _tariff_schema.TariffMinimal,
    _rate_schema.TariffRateExtended,
    _rate_schema.TariffRateStandard,
    _rate_schema.TariffRateMinimal,
    _tou_schema.TimeOfUseExtended,
    _tou_schema.TimeOfUseStandard,
):
    _cls.__pydantic_config__ = {**_cls.__pydantic_config__, "extra": "allow"}  # type: ignore[attr-defined]

# ---------------------------------------------------------------------------
# Use direct GET /tariffs/{id} instead of paginated search for riders
# ---------------------------------------------------------------------------
# tariff-fetch's TariffLibrary._fetch_tariff uses iter_pages (search endpoint).
# For tariffs with many versions the search paginates past page 1, which our
# API key can't access (403).  The direct endpoint works and doesn't paginate.
from pydantic import TypeAdapter as _TypeAdapter  # noqa: E402

import tariff_fetch.urdb.arcadia.rateutils as _ru_mod  # noqa: E402
from tariff_fetch.urdb.arcadia.library import TariffLibrary as _TariffLibrary  # noqa: E402

_tariff_list_adapter = _TypeAdapter(list[_tariff_schema.TariffExtended])


_tariff_cache: dict[int, _tariff_schema.TariffExtended] = {}
_forbidden_tariff_ids: set[int] = set()


def _fetch_tariff_direct(
    self: _TariffLibrary, tariff_id: int
) -> _tariff_schema.TariffExtended:
    if tariff_id in _forbidden_tariff_ids:
        raise PermissionError(f"403 cached for tariff {tariff_id}")
    if tariff_id in _tariff_cache:
        return _tariff_cache[tariff_id]
    try:
        raw = self.api._request(
            f"tariffs/{tariff_id}",
            fields="ext",
            populateProperties=True,
            populateRates=True,
        )
    except Exception as exc:
        if "403" in str(exc):
            _forbidden_tariff_ids.add(tariff_id)
            logging.getLogger(__name__).warning(
                "Rider tariff %d returned 403 — cached, will skip for remainder of run",
                tariff_id,
            )
        raise
    results = _tariff_list_adapter.validate_python(raw.get("results", []))
    if not results:
        from tariff_fetch.urdb.arcadia.exception import TariffNotFoundById

        raise TariffNotFoundById(tariff_id)
    _tariff_cache[tariff_id] = results[0]
    return results[0]


_TariffLibrary._fetch_tariff = _fetch_tariff_direct  # type: ignore[assignment]

# Patch rate iteration to skip individual riders that return 403.
_original_iter_rates = _ru_mod.tariff_iter_rates_for_dt


def _iter_rates_skip_forbidden(tariff, scenario, library, dt):
    rates = tariff.get("rates", [])
    for rate in rates:
        if not _ru_mod.rate_is_applied_to_scenario(rate, scenario, library):
            continue
        if not _ru_mod.rate_is_applied_to_datetime(rate, dt):
            continue
        if rate["rate_bands"]:
            yield rate
        elif rider_id := rate.get("rider_id"):
            try:
                rider_tariff = library.tariffs.get_tariff(rider_id)
            except Exception as exc:
                if "403" in str(exc) or isinstance(exc, PermissionError):
                    continue
                raise
            yield from _iter_rates_skip_forbidden(rider_tariff, scenario, library, dt)


_ru_mod.tariff_iter_rates_for_dt = _iter_rates_skip_forbidden  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# CHOICE property opt-out heuristic
# ---------------------------------------------------------------------------
_OPT_OUT_VALUES = {"none", "not applicable", "n/a", "non-participant", "no"}


def _pick_choice_value(choices: list[dict]) -> list[str]:
    """Pick the best default from a list of Arcadia CHOICE options.

    Prefers an opt-out / non-participant value over the first listed choice so
    that low-income credits, green-up programs, and similar opt-in riders are
    not accidentally activated.
    """
    for choice in choices:
        val = choice.get("value") or choice.get("displayValue")
        if val is not None and str(val).strip().lower() in _OPT_OUT_VALUES:
            return [str(val)]
    first_val = choices[0].get("value") or choices[0].get("displayValue")
    return [str(first_val)] if first_val is not None else []


# Replace the interactive prompt function used by Library.get_property with
# one that returns safe defaults (False for BOOLEAN, opt-out for CHOICE).
# This avoids hangs when converting tariffs with rider-level properties that
# aren't in the initial tariff's property list.
_original_prompt_property = _lib_mod._prompt_property


def _noninteractive_prompt_property(tariff_property):
    key = tariff_property.get("key_name") or tariff_property.get("keyName")
    data_type = tariff_property.get("data_type") or tariff_property.get("dataType")
    if data_type == "BOOLEAN":
        default = tariff_property.get("default_value") or tariff_property.get(
            "defaultValue"
        )
        result = default in ("true", True, "1") if default is not None else False
        logging.getLogger(__name__).info(
            "Auto-resolved BOOLEAN property %s -> %s", key, result
        )
        return result
    if data_type == "CHOICE":
        choices = tariff_property.get("choices") or []
        default = tariff_property.get("default_value") or tariff_property.get(
            "defaultValue"
        )
        if default is not None:
            result: list[str] = [str(default)]
        else:
            result = _pick_choice_value(choices) if choices else []
        logging.getLogger(__name__).info(
            "Auto-resolved CHOICE property %s -> %s", key, result
        )
        return result
    logging.getLogger(__name__).warning(
        "Unhandled property type %s for %s — falling back to interactive prompt",
        data_type,
        key,
    )
    return _original_prompt_property(tariff_property)


_lib_mod._prompt_property = _noninteractive_prompt_property  # type: ignore[attr-defined]

# Load .env from project root (same as utils/__init__.py)
_env_path = Path(__file__).resolve().parents[3] / ".env"
if _env_path.exists():
    from dotenv import load_dotenv

    load_dotenv(_env_path)

log = logging.getLogger(__name__)


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


def _lse_id_for_eia(api: ArcadiaSignalAPI, eia_id: int) -> int:
    """Resolve EIA ID to Arcadia LSE id. Raises if not found or ambiguous."""
    result = api.lses.get_page(
        fields="min",
        search_on=["code"],
        search=str(eia_id),
        starts_with=True,
        ends_with=True,
    )
    results = result.get("results") or []
    if len(results) == 0:
        raise ValueError(
            f"EIA ID {eia_id} not found in Arcadia. "
            "Check that the utility exists in Genability/Arcadia."
        )
    if len(results) > 1:
        names = [r.get("name", r.get("lse_id")) for r in results]
        raise ValueError(
            f"EIA ID {eia_id} matches multiple LSEs in Arcadia: {names}. "
            "Use masterTariffId in genability_tariffs.yaml to pick one."
        )
    return int(results[0]["lse_id"])


def _resolve_identifier_to_master_tariff_id(
    api: ArcadiaSignalAPI,
    lse_id: int,
    identifier: str | int,
    effective_on: datetime,
) -> int:
    """Resolve YAML identifier to Arcadia masterTariffId."""
    if isinstance(identifier, int):
        return identifier
    if str(identifier).strip().lower() == "default":
        tariffs = list(
            api.tariffs.iter_pages(
                lse_id=lse_id,
                effective_on=effective_on.date(),
                customer_classes=["RESIDENTIAL"],
                tariff_types=["DEFAULT"],
            )
        )
        if not tariffs:
            raise ValueError(
                f"LSE {lse_id}: no RESIDENTIAL DEFAULT tariff found in Arcadia"
            )
        chosen = tariffs[0]
        chosen_id = int(chosen["master_tariff_id"])
        chosen_name = chosen.get("tariff_name") or "(no name)"
        if len(tariffs) > 1:
            all_names = [t.get("tariff_name") or "(no name)" for t in tariffs]
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
    name_substring = str(identifier).strip()
    all_tariffs = list(
        api.tariffs.iter_pages(
            lse_id=lse_id,
            effective_on=effective_on.date(),
        )
    )
    for t in all_tariffs:
        if name_substring.lower() in (t.get("tariff_name") or "").lower():
            tid = int(t["master_tariff_id"])
            log.info(
                "LSE %s: matched tariff by name: %s (masterTariffId=%s)",
                lse_id,
                t.get("tariff_name") or "(no name)",
                tid,
            )
            return tid
    available = [t.get("tariff_name") for t in all_tariffs[:20]]
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


# ---------------------------------------------------------------------------
# URDB conversion helpers
# ---------------------------------------------------------------------------

_DELIVERY_CHARGE_CLASSES: set[RateChargeClass] = {
    "DISTRIBUTION",
    "TRANSMISSION",
    "OTHER",
}
_DELIVERY_SUPPLY_CHARGE_CLASSES: set[RateChargeClass] = {
    *_DELIVERY_CHARGE_CLASSES,
    "SUPPLY",
    "CONTRACTED",
}


def _extract_default_properties(
    tariff_data: list[dict],
) -> dict[str, _PropertyValue]:
    """Extract default property values from fetched tariff data to avoid interactive prompts.

    For CHOICE properties, picks opt-out values (e.g. "None", "Not Applicable") when
    available so that low-income credits and similar opt-in riders are not activated.
    Falls back to the first choice when no opt-out option exists.
    """
    properties: dict[str, _PropertyValue] = {}
    for tariff in tariff_data:
        for prop in tariff.get("properties", []):
            key = prop.get("key_name") or prop.get("keyName")
            if key is None or key in properties:
                continue
            data_type = prop.get("data_type") or prop.get("dataType")
            if data_type == "CHOICE":
                choices = prop.get("choices") or []
                default = prop.get("default_value") or prop.get("defaultValue")
                if default is not None:
                    properties[key] = [str(default)]
                elif choices:
                    picked = _pick_choice_value(choices)
                    if picked:
                        properties[key] = picked
            elif data_type == "BOOLEAN":
                default = prop.get("default_value") or prop.get("defaultValue")
                if default is not None:
                    properties[key] = default in ("true", True, "1")
    return properties


def _build_urdb_noninteractive(
    api: ArcadiaSignalAPI,
    master_tariff_id: int,
    year: int,
    charge_classes: set[RateChargeClass],
    properties: dict[str, _PropertyValue] | None = None,
) -> dict:
    """Build URDB JSON from an Arcadia tariff without interactive prompts."""
    scenario = _Scenario(
        master_tariff_id=master_tariff_id,
        year=year,
        apply_percentages=False,
        charge_classes=charge_classes,
    )
    library = _Library(api, properties=properties)

    urdb: dict = {}
    urdb.update(build_energy_schedule(scenario, library))
    urdb.update(build_fixed_charge(scenario, library))
    urdb.update(build_metadata(scenario, library))
    return urdb


def _write_urdb_json(urdb: dict, path: Path) -> None:
    """Write URDB dict as pretty-printed JSON with trailing newline."""
    path.write_text(json.dumps(urdb, indent=2) + "\n")
    log.info("Wrote URDB %s", path)


# ---------------------------------------------------------------------------
# Main fetch orchestrator
# ---------------------------------------------------------------------------


def fetch_genability_tariffs(
    yaml_path: Path,
    output_dir: Path,
    state: str,
    effective_date: date | None = None,
    *,
    urdb: bool = False,
    path_urdb_dir: Path | None = None,
) -> None:
    """Load YAML config and fetch all listed Genability tariffs.

    Writes <key>_<identifier>_<date>.json per entry to output_dir (Genability JSON).

    When urdb=True, also converts each tariff to URDB v7 JSON:
      - <tariff_key>_default.json         (delivery only)
      - <tariff_key>_default_supply.json   (delivery + supply)
    written to path_urdb_dir (defaults to output_dir).
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
    urdb_dir = (path_urdb_dir or output_dir).resolve()
    urdb_failures: list[str] = []
    if urdb:
        urdb_dir.mkdir(parents=True, exist_ok=True)
        log.info("URDB output directory: %s", urdb_dir)

    api = ArcadiaSignalAPI()
    _tariff_ext_adapter = TypeAdapter(list[TariffExtended])

    for std_name, tariff_map in utilities_config.items():
        if std_name not in valid_std:
            raise ValueError(
                f"Utility {std_name!r} in YAML is not an electric utility for {state_upper}. "
                f"Valid std_names: {sorted(valid_std)}"
            )
        eia_id = _eia_id_for_utility(state_upper, std_name)
        log.info("Resolving %r (EIA %s) to Arcadia LSE...", std_name, eia_id)
        lse_id = _lse_id_for_eia(api, eia_id)
        log.info("LSE id %s for %r", lse_id, std_name)

        for tariff_key, identifier in tariff_map.items():
            if isinstance(identifier, str) and identifier.isdigit():
                identifier = int(identifier)
            log.info("Resolving tariff %r -> %s...", tariff_key, identifier)
            master_tariff_id = _resolve_identifier_to_master_tariff_id(
                api, lse_id, identifier, effective_on
            )
            log.info("Fetching full tariff masterTariffId=%s", master_tariff_id)
            results = list(
                api.tariffs.iter_pages(
                    fields="ext",
                    master_tariff_id=master_tariff_id,
                    effective_on=effective_date,
                    populate_properties=True,
                    populate_rates=True,
                )
            )
            if not results:
                raise ValueError(
                    f"masterTariffId {master_tariff_id} returned no tariff data"
                )

            # Genability JSON
            stem = _filename_stem(tariff_key, identifier, effective_date)
            out_path = output_dir / f"{stem}.json"
            out_path.write_bytes(
                _tariff_ext_adapter.dump_json(results, by_alias=True, indent=2)
            )
            log.info("Wrote %s", out_path)

            # URDB conversion (delivery-only and delivery+supply)
            if urdb:
                props = _extract_default_properties(results)
                if props:
                    log.info("Pre-filled URDB properties: %s", list(props.keys()))

                for label_suffix, cc in (
                    ("default", _DELIVERY_CHARGE_CLASSES),
                    ("default_supply", _DELIVERY_SUPPLY_CHARGE_CLASSES),
                ):
                    urdb_filename = f"{tariff_key}_{label_suffix}.json"
                    log.info(
                        "Converting to URDB (%s) masterTariffId=%s ...",
                        label_suffix,
                        master_tariff_id,
                    )
                    try:
                        urdb_data = _build_urdb_noninteractive(
                            api,
                            master_tariff_id,
                            effective_date.year,
                            cc,
                            properties=props,
                        )
                        _write_urdb_json(urdb_data, urdb_dir / urdb_filename)
                    except Exception:
                        log.exception(
                            "URDB conversion FAILED for %s (%s) — skipping",
                            tariff_key,
                            label_suffix,
                        )
                        urdb_failures.append(f"{tariff_key} ({label_suffix})")
                        break  # skip the supply variant too if delivery failed

    if urdb_failures:
        log.warning(
            "URDB conversion failed for %d tariff(s): %s",
            len(urdb_failures),
            ", ".join(urdb_failures),
        )


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
        help="Output directory for <key>_<identifier>_<date>.json files (e.g. rie_default_2025-01-01.json)",
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
        "--urdb",
        action="store_true",
        default=False,
        help="Also convert each tariff to URDB v7 JSON (delivery-only and delivery+supply).",
    )
    parser.add_argument(
        "--path-urdb-dir",
        type=Path,
        default=None,
        help="Output directory for URDB JSON files. Defaults to path_output_dir.",
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
        args.path_yaml,
        args.path_output_dir,
        args.state,
        args.effective_date,
        urdb=args.urdb,
        path_urdb_dir=args.path_urdb_dir,
    )


if __name__ == "__main__":
    main()
