"""Copy calibrated tariff from a run directory into state tariff config.

CAIRO writes ``tariff_final_config.json`` in its internal PySAM-oriented shape.
This utility converts that payload into URDB ``items`` format and saves it as
``<tariff_key>_calibrated.json`` under the state electric tariffs directory.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

from cloudpathlib import S3Path

from utils import get_project_root
from utils.pre.tariff_naming import parse_tariff_key_from_run_name

CAIRO_MAX_USAGE_SENTINEL = 1e38
CAIRO_TO_URDB_UNIT = {
    0: "kWh",
    1: "kWh/kW",
    2: "kWh daily",
    3: "kWh/kW daily",
}
METERING_RULES = {
    0: "Net Metering",
    2: "Net Billing Instantaneous",
    3: "Net Billing Hourly",
    4: "Buy All Sell All",
}


def _resolve_path_or_s3(path_value: str) -> Path | S3Path:
    """Resolve a CLI path argument to local ``Path`` or ``S3Path``."""
    return S3Path(path_value) if path_value.startswith("s3://") else Path(path_value)


def _read_json(path: Path | S3Path) -> dict[str, Any]:
    """Read JSON payload from local disk or S3."""
    if isinstance(path, S3Path):
        return json.loads(path.read_text())
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    """Write formatted JSON to local disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def _to_int(value: Any, field_name: str) -> int:
    """Parse a value as integer with field-aware error reporting."""
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer for {field_name}: {value}") from exc


def _to_float(value: Any, field_name: str) -> float:
    """Parse a value as float with field-aware error reporting."""
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid float for {field_name}: {value}") from exc


def _convert_schedule_to_urdb(schedule: Any, field_name: str) -> list[list[int]]:
    """Convert CAIRO/PySAM 1-based 12x24 schedule to URDB 0-based schedule."""
    if not isinstance(schedule, list) or len(schedule) != 12:
        raise ValueError(f"{field_name} must contain 12 monthly rows")

    urdb_schedule: list[list[int]] = []
    for month_idx, row in enumerate(schedule, start=1):
        if not isinstance(row, list) or len(row) != 24:
            raise ValueError(f"{field_name}[{month_idx}] must contain 24 hourly values")
        urdb_row = []
        for hour_value in row:
            period_idx = _to_int(hour_value, field_name)
            urdb_row.append(period_idx - 1)
        urdb_schedule.append(urdb_row)
    return urdb_schedule


def _convert_tou_matrix_to_urdb(
    tou_mat: Any,
) -> list[list[dict[str, Any]]]:
    """Convert CAIRO ``ur_ec_tou_mat`` rows to URDB ``energyratestructure``."""
    if not isinstance(tou_mat, list) or not tou_mat:
        raise ValueError("CAIRO tariff is missing non-empty `ur_ec_tou_mat`")

    rates_by_period: dict[int, dict[int, dict[str, Any]]] = {}
    max_period = 0

    for row in tou_mat:
        if not isinstance(row, list) or len(row) < 7:
            raise ValueError("Each `ur_ec_tou_mat` row must have 7 values")
        period_1_based = _to_int(row[0], "ur_ec_tou_mat period")
        tier_1_based = _to_int(row[1], "ur_ec_tou_mat tier")
        max_usage = _to_float(row[2], "ur_ec_tou_mat max_usage")
        units = _to_int(row[3], "ur_ec_tou_mat units")
        rate = _to_float(row[4], "ur_ec_tou_mat rate")
        adj = _to_float(row[5], "ur_ec_tou_mat adjustment")
        sell = _to_float(row[6], "ur_ec_tou_mat sell")

        if units not in CAIRO_TO_URDB_UNIT:
            raise ValueError(f"Unsupported usage unit code in ur_ec_tou_mat: {units}")

        period_idx = period_1_based - 1
        tier_idx = tier_1_based - 1
        max_period = max(max_period, period_idx)
        period_rows = rates_by_period.setdefault(period_idx, {})
        entry: dict[str, Any] = {
            "rate": rate,
            "adj": adj,
            "unit": CAIRO_TO_URDB_UNIT[units],
        }
        if not math.isclose(max_usage, CAIRO_MAX_USAGE_SENTINEL, rel_tol=0.0, abs_tol=1e-6):
            entry["max"] = max_usage
        if not math.isclose(sell, 0.0, rel_tol=0.0, abs_tol=1e-9):
            entry["sell"] = sell
        period_rows[tier_idx] = entry

    urdb_rate_structure: list[list[dict[str, Any]]] = []
    for period_idx in range(max_period + 1):
        tiers = rates_by_period.get(period_idx, {})
        if not tiers:
            raise ValueError(f"Missing period {period_idx + 1} in ur_ec_tou_mat")
        max_tier = max(tiers.keys())
        period_tiers: list[dict[str, Any]] = []
        for tier_idx in range(max_tier + 1):
            if tier_idx not in tiers:
                raise ValueError(
                    f"Missing tier {tier_idx + 1} for period {period_idx + 1} "
                    "in ur_ec_tou_mat"
                )
            period_tiers.append(tiers[tier_idx])
        urdb_rate_structure.append(period_tiers)
    return urdb_rate_structure


def _to_urdb_tariff(
    cairo_tariff: dict[str, Any],
    *,
    tariff_key: str,
) -> dict[str, Any]:
    """Build a URDB ``items`` payload from one CAIRO tariff definition."""
    weekday = _convert_schedule_to_urdb(
        cairo_tariff.get("ur_ec_sched_weekday"),
        "ur_ec_sched_weekday",
    )
    weekend = _convert_schedule_to_urdb(
        cairo_tariff.get("ur_ec_sched_weekend"),
        "ur_ec_sched_weekend",
    )
    rate_structure = _convert_tou_matrix_to_urdb(cairo_tariff.get("ur_ec_tou_mat"))

    item: dict[str, Any] = {
        "label": tariff_key,
        "name": tariff_key,
        "uri": "",
        "sector": "Residential",
        "servicetype": "Bundled",
        "utility": "GenericUtility",
        "country": "USA",
        "energyweekdayschedule": weekday,
        "energyweekendschedule": weekend,
        "energyratestructure": rate_structure,
    }
    if "ur_monthly_fixed_charge" in cairo_tariff:
        item["fixedchargefirstmeter"] = _to_float(
            cairo_tariff["ur_monthly_fixed_charge"],
            "ur_monthly_fixed_charge",
        )
        item["fixedchargeunits"] = "$/month"
    if "ur_monthly_min_charge" in cairo_tariff:
        item["mincharge"] = _to_float(
            cairo_tariff["ur_monthly_min_charge"], "mincharge"
        )
        item["minchargeunits"] = "$/month"
    elif "ur_annual_min_charge" in cairo_tariff:
        item["mincharge"] = _to_float(cairo_tariff["ur_annual_min_charge"], "mincharge")
        item["minchargeunits"] = "$/year"
    if "ur_metering_option" in cairo_tariff:
        metering = _to_int(cairo_tariff["ur_metering_option"], "ur_metering_option")
        if metering in METERING_RULES:
            item["dgrules"] = METERING_RULES[metering]
    return {"items": [item]}


def _extract_cairo_tariff(payload: dict[str, Any], tariff_key: str) -> dict[str, Any]:
    """Extract the single CAIRO tariff dict from ``tariff_final_config`` payload."""
    if tariff_key in payload and isinstance(payload[tariff_key], dict):
        return payload[tariff_key]
    if len(payload) == 1:
        only_value = next(iter(payload.values()))
        if isinstance(only_value, dict):
            return only_value
    raise ValueError(
        "Could not locate CAIRO tariff payload by tariff_key in tariff_final_config.json"
    )


def _convert_cairo_payload_to_urdb(
    payload: dict[str, Any],
    *,
    tariff_key: str,
) -> dict[str, Any]:
    """Convert CAIRO-native tariff payload to URDB payload."""
    if "items" in payload:
        return payload

    cairo_tariff = _extract_cairo_tariff(payload, tariff_key)
    return _to_urdb_tariff(cairo_tariff, tariff_key=tariff_key)


def _default_destination_dir(state: str) -> Path:
    """Return the default local config destination for calibrated electric tariffs."""
    state_code = state.lower()
    return (
        get_project_root()
        / "rate_design"
        / state_code
        / "hp_rates"
        / "config"
        / "tariffs"
        / "electric"
    )


def copy_calibrated_tariff_from_run_dir(
    run_dir: Path | S3Path,
    *,
    state: str,
    destination_dir: Path | None = None,
) -> Path:
    """Convert and copy calibrated tariff from ``run_dir`` into config tariffs."""
    run_name = run_dir.name
    tariff_key = parse_tariff_key_from_run_name(run_name)

    tariff_final_config_path = run_dir / "tariff_final_config.json"
    cairo_payload = _read_json(tariff_final_config_path)
    urdb_payload = _convert_cairo_payload_to_urdb(cairo_payload, tariff_key=tariff_key)

    target_dir = (
        destination_dir if destination_dir is not None else _default_destination_dir(state)
    )
    output_path = target_dir / f"{tariff_key}_calibrated.json"
    return _write_json(output_path, urdb_payload)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Copy <run_dir>/tariff_final_config.json into state tariff config as "
            "<tariff_key>_calibrated.json using the RI run-directory naming convention."
        )
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Run output directory path (local or s3://...).",
    )
    parser.add_argument(
        "--state",
        required=True,
        help="Two-letter state code used to resolve rate_design/<state>/... output path.",
    )
    parser.add_argument(
        "--destination-dir",
        default="",
        help=(
            "Optional local directory override for output tariff file. "
            "Defaults to rate_design/<state>/hp_rates/config/tariffs/electric."
        ),
    )
    args = parser.parse_args()

    run_dir = _resolve_path_or_s3(args.run_dir)
    destination_dir = Path(args.destination_dir) if args.destination_dir else None
    output_path = copy_calibrated_tariff_from_run_dir(
        run_dir,
        state=args.state,
        destination_dir=destination_dir,
    )
    print(f"Copied calibrated tariff to: {output_path}")


if __name__ == "__main__":
    main()
