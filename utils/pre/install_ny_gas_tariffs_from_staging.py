"""
Convert Rate Acuity URDB staging output to tariff_key JSON files.

Reads JSON file(s) from a staging directory. Maps each rate to tariff_key using
the same rateacuity_tariffs.yaml as fetch_gas_tariffs_rateacuity.py (utility and
schedule name -> tariff_key). For a fresh fetch use fetch_gas_tariffs_rateacuity.py
instead; this script is for converting existing staging dirs only.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from utils.pre.fetch_gas_tariffs_rateacuity import load_config
from utils.utility_codes import get_rate_acuity_utility_names


def _build_utility_schedule_to_tariff_key(
    state: str, utilities_config: dict[str, dict[str, str]]
) -> dict[tuple[str, str], str]:
    """(rate_acuity_utility_name, schedule_name) -> tariff_key for lookup from staging JSON."""
    out: dict[tuple[str, str], str] = {}
    for utility_shortcode, tariff_map in utilities_config.items():
        candidates = get_rate_acuity_utility_names(state, utility_shortcode)
        for rate_acuity_name in candidates:
            for tariff_key, schedule_name in tariff_map.items():
                out[(rate_acuity_name, schedule_name)] = tariff_key
    return out


def install_from_file(
    path: Path,
    output_dir: Path,
    utility_schedule_to_tariff_key: dict[tuple[str, str], str],
) -> list[tuple[str, str]]:
    """Load one Rate Acuity gas URDB JSON, write one file per tariff_key using the map."""
    text = path.read_text()
    data = json.loads(text)
    if not isinstance(data, list):
        data = [data]
    written: list[tuple[str, str]] = []
    for rate in data:
        utility = rate.get("utility") or ""
        name = rate.get("name") or ""
        tariff_key = utility_schedule_to_tariff_key.get((utility, name))
        if not tariff_key:
            continue
        out_path = output_dir / f"{tariff_key}.json"
        out_path.write_text(json.dumps(rate, indent=2))
        written.append((tariff_key, str(out_path)))
    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Install gas URDB tariffs from staging into config/tariffs/gas."
    )
    parser.add_argument(
        "staging_dir",
        type=Path,
        help="Directory containing rateacuity_*.json or *.urdb.*.json file(s)",
    )
    parser.add_argument(
        "--yaml",
        type=Path,
        required=True,
        help="Path to rateacuity_tariffs.yaml for this state (same as fetch script)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for <tariff_key>.json files",
    )
    args = parser.parse_args()
    staging = args.staging_dir.resolve()
    if not staging.is_dir():
        raise SystemExit(f"Staging directory does not exist: {staging}")

    state, utilities_config = load_config(args.yaml.resolve())
    utility_schedule_to_tariff_key = _build_utility_schedule_to_tariff_key(
        state, utilities_config
    )

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    json_files = list(staging.glob("rateacuity_*.json")) + list(
        staging.glob("*.urdb.*.json")
    )
    if not json_files:
        for sub in staging.iterdir():
            if sub.is_dir():
                json_files.extend(sub.glob("rateacuity_*.json"))
                json_files.extend(sub.glob("*.urdb.*.json"))
    if not json_files:
        raise SystemExit(
            f"No rateacuity_*.json or *.urdb.*.json found in {staging} or its subdirs"
        )

    all_written: list[tuple[str, str]] = []
    for path in sorted(json_files):
        all_written.extend(
            install_from_file(path, output_dir, utility_schedule_to_tariff_key)
        )

    for tariff_key, out_path in sorted(all_written):
        print(f"  {tariff_key}.json -> {out_path}")


if __name__ == "__main__":
    main()
