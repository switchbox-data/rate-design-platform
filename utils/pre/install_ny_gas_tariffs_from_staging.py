"""
Install gas tariffs from Rate Acuity URDB staging output into config/tariffs/gas.

Reads JSON file(s) from a staging directory (array of URDB rates from
tariff_fetch.cli_gas --urdb or other exports). Maps each rate to tariff_key
using --state and writes one JSON per tariff. For one-step fetch+write use
fetch_gas_tariffs_rateacuity.py with --state and --output-dir.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from utils.pre.ny_gas_tariff_mapping import match_tariff_key as match_tariff_key_ny
from utils.pre.ri_gas_tariff_mapping import match_tariff_key as match_tariff_key_ri


def install_from_file(
    path: Path, output_dir: Path, state: str
) -> list[tuple[str, str]]:
    """Load one Rate Acuity gas URDB JSON (array of rates), write one file per tariff_key."""
    text = path.read_text()
    data = json.loads(text)
    if not isinstance(data, list):
        data = [data]
    written: list[tuple[str, str]] = []
    state_upper = state.upper()
    for rate in data:
        utility = rate.get("utility") or ""
        name = rate.get("name") or ""
        tariff_key = (
            match_tariff_key_ny(utility, name)
            if state_upper == "NY"
            else match_tariff_key_ri(utility, name)
        )
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
        help="Directory containing rateacuity_*.urdb.*.json or *.json file(s)",
    )
    parser.add_argument(
        "--state",
        required=True,
        help="Two-letter state (e.g. NY, RI) for tariff_key mapping",
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

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Staging may be a flat dir or contain per-utility subdirs (e.g. coned/, kedny/).
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
        all_written.extend(install_from_file(path, output_dir, args.state))

    for tariff_key, out_path in sorted(all_written):
        print(f"  {tariff_key}.json -> {out_path}")


if __name__ == "__main__":
    main()
