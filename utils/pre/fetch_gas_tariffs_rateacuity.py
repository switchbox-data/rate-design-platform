"""
Fetch gas tariffs from Rate Acuity (URDB) using a state-specific YAML config.

Reads rateacuity_tariffs.yaml (utility_shortcode -> tariff_key -> exact schedule name).
Resolves shortcodes to Rate Acuity names via utils/utility_codes.py. Validates all
schedule names against the Rate Acuity dropdown before downloading; writes one
<tariff_key>.json per entry to the output directory.

Requires RATEACUITY_USERNAME and RATEACUITY_PASSWORD in the environment.
"""

from __future__ import annotations

import argparse
import ast
import json
import logging
import os
import re
from collections.abc import Collection
from datetime import date
from pathlib import Path
from statistics import mean

import yaml
from tariff_fetch.rateacuity import LoginState, create_context
from tariff_fetch.urdb.rateacuity_history_gas import build_urdb
from tariff_fetch.urdb.rateacuity_history_gas.history_data import (
    HistoryData,
    PercentageRow,
    Row,
)

from utils.utility_codes import get_rate_acuity_utility_names, get_utilities_for_state

# Load .env from project root (same as utils/__init__.py)
_env_path = Path(__file__).resolve().parents[2] / ".env"
if _env_path.exists():
    from dotenv import load_dotenv

    load_dotenv(_env_path)

log = logging.getLogger(__name__)


def _get_rateacuity_credentials() -> tuple[str, str]:
    """Return (username, password) from env."""
    username = os.environ.get("RATEACUITY_USERNAME")
    password = os.environ.get("RATEACUITY_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            "Set RATEACUITY_USERNAME and RATEACUITY_PASSWORD in the environment"
        )
    return (username, password)


def _resolve_utility(state: str, utilities: list[str], std_name: str) -> str:
    """Return exact utility string from dropdown; std_name from utils/utility_codes."""
    name = std_name.strip()
    if name in utilities:
        return name
    lower_opt = {opt.lower(): opt for opt in utilities}
    if name.lower() in lower_opt:
        return lower_opt[name.lower()]
    candidates = get_rate_acuity_utility_names(
        state.upper(), name.lower().replace(" ", "_")
    )
    for candidate in candidates:
        if candidate in utilities:
            return candidate
        if candidate.lower() in lower_opt:
            return lower_opt[candidate.lower()]
    raise ValueError(
        f"Utility {std_name!r} not found for {state}. Candidates: {candidates!r}; "
        f"dropdown: {utilities[:5]!r}{'...' if len(utilities) > 5 else ''}"
    )


def _reraise_with_formatted_options(e: ValueError) -> None:
    """Re-raise ValueError with 'Available options' list formatted as newline-separated lines."""
    msg = str(e)
    if "Available options are:" not in msg:
        raise
    prefix = "Available options are:"
    idx = msg.index(prefix) + len(prefix)
    rest = msg[idx:].strip()
    match = re.match(r"^(\[.*\])\s*$", rest, re.DOTALL)
    if match:
        try:
            options = ast.literal_eval(match.group(1))
            opts_text = "\n".join(f"  - {o!r}" for o in sorted(options))
            new_msg = msg[: msg.index(prefix)] + prefix + "\n" + opts_text
            raise ValueError(new_msg) from e
        except (ValueError, SyntaxError):
            pass
    raise


def _get_percentage_columns(
    rows: Collection[Row],
) -> list[tuple[str, str | None, float]]:
    return [
        (
            row.rate,
            row.location,
            mean(row.month_value_float(month) for month in range(12)),
        )
        for row in rows
        if isinstance(row, PercentageRow)
    ]


def load_config(yaml_path: Path) -> tuple[str, dict[str, dict[str, str]]]:
    """Load rateacuity_tariffs.yaml; return (state, { utility_shortcode: { tariff_key: schedule_name } })."""
    data = yaml.safe_load(yaml_path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"{yaml_path}: expected a YAML mapping")
    state = data.get("state")
    if not state or not isinstance(state, str):
        raise ValueError(f"{yaml_path}: top-level 'state' (e.g. NY, RI) is required")
    state_upper = state.upper()
    utilities = {k: v for k, v in data.items() if k != "state" and isinstance(v, dict)}
    for u, tariffs in utilities.items():
        if not all(
            isinstance(tk, str) and isinstance(sn, str) for tk, sn in tariffs.items()
        ):
            raise ValueError(
                f"{yaml_path}: under {u!r} expect tariff_key -> schedule name strings"
            )
    return state_upper, utilities


def list_utilities(state: str) -> list[str]:
    """Log in, select state, return utility names from the gas history dropdown."""
    username, password = _get_rateacuity_credentials()
    with create_context() as context:
        scraping_state = (
            LoginState(context)
            .login(username, password)
            .gas()
            .history()
            .select_state(state.upper())
        )
        return [u for u in scraping_state.get_utilities() if u]


def fetch_gas_urdb(
    yaml_path: Path,
    output_dir: Path,
    *,
    year: int = 2025,
    apply_percentages: bool = False,
) -> None:
    """Load YAML config and fetch all listed gas tariffs; write <tariff_key>.json to output_dir."""
    log.info("Loading config from %s", yaml_path)
    state_upper, utilities_config = load_config(yaml_path)
    total_tariffs = sum(len(m) for m in utilities_config.values())
    log.info(
        "Config: state=%s, %d utilit(ies), %d tariff(s) to fetch",
        state_upper,
        len(utilities_config),
        total_tariffs,
    )
    valid_std = get_utilities_for_state(state_upper, "gas")

    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[tuple[str, Path]] = []

    username, password = _get_rateacuity_credentials()
    log.info("Logging in to Rate Acuity and opening gas history...")

    with create_context() as context:
        scraping_state = (
            LoginState(context)
            .login(username, password)
            .gas()
            .history()
            .select_state(state_upper)
        )
        dropdown_utilities = [u for u in scraping_state.get_utilities() if u]
        log.info(
            "Selected state %s; found %d utilit(ies) in dropdown",
            state_upper,
            len(dropdown_utilities),
        )

        for utility_shortcode, tariff_map in utilities_config.items():
            if utility_shortcode not in valid_std:
                raise ValueError(
                    f"Utility {utility_shortcode!r} in YAML is not a gas utility for {state_upper}. "
                    f"Valid std_names: {valid_std}"
                )
            log.info("Looking up utility %r -> Rate Acuity name...", utility_shortcode)
            selected_utility = _resolve_utility(
                state_upper, dropdown_utilities, utility_shortcode
            )
            log.info("Utility %r: selected %r", utility_shortcode, selected_utility)
            try:
                scraping_state = scraping_state.select_utility(selected_utility)
            except ValueError as e:
                _reraise_with_formatted_options(e)
            all_schedules = [
                s
                for s in scraping_state.get_schedules()  # type: ignore[union-attr]
                if s
            ]
            schedule_names = list(tariff_map.values())
            log.info(
                "Utility %r: found %d schedule(s) in dropdown; validating %d tariff(s) from YAML",
                utility_shortcode,
                len(all_schedules),
                len(tariff_map),
            )

            # Validate all schedule names exist before downloading any for this utility.
            missing = [s for s in schedule_names if s not in all_schedules]
            if missing:
                matched = [s for s in schedule_names if s in all_schedules]
                lines = [
                    f"Some schedule names in YAML are not in the Rate Acuity list for {selected_utility!r}.",
                    "",
                    "Matched (will fetch):",
                    *([f"  - {s!r}" for s in matched] if matched else ["  (none)"]),
                    "",
                    "Not found (update rateacuity_tariffs.yaml to match the Schedules currently available on Rate Acuity):",
                    *[f"  - {s!r}" for s in missing],
                    "",
                    "Schedules currently available for this utility (alphabetical):",
                    *[f"  - {s!r}" for s in sorted(all_schedules)],
                ]
                raise ValueError("\n".join(lines))

            for idx, (tariff_key, schedule_name) in enumerate(tariff_map.items(), 1):
                log.info(
                    "Downloading tariff %d/%d for %r: %s -> %s",
                    idx,
                    len(tariff_map),
                    utility_shortcode,
                    schedule_name,
                    tariff_key,
                )
                report = (
                    scraping_state.select_schedule(schedule_name)  # type: ignore[union-attr]
                    .set_enddate(date(year, 12, 1))
                    .set_number_of_comparisons(12)
                    .set_frequency(1)
                )
                df = report.as_dataframe()
                csv_path = output_dir / f"{tariff_key}.csv"
                if csv_path.exists():
                    log.info("Overwriting existing %s", csv_path)
                df.write_csv(csv_path)
                log.info("Wrote raw CSV %s", csv_path)
                log.info("Converting %s to URDB...", tariff_key)
                hd = HistoryData(df)
                validation_errors = hd.validate_rows()
                if validation_errors:
                    for err in validation_errors:
                        log.warning("Validation: %s", err.row)
                rows = list(hd.rows())
                pct_columns = _get_percentage_columns(rows)
                apply_pct = apply_percentages and bool(pct_columns)
                urdb = build_urdb(rows, apply_pct)
                urdb["utility"] = selected_utility
                urdb["name"] = schedule_name
                out_path = output_dir / f"{tariff_key}.json"
                if out_path.exists():
                    log.info("Overwriting existing %s", out_path)
                out_path.write_text(json.dumps(urdb, indent=2))
                written.append((tariff_key, out_path))
                log.info("Wrote %s -> %s", tariff_key, out_path)

                # After this schedule: if more schedules for this utility, return to schedule dropdown;
                # if last schedule for this utility, return to state-only so next utility sees utility dropdown.
                scraping_state = (
                    report.back_to_selections().history().select_state(state_upper)
                )
                if idx < len(tariff_map):
                    scraping_state = scraping_state.select_utility(selected_utility)

    log.info("Done: wrote %d tariff file(s) to %s", len(written), output_dir)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(
        description="Fetch gas tariffs from Rate Acuity using a state rateacuity_tariffs.yaml."
    )
    parser.add_argument(
        "yaml_path",
        type=Path,
        help="Path to rateacuity_tariffs.yaml (state + utility_shortcode -> tariff_key -> schedule name).",
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Output directory for <tariff_key>.json files (e.g. config/tariffs/gas).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Supply year for rates (default: 2025)",
    )
    parser.add_argument(
        "--list-utilities",
        action="store_true",
        help="Ignore YAML; print Rate Acuity utility names for the state in YAML and exit.",
    )
    parser.add_argument(
        "--apply-percentages",
        action="store_true",
        help="Apply percentage columns when present (default: false)",
    )
    args = parser.parse_args()

    if args.list_utilities:
        state, _ = load_config(args.yaml_path)
        for u in list_utilities(state):
            print(u)
        return

    args.yaml_path = args.yaml_path.resolve()
    if not args.yaml_path.is_file():
        raise SystemExit(f"YAML file not found: {args.yaml_path}")

    fetch_gas_urdb(
        args.yaml_path,
        args.output_dir,
        year=args.year,
        apply_percentages=args.apply_percentages,
    )


if __name__ == "__main__":
    main()
