"""
Fetch gas tariffs from Rate Acuity (URDB) non-interactively for any state.

Uses tariff_fetch's rateacuity and urdb APIs. --state and --utility (std_name from
utils/utility_codes.py) select the utility; Rate Acuity dropdown names come from
utility_codes.rate_acuity_utility_names. Writes one JSON per utility (array of
rates) to --output-dir as rateacuity_{state}_{std_name}.json. Use the install
script with --state to map those to tariff_key-named files for the pipeline.

Requires RATEACUITY_USERNAME and RATEACUITY_PASSWORD in the environment.
"""

from __future__ import annotations

import argparse
from typing import cast
import json
import logging
import os
from collections.abc import Collection
from datetime import date
from pathlib import Path
from statistics import mean

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


def list_utilities(state: str) -> list[str]:
    """Log in, select state, return utility names from the gas history dropdown (for mapping QA)."""
    username = os.environ.get("RATEACUITY_USERNAME")
    password = os.environ.get("RATEACUITY_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            "Set RATEACUITY_USERNAME and RATEACUITY_PASSWORD in the environment"
        )
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
    state: str,
    year: int,
    utility: str,
    tariffs: list[str] | None,
    output_dir: Path,
    *,
    apply_percentages: bool = False,
) -> None:
    """Fetch gas URDB rates for one utility and write one JSON (array of rates) into output_dir."""
    username = os.environ.get("RATEACUITY_USERNAME")
    password = os.environ.get("RATEACUITY_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            "Set RATEACUITY_USERNAME and RATEACUITY_PASSWORD in the environment"
        )

    state_upper = state.upper()
    valid_std = get_utilities_for_state(state_upper, "gas")
    if utility not in valid_std:
        raise ValueError(
            f"Utility {utility!r} is not a gas utility for {state}. "
            f"Valid std_names: {valid_std}"
        )

    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    rates: list[dict[str, object]] = []

    with create_context() as context:
        scraping_state = (
            LoginState(context)
            .login(username, password)
            .gas()
            .history()
            .select_state(state_upper)
        )
        utilities = [u for u in scraping_state.get_utilities() if u]
        selected_utility = _resolve_utility(state_upper, utilities, utility)
        scraping_state = scraping_state.select_utility(selected_utility)
        all_schedules = [s for s in scraping_state.get_schedules() if s]
        to_fetch = tariffs if tariffs else all_schedules
        if not to_fetch:
            raise ValueError(f"No tariffs to fetch for utility {selected_utility}")

        for tariff in to_fetch:
            if tariff not in all_schedules:
                log.warning(
                    "Tariff %r not in schedule list; skipping. Available: %s",
                    tariff,
                    all_schedules[:8],
                )
                continue
            log.info("Fetching %s", tariff)
            report = (
                scraping_state.select_schedule(tariff)
                .set_enddate(date(year, 12, 1))
                .set_number_of_comparisons(12)
                .set_frequency(1)
            )
            df = report.as_dataframe()
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
            urdb["name"] = tariff
            rates.append(cast(dict[str, object], urdb))

            scraping_state = (
                report.back_to_selections()
                .history()
                .select_state(state_upper)
                .select_utility(selected_utility)
            )

    out_path = output_dir / f"rateacuity_{state_upper}_{utility}.json"
    out_path.write_text(json.dumps(rates, indent=2))
    log.info("Wrote %d rate(s) to %s", len(rates), out_path)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(
        description="Fetch gas tariffs from Rate Acuity (URDB) non-interactively for any state."
    )
    parser.add_argument(
        "--state",
        required=True,
        help="Two-letter state (e.g. NY, RI). --utility must be a gas std_name from utils/utility_codes for this state.",
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
        help="Print utility names from Rate Acuity for --state and exit (use to verify mapping)",
    )
    parser.add_argument(
        "--utility",
        help="std_name from utils/utility_codes (e.g. coned, rie), or exact Rate Acuity name",
    )
    parser.add_argument(
        "--tariffs",
        nargs="*",
        default=None,
        help="Tariff/schedule names to fetch; if omitted, fetch all schedules for the utility",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory for <tariff_key>.json files (e.g. config/tariffs/gas); required when fetching",
    )
    parser.add_argument(
        "--apply-percentages",
        action="store_true",
        help="Apply percentage columns when present (default: false)",
    )
    args = parser.parse_args()
    if args.list_utilities:
        for u in list_utilities(args.state):
            print(u)
        return
    if not args.utility:
        parser.error("--utility is required unless --list-utilities is set")
    if not args.output_dir:
        parser.error("--output-dir is required for fetch")
    fetch_gas_urdb(
        state=args.state,
        year=args.year,
        utility=args.utility,
        tariffs=args.tariffs,
        output_dir=args.output_dir,
        apply_percentages=args.apply_percentages,
    )


if __name__ == "__main__":
    main()
