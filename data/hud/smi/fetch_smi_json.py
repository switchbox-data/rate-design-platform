#!/usr/bin/env python3
"""
Fetch HUD State Median Income (SMI) data from the HUD API and save as JSON.

Calls fmr/listStates once, then for each (state_code, year) calls
il/statedata/{statecode}?year=year. Writes:
  - json/states.json — list of states (state_code, state_num, state_name)
  - json/fy={year}/{statecode}.json — raw API response per state per year

Minimizes API calls by caching; re-run convert only after fetch. Skips existing
JSON files so you can re-run after 429 (rate limit) to fill gaps. Requires
HUD_API_KEY in the environment or .env.

    uv run python fetch_smi_json.py --start-year 2017 --end-year 2025 --output json/
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from dotenv import load_dotenv
from tqdm import tqdm

# Backoff when API returns 429 Too Many Attempts
RETRY_MAX = 4
RETRY_BASE_SEC = 2

# API: 2017 is first year that returns 200; 2015/2016 return "Invalid year"
FY_MIN = 2017
FY_MAX = 2025
BASE_URL = "https://www.huduser.gov/hudapi/public"
LIST_STATES_PATH = "fmr/listStates"
IL_STATEDATA_PATH = "il/statedata"

# il/statedata is not available for these; skip to avoid noisy "failed" output
SKIP_STATE_CODES = {"AS", "DC", "GU", "MP", "PR", "VI"}


def _get_token() -> str:
    load_dotenv()
    token = os.getenv("HUD_API_KEY")
    if not token:
        print(
            "Error: HUD_API_KEY not set. Set it in the environment or .env.",
            file=sys.stderr,
        )
        sys.exit(1)
    return token


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def fetch_list_states(token: str) -> list[dict]:
    """Return list of states from fmr/listStates (state_code, state_name, state_num, category)."""
    url = f"{BASE_URL}/{LIST_STATES_PATH}"
    for attempt in range(RETRY_MAX):
        r = requests.get(url, headers=_headers(token), timeout=30)
        if r.status_code == 429 and attempt < RETRY_MAX - 1:
            time.sleep(RETRY_BASE_SEC**attempt)
            continue
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        return data.get("data", [])
    raise requests.RequestException("listStates failed after retries")


def fetch_statedata(token: str, state_code: str, year: int) -> dict | None:
    """Return il/statedata/{statecode} for the given year, or None on 4xx/5xx after retries."""
    url = f"{BASE_URL}/{IL_STATEDATA_PATH}/{state_code}"
    for attempt in range(RETRY_MAX):
        r = requests.get(
            url, headers=_headers(token), params={"year": year}, timeout=30
        )
        if r.ok:
            return r.json()
        if r.status_code == 429 and attempt < RETRY_MAX - 1:
            time.sleep(RETRY_BASE_SEC**attempt)
            continue
        return None
    return None


def main() -> int:
    args = _parse_args()
    token = _get_token()
    output_dir = Path(args.output)
    start_year = args.start_year
    end_year = args.end_year
    if start_year > end_year:
        print("Error: start-year must be <= end-year", file=sys.stderr)
        return 1

    # 1) Fetch or load list of states
    output_dir.mkdir(parents=True, exist_ok=True)
    states_path = output_dir / "states.json"
    if states_path.exists() and not args.force:
        with open(states_path) as f:
            states = json.load(f)
        print(f"Using existing {states_path} ({len(states)} states)")
    else:
        try:
            states = fetch_list_states(token)
        except requests.RequestException as e:
            print(f"Error fetching listStates: {e}", file=sys.stderr)
            return 1
        with open(states_path, "w") as f:
            json.dump(states, f, indent=2)
        print(f"Saved {len(states)} states to {states_path}")

    years = list(range(start_year, end_year + 1))
    for year in years:
        (output_dir / f"fy={year}").mkdir(parents=True, exist_ok=True)

    tasks: list[tuple[str, int]] = []
    for state in states:
        state_code = state.get("state_code") or state.get("statecode")
        if not state_code or state_code in SKIP_STATE_CODES:
            continue
        for year in years:
            out_file = output_dir / f"fy={year}" / f"{state_code}.json"
            if out_file.exists() and not args.force:
                continue
            tasks.append((state_code, year))

    if not tasks:
        print("Nothing to fetch (all files exist; use --force to re-fetch).")
        return 0

    workers = args.workers
    failed: list[tuple[str, int, str]] = []

    def do_one(sc: str, yr: int) -> tuple[str, int, str] | None:
        time.sleep(1.2)  # Throttle to avoid 429 (~1 req/s per worker)
        out_file = output_dir / f"fy={yr}" / f"{sc}.json"
        try:
            raw = fetch_statedata(token, sc, yr)
            if raw is None:
                return (sc, yr, "non-200 response")
            with open(out_file, "w") as f:
                json.dump(raw, f, indent=2)
            return None
        except Exception as e:
            return (sc, yr, str(e))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_task = {
            executor.submit(do_one, sc, yr): (sc, yr) for (sc, yr) in tasks
        }
        with tqdm(total=len(tasks), desc="Fetching SMI", unit="call") as pbar:
            for future in as_completed(future_to_task):
                result = future.result()
                if result is not None:
                    failed.append(result)
                pbar.update(1)

    if failed:
        print(f"\nFailed ({len(failed)}):", file=sys.stderr)
        for sc, yr, msg in failed[:25]:
            print(f"  {sc} fy={yr}: {msg}", file=sys.stderr)
        if len(failed) > 25:
            print(f"  ... and {len(failed) - 25} more", file=sys.stderr)
    print(f"\nDone. JSON under {output_dir.absolute()}")
    return 0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch HUD SMI (state-level income limits) from API to JSON"
    )
    p.add_argument(
        "--start-year",
        type=int,
        default=FY_MIN,
        metavar="YEAR",
        help=f"First fiscal year (default: {FY_MIN})",
    )
    p.add_argument(
        "--end-year",
        type=int,
        default=FY_MAX,
        metavar="YEAR",
        help=f"Last fiscal year (default: {FY_MAX})",
    )
    p.add_argument(
        "--output",
        "-o",
        metavar="DIR",
        default="json",
        help="Output directory for json/states.json and json/fy=<year>/<state>.json (default: json)",
    )
    p.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Re-fetch even if JSON file already exists",
    )
    p.add_argument(
        "--workers",
        "-j",
        type=int,
        default=4,
        metavar="N",
        help="Number of parallel requests (default: 4; lower if hitting 429)",
    )
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(main())
