"""Naming helpers for run directories and electric tariff keys.

This module centralizes the run-name convention so generation and parsing stay
in sync across utilities.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TypedDict

RI_RUN_NAME_PATTERN = re.compile(
    r"^(?P<state>[a-z]{2})_(?P<utility>[a-z0-9]+)_run_(?P<run_num>\d{2})_"
    r"(?P<run_type>[a-z0-9]+)_(?P<tariff_key>[a-z0-9_]+)(?P<supply>_supply)?_up(?P<upgrade>\d{2})_"
    r"y(?P<year>\d{4})$"
)


class RunNameParts(TypedDict):
    """Structured fields parsed from a run name."""

    state: str
    utility: str
    run_num: str
    run_type: str
    tariff_key: str
    supply: bool
    upgrade: str
    year: str


def derive_tariff_key_from_electric_tariff_filename(tariff_path: Path) -> str:
    """Return tariff key from an electric tariff path (file stem)."""
    tariff_key = tariff_path.stem.strip()
    if not tariff_key:
        raise ValueError(f"Could not derive tariff key from path: {tariff_path}")
    return tariff_key


def build_run_name(
    *,
    state: str,
    utility: str,
    run_num: int,
    run_type: str,
    tariff_key: str,
    supply: bool,
    upgrade: str,
    year_run: int,
) -> str:
    """Build run name with optional supply suffix.

    Format:
    ``<state>_<utility>_run_<NN>_<run_type>_<tariff_key>[_supply]_up<NN>_y<YYYY>``
    """
    supply_segment = "_supply" if supply else ""
    return (
        f"{state.lower()}_{utility.lower()}_run_{run_num:02d}_"
        f"{run_type.lower()}_{tariff_key}{supply_segment}_up{upgrade}_y{year_run}"
    )


def parse_run_name(run_name: str) -> RunNameParts:
    """Parse a run name into typed fields."""
    match = RI_RUN_NAME_PATTERN.fullmatch(run_name)
    if match is None:
        raise ValueError(
            "Run directory name does not match RI convention: "
            "<state>_<utility>_run_<NN>_<run_type>_<tariff_key>[_supply]_up<NN>_y<YYYY>"
        )
    parts = match.groupdict()
    return RunNameParts(
        state=parts["state"],
        utility=parts["utility"],
        run_num=parts["run_num"],
        run_type=parts["run_type"],
        tariff_key=parts["tariff_key"],
        supply=bool(parts["supply"]),
        upgrade=parts["upgrade"],
        year=parts["year"],
    )


def parse_tariff_key_from_run_name(run_name: str) -> str:
    """Extract tariff key from a valid run name."""
    return parse_run_name(run_name)["tariff_key"]
