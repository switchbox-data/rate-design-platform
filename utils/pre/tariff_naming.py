"""Helpers for tariff-key and RI run-directory naming conventions."""

from __future__ import annotations

import re
from pathlib import Path

RI_RUN_NAME_PATTERN = re.compile(
    r"^(?P<state>[a-z]{2})_(?P<utility>[a-z0-9]+)_run_(?P<run_num>\d{2})_"
    r"(?P<run_type>[a-z0-9]+)_(?P<tariff_key>[a-z0-9_]+)_up(?P<upgrade>\d{2})_"
    r"y(?P<year>\d{4})$"
)


def derive_tariff_key_from_electric_tariff_filename(tariff_path: Path) -> str:
    """Derive tariff key from the electric tariff filename stem."""
    tariff_key = tariff_path.stem.strip()
    if not tariff_key:
        raise ValueError(f"Could not derive tariff key from path: {tariff_path}")
    return tariff_key


def build_ri_run_name(
    *,
    state: str,
    utility: str,
    run_num: int,
    run_type: str,
    tariff_key: str,
    upgrade: str,
    year_run: int,
) -> str:
    """Build RI run name that encodes tariff key for downstream parsing."""
    return (
        f"{state.lower()}_{utility.lower()}_run_{run_num:02d}_"
        f"{run_type.lower()}_{tariff_key}_up{upgrade}_y{year_run}"
    )


def parse_ri_run_name(run_name: str) -> dict[str, str]:
    """Parse RI run name and return captured fields."""
    match = RI_RUN_NAME_PATTERN.fullmatch(run_name)
    if match is None:
        raise ValueError(
            "Run directory name does not match RI convention: "
            "<state>_<utility>_run_<NN>_<run_type>_<tariff_key>_up<NN>_y<YYYY>"
        )
    return match.groupdict()


def parse_tariff_key_from_ri_run_name(run_name: str) -> str:
    """Extract tariff key from RI run-name convention."""
    return parse_ri_run_name(run_name)["tariff_key"]

