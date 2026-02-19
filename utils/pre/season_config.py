"""Shared season configuration helpers (winter-first contract)."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import yaml

ALL_MONTHS: tuple[int, ...] = tuple(range(1, 13))
DEFAULT_TOU_WINTER_MONTHS: tuple[int, ...] = (1, 2, 3, 4, 5, 10, 11, 12)
DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS: tuple[int, ...] = (10, 11, 12, 1, 2, 3)


def parse_months_arg(value: str) -> list[int]:
    """Parse comma-separated month numbers from CLI input."""
    cleaned = value.strip()
    if cleaned == "":
        return []
    return [int(part.strip()) for part in cleaned.split(",")]


def normalize_winter_months(
    winter_months: Sequence[int] | None,
    *,
    default_winter_months: Sequence[int] | None = None,
) -> list[int]:
    """Validate and normalize a winter month list.

    Returns:
        Sorted unique month numbers in ``1..12``.
    """
    if winter_months is None:
        if default_winter_months is None:
            raise ValueError("winter_months must be provided")
        winter_months = list(default_winter_months)

    normalized = sorted(set(int(month) for month in winter_months))
    if not normalized:
        raise ValueError("winter_months must not be empty")
    if any(month < 1 or month > 12 for month in normalized):
        raise ValueError("winter_months must contain only integers in 1..12")
    if len(normalized) == len(ALL_MONTHS):
        raise ValueError("winter_months must not include all 12 months")
    return normalized


def derive_summer_months(winter_months: Sequence[int]) -> list[int]:
    """Return summer months as the complement of winter months."""
    winter_set = set(normalize_winter_months(winter_months))
    return [month for month in ALL_MONTHS if month not in winter_set]


def resolve_winter_summer_months(
    winter_months: Sequence[int] | None,
    *,
    default_winter_months: Sequence[int] | None = None,
) -> tuple[list[int], list[int]]:
    """Return normalized ``(winter_months, summer_months)``."""
    normalized_winter = normalize_winter_months(
        winter_months,
        default_winter_months=default_winter_months,
    )
    summer_months = derive_summer_months(normalized_winter)
    return normalized_winter, summer_months


def get_utility_periods_yaml_path(project_root: Path, state: str, utility: str) -> Path:
    """Return `<project_root>/rate_design/<state>/hp_rates/config/periods/<utility>.yaml`."""
    return (
        project_root
        / "rate_design"
        / state.lower()
        / "hp_rates"
        / "config"
        / "periods"
        / f"{utility.lower()}.yaml"
    )


def _load_periods_yaml(periods_yaml_path: Path) -> dict[str, Any]:
    with periods_yaml_path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(
            f"Invalid periods YAML at {periods_yaml_path}: expected top-level mapping"
        )
    return data


def load_winter_months_from_periods(
    periods_yaml_path: Path,
    *,
    default_winter_months: Sequence[int] | None = None,
) -> list[int]:
    """Load and validate winter months from periods YAML."""
    config = _load_periods_yaml(periods_yaml_path)
    raw = config.get("winter_months")
    if raw is None:
        return normalize_winter_months(
            None,
            default_winter_months=default_winter_months,
        )
    if not isinstance(raw, list):
        raise ValueError(
            f"Invalid `winter_months` in {periods_yaml_path}: expected list[int]"
        )
    return normalize_winter_months(cast(list[int], raw))


def load_tou_window_hours_from_periods(
    periods_yaml_path: Path,
    *,
    default_tou_window_hours: int,
) -> int:
    """Load TOU window hours from periods YAML with fallback default."""
    config = _load_periods_yaml(periods_yaml_path)
    raw = config.get("tou_window_hours")
    if raw is None:
        return default_tou_window_hours
    try:
        window = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid `tou_window_hours` in {periods_yaml_path}: expected integer"
        ) from exc
    if window < 1 or window > 23:
        raise ValueError(
            f"Invalid `tou_window_hours` in {periods_yaml_path}: expected 1..23"
        )
    return window
