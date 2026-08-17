"""Narrow Polars/pandas scalar aggregates to concrete numeric types.

Polars stubs type ``Series.min()`` / ``max()`` / ``mean()`` / ``sum()`` as a
wide union (``int | float | Decimal | date | …``). Wrapping those results in
``float(...)`` / ``int(...)`` then fails ty's constructor checks. Call these
helpers at the boundary instead of scattering ``# type: ignore`` comments.
"""

from __future__ import annotations

from typing import Any, cast


def as_float(value: object) -> float:
    """Convert a scalar aggregate to ``float`` for arithmetic / formatting."""
    return float(cast(Any, value))


def as_int(value: object) -> int:
    """Convert a scalar aggregate to ``int`` for arithmetic / formatting."""
    return int(cast(Any, value))
