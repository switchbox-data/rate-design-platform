"""Structure-specific tariff derivation handlers.

Each handler implements the derivation logic for one tariff structure type
(seasonal, flat, TOU, etc.).  The pipeline dispatches to handlers via
``derive_subgroup_tariff()`` — it never needs to know the details.

To add a new structure:
1. Write a handler function matching the ``StructureHandler`` protocol.
2. Add it to ``STRUCTURE_HANDLERS``.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from utils.mid.compute_subclass_rr import (
    compute_subclass_flat_discount_inputs,
    compute_subclass_seasonal_discount_inputs,
)
from utils.pre.create_tariff import create_flat_rate, create_seasonal_rate

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Common context passed to every handler
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DeriveContext:
    """Everything a structure handler needs to produce a tariff JSON.

    Built once per (subgroup × variant) by the pipeline's derive flow,
    then passed to the dispatched handler.  Handlers should not reach
    outside this context for inputs.
    """

    run_dir: Path
    """Dependency's precalc output dir (delivery or supply)."""

    base_tariff_path: Path
    """Dependency's calibrated tariff JSON (used as base for derived rate)."""

    stem: str
    """Output tariff label/stem (e.g. ``bge_hp_seasonal_percustomer``)."""

    out_path: Path
    """Where to write the resulting tariff JSON."""

    resstock_base: str
    """ResStock base path (for load curves in discount computation)."""

    state: str
    """Uppercase state code (e.g. ``MD``)."""

    upgrade: str
    """Upgrade ID for precalc stage (e.g. ``00``)."""

    group_col: str
    """Subclass group column (e.g. ``has_hp``)."""

    subclass_value: str
    """This subgroup's alias (e.g. ``hp``)."""

    bat_col: str
    """BAT cross-subsidy column (e.g. ``BAT_percustomer``)."""

    group_value_to_subclass: dict[str, str]
    """Mapping of raw group values to subclass aliases."""

    utility: str
    """Utility code (e.g. ``bge``)."""

    winter_months: tuple[int, ...]
    """Winter months from periods.yaml (e.g. ``(10, 11, 12, 1, 2, 3, 4, 5)``)."""


# ---------------------------------------------------------------------------
# Handler protocol
# ---------------------------------------------------------------------------


class StructureHandler(Protocol):
    """Callable that derives a tariff JSON from a ``DeriveContext``."""

    def __call__(self, ctx: DeriveContext) -> Path: ...


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _handle_seasonal(ctx: DeriveContext) -> Path:
    """Derive a 2-period seasonal tariff from BAT cross-subsidy inputs."""
    inputs = compute_subclass_seasonal_discount_inputs(
        run_dir=ctx.run_dir,
        resstock_base=ctx.resstock_base,
        state=ctx.state,
        upgrade=ctx.upgrade,
        group_col=ctx.group_col,
        subclass_value=ctx.subclass_value,
        cross_subsidy_col=ctx.bat_col,
        group_value_to_subclass=ctx.group_value_to_subclass,
        base_tariff_json_path=ctx.base_tariff_path,
        winter_months=ctx.winter_months,
    )
    row = inputs.row(0, named=True)
    base_tariff = json.loads(ctx.base_tariff_path.read_text(encoding="utf-8"))
    tariff = create_seasonal_rate(
        base_tariff=base_tariff,
        label=ctx.stem,
        winter_rate=float(row["winter_rate"]),
        summer_rate=float(row["summer_rate"]),
        winter_months=list(ctx.winter_months),
    )
    ctx.out_path.parent.mkdir(parents=True, exist_ok=True)
    ctx.out_path.write_text(json.dumps(tariff, indent=2) + "\n", encoding="utf-8")
    log.info("derive[seasonal]: wrote %s", ctx.out_path.name)
    return ctx.out_path


def _handle_flat(ctx: DeriveContext) -> Path:
    """Derive a flat-discount tariff from BAT cross-subsidy inputs."""
    inputs = compute_subclass_flat_discount_inputs(
        run_dir=ctx.run_dir,
        resstock_base=ctx.resstock_base,
        state=ctx.state,
        upgrade=ctx.upgrade,
        group_col=ctx.group_col,
        subclass_value=ctx.subclass_value,
        cross_subsidy_col=ctx.bat_col,
        group_value_to_subclass=ctx.group_value_to_subclass,
        base_tariff_json_path=ctx.base_tariff_path,
    )
    row = inputs.row(0, named=True)
    base_tariff = json.loads(ctx.base_tariff_path.read_text(encoding="utf-8"))
    tariff = create_flat_rate(
        base_tariff=base_tariff,
        label=ctx.stem,
        volumetric_rate=float(row["flat_rate"]),
    )
    ctx.out_path.parent.mkdir(parents=True, exist_ok=True)
    ctx.out_path.write_text(json.dumps(tariff, indent=2) + "\n", encoding="utf-8")
    log.info("derive[flat]: wrote %s", ctx.out_path.name)
    return ctx.out_path


# ---------------------------------------------------------------------------
# Registry and dispatch
# ---------------------------------------------------------------------------

STRUCTURE_HANDLERS: dict[str, StructureHandler] = {
    "seasonal": _handle_seasonal,
    "flat": _handle_flat,
}


def derive_subgroup_tariff(structure: str, ctx: DeriveContext) -> Path:
    """Dispatch tariff derivation to the appropriate structure handler.

    Raises ``ValueError`` if the structure has no registered handler.
    """
    handler = STRUCTURE_HANDLERS.get(structure)
    if handler is None:
        raise ValueError(
            f"No handler registered for structure {structure!r}. "
            f"Known structures: {sorted(STRUCTURE_HANDLERS)}"
        )
    return handler(ctx)
