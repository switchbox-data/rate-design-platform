"""Helpers for subclass-aware validation.

The original validation flow assumed HP/non-HP subclasses everywhere. This
module centralizes subclass metadata so validation can also handle other NY
subclass splits, such as ``heating_type_v2`` electrified vs non-electric.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

SUBCLASS_COL = "subclass"


def _normalize_group_value(value: object) -> str:
    return str(value).strip().lower()


@dataclass(frozen=True, slots=True)
class SubclassSpec:
    """Describe how validation should map metadata values to subclass aliases."""

    group_col: str
    selectors: dict[str, tuple[str, ...]]

    @property
    def aliases(self) -> tuple[str, ...]:
        return tuple(self.selectors)

    @property
    def value_to_alias(self) -> dict[str, str]:
        return {
            _normalize_group_value(value): alias
            for alias, values in self.selectors.items()
            for value in values
        }


def legacy_hp_subclass_spec() -> SubclassSpec:
    """Return the historical HP/non-HP subclass convention."""
    return SubclassSpec(
        group_col="postprocess_group.has_hp",
        selectors={"hp": ("true",), "non-hp": ("false",)},
    )


def subclass_spec_from_raw(raw: object) -> SubclassSpec:
    """Build a :class:`SubclassSpec` from YAML-style ``subclass_config`` data."""
    if not isinstance(raw, dict):
        raise ValueError("subclass_config must be a mapping")

    raw_dict = {str(key): value for key, value in raw.items()}
    raw_group_col = str(raw_dict["group_col"])
    _prefix = "postprocess_group."
    group_col = (
        raw_group_col
        if raw_group_col.startswith(_prefix)
        else f"{_prefix}{raw_group_col}"
    )
    raw_selectors = raw_dict["selectors"]
    if not isinstance(raw_selectors, dict):
        raise ValueError("subclass_config.selectors must be a mapping")

    selectors = {
        str(alias): tuple(
            value.strip() for value in str(values).split(",") if value.strip()
        )
        for alias, values in raw_selectors.items()
    }
    return SubclassSpec(group_col=group_col, selectors=selectors)


def subclass_spec_from_run(
    run_dict: dict[str, Any],
    *,
    scenario_subclass_config: object | None = None,
) -> SubclassSpec | None:
    """Resolve subclass metadata for a run entry.

    Subclass runs may define ``subclass_config`` directly or inherit it from the
    scenario. Legacy HP/non-HP runs omit it entirely, so we preserve the old
    validator behavior by falling back to the historical boolean convention.
    """
    if not bool(run_dict.get("run_includes_subclasses", False)):
        return None

    raw = run_dict.get("subclass_config", scenario_subclass_config)
    if raw is None:
        return legacy_hp_subclass_spec()
    return subclass_spec_from_raw(raw)


def subclass_alias_expr(
    spec: SubclassSpec,
    *,
    source_col: str | None = None,
) -> pl.Expr:
    """Return a Polars expression mapping raw metadata values to subclass aliases."""
    normalized = (
        pl.col(source_col or spec.group_col)
        .cast(pl.Utf8, strict=False)
        .str.to_lowercase()
        .str.strip_chars()
    )

    expr = pl.lit(None).cast(pl.Utf8)
    for raw_value, alias in spec.value_to_alias.items():
        expr = pl.when(normalized == raw_value).then(pl.lit(alias)).otherwise(expr)
    return expr.alias(SUBCLASS_COL)


def with_subclass_column(
    metadata: pl.LazyFrame | pl.DataFrame,
    spec: SubclassSpec,
) -> pl.LazyFrame | pl.DataFrame:
    """Attach a normalized ``subclass`` column to metadata."""
    return metadata.with_columns(subclass_alias_expr(spec))


def display_subclass(alias: str) -> str:
    """Return a human-readable subclass label for plots and summaries."""
    special = {
        "hp": "HP",
        "non-hp": "Non-HP",
        "electric_heating": "Electric Heating",
        "non_electric_heating": "Non-Electric Heating",
    }
    return special.get(alias, alias.replace("_", " ").replace("-", " ").title())
