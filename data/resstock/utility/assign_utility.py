"""Utility assignment orchestration for the ResStock pipeline.

Uses dynamic module import to dispatch to state-specific implementations.
Each state's ``utility_assignment`` config in ``state_configs.yaml`` specifies
the Python module to import (via ``module``) and keyword arguments to pass
(via ``kwargs``).  The target module must expose an ``assign_utility()``
function with the signature ``assign_utility(metadata, **kwargs) -> LazyFrame``.

Callers (e.g. ``data/resstock/main.py``) should import only ``assign_utility``
and ``SUPPORTED_UTILITY_STATES`` from here; state-specific modules are an
implementation detail resolved at runtime.
"""

from __future__ import annotations

import importlib

import polars as pl

from data.resstock.utils import load_state_configs

_STATE_CONFIGS = load_state_configs()

# Any state whose config entry contains a ``utility_assignment`` key (even if
# the value only has ``module`` and no ``kwargs``) is considered supported.
SUPPORTED_UTILITY_STATES: frozenset[str] = frozenset(
    state for state, cfg in _STATE_CONFIGS.items() if "utility_assignment" in cfg
)


def assign_utility(
    state: str,
    metadata: pl.LazyFrame,
    *,
    path_s3_gis_dir: str | None = None,
    electric_poly_filename: str | None = None,
    gas_poly_filename: str | None = None,
) -> pl.LazyFrame:
    """Assign electric and gas utilities to ResStock buildings for a given state.

    Dynamically imports the state's module from ``state_configs.yaml`` and
    calls its ``assign_utility(metadata, **kwargs)``.  CLI values for
    ``electric_poly_filename`` / ``gas_poly_filename`` / ``path_s3_gis_dir``
    override or supplement the YAML kwargs when provided.

    Returns a LazyFrame with all original metadata columns plus
    ``sb.electric_utility`` and ``sb.gas_utility``.

    Args:
        state: 2-letter state code (e.g. ``"NY"``, ``"RI"``).
        metadata: ResStock metadata LazyFrame (from ``metadata-sb.parquet``).
        path_s3_gis_dir: S3 directory containing utility polygon CSV files.
            Passed through to the state module when provided.
        electric_poly_filename: Override for the electric polygon CSV filename.
            Replaces the YAML default when provided.
        gas_poly_filename: Override for the gas polygon CSV filename.
            Replaces the YAML default when provided.

    Raises:
        ValueError: If ``state`` is not in ``SUPPORTED_UTILITY_STATES``, or if
            the state's config is missing a ``module`` key.
    """
    if state not in SUPPORTED_UTILITY_STATES:
        raise ValueError(
            f"Utility assignment is not implemented for state {state!r}. "
            f"Supported states: {sorted(SUPPORTED_UTILITY_STATES)}."
        )

    ua_cfg = _STATE_CONFIGS[state]["utility_assignment"]
    module_path = ua_cfg.get("module")
    if not module_path:
        raise ValueError(
            f"State {state!r} has a utility_assignment config but no 'module' key."
        )

    kwargs: dict = dict(ua_cfg.get("kwargs") or {})

    if path_s3_gis_dir is not None:
        kwargs["path_s3_gis_dir"] = path_s3_gis_dir
    if electric_poly_filename is not None:
        kwargs["electric_poly_filename"] = electric_poly_filename
    if gas_poly_filename is not None:
        kwargs["gas_poly_filename"] = gas_poly_filename

    mod = importlib.import_module(module_path)
    return mod.assign_utility(metadata, **kwargs)


__all__ = [
    "SUPPORTED_UTILITY_STATES",
    "assign_utility",
]
