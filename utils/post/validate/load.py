"""Read CAIRO run outputs from S3 for validation.

CSVs are scanned lazily via ``pl.scan_csv``; JSON files are fetched with ``boto3``.
Local config reads (input tariffs, RR YAMLs) use standard file I/O.

Run directory layout::

    bills/{elec,gas,comb}_bills_year_target.csv
    cross_subsidization/cross_subsidization_BAT_values.csv
    customer_metadata.csv
    tariff_final_config.json
    seasonal_discount_rate_inputs.csv   (runs 5-6 only)
"""

from __future__ import annotations

import json
from typing import Any, Literal

import boto3
import polars as pl
import yaml

from utils import get_project_root

BillType = Literal["elec", "gas", "comb"]

_VALID_BILL_TYPES: frozenset[str] = frozenset({"elec", "gas", "comb"})
_REL_BAT = "cross_subsidization/cross_subsidization_BAT_values.csv"
_REL_METADATA = "customer_metadata.csv"
_REL_TARIFF_CONFIG = "tariff_final_config.json"
_REL_SEASONAL_DISCOUNT_INPUTS = "seasonal_discount_rate_inputs.csv"


def _s3_join(s3_dir: str, relative: str) -> str:
    return f"{s3_dir.rstrip('/')}/{relative}"


def _s3_get_bytes(s3_uri: str) -> bytes:
    """Fetch raw bytes from an S3 URI."""
    without_scheme = s3_uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()


def load_bills(s3_dir: str, bill_type: BillType = "elec") -> pl.LazyFrame:
    """Lazily scan ``bills/{bill_type}_bills_year_target.csv`` from a run directory.

    ``bill_type`` is one of ``"elec"`` (default), ``"gas"``, or ``"comb"``.
    """
    if bill_type not in _VALID_BILL_TYPES:
        raise ValueError(
            f"bill_type must be one of {sorted(_VALID_BILL_TYPES)!r}, got {bill_type!r}"
        )
    return pl.scan_csv(_s3_join(s3_dir, f"bills/{bill_type}_bills_year_target.csv"))


def load_bat(s3_dir: str) -> pl.LazyFrame:
    """Lazily scan ``cross_subsidization/cross_subsidization_BAT_values.csv``."""
    return pl.scan_csv(_s3_join(s3_dir, _REL_BAT))


def load_metadata(s3_dir: str) -> pl.LazyFrame:
    """Lazily scan ``customer_metadata.csv`` (ResStock metadata with ``bldg_id``, ``weight``)."""
    return pl.scan_csv(_s3_join(s3_dir, _REL_METADATA))


def load_tariff_config(s3_dir: str) -> dict[str, Any]:
    """Fetch and parse ``tariff_final_config.json`` from a run directory via boto3."""
    return json.loads(_s3_get_bytes(_s3_join(s3_dir, _REL_TARIFF_CONFIG)))  # type: ignore[no-any-return]


def load_input_tariff(state: str, utility: str, tariff_filename: str) -> dict[str, Any]:
    """Read a URDB tariff JSON from ``rate_design/hp_rates/{state}/config/tariffs/electric/``.

    ``utility`` is accepted for call-site clarity but not used in the path.
    """
    path = (
        get_project_root()
        / "rate_design"
        / "hp_rates"
        / state.lower()
        / "config"
        / "tariffs"
        / "electric"
        / tariff_filename
    )
    if not path.exists():
        raise FileNotFoundError(
            f"Input tariff not found for state={state!r} utility={utility!r}: {path}"
        )
    return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def load_revenue_requirement(
    state: str,
    utility: str,
    rr_filename: str | None = None,
) -> dict[str, Any]:
    """Read a revenue requirement YAML from ``rate_design/hp_rates/{state}/config/rev_requirement/``.

    Defaults to ``{utility}.yaml``; pass ``rr_filename`` to load the subclass variant
    (e.g. ``"{utility}_hp_vs_nonhp.yaml"``).
    """
    filename = rr_filename if rr_filename is not None else f"{utility.lower()}.yaml"
    path = (
        get_project_root()
        / "rate_design"
        / "hp_rates"
        / state.lower()
        / "config"
        / "rev_requirement"
        / filename
    )
    if not path.exists():
        raise FileNotFoundError(
            f"Revenue requirement YAML not found for state={state!r} utility={utility!r}: {path}"
        )
    return yaml.safe_load(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def load_seasonal_discount_inputs(s3_dir: str) -> pl.LazyFrame:
    """Lazily scan ``seasonal_discount_rate_inputs.csv`` (produced only for runs 5-6)."""
    return pl.scan_csv(_s3_join(s3_dir, _REL_SEASONAL_DISCOUNT_INPUTS))
