"""Shared data-parsing helpers used by multiple mid-run and pre-run scripts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import polars as pl
import yaml


# Path resolution
def _resolve_path(value: str, base_dir: Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (base_dir / path)


def _resolve_path_or_uri(value: str, base_dir: Path) -> str | Path:
    if value.startswith("s3://"):
        return value
    return _resolve_path(value, base_dir)


# Customer counts
def get_residential_customer_count_from_utility_stats(
    path: str | Path,
    utility: str,
    *,
    storage_options: dict[str, str] | None = None,
) -> int:
    """Read EIA-861 utility stats parquet and return residential customer count for the utility.

    The parquet is state-partitioned (e.g. state=NY/data.parquet) with columns
    utility_code and residential_customers. Filters for utility_code == utility
    (the YAML utility field, e.g. 'coned', 'rie') and returns the single row's
    residential_customers value.

    Raises:
        ValueError: If path has no row for that utility, or more than one row.
    """
    path_str = str(path)
    opts = storage_options if path_str.startswith("s3://") else None
    lf = (
        pl.scan_parquet(path_str, storage_options=opts)
        .filter(pl.col("utility_code") == utility)
        .select("residential_customers")
    )
    df = cast(pl.DataFrame, lf.collect())
    if df.height == 0:
        raise ValueError(
            f"No row with utility_code={utility!r} in {path_str}. "
            "Check path_electric_utility_stats and utility in the scenario YAML."
        )
    if df.height > 1:
        raise ValueError(
            f"Expected one row for utility_code={utility!r} in {path_str}, got {df.height}"
        )
    value = df.item(0, 0)
    if value is None:
        raise ValueError(
            f"residential_customers is null for utility_code={utility!r} in {path_str}"
        )
    return int(value)


def get_residential_sales_revenue_from_utility_stats(
    path: str | Path,
    utility: str,
    *,
    storage_options: dict[str, str] | None = None,
) -> float:
    """Read EIA-861 utility stats parquet and return residential sales revenue in dollars.

    The parquet column ``residential_sales_revenue`` is already in whole dollars
    (PUDL converts from EIA's original thousands-of-dollars reporting).

    Raises:
        ValueError: If path has no row for that utility, or more than one row.
    """
    path_str = str(path)
    opts = storage_options if path_str.startswith("s3://") else None
    lf = (
        pl.scan_parquet(path_str, storage_options=opts)
        .filter(pl.col("utility_code") == utility)
        .select("residential_sales_revenue")
    )
    df = cast(pl.DataFrame, lf.collect())
    if df.height == 0:
        raise ValueError(
            f"No row with utility_code={utility!r} in {path_str}. "
            "Check path_electric_utility_stats and utility in the scenario YAML."
        )
    if df.height > 1:
        raise ValueError(
            f"Expected one row for utility_code={utility!r} in {path_str}, got {df.height}"
        )
    value = df.item(0, 0)
    if value is None:
        raise ValueError(
            f"residential_sales_revenue is null for utility_code={utility!r} in {path_str}"
        )
    return float(value)


MWH_TO_KWH = 1000


def get_residential_sales_kwh_from_utility_stats(
    path: str | Path,
    utility: str,
    *,
    storage_options: dict[str, str] | None = None,
) -> float:
    """Read EIA-861 utility stats parquet and return residential sales in kWh for the utility.

    The parquet is state-partitioned (e.g. state=NY/data.parquet) with columns
    utility_code and residential_sales_mwh. Filters for utility_code == utility
    and returns residential_sales_mwh converted to kWh (multiplied by 1000).

    Raises:
        ValueError: If path has no row for that utility, or more than one row.
    """
    path_str = str(path)
    opts = storage_options if path_str.startswith("s3://") else None
    lf = (
        pl.scan_parquet(path_str, storage_options=opts)
        .filter(pl.col("utility_code") == utility)
        .select(
            (pl.col("residential_sales_mwh") * MWH_TO_KWH).alias(
                "residential_sales_kwh"
            )
        )
    )
    df = cast(pl.DataFrame, lf.collect())
    if df.height == 0:
        raise ValueError(
            f"No row with utility_code={utility!r} in {path_str}. "
            "Check path_electric_utility_stats and utility in the scenario YAML."
        )
    if df.height > 1:
        raise ValueError(
            f"Expected one row for utility_code={utility!r} in {path_str}, got {df.height}"
        )
    value = df.item(0, 0)
    if value is None:
        raise ValueError(
            f"residential_sales_mwh is null for utility_code={utility!r} in {path_str}"
        )
    return float(value)


# Revenue requirement parsing


@dataclass(frozen=True, slots=True)
class RevenueRequirementConfig:
    """Parsed revenue requirement configuration.

    rr_total: scalar for MC decomposition (delivery or delivery+supply).
    subclass_rr: per-tariff-key RR dict, or None for non-subclass runs.
    run_includes_subclasses: whether the run uses per-subclass RRs.
    residual_allocation_delivery: delivery allocation method (e.g. "percustomer",
        "epmc"), or None for non-subclass runs.
    residual_allocation_supply: supply allocation method (e.g. "passthrough",
        "percustomer"), or None for non-subclass runs.
    """

    rr_total: float
    subclass_rr: dict[str, float] | None
    run_includes_subclasses: bool
    residual_allocation_delivery: str | None
    residual_allocation_supply: str | None
    customer_count_override: float | None = None
    kwh_scale_factor: float | None = None


def _parse_subclass_revenue_requirement(
    rr_data: dict[str, Any],
    raw_path_tariffs_electric: dict[str, Any],
    base_dir: Path,
    *,
    add_supply: bool,
    residual_allocation_delivery: str,
    residual_allocation_supply: str,
) -> dict[str, float]:
    """Map subclass revenue requirements to tariff keys.

    Expects YAML structure with separate delivery and supply blocks::

        subclass_revenue_requirements:
          delivery:
            percustomer: {hp: ..., non-hp: ...}
            epmc: {hp: ..., non-hp: ...}
          supply:
            passthrough: {hp: ..., non-hp: ...}
            percustomer: {hp: ..., non-hp: ...}

    *residual_allocation_delivery* selects the delivery method.
    *residual_allocation_supply* selects the supply method.
    Subclass alias keys match keys in *raw_path_tariffs_electric*.
    For delivery-only runs (add_supply=False), only delivery is used.
    For delivery+supply runs, total = delivery + supply per subclass.
    """
    raw_subclass = rr_data.get("subclass_revenue_requirements")
    if not isinstance(raw_subclass, dict) or not raw_subclass:
        raise ValueError("subclass_revenue_requirements must be a non-empty mapping")

    delivery_block = raw_subclass.get("delivery")
    if not isinstance(delivery_block, dict) or not delivery_block:
        raise ValueError("subclass_revenue_requirements must have a 'delivery' block")

    if residual_allocation_delivery not in delivery_block:
        available = sorted(delivery_block.keys())
        raise ValueError(
            f"residual_allocation_delivery={residual_allocation_delivery!r} "
            f"not found in subclass_revenue_requirements.delivery. "
            f"Available: {available}"
        )

    delivery_rr = delivery_block[residual_allocation_delivery]

    alias_to_tariff_key = {
        alias: _resolve_path(str(path_str), base_dir).stem
        for alias, path_str in raw_path_tariffs_electric.items()
    }

    result: dict[str, float] = {}
    for alias_str, amount in delivery_rr.items():
        tariff_key = alias_to_tariff_key.get(alias_str)
        if tariff_key is None:
            raise ValueError(
                f"Subclass alias {alias_str!r} in YAML not found in "
                f"path_tariffs_electric (available: {sorted(alias_to_tariff_key)})"
            )
        result[tariff_key] = _parse_float(
            amount,
            f"subclass_revenue_requirements.delivery"
            f".{residual_allocation_delivery}.{alias_str}",
        )

    if add_supply:
        supply_block = raw_subclass.get("supply")
        if not isinstance(supply_block, dict) or not supply_block:
            raise ValueError(
                "subclass_revenue_requirements must have a 'supply' block "
                "when add_supply=True"
            )
        if residual_allocation_supply not in supply_block:
            available = sorted(supply_block.keys())
            raise ValueError(
                f"residual_allocation_supply={residual_allocation_supply!r} "
                f"not found in subclass_revenue_requirements.supply. "
                f"Available: {available}"
            )
        supply_rr = supply_block[residual_allocation_supply]
        for alias_str, amount in supply_rr.items():
            tariff_key = alias_to_tariff_key.get(alias_str)
            if tariff_key is None:
                raise ValueError(
                    f"Subclass alias {alias_str!r} in supply YAML not found in "
                    f"path_tariffs_electric (available: {sorted(alias_to_tariff_key)})"
                )
            if tariff_key not in result:
                raise ValueError(
                    f"Subclass {alias_str!r} in supply but not in delivery"
                )
            result[tariff_key] += _parse_float(
                amount,
                f"subclass_revenue_requirements.supply"
                f".{residual_allocation_supply}.{alias_str}",
            )

    return result


def _parse_utility_revenue_requirement(
    value: Any,
    base_dir: Path,
    raw_path_tariffs_electric: dict[str, Any],
    *,
    add_supply: bool,
    run_includes_subclasses: bool = False,
    residual_allocation_delivery: str | None = None,
    residual_allocation_supply: str | None = None,
) -> RevenueRequirementConfig:
    """Parse utility_revenue_requirement from a YAML path.

    Returns a RevenueRequirementConfig with:
      - rr_total: scalar from total_delivery[_and_supply]_revenue_requirement
      - subclass_rr: per-tariff-key RR dict (or None)
      - run_includes_subclasses: whether this run uses subclass RRs
      - residual_allocation_delivery / _supply: which methods were used

    When *run_includes_subclasses* is True, both *residual_allocation_delivery*
    and *residual_allocation_supply* are required.
    """
    if not isinstance(value, str):
        raise ValueError(
            "utility_revenue_requirement must be a YAML path string "
            f"(.yaml/.yml), got {type(value).__name__}"
        )
    raw = value.strip()
    if raw == "":
        raise ValueError("Missing required field: utility_revenue_requirement")
    if not (raw.endswith(".yaml") or raw.endswith(".yml")):
        raise ValueError(
            "utility_revenue_requirement must be a YAML file path "
            f"(.yaml/.yml), got {value!r}"
        )

    path = _resolve_path(raw, base_dir)
    with path.open(encoding="utf-8") as f:
        rr_data = yaml.safe_load(f)
    if not isinstance(rr_data, dict):
        raise ValueError(
            "Revenue requirement YAML must be a mapping; "
            f"got {type(rr_data).__name__} in {path}"
        )

    rr_key = (
        "total_delivery_and_supply_revenue_requirement"
        if add_supply
        else "total_delivery_revenue_requirement"
    )

    if rr_key not in rr_data:
        if "revenue_requirement" in rr_data:
            rr_total = _parse_float(
                rr_data["revenue_requirement"], "revenue_requirement"
            )
        else:
            raise ValueError(
                f"{path} must contain '{rr_key}' (or 'revenue_requirement')."
            )
    else:
        rr_total = _parse_float(rr_data[rr_key], rr_key)

    subclass_rr: dict[str, float] | None = None
    if run_includes_subclasses:
        if residual_allocation_delivery is None:
            raise ValueError(
                "run_includes_subclasses is true but residual_allocation_delivery "
                "is not set. Add 'residual_allocation_delivery: percustomer' "
                "(or 'epmc') to the scenario YAML."
            )
        if residual_allocation_supply is None:
            raise ValueError(
                "run_includes_subclasses is true but residual_allocation_supply "
                "is not set. Add 'residual_allocation_supply: passthrough' "
                "(or 'percustomer') to the scenario YAML."
            )
        if "subclass_revenue_requirements" not in rr_data:
            raise ValueError(
                f"run_includes_subclasses is true but {path} has no "
                "'subclass_revenue_requirements'."
            )
        subclass_rr = _parse_subclass_revenue_requirement(
            rr_data,
            raw_path_tariffs_electric,
            base_dir,
            add_supply=add_supply,
            residual_allocation_delivery=residual_allocation_delivery,
            residual_allocation_supply=residual_allocation_supply,
        )

    customer_count_override: float | None = None
    if "test_year_customer_count" in rr_data:
        customer_count_override = float(rr_data["test_year_customer_count"])

    kwh_scale_factor: float | None = None
    if "resstock_kwh_scale_factor" in rr_data:
        kwh_scale_factor = float(rr_data["resstock_kwh_scale_factor"])

    return RevenueRequirementConfig(
        rr_total=rr_total,
        subclass_rr=subclass_rr,
        run_includes_subclasses=run_includes_subclasses,
        residual_allocation_delivery=residual_allocation_delivery,
        residual_allocation_supply=residual_allocation_supply,
        customer_count_override=customer_count_override,
        kwh_scale_factor=kwh_scale_factor,
    )


# Tariff parsing
def _parse_path_tariffs(
    value: Any,
    path_tariff_map: Path,
    base_dir: Path,
    label: str,
) -> dict[str, Path]:
    """Parse path_tariffs_electric from YAML (dict key -> path) and reconcile with map.

    Value must be a dict mapping keys to path strings; keys used for the tariff map
    are derived from filename stem (e.g. tariffs/electric/foo.json -> foo). Every
    tariff_key in the map must have an entry, and every path must appear in the map.
    """
    if not isinstance(value, dict):
        raise ValueError(
            f"path_tariffs_{label} must be a dict of key -> path; got {type(value).__name__}"
        )
    path_tariffs = {}
    for item in value.values():
        if not isinstance(item, str):
            raise ValueError(
                f"path_tariffs_{label} dict values must be path strings; "
                f"got {type(item).__name__}"
            )
        path = _resolve_path(item, base_dir)
        key = path.stem
        if key in path_tariffs:
            raise ValueError(
                f"path_tariffs_{label}: duplicate key '{key}' from paths "
                f"{path_tariffs[key]} and {path}"
            )
        path_tariffs[key] = path

    map_keys = _tariff_map_keys(path_tariff_map)
    list_keys = set(path_tariffs.keys())
    only_in_map = map_keys - list_keys
    only_in_list = list_keys - map_keys
    if only_in_map:
        raise ValueError(
            f"{label.capitalize()} tariff map references tariff_key(s) with no file "
            f"in path_tariffs_{label}: {sorted(only_in_map)}"
        )
    if only_in_list:
        raise ValueError(
            f"path_tariffs_{label} includes file(s) not referenced in {label} "
            f"tariff map: {sorted(only_in_list)}"
        )
    return path_tariffs


def _parse_path_tariffs_gas(
    value: Any,
    path_tariff_map: Path,
    base_dir: Path,
) -> dict[str, Path]:
    """Parse path_tariffs_gas from YAML: must be a directory path (string).

    Unique tariff_key values are read from the gas tariff map; each must have
    a file at directory/tariff_key.json.
    """
    if not isinstance(value, str):
        raise ValueError(
            "path_tariffs_gas must be a directory path (string) containing "
            f"gas tariff JSONs named <tariff_key>.json; got {type(value).__name__}"
        )
    path_dir = _resolve_path(value, base_dir)
    if not path_dir.is_dir():
        raise ValueError(
            f"path_tariffs_gas is a directory path but not a directory: {path_dir}"
        )
    map_keys = _tariff_map_keys(path_tariff_map)
    path_tariffs = {k: path_dir / f"{k}.json" for k in map_keys}
    missing = [k for k, p in path_tariffs.items() if not p.is_file()]
    if missing:
        raise ValueError(
            "Gas tariff map references tariff_key(s) with no file under "
            f"{path_dir}: {sorted(missing)}. "
            f"Expected e.g. {path_dir / 'tariff_key.json'}"
        )
    return path_tariffs


def _tariff_map_keys(path_tariff_map: Path) -> set[str]:
    """Return the set of tariff_key values in a tariff map CSV (electric or gas)."""
    df = pl.read_csv(path_tariff_map)
    if "tariff_key" not in df.columns:
        raise ValueError(
            f"Tariff map {path_tariff_map} must have a 'tariff_key' column"
        )
    return set(df["tariff_key"].unique().to_list())


# Parse int, float, bool
def _parse_int(value: object, field_name: str) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    cleaned = str(value).strip().replace(",", "")
    if cleaned == "":
        raise ValueError(f"Missing required field: {field_name}")
    try:
        return int(float(cleaned))
    except ValueError as exc:
        raise ValueError(f"Invalid integer for {field_name}: {value}") from exc


def _parse_float(value: object, field_name: str) -> float:
    if isinstance(value, int | float):
        return float(value)
    cleaned = str(value).strip().replace(",", "")
    if cleaned == "":
        raise ValueError(f"Missing required field: {field_name}")
    try:
        return float(cleaned)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {field_name}: {value}") from exc


def _parse_bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(
        f"Invalid boolean for {field_name}: {value!r}. Use unquoted YAML true/false."
    )


def resolve_subclass_rr_for_validation(
    subclass_rr_data: dict[str, Any],
    cost_scope: str,
    residual_allocation_delivery: str = "percustomer",
    residual_allocation_supply: str = "passthrough",
) -> dict[str, dict[str, float]]:
    """Resolve the new delivery/supply YAML format into the old-style dict.

    Returns ``{alias: {"delivery": float, "supply": float, "total": float}}``
    for use by validation checks that expect the old format.
    """
    raw = subclass_rr_data.get("subclass_revenue_requirements", subclass_rr_data)
    delivery_block = raw.get("delivery", {})
    supply_block = raw.get("supply", {})

    del_rr = delivery_block.get(residual_allocation_delivery, {})
    sup_rr = supply_block.get(residual_allocation_supply, {})

    result: dict[str, dict[str, float]] = {}
    for alias in del_rr:
        d = float(del_rr[alias])
        s = float(sup_rr.get(alias, 0.0))
        result[alias] = {"delivery": d, "supply": s, "total": d + s}
    return result
