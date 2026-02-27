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
    """

    rr_total: float
    subclass_rr: dict[str, float] | None
    run_includes_subclasses: bool


def _parse_subclass_revenue_requirement(
    rr_data: dict[str, Any],
    raw_path_tariffs_electric: dict[str, Any],
    base_dir: Path,
    *,
    add_supply: bool,
) -> dict[str, float]:
    """Map subclass revenue requirements to tariff keys.

    YAML subclass keys are aliases ('hp'/'non-hp') matching the keys in
    path_tariffs_electric. Each alias resolves to a tariff key (file stem).
    Picks 'delivery' or 'total' per subclass based on *add_supply*.
    """
    subclass_rr = rr_data.get("subclass_revenue_requirements")
    if not isinstance(subclass_rr, dict) or not subclass_rr:
        raise ValueError("subclass_revenue_requirements must be a non-empty mapping")

    alias_to_tariff_key = {
        alias: _resolve_path(str(path_str), base_dir).stem
        for alias, path_str in raw_path_tariffs_electric.items()
    }

    result: dict[str, float] = {}
    for alias, amount in subclass_rr.items():
        alias_str = str(alias)
        tariff_key = alias_to_tariff_key.get(alias_str)
        if tariff_key is None:
            raise ValueError(
                f"Subclass alias {alias_str!r} in YAML not found in "
                f"path_tariffs_electric (available: {sorted(alias_to_tariff_key)})"
            )
        if isinstance(amount, dict):
            rr_field = "total" if add_supply else "delivery"
            if rr_field not in amount:
                raise ValueError(
                    f"subclass_revenue_requirements[{alias_str}] missing "
                    f"required field {rr_field!r}"
                )
            result[tariff_key] = _parse_float(
                amount[rr_field],
                f"subclass_revenue_requirements[{alias_str}].{rr_field}",
            )
        else:
            result[tariff_key] = _parse_float(
                amount, f"subclass_revenue_requirements[{alias_str}]"
            )

    return result


def _parse_utility_revenue_requirement(
    value: Any,
    base_dir: Path,
    raw_path_tariffs_electric: dict[str, Any],
    *,
    add_supply: bool,
    run_includes_subclasses: bool = False,
) -> RevenueRequirementConfig:
    """Parse utility_delivery_revenue_requirement from a YAML path.

    Returns a RevenueRequirementConfig with:
      - rr_total: scalar from total_delivery[_and_supply]_revenue_requirement
      - subclass_rr: per-tariff-key RR dict (or None)
      - run_includes_subclasses: whether this run uses subclass RRs
    """
    if not isinstance(value, str):
        raise ValueError(
            "utility_delivery_revenue_requirement must be a YAML path string "
            f"(.yaml/.yml), got {type(value).__name__}"
        )
    raw = value.strip()
    if raw == "":
        raise ValueError("Missing required field: utility_delivery_revenue_requirement")
    if not (raw.endswith(".yaml") or raw.endswith(".yml")):
        raise ValueError(
            "utility_delivery_revenue_requirement must be a YAML file path "
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
        # Fallback: legacy YAMLs (e.g. *_large_number.yaml) use a bare
        # 'revenue_requirement' key with the same value for both modes.
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
        if "subclass_revenue_requirements" not in rr_data:
            raise ValueError(
                f"run_includes_subclasses is true but {path} has no "
                "'subclass_revenue_requirements'."
            )
        subclass_rr = _parse_subclass_revenue_requirement(
            rr_data, raw_path_tariffs_electric, base_dir, add_supply=add_supply
        )

    return RevenueRequirementConfig(
        rr_total=rr_total,
        subclass_rr=subclass_rr,
        run_includes_subclasses=run_includes_subclasses,
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
