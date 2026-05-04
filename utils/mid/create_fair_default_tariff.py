"""Create a fair-default tariff JSON from computed fair-default inputs."""

from __future__ import annotations

import argparse
import copy
import json
import logging
import math
from pathlib import Path
from typing import Any, Literal

import polars as pl
from cloudpathlib import S3Path

from utils.pre.create_tariff import (
    create_seasonal_rate,
    write_tariff_json,
)
from utils.pre.season_config import (
    DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
    load_winter_months_from_periods,
    parse_months_arg,
    resolve_winter_summer_months,
)

LOGGER = logging.getLogger(__name__)

FairDefaultStrategy = Literal[
    "fixed_charge_only",
    "seasonal_rates_only",
    "fixed_plus_seasonal_mc",
]


def _resolve_path(path_value: str) -> S3Path | Path:
    return S3Path(path_value) if path_value.startswith("s3://") else Path(path_value)


def _read_json(path: S3Path | Path) -> dict[str, Any]:
    if isinstance(path, S3Path):
        return json.loads(path.read_text())
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: S3Path | Path) -> pl.DataFrame:
    if isinstance(path, S3Path):
        return pl.read_csv(str(path))
    return pl.read_csv(path)


def _write_json(path: S3Path | Path, payload: dict[str, Any]) -> str:
    if isinstance(path, S3Path):
        path.write_text(json.dumps(payload, indent=2) + "\n")
        return str(path)
    written = write_tariff_json(payload, path)
    return str(written)


def _with_fixed_charge(tariff: dict[str, Any], fixed_charge: float) -> dict[str, Any]:
    items = tariff.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("Tariff must contain at least one item in `items`.")
    items[0]["fixedchargefirstmeter"] = float(fixed_charge)
    return tariff


def _with_only_fixed_charge_overridden(
    *,
    base_tariff: dict[str, Any],
    fixed_charge: float,
    label: str,
) -> dict[str, Any]:
    tariff = copy.deepcopy(base_tariff)
    tariff = _with_fixed_charge(tariff, fixed_charge)
    tariff["items"][0]["label"] = label
    tariff["items"][0]["name"] = label
    return tariff


def _required_float(row: dict[str, Any], column: str) -> float:
    if column not in row or row[column] is None:
        raise ValueError(f"Fair default inputs CSV must contain `{column}`.")
    value = float(row[column])
    if not math.isfinite(value):
        raise ValueError(f"Fair default input `{column}` must be finite; got {value}.")
    return value


def _required_bool(row: dict[str, Any], column: str) -> bool:
    if column not in row or row[column] is None:
        raise ValueError(f"Fair default inputs CSV must contain `{column}`.")
    value = row[column]
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return bool(value)


def _ensure_feasible(
    *,
    strategy: FairDefaultStrategy,
    row: dict[str, Any],
    flag_column: str,
    values: dict[str, float],
    allow_infeasible: bool,
) -> None:
    feasible = _required_bool(row, flag_column)
    negative_values = {name: value for name, value in values.items() if value < 0.0}
    if feasible and not negative_values:
        return

    details = ", ".join(
        f"{name}={value}" for name, value in sorted(negative_values.items())
    )
    if not details:
        details = f"{flag_column}=false"
    message = f"{strategy} fair-default design is infeasible ({details})."
    if not allow_infeasible:
        raise ValueError(message + " Re-run with --allow-infeasible to clip at zero.")
    LOGGER.warning("%s Clipping negative charges/rates at zero.", message)


def _clip_nonnegative(value: float) -> float:
    return max(value, 0.0)


def _resolve_winter_months(
    row: dict[str, Any],
    periods_yaml_path: Path | None,
) -> list[int]:
    if periods_yaml_path is not None:
        return load_winter_months_from_periods(
            periods_yaml_path,
            default_winter_months=DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
        )
    if "winter_months" in row and row["winter_months"] is not None:
        return parse_months_arg(str(row["winter_months"]))
    winter_months, _ = resolve_winter_summer_months(
        None,
        default_winter_months=DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
    )
    return winter_months


def _fixed_strategy_values(
    row: dict[str, Any],
    *,
    allow_infeasible: bool,
) -> float:
    fixed_charge = _required_float(row, "fixed_charge_only_fixed_charge")
    _ensure_feasible(
        strategy="fixed_charge_only",
        row=row,
        flag_column="fixed_charge_only_feasible",
        values={"fixed_charge": fixed_charge},
        allow_infeasible=allow_infeasible,
    )
    if allow_infeasible:
        fixed_charge = _clip_nonnegative(fixed_charge)
    return fixed_charge


def _seasonal_strategy_values(
    row: dict[str, Any],
    *,
    allow_infeasible: bool,
) -> tuple[float, float, float]:
    fixed_charge = _required_float(row, "seasonal_rates_only_fixed_charge")
    winter_rate = _required_float(row, "seasonal_rates_only_winter_rate")
    summer_rate = _required_float(row, "seasonal_rates_only_summer_rate")
    _ensure_feasible(
        strategy="seasonal_rates_only",
        row=row,
        flag_column="seasonal_rates_only_feasible",
        values={
            "fixed_charge": fixed_charge,
            "summer_rate": summer_rate,
            "winter_rate": winter_rate,
        },
        allow_infeasible=allow_infeasible,
    )
    if allow_infeasible:
        winter_rate = _clip_nonnegative(
            _required_float(row, "seasonal_rates_only_clipped_winter_rate")
        )
        summer_rate = _clip_nonnegative(
            _required_float(row, "seasonal_rates_only_clipped_summer_rate")
        )
        fixed_charge = _clip_nonnegative(fixed_charge)
        if (
            "seasonal_rates_only_residual_cross_subsidy_after_clipping" in row
            and row["seasonal_rates_only_residual_cross_subsidy_after_clipping"]
            is not None
        ):
            LOGGER.warning(
                "Clipped seasonal fair-default tariff leaves residual target "
                "cross-subsidy of %s.",
                row["seasonal_rates_only_residual_cross_subsidy_after_clipping"],
            )
    return fixed_charge, winter_rate, summer_rate


def _combined_strategy_values(
    row: dict[str, Any],
    *,
    allow_infeasible: bool,
) -> tuple[float, float, float]:
    fixed_charge = _required_float(row, "fixed_plus_seasonal_mc_fixed_charge")
    winter_rate = _required_float(row, "fixed_plus_seasonal_mc_winter_rate")
    summer_rate = _required_float(row, "fixed_plus_seasonal_mc_summer_rate")
    _ensure_feasible(
        strategy="fixed_plus_seasonal_mc",
        row=row,
        flag_column="fixed_plus_seasonal_mc_feasible",
        values={
            "fixed_charge": fixed_charge,
            "summer_rate": summer_rate,
            "winter_rate": winter_rate,
        },
        allow_infeasible=allow_infeasible,
    )
    if allow_infeasible:
        fixed_charge = _clip_nonnegative(fixed_charge)
        winter_rate = _clip_nonnegative(winter_rate)
        summer_rate = _clip_nonnegative(summer_rate)
    return fixed_charge, winter_rate, summer_rate


def create_fair_default_tariff(
    *,
    base_tariff: dict[str, Any],
    inputs_row: dict[str, Any],
    strategy: FairDefaultStrategy,
    label: str,
    periods_yaml_path: Path | None = None,
    allow_infeasible: bool = False,
) -> dict[str, Any]:
    """Create a URDB tariff for one fair-default strategy."""
    if strategy == "fixed_charge_only":
        fixed_charge = _fixed_strategy_values(
            inputs_row,
            allow_infeasible=allow_infeasible,
        )
        return _with_only_fixed_charge_overridden(
            base_tariff=base_tariff,
            fixed_charge=fixed_charge,
            label=label,
        )

    winter_months = _resolve_winter_months(inputs_row, periods_yaml_path)
    if strategy == "seasonal_rates_only":
        fixed_charge, winter_rate, summer_rate = _seasonal_strategy_values(
            inputs_row,
            allow_infeasible=allow_infeasible,
        )
    elif strategy == "fixed_plus_seasonal_mc":
        fixed_charge, winter_rate, summer_rate = _combined_strategy_values(
            inputs_row,
            allow_infeasible=allow_infeasible,
        )
    else:
        raise ValueError(f"Unsupported fair-default strategy: {strategy}")

    return _with_fixed_charge(
        create_seasonal_rate(
            base_tariff=base_tariff,
            label=label,
            winter_rate=winter_rate,
            summer_rate=summer_rate,
            winter_months=winter_months,
        ),
        fixed_charge,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Create a fair-default tariff JSON from fair_default_inputs.csv."
    )
    parser.add_argument(
        "--base-tariff-json",
        required=True,
        help="Path to calibrated URDB default tariff JSON.",
    )
    parser.add_argument(
        "--inputs-csv",
        required=True,
        help="Path to fair_default_inputs.csv.",
    )
    parser.add_argument(
        "--strategy",
        required=True,
        choices=(
            "fixed_charge_only",
            "seasonal_rates_only",
            "fixed_plus_seasonal_mc",
        ),
        help="Fair-default strategy to write.",
    )
    parser.add_argument(
        "--label",
        required=True,
        help="Tariff label/name for the created fair-default tariff.",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Output path for created tariff JSON (local or s3://...).",
    )
    parser.add_argument(
        "--periods-yaml",
        type=Path,
        help=(
            "Optional periods YAML containing `winter_months` for seasonal "
            "strategies. If omitted, uses the input CSV winter_months column."
        ),
    )
    parser.add_argument(
        "--allow-infeasible",
        action="store_true",
        help="Write an infeasible design by clipping negative charges/rates at zero.",
    )
    args = parser.parse_args()

    base_tariff_path = _resolve_path(args.base_tariff_json)
    inputs_path = _resolve_path(args.inputs_csv)
    output_path = _resolve_path(args.output_path)

    base_tariff = _read_json(base_tariff_path)
    inputs = _read_csv(inputs_path)
    if inputs.is_empty():
        raise ValueError("Fair default inputs CSV is empty.")
    row = inputs.row(0, named=True)

    fair_default_tariff = create_fair_default_tariff(
        base_tariff=base_tariff,
        inputs_row=row,
        strategy=args.strategy,
        label=args.label,
        periods_yaml_path=args.periods_yaml,
        allow_infeasible=args.allow_infeasible,
    )
    written_path = _write_json(output_path, fair_default_tariff)
    print(f"Created fair-default tariff file: {written_path}")


if __name__ == "__main__":
    main()
