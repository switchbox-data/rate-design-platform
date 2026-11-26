"""Run one marginal-cost allocation from a state ``allocation.yaml``.

The config names the method and where each input comes from. This script
resolves the inputs, calls the method, checks the 8760, and either prints a
summary or, with ``--upload``, writes the partition and appends ``manifest.yaml``.

Usage:
    uv run python utils/data_prep/marginal_costs/run_allocation.py \\
        --config rate_design/hp_rates/<state>/config/marginal_costs/allocation.yaml \\
        --component {dist_and_sub_tx|bulk_tx} \\
        --utility <utility> \\
        --year <year> \\
        [--load-year <year>] [--variant <name>] [--upload]
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

import polars as pl
import yaml
from dotenv import load_dotenv

from utils.data_prep.marginal_costs.inputs import InputContext, Resolved, resolve_input
from utils.data_prep.marginal_costs.inputs.generic import voltage_from_table_paths
from utils.data_prep.marginal_costs.mc_manifest import (
    McSpec,
    record_mc_write,
    require_published_code,
)
from utils.data_prep.marginal_costs.methods.month_hour import month_hour
from utils.data_prep.marginal_costs.methods.pop_kde import (
    DEFAULT_TRUNCATION_SIGMAS,
    pop_kde,
    print_pop_summary,
    probability_of_peak,
)
from utils.data_prep.marginal_costs.methods.seasonal_scr_exceedance import seasonal_scr_exceedance
from utils.data_prep.marginal_costs.methods.top_n_exceedance import top_n_exceedance
from utils.data_prep.marginal_costs.methods.top_n_load_share import top_n_load_share
from utils.data_prep.marginal_costs.supply_utils import (
    build_cairo_8760_timestamps,
    output_base_for_load_year,
    warn_if_multiple_partition_parquets,
)
from utils.file_io import get_aws_storage_options
from utils.pre.season_config import derive_summer_months

METHODS = {
    "top_n_load_share": top_n_load_share,
    "top_n_exceedance": top_n_exceedance,
    "seasonal_scr_exceedance": seasonal_scr_exceedance,
    "pop_kde": pop_kde,
    "month_hour": month_hour,
}

_ANNUAL_SUM_TOLERANCE_PCT = 0.01
_HOLIDAY_RULE = "5 USC 6103, Sat->Fri, Sun->Mon"
_INTEGER = re.compile(r"-?\d+")


class _Placeholders(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        raise KeyError(f"Placeholder {{{key}}} is not set for this allocation run")


def substitute(value: Any, placeholders: dict[str, Any]) -> Any:
    """Fill ``{utility}``, ``{year}``, and ``{load_year}``. A whole-string integer becomes an int."""
    if isinstance(value, str):
        rendered = value.format_map(_Placeholders(placeholders))
        if _INTEGER.fullmatch(rendered):
            return int(rendered)
        return rendered
    if isinstance(value, dict):
        return {key: substitute(item, placeholders) for key, item in value.items()}
    if isinstance(value, list):
        return [substitute(item, placeholders) for item in value]
    return value


def select_block(
    document: dict[str, Any],
    component: str,
    variant: str | None,
    utility: str | None,
) -> dict[str, Any]:
    """The component block, or one alternative. Checks ``utility`` when it is passed."""
    if component not in document:
        raise ValueError(f"Config has no {component!r} block. Components: {sorted(document)}")
    block = document[component]
    if not isinstance(block, dict):
        raise ValueError(f"{component} must be a mapping")
    chosen = block
    if variant:
        alternatives = block.get("alternatives") or {}
        if variant in alternatives:
            chosen = dict(alternatives[variant])
            for key in ("output_column", "output_base", "utilities", "combine"):
                if key not in chosen and key in block:
                    chosen[key] = block[key]
        elif variant != block.get("method"):
            known = ", ".join(sorted(alternatives)) or "(none)"
            raise ValueError(f"Unknown variant {variant!r} for {component}. Known variants: {known}")
    if utility is not None and utility not in chosen.get("utilities", []):
        raise ValueError(f"Utility {utility!r} is not in {component} utilities: {list(chosen.get('utilities', []))}")
    return chosen


def _apply_sets(inputs: dict[str, Any], sets: list[str]) -> dict[str, Any]:
    """Override literal inputs. ``--set include_holidays=false`` replaces that key."""
    updated = dict(inputs)
    for item in sets:
        key, sep, raw = item.partition("=")
        if sep != "=" or not key:
            raise ValueError(f"--set must be key=value, got {item!r}")
        if raw.lower() in {"true", "false"}:
            value: Any = raw.lower() == "true"
        elif _INTEGER.fullmatch(raw):
            value = int(raw)
        else:
            try:
                value = float(raw)
            except ValueError:
                value = raw
        updated[key] = value
    return updated


def _resolve_inputs(inputs: dict[str, Any], ctx: InputContext) -> dict[str, Resolved]:
    resolved: dict[str, Resolved] = {}
    for key, raw in inputs.items():
        if isinstance(raw, dict) and "from" in raw:
            resolved[key] = resolve_input(str(raw["from"]), raw, ctx, role=key)
        else:
            resolved[key] = Resolved(raw)
    return resolved


def _call_method(method_name: str, kwargs: dict[str, Any]) -> pl.DataFrame:
    method = METHODS.get(method_name)
    if method is None:
        known = ", ".join(sorted(METHODS))
        raise ValueError(f"Unknown method {method_name!r}. Known methods: {known}")
    try:
        result = method(**kwargs)
    except TypeError as exc:
        raise TypeError(f"Method {method_name} rejected input keys {sorted(kwargs)}: {exc}") from exc
    if not isinstance(result, pl.DataFrame):
        raise TypeError(f"Method {method_name} returned {type(result).__name__}, expected a DataFrame")
    return result


def _run_method(
    method_name: str,
    resolved: dict[str, Resolved],
    column: str,
    *,
    combine: str | None,
) -> tuple[pl.DataFrame, float, Any]:
    """Call the method. ``combine: sum`` calls it once per component and adds the cost column."""
    values = {key: item.value for key, item in resolved.items()}
    pop = None
    if combine == "sum":
        components = values.pop("components")
        frames: list[pl.DataFrame] = []
        expected = 0.0
        for component in components:
            call = {
                **values,
                "load": component["load"],
                "annual_cost_kw_year": component["annual_cost_kw_year"],
                "output_column": column,
            }
            frames.append(_call_method(method_name, call))
            expected += float(component["annual_cost_kw_year"])
        combined = pl.concat(frames).group_by("timestamp").agg(pl.col(column).sum().alias(column)).sort("timestamp")
        return combined, expected, None

    if method_name == "pop_kde":
        pop = probability_of_peak(values["load"])
        values["fitted"] = pop
    values["output_column"] = column
    result = _call_method(method_name, values)
    if "annual_cost_kw_year" not in resolved:
        raise ValueError(f"Method {method_name} needs annual_cost_kw_year to check the 8760 sum")
    return result, float(resolved["annual_cost_kw_year"].value), pop


def align_8760(df: pl.DataFrame, year: int, column: str) -> pl.DataFrame:
    """Left-join onto the Cairo clock and set a missing hour to zero.

    Duplicate timestamps are averaged, which is a no-op when the method already
    returned one row per clock hour.
    """
    clock = build_cairo_8760_timestamps(year).with_columns(pl.col("timestamp").cast(pl.Datetime("us")))
    hourly = (
        df.with_columns(pl.col("timestamp").cast(pl.Datetime("us")).dt.truncate("1h").alias("timestamp"))
        .group_by("timestamp")
        .agg(pl.col(column).mean().alias(column))
    )
    output = (
        clock.join(hourly, on="timestamp", how="left")
        .with_columns(pl.col(column).fill_null(0.0).alias(column))
        .sort("timestamp")
    )
    if output.height != 8760:
        raise ValueError(f"Expected 8760 rows, got {output.height}")
    if output[column].null_count() != 0:
        raise ValueError(f"{column} has nulls after aligning to the 8760")
    if output.select(pl.col("timestamp").n_unique()).item() != 8760:
        raise ValueError("8760 timestamps are not unique")
    return output.select("timestamp", column)


def _check_annual_sum(df: pl.DataFrame, column: str, expected: float) -> None:
    """Fail when the 8760 does not recover ``expected`` $/kW-yr within 0.01%."""
    actual = float(df[column].sum())
    error = abs(actual - expected)
    if expected == 0.0:
        if error > 1e-6:
            raise ValueError(f"Annual sum is {actual}, expected 0")
        return
    error_pct = error / abs(expected) * 100
    print("\nVALIDATION: 1 kW constant load")
    print(f"  Expected: ${expected:.4f}/kW-yr")
    print(f"  Actual:   ${actual:.4f}/kW-yr")
    print(f"  Error:    {error_pct:.6f}%")
    if error_pct > _ANNUAL_SUM_TOLERANCE_PCT:
        raise ValueError(
            f"Annual sum {actual:.6f} differs from {expected:.6f} by {error_pct:.6f}% "
            f"(tolerance {_ANNUAL_SUM_TOLERANCE_PCT}%)"
        )
    print("  ✓ Validation PASSED")


def _annual_meta(item: Resolved, year: int) -> dict[str, Any]:
    meta = item.meta
    return {
        "as_filed": meta.get("as_filed", item.value),
        "allocated": meta.get("allocated", item.value),
        "dollar_year": meta.get("dollar_year"),
        "cpi_factor": meta.get("cpi_factor", 1.0),
        "target_dollar_year": meta.get("target_dollar_year", year),
    }


def _load_meta(resolved: dict[str, Resolved], load_year: int) -> dict[str, Any]:
    if "load" not in resolved:
        return {"load_year": load_year}
    return resolved["load"].meta


def manifest_params(
    component: str,
    manifest_method: str,
    resolved: dict[str, Resolved],
    year: int,
    load_year: int,
    *,
    pop: Any = None,
) -> dict[str, Any]:
    """Params for ``mc_manifest.KEY_BUILDERS``. Names match the builders already in use."""
    annual = _annual_meta(resolved["annual_cost_kw_year"], year) if "annual_cost_kw_year" in resolved else {}
    load = _load_meta(resolved, load_year)
    if manifest_method == "pop":
        return {
            "load_source": load.get("load_source", "load"),
            "load_year": load.get("load_year", load_year),
            "n_hours": int(resolved["n_hours"].value),
            "weighting": "load_weighted_top_n",
            "mc_kw_yr_as_filed": annual["as_filed"],
            "mc_dollar_year": annual["dollar_year"],
            "target_dollar_year": annual["target_dollar_year"],
            "cpi_factor": annual["cpi_factor"],
            "mc_kw_yr_allocated": annual["allocated"],
        }
    if (component, manifest_method) == ("dist_and_sub_tx", "month_hour"):
        weekday = str(resolved["weekday"].meta.get("path", ""))
        weekend = str(resolved["weekend"].meta.get("path", ""))
        return {
            "table_source": "occ863att2",
            "voltage": voltage_from_table_paths(weekday, weekend),
            "include_holidays": bool(resolved["include_holidays"].value),
            "holiday_rule": _HOLIDAY_RULE,
            "calendar_year": int(resolved["calendar_year"].value),
            "timestamp_year": year,
            "mc_kw_yr_as_filed": annual["as_filed"],
            "mc_dollar_year": annual["dollar_year"],
            "target_dollar_year": annual["target_dollar_year"],
            "cpi_factor": annual["cpi_factor"],
            "mc_kw_yr_target": annual["allocated"],
        }
    if manifest_method == "isone_aesc_ptf":
        return {
            "ptf_kw_year": float(annual["as_filed"]),
            "allocation_load": load.get("allocation_load", "utility_zone"),
            "allocation_zones": list(load.get("zones", [])),
            "n_peak_hours": int(resolved["n_hours"].value),
            "weighting": "exceedance_top_n",
            "load_year": load.get("load_year", load_year),
        }
    if manifest_method == "isone_aesc_ptf_pop_kde":
        history = list(load.get("history_years", [load.get("load_year", load_year)]))
        params: dict[str, Any] = {
            "ptf_kw_year": float(annual["as_filed"]),
            "aesc_dollar_year": annual["dollar_year"],
            "target_dollar_year": annual["target_dollar_year"],
            "cpi_factor": annual["cpi_factor"],
            "ptf_kw_year_allocated": annual["allocated"],
            "allocation_load": load.get("allocation_load", "ne_system"),
            "allocation_zones": list(load.get("zones", [])),
            "history_years": history,
            "calendar_year": int(resolved["calendar_year"].value),
            "timestamp_year": year,
            "weighting": "pop_kde_per_day",
            "kde_bandwidth": "scott",
            "kde_truncation_sigmas": DEFAULT_TRUNCATION_SIGMAS,
            "holiday_rule": "5 USC 6103, Sat->Fri, Sun->Mon, Juneteenth from 2021",
        }
        if pop is not None:
            params["delimiter"] = pop.delimiter
            params["delimiter_share_of_peak"] = pop.delimiter_share_of_peak
        return params
    if (component, manifest_method) == ("bulk_tx", "month_hour"):
        return {
            "table_source": "occ863att2",
            "ptf_kw_year": float(annual["as_filed"]),
            "ptf_dollar_year": annual["dollar_year"],
            "target_dollar_year": annual["target_dollar_year"],
            "cpi_factor": annual["cpi_factor"],
            "include_holidays": bool(resolved["include_holidays"].value),
            "holiday_rule": _HOLIDAY_RULE,
            "calendar_year": int(resolved["calendar_year"].value),
            "timestamp_year": year,
            "mc_kw_yr_as_filed": annual["as_filed"],
            "mc_kw_yr_target": annual["allocated"],
        }
    if manifest_method == "pjm_nits_pcaf":
        nits_year = resolved["annual_cost_kw_year"].meta.get("nits_dollar_year", year)
        return {
            "k_peak_hours": int(resolved["n_hours"].value),
            "load_year": load.get("load_year", load_year),
            "nits_dollar_year": nits_year,
        }
    if manifest_method == "nyiso_scr":
        winter = [int(month) for month in resolved["winter_months"].value]
        components = resolved["components"]
        return {
            "scr_hours_per_season": int(resolved["n_hours_per_season"].value),
            "load_year": components.meta.get("load_year", load_year),
            "winter_months": winter,
            "summer_months": derive_summer_months(winter),
            "nested_localities": list(components.meta.get("nested_localities", [])),
        }
    raise ValueError(f"No manifest params for {component} method {manifest_method!r}")


def _default_source(component: str, manifest_method: str, params: dict[str, Any]) -> str | None:
    if (component, manifest_method) == ("bulk_tx", "pjm_nits_pcaf"):
        return "PJM NITS network service rates, blended by zone"
    if manifest_method == "nyiso_scr":
        return "NYISO bulk TX constraint-group costs"
    if manifest_method == "isone_aesc_ptf":
        return (
            "AESC 2024 avoided PTF cost"
            if float(params["ptf_kw_year"]) == 69.0
            else ("AESC avoided PTF cost, overridden via --aesc-ptf-kw-year")
        )
    if manifest_method == "isone_aesc_ptf_pop_kde":
        if float(params["ptf_kw_year"]) == 69.0:
            return "AESC 2024 avoided PTF cost, CPI-adjusted to the analysis year"
        return "AESC avoided PTF cost, overridden via --aesc-ptf-kw-year, CPI-adjusted to the analysis year"
    if (component, manifest_method) == ("bulk_tx", "month_hour"):
        return (
            f"AESC 2024 avoided PTF (${float(params['ptf_kw_year']):.2f}/kW-yr, "
            f"{params['ptf_dollar_year']}$) × probability of peak / num_days, "
            f"CPI to {params['target_dollar_year']}$, scaled so the 8760 sums to that inflated annual total"
        )
    return None


def _output_frame(df: pl.DataFrame, component: str, utility: str, year: int, column: str) -> pl.DataFrame:
    if component == "dist_and_sub_tx":
        return df.select(
            "timestamp",
            pl.lit(utility).alias("utility"),
            pl.lit(year).alias("year"),
            column,
        )
    return df.select("timestamp", column)


def _write(
    df: pl.DataFrame,
    *,
    component: str,
    utility: str,
    year: int,
    column: str,
    output_base: str,
    storage_options: dict[str, str],
    path_local_base: str | None,
    spec: McSpec,
    upload: bool,
) -> None:
    frame = _output_frame(df, component, utility, year, column)
    if upload:
        path_partition = f"{output_base.rstrip('/')}/utility={utility}/year={year}"
        output_path = f"{path_partition}/data.parquet"
        warn_if_multiple_partition_parquets(
            path_partition=path_partition,
            expected_filename="data.parquet",
            context="pre-write marginal cost",
        )
        frame.write_parquet(output_path, storage_options=storage_options)
        warn_if_multiple_partition_parquets(
            path_partition=path_partition,
            expected_filename="data.parquet",
            context="post-write marginal cost",
        )
        print(f"\n✓ Saved marginal costs to {output_path}")
        record_mc_write(output_path, spec, frame, column)
    if path_local_base:
        path_local = Path(path_local_base) / f"utility={utility}" / f"year={year}" / "data.parquet"
        path_local.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path_local)
        print(f"✓ Saved local copy to {path_local}")
        if upload:
            record_mc_write(str(path_local), spec, frame, column)


def _print_summary(df: pl.DataFrame, column: str) -> None:
    print("\nTop 10 hours by cost:")
    print(df.sort(column, descending=True).head(10))
    total = float(df[column].sum())
    nonzero = df.filter(pl.col(column) > 0).height
    print(f"\n  Rows: {df.height}")
    print(f"  Annual sum: ${total:.4f}/kW-yr")
    print(f"  Non-zero hours: {nonzero}")


def load_document(path_config: str | Path) -> dict[str, Any]:
    loaded = yaml.safe_load(Path(path_config).read_text())
    if not isinstance(loaded, dict):
        raise ValueError(f"{path_config} must be a YAML mapping")
    return loaded


def utilities_for(path_config: str | Path, component: str, variant: str | None = None) -> list[str]:
    """Utilities the component (or one variant) lists. Used by the ``-all`` recipes."""
    chosen = select_block(load_document(path_config), component, variant, utility=None)
    return [str(utility) for utility in chosen["utilities"]]


def run(
    path_config: str | Path,
    component: str,
    utility: str,
    year: int,
    *,
    load_year: int | None = None,
    variant: str | None = None,
    upload: bool = False,
    path_local_base: str | None = None,
    source: str | None = None,
    sets: list[str] | None = None,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Resolve ``allocation.yaml`` and return the 8760. Writes only when asked."""
    load_year = year if load_year is None else load_year
    document = load_document(path_config)
    chosen = select_block(document, component, variant, utility)
    method_name = str(chosen["method"])
    if method_name not in METHODS:
        known = ", ".join(sorted(METHODS))
        raise ValueError(f"Unknown method {method_name!r}. Known methods: {known}")
    column = str(chosen["output_column"])
    manifest_method = str(chosen["manifest_method"])
    output_base = output_base_for_load_year(str(chosen["output_base"]), year, load_year)
    placeholders = {"utility": utility, "year": year, "load_year": load_year}
    inputs = substitute(chosen["inputs"], placeholders)
    if not isinstance(inputs, dict):
        raise ValueError("inputs must be a mapping")
    inputs = _apply_sets(inputs, sets or [])

    if upload:
        require_published_code()

    ctx = InputContext(
        utility=utility,
        year=year,
        load_year=load_year,
        storage_options=storage_options if storage_options is not None else get_aws_storage_options(),
    )
    resolved = _resolve_inputs(inputs, ctx)
    result, expected, pop = _run_method(method_name, resolved, column, combine=chosen.get("combine"))
    if pop is not None:
        print_pop_summary(pop)
    result = align_8760(result, year, column)
    _check_annual_sum(result, column, expected)
    _print_summary(result, column)

    params = manifest_params(component, manifest_method, resolved, year, load_year, pop=pop)
    provenance = [record for item in resolved.values() for record in item.inputs]
    spec = McSpec(
        mc_type=component,
        method=manifest_method,
        utility=utility,
        year=year,
        params=params,
        source=source if source is not None else _default_source(component, manifest_method, params),
        inputs=provenance,
    )
    print(f"\nManifest key: {spec.key}")
    print(f"Output base: {output_base}")
    if upload or path_local_base:
        _write(
            result,
            component=component,
            utility=utility,
            year=year,
            column=column,
            output_base=output_base,
            storage_options=ctx.storage_options,
            path_local_base=path_local_base,
            spec=spec,
            upload=upload,
        )
    elif not upload:
        print("\nNo data uploaded (pass --upload to write, or --path-local-base to keep a local copy)")
    return result


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Allocate one marginal-cost 8760 from allocation.yaml.")
    parser.add_argument("--config", required=True, help="Path to the state's allocation.yaml.")
    parser.add_argument("--component", required=True, choices=["dist_and_sub_tx", "bulk_tx"])
    parser.add_argument("--utility", default=None, help="Utility short code. Required unless --list-utilities.")
    parser.add_argument("--year", type=int, default=None, help="Output year. Required unless --list-utilities.")
    parser.add_argument("--load-year", type=int, default=None, help="Load year. Defaults to --year.")
    parser.add_argument("--variant", default=None, help="Alternative name. The default method is used when omitted.")
    parser.add_argument("--upload", action="store_true", help="Write the S3 partition and record the manifest.")
    parser.add_argument(
        "--path-local-base",
        default=None,
        help="Also write utility=/year=/data.parquet under this local directory.",
    )
    parser.add_argument("--source", default=None, help="Citation recorded on the manifest entry.")
    parser.add_argument(
        "--set",
        dest="sets",
        action="append",
        default=[],
        help="Override one input, as key=value. Repeatable. Example: include_holidays=false.",
    )
    parser.add_argument(
        "--list-utilities",
        action="store_true",
        help="Print the utilities for --component (and --variant) and exit.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.list_utilities:
        for utility in utilities_for(args.config, args.component, args.variant):
            print(utility)
        return
    if args.utility is None or args.year is None:
        raise SystemExit("--utility and --year are required")
    load_dotenv()
    run(
        args.config,
        args.component,
        args.utility,
        args.year,
        load_year=args.load_year,
        variant=args.variant,
        upload=args.upload,
        path_local_base=args.path_local_base,
        source=args.source,
        sets=args.sets,
    )


if __name__ == "__main__":
    main()
