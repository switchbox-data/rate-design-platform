"""Config loaders that are not specific to one ISO.

``literal``, ``mc_table``, ``cpi``, ``parquet``, ``hive_load``, and
``periods_yaml``. They read a number, a table, or an 8760. They do not allocate.
"""

from __future__ import annotations

import io
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from cloudpathlib import S3Path

from utils.data_prep.marginal_costs.inputs.context import InputContext, Resolved, resolve_path
from utils.data_prep.marginal_costs.mc_manifest import input_file, input_partition, partition_file_path
from utils.data_prep.marginal_costs.supply_utils import normalize_load_to_cairo_8760, remap_year_if_needed
from utils.file_io import read_parquet_local_or_s3
from utils.pre.season_config import load_winter_months_from_periods, normalize_winter_months

_DEFAULT_CPI_S3_BASE = "s3://data.sb/fred/cpi/"


@dataclass(frozen=True, slots=True)
class ResolvedAnnualMc:
    """The $/kW-yr a method should recover, as filed and after any CPI."""

    as_filed: float
    allocated: float
    dollar_year: int | None
    cpi_factor: float


def _mc_meta(resolved: ResolvedAnnualMc, target_dollar_year: int) -> dict[str, Any]:
    return {
        "as_filed": resolved.as_filed,
        "allocated": resolved.allocated,
        "dollar_year": resolved.dollar_year,
        "cpi_factor": resolved.cpi_factor,
        "target_dollar_year": target_dollar_year,
    }


def literal(entry: Mapping[str, Any], ctx: InputContext, role: str) -> Resolved:
    """The YAML ``value``, already placeholder-substituted by the runner."""
    del ctx, role
    return Resolved(entry["value"])


def load_marginal_cost_table(mc_table_path: str) -> pl.DataFrame:
    """Load a marginal-cost CSV from a local path or S3 and strip stray whitespace."""
    if mc_table_path.startswith("s3://"):
        csv_bytes = S3Path(mc_table_path).read_bytes()
        df = pl.read_csv(io.BytesIO(csv_bytes))
    else:
        df = pl.read_csv(mc_table_path)
    df = df.rename({c: c.strip() for c in df.columns})
    df = df.with_columns([pl.col(c).str.strip_chars() for c in df.columns if df[c].dtype == pl.String])
    print(f"Loaded marginal cost table with {len(df)} rows")
    return df


def validate_mc_table_path(mc_table_path: str) -> None:
    """Fail early if the marginal-cost table path does not exist."""
    if mc_table_path.startswith("s3://"):
        if not S3Path(mc_table_path).exists():
            raise FileNotFoundError(f"Marginal cost table not found: {mc_table_path}")
        return
    if not Path(mc_table_path).exists():
        raise FileNotFoundError(f"Marginal cost table not found: {mc_table_path}")


def get_marginal_cost_for_utility(
    mc_df: pl.DataFrame,
    utility: str,
    column: str = "sub_tx_and_dist_mc_kw_yr",
) -> float:
    """Return one utility's ``$/kW-yr`` from ``column``."""
    if column not in mc_df.columns:
        raise ValueError(f"Marginal cost table is missing column {column!r}")
    row = mc_df.filter(pl.col("utility") == utility)
    if len(row) == 0:
        raise ValueError(f"No marginal cost data found for {utility}")
    if len(row) > 1:
        raise ValueError(f"Multiple marginal cost entries found for {utility}")
    mc = float(row[column][0])
    print(f"\nMarginal Cost for {utility}:")
    print(f"  Sub-Tx and Distribution: ${mc:.2f}/kW-yr")
    return mc


def load_cpi_inflation_factor(
    cpi_s3_base: str,
    from_year: int,
    to_year: int,
    storage_options: dict[str, str],
) -> float:
    """Return CPI_to / CPI_from from the latest CPIAUCSL snapshot under ``cpi_s3_base``.

    The parquets have columns ``year`` and ``value``. The newest file wins when
    snapshots overlap, using the ``YYYYMMDD`` in the filename.
    """
    cpi_glob = cpi_s3_base.rstrip("/") + "/*.parquet"
    collected = pl.scan_parquet(cpi_glob, storage_options=storage_options, include_file_paths="path").collect()
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from CPI collect()")
    vintage = pl.col("path").str.extract(r"(\d{8})\.parquet$").cast(pl.Int64)
    latest_vintage = collected.select(vintage.max()).item()
    if latest_vintage is None:
        raise ValueError(f"CPI parquet names under {cpi_s3_base} have no YYYYMMDD before .parquet")
    cpi_df = collected.filter(vintage == latest_vintage)

    years_needed = {from_year, to_year}
    available_years = set(cpi_df["year"].to_list())
    missing = years_needed - available_years
    if missing:
        raise ValueError(
            f"CPI data missing for year(s) {sorted(missing)}. "
            f"Available years: {sorted(available_years)}. "
            f"Refresh CPI data with `just fetch-cpi` in data/fred/cpi/."
        )

    cpi_from = float(cpi_df.filter(pl.col("year") == from_year)["value"][0])
    cpi_to = float(cpi_df.filter(pl.col("year") == to_year)["value"][0])
    factor = cpi_to / cpi_from
    print(f"\nCPI Inflation Adjustment ({from_year} → {to_year} dollars):")
    print(f"  Snapshot: {latest_vintage}")
    print(f"  CPI {from_year}: {cpi_from:.4f}")
    print(f"  CPI {to_year}:   {cpi_to:.4f}")
    print(f"  Factor: {factor:.6f}")
    return factor


def inflate_annual_mc(
    as_filed: float,
    dollar_year: int,
    target_dollar_year: int,
    cpi_s3_base: str,
    storage_options: dict[str, str],
) -> ResolvedAnnualMc:
    """Express a ``$/kW-yr`` filed in ``dollar_year`` in ``target_dollar_year``.

    CPI is only read when the years differ.
    """
    if dollar_year == target_dollar_year:
        print(f"\n  MC already in {target_dollar_year} dollars — no inflation applied")
        return ResolvedAnnualMc(as_filed, as_filed, dollar_year, 1.0)
    cpi_factor = load_cpi_inflation_factor(cpi_s3_base, dollar_year, target_dollar_year, storage_options)
    allocated = as_filed * cpi_factor
    print(f"  Inflated MC: ${as_filed:.2f} → ${allocated:.2f}/kW-yr")
    return ResolvedAnnualMc(as_filed, allocated, dollar_year, cpi_factor)


def resolve_annual_mc(
    mc_df: pl.DataFrame,
    utility: str,
    target_dollar_year: int,
    cpi_s3_base: str,
    storage_options: dict[str, str],
    *,
    column: str = "sub_tx_and_dist_mc_kw_yr",
    dollar_year_column: str = "dollar_year",
) -> ResolvedAnnualMc:
    """Read one utility's ``$/kW-yr`` and inflate it when its dollar year differs."""
    as_filed = get_marginal_cost_for_utility(mc_df, utility, column)
    if dollar_year_column not in mc_df.columns:
        print("\n  No dollar_year column in MC table — using raw value (no inflation)")
        return ResolvedAnnualMc(as_filed, as_filed, None, 1.0)
    dollar_year_val = mc_df.filter(pl.col("utility") == utility)[dollar_year_column][0]
    if dollar_year_val is None:
        print("\n  dollar_year is null in MC table — no inflation applied")
        return ResolvedAnnualMc(as_filed, as_filed, None, 1.0)
    return inflate_annual_mc(
        as_filed,
        int(dollar_year_val),
        target_dollar_year,
        cpi_s3_base,
        storage_options,
    )


def mc_table(entry: Mapping[str, Any], ctx: InputContext, role: str) -> Resolved:
    """One utility's ``$/kW-yr`` from a CSV, CPI-adjusted when the dollar years differ."""
    del role
    path = resolve_path(str(entry["path"]))
    validate_mc_table_path(path)
    table = load_marginal_cost_table(path)
    column = str(entry["column"])
    target = int(entry["target_dollar_year"]) if "target_dollar_year" in entry else ctx.year
    dollar_col = entry.get("dollar_year_column")
    cpi_base = str(entry.get("cpi_s3_base", _DEFAULT_CPI_S3_BASE))
    resolved = resolve_annual_mc(
        table,
        ctx.utility,
        target,
        cpi_base,
        ctx.storage_options,
        column=column,
        dollar_year_column=str(dollar_col) if dollar_col else "",
    )
    inputs = [input_file("mc_table", path)]
    if resolved.cpi_factor != 1.0:
        inputs.append(input_partition("cpi", cpi_base))
    return Resolved(resolved.allocated, inputs, _mc_meta(resolved, target))


def cpi(entry: Mapping[str, Any], ctx: InputContext, role: str) -> Resolved:
    """A literal ``$/kW-yr`` times CPI from ``dollar_year`` to ``target_dollar_year``."""
    del role
    as_filed = float(entry["value"])
    dollar_year = int(entry["dollar_year"])
    target = int(entry["target_dollar_year"])
    cpi_base = str(entry.get("cpi_s3_base", _DEFAULT_CPI_S3_BASE))
    resolved = inflate_annual_mc(as_filed, dollar_year, target, cpi_base, ctx.storage_options)
    inputs: list[dict[str, Any]] = []
    if resolved.cpi_factor != 1.0:
        inputs.append(input_partition("cpi", cpi_base))
    return Resolved(resolved.allocated, inputs, _mc_meta(resolved, target))


def load_utility_load_profile(
    s3_base: str,
    year_load: int,
    utility: str,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load a hive ``utility=X/year=YYYY`` profile.

    The layout is the one NYISO, PJM, and ISO-NE utility loads share.
    """
    base = s3_base.rstrip("/") + "/"
    scan_kwargs: dict[str, Any] = {"hive_partitioning": True}
    if base.startswith("s3://"):
        scan_kwargs["storage_options"] = storage_options
    collected = (
        pl.scan_parquet(base, **scan_kwargs).filter(pl.col("utility") == utility, pl.col("year") == year_load).collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from utility load collect()")
    if collected.is_empty():
        raise FileNotFoundError(f"Utility load profile not found for utility={utility}, year={year_load} under {base}")
    print(f"Loaded {len(collected):,} hourly load records for {utility} (year {year_load})")
    return collected


def load_utility_load_file(path: str, storage_options: dict[str, str]) -> pl.DataFrame:
    """Load one load parquet that is not in the hive utility/year layout.

    ``storage_options`` is accepted for callers that still pass it. S3 reads go
    through :func:`read_parquet_local_or_s3`.
    """
    del storage_options
    df = read_parquet_local_or_s3(path)
    missing = {"timestamp", "load_mw"} - set(df.columns)
    if missing:
        raise ValueError(f"Utility load file {path} is missing columns: {sorted(missing)}")
    if df.is_empty():
        raise FileNotFoundError(f"Utility load file is empty: {path}")
    print(f"Loaded {len(df):,} hourly load records from {path}")
    return df


def resolve_utility_load(
    path_utility_load: str | None,
    utility_load_s3_base: str | None,
    year_load: int,
    utility: str,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load the profile from one parquet or from the hive utility/year scan.

    Exactly one of the two sources must be set.
    """
    if path_utility_load and utility_load_s3_base:
        raise ValueError("Pass either --path-utility-load or --utility-load-s3-base, not both.")
    if path_utility_load:
        return load_utility_load_file(path_utility_load, storage_options)
    if utility_load_s3_base:
        return load_utility_load_profile(utility_load_s3_base, year_load, utility, storage_options)
    raise ValueError("Pass --path-utility-load or --utility-load-s3-base.")


def _default_hive_load_source(path: str) -> str:
    """``s3://data.sb/nyiso/...`` -> ``nyiso_utility``. A local base uses its directory name."""
    if path.startswith("s3://"):
        iso = path.removeprefix("s3://").split("/")[1]
        return f"{iso}_utility"
    return f"{Path(path.rstrip('/')).name}_utility"


def _aligned_load(raw: pl.DataFrame, utility: str, load_year: int, output_year: int) -> pl.DataFrame:
    df = normalize_load_to_cairo_8760(raw, utility, load_year)
    return remap_year_if_needed(df, "timestamp", load_year, output_year)


def parquet(entry: Mapping[str, Any], ctx: InputContext, role: str) -> Resolved:
    """One parquet file.

    A load file (``load_year`` or ``load_source`` on the entry) is checked for
    ``timestamp`` and ``load_mw``, filtered to ``load_year``, and aligned to the
    output year. Any other parquet is returned as read. Month-hour tables take
    that path.
    """
    path = resolve_path(str(entry["path"]))
    df = read_parquet_local_or_s3(path)
    recorded = [input_file(role, path)]
    if "load_year" in entry or "load_source" in entry:
        missing = {"timestamp", "load_mw"} - set(df.columns)
        if missing:
            raise ValueError(f"Utility load file {path} is missing columns: {sorted(missing)}")
        if df.is_empty():
            raise FileNotFoundError(f"Utility load file is empty: {path}")
        load_year = int(entry.get("load_year", ctx.load_year))
        aligned = _aligned_load(df, ctx.utility, load_year, ctx.year)
        source = str(entry.get("load_source") or Path(path).stem)
        return Resolved(aligned, recorded, {"load_year": load_year, "load_source": source, "path": path})
    return Resolved(df, recorded, {"path": path})


def hive_load(entry: Mapping[str, Any], ctx: InputContext, role: str) -> Resolved:
    """An 8760 from ``utility={utility}/year={load_year}`` under ``path``, on the output year."""
    base = str(entry["path"])
    load_year = int(entry.get("load_year", ctx.load_year))
    raw = load_utility_load_profile(base, load_year, ctx.utility, ctx.storage_options)
    aligned = _aligned_load(raw, ctx.utility, load_year, ctx.year)
    source = str(entry["load_source"]) if "load_source" in entry else _default_hive_load_source(base)
    partition = partition_file_path(base, ctx.utility, load_year, "")
    return Resolved(
        aligned,
        [input_partition(role, partition)],
        {"load_year": load_year, "load_source": source},
    )


def periods_yaml(entry: Mapping[str, Any], ctx: InputContext, role: str) -> Resolved:
    """Winter months from a periods YAML, or ``default`` when that file is absent."""
    del ctx
    path = Path(resolve_path(str(entry["path"])))
    default = entry.get("default")
    if path.exists():
        months = load_winter_months_from_periods(
            path,
            default_winter_months=default if isinstance(default, list) else None,
        )
        inputs = [input_file(role, str(path))]
    else:
        if not isinstance(default, list):
            raise FileNotFoundError(f"Periods YAML not found and no default winter months: {path}")
        months = normalize_winter_months(default)
        inputs = []
    return Resolved(months, inputs, {})


def voltage_from_table_paths(path_weekday: str, path_weekend: str) -> str:
    """``secondary`` or ``primary``, read from both month-hour table names."""
    found = {
        voltage
        for voltage in ("secondary", "primary")
        for path in (path_weekday, path_weekend)
        if Path(path).stem.endswith(f"_{voltage}")
    }
    if len(found) != 1:
        raise ValueError(
            f"Cannot tell the voltage from the month-hour table names ({path_weekday}, {path_weekend}); pass --voltage."
        )
    return found.pop()


# The dist script and its tests still import this private name.
_voltage_from_table_paths = voltage_from_table_paths
