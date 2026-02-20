"""Utility functions for Cairo-related operations."""

from __future__ import annotations

import io
import logging
from copy import deepcopy
from functools import reduce
from pathlib import Path, PurePath
from typing import cast

import numpy as np
import pandas as pd
import polars as pl
import yaml
from cairo.rates_tool import config
from cairo.rates_tool.loads import __timeshift__
from cloudpathlib import S3Path

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options

CambiumPathLike = str | Path | S3Path
WorkbookSource = Path | io.BytesIO
"""A local file path or in-memory buffer that ``pd.read_excel`` can consume."""
_CAIRO_MULTITARIFF_WORKAROUND_APPLIED = False
log = logging.getLogger("rates_analysis").getChild("utils.cairo")


def enable_cairo_multitariff_mapping_workaround() -> None:
    """Patch CAIRO to support multi-tariff mappings with preinitialized tariff dicts.

    CAIRO currently re-initializes tariffs from disk when more than one tariff key is
    present in a map, even when a ready-to-use `tariff_base` dict is already passed.
    In some code paths (notably gas post-processing), this drops explicit path mapping
    and fails with "tariff path build failed".

    This workaround reuses the provided `tariff_base` values directly when:
    - `tariff_base` is a dict with more than one key, and
    - keys referenced by `tariff_map` are all present in `tariff_base`.
    """

    global _CAIRO_MULTITARIFF_WORKAROUND_APPLIED
    if _CAIRO_MULTITARIFF_WORKAROUND_APPLIED:
        return

    from cairo.rates_tool import tariffs as tariff_funcs

    original_load_base_tariffs = tariff_funcs._load_base_tariffs

    def _patched_load_base_tariffs(
        tariff_base: dict[str, dict],
        tariff_map: str | PurePath | pd.DataFrame | None,
        prototype_ids: list[int] | None = None,
    ) -> tuple[dict[int, str], dict[str, dict]]:
        should_try_workaround = (
            isinstance(tariff_base, dict)
            and len(tariff_base.keys()) > 1
            and tariff_map is not None
        )
        if not should_try_workaround:
            return original_load_base_tariffs(tariff_base, tariff_map, prototype_ids)

        if isinstance(tariff_map, (str, PurePath)):
            tariff_map_df = tariff_funcs.__load_tariff_maps__(tariff_map)
        else:
            if not isinstance(tariff_map, pd.DataFrame):
                return original_load_base_tariffs(
                    tariff_base, tariff_map, prototype_ids
                )
            tariff_map_df = tariff_map
            if "bldg_id" in tariff_map_df.columns:
                tariff_map_df = tariff_map_df.set_index("bldg_id")
            elif tariff_map_df.index.name != "bldg_id":
                return original_load_base_tariffs(
                    tariff_base, tariff_map, prototype_ids
                )

        if "tariff_key" not in tariff_map_df.columns:
            return original_load_base_tariffs(tariff_base, tariff_map, prototype_ids)

        prototype_tariff_map = tariff_map_df["tariff_key"].to_dict()
        unique_tariffs = set(prototype_tariff_map.values())
        if not unique_tariffs.issubset(set(tariff_base.keys())):
            return original_load_base_tariffs(tariff_base, tariff_map, prototype_ids)

        # Reuse already initialized tariff dicts instead of reloading by default path.
        tariff_dict = {k: deepcopy(tariff_base[k]) for k in unique_tariffs}
        return prototype_tariff_map, tariff_dict

    tariff_funcs._load_base_tariffs = _patched_load_base_tariffs
    _CAIRO_MULTITARIFF_WORKAROUND_APPLIED = True
    log.info(".... Enabled CAIRO multi-tariff mapping workaround")


def _normalize_cambium_path(cambium_scenario: CambiumPathLike):  # noqa: ANN201
    """Return a single path-like (Path or S3Path) for Cambium CSV or Parquet."""
    if isinstance(cambium_scenario, S3Path):
        return cambium_scenario
    if isinstance(cambium_scenario, Path):
        return cambium_scenario
    if isinstance(cambium_scenario, str):
        if cambium_scenario.startswith("s3://"):
            return S3Path(cambium_scenario)
        if "/" in cambium_scenario or cambium_scenario.endswith((".csv", ".parquet")):
            return Path(cambium_scenario)
        return config.MARGINALCOST_DIR / f"{cambium_scenario}.csv"
    raise TypeError(
        f"cambium_scenario must be str, Path, or S3Path; got {type(cambium_scenario)}"
    )


def _load_cambium_marginal_costs(
    cambium_scenario: CambiumPathLike, target_year: int
) -> pd.DataFrame:
    """
    Load Cambium marginal costs from CSV or Parquet (local or S3). Returns costs in $/kWh.

    Accepts: scenario name (str → CSV under config dir), local path (str or Path),
    or S3 URI (str or S3Path). Example S3 Parquet:
    s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet

    Assumptions (verified against S3 Parquet and repo CSV example_marginal_costs.csv):
    - CSV: first 5 rows are metadata; row 6 is header; data has columns timestamp,
      energy_cost_enduse, capacity_cost_enduse. Costs are in $/MWh.
    - Parquet: columns timestamp (datetime), energy_cost_enduse, capacity_cost_enduse
      (float). Costs are in $/MWh. Exactly 8760 rows (hourly). No partition columns
      in the DataFrame (single-file read).
    - Both: we divide cost columns by 1000 to get $/kWh; then common_year alignment,
      __timeshift__ to target_year, and tz_localize("EST") so output matches CAIRO.
    """
    path = _normalize_cambium_path(cambium_scenario)
    if not path.exists():
        raise FileNotFoundError(f"Cambium marginal cost file {path} does not exist")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        if isinstance(path, S3Path):
            raw = path.read_bytes()
            df = pd.read_csv(
                io.BytesIO(raw),
                skiprows=5,
                index_col="timestamp",
                parse_dates=True,
            )
        else:
            df = pd.read_csv(
                path,
                skiprows=5,
                index_col="timestamp",
                parse_dates=True,
            )
    elif suffix == ".parquet":
        if isinstance(path, S3Path):
            # Read as bytes and pass BytesIO so PyArrow reads a single file. If we pass
            # the S3 path (even with explicit S3FileSystem), PyArrow infers a partitioned
            # dataset from path segments like scenario=MidCase/... and raises
            # ArrowTypeError when merging partition schemas.
            raw = path.read_bytes()
            df = pd.read_parquet(io.BytesIO(raw), engine="pyarrow")
        else:
            df = pd.read_parquet(path)
        if "timestamp" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
            df = df.set_index("timestamp")
        if df.index.name != "time":
            df.index.name = "time"
    else:
        raise ValueError(
            f"Cambium file must be .csv or .parquet; got {path} (suffix {suffix})"
        )

    keep_cols = {
        "energy_cost_enduse": "Marginal Energy Costs ($/kWh)",
        "capacity_cost_enduse": "Marginal Capacity Costs ($/kWh)",
    }
    numeric_input_cols = list(keep_cols.keys())
    for col in numeric_input_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=numeric_input_cols, how="any")
    if df.empty:
        raise ValueError(
            f"Cambium marginal cost file {path} has no valid numeric rows in required "
            f"columns: {numeric_input_cols}"
        )

    df = df.loc[:, list(keep_cols.keys())].rename(columns=keep_cols)
    df.loc[:, [c for c in df.columns if "/kWh" in c]] /= 1000  # $/MWh → $/kWh

    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.loc[~df.index.isna()].copy()
    if df.empty:
        raise ValueError(f"Cambium marginal cost file {path} has no valid timestamps")

    common_years = [2017, 2023, 2034, 2045, 2051]
    year_diff = [abs(y - target_year) for y in common_years]
    common_year = common_years[year_diff.index(min(year_diff))]
    df.index = pd.DatetimeIndex(
        [t.replace(year=common_year) for t in df.index],
        name="time",
    )
    df = __timeshift__(df, target_year)
    df.index = df.index.tz_localize("EST")
    df.index.name = "time"
    return df


def _normalize_iso_market_path(
    analysis_year: int, market_data_path: str | Path | None
) -> Path | io.BytesIO:
    """Resolve ISO market workbook path, defaulting to CAIRO marginal cost directory.

    For S3 URIs the workbook is downloaded into an in-memory BytesIO buffer so
    that ``pd.read_excel`` (which cannot read S3 directly) can consume it.
    """
    if market_data_path is None:
        path: Path | S3Path = config.MARGINALCOST_DIR / f"{analysis_year}_smd_hourly.xlsx"
    elif isinstance(market_data_path, Path):
        path = market_data_path
    elif isinstance(market_data_path, str) and market_data_path.startswith("s3://"):
        s3_path = S3Path(market_data_path)
        if not s3_path.exists():
            raise FileNotFoundError(f"ISO market workbook does not exist on S3: {s3_path}")
        log.info("Downloading ISO market workbook from %s", s3_path)
        buf = io.BytesIO(s3_path.read_bytes())
        buf.name = s3_path.name  # type: ignore[attr-defined]  # helps openpyxl/pd identify filetype
        return buf
    else:
        path = Path(str(market_data_path))

    if not path.exists():
        raise FileNotFoundError(f"ISO market workbook does not exist: {path}")
    suffix = path.suffix.lower() if isinstance(path, Path) else PurePath(str(path)).suffix.lower()
    if suffix != ".xlsx":
        raise ValueError(f"ISO market workbook must be an .xlsx file: {path}")
    return path  # type: ignore[return-value]


def _load_isone_fca_segments(
    year_focus: int,
) -> tuple[list[dict[str, float]], str | None]:
    """Load ISO-NE FCA assumptions for a given year from YAML config."""
    assumptions_path = (
        Path(__file__).resolve().parents[1]
        / "rate_design"
        / "ri"
        / "hp_rates"
        / "config"
        / "marginal_costs"
        / "isone_fca_assumptions.yaml"
    )
    if not assumptions_path.exists():
        raise FileNotFoundError(
            "Missing ISO-NE FCA assumptions file: " f"{assumptions_path}"
        )

    with assumptions_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML format in {assumptions_path}")

    years = data.get("years")
    if not isinstance(years, dict):
        raise ValueError(f"`years` mapping missing in {assumptions_path}")

    year_data = years.get(year_focus) or years.get(str(year_focus))
    if not isinstance(year_data, dict):
        raise ValueError(
            f"No ISO-NE FCA assumptions configured for year_focus={year_focus} "
            f"in {assumptions_path}"
        )

    raw_segments = year_data.get("segments")
    if not isinstance(raw_segments, list) or not raw_segments:
        raise ValueError(
            f"`segments` missing/empty for year_focus={year_focus} in {assumptions_path}"
        )

    segments: list[dict[str, float]] = []
    for raw in raw_segments:
        if not isinstance(raw, dict):
            raise ValueError(
                f"Invalid segment entry for year_focus={year_focus}: {raw!r}"
            )
        try:
            months = float(raw["months"])
            payment_rate = float(raw["payment_rate_per_kw_month"])
            cso_mw = float(raw["capacity_supply_obligation_mw"])
        except KeyError as exc:
            raise ValueError(
                "Each segment must include `months`, `payment_rate_per_kw_month`, "
                f"and `capacity_supply_obligation_mw` (year_focus={year_focus})"
            ) from exc
        segments.append(
            {
                "months": months,
                "payment_rate_per_kw_month": payment_rate,
                "capacity_supply_obligation_mw": cso_mw,
            }
        )

    source_url: str | None = None
    source = data.get("source")
    if isinstance(source, dict):
        primary = source.get("primary")
        if isinstance(primary, str):
            source_url = primary
    return segments, source_url


def _load_iso_marginal_costs(
    analysis_year: int,
    market_data_path: str | Path | None = None,
) -> pd.DataFrame:
    """Load ISO-NE hourly marginal energy + capacity costs in $/kWh."""
    workbook_src = _normalize_iso_market_path(analysis_year, market_data_path)
    lmp_df, cap_df, ancillary_df = _return_full_isone_costs(
        year_focus=analysis_year,
        workbook_path=workbook_src,
    )

    energy_df = pd.DataFrame(
        pd.merge(lmp_df, ancillary_df, left_index=True, right_index=True).sum(axis=1),
        columns=["Marginal Energy Costs ($/kWh)"],
    )
    energy_df /= 1000

    capacity_df = pd.DataFrame(
        cap_df.values,
        index=cap_df.index,
        columns=["Marginal Capacity Costs ($/kWh)"],
    )
    capacity_df /= 1000

    marginal_df = pd.merge(energy_df, capacity_df, left_index=True, right_index=True)

    if marginal_df.index[0].is_leap_year and len(marginal_df.index) == 8784:
        new_index = marginal_df.loc[
            ~((marginal_df.index.month == 2) & (marginal_df.index.day == 29))
        ].index
        marginal_df = marginal_df.loc[
            ~((marginal_df.index.month == 12) & (marginal_df.index.day == 31))
        ]
        marginal_df.index = new_index
        if len(marginal_df.index) != 8760:
            raise ValueError("ISO marginal cost index normalization did not produce 8760 rows")

    if marginal_df.index.tz is None:
        marginal_df.index = pd.DatetimeIndex(marginal_df.index).tz_localize("EST")
    marginal_df.index.name = "time"
    return marginal_df


def _open_workbook(source: WorkbookSource) -> pd.ExcelFile:
    """Open an Excel workbook from a local path or BytesIO buffer."""
    if isinstance(source, io.BytesIO):
        source.seek(0)
    return pd.ExcelFile(source)


def _return_full_isone_costs(
    year_focus: int,
    workbook_path: WorkbookSource,
    region: str = "RI",
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Read ISO-NE market workbook and return (LMP, capacity, ancillary) series.

    Parameters
    ----------
    year_focus : int
        Analysis year.
    workbook_path : WorkbookSource
        Path or in-memory buffer for the ``{year}_smd_hourly.xlsx`` workbook.
    region : str
        Sheet name to read for energy LMP/demand data. Defaults to ``"RI"``
        so that RI-specific zonal LMPs are used directly.  Falls back to the
        legacy SEMA/WCMA/NEMA load-weighted average if the requested sheet
        is not in the workbook.
    """
    # Open once; all downstream functions reuse this ExcelFile handle.
    xl = _open_workbook(workbook_path)
    if region in xl.sheet_names:
        lmp_df = _extract_regional_lmp(
            year_focus=year_focus,
            region_name=region,
            xl=xl,
        )
    else:
        log.warning(
            "Sheet '%s' not found in workbook; falling back to load-weighted "
            "SEMA/WCMA/NEMA LMP",
            region,
        )
        loads_df, prices_df = _return_prices_and_load_by_region(
            year_focus=year_focus,
            xl=xl,
        )
        lmp_df = _return_ISONE_lmps(loads_df, prices_df)

    cap_df = _return_ISONE_capacity_prices(year_focus, xl=xl)
    ancillary_df = _extract_ancillary_prices(year_focus, xl=xl)
    return lmp_df, cap_df, ancillary_df


def _extract_regional_lmp(
    year_focus: int,
    region_name: str,
    xl: pd.ExcelFile,
) -> pd.Series:
    """Read a single region sheet and return RT_LMP as a Series."""
    loads_df, prices_df = _extract_isone_regional_data(
        year_focus=year_focus,
        region_name=region_name,
        xl=xl,
    )
    # Return just the LMP column as a plain Series (no region prefix)
    return prices_df.squeeze().rename("weighted_LMP")


def _return_prices_and_load_by_region(
    year_focus: int,
    xl: pd.ExcelFile,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return regional load and LMP data for SEMA/WCMA/NEMA (legacy fallback)."""
    load_frames: list[pd.DataFrame] = []
    price_frames: list[pd.DataFrame] = []
    for region in ["SEMA", "WCMA", "NEMA"]:
        region_loads, region_prices = _extract_isone_regional_data(
            year_focus=year_focus,
            region_name=region,
            xl=xl,
        )
        load_frames.append(region_loads)
        price_frames.append(region_prices)

    loads_df = reduce(
        lambda left, right: pd.merge(left, right, right_index=True, left_index=True),
        load_frames,
    )
    prices_df = reduce(
        lambda left, right: pd.merge(left, right, right_index=True, left_index=True),
        price_frames,
    )
    return loads_df, prices_df


def _extract_isone_regional_data(
    year_focus: int,
    region_name: str,
    xl: pd.ExcelFile,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract hourly demand/LMP series from an ISO-NE region sheet."""
    valid_regions = {"SEMA", "WCMA", "NEMA", "ISO NE CA", "RI", "CT", "ME", "NH", "VT"}
    if region_name not in valid_regions:
        raise ValueError(
            f"region_name must be one of {sorted(valid_regions)}"
        )

    sheet = pd.read_excel(
        xl,
        sheet_name=region_name,
        index_col="Date",
        parse_dates=True,
    )

    # The workbook uses Eastern Prevailing Time (with DST) and marks the
    # fall-back repeated hour as "02X".  Rather than parsing those timestamps
    # (which creates duplicates), we assign a clean EST 8760 index positionally.
    # The data is already in chronological order with exactly 8760 rows.
    est_index = pd.date_range(
        start=f"{year_focus}-01-01",
        periods=len(sheet),
        freq="h",
        tz="EST",
    )
    sheet.index = est_index

    isone_loads = sheet[["RT_Demand"]].rename(
        columns={"RT_Demand": f"{region_name}_Demand"}
    )
    isone_prices = sheet[["RT_LMP"]].rename(columns={"RT_LMP": f"{region_name}_LMP"})
    return isone_loads, isone_prices


def _return_ISONE_lmps(  # noqa: N802
    loads_df: pd.DataFrame, prices_df: pd.DataFrame
) -> pd.Series:
    """Calculate load-weighted ISO-NE LMP across SEMA/WCMA/NEMA."""
    isone_lmp_df = pd.merge(loads_df, prices_df, right_index=True, left_index=True)
    isone_lmp_df["Total_Demand"] = isone_lmp_df[
        ["SEMA_Demand", "WCMA_Demand", "NEMA_Demand"]
    ].sum(axis=1)
    isone_lmp_df[["SEMA_weight", "WCMA_weight", "NEMA_weight"]] = isone_lmp_df[
        ["SEMA_Demand", "WCMA_Demand", "NEMA_Demand"]
    ].divide(isone_lmp_df["Total_Demand"], axis=0)

    isone_lmp_df["SEMA_weighted_LMP"] = isone_lmp_df["SEMA_LMP"].mul(
        isone_lmp_df["SEMA_weight"], axis=0
    )
    isone_lmp_df["WCMA_weighted_LMP"] = isone_lmp_df["WCMA_LMP"].mul(
        isone_lmp_df["WCMA_weight"], axis=0
    )
    isone_lmp_df["NEMA_weighted_LMP"] = isone_lmp_df["NEMA_LMP"].mul(
        isone_lmp_df["NEMA_weight"], axis=0
    )
    isone_lmp_df["weighted_LMP"] = isone_lmp_df[
        ["SEMA_weighted_LMP", "WCMA_weighted_LMP", "NEMA_weighted_LMP"]
    ].sum(axis=1)
    return isone_lmp_df["weighted_LMP"]


def _return_ISONE_capacity_prices(  # noqa: N802
    year_focus: int,
    xl: pd.ExcelFile,
) -> pd.Series:
    """Calculate ISO-NE capacity marginal costs ($/MWh), allocated to peak hours."""
    fca_segments, source_url = _load_isone_fca_segments(year_focus)
    if source_url is not None:
        log.info(
            "Using ISO-NE FCA assumptions for year %s from %s",
            year_focus,
            source_url,
        )

    # Annualized by market-year overlap (6 months each).
    fca_annual_costs = np.sum(
        [
            segment["capacity_supply_obligation_mw"]
            * 1000
            * segment["payment_rate_per_kw_month"]
            * segment["months"]
            for segment in fca_segments
        ]
    )

    loads_df, _ = _extract_isone_regional_data(
        year_focus=year_focus,
        region_name="ISO NE CA",
        xl=xl,
    )
    loads_df = loads_df.rename(columns={"ISO NE CA_Demand": "Total_Demand"})

    mw_capacity_thresh = float(
        min(
            loads_df["Total_Demand"].nlargest(101).min(),
            loads_df["Total_Demand"].max() * 0.95,
        )
    )

    exceed_thresh = loads_df["Total_Demand"].loc[
        loads_df["Total_Demand"] >= mw_capacity_thresh
    ]
    load_above = (exceed_thresh - mw_capacity_thresh).clip(lower=0)
    load_above_ratio = load_above / load_above.sum()

    cap_costs_df = pd.DataFrame(
        {"demand_ratio": load_above_ratio.values},
        index=exceed_thresh.index,
    )
    cap_costs_df["price_per_hour"] = fca_annual_costs * cap_costs_df["demand_ratio"]
    cap_costs_df["price_per_mwh"] = cap_costs_df["price_per_hour"].divide(
        exceed_thresh, axis=0
    )

    filler_df = pd.DataFrame(
        {"price_per_mwh": [0.0] * (len(loads_df.index) - len(cap_costs_df.index))},
        index=loads_df.loc[~loads_df.index.isin(cap_costs_df.index)].index,
    )
    cap_costs = pd.concat([cap_costs_df[["price_per_mwh"]], filler_df]).sort_index()
    return cap_costs.squeeze()


def _extract_ancillary_prices(year_focus: int, xl: pd.ExcelFile) -> pd.Series:
    """Return ISO-NE ancillary prices (reg service + reg capacity) in $/MWh."""
    sheet = pd.read_excel(
        xl,
        sheet_name="ISO NE CA",
        index_col="Date",
        parse_dates=True,
    )
    # Assign clean EST 8760 index (same rationale as _extract_isone_regional_data)
    est_index = pd.date_range(
        start=f"{year_focus}-01-01",
        periods=len(sheet),
        freq="h",
        tz="EST",
    )
    sheet.index = est_index

    ancillary_prices = sheet[["Reg_Service_Price", "Reg_Capacity_Price"]].copy()
    ancillary_prices["Total_ancillary"] = ancillary_prices.sum(axis=1)
    return ancillary_prices["Total_ancillary"]


def build_bldg_id_to_load_filepath(
    path_resstock_loads: Path,
    building_ids: list[int] | None = None,
    return_path_base: Path | None = None,
) -> dict[int, Path]:
    """
    Build a dictionary mapping building IDs to their load file paths.

    Args:
        path_resstock_loads: Directory containing parquet load files to scan
        building_ids: Optional list of building IDs to include. If None, includes all.
        return_path_base: Base directory for returned paths.
            If None, returns actual file paths from path_resstock_loads.
            If Path, returns paths as return_path_base / filename.

    Returns:
        Dictionary mapping building ID (int) to full file path (Path)

    Raises:
        FileNotFoundError: If path_resstock_loads does not exist
    """
    if not path_resstock_loads.exists():
        raise FileNotFoundError(f"Load directory not found: {path_resstock_loads}")

    building_ids_set = set(building_ids) if building_ids is not None else None

    bldg_id_to_load_filepath = {}
    for parquet_file in path_resstock_loads.glob("*.parquet"):
        try:
            bldg_id = int(parquet_file.stem.split("-")[0])
        except ValueError:
            continue  # Skip files that don't match expected pattern

        if building_ids_set is not None and bldg_id not in building_ids_set:
            continue

        if return_path_base is None:
            filepath = parquet_file
        else:
            filepath = return_path_base / parquet_file.name

        bldg_id_to_load_filepath[bldg_id] = filepath

    return bldg_id_to_load_filepath


def load_distribution_marginal_costs(
    path: str | Path,
) -> pd.Series:
    """Load distribution marginal costs from a parquet path and return a tz-aware Series."""
    path_str = str(path)
    if path_str.startswith("s3://"):
        distribution_mc_scan: pl.LazyFrame = pl.scan_parquet(
            path_str,
            storage_options=get_aws_storage_options(),
        )
    else:
        distribution_mc_scan = pl.scan_parquet(path_str)
    distribution_mc_df = cast(pl.DataFrame, distribution_mc_scan.collect())
    distribution_marginal_costs = distribution_mc_df.to_pandas()
    required_cols = {"timestamp", "mc_total_per_kwh"}
    missing_cols = required_cols.difference(distribution_marginal_costs.columns)
    if missing_cols:
        raise ValueError(
            "Distribution marginal costs parquet is missing required columns "
            f"{sorted(required_cols)}. Missing: {sorted(missing_cols)}"
        )
    distribution_marginal_costs = distribution_marginal_costs.set_index("timestamp")[
        "mc_total_per_kwh"
    ]
    distribution_marginal_costs.index = pd.DatetimeIndex(
        distribution_marginal_costs.index
    ).tz_localize("EST")
    distribution_marginal_costs.index.name = "time"
    distribution_marginal_costs.name = "Marginal Distribution Costs ($/kWh)"
    return distribution_marginal_costs
