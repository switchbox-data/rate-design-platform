"""Tests for utils.pre.rev_requirement.validate_psegli_flat_tariff.

Unit test writes tiny parquet fixtures under ``tests/`` via ``tempfile`` (avoids
pytest's basetemp under shared ``/ebs/tmp``, which can fail ownership checks).

Integration test scans a real ResStock release partition. Hourly ``total_grid_kwh``
uses ResStock ``weight`` from ``utility_assignment.parquet`` and scales to EIA
residential customers (``--eia-utility-stats`` / ``--eia-stats-year``), matching
``compute_rr`` ResStock mode. Fixed charges in diagnostics use that same customer
count.

Paths are built from ``--resstock-release-base``, ``--resstock-state``, and
``--resstock-upgrade`` (see ``tests/conftest.py``), matching AGENTS.md layout:

``{base}/load_curve_hourly/state={STATE}/upgrade={UP}/`` and
``{base}/metadata_utility/state={STATE}/utility_assignment.parquet``.

Calibrated URDB v7 delivery tariffs (``{utility}_flat_calibrated.json``,
``{utility}_default_calibrated.json``) are loaded from
``rate_design/hp_rates/{state}/config/tariffs/electric/`` — the same files CAIRO
precalc produces via ``copy-calibrated-tariff`` (not pre-DRR ``*_flat.json`` /
``*_default.json``). Aggregate annual energy charges follow CAIRO-style TOU
period assignment (``utils.cairo.assign_hourly_periods``) and monthly cumulative
kWh per ``energy_period``, with URDB tier ``max`` (kWh) applied within each
month × period block (ConEd-style volumetric tiers).

Example (integration test is opt-in — full partition scan)::

    uv run python -m pytest tests/test_validate_psegli_flat_tariff.py -v -s \\
      --run-resstock-integration \\
      --resstock-release-base=/ebs/data/nrel/resstock/res_2024_amy2018_2_sb \\
      --resstock-state=NY --resstock-upgrade=00 \\
      --resstock-electric-utilities=psegli,coned

Flat vs default **annual energy** (USD) must match within ``--flat-default-energy-rel-tol``
(default 0.10). After calibration, values are usually much closer than pre-DRR;
for a stricter bound pass e.g. ``--flat-default-energy-rel-tol=0.02``.
"""

from __future__ import annotations

import json
from collections import defaultdict
import shutil
import tempfile
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
import polars as pl
import pytest

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils import get_project_root
from utils.cairo import assign_hourly_periods
from utils.loads import (
    ELECTRIC_LOAD_COL,
    ELECTRIC_PV_COL,
    HOURLY_LOAD_TZ,
    resstock_load_curve_hourly_partition_dir,
    resstock_utility_assignment_parquet_path,
)
from utils.pre.create_tariff import create_default_flat_tariff
from utils.pre.rev_requirement.compute_rr import (
    _resolve_customer_count_with_year_fallback,
)
from utils.pre.rev_requirement.validate_psegli_flat_tariff import (
    hourly_totals_by_utility_lazy,
)

_UA_UTIL = "sb.electric_utility"


def _monthly_period_tiered_energy_charge_usd(
    month_period_kwh: float,
    tiers: list[dict[str, Any]],
) -> float:
    """Volumetric $ within one month and one ``energy_period`` (URDB tiers, kWh)."""
    rem = month_period_kwh
    cost = 0.0
    for tier in tiers:
        rate = float(tier["rate"]) + float(tier.get("adj", 0.0))
        cap = tier.get("max")
        if cap is None:
            cost += rem * rate
            break
        block = min(rem, float(cap))
        cost += block * rate
        rem -= block
        if rem <= 1e-12:
            break
    return cost


def _monthly_period_kwh_agg(
    hourly: pl.DataFrame,
    tariff: dict[str, Any],
) -> pl.DataFrame:
    """Sum kWh by (calendar month, ``energy_period``) for aggregate hourly loads."""
    if hourly.is_empty():
        return pl.DataFrame(
            schema={"month": pl.Int32, "energy_period": pl.Int32, "kwh": pl.Float64}
        )
    pdf = hourly.select(["_ts", "total_grid_kwh"]).to_pandas()
    idx = pd.DatetimeIndex(pd.to_datetime(pdf["_ts"], utc=False))
    if idx.tz is None:
        idx = idx.tz_localize(
            HOURLY_LOAD_TZ,
            ambiguous="infer",
            nonexistent="shift_forward",
        )
    else:
        idx = idx.tz_convert(HOURLY_LOAD_TZ)

    periods = assign_hourly_periods(idx, tariff)
    month_arr = pd.Series(idx).dt.month.to_numpy(dtype=np.int32)
    pldf = pl.DataFrame(
        {
            "kwh": pdf["total_grid_kwh"].astype(float),
            "month": month_arr,
            "energy_period": periods.to_numpy(dtype=np.int32),
        }
    )
    return pldf.group_by(["month", "energy_period"]).agg(pl.col("kwh").sum())


def _annual_energy_charge_aggregate_usd(
    hourly: pl.DataFrame,
    tariff: dict[str, Any],
) -> float:
    """Annual energy charges for aggregated hourly kWh (one row per hour).

    Maps each hour to ``energy_period`` via ``assign_hourly_periods``, sums kWh
    by (calendar month, period), then applies tiered ``energyratestructure`` rates.
    """
    agg = _monthly_period_kwh_agg(hourly, tariff)
    if agg.is_empty():
        return 0.0
    structure = tariff["items"][0]["energyratestructure"]
    total = 0.0
    for row in agg.iter_rows(named=True):
        tiers = structure[int(row["energy_period"])]
        total += _monthly_period_tiered_energy_charge_usd(
            float(row["kwh"]),
            tiers,
        )
    return total


def _annual_tier_kwh_by_energy_period(
    hourly: pl.DataFrame,
    tariff: dict[str, Any],
) -> list[tuple[int, int, float, float, float | None]]:
    """Roll up annual kWh per (energy_period, tier_index) after monthly tier stacking.

    For each month × period, kWh is allocated to tiers in URDB order (``max`` cap
    then next tier). Totals are summed across months.

    Returns tuples: ``(energy_period, tier_index, cumulative_kwh, rate_usd_per_kwh, tier_max_or_none)``
    """
    agg = _monthly_period_kwh_agg(hourly, tariff)
    if agg.is_empty():
        return []

    structure = tariff["items"][0]["energyratestructure"]
    kwh_by_pt: dict[tuple[int, int], float] = defaultdict(float)

    for row in agg.iter_rows(named=True):
        period = int(row["energy_period"])
        month_kwh = float(row["kwh"])
        tiers = structure[period]
        rem = month_kwh
        for tier_idx, tier in enumerate(tiers):
            rate = float(tier["rate"]) + float(tier.get("adj", 0.0))
            cap = tier.get("max")
            if cap is None:
                kwh_by_pt[(period, tier_idx)] += rem
                break
            block = min(rem, float(cap))
            kwh_by_pt[(period, tier_idx)] += block
            rem -= block
            if rem <= 1e-12:
                break

    out: list[tuple[int, int, float, float, float | None]] = []
    for (period, tier_idx), kwh_sum in sorted(kwh_by_pt.items()):
        tier_def = structure[period][tier_idx]
        rate = float(tier_def["rate"]) + float(tier_def.get("adj", 0.0))
        cap = tier_def.get("max")
        cap_f = float(cap) if cap is not None else None
        out.append((period, tier_idx, kwh_sum, rate, cap_f))
    return out


def _annual_fixed_charge_usd(
    tariff: dict[str, Any],
    n_service_accounts: int,
) -> float:
    """Total annual fixed charges = accounts × 12 × monthly fixed (URDB)."""
    per_month = float(tariff["items"][0]["fixedchargefirstmeter"])
    return float(n_service_accounts) * 12.0 * per_month


def _counts_unique_buildings_ua(ua_path: str, electric_utility: str) -> int:
    kw: dict[str, Any] = {}
    if ua_path.startswith("s3://"):
        kw["storage_options"] = get_aws_storage_options()
    lf = (
        pl.scan_parquet(ua_path, **kw)
        .filter(pl.col(_UA_UTIL) == electric_utility)
        .select(pl.col("bldg_id"))
        .unique()
    )
    return int(cast(pl.DataFrame, lf.select(pl.len().alias("n")).collect())["n"][0])


def _repo_calibrated_tariff_path(state: str, electric_utility: str, kind: str) -> Path:
    """``kind`` is ``flat`` or ``default`` (CAIRO-calibrated delivery tariffs)."""
    return (
        get_project_root()
        / "rate_design"
        / "hp_rates"
        / state.lower()
        / "config"
        / "tariffs"
        / "electric"
        / f"{electric_utility}_{kind}_calibrated.json"
    )


def test_resstock_partition_path_helpers() -> None:
    assert (
        resstock_load_curve_hourly_partition_dir(
            "/data/resstock/release_root",
            state="ny",
            upgrade="2",
        )
        == "/data/resstock/release_root/load_curve_hourly/state=NY/upgrade=02"
    )
    assert resstock_utility_assignment_parquet_path(
        "s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb",
        state="ny",
    ) == (
        "s3://data.sb/nrel/resstock/res_2024_amy2018_2_sb/"
        "metadata_utility/state=NY/utility_assignment.parquet"
    )


def test_monthly_period_tiered_energy_charge_usd() -> None:
    tiers = [
        {"rate": 0.10, "adj": 0.0, "unit": "kWh", "max": 250.0},
        {"rate": 0.20, "adj": 0.0, "unit": "kWh"},
    ]
    assert _monthly_period_tiered_energy_charge_usd(100.0, tiers) == pytest.approx(10.0)
    assert _monthly_period_tiered_energy_charge_usd(250.0, tiers) == pytest.approx(25.0)
    assert _monthly_period_tiered_energy_charge_usd(300.0, tiers) == pytest.approx(
        25.0 + 50.0 * 0.20
    )


def test_annual_energy_charge_single_period_flat_tariff() -> None:
    tariff = create_default_flat_tariff("unit_flat", 0.05, 1.0, utility="x")
    schedule_zeros = tariff["items"][0]["energyweekdayschedule"]
    assert all(v == 0 for row in schedule_zeros for v in row)
    ts = pl.datetime_range(
        pl.datetime(2018, 1, 1),
        pl.datetime(2018, 1, 3, 23),
        interval="1h",
        closed="left",
        eager=True,
    )
    hourly = pl.DataFrame({"_ts": ts, "total_grid_kwh": [2.0] * len(ts)})
    total_kwh = float(hourly["total_grid_kwh"].sum())
    assert _annual_energy_charge_aggregate_usd(hourly, tariff) == pytest.approx(
        0.05 * total_kwh
    )


def test_hourly_totals_by_utility_lazy_sums_per_timestamp() -> None:
    """Synthetic data; temp dir under ``tests/`` (current user), not pytest basetemp."""
    parent = Path(__file__).resolve().parent
    tmp = Path(tempfile.mkdtemp(prefix="tmp_validate_psegli_", dir=str(parent)))
    try:
        loads_dir = tmp / "load_curve_hourly"
        loads_dir.mkdir()

        ua_path = tmp / "utility_assignment.parquet"
        pl.DataFrame(
            {
                "bldg_id": [1, 2],
                "sb.electric_utility": ["acme", "other"],
            }
        ).write_parquet(ua_path)

        pl.DataFrame(
            {
                "bldg_id": [1, 1, 2],
                "timestamp": [
                    "2018-01-01T00:00:00",
                    "2018-01-01T01:00:00",
                    "2018-01-01T02:00:00",
                ],
                ELECTRIC_LOAD_COL: [10.0, 5.0, 999.0],
                ELECTRIC_PV_COL: [2.0, 0.0, 0.0],
            }
        ).write_parquet(loads_dir / "chunk.parquet")

        lf = hourly_totals_by_utility_lazy(
            str(loads_dir),
            str(ua_path),
            "acme",
        )
        df = cast(pl.DataFrame, lf.collect())

        assert df.height == 2
        rows = df.sort("total_grid_kwh")["total_grid_kwh"].to_list()
        assert rows == [5.0, 8.0]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_hourly_totals_resstock_partition_integration(
    request: pytest.FixtureRequest,
    electric_utility: str,
) -> None:
    if not request.config.getoption("--run-resstock-integration"):
        pytest.skip(
            "pass --run-resstock-integration to run (scans full load_curve_hourly partition)"
        )

    release_base = request.config.getoption("--resstock-release-base")
    state = request.config.getoption("--resstock-state")
    upgrade = request.config.getoption("--resstock-upgrade")

    loads_dir = resstock_load_curve_hourly_partition_dir(
        release_base,
        state=state,
        upgrade=upgrade,
    )
    ua_path = resstock_utility_assignment_parquet_path(
        release_base,
        state=state,
    )

    if not str(loads_dir).startswith("s3://"):
        if not Path(loads_dir).is_dir() or not Path(ua_path).is_file():
            pytest.skip(
                f"ResStock data not present (loads dir or UA missing): "
                f"{loads_dir!r} / {ua_path!r}"
            )

    state_upper = state.upper()
    eia_path = request.config.getoption("--eia-utility-stats")
    eia_year = int(request.config.getoption("--eia-stats-year"))
    if not eia_path:
        eia_path = (
            f"s3://data.sb/eia/861/electric_utility_stats/year={eia_year}/"
            f"state={state_upper}/data.parquet"
        )
    storage = get_aws_storage_options() if str(eia_path).startswith("s3://") else None
    customer_count, _eia_year_used = _resolve_customer_count_with_year_fallback(
        str(eia_path),
        eia_year,
        electric_utility,
        storage,
    )

    lf = hourly_totals_by_utility_lazy(
        loads_dir,
        ua_path,
        electric_utility,
        customer_count=customer_count,
    )
    df = cast(pl.DataFrame, lf.collect())

    assert df.height > 0, "expected at least one hour after join/filter"
    assert df["total_grid_kwh"].null_count() == 0

    flat_path = _repo_calibrated_tariff_path(state, electric_utility, "flat")
    default_path = _repo_calibrated_tariff_path(state, electric_utility, "default")
    if not flat_path.is_file() or not default_path.is_file():
        pytest.skip(
            f"need calibrated tariffs: {flat_path.name} and {default_path.name} "
            f"(hp_rates/{state.lower()}/config/tariffs/electric/; run CAIRO precalc "
            f"and copy-calibrated-tariff)"
        )

    flat_tariff = json.loads(flat_path.read_text(encoding="utf-8"))
    default_tariff = json.loads(default_path.read_text(encoding="utf-8"))

    n_acc = _counts_unique_buildings_ua(ua_path, electric_utility)
    assert n_acc > 0
    assert customer_count > 0

    total_kwh = float(df["total_grid_kwh"].sum())
    e_flat = _annual_energy_charge_aggregate_usd(df, flat_tariff)
    e_default = _annual_energy_charge_aggregate_usd(df, default_tariff)

    flat_rate = float(flat_tariff["items"][0]["energyratestructure"][0][0]["rate"])
    assert e_flat == pytest.approx(flat_rate * total_kwh, rel=1e-9, abs=1.0)

    assert e_flat > 0 and e_default > 0

    rel_tol = float(request.config.getoption("--flat-default-energy-rel-tol"))
    denom = max(e_flat, e_default)
    rel_diff = abs(e_flat - e_default) / denom if denom else 0.0
    assert e_default == pytest.approx(e_flat, rel=rel_tol, abs=1.0), (
        f"annual energy $ flat {e_flat:,.2f} vs default {e_default:,.2f} "
        f"(relative diff {rel_diff:.4f} > tol {rel_tol}); "
        "tighten tariff alignment or pass --flat-default-energy-rel-tol="
    )

    fixed_flat = _annual_fixed_charge_usd(flat_tariff, customer_count)
    fixed_def = _annual_fixed_charge_usd(default_tariff, customer_count)
    assert fixed_flat > 0 and fixed_def > 0

    bill_flat_total = e_flat + fixed_flat
    bill_default_total = e_default + fixed_def
    assert bill_flat_total > 0 and bill_default_total > 0

    default_tiers = _annual_tier_kwh_by_energy_period(df, default_tariff)
    print(
        f"\n=== Aggregate bill diagnostics: {electric_utility} ({state}) upgrade={upgrade} ===\n"
        f"Cumulative grid kWh (weighted, scaled to EIA residential customers={customer_count:,}): "
        f"{total_kwh:,.3f}\n"
        f"ResStock sample buildings (unique bldg_id in utility_assignment): {n_acc}\n"
        f"--- Flat tariff (calibrated, {flat_path.name}) ---\n"
        f"  Volumetric rate ($/kWh): {flat_rate:.6f}\n"
        f"  All {total_kwh:,.3f} kWh billed at that rate (single period / tier).\n"
        f"  Annual energy charge: ${e_flat:,.2f}; + fixed ${fixed_flat:,.2f} → ${bill_flat_total:,.2f}\n"
        f"--- Default tariff (calibrated, {default_path.name}): "
        f"annual kWh by energy_period × tier ---\n"
        "  (kWh stacks monthly per URDB tier max, then summed across the year)\n"
    )
    for period, tier_idx, kwh_t, rate_t, max_kwh in default_tiers:
        cap_s = f"{max_kwh:g} kWh cap" if max_kwh is not None else "unlimited"
        print(
            f"  energy_period={period} tier={tier_idx} "
            f"cumulative_kWh={kwh_t:,.3f} rate=${rate_t:.6f}/kWh tier_limit={cap_s}"
        )
    print(
        f"  Annual energy charge: ${e_default:,.2f}; + fixed ${fixed_def:,.2f} → ${bill_default_total:,.2f}\n"
    )
