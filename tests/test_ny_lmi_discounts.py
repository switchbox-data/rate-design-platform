"""Unit tests for NY EAP/EEAP discount application logic.

Tests cover:
  - _build_smi_threshold_column (occupant→SMI threshold mapping)
  - _apply_discounts_to_bills (credit subtraction, rider, edge cases)
  - load_cpi_ratio (shared CPI inflation helper)
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import polars as pl
import pytest

from utils.post.apply_ny_lmi_discounts_to_bills import (
    _apply_discounts_to_bills,
    _build_smi_threshold_column,
)
from utils.post.lmi_common import load_cpi_ratio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SMI_THRESHOLDS = {
    1: 36_000.0,
    2: 41_000.0,
    3: 46_000.0,
    4: 51_000.0,
    5: 56_000.0,
    6: 61_000.0,
    7: 66_000.0,
    8: 70_000.0,
}


def _make_bills_csv(path: Path, bldg_ids: list[int], bill: float) -> None:
    """Write a minimal bill CSV with Jan, Feb, and Annual rows per building."""
    rows: list[dict[str, object]] = []
    for bid in bldg_ids:
        for m in ("Jan", "Feb", "Annual"):
            rows.append(
                {
                    "bldg_id": bid,
                    "weight": 1.0,
                    "month": m,
                    "bill_level": bill if m != "Annual" else bill * 12,
                    "dollar_year": 2024,
                }
            )
    pl.DataFrame(rows).write_csv(path)


def _make_tier_consumption(
    bldg_ids: list[int],
    tiers: list[int],
    participates: list[bool],
    heats_elec: list[bool],
    heats_gas: list[bool],
    gas_utilities: list[str],
    elec_kwh: list[float] | None = None,
    gas_therms: list[float] | None = None,
) -> pl.LazyFrame:
    n = len(bldg_ids)
    return pl.DataFrame(
        {
            "bldg_id": bldg_ids,
            "lmi_tier": tiers,
            "lmi_tier_raw": tiers,
            "participates": participates,
            "heats_with_electricity": heats_elec,
            "heats_with_natgas": heats_gas,
            "gas_utility": gas_utilities,
            "elec_kwh": elec_kwh or [10_000.0] * n,
            "gas_therms": gas_therms or [500.0] * n,
        }
    ).lazy()


# ---------------------------------------------------------------------------
# 1. _build_smi_threshold_column
# ---------------------------------------------------------------------------


def test_build_smi_threshold_column_basic() -> None:
    """Occupants 1-8 each map to the correct SMI threshold."""
    df = pl.DataFrame({"occ": list(range(1, 9))})
    out = df.with_columns(
        _build_smi_threshold_column("occ", SMI_THRESHOLDS).alias("thresh")
    )
    for hh_size in range(1, 9):
        assert out.filter(pl.col("occ") == hh_size)["thresh"][0] == SMI_THRESHOLDS[hh_size]


def test_build_smi_threshold_column_over_8() -> None:
    """Occupants > 8 fall back to the 8-person threshold (HUD max)."""
    df = pl.DataFrame({"occ": [9, 10]})
    out = df.with_columns(
        _build_smi_threshold_column("occ", SMI_THRESHOLDS).alias("thresh")
    )
    assert out["thresh"].to_list() == [70_000.0, 70_000.0]


# ---------------------------------------------------------------------------
# 2. _apply_discounts_to_bills
# ---------------------------------------------------------------------------


def test_apply_discounts_elec_heat(tmp_path: Path) -> None:
    """Con Ed Tier 3 electric-heating customer: $126.21/month subtracted."""
    run_dir = tmp_path
    bills_dir = run_dir / "bills"
    bills_dir.mkdir()

    _make_bills_csv(bills_dir / "elec_bills_year_run.csv", [1], 200.0)
    _make_bills_csv(bills_dir / "gas_bills_year_run.csv", [1], 100.0)

    tc = _make_tier_consumption(
        [1], [3], [True], [True], [False], ["kedny"],
    )
    elec, _gas = _apply_discounts_to_bills(run_dir, tc, "coned", rider=False, opts={})
    elec_df = cast(pl.DataFrame, elec.collect())

    jan = elec_df.filter(pl.col("month") == "Jan")
    assert jan["bill_level"][0] == pytest.approx(200.0 - 126.21)

    annual = elec_df.filter(pl.col("month") == "Annual")
    assert annual["bill_level"][0] == pytest.approx(200.0 * 12 - 126.21 * 12)


def test_apply_discounts_nonheat(tmp_path: Path) -> None:
    """Con Ed Tier 3 non-heating customer: $73.47/month (lower credit)."""
    run_dir = tmp_path
    bills_dir = run_dir / "bills"
    bills_dir.mkdir()

    _make_bills_csv(bills_dir / "elec_bills_year_run.csv", [1], 200.0)
    _make_bills_csv(bills_dir / "gas_bills_year_run.csv", [1], 100.0)

    tc = _make_tier_consumption(
        [1], [3], [True], [False], [False], ["kedny"],
    )
    elec, _gas = _apply_discounts_to_bills(run_dir, tc, "coned", rider=False, opts={})
    elec_df = cast(pl.DataFrame, elec.collect())

    jan = elec_df.filter(pl.col("month") == "Jan")
    assert jan["bill_level"][0] == pytest.approx(200.0 - 73.47)


def test_apply_discounts_gas_by_gas_utility(tmp_path: Path) -> None:
    """Gas credits use the gas utility (kedny), not the electric utility (coned)."""
    run_dir = tmp_path
    bills_dir = run_dir / "bills"
    bills_dir.mkdir()

    _make_bills_csv(bills_dir / "elec_bills_year_run.csv", [1], 200.0)
    _make_bills_csv(bills_dir / "gas_bills_year_run.csv", [1], 100.0)

    # Building heats with gas, served by KEDNY for gas
    tc = _make_tier_consumption(
        [1], [3], [True], [False], [True], ["kedny"],
    )
    _elec, gas = _apply_discounts_to_bills(run_dir, tc, "coned", rider=False, opts={})
    gas_df = cast(pl.DataFrame, gas.collect())

    # KEDNY Tier 3 gas_heat = $138.67
    jan = gas_df.filter(pl.col("month") == "Jan")
    assert jan["bill_level"][0] == pytest.approx(100.0 - 138.67)


def test_apply_discounts_null_credit(tmp_path: Path) -> None:
    """Unpublished EEAP tier (NiMo Tier 5): null → $0 credit applied."""
    run_dir = tmp_path
    bills_dir = run_dir / "bills"
    bills_dir.mkdir()

    _make_bills_csv(bills_dir / "elec_bills_year_run.csv", [1], 200.0)
    _make_bills_csv(bills_dir / "gas_bills_year_run.csv", [1], 100.0)

    tc = _make_tier_consumption(
        [1], [5], [True], [True], [True], ["nimo"],
    )
    elec, gas = _apply_discounts_to_bills(run_dir, tc, "nimo", rider=False, opts={})
    elec_df = cast(pl.DataFrame, elec.collect())
    gas_df = cast(pl.DataFrame, gas.collect())

    # Null credits → $0 subtracted, bills unchanged
    jan_elec = elec_df.filter(pl.col("month") == "Jan")
    assert jan_elec["bill_level"][0] == pytest.approx(200.0)

    jan_gas = gas_df.filter(pl.col("month") == "Jan")
    assert jan_gas["bill_level"][0] == pytest.approx(100.0)


def test_apply_discounts_psegli_flat(tmp_path: Path) -> None:
    """PSEG LI: flat $45/month for all tiers regardless of heating status."""
    run_dir = tmp_path
    bills_dir = run_dir / "bills"
    bills_dir.mkdir()

    _make_bills_csv(bills_dir / "elec_bills_year_run.csv", [1, 2], 200.0)
    _make_bills_csv(bills_dir / "gas_bills_year_run.csv", [1, 2], 100.0)

    # Bldg 1: Tier 1 heating; Bldg 2: Tier 3 non-heating. Both should get $45.
    tc = _make_tier_consumption(
        [1, 2], [1, 3], [True, True], [True, False], [False, False],
        ["psegli", "psegli"],
    )
    elec, _gas = _apply_discounts_to_bills(run_dir, tc, "psegli", rider=False, opts={})
    elec_df = cast(pl.DataFrame, elec.collect())

    for bid in [1, 2]:
        jan = elec_df.filter((pl.col("bldg_id") == bid) & (pl.col("month") == "Jan"))
        assert jan["bill_level"][0] == pytest.approx(200.0 - 45.0)


def test_apply_discounts_tier_zero_no_credit(tmp_path: Path) -> None:
    """Ineligible customers (tier 0) get no credit subtracted."""
    run_dir = tmp_path
    bills_dir = run_dir / "bills"
    bills_dir.mkdir()

    _make_bills_csv(bills_dir / "elec_bills_year_run.csv", [1], 200.0)
    _make_bills_csv(bills_dir / "gas_bills_year_run.csv", [1], 100.0)

    tc = _make_tier_consumption(
        [1], [0], [False], [True], [True], ["kedny"],
    )
    elec, gas = _apply_discounts_to_bills(run_dir, tc, "coned", rider=False, opts={})
    elec_df = cast(pl.DataFrame, elec.collect())
    gas_df = cast(pl.DataFrame, gas.collect())

    jan_elec = elec_df.filter(pl.col("month") == "Jan")
    assert jan_elec["bill_level"][0] == pytest.approx(200.0)

    jan_gas = gas_df.filter(pl.col("month") == "Jan")
    assert jan_gas["bill_level"][0] == pytest.approx(100.0)


def test_apply_discounts_rider(tmp_path: Path) -> None:
    """Rider recovery from non-participants equals total discount (electric)."""
    run_dir = tmp_path
    bills_dir = run_dir / "bills"
    bills_dir.mkdir()

    # Two buildings: bldg 1 participates (Tier 3), bldg 2 does not (Tier 0)
    _make_bills_csv(bills_dir / "elec_bills_year_run.csv", [1, 2], 200.0)
    _make_bills_csv(bills_dir / "gas_bills_year_run.csv", [1, 2], 100.0)

    tc = _make_tier_consumption(
        [1, 2],
        [3, 0],
        [True, False],
        [True, True],
        [False, False],
        ["kedny", "kedny"],
        elec_kwh=[10_000.0, 10_000.0],
        gas_therms=[500.0, 500.0],
    )
    elec, _gas = _apply_discounts_to_bills(run_dir, tc, "coned", rider=True, opts={})
    elec_df = cast(pl.DataFrame, elec.collect())

    # On Annual rows: participant's discount should equal non-participant's rider surcharge
    annual = elec_df.filter(pl.col("month") == "Annual")
    original_annual = 200.0 * 12

    participant = annual.filter(pl.col("bldg_id") == 1)
    non_participant = annual.filter(pl.col("bldg_id") == 2)

    # Participant gets credit subtracted; non-participant gets rider added
    participant_discount = original_annual - float(participant["bill_level"][0])
    non_participant_surcharge = float(non_participant["bill_level"][0]) - original_annual

    # Total discount should equal total rider recovery
    assert participant_discount == pytest.approx(non_participant_surcharge, rel=1e-6)


# ---------------------------------------------------------------------------
# 3. load_cpi_ratio
# ---------------------------------------------------------------------------


def test_load_cpi_ratio_basic(tmp_path: Path) -> None:
    """CPI ratio = cpi_target / cpi_base."""
    cpi_path = str(tmp_path / "cpi.parquet")
    pl.DataFrame(
        {"year": [2019, 2024], "value": [255.0, 310.0]}
    ).write_parquet(cpi_path)

    ratio = load_cpi_ratio(cpi_path, 2024, {})
    assert ratio == pytest.approx(310.0 / 255.0)


def test_load_cpi_ratio_missing_year(tmp_path: Path) -> None:
    """Raises ValueError when the requested inflation_year is absent."""
    cpi_path = str(tmp_path / "cpi.parquet")
    pl.DataFrame({"year": [2019], "value": [255.0]}).write_parquet(cpi_path)

    with pytest.raises(ValueError, match="2024"):
        load_cpi_ratio(cpi_path, 2024, {})
