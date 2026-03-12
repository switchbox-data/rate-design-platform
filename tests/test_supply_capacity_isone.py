"""Unit tests for ISO-NE FCA supply capacity marginal cost logic.

Tests cover:
- FCA price resolution for a calendar year spanning two CCPs
- Zone fallback when SENE-specific price is absent for an FCA
- Annual exceedance allocation: exactly N nonzero hours, weights sum to 1,
  cost sums to capacity_cost_kw_year
- Tie-breaking at the Nth peak hour
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl
import pytest

from utils.pre.marginal_costs.supply_capacity_isone import (
    _get_fca_price_for_ccp,
    allocate_fca_to_hours,
    resolve_fca_price_for_calendar_year,
    validate_fca_allocation,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fca_df(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal FCA DataFrame from a list of dicts.

    Each dict should have keys matching the FCA parquet schema:
        fca_number, ccp_start, ccp_end, capacity_zone_id,
        clearing_price_per_kw_month, resource_status
    """
    schema = {
        "fca_number": pl.Int64,
        "ccp_start": pl.Date,
        "ccp_end": pl.Date,
        "capacity_zone_id": pl.Int64,
        "clearing_price_per_kw_month": pl.Float64,
        "resource_status": pl.String,
    }
    return pl.DataFrame(rows, schema=schema)


def _make_annual_load(
    n_hours: int = 8760,
    year: int = 2025,
    base_mw: float = 1000.0,
    ramp: float = 0.01,
) -> pl.DataFrame:
    """Build a synthetic annual load profile with monotonically increasing load."""
    start = datetime(year, 1, 1, 0, 0, 0)
    timestamps = [start + timedelta(hours=h) for h in range(n_hours)]
    loads = [base_mw + h * ramp for h in range(n_hours)]
    return pl.DataFrame({"timestamp": timestamps, "load_mw": loads})


# ---------------------------------------------------------------------------
# _get_fca_price_for_ccp
# ---------------------------------------------------------------------------


def test_get_fca_price_primary_zone() -> None:
    """Returns price from the primary zone when present."""
    fca_df = _make_fca_df(
        [
            {
                "fca_number": 15,
                "ccp_start": date(2024, 6, 1),
                "ccp_end": date(2025, 5, 31),
                "capacity_zone_id": 8506,
                "clearing_price_per_kw_month": 3.98,
                "resource_status": "all",
            }
        ]
    )
    price, zone_used = _get_fca_price_for_ccp(
        fca_df, 2024, primary_zone_id=8506, fallback_zone_id=8500
    )
    assert price == pytest.approx(3.98)
    assert zone_used == 8506


def test_get_fca_price_fallback_zone_when_primary_absent() -> None:
    """Falls back to fallback zone when primary zone has no entry."""
    fca_df = _make_fca_df(
        [
            {
                "fca_number": 18,
                "ccp_start": date(2027, 6, 1),
                "ccp_end": date(2028, 5, 31),
                "capacity_zone_id": 8500,  # only System/RoP, not SENE
                "clearing_price_per_kw_month": 2.50,
                "resource_status": "all",
            }
        ]
    )
    price, zone_used = _get_fca_price_for_ccp(
        fca_df, 2027, primary_zone_id=8506, fallback_zone_id=8500
    )
    assert price == pytest.approx(2.50)
    assert zone_used == 8500  # fallback was used


def test_get_fca_price_raises_when_neither_zone_present() -> None:
    """Raises ValueError when neither primary nor fallback zone has an entry."""
    fca_df = _make_fca_df(
        [
            {
                "fca_number": 15,
                "ccp_start": date(2024, 6, 1),
                "ccp_end": date(2025, 5, 31),
                "capacity_zone_id": 9999,  # some other zone
                "clearing_price_per_kw_month": 1.00,
                "resource_status": "all",
            }
        ]
    )
    with pytest.raises(ValueError, match="No FCA price found"):
        _get_fca_price_for_ccp(
            fca_df, 2024, primary_zone_id=8506, fallback_zone_id=8500
        )


def test_get_fca_price_primary_takes_precedence_over_fallback() -> None:
    """Primary zone takes precedence even when fallback is also present."""
    fca_df = _make_fca_df(
        [
            {
                "fca_number": 15,
                "ccp_start": date(2024, 6, 1),
                "ccp_end": date(2025, 5, 31),
                "capacity_zone_id": 8506,
                "clearing_price_per_kw_month": 3.98,
                "resource_status": "all",
            },
            {
                "fca_number": 15,
                "ccp_start": date(2024, 6, 1),
                "ccp_end": date(2025, 5, 31),
                "capacity_zone_id": 8500,
                "clearing_price_per_kw_month": 9.99,  # would be wrong if used
                "resource_status": "all",
            },
        ]
    )
    price, zone_used = _get_fca_price_for_ccp(
        fca_df, 2024, primary_zone_id=8506, fallback_zone_id=8500
    )
    assert price == pytest.approx(3.98)
    assert zone_used == 8506


# ---------------------------------------------------------------------------
# resolve_fca_price_for_calendar_year
# ---------------------------------------------------------------------------


def test_resolve_fca_price_two_ccp_blend() -> None:
    """Calendar year cost = CCP1_price*5 + CCP2_price*7 (two overlapping CCPs)."""
    # Calendar year 2025:
    #   CCP1: Jun 2024–May 2025 (FCA 15) → covers Jan–May 2025 (5 months)
    #   CCP2: Jun 2025–May 2026 (FCA 16) → covers Jun–Dec 2025 (7 months)
    fca_df = _make_fca_df(
        [
            {
                "fca_number": 15,
                "ccp_start": date(2024, 6, 1),
                "ccp_end": date(2025, 5, 31),
                "capacity_zone_id": 8506,
                "clearing_price_per_kw_month": 3.980,
                "resource_status": "all",
            },
            {
                "fca_number": 16,
                "ccp_start": date(2025, 6, 1),
                "ccp_end": date(2026, 5, 31),
                "capacity_zone_id": 8506,
                "clearing_price_per_kw_month": 2.639,
                "resource_status": "all",
            },
        ]
    )
    result = resolve_fca_price_for_calendar_year(
        fca_df, capacity_zone_id=8506, calendar_year=2025
    )
    expected = 3.980 * 5 + 2.639 * 7  # = 19.90 + 18.473 = 38.373
    assert result == pytest.approx(expected, abs=1e-4)


def test_resolve_fca_price_uses_fallback_for_one_ccp() -> None:
    """When CCP1 has only the fallback zone, the fallback price is used for those 5 months."""
    fca_df = _make_fca_df(
        [
            # CCP1 (Jun 2024–May 2025): SENE absent → only system/RoP
            {
                "fca_number": 15,
                "ccp_start": date(2024, 6, 1),
                "ccp_end": date(2025, 5, 31),
                "capacity_zone_id": 8500,  # fallback RoP
                "clearing_price_per_kw_month": 2.00,
                "resource_status": "all",
            },
            # CCP2 (Jun 2025–May 2026): SENE present
            {
                "fca_number": 16,
                "ccp_start": date(2025, 6, 1),
                "ccp_end": date(2026, 5, 31),
                "capacity_zone_id": 8506,
                "clearing_price_per_kw_month": 3.00,
                "resource_status": "all",
            },
        ]
    )
    result = resolve_fca_price_for_calendar_year(
        fca_df, capacity_zone_id=8506, calendar_year=2025, fallback_zone_id=8500
    )
    expected = 2.00 * 5 + 3.00 * 7  # = 10.00 + 21.00 = 31.00
    assert result == pytest.approx(expected, abs=1e-4)


def test_resolve_fca_price_symmetric_months() -> None:
    """Verify the 5/7 month split is hardcoded correctly (Jan-May = 5, Jun-Dec = 7)."""
    fca_df = _make_fca_df(
        [
            {
                "fca_number": 10,
                "ccp_start": date(2022, 6, 1),
                "ccp_end": date(2023, 5, 31),
                "capacity_zone_id": 8506,
                "clearing_price_per_kw_month": 1.00,
                "resource_status": "all",
            },
            {
                "fca_number": 11,
                "ccp_start": date(2023, 6, 1),
                "ccp_end": date(2024, 5, 31),
                "capacity_zone_id": 8506,
                "clearing_price_per_kw_month": 1.00,
                "resource_status": "all",
            },
        ]
    )
    result = resolve_fca_price_for_calendar_year(
        fca_df, capacity_zone_id=8506, calendar_year=2023
    )
    # With price = 1.00/kW-month: total = 1*5 + 1*7 = 12 (= 12 months * 1)
    assert result == pytest.approx(12.0, abs=1e-6)


# ---------------------------------------------------------------------------
# allocate_fca_to_hours
# ---------------------------------------------------------------------------


def test_allocate_fca_exactly_n_nonzero_hours() -> None:
    """Allocation produces exactly n_peak_hours nonzero rows."""
    n_peak = 100
    load_df = _make_annual_load(n_hours=8760)
    result = allocate_fca_to_hours(
        load_df, capacity_cost_kw_year=38.373, n_peak_hours=n_peak
    )
    nonzero = result.filter(pl.col("capacity_cost_per_kw") > 0)
    assert nonzero.height == n_peak, (
        f"Expected {n_peak} nonzero hours, got {nonzero.height}"
    )


def test_allocate_fca_cost_sums_to_annual_total() -> None:
    """Sum of all hourly capacity_cost_per_kw equals capacity_cost_kw_year."""
    capacity_cost_kw_year = 38.373
    load_df = _make_annual_load(n_hours=8760)
    result = allocate_fca_to_hours(
        load_df, capacity_cost_kw_year=capacity_cost_kw_year, n_peak_hours=100
    )
    actual_sum = float(result["capacity_cost_per_kw"].sum())
    assert actual_sum == pytest.approx(capacity_cost_kw_year, rel=1e-6)


def test_allocate_fca_weights_sum_to_one() -> None:
    """Exceedance weights (normalized by total exceedance) sum to 1.0."""
    load_df = _make_annual_load(n_hours=8760)
    capacity_cost = 50.0
    n_peak = 10
    result = allocate_fca_to_hours(
        load_df, capacity_cost_kw_year=capacity_cost, n_peak_hours=n_peak
    )
    # Implied weight = capacity_cost_per_kw / capacity_cost_kw_year
    weights = result["capacity_cost_per_kw"] / capacity_cost
    assert float(weights.sum()) == pytest.approx(1.0, abs=1e-6)


def test_allocate_fca_output_sorted_by_timestamp() -> None:
    """Output rows are sorted by timestamp ascending."""
    load_df = _make_annual_load(n_hours=8760)
    result = allocate_fca_to_hours(load_df, capacity_cost_kw_year=10.0, n_peak_hours=50)
    timestamps = result["timestamp"].to_list()
    assert timestamps == sorted(timestamps)


def test_allocate_fca_only_peak_hours_have_cost() -> None:
    """Non-peak hours are absent from the output (they carry no capacity cost)."""
    n_peak = 5
    load_df = _make_annual_load(n_hours=8760, base_mw=1000.0, ramp=0.01)
    result = allocate_fca_to_hours(
        load_df, capacity_cost_kw_year=20.0, n_peak_hours=n_peak
    )
    # All returned rows should have positive cost (non-peak hours are excluded)
    assert (result["capacity_cost_per_kw"] > 0).all()
    assert result.height == n_peak


def test_allocate_fca_raises_when_too_few_hours() -> None:
    """Raises ValueError if load profile has fewer rows than n_peak_hours."""
    load_df = _make_annual_load(n_hours=10)
    with pytest.raises(ValueError, match="Load profile has only"):
        allocate_fca_to_hours(load_df, capacity_cost_kw_year=10.0, n_peak_hours=100)


def test_allocate_fca_selects_highest_load_hours() -> None:
    """The peak hours selected are indeed the top-N by load_mw."""
    n_peak = 5
    load_df = _make_annual_load(n_hours=8760, base_mw=1000.0, ramp=1.0)
    result = allocate_fca_to_hours(
        load_df, capacity_cost_kw_year=10.0, n_peak_hours=n_peak
    )
    peak_timestamps = set(result["timestamp"].to_list())

    # The top-N timestamps are the last n_peak hours (highest load due to ramp=1.0)
    sorted_load = load_df.sort("load_mw", descending=True)
    expected_top_timestamps = set(sorted_load.head(n_peak)["timestamp"].to_list())
    assert peak_timestamps == expected_top_timestamps


# ---------------------------------------------------------------------------
# Tie-breaking at the Nth peak hour
# ---------------------------------------------------------------------------


def _make_load_with_tie_at_nth(
    n_peak: int = 10,
    n_hours: int = 8760,
    year: int = 2025,
) -> pl.DataFrame:
    """Build a load profile where the Nth and (N+1)th highest loads are equal.

    The top (n_peak - 1) hours have strictly distinct loads (descending).
    Hours n_peak through n_peak+4 all have the same load value (tie).
    All remaining hours have strictly lower loads.
    """
    start = datetime(year, 1, 1, 0, 0, 0)
    timestamps = [start + timedelta(hours=h) for h in range(n_hours)]

    # Base monotonically increasing load so last hours are highest
    loads = [500.0 + h * 0.1 for h in range(n_hours)]

    # Create a tie: set hours at positions (n_hours - n_peak) through
    # (n_hours - n_peak + 4) to the same value as position (n_hours - n_peak)
    # so that the Nth and (N+1)th highest loads are equal.
    tie_value = loads[n_hours - n_peak]
    for i in range(n_hours - n_peak, min(n_hours - n_peak + 5, n_hours)):
        loads[i] = tie_value

    return pl.DataFrame({"timestamp": timestamps, "load_mw": loads})


def test_allocate_fca_tie_at_nth_hour_exactly_n_nonzero() -> None:
    """When Nth and (N+1)th loads tie, exactly N hours get nonzero cost."""
    n_peak = 10
    load_df = _make_load_with_tie_at_nth(n_peak=n_peak)
    result = allocate_fca_to_hours(
        load_df, capacity_cost_kw_year=30.0, n_peak_hours=n_peak
    )
    nonzero = result.filter(pl.col("capacity_cost_per_kw") > 0)
    assert nonzero.height == n_peak, (
        f"Expected exactly {n_peak} nonzero hours with tie at Nth, got {nonzero.height}"
    )


def test_allocate_fca_tie_at_nth_hour_cost_sum_correct() -> None:
    """With a tie at the Nth hour, the total allocated cost still equals capacity_cost_kw_year."""
    n_peak = 10
    capacity_cost = 25.0
    load_df = _make_load_with_tie_at_nth(n_peak=n_peak)
    result = allocate_fca_to_hours(
        load_df, capacity_cost_kw_year=capacity_cost, n_peak_hours=n_peak
    )
    actual_sum = float(result["capacity_cost_per_kw"].sum())
    assert actual_sum == pytest.approx(capacity_cost, rel=1e-6)


def test_allocate_fca_all_zero_loads_raises() -> None:
    """If all loads are zero, total exceedance is zero and allocation raises.

    When load_mw is 0 for all hours: load_nth=0, no rows are strictly below 0
    so threshold=0, and exceedance = 0 - 0 = 0 for every row → raises.
    """
    timestamps = [datetime(2025, 1, 1) + timedelta(hours=h) for h in range(8760)]
    loads = [0.0] * 8760
    load_df = pl.DataFrame({"timestamp": timestamps, "load_mw": loads})
    with pytest.raises(ValueError, match="Total exceedance is zero or negative"):
        allocate_fca_to_hours(load_df, capacity_cost_kw_year=10.0, n_peak_hours=100)


# ---------------------------------------------------------------------------
# validate_fca_allocation
# ---------------------------------------------------------------------------


def test_validate_fca_allocation_passes_for_correct_sum() -> None:
    """validate_fca_allocation passes when sum matches capacity_cost_kw_year."""
    capacity_cost = 38.373
    load_df = _make_annual_load(n_hours=8760)
    result = allocate_fca_to_hours(
        load_df, capacity_cost_kw_year=capacity_cost, n_peak_hours=100
    )
    # Should not raise
    validate_fca_allocation(result, capacity_cost)


def test_validate_fca_allocation_fails_for_wrong_sum() -> None:
    """validate_fca_allocation raises ValueError when sum is wrong."""
    # Manually corrupt the capacity cost values
    bad_df = pl.DataFrame(
        {
            "timestamp": [datetime(2025, 1, 1) + timedelta(hours=h) for h in range(5)],
            "capacity_cost_per_kw": [1.0, 2.0, 3.0, 4.0, 5.0],  # sums to 15
        }
    )
    with pytest.raises(ValueError, match="FCA validation failed"):
        validate_fca_allocation(bad_df, capacity_cost_kw_year=100.0)


# ---------------------------------------------------------------------------
# Zone fallback integration: resolve + allocate
# ---------------------------------------------------------------------------


def test_full_resolution_with_fallback_zone_gives_correct_annual_cost() -> None:
    """End-to-end: resolve with fallback zone, then verify allocation sums correctly."""
    # Both CCPs use the fallback RoP zone (SENE absent for both)
    fca_df = _make_fca_df(
        [
            {
                "fca_number": 17,
                "ccp_start": date(2026, 6, 1),
                "ccp_end": date(2027, 5, 31),
                "capacity_zone_id": 8500,  # RoP only
                "clearing_price_per_kw_month": 4.00,
                "resource_status": "all",
            },
            {
                "fca_number": 18,
                "ccp_start": date(2027, 6, 1),
                "ccp_end": date(2028, 5, 31),
                "capacity_zone_id": 8500,  # RoP only
                "clearing_price_per_kw_month": 5.00,
                "resource_status": "all",
            },
        ]
    )
    capacity_cost_kw_year = resolve_fca_price_for_calendar_year(
        fca_df, capacity_zone_id=8506, calendar_year=2027, fallback_zone_id=8500
    )
    expected = 4.00 * 5 + 5.00 * 7  # = 20.00 + 35.00 = 55.00
    assert capacity_cost_kw_year == pytest.approx(expected, abs=1e-4)

    load_df = _make_annual_load(n_hours=8760)
    result = allocate_fca_to_hours(
        load_df, capacity_cost_kw_year=capacity_cost_kw_year, n_peak_hours=100
    )
    actual_sum = float(result["capacity_cost_per_kw"].sum())
    assert actual_sum == pytest.approx(capacity_cost_kw_year, rel=1e-6)
