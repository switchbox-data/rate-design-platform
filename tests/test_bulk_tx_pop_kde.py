"""ISO-NE bulk TX: PTF spread by the KDE probability of peak (pop_kde)."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import polars as pl
import pytest

from utils.data_prep.marginal_costs.bulk_tx_isone import (
    AESC_2024_DOLLAR_YEAR,
    BULK_TX_METHODS,
    DEFAULT_POP_KDE_HISTORY_YEARS,
    compute_isone_bulk_tx_pop_kde,
    parse_history_years,
    validate_allocation,
)
from utils.data_prep.marginal_costs.mc_manifest import build_key
from utils.data_prep.marginal_costs.probability_of_peak import (
    WEEKDAY,
    label_hour_types,
)


def _history(years: tuple[int, ...], *, seed: int = 0) -> pl.DataFrame:
    """Summer-peaking hourly load, like the NE system, over several years."""
    rng = np.random.default_rng(seed)
    stamps: list[datetime] = []
    for year in years:
        cur = datetime(year, 1, 1)
        while cur.year == year:
            stamps.append(cur)
            cur += timedelta(hours=1)
    frame = label_hour_types(pl.DataFrame({"timestamp": stamps}))
    month = frame["month"].to_numpy().astype(int)
    hour = frame["hour"].to_numpy().astype(float)
    weekday = (frame["daytype"] == WEEKDAY).to_numpy()
    summer = np.isin(month, (6, 7, 8)).astype(float) * (1.0 + 0.5 * (month == 7))
    afternoon = np.exp(-((hour - 17) ** 2) / 8.0)
    base = 13_000.0 + 8_000.0 * summer * afternoon * (1.0 + 0.3 * weekday)
    load = base * rng.lognormal(0.0, 0.06, size=base.size)
    return frame.select("timestamp").with_columns(pl.Series("load_mw", load))


def test_constants() -> None:
    assert BULK_TX_METHODS == ("exceedance_top_n", "pop_kde")
    assert DEFAULT_POP_KDE_HISTORY_YEARS == (2022, 2023, 2024, 2025)
    assert AESC_2024_DOLLAR_YEAR == 2024


def test_parse_history_years() -> None:
    assert parse_history_years("2022-2025") == (2022, 2023, 2024, 2025)
    assert parse_history_years("2025,2023, 2024") == (2023, 2024, 2025)
    assert parse_history_years("2025") == (2025,)
    with pytest.raises(ValueError, match="end before start"):
        parse_history_years("2025-2022")
    with pytest.raises(ValueError, match="at least one"):
        parse_history_years(",")


def test_pop_kde_recovers_the_annual_cost_on_an_8760() -> None:
    hourly, pop = compute_isone_bulk_tx_pop_kde(
        _history((2022, 2023)), 70.82, 2025, calendar_year=2025
    )
    assert hourly.columns == ["timestamp", "bulk_tx_cost_enduse"]
    assert hourly.height == 8760
    assert hourly["timestamp"].dt.year().unique().to_list() == [2025]
    assert float(hourly["bulk_tx_cost_enduse"].sum()) == pytest.approx(70.82)
    assert hourly["bulk_tx_cost_enduse"].null_count() == 0
    validate_allocation(hourly, 70.82)

    # The signal sits on summer afternoons and is flat across like days.
    top = hourly.sort("bulk_tx_cost_enduse", descending=True).head(1)["timestamp"]
    assert top.item().month == 7
    assert 15 <= top.item().hour <= 19
    winter = hourly.filter(pl.col("timestamp").dt.month().is_in([12, 1, 2]))
    assert float(winter["bulk_tx_cost_enduse"].sum()) == pytest.approx(0.0, abs=1e-6)

    assert pop.cells.height == 576
    assert pop.years["year"].to_list() == [2022, 2023]
    assert float(pop.cells["probability_of_peak"].sum()) == pytest.approx(1.0)


def test_pop_kde_year_stamps_output_and_calendar_sets_day_types() -> None:
    hourly, _ = compute_isone_bulk_tx_pop_kde(
        _history((2022,)), 69.0, 2026, calendar_year=2025
    )
    assert hourly["timestamp"].dt.year().unique().to_list() == [2026]
    assert float(hourly["bulk_tx_cost_enduse"].sum()) == pytest.approx(69.0)


def test_pop_kde_rejects_a_partial_year() -> None:
    with pytest.raises(ValueError, match="complete"):
        compute_isone_bulk_tx_pop_kde(_history((2022,)).head(6000), 69.0, 2025)


def test_manifest_key_for_pop_kde() -> None:
    params = {
        "ptf_kw_year": 69.0,
        "allocation_load": "ne_system",
        "history_years": [2022, 2023, 2024, 2025],
        "calendar_year": 2025,
        "target_dollar_year": 2025,
    }
    assert (
        build_key("bulk_tx", "isone_aesc_ptf_pop_kde", params)
        == "aesc_ptf69_pop_kde_ne_system_hist2022to2025_cal2025_2025usd"
    )
    params["allocation_load"] = "utility_zone"
    params["history_years"] = [2021, 2022, 2023, 2024, 2025]
    assert (
        build_key("bulk_tx", "isone_aesc_ptf_pop_kde", params)
        == "aesc_ptf69_pop_kde_utility_zone_hist2021to2025_cal2025_2025usd"
    )
