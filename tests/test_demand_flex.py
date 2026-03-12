from __future__ import annotations

import logging

import pandas as pd
import pytest

from utils.demand_flex import recompute_tou_precalc_mapping
from utils.pre.compute_tou import Season, SeasonTouSpec


def _make_mc_index() -> pd.DatetimeIndex:
    winter = pd.date_range("2025-01-01", periods=24, freq="h", tz="UTC")
    summer = pd.date_range("2025-07-01", periods=24, freq="h", tz="UTC")
    return pd.DatetimeIndex(list(winter) + list(summer))


def _make_bulk_mc(
    index: pd.DatetimeIndex,
    *,
    winter_mc: float,
    summer_mc: float,
) -> pd.DataFrame:
    energy = [winter_mc if ts.month == 1 else summer_mc for ts in index]
    return pd.DataFrame(
        {
            "Marginal Energy Costs ($/kWh)": energy,
            "Marginal Capacity Costs ($/kWh)": 0.0,
        },
        index=index,
    )


def _make_precalc_mapping(*tariffs: str) -> pd.DataFrame:
    rows = []
    for tariff in tariffs:
        for period in range(1, 5):
            rows.append({"tariff": tariff, "period": period, "rel_value": 1.0})
    return pd.DataFrame(rows)


def _make_specs() -> list[SeasonTouSpec]:
    return [
        SeasonTouSpec(
            season=Season(name="winter", months=[1]),
            base_rate=0.1,
            peak_hours=[17, 18],
            peak_offpeak_ratio=2.0,
        ),
        SeasonTouSpec(
            season=Season(name="summer", months=[7]),
            base_rate=0.2,
            peak_hours=[16, 17],
            peak_offpeak_ratio=2.5,
        ),
    ]


def test_recompute_tou_precalc_mapping_warns_for_multiple_tou_tariffs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="rates_analysis.demand_flex")
    index = _make_mc_index()

    recompute_tou_precalc_mapping(
        precalc_mapping=_make_precalc_mapping("tou_a", "tou_b"),
        shifted_load_raw=pd.Series(10.0, index=index, name="load"),
        bulk_marginal_costs=_make_bulk_mc(index, winter_mc=0.1, summer_mc=0.2),
        dist_and_sub_tx_marginal_costs=pd.Series(0.0, index=index, name="dist_mc"),
        tou_season_specs={"tou_a": _make_specs(), "tou_b": _make_specs()},
    )

    assert "only one aggregated shifted TOU load curve" in caplog.text


def test_recompute_tou_precalc_mapping_warns_and_flattens_zero_mc_season(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="rates_analysis.demand_flex")
    index = _make_mc_index()

    updated = recompute_tou_precalc_mapping(
        precalc_mapping=_make_precalc_mapping("tou_a"),
        shifted_load_raw=pd.Series(10.0, index=index, name="load"),
        bulk_marginal_costs=_make_bulk_mc(index, winter_mc=0.0, summer_mc=0.2),
        dist_and_sub_tx_marginal_costs=pd.Series(0.0, index=index, name="dist_mc"),
        tou_season_specs={"tou_a": _make_specs()},
    )

    assert "defaulting to flat ratio 1.0 and flat seasonal base rate" in caplog.text
    winter_rel_values = updated.loc[
        (updated["tariff"] == "tou_a") & (updated["period"].isin([1, 2])),
        "rel_value",
    ].tolist()
    assert len(set(winter_rel_values)) == 1
