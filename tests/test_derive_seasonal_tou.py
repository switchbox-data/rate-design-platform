from __future__ import annotations

import pandas as pd
import pytest

from utils.pre.derive_seasonal_tou import derive_seasonal_tou


def test_derive_seasonal_tou_rejects_flat_or_inverted_ratio() -> None:
    idx_winter = pd.date_range("2025-01-01", periods=24, freq="h", tz="UTC")
    idx_summer = pd.date_range("2025-07-01", periods=24, freq="h", tz="UTC")
    idx = pd.DatetimeIndex(list(idx_winter) + list(idx_summer))

    bulk_mc = pd.DataFrame(
        {
            "Marginal Energy Costs ($/kWh)": [0.1] * len(idx),
            "Marginal Capacity Costs ($/kWh)": [0.0] * len(idx),
        },
        index=idx,
    )
    dist_mc = pd.Series([0.0] * len(idx), index=idx, name="dist_mc")
    hourly_load = pd.Series([10.0] * len(idx), index=idx, name="load")

    with pytest.raises(ValueError, match="must be > 1.0"):
        derive_seasonal_tou(
            bulk_marginal_costs=bulk_mc,
            dist_and_sub_tx_marginal_costs=dist_mc,
            hourly_load=hourly_load,
            winter_months=[1],
            tou_window_hours=4,
            tou_base_rate=0.1,
            tou_fixed_charge=5.0,
            tou_tariff_key="test_tou",
            utility="test_utility",
        )
