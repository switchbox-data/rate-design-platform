"""Tests for shared TOU marginal-cost helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from utils.pre.compute_tou import compute_mc_seasonal_ratio


def test_compute_mc_seasonal_ratio_uses_load_weighted_seasonal_averages() -> None:
    idx = pd.date_range("2025-01-01", periods=8760, freq="h", tz="UTC")
    winter_months = [1]
    is_winter = idx.month == 1
    is_heavy_load_hour = idx.hour < 12

    mc = pd.Series(
        np.select(
            [is_winter & is_heavy_load_hour, is_winter, is_heavy_load_hour],
            [0.30, 0.10, 0.05],
            default=0.15,
        ),
        index=idx,
        name="total_mc_per_kwh",
    )
    load = pd.Series(
        np.where(is_heavy_load_hour, 3.0, 1.0),
        index=idx,
        name="load",
    )

    ratio = compute_mc_seasonal_ratio(mc, load, winter_months=winter_months)

    # Winter: (0.30 * 3 + 0.10 * 1) / 4 = 0.25.
    # Summer: (0.05 * 3 + 0.15 * 1) / 4 = 0.075.
    assert ratio == pytest.approx(0.25 / 0.075)
