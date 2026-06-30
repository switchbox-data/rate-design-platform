"""Tests for sub-TX/DX load-layout handling in generate_utility_tx_dx_mc.

All states (NY, MD, RI) now use ISO-native layouts:
    utility=X/year=YYYY[/month=MM]/data.parquet

The EIA legacy layout (region=<iso>/utility=X/...) is no longer supported.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.data_prep.marginal_costs.generate_utility_tx_dx_mc import (
    load_utility_load_profile,
)


def _write_partition(path: Path, n: int = 48) -> None:
    """Write a small (timestamp, load_mw) parquet at an exact partition dir."""
    path.mkdir(parents=True, exist_ok=True)
    start = datetime(2025, 1, 1)
    ts = [start + timedelta(hours=i) for i in range(n)]
    df = pl.DataFrame({"timestamp": ts, "load_mw": [100.0 + i for i in range(n)]})
    df.write_parquet(path / "data.parquet")


def test_iso_native_layout_loads(tmp_path):
    """ISO-native layout (utility/year/month) loads correctly."""
    base = tmp_path / "isone"
    _write_partition(base / "utility=rie" / "year=2025" / "month=01")

    df = load_utility_load_profile(
        s3_base=str(base),
        year_load=2025,
        utility="rie",
        storage_options={},
    )
    assert df.height > 0
    assert df["load_mw"].null_count() == 0


def test_missing_profile_raises(tmp_path):
    """A missing utility/year partition raises FileNotFoundError."""
    base = tmp_path / "isone"
    _write_partition(base / "utility=rie" / "year=2025" / "month=01")

    with pytest.raises(FileNotFoundError):
        load_utility_load_profile(
            s3_base=str(base),
            year_load=2099,
            utility="rie",
            storage_options={},
        )
