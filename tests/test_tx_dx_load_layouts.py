"""Tests for sub-TX/DX load-layout handling and MD state config.

Covers the EIA-retirement-aware behavior of generate_utility_tx_dx_mc:
- EIA load layout carries a ``region=`` partition and must be filtered by it.
- ISO-native layouts (NYISO, PJM, future ISO-NE utilities) have no ``region=``
  partition and must skip the filter (iso_region=None).
- MD resolves via get_state_config for the PJM-native PoP workflow.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.eia.hourly_loads.eia_region_config import get_state_config
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


def test_get_state_config_md_resolves_pjm():
    """MD resolves with PJM iso_region; lookup is case-insensitive."""
    cfg = get_state_config("MD")
    assert cfg.state == "MD"
    assert cfg.iso_region == "pjm"
    assert get_state_config("md").iso_region == "pjm"


def test_iso_native_layout_no_region_filter(tmp_path):
    """ISO-native layout (utility/year/month, no region) loads with iso_region=None."""
    base = tmp_path / "pjm"
    _write_partition(base / "utility=bge" / "year=2025" / "month=01")

    df = load_utility_load_profile(
        s3_base=str(base),
        iso_region=None,
        year_load=2025,
        utility="bge",
        storage_options={},
    )
    assert df.height > 0
    assert df["load_mw"].null_count() == 0


def test_eia_layout_region_filter_applied(tmp_path):
    """EIA layout carries region=; filtering by iso_region selects one region."""
    base = tmp_path / "eia"
    _write_partition(base / "region=isone" / "utility=rie" / "year=2025" / "month=01")
    _write_partition(base / "region=nyiso" / "utility=rie" / "year=2025" / "month=01")

    df = load_utility_load_profile(
        s3_base=str(base),
        iso_region="isone",
        year_load=2025,
        utility="rie",
        storage_options={},
    )
    assert df.height > 0
    assert set(df["region"].unique().to_list()) == {"isone"}


def test_missing_profile_raises(tmp_path):
    """A missing utility/year partition raises FileNotFoundError."""
    base = tmp_path / "pjm"
    _write_partition(base / "utility=bge" / "year=2025" / "month=01")

    with pytest.raises(FileNotFoundError):
        load_utility_load_profile(
            s3_base=str(base),
            iso_region=None,
            year_load=2099,
            utility="bge",
            storage_options={},
        )


if __name__ == "__main__":
    test_get_state_config_md_resolves_pjm()
    print("✓ test_get_state_config_md_resolves_pjm passed")
