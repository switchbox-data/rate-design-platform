"""Tests for the Eversource billed-kWh vs Connecticut zone-load comparison."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from data.isone.hourly_demand.compare_ct_zone_load_to_eversource_sales import (
    compare_monthly,
    load_filing_monthly_kwh,
    monthly_zone_kwh,
    parse_kwh,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
FILING_CSV = REPO_ROOT / "data/isone/hourly_demand/ct_eversource_ciec020_kwh_ty2025.csv"


def test_parse_kwh_strips_commas_spaces_and_parentheses() -> None:
    assert parse_kwh("  1,932,841,046 ") == 1_932_841_046
    assert parse_kwh("(28,934)") == -28_934
    assert parse_kwh("18673895") == 18_673_895


def test_parse_kwh_rejects_blank() -> None:
    with pytest.raises(ValueError, match="Cannot parse"):
        parse_kwh("  ")


def test_filing_total_row_matches_sheet() -> None:
    monthly = load_filing_monthly_kwh(FILING_CSV)
    assert monthly[1] == 1_932_841_046
    assert monthly[6] == 1_544_438_227
    assert monthly[12] == 1_811_064_329
    assert sum(monthly.values()) == 20_277_352_393


def test_monthly_zone_kwh_sums_megawatts_to_kwh(tmp_path: Path) -> None:
    part = tmp_path / "utility=ct_eversource" / "year=2025" / "month=01"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "timestamp": [
                datetime(2025, 1, 1, 0, tzinfo=ZoneInfo("America/New_York")),
                datetime(2025, 1, 1, 1, tzinfo=ZoneInfo("America/New_York")),
            ],
            "load_mw": [1.5, 2.5],
        }
    ).write_parquet(part / "data.parquet")
    for month in range(2, 13):
        other = tmp_path / "utility=ct_eversource" / "year=2025" / f"month={month:02d}"
        other.mkdir()
        pl.DataFrame(
            {
                "timestamp": [
                    datetime(2025, month, 1, 0, tzinfo=ZoneInfo("America/New_York"))
                ],
                "load_mw": [1.0],
            }
        ).write_parquet(other / "data.parquet")

    zone = monthly_zone_kwh(tmp_path, "ct_eversource", 2025)
    january = zone.filter(pl.col("month") == 1)
    assert january["hours"].item() == 2
    assert january["zone_kwh"].item() == 4_000.0


def test_compare_monthly_ratio() -> None:
    filing = {month: 100 for month in range(1, 13)}
    zone = pl.DataFrame(
        {
            "month": list(range(1, 13)),
            "hours": [1] * 12,
            "zone_kwh": [133.0] * 12,
        }
    )
    compared = compare_monthly(filing, zone)
    assert compared["zone_over_filing"].to_list() == pytest.approx([1.33] * 12)
