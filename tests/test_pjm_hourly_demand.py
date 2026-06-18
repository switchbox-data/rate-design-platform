"""Unit tests for the PJM hourly zonal load pipeline.

Covers the Data Miner client's date chunking / filter formatting, the
zone→utility aggregation, and the validator's completeness logic — none of which
touch the network or S3.
"""

import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.pjm import dataminer
from data.pjm.hourly_demand.aggregate_pjm_utility_loads import (
    aggregate_utility_load,
    get_utility_zone_mapping,
)
from data.pjm.hourly_demand.fetch_pjm_zone_loads import md_zone_codes
from data.pjm.hourly_demand.validate_pjm_demand_parquet import (
    expected_hours_in_year,
    validate_zone_loads,
)
from data.pjm.zone_mapping.generate_zone_mapping_csv import build_zone_mapping

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


# ── Data Miner client ─────────────────────────────────────────────────────────


def test_ept_date_filter_format():
    """Filter string uses unpadded M-D-YYYY with 00:00..23:59 bounds."""
    assert (
        dataminer.ept_date_filter(date(2025, 1, 5), date(2025, 3, 9))
        == "1-5-2025 00:00 to 3-9-2025 23:59"
    )


def test_split_date_range_standard_single_chunk():
    """A short recent range stays a single chunk."""
    start, end = date(2025, 1, 1), date(2025, 1, 31)
    chunks = dataminer.split_date_range(start, end)
    assert chunks == [(start, end)]


def test_split_date_range_standard_respects_max_days(monkeypatch):
    """A >365-day standard range is split into <=365-day chunks."""
    # Pin the cutoff far back so the whole range is in the standard regime.
    monkeypatch.setattr(dataminer, "archive_cutoff", lambda: date(2020, 1, 1))
    start, end = date(2024, 1, 1), date(2025, 6, 1)
    chunks = dataminer.split_date_range(start, end)
    assert len(chunks) > 1
    for cs, ce in chunks:
        assert (ce - cs).days < dataminer.MAX_RANGE_DAYS
    # Chunks are contiguous and cover the whole range.
    assert chunks[0][0] == start
    assert chunks[-1][1] == end


def test_split_date_range_archive_within_calendar_year(monkeypatch):
    """Archive chunks never cross a calendar-year boundary."""
    # Pin the archive cutoff so 2019-2021 is entirely archive.
    monkeypatch.setattr(dataminer, "archive_cutoff", lambda: date(2024, 1, 1))
    chunks = dataminer.split_date_range(date(2019, 6, 1), date(2021, 3, 1))
    for cs, ce in chunks:
        assert cs.year == ce.year, f"chunk {cs}..{ce} crosses a year boundary"
    assert chunks[0][0] == date(2019, 6, 1)
    assert chunks[-1][1] == date(2021, 3, 1)


def test_split_date_range_spans_boundary(monkeypatch):
    """A range straddling the cutoff is split into archive + standard segments."""
    monkeypatch.setattr(dataminer, "archive_cutoff", lambda: date(2024, 6, 18))
    chunks = dataminer.split_date_range(date(2024, 1, 1), date(2024, 12, 31))
    # No chunk may straddle the cutoff.
    for cs, ce in chunks:
        assert not (cs < date(2024, 6, 18) <= ce)
    assert chunks[0][0] == date(2024, 1, 1)
    assert chunks[-1][1] == date(2024, 12, 31)


def test_parse_ts_column_iso():
    df = pl.DataFrame({"datetime_beginning_utc": ["2025-01-01T05:00:00"]})
    out = dataminer.parse_ts_column(df, "datetime_beginning_utc")
    assert out.schema["datetime_beginning_utc"] == pl.Datetime
    assert out["datetime_beginning_utc"][0] == datetime(2025, 1, 1, 5, 0, 0)


# ── Zone / utility mapping ──────────────────────────────────────────────────


def test_md_zone_codes():
    assert md_zone_codes() == ["AP", "BC", "DPL", "PEP"]


def test_get_utility_zone_mapping_single_zone_per_md_utility():
    mapping = build_zone_mapping()
    util_map = get_utility_zone_mapping(mapping)
    assert util_map["bge"] == ["BC"]
    assert util_map["pepco"] == ["PEP"]
    assert util_map["dpl"] == ["DPL"]
    assert util_map["potomac-edison"] == ["AP"]


def test_aggregate_utility_load_sums_zones_by_timestamp():
    ts = pl.datetime_range(
        datetime(2025, 1, 1), datetime(2025, 1, 1, 2), interval="1h", eager=True
    )
    zone_df = pl.concat(
        [
            pl.DataFrame(
                {"timestamp": ts, "zone": "PEP", "load_mw": [10.0, 20.0, 30.0]}
            ),
            pl.DataFrame({"timestamp": ts, "zone": "BC", "load_mw": [1.0, 2.0, 3.0]}),
        ]
    )
    # Utility mapped to a single zone keeps that zone's series.
    bge = aggregate_utility_load(zone_df, "bge", ["BC"])
    assert bge["load_mw"].to_list() == [1.0, 2.0, 3.0]
    assert bge["utility"].unique().to_list() == ["bge"]

    # A hypothetical multi-zone utility sums across its zones per timestamp.
    multi = aggregate_utility_load(zone_df, "multi", ["BC", "PEP"])
    assert multi["load_mw"].to_list() == [11.0, 22.0, 33.0]


# ── Validator ────────────────────────────────────────────────────────────────


def test_expected_hours_in_year():
    assert expected_hours_in_year(2025) == 8760  # non-leap
    assert expected_hours_in_year(2024) == 8784  # leap


def _full_year_zone_df(year: int, zone: str, load: float = 5000.0) -> pl.DataFrame:
    """Build a complete tz-aware Eastern year for one zone (DST-correct hours)."""
    start = datetime(year, 1, 1, tzinfo=ET).astimezone(UTC)
    end = datetime(year + 1, 1, 1, tzinfo=ET).astimezone(UTC)
    utc_hours = pl.datetime_range(
        start, end, interval="1h", closed="left", eager=True, time_zone="UTC"
    )
    df = pl.DataFrame({"timestamp": utc_hours}).with_columns(
        pl.col("timestamp").dt.convert_time_zone("America/New_York")
    )
    df = df.filter(pl.col("timestamp").dt.year() == year)
    return df.with_columns(zone=pl.lit(zone), load_mw=pl.lit(load))


def test_validate_zone_loads_complete_year_passes():
    df = _full_year_zone_df(2025, "BC", load=6100.0)
    ok, msgs = validate_zone_loads(df, ["BC"], 2025)
    assert ok, msgs


def test_validate_zone_loads_wrong_count_fails():
    df = _full_year_zone_df(2025, "BC").head(8000)
    ok, msgs = validate_zone_loads(df, ["BC"], 2025)
    assert not ok
    assert any("expected 8760" in m for m in msgs)


def test_validate_zone_loads_negative_fails():
    df = _full_year_zone_df(2025, "BC")
    df = df.with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(-1.0)
        .otherwise(pl.col("load_mw"))
        .alias("load_mw")
    )
    ok, msgs = validate_zone_loads(df, ["BC"], 2025)
    assert not ok
    assert any("negative" in m for m in msgs)


def test_validate_zone_loads_missing_zone_fails():
    df = _full_year_zone_df(2025, "BC")
    ok, msgs = validate_zone_loads(df, ["BC", "PEP"], 2025)
    assert not ok
    assert any("missing" in m for m in msgs)
