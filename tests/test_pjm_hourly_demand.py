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
    interpolate_flagged_zone_hours,
)
from data.pjm.hourly_demand.fetch_pjm_zone_loads import (
    md_zone_codes,
    sum_load_areas_to_zone,
)
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
    assert util_map["poted"] == ["AP"]


def _four_hours():
    return pl.datetime_range(
        datetime(2025, 6, 1, 0),
        datetime(2025, 6, 1, 3),
        interval="1h",
        time_zone="America/New_York",
        eager=True,
    )


def test_sum_load_areas_flags_bad_value_but_keeps_raw():
    """Zone = raw sum of load areas; a load-area spike-down (DPLCO=0 between
    ~2,000s) sets value_flag=True without altering the raw sum."""
    ts = _four_hours()
    dplco = pl.DataFrame(
        {
            "timestamp": ts,
            "zone": "DPL",
            "load_area": "DPLCO",
            "mw": [2032.0, 0.0, 2064.0, 2114.0],
        }
    )
    # EASTON is a genuinely tiny load area (~28-30 MW) — never flagged.
    easton = pl.DataFrame(
        {
            "timestamp": ts,
            "zone": "DPL",
            "load_area": "EASTON",
            "mw": [28.0, 29.58, 29.5, 30.0],
        }
    )
    out = sum_load_areas_to_zone(pl.concat([dplco, easton]), 2025).sort("timestamp")
    # Raw sums preserved (the flagged hour stays 29.58, NOT interpolated here).
    assert out["load_mw"].round(2).to_list() == [2060.0, 29.58, 2093.5, 2144.0]
    assert out["value_flag"].to_list() == [False, True, False, False]


def test_sum_load_areas_flags_spike_up():
    """A garbage HIGH single-hour reading is flagged symmetrically (>4x neighbors)."""
    ts = _four_hours()
    bc = pl.DataFrame(
        {
            "timestamp": ts,
            "zone": "BC",
            "load_area": "BC",
            "mw": [3000.0, 20000.0, 3200.0, 3100.0],
        }
    )
    out = sum_load_areas_to_zone(bc, 2025).sort("timestamp")
    assert out["value_flag"].to_list() == [False, True, False, False]
    # Raw value is preserved at the zone level (interpolation happens later).
    assert out["load_mw"][1] == 20000.0


def test_sum_load_areas_good_neighbor_of_bad_point_not_flagged():
    """A good hour adjacent to a bad one is NOT falsely flagged: the down-test
    uses min(neighbors) and the up-test uses max(neighbors), so the bad neighbor
    is neutralized in the direction that matters."""
    ts = pl.datetime_range(
        datetime(2025, 6, 1, 0),
        datetime(2025, 6, 1, 4),
        interval="1h",
        time_zone="America/New_York",
        eager=True,
    )
    # Bad-low at the middle (index 2); indices 1 and 3 are good and sit directly
    # beside it (and have valid prev/next, so they're genuinely tested).
    bc = pl.DataFrame(
        {
            "timestamp": ts,
            "zone": "BC",
            "load_area": "BC",
            "mw": [3000.0, 3000.0, 50.0, 3000.0, 3000.0],
        }
    )
    out = sum_load_areas_to_zone(bc, 2025).sort("timestamp")
    assert out["value_flag"].to_list() == [False, False, True, False, False]


def test_sum_load_areas_single_area_zone_never_flagged():
    """A single-load-area zone (e.g. BC) with normal diurnal values isn't flagged."""
    ts = _four_hours()
    bc = pl.DataFrame(
        {
            "timestamp": ts,
            "zone": "BC",
            "load_area": "BC",
            "mw": [3000.0, 2800.0, 3200.0, 4000.0],
        }
    )
    out = sum_load_areas_to_zone(bc, 2025).sort("timestamp")
    assert out["load_mw"].to_list() == [3000.0, 2800.0, 3200.0, 4000.0]
    assert out["value_flag"].to_list() == [False, False, False, False]


def test_interpolate_flagged_zone_hours():
    """The utility step nulls flagged hours and linearly interpolates per zone."""
    ts = _four_hours()
    zone_df = pl.DataFrame(
        {
            "timestamp": ts,
            "zone": "DPL",
            "load_mw": [2060.0, 29.58, 2093.5, 2144.0],
            "value_flag": [False, True, False, False],
        }
    )
    out = interpolate_flagged_zone_hours(zone_df).sort("timestamp")
    # Flagged hour is replaced by the mean of its neighbours; others untouched.
    assert out["load_mw"][1] == (2060.0 + 2093.5) / 2
    assert out["load_mw"].null_count() == 0
    assert out["load_mw"][0] == 2060.0
    assert out["load_mw"][3] == 2144.0


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
    # No value_flag column on input -> interpolated is all False.
    assert bge["interpolated"].to_list() == [False, False, False]

    # A hypothetical multi-zone utility sums across its zones per timestamp.
    multi = aggregate_utility_load(zone_df, "multi", ["BC", "PEP"])
    assert multi["load_mw"].to_list() == [11.0, 22.0, 33.0]


def test_aggregate_utility_load_propagates_interpolated_flag():
    """A utility-hour is marked interpolated when any constituent zone-hour was."""
    ts = pl.datetime_range(
        datetime(2025, 1, 1), datetime(2025, 1, 1, 2), interval="1h", eager=True
    )
    zone_df = pl.concat(
        [
            pl.DataFrame(
                {
                    "timestamp": ts,
                    "zone": "PEP",
                    "load_mw": [10.0, 20.0, 30.0],
                    "value_flag": [False, True, False],
                }
            ),
            pl.DataFrame(
                {
                    "timestamp": ts,
                    "zone": "BC",
                    "load_mw": [1.0, 2.0, 3.0],
                    "value_flag": [False, False, False],
                }
            ),
        ]
    )
    # Single-zone utility inherits its zone's flag.
    pep = aggregate_utility_load(zone_df, "pepco", ["PEP"]).sort("timestamp")
    assert pep["interpolated"].to_list() == [False, True, False]
    # Multi-zone utility is interpolated if ANY constituent zone-hour was.
    multi = aggregate_utility_load(zone_df, "multi", ["BC", "PEP"]).sort("timestamp")
    assert multi["interpolated"].to_list() == [False, True, False]


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
