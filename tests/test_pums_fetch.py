"""Tests for PUMS fetch: state codes and CLI behavior."""

from __future__ import annotations


from data.census.pums.convert_pums_csv_to_parquet import PUMS_STATE_CODES


def test_pums_state_codes_has_51_entries() -> None:
    """All states + DC = 51."""
    assert len(PUMS_STATE_CODES) == 51


def test_pums_state_codes_lowercase() -> None:
    """State codes are lowercase for Census URLs."""
    for code in PUMS_STATE_CODES:
        assert code == code.lower()
        assert len(code) == 2


def test_pums_state_codes_includes_dc_and_ri() -> None:
    """Sample states used in plan."""
    assert "dc" in PUMS_STATE_CODES
    assert "ri" in PUMS_STATE_CODES
    assert "ny" in PUMS_STATE_CODES


def test_pums_state_codes_excludes_us() -> None:
    """National 'us' is not in state list (fetch uses only state-level zips)."""
    assert "us" not in PUMS_STATE_CODES
