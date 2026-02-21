"""Tests for copy_calibrated_tariff_from_run utility."""

import json
from pathlib import Path

import pytest

from utils.midrun.copy_calibrated_tariff_from_run import (
    copy_calibrated_tariff_from_run_dir,
    _tariff_keys_and_payloads,
)


def _minimal_cairo_tariff_dict() -> dict:
    """Minimal CAIRO tariff dict (schedules 12x24, one TOU row)."""
    schedule = [[1] * 24 for _ in range(12)]
    return {
        "ur_ec_sched_weekday": schedule,
        "ur_ec_sched_weekend": schedule,
        "ur_ec_tou_mat": [[1, 1, 1e38, 0, 0.21, 0.0, 0]],
    }


def test_tariff_keys_and_payloads_single_key() -> None:
    """Single tariff key returns one (key, payload) pair."""
    payload = {"rie_a16": _minimal_cairo_tariff_dict()}
    result = _tariff_keys_and_payloads(payload)
    assert len(result) == 1
    assert result[0][0] == "rie_a16"
    assert result[0][1] == {"rie_a16": payload["rie_a16"]}


def test_tariff_keys_and_payloads_multiple_keys() -> None:
    """Multiple tariff keys return one pair per key."""
    payload = {
        "rie_hp_seasonal": _minimal_cairo_tariff_dict(),
        "rie_flat": _minimal_cairo_tariff_dict(),
    }
    result = _tariff_keys_and_payloads(payload)
    assert len(result) == 2
    keys = [r[0] for r in result]
    assert set(keys) == {"rie_hp_seasonal", "rie_flat"}


def test_tariff_keys_and_payloads_empty_raises() -> None:
    """Empty payload raises ValueError."""
    with pytest.raises(ValueError, match="tariff_final_config.json is empty"):
        _tariff_keys_and_payloads({})


def test_tariff_keys_and_payloads_no_dict_values_raises() -> None:
    """Payload with no dict values raises ValueError."""
    with pytest.raises(ValueError, match="No tariff keys found"):
        _tariff_keys_and_payloads({"a": 1, "b": "x"})


def test_tariff_keys_and_payloads_urdb_shape_uses_label() -> None:
    """Already URDB shape (has 'items') returns one entry using first item label."""
    payload = {
        "items": [
            {"label": "my_tariff", "energyratestructure": [[{"rate": 0.1}]]},
        ]
    }
    result = _tariff_keys_and_payloads(payload)
    assert len(result) == 1
    assert result[0][0] == "my_tariff"
    assert result[0][1] is payload


def test_copy_calibrated_tariff_from_run_dir_single_key(
    tmp_path: Path,
) -> None:
    """Single tariff in JSON produces one _calibrated.json file."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    payload = {"rie_a16": _minimal_cairo_tariff_dict()}
    (run_dir / "tariff_final_config.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    written = copy_calibrated_tariff_from_run_dir(
        run_dir,
        state="RI",
        destination_dir=out_dir,
    )

    assert len(written) == 1
    assert written[0] == out_dir / "rie_a16_calibrated.json"
    assert written[0].exists()
    content = json.loads(written[0].read_text(encoding="utf-8"))
    assert "items" in content
    assert len(content["items"]) == 1
    assert content["items"][0]["label"] == "rie_a16"


def test_copy_calibrated_tariff_from_run_dir_multiple_keys(
    tmp_path: Path,
) -> None:
    """Multiple tariffs in JSON produce one _calibrated.json per key."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    payload = {
        "rie_hp_seasonal": _minimal_cairo_tariff_dict(),
        "rie_flat": _minimal_cairo_tariff_dict(),
    }
    (run_dir / "tariff_final_config.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    written = copy_calibrated_tariff_from_run_dir(
        run_dir,
        state="RI",
        destination_dir=out_dir,
    )

    assert len(written) == 2
    assert set(written) == {
        out_dir / "rie_hp_seasonal_calibrated.json",
        out_dir / "rie_flat_calibrated.json",
    }
    for path in written:
        assert path.exists()
        content = json.loads(path.read_text(encoding="utf-8"))
        assert "items" in content and len(content["items"]) == 1
