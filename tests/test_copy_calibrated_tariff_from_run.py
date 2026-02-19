"""Tests for copying calibrated tariffs from RI run output directories."""

from __future__ import annotations

import json
from pathlib import Path

from utils.pre.copy_calibrated_tariff_from_run import copy_calibrated_tariff_from_run_dir


def test_copy_calibrated_tariff_from_run_dir(tmp_path: Path) -> None:
    run_dir = tmp_path / "ri_rie_run_01_precalc_rie_a16_up00_y2025"
    run_dir.mkdir(parents=True)
    payload = {
        "items": [
            {
                "label": "rie_a16_calibrated",
                "name": "rie_a16_calibrated",
            }
        ]
    }
    (run_dir / "tariff_final_config.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    destination_dir = tmp_path / "rate_design" / "ri" / "hp_rates" / "config" / "tariffs" / "electric"
    output_path = copy_calibrated_tariff_from_run_dir(
        run_dir,
        destination_dir=destination_dir,
    )

    assert output_path == destination_dir / "rie_a16_calibrated.json"
    assert json.loads(output_path.read_text(encoding="utf-8")) == payload

