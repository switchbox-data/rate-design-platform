from __future__ import annotations

import json
from pathlib import Path

import pytest

from utils.pre.create_flat_tariff import main
from utils.pre.create_tariff import create_default_flat_tariff


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_legacy_mode_generates_single_flat_tariff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = tmp_path / "legacy_flat.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "create_flat_tariff.py",
            "--label",
            "legacy_flat",
            "--volumetric-rate",
            "0.2",
            "--fixed-charge",
            "5.0",
            "--output-path",
            str(out),
        ],
    )
    main()

    payload = json.loads(out.read_text(encoding="utf-8"))
    item = payload["items"][0]
    assert item["label"] == "legacy_flat"
    assert item["name"] == "legacy_flat"
    assert item["energyratestructure"][0][0]["rate"] == 0.2
    assert item["energyratestructure"][0][0]["adj"] == 0.0
    assert item["fixedchargefirstmeter"] == 5.0


def test_template_mode_generates_flat_and_flat_supply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    default_src = tmp_path / "rie_a16.json"
    supply_src = tmp_path / "rie_a16_supply.json"
    default_payload = create_default_flat_tariff(
        label="rie_a16",
        volumetric_rate=0.06455,
        fixed_charge=6.75,
        adjustment=0.0,
        utility="Rhode Island Energy",
    )
    supply_payload = create_default_flat_tariff(
        label="rie_a16_supply",
        volumetric_rate=0.06455,
        fixed_charge=6.75,
        adjustment=0.1477,
        utility="Rhode Island Energy",
    )
    _write_json(default_src, default_payload)
    _write_json(supply_src, supply_payload)

    monkeypatch.setattr(
        "sys.argv",
        [
            "create_flat_tariff.py",
            "--default-tariff-json",
            str(default_src),
            "--supply-default-tariff-json",
            str(supply_src),
            "--flat-label",
            "rie_flat",
            "--flat-supply-label",
            "rie_flat_supply",
            "--output-dir",
            str(tmp_path),
        ],
    )
    main()

    flat_payload = json.loads((tmp_path / "rie_flat.json").read_text(encoding="utf-8"))
    flat_supply_payload = json.loads(
        (tmp_path / "rie_flat_supply.json").read_text(encoding="utf-8")
    )
    flat_item = flat_payload["items"][0]
    flat_supply_item = flat_supply_payload["items"][0]
    assert flat_item["label"] == "rie_flat"
    assert flat_item["name"] == "rie_flat"
    assert flat_item["energyratestructure"][0][0]["rate"] == 0.06455
    assert flat_item["energyratestructure"][0][0]["adj"] == 0.0
    assert flat_supply_item["label"] == "rie_flat_supply"
    assert flat_supply_item["name"] == "rie_flat_supply"
    assert flat_supply_item["energyratestructure"][0][0]["rate"] == 0.06455
    assert flat_supply_item["energyratestructure"][0][0]["adj"] == 0.1477


def test_template_mode_auto_resolves_supply_suffix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    default_src = tmp_path / "foo.json"
    supply_src = tmp_path / "foo_supply.json"
    _write_json(
        default_src,
        create_default_flat_tariff(
            label="foo",
            volumetric_rate=0.1,
            fixed_charge=2.0,
            adjustment=0.0,
            utility="U",
        ),
    )
    _write_json(
        supply_src,
        create_default_flat_tariff(
            label="foo_supply",
            volumetric_rate=0.1,
            fixed_charge=2.0,
            adjustment=0.2,
            utility="U",
        ),
    )

    monkeypatch.setattr(
        "sys.argv",
        [
            "create_flat_tariff.py",
            "--default-tariff-json",
            str(default_src),
            "--flat-label",
            "flat",
            "--flat-supply-label",
            "flat_supply",
            "--output-dir",
            str(tmp_path),
        ],
    )
    main()

    assert (tmp_path / "flat.json").exists()
    assert (tmp_path / "flat_supply.json").exists()


def test_rejects_mixed_legacy_and_template_args(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    default_src = tmp_path / "foo.json"
    _write_json(
        default_src,
        create_default_flat_tariff(
            label="foo",
            volumetric_rate=0.1,
            fixed_charge=2.0,
            adjustment=0.0,
            utility="U",
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "create_flat_tariff.py",
            "--label",
            "legacy",
            "--volumetric-rate",
            "0.2",
            "--fixed-charge",
            "5.0",
            "--output-path",
            str(tmp_path / "legacy.json"),
            "--default-tariff-json",
            str(default_src),
            "--flat-label",
            "flat",
            "--flat-supply-label",
            "flat_supply",
            "--output-dir",
            str(tmp_path),
        ],
    )
    with pytest.raises(SystemExit):
        main()


def test_template_mode_errors_for_malformed_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    default_src = tmp_path / "bad.json"
    supply_src = tmp_path / "bad_supply.json"
    _write_json(default_src, {"items": []})
    _write_json(supply_src, {"items": [{"label": "x"}]})

    monkeypatch.setattr(
        "sys.argv",
        [
            "create_flat_tariff.py",
            "--default-tariff-json",
            str(default_src),
            "--supply-default-tariff-json",
            str(supply_src),
            "--flat-label",
            "flat",
            "--flat-supply-label",
            "flat_supply",
            "--output-dir",
            str(tmp_path),
        ],
    )
    with pytest.raises(ValueError):
        main()
