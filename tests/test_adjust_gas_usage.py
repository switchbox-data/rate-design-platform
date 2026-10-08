"""Tests for data/resstock/load_curve/adjust_gas_usage.py."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from data.resstock.load_curve.adjust_gas_usage import (
    ANNUAL_GAS_TOTAL_COL,
    HOURLY_GAS_COLS,
    KWH_PER_MMBTU,
    adjust_gas_usage_hourly_dir,
    load_eia176_residential_kwh,
    resstock_weighted_gas_kwh,
    scale_gas_hourly,
)
from data.resstock.validations import validate_gas_usage_inputs

ELEC_COL = "out.electricity.total.energy_consumption"


def _hourly_frame() -> pl.DataFrame:
    data: dict[str, list[float]] = {c: [1.0, 2.0] for c in HOURLY_GAS_COLS}
    data[ELEC_COL] = [5.0, 6.0]
    return pl.DataFrame(data)


def test_scale_gas_hourly_scales_all_gas_columns_only() -> None:
    out = scale_gas_hourly(_hourly_frame().lazy(), 1.5).collect()
    for col in HOURLY_GAS_COLS:
        assert out[col].to_list() == [1.5, 3.0]
    assert out[ELEC_COL].to_list() == [5.0, 6.0]


def test_hourly_gas_cols_cover_consumption_and_intensity() -> None:
    assert len(HOURLY_GAS_COLS) == 22
    assert "out.natural_gas.total.energy_consumption" in HOURLY_GAS_COLS
    assert "out.natural_gas.heating.energy_consumption_intensity" in HOURLY_GAS_COLS


def test_scale_gas_hourly_raises_on_missing_gas_column() -> None:
    frame = _hourly_frame().drop("out.natural_gas.grill.energy_consumption")
    with pytest.raises(ValueError, match="grill"):
        scale_gas_hourly(frame.lazy(), 1.1)


def test_resstock_weighted_gas_kwh_treats_null_as_zero() -> None:
    annual = pl.LazyFrame(
        {
            "bldg_id": [1, 2, 3],
            ANNUAL_GAS_TOTAL_COL: [100.0, None, 50.0],
            "weight": [2.0, 3.0, 4.0],
        }
    )
    assert resstock_weighted_gas_kwh(annual) == pytest.approx(400.0)


def test_resstock_weighted_gas_kwh_requires_weight() -> None:
    annual = pl.LazyFrame({"bldg_id": [1], ANNUAL_GAS_TOTAL_COL: [1.0]})
    with pytest.raises(ValueError, match="weight"):
        resstock_weighted_gas_kwh(annual)


def _write_eia(tmp_path: Path) -> tuple[str, str]:
    by_consumer = tmp_path / "by_consumer.parquet"
    disposition = tmp_path / "disposition.parquet"
    pl.DataFrame(
        {
            "report_year": [2018, 2018, 2018, 2018, 2019, 2018],
            "operator_id_eia": ["A", "A", "B", "B", "A", "C"],
            "operating_state": ["CT", "CT", "CT", "CT", "CT", "NY"],
            "customer_class": [
                "residential",
                "residential",
                "residential",
                "commercial",
                "residential",
                "residential",
            ],
            "revenue_class": ["sales", "transport", "sales", "sales", "sales", "sales"],
            "volume_mcf": [100.0, 10.0, 200.0, 999.0, 999.0, 999.0],
        }
    ).write_parquet(by_consumer)
    pl.DataFrame(
        {
            "report_year": [2018, 2018, 2018],
            "operator_id_eia": ["A", "B", "C"],
            "operating_state": ["CT", "CT", "NY"],
            "delivered_gas_heat_content_mmbtu_per_mcf": [1.0, 1.05, 1.0],
        }
    ).write_parquet(disposition)
    return str(by_consumer), str(disposition)


def test_eia176_residential_includes_transport_and_excludes_other_rows(
    tmp_path: Path,
) -> None:
    by_consumer, disposition = _write_eia(tmp_path)
    kwh = load_eia176_residential_kwh(
        "CT", 2018, path_by_consumer=by_consumer, path_disposition=disposition
    )
    assert kwh == pytest.approx(((100.0 + 10.0) * 1.0 + 200.0 * 1.05) * KWH_PER_MMBTU)


def test_eia176_residential_raises_when_no_rows(tmp_path: Path) -> None:
    by_consumer, disposition = _write_eia(tmp_path)
    with pytest.raises(ValueError, match="no residential deliveries"):
        load_eia176_residential_kwh(
            "RI", 2018, path_by_consumer=by_consumer, path_disposition=disposition
        )


def _validate(tmp_path: Path, upgrade_ids: list[str], file_types: list[str]) -> None:
    validate_gas_usage_inputs(
        states=["CT"],
        upgrade_ids=upgrade_ids,
        file_types=file_types,
        adjust_gas_usage=True,
        path_raw=tmp_path,
    )


def test_validate_gas_usage_passes_when_run_fetches_u00_annual(
    tmp_path: Path,
) -> None:
    _validate(tmp_path, ["0", "2"], ["load_curve_hourly", "load_curve_annual"])


@pytest.mark.parametrize(
    ("upgrade_ids", "file_types"),
    [
        (["2"], ["load_curve_hourly", "load_curve_annual"]),
        (["0", "2"], ["load_curve_hourly"]),
    ],
)
def test_validate_gas_usage_raises_when_u00_annual_missing(
    tmp_path: Path, upgrade_ids: list[str], file_types: list[str]
) -> None:
    with pytest.raises(RuntimeError, match="upgrade-00 load_curve_annual"):
        _validate(tmp_path, upgrade_ids, file_types)


def test_validate_gas_usage_passes_when_u00_annual_on_disk(tmp_path: Path) -> None:
    lca_dir = tmp_path / "load_curve_annual" / "state=CT" / "upgrade=00"
    lca_dir.mkdir(parents=True)
    pl.DataFrame({"bldg_id": [1]}).write_parquet(lca_dir / "annual.parquet")
    _validate(tmp_path, ["2"], ["load_curve_hourly"])


def test_validate_gas_usage_skips_when_disabled_or_no_hourly(tmp_path: Path) -> None:
    validate_gas_usage_inputs(
        states=["CT"],
        upgrade_ids=["2"],
        file_types=["load_curve_hourly"],
        adjust_gas_usage=False,
        path_raw=tmp_path,
    )
    _validate(tmp_path, ["2"], ["metadata"])


def test_adjust_gas_usage_hourly_dir_rewrites_every_file(tmp_path: Path) -> None:
    for bldg_id in (1, 2):
        _hourly_frame().write_parquet(tmp_path / f"{bldg_id}-0.parquet")
    n = adjust_gas_usage_hourly_dir(tmp_path, 2.0, max_workers=2)
    assert n == 2
    for bldg_id in (1, 2):
        out = pl.read_parquet(tmp_path / f"{bldg_id}-0.parquet")
        assert out["out.natural_gas.total.energy_consumption"].to_list() == [2.0, 4.0]
        assert out[ELEC_COL].to_list() == [5.0, 6.0]
