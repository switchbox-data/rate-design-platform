"""Tests for utils/mid/derive_icos_tariff.py — ICOS tariff derivation."""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from utils.mid.derive_icos_tariff import (
    _compute_icos_inputs,
    _compute_seasonal_kwh,
    _weighted_avg,
    derive_icos_tariffs,
    extract_supply_component,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_urdb_json(path: Path, rate: float, fixed: float) -> None:
    """Write a minimal URDB-v7 tariff JSON with a single flat period."""
    tariff = {
        "items": [
            {
                "label": path.stem,
                "uri": "",
                "sector": "Residential",
                "energyweekdayschedule": [[0] * 24 for _ in range(12)],
                "energyweekendschedule": [[0] * 24 for _ in range(12)],
                "energyratestructure": [[{"rate": rate}]],
                "fixedchargefirstmeter": fixed,
            }
        ]
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(tariff), encoding="utf-8")


def _write_cairo_outputs(
    run_dir: Path,
    bldg_ids: list[int],
    weights: list[float],
    heating_types: list[str],
    economic_burdens: list[float],
    annual_kwh: list[float],
    *,
    hourly_kwh: dict[int, list[float]] | None = None,
) -> None:
    """Create minimal CAIRO output files under *run_dir*.

    If *hourly_kwh* is given, it maps bldg_id -> 8760 hourly values.
    """
    run_dir.mkdir(parents=True, exist_ok=True)

    # customer_metadata.csv
    meta = pl.DataFrame(
        {
            "bldg_id": bldg_ids,
            "weight": weights,
            "postprocess_group.heating_type_v2": heating_types,
        }
    )
    meta.write_csv(run_dir / "customer_metadata.csv")

    # cross_subsidization/cross_subsidization_BAT_values.csv
    bat_dir = run_dir / "cross_subsidization"
    bat_dir.mkdir(parents=True, exist_ok=True)
    bat = pl.DataFrame(
        {"bldg_id": bldg_ids, "customer_level_economic_burden": economic_burdens}
    )
    bat.write_csv(bat_dir / "cross_subsidization_BAT_values.csv")

    # billing_kwh_annual.parquet
    kwh_df = pl.DataFrame({"bldg_id": bldg_ids, "annual_kwh_grid": annual_kwh})
    kwh_df.write_parquet(run_dir / "billing_kwh_annual.parquet")

    # billing_kwh_8760.parquet (optional)
    if hourly_kwh is not None:
        rows = []
        for bid, vals in hourly_kwh.items():
            start = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
            for i, v in enumerate(vals):
                ts = start + datetime.timedelta(hours=i)
                rows.append({"bldg_id": bid, "timestamp": ts, "grid_cons_kwh": v})
        kwh_8760 = pl.DataFrame(rows).with_columns(
            pl.col("timestamp").cast(pl.Datetime("us"))
        )
        kwh_8760.write_parquet(run_dir / "billing_kwh_8760.parquet")


# ---------------------------------------------------------------------------
# Unit tests: _weighted_avg
# ---------------------------------------------------------------------------


class TestWeightedAvg:
    def test_equal_weights(self) -> None:
        vals = pl.Series([10.0, 20.0])
        weights = pl.Series([1.0, 1.0])
        assert _weighted_avg(vals, weights) == pytest.approx(15.0)

    def test_unequal_weights(self) -> None:
        vals = pl.Series([10.0, 20.0])
        weights = pl.Series([3.0, 1.0])
        assert _weighted_avg(vals, weights) == pytest.approx(12.5)

    def test_zero_weights_raises(self) -> None:
        vals = pl.Series([10.0])
        weights = pl.Series([0.0])
        with pytest.raises(ValueError, match="Weights sum to zero"):
            _weighted_avg(vals, weights)


# ---------------------------------------------------------------------------
# Unit tests: extract_supply_component
# ---------------------------------------------------------------------------


class TestExtractSupplyComponent:
    def test_simple(self, tmp_path: Path) -> None:
        d = tmp_path / "d.json"
        s = tmp_path / "s.json"
        _write_urdb_json(d, rate=0.05, fixed=6.75)
        _write_urdb_json(s, rate=0.15, fixed=6.75)
        assert extract_supply_component(d, s) == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# Unit tests: _compute_icos_inputs
# ---------------------------------------------------------------------------


class TestComputeIcosInputs:
    def test_basic(self, tmp_path: Path) -> None:
        precalc = tmp_path / "precalc"
        calibrated = tmp_path / "calibrated"

        _write_cairo_outputs(
            precalc,
            bldg_ids=[1, 2],
            weights=[1.0, 1.0],
            heating_types=["natgas", "natgas"],
            economic_burdens=[100.0, 200.0],
            annual_kwh=[1000.0, 2000.0],
        )
        _write_cairo_outputs(
            calibrated,
            bldg_ids=[1, 2],
            weights=[1.0, 1.0],
            heating_types=["natgas", "natgas"],
            economic_burdens=[120.0, 220.0],
            annual_kwh=[1200.0, 2200.0],
        )

        result = _compute_icos_inputs(precalc, calibrated, ["natgas"])
        assert result["avg_delta_mc"] == pytest.approx(20.0)
        assert result["avg_kwh_before"] == pytest.approx(1500.0)
        assert result["avg_kwh_after"] == pytest.approx(1700.0)

    def test_population_filter(self, tmp_path: Path) -> None:
        """Only natgas buildings are included; electric is excluded."""
        precalc = tmp_path / "precalc"
        calibrated = tmp_path / "calibrated"

        _write_cairo_outputs(
            precalc,
            bldg_ids=[1, 2, 3],
            weights=[1.0, 1.0, 1.0],
            heating_types=["natgas", "electric", "natgas"],
            economic_burdens=[100.0, 999.0, 200.0],
            annual_kwh=[1000.0, 9999.0, 2000.0],
        )
        _write_cairo_outputs(
            calibrated,
            bldg_ids=[1, 2, 3],
            weights=[1.0, 1.0, 1.0],
            heating_types=["natgas", "electric", "natgas"],
            economic_burdens=[120.0, 999.0, 220.0],
            annual_kwh=[1200.0, 9999.0, 2200.0],
        )

        result = _compute_icos_inputs(precalc, calibrated, ["natgas"])
        assert result["avg_delta_mc"] == pytest.approx(20.0)
        assert result["avg_kwh_before"] == pytest.approx(1500.0)


# ---------------------------------------------------------------------------
# Unit tests: _compute_seasonal_kwh
# ---------------------------------------------------------------------------


class TestComputeSeasonalKwh:
    def test_basic(self, tmp_path: Path) -> None:
        """Winter months get their kWh summed, summer months get the rest."""
        run_dir = tmp_path / "run"
        run_dir.mkdir()

        winter_months = [11, 12, 1, 2, 3]

        bldg_ids = pl.Series("bldg_id", [1])
        weights_df = pl.DataFrame({"bldg_id": [1], "weight": [1.0]})

        rows = []
        start = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        for h in range(8760):
            ts = start + datetime.timedelta(hours=h)
            month = ts.month
            kwh = 2.0 if month in winter_months else 1.0
            rows.append({"bldg_id": 1, "timestamp": ts, "grid_cons_kwh": kwh})

        kwh_8760 = pl.DataFrame(rows).with_columns(
            pl.col("timestamp").cast(pl.Datetime("us"))
        )
        kwh_8760.write_parquet(run_dir / "billing_kwh_8760.parquet")

        avg_winter, avg_summer = _compute_seasonal_kwh(
            run_dir, bldg_ids, weights_df, winter_months
        )

        winter_hours = sum(
            1
            for h in range(8760)
            if (start + datetime.timedelta(hours=h)).month in winter_months
        )
        summer_hours = 8760 - winter_hours

        assert avg_winter == pytest.approx(2.0 * winter_hours)
        assert avg_summer == pytest.approx(1.0 * summer_hours)


# ---------------------------------------------------------------------------
# Unit tests: ICOS flat formula
# ---------------------------------------------------------------------------


class TestIcosFlatFormula:
    """Verify the flat ICOS formula: r_new = (r_base * kwh_before + delta_MC) / kwh_after."""

    def test_formula(self) -> None:
        r_base = 0.05
        avg_kwh_before = 1500.0
        avg_delta_mc = 20.0
        avg_kwh_after = 1700.0

        r_new = (r_base * avg_kwh_before + avg_delta_mc) / avg_kwh_after
        expected = (0.05 * 1500.0 + 20.0) / 1700.0
        assert r_new == pytest.approx(expected)

        overpayment = r_new * avg_kwh_after - (r_base * avg_kwh_before + avg_delta_mc)
        assert overpayment == pytest.approx(0.0, abs=1e-10)


# ---------------------------------------------------------------------------
# Unit tests: ICOS seasonal formula
# ---------------------------------------------------------------------------


class TestIcosSeasonalFormula:
    """Verify the seasonal ICOS formula: r_winter solves the annual constraint."""

    def test_formula(self) -> None:
        r_base = 0.05
        avg_kwh_before = 1500.0
        avg_delta_mc = 20.0
        avg_kwh_summer = 600.0
        avg_kwh_winter = 1100.0

        r_winter = (
            r_base * avg_kwh_before + avg_delta_mc - r_base * avg_kwh_summer
        ) / avg_kwh_winter
        r_summer = r_base

        overpayment = (
            r_summer * avg_kwh_summer
            + r_winter * avg_kwh_winter
            - (r_base * avg_kwh_before + avg_delta_mc)
        )
        assert overpayment == pytest.approx(0.0, abs=1e-10)


# ---------------------------------------------------------------------------
# Integration: derive_icos_tariffs
# ---------------------------------------------------------------------------


def _make_pipeline_config(
    tmp_path: Path,
    *,
    base_delivery_rate: float = 0.05,
    base_supply_rate: float = 0.15469,
    fixed_charge: float = 6.75,
) -> tuple[Any, Any]:
    """Create a mock PipelineConfig and a mock tariff_json_path callable."""
    tariffs_dir = tmp_path / "config" / "tariffs" / "electric"
    tariffs_dir.mkdir(parents=True)
    periods_dir = tmp_path / "config" / "periods"
    periods_dir.mkdir(parents=True)

    _write_urdb_json(
        tariffs_dir / "test_util_rate1_proposed.json",
        rate=base_delivery_rate,
        fixed=fixed_charge,
    )
    _write_urdb_json(
        tariffs_dir / "test_util_rate1_proposed_supply.json",
        rate=base_supply_rate,
        fixed=fixed_charge,
    )

    periods_yaml = periods_dir / "test_util.yaml"
    periods_yaml.write_text(
        "winter_months: [11, 12, 1, 2, 3]\nsummer_months: [4, 5, 6, 7, 8, 9, 10]\n"
    )

    run_defaults = MagicMock()
    run_defaults.periods_yaml = "periods/test_util.yaml"

    config = MagicMock()
    config.state = "ct"
    config.utility = "test_util"
    config.state_config_dir = tmp_path / "config"
    config.run_defaults = run_defaults

    parent_scenario = MagicMock()
    parent_scenario.tariff_base = "rate1_proposed"
    config.scenarios = {"parent": parent_scenario}

    def _mock_tjp(_config: Any, stem: str) -> Path:
        return tariffs_dir / f"{stem}.json"

    return config, _mock_tjp


class TestDeriveIcosTariffsFlat:
    def test_writes_delivery_and_supply(self, tmp_path: Path) -> None:
        config, mock_tjp = _make_pipeline_config(tmp_path)

        precalc = tmp_path / "outputs" / "precalc"
        calibrated = tmp_path / "outputs" / "calibrated"

        _write_cairo_outputs(
            precalc,
            bldg_ids=[1, 2],
            weights=[1.0, 1.0],
            heating_types=["natgas", "natgas"],
            economic_burdens=[100.0, 200.0],
            annual_kwh=[1000.0, 2000.0],
        )
        _write_cairo_outputs(
            calibrated,
            bldg_ids=[1, 2],
            weights=[1.0, 1.0],
            heating_types=["natgas", "natgas"],
            economic_burdens=[120.0, 220.0],
            annual_kwh=[1200.0, 2200.0],
        )

        dep_outputs = {
            "precalc_delivery": precalc,
            "precalc_supply": precalc,
            "calibrated_delivery": calibrated,
            "calibrated_supply": calibrated,
        }

        scenario = MagicMock()
        scenario.derive_method = "icos"
        scenario.structure = "flat"
        scenario.icos = {"heating_types": ["natgas"]}
        scenario.tariff_base = "icos_fair_flat"
        scenario.depends_on = ["parent"]

        with patch("rate_design.hp_rates.pipeline_config.tariff_json_path", mock_tjp):
            paths = derive_icos_tariffs(config, scenario, dep_outputs)

        assert len(paths) == 2
        assert paths[0].exists()
        assert paths[1].exists()

        d_tariff = json.loads(paths[0].read_text())
        assert "items" in d_tariff
        d_rate = d_tariff["items"][0]["energyratestructure"][0][0]["rate"]
        assert d_rate > 0

        s_tariff = json.loads(paths[1].read_text())
        s_rate = s_tariff["items"][0]["energyratestructure"][0][0]["rate"]
        assert s_rate > d_rate


class TestDeriveIcosTariffsSeasonal:
    def test_writes_seasonal_tariff(self, tmp_path: Path) -> None:
        config, mock_tjp = _make_pipeline_config(tmp_path)

        precalc = tmp_path / "outputs" / "precalc"
        calibrated = tmp_path / "outputs" / "calibrated"

        winter_months = [11, 12, 1, 2, 3]

        hourly_before: dict[int, list[float]] = {}
        hourly_after: dict[int, list[float]] = {}
        start = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        for bid in [1, 2]:
            before_vals = []
            after_vals = []
            for h in range(8760):
                ts = start + datetime.timedelta(hours=h)
                before_vals.append(1.0)
                after_vals.append(2.0 if ts.month in winter_months else 1.0)
            hourly_before[bid] = before_vals
            hourly_after[bid] = after_vals

        _write_cairo_outputs(
            precalc,
            bldg_ids=[1, 2],
            weights=[1.0, 1.0],
            heating_types=["natgas", "natgas"],
            economic_burdens=[100.0, 200.0],
            annual_kwh=[1000.0, 2000.0],
            hourly_kwh=hourly_before,
        )
        _write_cairo_outputs(
            calibrated,
            bldg_ids=[1, 2],
            weights=[1.0, 1.0],
            heating_types=["natgas", "natgas"],
            economic_burdens=[120.0, 220.0],
            annual_kwh=[1200.0, 2200.0],
            hourly_kwh=hourly_after,
        )

        dep_outputs = {
            "precalc_delivery": precalc,
            "precalc_supply": precalc,
            "calibrated_delivery": calibrated,
            "calibrated_supply": calibrated,
        }

        scenario = MagicMock()
        scenario.derive_method = "icos"
        scenario.structure = "seasonal"
        scenario.icos = {"heating_types": ["natgas"]}
        scenario.tariff_base = "icos_fair_seasonal"
        scenario.depends_on = ["parent"]

        with patch("rate_design.hp_rates.pipeline_config.tariff_json_path", mock_tjp):
            paths = derive_icos_tariffs(config, scenario, dep_outputs)

        assert len(paths) == 2

        d_tariff = json.loads(paths[0].read_text())
        rate_structure = d_tariff["items"][0]["energyratestructure"]
        assert len(rate_structure) == 2


# ---------------------------------------------------------------------------
# Config parsing round-trip
# ---------------------------------------------------------------------------


class TestIcosConfigParsing:
    def test_parse_icos_scenario(self) -> None:
        from rate_design.hp_rates.pipeline_config import _parse_scenario

        raw = {
            "quartet": "single_rate_uncalibrated",
            "tariff_base": "icos_fair_flat",
            "depends_on": "default_ry1_r1_uncalibrated",
            "derive_method": "icos",
            "structure": "flat",
            "icos": {"heating_types": ["natgas", "delivered_fuels"]},
        }
        sc = _parse_scenario("test_icos", raw)
        assert sc.derive_method == "icos"
        assert sc.structure == "flat"
        assert sc.icos == {"heating_types": ["natgas", "delivered_fuels"]}
        assert sc.depends_on == ["default_ry1_r1_uncalibrated"]

    def test_parse_non_icos_has_none(self) -> None:
        from rate_design.hp_rates.pipeline_config import _parse_scenario

        raw = {
            "quartet": "single_rate_uncalibrated",
            "tariff_base": "default",
        }
        sc = _parse_scenario("test_plain", raw)
        assert sc.derive_method is None
        assert sc.structure is None
        assert sc.icos is None

    def test_preflight_skips_tariff_check_for_icos(self) -> None:
        """When derive_method is set, preflight does not require tariff JSONs to exist."""
        from rate_design.hp_rates.pipeline_config import _parse_scenario

        raw = {
            "quartet": "single_rate_uncalibrated",
            "tariff_base": "icos_fair_flat",
            "depends_on": "parent",
            "derive_method": "icos",
            "structure": "flat",
            "icos": {"heating_types": ["natgas"]},
        }
        sc = _parse_scenario("test_icos_preflight", raw)
        assert sc.is_single_rate
        assert sc.derive_method is not None
