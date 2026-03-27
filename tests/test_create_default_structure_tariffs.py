"""Tests for the default-structure tariff generation script."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from utils.pre.create_default_structure_tariffs import (
    _build_flat_tariff,
    _build_seasonal_tiered_tariff,
    _build_seasonal_tou_tariff,
    _detect_periods_from_monthly_rates,
    _extract_fixed_charge_avg,
    _extract_flat_kwh_rates,
    _extract_tou_supply_monthly,
    _season_months,
    main,
    process_utility,
)
from utils.pre.create_tariff import (
    create_seasonal_tiered_tariff,
    create_seasonal_tou_tariff_direct,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uniform(rate: float, year: int = 2025) -> dict[str, float]:
    return {f"{year}-{m:02d}": rate for m in range(1, 13)}


def _quarterly(rates: list[float], year: int = 2025) -> dict[str, float]:
    """Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec."""
    out: dict[str, float] = {}
    for m in range(1, 13):
        q = (m - 1) // 3
        out[f"{year}-{m:02d}"] = rates[q]
    return out


def _write_yaml(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(content, default_flow_style=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# create_seasonal_tiered_tariff
# ---------------------------------------------------------------------------


class TestCreateSeasonalTieredTariff:
    def test_single_tier_degenerate(self) -> None:
        tariff = create_seasonal_tiered_tariff(
            label="test_default",
            periods=[
                ([1, 2, 3], [(0.12, None)]),
                ([4, 5, 6, 7, 8, 9, 10, 11, 12], [(0.10, None)]),
            ],
            fixed_charge=6.0,
            utility="TestUtil",
        )
        item = tariff["items"][0]
        assert item["label"] == "test_default"
        assert len(item["energyratestructure"]) == 2
        assert item["energyratestructure"][0][0]["rate"] == 0.12
        assert item["energyratestructure"][1][0]["rate"] == 0.10
        assert "max" not in item["energyratestructure"][0][0]
        assert item["fixedchargefirstmeter"] == 6.0

    def test_two_tiers_with_max(self) -> None:
        tariff = create_seasonal_tiered_tariff(
            label="coned_default",
            periods=[
                ([6, 7, 8, 9], [(0.16, 250.0), (0.185, None)]),
                ([1, 2, 3, 4, 5, 10, 11, 12], [(0.16, 250.0), (0.16, None)]),
            ],
            fixed_charge=21.28,
            utility="coned",
        )
        item = tariff["items"][0]
        assert len(item["energyratestructure"]) == 2
        summer_tiers = item["energyratestructure"][0]
        assert len(summer_tiers) == 2
        assert summer_tiers[0]["rate"] == 0.16
        assert summer_tiers[0]["max"] == 250.0
        assert summer_tiers[1]["rate"] == 0.185
        assert "max" not in summer_tiers[1]

    def test_schedule_maps_months(self) -> None:
        tariff = create_seasonal_tiered_tariff(
            label="t",
            periods=[
                ([1, 2, 3], [(0.1, None)]),
                ([4, 5, 6, 7, 8, 9, 10, 11, 12], [(0.2, None)]),
            ],
            fixed_charge=5.0,
        )
        item = tariff["items"][0]
        assert item["energyweekdayschedule"][0][0] == 0  # Jan -> period 0
        assert item["energyweekdayschedule"][3][0] == 1  # Apr -> period 1
        assert item["energyweekdayschedule"][11][0] == 1  # Dec -> period 1


# ---------------------------------------------------------------------------
# create_seasonal_tou_tariff_direct
# ---------------------------------------------------------------------------


class TestCreateSeasonalTouTariffDirect:
    def test_custom_schedules_preserved(self) -> None:
        weekday = [[0] * 24 for _ in range(12)]
        weekend = [[0] * 24 for _ in range(12)]
        weekday[0][15] = 1
        rate_structure = [
            [{"rate": 0.10, "adj": 0.0, "unit": "kWh"}],
            [{"rate": 0.20, "adj": 0.0, "unit": "kWh"}],
        ]
        tariff = create_seasonal_tou_tariff_direct(
            label="tou_test",
            weekday_schedule=weekday,
            weekend_schedule=weekend,
            rate_structure=rate_structure,
            fixed_charge=16.44,
        )
        item = tariff["items"][0]
        assert item["energyweekdayschedule"][0][15] == 1
        assert item["energyweekendschedule"][0][15] == 0
        assert item["fixedchargefirstmeter"] == 16.44
        assert len(item["energyratestructure"]) == 2


# ---------------------------------------------------------------------------
# Fixed charge extraction
# ---------------------------------------------------------------------------


class TestExtractFixedChargeAvg:
    def test_dollar_per_month_customer_charge(self) -> None:
        section = {
            "charges": {
                "customer_charge": {
                    "charge_unit": "$/month",
                    "monthly_rates": _uniform(6.0),
                },
            }
        }
        assert _extract_fixed_charge_avg(section) == pytest.approx(6.0)

    def test_dollar_per_day_customer_charge(self) -> None:
        section = {
            "charges": {
                "customer_charge": {
                    "charge_unit": "$/day",
                    "monthly_rates": _uniform(0.54),
                },
            }
        }
        days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        expected = sum(0.54 * d for d in days) / 12
        assert _extract_fixed_charge_avg(section) == pytest.approx(expected, rel=1e-3)

    def test_includes_billing_payment_processing(self) -> None:
        section = {
            "charges": {
                "customer_charge": {
                    "charge_unit": "$/month",
                    "monthly_rates": _uniform(20.0),
                },
                "billing_payment_processing": {
                    "charge_unit": "$/month",
                    "monthly_rates": _uniform(1.28),
                },
            }
        }
        assert _extract_fixed_charge_avg(section) == pytest.approx(21.28)

    def test_includes_all_fixed_charges(self) -> None:
        section = {
            "charges": {
                "customer_charge": {
                    "charge_unit": "$/month",
                    "monthly_rates": _uniform(6.0),
                },
                "re_growth_charge": {
                    "charge_unit": "$/month",
                    "monthly_rates": _uniform(5.75),
                },
                "liheap_enhancement_charge": {
                    "charge_unit": "$/month",
                    "monthly_rates": _uniform(0.79),
                },
            }
        }
        assert _extract_fixed_charge_avg(section) == pytest.approx(6.0 + 5.75 + 0.79)

    def test_excludes_kwh_charges(self) -> None:
        section = {
            "charges": {
                "core_delivery_rate": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.05),
                },
            }
        }
        assert _extract_fixed_charge_avg(section) == 0.0


# ---------------------------------------------------------------------------
# Flat kWh rate extraction
# ---------------------------------------------------------------------------


class TestExtractFlatKwhRates:
    def test_sums_multiple_charges(self) -> None:
        section = {
            "charges": {
                "core_delivery": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.05),
                },
                "o_m": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.002),
                },
            }
        }
        rates = _extract_flat_kwh_rates(section)
        assert rates["2025-01"] == pytest.approx(0.052)

    def test_ignores_fixed_charges(self) -> None:
        section = {
            "charges": {
                "customer_charge": {
                    "charge_unit": "$/month",
                    "monthly_rates": _uniform(6.0),
                },
            }
        }
        assert _extract_flat_kwh_rates(section) == {}


# ---------------------------------------------------------------------------
# Period detection
# ---------------------------------------------------------------------------


class TestDetectPeriods:
    def test_uniform_is_one_period(self) -> None:
        rates = _uniform(0.10)
        periods = _detect_periods_from_monthly_rates(rates)
        assert len(periods) == 1
        assert periods[0][0] == list(range(1, 13))
        assert periods[0][1] == pytest.approx(0.10)

    def test_quarterly_is_four_periods(self) -> None:
        rates = _quarterly([0.10, 0.12, 0.15, 0.11])
        periods = _detect_periods_from_monthly_rates(rates)
        assert len(periods) == 4
        assert periods[0] == ([1, 2, 3], pytest.approx(0.10))
        assert periods[1] == ([4, 5, 6], pytest.approx(0.12))

    def test_merges_identical_adjacent_quarters(self) -> None:
        rates = _quarterly([0.10, 0.10, 0.12, 0.12])
        periods = _detect_periods_from_monthly_rates(rates)
        assert len(periods) == 2
        assert periods[0] == ([1, 2, 3, 4, 5, 6], pytest.approx(0.10))
        assert periods[1] == ([7, 8, 9, 10, 11, 12], pytest.approx(0.12))


# ---------------------------------------------------------------------------
# Season months parsing
# ---------------------------------------------------------------------------


class TestSeasonMonths:
    def test_summer_winter(self) -> None:
        sm = _season_months(
            {
                "summer": {"from_month": 6, "from_day": 1, "to_month": 9, "to_day": 30},
                "winter": {
                    "from_month": 10,
                    "from_day": 1,
                    "to_month": 5,
                    "to_day": 31,
                },
            }
        )
        assert sm["summer"] == [6, 7, 8, 9]
        assert sm["winter"] == [10, 11, 12, 1, 2, 3, 4, 5]


# ---------------------------------------------------------------------------
# Build flat tariff (integration)
# ---------------------------------------------------------------------------


class TestBuildFlatTariff:
    def test_rie_like(self) -> None:
        already = {
            "rate_structure": "flat",
            "charges": {
                "customer_charge": {
                    "charge_unit": "$/month",
                    "monthly_rates": _uniform(6.0),
                },
                "core_delivery_rate": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.0458),
                },
            },
        }
        add_drr = {
            "rate_structure": "flat",
            "charges": {
                "transmission": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _quarterly([0.04, 0.04, 0.05, 0.05]),
                },
            },
        }
        add_srr = {
            "rate_structure": "flat",
            "charges": {
                "supply": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _quarterly([0.15, 0.08, 0.08, 0.13]),
                },
            },
        }
        delivery, supply = _build_flat_tariff(
            "testutil", already, add_drr, add_srr, 6.0
        )

        d_item = delivery["items"][0]
        assert d_item["label"] == "testutil_default"
        assert d_item["fixedchargefirstmeter"] == 6.0
        assert len(d_item["energyratestructure"]) == 2

        s_item = supply["items"][0]
        assert s_item["label"] == "testutil_default_supply"
        assert len(s_item["energyratestructure"]) >= 2


# ---------------------------------------------------------------------------
# Build seasonal_tiered tariff (integration)
# ---------------------------------------------------------------------------


class TestBuildSeasonalTieredTariff:
    def test_coned_like(self) -> None:
        already = {
            "rate_structure": "seasonal_tiered",
            "seasons": {
                "summer": {"from_month": 6, "from_day": 1, "to_month": 9, "to_day": 30},
                "winter": {
                    "from_month": 10,
                    "from_day": 1,
                    "to_month": 5,
                    "to_day": 31,
                },
            },
            "charges": {
                "customer_charge": {
                    "charge_unit": "$/month",
                    "monthly_rates": _uniform(20.0),
                },
                "core_delivery_rate": {
                    "charge_unit": "$/kWh",
                    "tiers": [
                        {
                            "upper_limit_kwh": 250.0,
                            "monthly_rates": {
                                "summer": _uniform(0.16),
                                "winter": _uniform(0.16),
                            },
                        },
                        {
                            "upper_limit_kwh": None,
                            "monthly_rates": {
                                "summer": _uniform(0.185),
                                "winter": _uniform(0.16),
                            },
                        },
                    ],
                },
            },
        }
        add_drr = {
            "rate_structure": "flat",
            "charges": {
                "surcharge": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.01),
                },
            },
        }
        add_srr = {
            "rate_structure": "flat",
            "charges": {
                "supply": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.09),
                },
            },
        }
        delivery, supply = _build_seasonal_tiered_tariff(
            "coned", already, add_drr, add_srr, 20.0
        )

        d_item = delivery["items"][0]
        assert d_item["label"] == "coned_default"
        assert len(d_item["energyratestructure"]) == 2
        summer_tiers = d_item["energyratestructure"][0]
        assert len(summer_tiers) == 2
        assert summer_tiers[0]["max"] == 250.0

        s_item = supply["items"][0]
        assert s_item["label"] == "coned_default_supply"


# ---------------------------------------------------------------------------
# Build seasonal_tou tariff (integration)
# ---------------------------------------------------------------------------


class TestBuildSeasonalTouTariff:
    def test_psegli_like(self) -> None:
        already = {
            "rate_structure": "seasonal_tou",
            "seasons": {
                "summer": {"from_month": 6, "from_day": 1, "to_month": 9, "to_day": 30},
                "winter": {
                    "from_month": 10,
                    "from_day": 1,
                    "to_month": 5,
                    "to_day": 31,
                },
            },
            "tou_periods": {
                "on_peak": {
                    "from_hour": 15,
                    "to_hour": 19,
                    "weekdays_only": True,
                },
                "off_peak": {
                    "from_hour": 19,
                    "to_hour": 15,
                    "weekdays_only": True,
                },
            },
            "charges": {
                "customer_charge": {
                    "charge_unit": "$/day",
                    "monthly_rates": _uniform(0.54),
                },
                "delivery_charge": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": {
                        "summer_off_peak": _uniform(0.10),
                        "summer_on_peak": _uniform(0.21),
                        "winter_off_peak": _uniform(0.09),
                        "winter_on_peak": _uniform(0.18),
                    },
                },
            },
        }
        add_drr = {
            "rate_structure": "flat",
            "charges": {
                "surcharge": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.005),
                },
            },
        }
        fixed = 0.54 * 365.25 / 12
        delivery, supply = _build_seasonal_tou_tariff(
            "psegli", already, add_drr, None, fixed
        )

        d_item = delivery["items"][0]
        assert d_item["label"] == "psegli_default"
        assert len(d_item["energyratestructure"]) == 4

        # Check TOU schedule: weekday peak hours use on-peak period
        wd = d_item["energyweekdayschedule"]
        we = d_item["energyweekendschedule"]
        jan_wd_15 = wd[0][15]
        jan_wd_10 = wd[0][10]
        assert jan_wd_15 != jan_wd_10
        assert we[0][15] == we[0][10]


# ---------------------------------------------------------------------------
# _extract_tou_supply_monthly
# ---------------------------------------------------------------------------


class TestExtractTouSupplyMonthly:
    """Flat riders must be distributed across TOU slot keys, not lost in _flat."""

    @staticmethod
    def _make_psegli_like_srr() -> dict:
        """Supply section with flat charges listed BEFORE the TOU charge."""
        return {
            "rate_structure": "seasonal_tou",
            "charges": {
                "merchant_function_charge": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.002),
                },
                "securitization_offset": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(-0.020),
                },
                "supply_commodity": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": {
                        "summer_on_peak": _uniform(0.24),
                        "summer_off_peak": _uniform(0.10),
                        "winter_on_peak": _uniform(0.25),
                        "winter_off_peak": _uniform(0.10),
                    },
                },
            },
        }

    def test_flat_riders_included_in_tou_slots(self) -> None:
        result = _extract_tou_supply_monthly(self._make_psegli_like_srr())
        assert "_flat" not in result
        assert sorted(result.keys()) == [
            "summer_off_peak",
            "summer_on_peak",
            "winter_off_peak",
            "winter_on_peak",
        ]
        # Each slot should be commodity + merchant + securitization
        assert result["summer_on_peak"]["2025-01"] == pytest.approx(
            0.24 + 0.002 - 0.020
        )
        assert result["winter_off_peak"]["2025-06"] == pytest.approx(
            0.10 + 0.002 - 0.020
        )

    def test_order_independence(self) -> None:
        """Same result whether TOU charge comes first or last."""
        srr_tou_first = {
            "rate_structure": "seasonal_tou",
            "charges": {
                "supply_commodity": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": {
                        "summer_on_peak": _uniform(0.24),
                        "summer_off_peak": _uniform(0.10),
                        "winter_on_peak": _uniform(0.25),
                        "winter_off_peak": _uniform(0.10),
                    },
                },
                "flat_rider": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.005),
                },
            },
        }
        srr_flat_first = {
            "rate_structure": "seasonal_tou",
            "charges": {
                "flat_rider": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.005),
                },
                "supply_commodity": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": {
                        "summer_on_peak": _uniform(0.24),
                        "summer_off_peak": _uniform(0.10),
                        "winter_on_peak": _uniform(0.25),
                        "winter_off_peak": _uniform(0.10),
                    },
                },
            },
        }
        result_a = _extract_tou_supply_monthly(srr_tou_first)
        result_b = _extract_tou_supply_monthly(srr_flat_first)
        for sk in result_a:
            for mk in result_a[sk]:
                assert result_a[sk][mk] == pytest.approx(result_b[sk][mk])

    def test_flat_only_falls_back_to_flat_key(self) -> None:
        srr = {
            "rate_structure": "flat",
            "charges": {
                "supply": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.08),
                },
            },
        }
        result = _extract_tou_supply_monthly(srr)
        assert "_flat" in result
        assert result["_flat"]["2025-01"] == pytest.approx(0.08)


class TestBuildSeasonalTouWithSupply:
    """Integration: TOU supply tariff includes flat riders in rate structure."""

    @staticmethod
    def _psegli_like_inputs(
        supply_monthly: dict[str, float] | None = None,
    ) -> tuple[dict, dict, dict, float]:
        """Return (already, add_drr, add_srr, fixed_charge) for a PSEGLI-like utility."""
        already = {
            "rate_structure": "seasonal_tou",
            "seasons": {
                "summer": {"from_month": 6, "from_day": 1, "to_month": 9, "to_day": 30},
                "winter": {
                    "from_month": 10,
                    "from_day": 1,
                    "to_month": 5,
                    "to_day": 31,
                },
            },
            "tou_periods": {
                "on_peak": {
                    "from_hour": 15,
                    "to_hour": 19,
                    "weekdays_only": True,
                },
                "off_peak": {
                    "from_hour": 19,
                    "to_hour": 15,
                    "weekdays_only": True,
                },
            },
            "charges": {
                "customer_charge": {
                    "charge_unit": "$/day",
                    "monthly_rates": _uniform(0.54),
                },
                "delivery_charge": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": {
                        "summer_off_peak": _uniform(0.10),
                        "summer_on_peak": _uniform(0.21),
                        "winter_off_peak": _uniform(0.09),
                        "winter_on_peak": _uniform(0.18),
                    },
                },
            },
        }
        add_drr = {
            "rate_structure": "flat",
            "charges": {
                "surcharge": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(0.005),
                },
            },
        }
        if supply_monthly is None:
            supply_monthly = _uniform(0.10)
        add_srr = {
            "rate_structure": "seasonal_tou",
            "charges": {
                "flat_rider": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": _uniform(-0.018),
                },
                "supply_commodity": {
                    "charge_unit": "$/kWh",
                    "monthly_rates": {
                        "summer_on_peak": {
                            k: v * 2.4 for k, v in supply_monthly.items()
                        },
                        "summer_off_peak": supply_monthly,
                        "winter_on_peak": {
                            k: v * 2.5 for k, v in supply_monthly.items()
                        },
                        "winter_off_peak": supply_monthly,
                    },
                },
            },
        }
        fixed = 0.54 * 365.25 / 12
        return already, add_drr, add_srr, fixed

    def test_no_dead_rate_entries_monthly_supply(self) -> None:
        """Every energyratestructure entry must be referenced by the schedule.

        When supply varies monthly, the old code created entries for all 4
        slot_keys (2 seasons × 2 TOU) per period group, even though only
        2 are used per month.  CAIRO iterates *all* entries and fails with
        'pre-calculated energy charge for period incorrectly subselected!'
        on the dead entries.
        """
        supply_monthly = {f"2025-{m:02d}": 0.08 + m * 0.001 for m in range(1, 13)}
        already, add_drr, add_srr, fixed = self._psegli_like_inputs(supply_monthly)

        _delivery, supply = _build_seasonal_tou_tariff(
            "psegli", already, add_drr, add_srr, fixed
        )
        s_item = supply["items"][0]
        n_entries = len(s_item["energyratestructure"])

        all_schedule_indices: set[int] = set()
        for row in s_item["energyweekdayschedule"]:
            all_schedule_indices.update(row)
        for row in s_item["energyweekendschedule"]:
            all_schedule_indices.update(row)

        assert all_schedule_indices == set(range(n_entries)), (
            f"Schedule references {sorted(all_schedule_indices)} "
            f"but rate structure has {n_entries} entries (expected 0..{n_entries - 1})"
        )

        # 12 months each using 2 TOU slots = 24 (not 48)
        assert n_entries == 24

    def test_supply_rates_include_flat_riders(self) -> None:
        already, add_drr, add_srr, fixed = self._psegli_like_inputs()
        _delivery, supply = _build_seasonal_tou_tariff(
            "psegli", already, add_drr, add_srr, fixed
        )

        s_item = supply["items"][0]
        rates = [p[0]["rate"] for p in s_item["energyratestructure"]]
        # All combined rates should include the -0.018 flat rider
        # Winter off-peak = delivery(0.09+0.005) + supply(0.10-0.018) = 0.177
        expected_winter_off = 0.09 + 0.005 + 0.10 - 0.018
        assert any(abs(r - expected_winter_off) < 1e-4 for r in rates), (
            f"Expected ~{expected_winter_off} in rates {rates}"
        )

        # Every rate entry must be schedule-referenced (no dead entries)
        all_indices: set[int] = set()
        for row in s_item["energyweekdayschedule"] + s_item["energyweekendschedule"]:
            all_indices.update(row)
        assert all_indices == set(range(len(s_item["energyratestructure"])))


# ---------------------------------------------------------------------------
# process_utility (full integration with YAML)
# ---------------------------------------------------------------------------


class TestProcessUtility:
    def _make_flat_yaml(self, path: Path) -> None:
        _write_yaml(
            path,
            {
                "utility": "testutil",
                "start_month": "2025-01",
                "end_month": "2025-12",
                "already_in_drr": {
                    "rate_structure": "flat",
                    "charges": {
                        "customer_charge": {
                            "charge_unit": "$/month",
                            "monthly_rates": _uniform(6.0),
                        },
                        "core_delivery_rate": {
                            "charge_unit": "$/kWh",
                            "monthly_rates": _uniform(0.05),
                        },
                    },
                },
                "add_to_drr": {
                    "rate_structure": "flat",
                    "charges": {
                        "transmission": {
                            "charge_unit": "$/kWh",
                            "monthly_rates": _quarterly([0.04, 0.04, 0.05, 0.05]),
                        },
                    },
                },
                "add_to_srr": {
                    "rate_structure": "flat",
                    "charges": {
                        "supply": {
                            "charge_unit": "$/kWh",
                            "monthly_rates": _quarterly([0.15, 0.08, 0.08, 0.13]),
                        },
                    },
                },
            },
        )

    def test_creates_both_files(self, tmp_path: Path) -> None:
        mr_path = tmp_path / "testutil_monthly_rates_2025.yaml"
        self._make_flat_yaml(mr_path)
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        process_utility("testutil", mr_path, output_dir)

        assert (output_dir / "testutil_default.json").exists()
        assert (output_dir / "testutil_default_supply.json").exists()

    def test_tariff_structure_valid(self, tmp_path: Path) -> None:
        mr_path = tmp_path / "testutil_monthly_rates_2025.yaml"
        self._make_flat_yaml(mr_path)
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        process_utility("testutil", mr_path, output_dir)

        tariff = json.loads((output_dir / "testutil_default.json").read_text())
        item = tariff["items"][0]
        assert item["label"] == "testutil_default"
        assert item["fixedchargefirstmeter"] == 6.0
        assert len(item["energyweekdayschedule"]) == 12
        assert len(item["energyweekdayschedule"][0]) == 24
        assert len(item["energyratestructure"]) >= 1

    def test_preserves_seasonal_variation(self, tmp_path: Path) -> None:
        mr_path = tmp_path / "testutil_monthly_rates_2025.yaml"
        self._make_flat_yaml(mr_path)
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        process_utility("testutil", mr_path, output_dir)

        tariff = json.loads((output_dir / "testutil_default.json").read_text())
        rates = [p[0]["rate"] for p in tariff["items"][0]["energyratestructure"]]
        assert len(set(rates)) > 1

    def test_fixed_charge_includes_add_to_drr_riders(self, tmp_path: Path) -> None:
        mr_path = tmp_path / "rie_monthly_rates_2025.yaml"
        _write_yaml(
            mr_path,
            {
                "utility": "rie",
                "start_month": "2025-01",
                "end_month": "2025-12",
                "already_in_drr": {
                    "rate_structure": "flat",
                    "charges": {
                        "customer_charge": {
                            "charge_unit": "$/month",
                            "monthly_rates": _uniform(6.0),
                        },
                        "core_delivery_rate": {
                            "charge_unit": "$/kWh",
                            "monthly_rates": _uniform(0.05),
                        },
                    },
                },
                "add_to_drr": {
                    "rate_structure": "flat",
                    "charges": {
                        "re_growth_charge": {
                            "charge_unit": "$/month",
                            "monthly_rates": _uniform(5.75),
                        },
                        "liheap_enhancement_charge": {
                            "charge_unit": "$/month",
                            "monthly_rates": _uniform(0.79),
                        },
                        "transmission": {
                            "charge_unit": "$/kWh",
                            "monthly_rates": _uniform(0.04),
                        },
                    },
                },
                "add_to_srr": {
                    "rate_structure": "flat",
                    "charges": {
                        "supply": {
                            "charge_unit": "$/kWh",
                            "monthly_rates": _uniform(0.10),
                        },
                    },
                },
            },
        )
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        process_utility("rie", mr_path, output_dir)

        tariff = json.loads((output_dir / "rie_default.json").read_text())
        item = tariff["items"][0]
        assert item["fixedchargefirstmeter"] == pytest.approx(6.0 + 5.75 + 0.79)


# ---------------------------------------------------------------------------
# CLI: main()
# ---------------------------------------------------------------------------


class TestCLI:
    def test_generates_tariffs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mr_dir = tmp_path / "monthly_rates"
        mr_dir.mkdir()
        _write_yaml(
            mr_dir / "acme_monthly_rates_2025.yaml",
            {
                "utility": "acme",
                "start_month": "2025-01",
                "end_month": "2025-12",
                "already_in_drr": {
                    "rate_structure": "flat",
                    "charges": {
                        "customer_charge": {
                            "charge_unit": "$/month",
                            "monthly_rates": _uniform(5.0),
                        },
                        "delivery": {
                            "charge_unit": "$/kWh",
                            "monthly_rates": _uniform(0.10),
                        },
                    },
                },
                "add_to_drr": {"rate_structure": "flat", "charges": {}},
                "add_to_srr": {
                    "rate_structure": "flat",
                    "charges": {
                        "supply": {
                            "charge_unit": "$/kWh",
                            "monthly_rates": _uniform(0.08),
                        },
                    },
                },
            },
        )
        output_dir = tmp_path / "output"

        monkeypatch.setattr(
            "sys.argv",
            [
                "create_default_structure_tariffs.py",
                "--monthly-rates-dir",
                str(mr_dir),
                "--output-dir",
                str(output_dir),
                "--year",
                "2025",
            ],
        )
        main()

        assert (output_dir / "acme_default.json").exists()
        assert (output_dir / "acme_default_supply.json").exists()

    def test_no_matching_files_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        empty = tmp_path / "empty"
        empty.mkdir()
        monkeypatch.setattr(
            "sys.argv",
            [
                "create_default_structure_tariffs.py",
                "--monthly-rates-dir",
                str(empty),
                "--output-dir",
                str(tmp_path / "out"),
            ],
        )
        with pytest.raises(FileNotFoundError, match="No files matching"):
            main()
