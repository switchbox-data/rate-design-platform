"""Tests for utils/pre/rev_requirement/fetch_monthly_rates.py."""

from __future__ import annotations

import pytest

from utils.pre.rev_requirement.fetch_monthly_rates import (
    _build_grouped_output,
    _collapse_redundant_bands,
    _determine_rate_structure,
    _extract_bands,
    _season_contains_month,
    _signed_rate,
)

MONTHS = [f"2025-{m:02d}" for m in range(1, 13)]

WINTER_H1 = {
    "name": "Winter",
    "from_month": 1,
    "from_day": 1,
    "to_month": 6,
    "to_day": 30,
}
SUMMER_H2 = {
    "name": "Summer",
    "from_month": 7,
    "from_day": 1,
    "to_month": 12,
    "to_day": 31,
}
ON_PEAK = {"type": "On-Peak", "name": "On-Peak", "from_hour": 12, "to_hour": 20}
OFF_PEAK = {"type": "Off-Peak", "name": "Off-Peak", "from_hour": 20, "to_hour": 12}

# (rate, upper_limit) pairs; upper_limit None means the unbounded top band.
BandPairs = list[tuple[float, float | None]]


def _all_months(value: float) -> dict[str, float]:
    """A monthly_rates dict with the same value in every month.

    Genability resolves a rate for every queried month even outside its own
    season, which is what makes season-aware merging necessary.
    """
    return dict.fromkeys(MONTHS, value)


def _bands(pairs: BandPairs) -> list[dict]:
    return [
        {"upper_limit": upper, "monthly_rates": _all_months(rate)}
        for rate, upper in pairs
    ]


def _entry(
    rate_name: str,
    master_charge: str,
    decision: str,
    value: float,
    *,
    band_pairs: BandPairs | None = None,
    season: dict | None = None,
    tou: dict | None = None,
) -> dict:
    return {
        "tariff_rate_id": abs(hash(rate_name)) % 100_000,
        "master_charge": master_charge,
        "rate_name": rate_name,
        "decision": decision,
        "charge_unit": "$/kWh",
        "monthly_rates": _all_months(value),
        "band_monthly_rates": _bands(band_pairs) if band_pairs else None,
        "season_meta": season,
        "tou_meta": tou,
    }


class TestSignedRate:
    """_signed_rate negates positive rateAmount when isCredit is true."""

    def test_normal_positive(self):
        assert _signed_rate({"rateAmount": 0.05}) == 0.05

    def test_normal_negative(self):
        assert _signed_rate({"rateAmount": -0.02}) == -0.02

    def test_credit_positive_negated(self):
        assert _signed_rate({"rateAmount": 0.00274, "isCredit": True}) == -0.00274

    def test_credit_already_negative_unchanged(self):
        assert _signed_rate({"rateAmount": -0.02, "isCredit": True}) == -0.02

    def test_credit_zero_unchanged(self):
        assert _signed_rate({"rateAmount": 0.0, "isCredit": True}) == 0.0

    def test_credit_false_unchanged(self):
        assert _signed_rate({"rateAmount": 0.05, "isCredit": False}) == 0.05

    def test_none_amount(self):
        assert _signed_rate({"rateAmount": None}) is None

    def test_missing_amount(self):
        assert _signed_rate({}) is None


class TestExtractBands:
    """_extract_bands applies _signed_rate to each band."""

    def test_credit_band_negated(self):
        rate = {
            "rateBands": [
                {
                    "rateAmount": 0.00274,
                    "isCredit": True,
                    "consumptionUpperLimit": None,
                    "rateSequenceNumber": 1,
                }
            ]
        }
        bands = _extract_bands(rate)
        assert len(bands) == 1
        assert bands[0]["rateAmount"] == -0.00274

    def test_normal_band_unchanged(self):
        rate = {
            "rateBands": [
                {
                    "rateAmount": 0.05,
                    "isCredit": False,
                    "consumptionUpperLimit": 250,
                    "rateSequenceNumber": 1,
                },
                {
                    "rateAmount": 0.07,
                    "consumptionUpperLimit": None,
                    "rateSequenceNumber": 2,
                },
            ]
        }
        bands = _extract_bands(rate)
        assert bands[0]["rateAmount"] == 0.05
        assert bands[1]["rateAmount"] == 0.07
        assert bands[0]["consumptionUpperLimit"] == 250


class TestSeasonContainsMonth:
    """_season_contains_month handles normal and year-wrapping windows."""

    def test_normal_window_inside(self):
        assert _season_contains_month(WINTER_H1, 3) is True

    def test_normal_window_outside(self):
        assert _season_contains_month(WINTER_H1, 9) is False

    def test_normal_window_boundaries_inclusive(self):
        assert _season_contains_month(SUMMER_H2, 7) is True
        assert _season_contains_month(SUMMER_H2, 12) is True

    def test_wrapping_window(self):
        winter_wrap = {"name": "Winter", "from_month": 10, "to_month": 5}
        assert _season_contains_month(winter_wrap, 12) is True
        assert _season_contains_month(winter_wrap, 2) is True
        assert _season_contains_month(winter_wrap, 7) is False

    def test_missing_bounds_matches_all(self):
        assert _season_contains_month({"name": "X"}, 6) is True


class TestCollapseRedundantBands:
    """Duplicate bands collapse only when no entry has real tier variation."""

    def test_uniform_bands_collapse(self):
        # CT Rate 1 generation: 5 bands, all the same rate.
        entry = _entry(
            "Generation Service Charge (Jan - June)",
            "Supply commodity (bundled)",
            "add_to_srr",
            0.1129,
            band_pairs=[
                (0.1129, None),
                (0.1129, 800.0),
                (0.1129, None),
                (0.1129, 800.0),
                (0.1129, None),
            ],
            season=WINTER_H1,
        )
        _collapse_redundant_bands([entry])
        assert len(entry["band_monthly_rates"]) == 1
        assert entry["band_monthly_rates"][0]["upper_limit"] is None

    def test_real_tiers_veto_collapse_for_whole_group(self):
        # ConEd: summer varies across the 250 kWh boundary, winter does not.
        # The varying sibling proves the tier structure is real, so neither
        # entry may be collapsed.
        summer = _entry(
            "Summer Rate",
            "Core Delivery Rate",
            "already_in_drr",
            0.16107,
            band_pairs=[(0.16107, 250.0), (0.18518, None)],
            season=SUMMER_H2,
        )
        winter = _entry(
            "Winter Rate",
            "Core Delivery Rate",
            "already_in_drr",
            0.16107,
            band_pairs=[(0.16107, 250.0), (0.16107, None)],
            season=WINTER_H1,
        )
        _collapse_redundant_bands([summer, winter])
        assert len(summer["band_monthly_rates"]) == 2
        assert len(winter["band_monthly_rates"]) == 2

    def test_collapse_is_idempotent(self):
        entry = _entry(
            "X",
            "X",
            "add_to_srr",
            0.5,
            band_pairs=[(0.5, None), (0.5, 800.0)],
            season=WINTER_H1,
        )
        _collapse_redundant_bands([entry])
        _collapse_redundant_bands([entry])
        assert len(entry["band_monthly_rates"]) == 1


class TestDetermineRateStructure:
    """Only surviving multi-band entries make a seasonal group tiered."""

    def test_real_tiers_are_seasonal_tiered(self):
        entries = [
            _entry(
                "Summer Rate",
                "Core Delivery Rate",
                "already_in_drr",
                0.16107,
                band_pairs=[(0.16107, 250.0), (0.18518, None)],
                season=SUMMER_H2,
            )
        ]
        assert _determine_rate_structure(entries) == "seasonal_tiered"

    def test_seasonal_without_real_tiers_is_flat(self):
        entries = [
            _entry(
                "GSC",
                "Supply commodity (bundled)",
                "add_to_srr",
                0.1129,
                band_pairs=[(0.1129, None)],
                season=WINTER_H1,
            )
        ]
        assert _determine_rate_structure(entries) == "flat"

    def test_any_tou_wins(self):
        entries = [
            _entry(
                "GSC On", "Supply commodity (bundled)", "add_to_srr", 0.15, tou=ON_PEAK
            )
        ]
        assert _determine_rate_structure(entries) == "seasonal_tou"


class TestSeasonalFlatMerge:
    """Seasonal alternatives are selected per month, not summed together."""

    def _ct_rate1_supply(self) -> dict:
        uniform: BandPairs = [(0.1129, None), (0.1129, 800.0), (0.1129, None)]
        uniform_summer: BandPairs = [
            (0.09115, None),
            (0.09115, 800.0),
            (0.09115, None),
        ]
        fmcc_winter: BandPairs = [(-0.001, None), (-0.001, 800.0), (-0.001, None)]
        fmcc_summer: BandPairs = [(-0.0012, None), (-0.0012, 800.0), (-0.0012, None)]
        charges = {
            "a": _entry(
                "Generation Service Charge (Jan - June)",
                "Supply commodity (bundled)",
                "add_to_srr",
                0.1129,
                band_pairs=uniform,
                season=WINTER_H1,
            ),
            "b": _entry(
                "FMCC Generation Charge (Jan - June)",
                "Supply commodity (bundled)",
                "add_to_srr",
                -0.001,
                band_pairs=fmcc_winter,
                season=WINTER_H1,
            ),
            "c": _entry(
                "Generation Service Charge (July - Dec)",
                "Supply commodity (bundled)",
                "add_to_srr",
                0.09115,
                band_pairs=uniform_summer,
                season=SUMMER_H2,
            ),
            "d": _entry(
                "FMCC Generation Charge (July - Dec)",
                "Supply commodity (bundled)",
                "add_to_srr",
                -0.0012,
                band_pairs=fmcc_summer,
                season=SUMMER_H2,
            ),
        }
        return _build_grouped_output(
            "ct_eversource", 614, "2025-01", "2025-12", charges, {}
        )

    def test_structure_is_flat(self):
        out = self._ct_rate1_supply()
        assert out["add_to_srr"]["rate_structure"] == "flat"

    def test_monthly_rates_are_month_keyed(self):
        out = self._ct_rate1_supply()
        charge = out["add_to_srr"]["charges"]["supply_commodity_bundled"]
        assert set(charge["monthly_rates"]) == set(MONTHS)

    def test_in_season_components_sum(self):
        out = self._ct_rate1_supply()
        rates = out["add_to_srr"]["charges"]["supply_commodity_bundled"][
            "monthly_rates"
        ]
        # Jan-Jun: winter GSC + winter FMCC
        assert rates["2025-01"] == pytest.approx(0.1129 - 0.001)
        assert rates["2025-06"] == pytest.approx(0.1129 - 0.001)
        # Jul-Dec: summer GSC + summer FMCC
        assert rates["2025-07"] == pytest.approx(0.09115 - 0.0012)
        assert rates["2025-12"] == pytest.approx(0.09115 - 0.0012)

    def test_seasons_are_not_summed_together(self):
        out = self._ct_rate1_supply()
        rates = out["add_to_srr"]["charges"]["supply_commodity_bundled"][
            "monthly_rates"
        ]
        both_seasons = 0.1129 - 0.001 + 0.09115 - 0.0012
        assert rates["2025-01"] != pytest.approx(both_seasons)

    def test_single_season_still_sums(self):
        charges = {
            "a": _entry("GSC", "Supply commodity (bundled)", "add_to_srr", 0.1129),
            "b": _entry("FMCC", "Supply commodity (bundled)", "add_to_srr", -0.001),
        }
        out = _build_grouped_output(
            "ct_eversource_elecheat", 616, "2025-01", "2025-12", charges, {}
        )
        rates = out["add_to_srr"]["charges"]["supply_commodity_bundled"][
            "monthly_rates"
        ]
        assert rates["2025-01"] == pytest.approx(0.1129 - 0.001)


class TestSeasonalTieredMerge:
    """Real tiered structures keep their boundaries and sum within a season."""

    def _coned(self, winter_first: bool) -> dict:
        summer = _entry(
            "Summer Rate",
            "Core Delivery Rate",
            "already_in_drr",
            0.16107,
            band_pairs=[(0.16107, 250.0), (0.18518, None)],
            season=SUMMER_H2,
        )
        winter = _entry(
            "Winter Rate",
            "Core Delivery Rate",
            "already_in_drr",
            0.16107,
            band_pairs=[(0.16107, 250.0), (0.16107, None)],
            season=WINTER_H1,
        )
        ordered = [winter, summer] if winter_first else [summer, winter]
        charges = {f"e{i}": e for i, e in enumerate(ordered)}
        return _build_grouped_output("coned", 809, "2025-01", "2025-12", charges, {})

    def test_tier_boundaries_survive_regardless_of_entry_order(self):
        for winter_first in (False, True):
            out = self._coned(winter_first)
            tiers = out["already_in_drr"]["charges"]["core_delivery_rate"]["tiers"]
            assert [t["upper_limit_kwh"] for t in tiers] == [250.0, None]

    def test_per_season_tier_rates_preserved(self):
        out = self._coned(winter_first=False)
        tiers = out["already_in_drr"]["charges"]["core_delivery_rate"]["tiers"]
        assert tiers[1]["monthly_rates"]["summer"]["2025-01"] == pytest.approx(0.18518)
        assert tiers[1]["monthly_rates"]["winter"]["2025-01"] == pytest.approx(0.16107)

    def test_same_season_entries_are_summed(self):
        # Two additive components both tagged winter with real tiers.
        gsc = _entry(
            "GSC",
            "Supply commodity (bundled)",
            "add_to_srr",
            0.1129,
            band_pairs=[(0.1129, 800.0), (0.20, None)],
            season=WINTER_H1,
        )
        fmcc = _entry(
            "FMCC",
            "Supply commodity (bundled)",
            "add_to_srr",
            -0.001,
            band_pairs=[(-0.001, 800.0), (-0.002, None)],
            season=WINTER_H1,
        )
        charges = {"a": gsc, "b": fmcc}
        out = _build_grouped_output("x", 1, "2025-01", "2025-12", charges, {})
        tiers = out["add_to_srr"]["charges"]["supply_commodity_bundled"]["tiers"]
        assert tiers[0]["monthly_rates"]["winter"]["2025-01"] == pytest.approx(
            0.1129 - 0.001
        )
        assert tiers[1]["monthly_rates"]["winter"]["2025-01"] == pytest.approx(
            0.20 - 0.002
        )


class TestSeasonalTouMerge:
    """Non-TOU components broadcast into every TOU slot."""

    def _ct_rate7_supply(self) -> dict:
        charges = {
            "a": _entry(
                "Generation Service Charge On-Peak",
                "Supply commodity (bundled)",
                "add_to_srr",
                0.15396,
                tou=ON_PEAK,
            ),
            "b": _entry(
                "Generation Service Charge Off-Peak",
                "Supply commodity (bundled)",
                "add_to_srr",
                0.11096,
                tou=OFF_PEAK,
            ),
            "c": _entry(
                "FMCC Generation Charge",
                "Supply commodity (bundled)",
                "add_to_srr",
                -0.001,
            ),
        }
        return _build_grouped_output(
            "ct_eversource_tou", 615, "2025-01", "2025-12", charges, {}
        )

    def test_no_phantom_all_slot(self):
        out = self._ct_rate7_supply()
        rates = out["add_to_srr"]["charges"]["supply_commodity_bundled"][
            "monthly_rates"
        ]
        assert "all" not in rates
        assert set(rates) == {"all_on_peak", "all_off_peak"}

    def test_non_tou_component_added_to_each_slot(self):
        out = self._ct_rate7_supply()
        rates = out["add_to_srr"]["charges"]["supply_commodity_bundled"][
            "monthly_rates"
        ]
        assert rates["all_on_peak"]["2025-01"] == pytest.approx(0.15396 - 0.001)
        assert rates["all_off_peak"]["2025-01"] == pytest.approx(0.11096 - 0.001)

    def test_distinct_tou_slots_are_not_cross_added(self):
        # PSEGLI shape: four season x TOU slots, each its own rate.
        charges = {
            "a": _entry(
                "S On",
                "Core Delivery Rate",
                "already_in_drr",
                0.20,
                season=SUMMER_H2,
                tou=ON_PEAK,
            ),
            "b": _entry(
                "S Off",
                "Core Delivery Rate",
                "already_in_drr",
                0.10,
                season=SUMMER_H2,
                tou=OFF_PEAK,
            ),
            "c": _entry(
                "W On",
                "Core Delivery Rate",
                "already_in_drr",
                0.18,
                season=WINTER_H1,
                tou=ON_PEAK,
            ),
            "d": _entry(
                "W Off",
                "Core Delivery Rate",
                "already_in_drr",
                0.09,
                season=WINTER_H1,
                tou=OFF_PEAK,
            ),
        }
        out = _build_grouped_output("psegli", 1, "2025-01", "2025-12", charges, {})
        rates = out["already_in_drr"]["charges"]["core_delivery_rate"]["monthly_rates"]
        assert set(rates) == {
            "summer_on_peak",
            "summer_off_peak",
            "winter_on_peak",
            "winter_off_peak",
        }
        assert rates["summer_on_peak"]["2025-01"] == pytest.approx(0.20)
        assert rates["winter_off_peak"]["2025-01"] == pytest.approx(0.09)
