"""Tests for utils/pre/rev_requirement/fetch_monthly_rates.py."""

from __future__ import annotations

from utils.pre.rev_requirement.fetch_monthly_rates import (
    _extract_bands,
    _signed_rate,
)


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
