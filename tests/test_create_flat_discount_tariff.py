from __future__ import annotations

import pytest

from utils.mid.create_flat_discount_tariff import _extract_flat_rate


def test_extract_flat_rate_prefers_current_column() -> None:
    row = {
        "flat_rate": "0.123",
    }

    assert _extract_flat_rate(row) == pytest.approx(0.123)


def test_extract_flat_rate_requires_supported_column() -> None:
    with pytest.raises(ValueError, match="must contain a 'flat_rate' column"):
        _extract_flat_rate({"flat_rate_hp": "0.1"})
