from __future__ import annotations

import pytest

from utils.mid.create_fair_default_tariff import create_fair_default_tariff


type TariffItem = dict[str, object]
type TariffPayload = dict[str, list[TariffItem]]


def _base_tariff() -> TariffPayload:
    return {
        "items": [
            {
                "label": "base",
                "name": "base",
                "fixedchargefirstmeter": 9.0,
                "energyratestructure": [[{"rate": 0.2, "unit": "kWh"}]],
                "energyweekdayschedule": [[0] * 24 for _ in range(12)],
                "energyweekendschedule": [[0] * 24 for _ in range(12)],
            }
        ]
    }


def _inputs_row() -> dict[str, object]:
    return {
        "winter_months": "10,11,12,1,2,3",
        "fixed_charge_only_feasible": True,
        "fixed_charge_only_fixed_charge": 4.5,
        "seasonal_rates_only_feasible": True,
        "seasonal_rates_only_fixed_charge": 9.0,
        "seasonal_rates_only_winter_rate": 0.42,
        "seasonal_rates_only_summer_rate": 0.18,
        "seasonal_rates_only_clipped_winter_rate": 0.42,
        "seasonal_rates_only_clipped_summer_rate": 0.18,
        "seasonal_rates_only_residual_cross_subsidy_after_clipping": 0.0,
        "fixed_plus_seasonal_mc_feasible": True,
        "fixed_plus_seasonal_mc_fixed_charge": 6.0,
        "fixed_plus_seasonal_mc_winter_rate": 0.36,
        "fixed_plus_seasonal_mc_summer_rate": 0.24,
    }


def _tou_base_tariff() -> TariffPayload:
    return {
        "items": [
            {
                "label": "base_tou",
                "name": "base_tou",
                "fixedchargefirstmeter": 12.0,
                "energyratestructure": [
                    [{"rate": 0.18, "unit": "kWh"}],
                    [{"rate": 0.29, "unit": "kWh"}],
                ],
                "energyweekdayschedule": [
                    [0] * 16 + [1] * 8 if month in {0, 1, 2, 10, 11} else [0] * 24
                    for month in range(12)
                ],
                "energyweekendschedule": [[0] * 24 for _ in range(12)],
            }
        ]
    }


@pytest.mark.parametrize("base_tariff", [_base_tariff(), _tou_base_tariff()])
def test_create_fixed_fair_default_tariff_preserves_shape(
    base_tariff: TariffPayload,
) -> None:
    base_item = base_tariff["items"][0]
    tariff = create_fair_default_tariff(
        base_tariff=base_tariff,
        inputs_row=_inputs_row(),
        strategy="fixed_charge_only",
        label="fair_fixed",
    )

    item = tariff["items"][0]
    assert item["label"] == "fair_fixed"
    assert item["fixedchargefirstmeter"] == pytest.approx(4.5)
    assert item["energyratestructure"] == base_item["energyratestructure"]
    assert item["energyweekdayschedule"] == base_item["energyweekdayschedule"]
    assert item["energyweekendschedule"] == base_item["energyweekendschedule"]


def test_create_combined_fair_default_tariff_uses_seasonal_rates() -> None:
    tariff = create_fair_default_tariff(
        base_tariff=_base_tariff(),
        inputs_row=_inputs_row(),
        strategy="fixed_plus_seasonal_mc",
        label="fair_combined",
    )

    item = tariff["items"][0]
    assert item["fixedchargefirstmeter"] == pytest.approx(6.0)
    assert item["energyratestructure"] == [
        [{"rate": 0.24, "adj": 0.0, "unit": "kWh"}],
        [{"rate": 0.36, "adj": 0.0, "unit": "kWh"}],
    ]
    assert item["energyweekdayschedule"][0] == [1] * 24
    assert item["energyweekdayschedule"][6] == [0] * 24


def test_infeasible_seasonal_design_requires_explicit_allow() -> None:
    row = _inputs_row()
    row["seasonal_rates_only_feasible"] = False
    row["seasonal_rates_only_summer_rate"] = -0.1
    row["seasonal_rates_only_clipped_summer_rate"] = 0.0

    with pytest.raises(
        ValueError, match="seasonal_rates_only fair-default design is infeasible"
    ):
        create_fair_default_tariff(
            base_tariff=_base_tariff(),
            inputs_row=row,
            strategy="seasonal_rates_only",
            label="fair_seasonal",
        )

    tariff = create_fair_default_tariff(
        base_tariff=_base_tariff(),
        inputs_row=row,
        strategy="seasonal_rates_only",
        label="fair_seasonal",
        allow_infeasible=True,
    )

    assert tariff["items"][0]["energyratestructure"] == [
        [{"rate": 0.0, "adj": 0.0, "unit": "kWh"}],
        [{"rate": 0.42, "adj": 0.0, "unit": "kWh"}],
    ]
