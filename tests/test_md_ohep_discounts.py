"""Unit tests for Maryland FY26 OHEP benefit assignment and application."""

from __future__ import annotations

from types import ModuleType

import polars as pl
import pytest

from utils.post import build_master_bills, build_master_bills_prefect
from utils.post.apply_md_ohep_to_master_bills import (
    _apply_md_ohep_benefits,
    _sample_md_participation,
    _validate_md_ohep,
    primary_heating_fuel_expr,
)
from utils.post.lmi_common import (
    assign_md_eusp_kwh_band_expr,
    assign_md_ohep_level_expr,
    get_md_eusp_benefits_df,
    get_md_meap_benefits_df,
    load_md_ohep_config,
)

MONTHS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
    "Annual",
]


def test_load_md_ohep_config() -> None:
    config = load_md_ohep_config()
    assert config["program_year"] == "fy26"
    assert [level["level"] for level in config["poverty_levels"]] == [1, 2, 3, 4, 5]
    assert [level["level"] for level in config["excluded_levels"]] == [6, 7]


def test_assign_md_ohep_level_boundaries() -> None:
    df = pl.DataFrame(
        {
            "fpl_pct": [
                None,
                -1.0,
                0.0,
                25.0,
                25.01,
                50.0,
                50.01,
                100.0,
                100.01,
                150.0,
                150.01,
                200.0,
                200.01,
            ]
        }
    )
    result = df.with_columns(assign_md_ohep_level_expr("fpl_pct").alias("level"))
    assert result["level"].to_list() == [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 0]


def test_assign_md_eusp_kwh_band_boundaries() -> None:
    df = pl.DataFrame(
        {
            "annual_kwh": [
                None,
                -1.0,
                0.0,
                4000.0,
                4000.01,
                8000.0,
                8000.01,
                12000.0,
                12000.01,
            ]
        }
    )
    result = df.with_columns(assign_md_eusp_kwh_band_expr("annual_kwh").alias("band"))
    assert result["band"].to_list() == [0, 0, 1, 1, 2, 2, 3, 3, 4]


def test_meap_source_matrix_cells() -> None:
    benefits = get_md_meap_benefits_df()
    l1_gas = benefits.filter(
        (pl.col("ohep_poverty_level") == 1) & (pl.col("primary_heating_fuel") == "gas")
    )
    l5_oil = benefits.filter(
        (pl.col("ohep_poverty_level") == 5)
        & (pl.col("primary_heating_fuel") == "oil_kerosene")
    )
    assert l1_gas["meap_annual_benefit"].item() == 550.0
    assert l5_oil["meap_annual_benefit"].item() == 650.0
    assert benefits.height == 25


def test_eusp_source_matrix_cells() -> None:
    benefits = get_md_eusp_benefits_df()
    l1_electric_band4 = benefits.filter(
        (pl.col("ohep_poverty_level") == 1)
        & (pl.col("primary_heating_fuel") == "electric")
        & (pl.col("eusp_kwh_band") == 4)
    )
    l5_gas_band1 = benefits.filter(
        (pl.col("ohep_poverty_level") == 5)
        & (pl.col("primary_heating_fuel") == "gas")
        & (pl.col("eusp_kwh_band") == 1)
    )
    assert l1_electric_band4["eusp_annual_benefit"].item() == 1000.0
    assert l5_gas_band1["eusp_annual_benefit"].item() == 175.0
    assert benefits.height == 100


def test_primary_heating_fuel_mapping_and_hp_override() -> None:
    df = pl.DataFrame(
        {
            "postprocess_group.has_hp": [True, False, False, False, False, False],
            "in.heating_fuel": [
                "Natural Gas",
                "Electricity",
                "Natural Gas",
                "Fuel Oil",
                "Propane",
                "Other Fuel",
            ],
        }
    ).with_columns(primary_heating_fuel_expr().alias("fuel"))
    assert df["fuel"].to_list() == [
        "electric",
        "electric",
        "gas",
        "oil_kerosene",
        "propane",
        "wood_coal",
    ]


def _make_enriched_master(
    *,
    bldg_id: int,
    fuel: str,
    monthly_elec: float,
    monthly_gas: float,
    monthly_oil: float,
    monthly_propane: float,
    meap: float,
    eusp: float,
    participates: bool = True,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for month in MONTHS:
        multiplier = 12.0 if month == "Annual" else 1.0
        rows.append(
            {
                "bldg_id": bldg_id,
                "month": month,
                "elec_total_bill": monthly_elec * multiplier,
                "gas_total_bill": monthly_gas * multiplier,
                "oil_total_bill": monthly_oil * multiplier,
                "propane_total_bill": monthly_propane * multiplier,
                "energy_total_bill": (
                    monthly_elec + monthly_gas + monthly_oil + monthly_propane
                )
                * multiplier,
                "primary_heating_fuel": fuel,
                "meap_annual_benefit": meap,
                "eusp_annual_benefit": eusp,
                "participates": participates,
                "is_lmi_elec": eusp > 0 or (fuel == "electric" and meap > 0),
            }
        )
    return pl.DataFrame(rows)


def test_apply_electric_heat_stacks_meap_and_eusp() -> None:
    master = _make_enriched_master(
        bldg_id=1,
        fuel="electric",
        monthly_elec=200.0,
        monthly_gas=0.0,
        monthly_oil=0.0,
        monthly_propane=0.0,
        meap=100.0,
        eusp=1000.0,
    )
    result = _apply_md_ohep_benefits(master, 100, keep_component_columns=True)
    annual = result.filter(pl.col("month") == "Annual")
    assert annual["elec_total_bill_lmi_100"].item() == pytest.approx(1300.0)
    assert annual["energy_total_bill_lmi_100"].item() == pytest.approx(1300.0)
    assert annual["meap_annual_credit_100"].item() == 100.0
    assert annual["eusp_annual_credit_100"].item() == 1000.0
    _validate_md_ohep(result, 100, 1.0)


def test_meap_and_eusp_can_be_toggled_independently() -> None:
    master = _make_enriched_master(
        bldg_id=1,
        fuel="electric",
        monthly_elec=200.0,
        monthly_gas=0.0,
        monthly_oil=0.0,
        monthly_propane=0.0,
        meap=100.0,
        eusp=1000.0,
    )
    eusp_only = _apply_md_ohep_benefits(
        master,
        100,
        keep_component_columns=True,
        include_meap=False,
    )
    meap_only = _apply_md_ohep_benefits(
        master,
        100,
        keep_component_columns=True,
        include_eusp=False,
    )
    assert eusp_only.filter(pl.col("month") == "Annual")[
        "elec_total_bill_lmi_100"
    ].item() == pytest.approx(1400.0)
    assert meap_only.filter(pl.col("month") == "Annual")[
        "elec_total_bill_lmi_100"
    ].item() == pytest.approx(2300.0)
    assert eusp_only["meap_annual_credit_100"].sum() == 0.0
    assert meap_only["eusp_annual_credit_100"].sum() == 0.0


def test_apply_gas_heat_splits_meap_and_eusp_by_bill() -> None:
    master = _make_enriched_master(
        bldg_id=2,
        fuel="gas",
        monthly_elec=100.0,
        monthly_gas=100.0,
        monthly_oil=0.0,
        monthly_propane=0.0,
        meap=550.0,
        eusp=350.0,
    )
    result = _apply_md_ohep_benefits(master, 100, keep_component_columns=False)
    annual = result.filter(pl.col("month") == "Annual")
    assert annual["elec_total_bill_lmi_100"].item() == pytest.approx(850.0)
    assert annual["gas_total_bill_lmi_100"].item() == pytest.approx(650.0)
    assert annual["energy_total_bill_lmi_100"].item() == pytest.approx(1500.0)


def test_credit_exceeds_annual_bill_zeros_all_months() -> None:
    """When annual credit > annual bill, all months go to $0."""
    master = _make_enriched_master(
        bldg_id=3,
        fuel="electric",
        monthly_elec=20.0,
        monthly_gas=0.0,
        monthly_oil=0.0,
        monthly_propane=0.0,
        meap=100.0,
        eusp=1000.0,
    )
    result = _apply_md_ohep_benefits(master, 100, keep_component_columns=False)
    assert result["elec_total_bill_lmi_100"].min() == 0.0
    assert (
        result.filter(pl.col("month") == "Annual")["elec_total_bill_lmi_100"].item()
        == 0.0
    )


def test_proportional_allocation_preserves_full_grant() -> None:
    """Non-uniform monthly bills: full grant is consumed, no credit is lost.

    With equal-12ths: monthly credit = $1100/12 ≈ $91.67. In months where the
    bill ($50) < credit, the excess is lost. Annual discounted would be < $500.

    With proportional allocation: fraction_remaining = (1200-1100)/1200 ≈ 0.0833.
    Each month's discounted bill = month_bill × 0.0833. Annual = $100.
    The full $1100 grant is applied.
    """
    months_bills = [50, 50, 50, 150, 150, 150, 150, 150, 100, 100, 50, 50]
    annual_total = sum(months_bills)
    assert annual_total == 1200

    rows: list[dict[str, object]] = []
    for i, month in enumerate(MONTHS[:-1]):
        rows.append(
            {
                "bldg_id": 99,
                "month": month,
                "elec_total_bill": float(months_bills[i]),
                "gas_total_bill": 0.0,
                "oil_total_bill": 0.0,
                "propane_total_bill": 0.0,
                "energy_total_bill": float(months_bills[i]),
                "primary_heating_fuel": "electric",
                "meap_annual_benefit": 100.0,
                "eusp_annual_benefit": 1000.0,
                "participates": True,
                "is_lmi_elec": True,
            }
        )
    rows.append(
        {
            "bldg_id": 99,
            "month": "Annual",
            "elec_total_bill": float(annual_total),
            "gas_total_bill": 0.0,
            "oil_total_bill": 0.0,
            "propane_total_bill": 0.0,
            "energy_total_bill": float(annual_total),
            "primary_heating_fuel": "electric",
            "meap_annual_benefit": 100.0,
            "eusp_annual_benefit": 1000.0,
            "participates": True,
            "is_lmi_elec": True,
        }
    )
    master = pl.DataFrame(rows)

    result = _apply_md_ohep_benefits(master, 100, keep_component_columns=False)
    annual = result.filter(pl.col("month") == "Annual")

    # Full $1100 credit consumed: annual discounted = 1200 - 1100 = $100
    assert annual["elec_total_bill_lmi_100"].item() == pytest.approx(100.0)
    # No monthly bill is negative
    monthly = result.filter(pl.col("month") != "Annual")
    assert monthly.filter(pl.col("elec_total_bill_lmi_100") < 0.0).height == 0
    # Each month is proportionally reduced (same fraction)
    fraction = 100.0 / 1200.0
    for i, month in enumerate(MONTHS[:-1]):
        row = result.filter(pl.col("month") == month)
        expected = months_bills[i] * fraction
        assert row["elec_total_bill_lmi_100"].item() == pytest.approx(
            expected, abs=1e-6
        )

    _validate_md_ohep(result, 100, 1.0)


def test_nonparticipant_bills_are_unchanged() -> None:
    master = _make_enriched_master(
        bldg_id=4,
        fuel="propane",
        monthly_elec=100.0,
        monthly_gas=0.0,
        monthly_oil=0.0,
        monthly_propane=100.0,
        meap=1000.0,
        eusp=350.0,
        participates=False,
    )
    result = _apply_md_ohep_benefits(master, 0, keep_component_columns=True)
    assert (
        result["elec_total_bill_lmi_0"].to_list() == result["elec_total_bill"].to_list()
    )
    assert (
        result["propane_total_bill_lmi_0"].to_list()
        == result["propane_total_bill"].to_list()
    )
    assert result["meap_annual_credit_0"].sum() == 0.0
    assert result["eusp_annual_credit_0"].sum() == 0.0


def test_electric_meap_without_eusp_still_discounts_electric() -> None:
    master = _make_enriched_master(
        bldg_id=5,
        fuel="electric",
        monthly_elec=100.0,
        monthly_gas=0.0,
        monthly_oil=0.0,
        monthly_propane=0.0,
        meap=120.0,
        eusp=0.0,
    )
    result = _apply_md_ohep_benefits(master, 100, keep_component_columns=True)
    annual = result.filter(pl.col("month") == "Annual")
    assert annual["elec_total_bill_lmi_100"].item() == pytest.approx(1080.0)
    assert annual["applied_discount_elec_100"].item() is True
    assert annual["meap_annual_credit_100"].item() == 120.0
    assert annual["eusp_annual_credit_100"].item() == 0.0
    _validate_md_ohep(result, 100, 1.0)


def test_unknown_participation_mode_raises() -> None:
    profiles = pl.DataFrame({"bldg_id": [1], "is_lmi_any": [True], "fpl_pct": [50.0]})
    with pytest.raises(ValueError, match="participation_mode"):
        _sample_md_participation(profiles, 0.4, "coin-flip", 42)


def test_sampling_never_selects_ineligible_profiles() -> None:
    profiles = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "is_lmi_any": [True, False],
            "fpl_pct": [50.0, 250.0],
        }
    )
    sampled = _sample_md_participation(profiles, 1.0, "uniform", 42)
    assert sampled["participates"].to_list() == [True, False]


def test_weighted_sampling_handles_no_eligible_profiles() -> None:
    profiles = pl.DataFrame(
        {
            "bldg_id": [1, 2],
            "is_lmi_any": [False, False],
            "fpl_pct": [250.0, 300.0],
        }
    )
    sampled = _sample_md_participation(profiles, 0.4, "weighted", 42)
    assert sampled["participates"].to_list() == [False, False]


@pytest.mark.parametrize("builder", [build_master_bills, build_master_bills_prefect])
def test_master_builder_dispatches_md_ohep(
    builder: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    master = pl.DataFrame({"bldg_id": [1]})
    expected = pl.DataFrame({"bldg_id": [1], "md_ohep_called": [True]})
    captured: dict[str, object] = {}

    def fake_apply(master_arg: pl.DataFrame, **kwargs: object) -> pl.DataFrame:
        captured["master"] = master_arg
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(builder, "apply_md_ohep_to_master", fake_apply)
    monkeypatch.setattr(builder, "get_aws_storage_options", lambda: {"region": "x"})

    result = builder._apply_lmi_discounts_to_master(
        master,
        state_upper="MD",
        utilities=["bge"],
        upgrade="00",
        path_resstock_release="/resstock",
        lmi_fpl_year=2025,
        lmi_cpi_s3_path="s3://cpi/",
        lmi_participation_rates=[1.0, 0.4],
        lmi_participation_mode="weighted",
        lmi_seed=42,
        lmi_calculation_type="monthly",
    )

    assert result.equals(expected)
    assert captured["master"] is master
    assert captured["state"] == "MD"
    assert captured["upgrade"] == "00"
    assert captured["participation_rates"] == [1.0, 0.4]
    assert captured["opts"] == {"region": "x"}
