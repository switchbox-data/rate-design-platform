"""Tests for generate_precalc_mapping utility."""

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from utils.generate_precalc_mapping import generate_default_precalc_mapping


def _create_temp_tariff_file(tariff_structure: dict) -> Path:
    """Create a temporary tariff JSON file and return its path."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(tariff_structure, f)
        return Path(f.name)


class TestGenerateDefaultPrecalcMapping:
    """Test suite for generate_default_precalc_mapping function."""

    def test_flat_tariff_single_period_single_tier(self):
        """Test flat tariff with single period and single tier."""
        tariff = {
            "items": [
                {
                    "label": "flat_test",
                    "energyratestructure": [
                        [{"rate": 0.10, "adj": 0.0, "unit": "kWh"}]
                    ],
                }
            ]
        }
        tariff_path = _create_temp_tariff_file(tariff)

        result = generate_default_precalc_mapping(tariff_path, "flat_test")

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["period", "tier", "rel_value", "tariff"]
        assert len(result) == 1
        assert result.iloc[0]["period"] == 1
        assert result.iloc[0]["tier"] == 1
        assert result.iloc[0]["rel_value"] == 1.0
        assert result.iloc[0]["tariff"] == "flat_test"

        tariff_path.unlink()

    def test_tou_tariff_multiple_periods(self):
        """Test TOU tariff with multiple periods (peak/off-peak)."""
        tariff = {
            "items": [
                {
                    "label": "tou_test",
                    "energyratestructure": [
                        [{"rate": 0.08, "adj": 0.0, "unit": "kWh"}],  # Off-peak
                        [{"rate": 0.12, "adj": 0.0, "unit": "kWh"}],  # Mid-peak
                        [{"rate": 0.20, "adj": 0.0, "unit": "kWh"}],  # On-peak
                    ],
                }
            ]
        }
        tariff_path = _create_temp_tariff_file(tariff)

        result = generate_default_precalc_mapping(tariff_path, "tou_test")

        assert len(result) == 3
        # rel_values should be normalized to min rate (0.08)
        # Off-peak: 0.08/0.08 = 1.0
        # Mid-peak: 0.12/0.08 = 1.5
        # On-peak: 0.20/0.08 = 2.5
        assert result[result["period"] == 1]["rel_value"].iloc[0] == 1.0
        assert result[result["period"] == 2]["rel_value"].iloc[0] == 1.5
        assert result[result["period"] == 3]["rel_value"].iloc[0] == 2.5

        tariff_path.unlink()

    def test_tiered_tariff_multiple_tiers(self):
        """Test tiered tariff with multiple tiers within a single period."""
        tariff = {
            "items": [
                {
                    "label": "tiered_test",
                    "energyratestructure": [
                        [
                            {"rate": 0.10, "adj": 0.0, "unit": "kWh", "max": 500},
                            {"rate": 0.15, "adj": 0.0, "unit": "kWh", "max": 1000},
                            {"rate": 0.20, "adj": 0.0, "unit": "kWh"},
                        ]
                    ],
                }
            ]
        }
        tariff_path = _create_temp_tariff_file(tariff)

        result = generate_default_precalc_mapping(tariff_path, "tiered_test")

        assert len(result) == 3
        # All in period 1, tiers 1, 2, 3 (1-based indexing per PySAM/CAIRO)
        # rel_values: 0.10/0.10=1.0, 0.15/0.10=1.5, 0.20/0.10=2.0
        assert all(result["period"] == 1)
        assert list(result["tier"]) == [1, 2, 3]
        assert list(result["rel_value"]) == [1.0, 1.5, 2.0]

        tariff_path.unlink()

    def test_combined_tou_tiered_tariff(self):
        """Test combined TOU + tiered tariff structure."""
        tariff = {
            "items": [
                {
                    "label": "combined_test",
                    "energyratestructure": [
                        # Off-peak (period 0) with 2 tiers
                        [
                            {"rate": 0.06, "adj": 0.0, "unit": "kWh", "max": 500},
                            {"rate": 0.08, "adj": 0.0, "unit": "kWh"},
                        ],
                        # On-peak (period 1) with 2 tiers
                        [
                            {"rate": 0.12, "adj": 0.0, "unit": "kWh", "max": 500},
                            {"rate": 0.18, "adj": 0.0, "unit": "kWh"},
                        ],
                    ],
                }
            ]
        }
        tariff_path = _create_temp_tariff_file(tariff)

        result = generate_default_precalc_mapping(tariff_path, "combined_test")

        assert len(result) == 4
        # Min rate is 0.06
        # Period 0, tier 0: 0.06/0.06 = 1.0
        # Period 0, tier 1: 0.08/0.06 = 1.3333
        # Period 1, tier 0: 0.12/0.06 = 2.0
        # Period 1, tier 1: 0.18/0.06 = 3.0
        expected = [
            {"period": 1, "tier": 1, "rel_value": 1.0},
            {"period": 1, "tier": 2, "rel_value": round(0.08 / 0.06, 4)},
            {"period": 2, "tier": 1, "rel_value": 2.0},
            {"period": 2, "tier": 2, "rel_value": 3.0},
        ]
        for i, exp in enumerate(expected):
            assert result.iloc[i]["period"] == exp["period"]
            assert result.iloc[i]["tier"] == exp["tier"]
            assert result.iloc[i]["rel_value"] == exp["rel_value"]

        tariff_path.unlink()

    def test_tariff_with_adjustment(self):
        """Test tariff with non-zero adjustment values."""
        tariff = {
            "items": [
                {
                    "label": "adj_test",
                    "energyratestructure": [
                        [{"rate": 0.08, "adj": 0.02, "unit": "kWh"}],  # Effective: 0.10
                        [{"rate": 0.10, "adj": 0.05, "unit": "kWh"}],  # Effective: 0.15
                    ],
                }
            ]
        }
        tariff_path = _create_temp_tariff_file(tariff)

        result = generate_default_precalc_mapping(tariff_path, "adj_test")

        # Effective rates: 0.10 and 0.15
        # rel_values: 0.10/0.10=1.0, 0.15/0.10=1.5
        assert result.iloc[0]["rel_value"] == 1.0
        assert result.iloc[1]["rel_value"] == 1.5

        tariff_path.unlink()

    def test_tariff_without_adjustment_field(self):
        """Test tariff where adj field is missing (should default to 0)."""
        tariff = {
            "items": [
                {
                    "label": "no_adj_test",
                    "energyratestructure": [
                        [{"rate": 0.10, "unit": "kWh"}],
                        [{"rate": 0.15, "unit": "kWh"}],
                    ],
                }
            ]
        }
        tariff_path = _create_temp_tariff_file(tariff)

        result = generate_default_precalc_mapping(tariff_path, "no_adj_test")

        # Should work even without adj field
        assert len(result) == 2
        assert result.iloc[0]["rel_value"] == 1.0
        assert result.iloc[1]["rel_value"] == 1.5

        tariff_path.unlink()

    def test_output_dataframe_columns(self):
        """Test that output DataFrame has correct column structure."""
        tariff = {
            "items": [
                {
                    "label": "columns_test",
                    "energyratestructure": [
                        [{"rate": 0.10, "adj": 0.0, "unit": "kWh"}]
                    ],
                }
            ]
        }
        tariff_path = _create_temp_tariff_file(tariff)

        result = generate_default_precalc_mapping(tariff_path, "columns_test")

        assert "period" in result.columns
        assert "tier" in result.columns
        assert "rel_value" in result.columns
        assert "tariff" in result.columns
        assert pd.api.types.is_integer_dtype(result["period"])
        assert pd.api.types.is_integer_dtype(result["tier"])
        assert pd.api.types.is_float_dtype(result["rel_value"])
        assert pd.api.types.is_string_dtype(result["tariff"])

        tariff_path.unlink()

    def test_tariff_key_propagates(self):
        """Test that tariff key is correctly applied to all rows."""
        tariff = {
            "items": [
                {
                    "label": "key_test",
                    "energyratestructure": [
                        [{"rate": 0.08, "adj": 0.0, "unit": "kWh"}],
                        [{"rate": 0.12, "adj": 0.0, "unit": "kWh"}],
                    ],
                }
            ]
        }
        tariff_path = _create_temp_tariff_file(tariff)

        result = generate_default_precalc_mapping(tariff_path, "my_custom_key")

        assert all(result["tariff"] == "my_custom_key")

        tariff_path.unlink()

    def test_file_not_found(self):
        """Test that FileNotFoundError is raised for missing tariff file."""
        with pytest.raises(FileNotFoundError):
            generate_default_precalc_mapping(Path("/nonexistent/path.json"), "test")
