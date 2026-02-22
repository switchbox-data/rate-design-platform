"""Tests for pre-step revenue requirement YAML creation."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import yaml

from utils.pre.create_revenue_yaml import create_revenue_yaml


def _write_revenue_csv(tmp_path: Path, rows: list[dict[str, object]]) -> Path:
    path_csv = tmp_path / "revenue_requirements.csv"
    pl.DataFrame(rows).write_csv(path_csv)
    return path_csv


def test_create_revenue_yaml_writes_expected_payload(tmp_path: Path) -> None:
    path_csv = _write_revenue_csv(
        tmp_path,
        rows=[
            {"utility_name": "rie", "run_year": 2025, "revenue_requirement": "12345.5"},
            {"utility_name": "foo", "run_year": 2025, "revenue_requirement": 1},
        ],
    )
    path_output = tmp_path / "rev_requirement" / "rie.yaml"

    out = create_revenue_yaml(
        path_revenue_csv=str(path_csv),
        path_output_yaml=path_output,
        utility="RIE",
        year="2025",
        utility_col="utility_name",
        year_col="run_year",
    )

    assert out == path_output
    payload = yaml.safe_load(path_output.read_text(encoding="utf-8"))
    assert payload["utility"] == "rie"
    assert payload["revenue_requirement"] == pytest.approx(12345.5)
    assert "revenue_requirement" in payload["source"]


def test_create_revenue_yaml_requires_exact_revenue_column_name(tmp_path: Path) -> None:
    path_csv = _write_revenue_csv(
        tmp_path,
        rows=[{"utility": "rie", "year": 2025, "revenue-requirement": 23456.0}],
    )

    with pytest.raises(ValueError, match="Missing required column 'revenue_requirement'"):
        create_revenue_yaml(
            path_revenue_csv=str(path_csv),
            path_output_yaml=tmp_path / "rie.yaml",
            utility="rie",
            year="2025",
            utility_col="utility",
            year_col="year",
        )


def test_create_revenue_yaml_raises_when_no_match(tmp_path: Path) -> None:
    path_csv = _write_revenue_csv(
        tmp_path,
        rows=[{"utility": "rie", "year": 2024, "revenue_requirement": 100.0}],
    )

    with pytest.raises(ValueError, match="Expected exactly one revenue requirement row"):
        create_revenue_yaml(
            path_revenue_csv=str(path_csv),
            path_output_yaml=tmp_path / "rie.yaml",
            utility="rie",
            year=2025,
            utility_col="utility",
            year_col="year",
        )


def test_create_revenue_yaml_raises_when_multiple_matches(tmp_path: Path) -> None:
    path_csv = _write_revenue_csv(
        tmp_path,
        rows=[
            {"utility": "rie", "year": 2025, "revenue_requirement": 100.0},
            {"utility": "RIE", "year": "2025", "revenue_requirement": 101.0},
        ],
    )

    with pytest.raises(ValueError, match="Expected exactly one revenue requirement row"):
        create_revenue_yaml(
            path_revenue_csv=str(path_csv),
            path_output_yaml=tmp_path / "rie.yaml",
            utility="rie",
            year="2025",
            utility_col="utility",
            year_col="year",
        )


def test_create_revenue_yaml_raises_for_bad_revenue_value(tmp_path: Path) -> None:
    path_csv = _write_revenue_csv(
        tmp_path,
        rows=[{"utility": "rie", "year": 2025, "revenue_requirement": "abc"}],
    )

    with pytest.raises(ValueError, match="Invalid revenue requirement value"):
        create_revenue_yaml(
            path_revenue_csv=str(path_csv),
            path_output_yaml=tmp_path / "rie.yaml",
            utility="rie",
            year=2025,
            utility_col="utility",
            year_col="year",
        )
