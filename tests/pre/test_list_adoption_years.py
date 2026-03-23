"""Tests for utils/pre/list_adoption_years.py."""

from __future__ import annotations

import pytest

from utils.pre.list_adoption_years import list_run_years, main


class TestListRunYears:
    def test_all_labels_when_run_years_omitted(self) -> None:
        config = {
            "scenario_name": "test",
            "year_labels": [2025, 2030, 2035],
        }
        assert list_run_years(config) == [2025, 2030, 2035]

    def test_run_years_subset(self) -> None:
        config = {
            "year_labels": [2025, 2030, 2035, 2040],
            "run_years": [2025, 2040],
        }
        assert list_run_years(config) == [2025, 2040]

    def test_run_years_snaps_to_nearest(self) -> None:
        config = {
            "year_labels": [2025, 2030, 2035],
            "run_years": [2027],
        }
        with pytest.warns(UserWarning, match="snapping"):
            result = list_run_years(config)
        assert result == [2025]

    def test_empty_year_labels(self) -> None:
        config: dict = {"year_labels": []}
        assert list_run_years(config) == []

    def test_single_year(self) -> None:
        config = {"year_labels": [2030]}
        assert list_run_years(config) == [2030]

    def test_string_year_labels_normalised(self) -> None:
        config = {"year_labels": ["2025", "2030"]}
        assert list_run_years(config) == [2025, 2030]


class TestMain:
    def test_prints_one_year_per_line(self, tmp_path, capsys) -> None:
        p = tmp_path / "adoption.yaml"
        p.write_text(
            "scenario_name: test\nyear_labels: [2025, 2030, 2035]\n",
            encoding="utf-8",
        )
        main([str(p)])
        out = capsys.readouterr().out.strip().splitlines()
        assert out == ["2025", "2030", "2035"]

    def test_respects_run_years(self, tmp_path, capsys) -> None:
        p = tmp_path / "adoption.yaml"
        p.write_text(
            "scenario_name: test\nyear_labels: [2025, 2030, 2035]\nrun_years: [2030]\n",
            encoding="utf-8",
        )
        main([str(p)])
        out = capsys.readouterr().out.strip().splitlines()
        assert out == ["2030"]

    def test_missing_arg_exits_nonzero(self, capsys) -> None:
        with pytest.raises(SystemExit) as exc:
            main([])
        assert exc.value.code != 0
