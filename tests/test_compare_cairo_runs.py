"""Tests for utils.post.compare_cairo_runs."""

from __future__ import annotations

import polars as pl
import pytest

from utils.post.compare_cairo_runs import ComparisonResult, _numeric_cols, compare_artifact


@pytest.fixture()
def _patch_s3_reader(monkeypatch: pytest.MonkeyPatch) -> dict[str, bytes]:
    """Patch _read_csv_from_s3 to return in-memory CSVs keyed by S3 URI."""
    store: dict[str, bytes] = {}

    def _fake_read(s3_dir: str, rel_path: str) -> pl.DataFrame | None:
        uri = f"{s3_dir.rstrip('/')}/{rel_path}"
        if uri not in store:
            return None
        return pl.read_csv(store[uri])

    monkeypatch.setattr(
        "utils.post.compare_cairo_runs._read_csv_from_s3", _fake_read
    )
    return store


def _df_to_csv_bytes(df: pl.DataFrame) -> bytes:
    return df.write_csv().encode()


class TestNumericCols:
    def test_identifies_numeric_types(self) -> None:
        df = pl.DataFrame(
            {"a": [1], "b": [1.0], "c": ["x"], "d": [True], "e": pl.Series([1], dtype=pl.Int32)}
        )
        result = _numeric_cols(df, exclude=[])
        assert "a" in result
        assert "b" in result
        assert "e" in result
        assert "c" not in result

    def test_excludes_specified(self) -> None:
        df = pl.DataFrame({"a": [1], "b": [2]})
        result = _numeric_cols(df, exclude=["a"])
        assert "a" not in result
        assert "b" in result


class TestCompareArtifact:
    def test_identical_data_passes(self, _patch_s3_reader: dict[str, bytes]) -> None:
        store = _patch_s3_reader
        df = pl.DataFrame({"bldg_id": [1, 2, 3], "bill": [100.0, 200.0, 300.0]})
        csv_bytes = _df_to_csv_bytes(df)
        store["s3://base/bat.csv"] = csv_bytes
        store["s3://chal/bat.csv"] = csv_bytes

        result = compare_artifact(
            "s3://base", "s3://chal", "bat", "bat.csv", ["bldg_id"]
        )
        assert result.passed
        assert result.max_abs_diff == 0.0
        assert result.mismatched_columns == []

    def test_small_diff_within_tolerance_passes(
        self, _patch_s3_reader: dict[str, bytes]
    ) -> None:
        store = _patch_s3_reader
        base_df = pl.DataFrame({"bldg_id": [1, 2], "bill": [100.0, 200.0]})
        chal_df = pl.DataFrame({"bldg_id": [1, 2], "bill": [100.0 + 1e-13, 200.0 - 1e-13]})
        store["s3://base/bat.csv"] = _df_to_csv_bytes(base_df)
        store["s3://chal/bat.csv"] = _df_to_csv_bytes(chal_df)

        result = compare_artifact(
            "s3://base", "s3://chal", "bat", "bat.csv", ["bldg_id"],
            rtol=1e-9, atol=1e-12,
        )
        assert result.passed

    def test_large_diff_fails(self, _patch_s3_reader: dict[str, bytes]) -> None:
        store = _patch_s3_reader
        base_df = pl.DataFrame({"bldg_id": [1, 2], "bill": [100.0, 200.0]})
        chal_df = pl.DataFrame({"bldg_id": [1, 2], "bill": [110.0, 200.0]})
        store["s3://base/bat.csv"] = _df_to_csv_bytes(base_df)
        store["s3://chal/bat.csv"] = _df_to_csv_bytes(chal_df)

        result = compare_artifact(
            "s3://base", "s3://chal", "bat", "bat.csv", ["bldg_id"],
        )
        assert not result.passed
        assert "bill" in result.mismatched_columns

    def test_both_missing_passes(self, _patch_s3_reader: dict[str, bytes]) -> None:
        result = compare_artifact(
            "s3://base", "s3://chal", "elasticity", "tracker.csv", ["bldg_id"],
        )
        assert result.passed
        assert result.error is not None
        assert "Both missing" in result.error

    def test_row_count_mismatch_fails(
        self, _patch_s3_reader: dict[str, bytes]
    ) -> None:
        store = _patch_s3_reader
        base_df = pl.DataFrame({"bldg_id": [1, 2, 3], "bill": [100.0, 200.0, 300.0]})
        chal_df = pl.DataFrame({"bldg_id": [1, 2], "bill": [100.0, 200.0]})
        store["s3://base/bat.csv"] = _df_to_csv_bytes(base_df)
        store["s3://chal/bat.csv"] = _df_to_csv_bytes(chal_df)

        result = compare_artifact(
            "s3://base", "s3://chal", "bat", "bat.csv", ["bldg_id"],
        )
        assert not result.passed
