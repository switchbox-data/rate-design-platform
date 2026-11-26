"""Tests for reading a parquet from a local path or from S3."""

from __future__ import annotations

import polars as pl

from utils.file_io import read_parquet_local_or_s3


def test_read_parquet_local(tmp_path) -> None:
    path = tmp_path / "local.parquet"
    pl.DataFrame({"x": [1, 2]}).write_parquet(path)

    out = read_parquet_local_or_s3(path)

    assert out["x"].to_list() == [1, 2]


def test_read_parquet_s3_delegates(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_read(path: str, **kwargs: object) -> pl.DataFrame:
        seen["path"] = path
        seen["kwargs"] = kwargs
        return pl.DataFrame({"x": [3]})

    monkeypatch.setattr("utils.file_io.read_parquet_from_s3", fake_read)

    out = read_parquet_local_or_s3("s3://bucket/key.parquet", columns=["x"])

    assert out["x"].to_list() == [3]
    assert seen["path"] == "s3://bucket/key.parquet"
    assert seen["kwargs"] == {"columns": ["x"]}
