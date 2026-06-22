"""General-purpose file I/O helpers for reading data from S3 and local paths."""

from __future__ import annotations

import io
import itertools
from pathlib import Path
from typing import Any

import boto3
import polars as pl


def read_csv_from_s3(s3_path: str, **kwargs: Any) -> pl.DataFrame:
    """Read a CSV file from S3 into a Polars DataFrame.

    Uses boto3 for the S3 download so that it works with the standard AWS
    credential chain (environment variables, instance profile, SSO profile)
    without needing to pass storage options to Polars.

    Parameters
    ----------
    s3_path:
        Full S3 URI, e.g. ``"s3://data.sb/pjm/lmp/real_time/rt_hrl_lmps.csv"``.
    **kwargs:
        Forwarded verbatim to :func:`polars.read_csv` (e.g.
        ``infer_schema_length``, ``schema_overrides``, ``separator``).

    Returns
    -------
    pl.DataFrame
    """
    path = s3_path.removeprefix("s3://")
    bucket, key = path.split("/", 1)
    s3 = boto3.client("s3")
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    kwargs.setdefault("infer_schema_length", 10_000)
    return pl.read_csv(io.BytesIO(raw), **kwargs)


def read_parquet_from_s3(s3_path: str, **kwargs: Any) -> pl.DataFrame:
    """Read a single Parquet file from S3 into a Polars DataFrame.

    Parameters
    ----------
    s3_path:
        Full S3 URI, e.g. ``"s3://data.sb/pjm/capacity/rpm/rpm_capacity_prices.parquet"``.
    **kwargs:
        Forwarded verbatim to :func:`polars.read_parquet`.

    Returns
    -------
    pl.DataFrame
    """
    path = s3_path.removeprefix("s3://")
    bucket, key = path.split("/", 1)
    s3 = boto3.client("s3")
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pl.read_parquet(io.BytesIO(raw), **kwargs)


def write_parquet_to_s3(df: pl.DataFrame, s3_path: str, **kwargs: Any) -> None:
    """Write a Polars DataFrame as a single Parquet file to S3.

    Parameters
    ----------
    df:
        DataFrame to write.
    s3_path:
        Full S3 URI, e.g. ``"s3://data.sb/pjm/lmp/real_time/zones/zone=BGE/year=2025/data.parquet"``.
    **kwargs:
        Forwarded verbatim to :func:`polars.DataFrame.write_parquet`.
    """
    path = s3_path.removeprefix("s3://")
    bucket, key = path.split("/", 1)
    buf = io.BytesIO()
    df.write_parquet(buf, **kwargs)
    buf.seek(0)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def write_hive_partitioned_parquet_to_s3(
    df: pl.DataFrame,
    s3_base: str,
    partition_cols: list[str],
    *,
    filename: str = "data.parquet",
    drop_partition_cols: bool = True,
    dry_run: bool = False,
    **kwargs: Any,
) -> None:
    """Write a Polars DataFrame to S3 as a Hive-partitioned Parquet dataset.

    Iterates over every combination of values present in *partition_cols*,
    builds a path of the form::

        s3://<base>/col1={v1}/col2={v2}/.../data.parquet

    and uploads each partition as a separate Parquet file.

    Parameters
    ----------
    df:
        DataFrame to partition and upload.
    s3_base:
        Root S3 prefix (no trailing slash), e.g.
        ``"s3://data.sb/pjm/lmp/real_time/zones"``.
    partition_cols:
        Ordered list of column names to partition on, e.g.
        ``["zone", "year"]``.
    filename:
        Parquet filename within each partition directory (default
        ``"data.parquet"``).
    drop_partition_cols:
        If ``True`` (default), the partition columns are dropped from
        the written Parquet file (standard Hive convention — their values
        are encoded in the path).
    dry_run:
        If ``True``, print the would-be S3 paths without writing anything.
    **kwargs:
        Forwarded to :meth:`polars.DataFrame.write_parquet`.
    """
    s3 = boto3.client("s3")
    base = s3_base.rstrip("/").removeprefix("s3://")
    bucket, prefix = base.split("/", 1)

    unique_values = [sorted(df[col].unique().to_list()) for col in partition_cols]

    for combo in itertools.product(*unique_values):
        filter_expr = pl.lit(True)
        for col, val in zip(partition_cols, combo, strict=True):
            filter_expr = filter_expr & (pl.col(col) == val)

        partition = df.filter(filter_expr)
        if partition.is_empty():
            continue

        if drop_partition_cols:
            partition = partition.drop(partition_cols)

        hive_path = "/".join(
            f"{col}={val}" for col, val in zip(partition_cols, combo, strict=True)
        )
        key = f"{prefix}/{hive_path}/{filename}"

        if dry_run:
            print(
                f"  [dry-run] would write {partition.height:,} rows → s3://{bucket}/{key}"
            )
            continue

        buf = io.BytesIO()
        partition.write_parquet(buf, **kwargs)
        buf.seek(0)
        s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
        print(f"  Uploaded {partition.height:,} rows → s3://{bucket}/{key}")


def read_csv_local_or_s3(path: str | Path, **kwargs: Any) -> pl.DataFrame:
    """Read a CSV from either a local path or an S3 URI.

    Parameters
    ----------
    path:
        Local filesystem path or full S3 URI (``s3://...``).
    **kwargs:
        Forwarded to :func:`polars.read_csv` / :func:`read_csv_from_s3`.
    """
    s = str(path)
    if s.startswith("s3://"):
        return read_csv_from_s3(s, **kwargs)
    kwargs.setdefault("infer_schema_length", 10_000)
    return pl.read_csv(s, **kwargs)
