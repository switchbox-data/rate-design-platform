"""General-purpose helpers for the ResStock data pipeline."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

import polars as pl
import yaml

_STATE_CONFIGS_PATH = Path(__file__).parent / "state_configs.yaml"


def load_state_configs(path: Path | None = None) -> dict[str, dict]:
    """Load per-state configuration from ``state_configs.yaml``.

    Returns a dict keyed by 2-letter state code (e.g. ``"NY"``, ``"RI"``).
    Each value is a dict of state-specific settings (``state_fips``,
    ``add_vulnerability_columns``, ``utility_assignment``, etc.).  See
    ``state_configs.yaml`` for the schema and which fields each state needs.
    """
    p = path or _STATE_CONFIGS_PATH
    with p.open() as f:
        return yaml.safe_load(f)


def parse_bool(v: str) -> bool:
    """Parse a string argument as a boolean for use with ``argparse``.

    Accepts ``true``/``1``/``yes`` and ``false``/``0``/``no`` (case-insensitive).
    Raises ``argparse.ArgumentTypeError`` for any other value.
    """
    if v.lower() in ("true", "1", "yes"):
        return True
    if v.lower() in ("false", "0", "no"):
        return False
    raise argparse.ArgumentTypeError(f"Expected a boolean (True/False), got '{v}'")


def upload(
    state: list[str],
    file_types: list[str],
    release: str,
    path_output_dir: str | Path,
    path_s3_dir: str,
) -> None:
    """Sync fetched/modified local files to S3 (data only, not the manifest)."""
    local_base = Path(path_output_dir) / release
    s3_base = f"{path_s3_dir.rstrip('/')}/{release}"

    for file_type in file_types:
        for s in state:
            local_path = local_base / file_type / f"state={s}"
            s3_path = f"{s3_base}/{file_type}/state={s}/"
            print(f"Uploading {local_path} → {s3_path}", flush=True)
            result = subprocess.run(
                ["aws", "s3", "sync", str(local_path), s3_path],
                check=False,
            )
            if result.returncode != 0:
                print(
                    f"  WARNING: aws s3 sync exited with code {result.returncode}",
                    flush=True,
                )


def upload_manifest(local_dir: Path, s3_base: str) -> None:
    """Push the manifest to S3 after the run record has been finalized on disk."""
    local_manifest = local_dir / "manifest.yaml"
    s3_manifest = f"{s3_base}/manifest.yaml"
    if not local_manifest.exists():
        return
    print(f"Uploading manifest {local_manifest} → {s3_manifest}", flush=True)
    result = subprocess.run(
        ["aws", "s3", "cp", str(local_manifest), s3_manifest],
        check=False,
    )
    if result.returncode != 0:
        print(
            f"  WARNING: manifest upload exited with code {result.returncode}",
            flush=True,
        )


def select_puma_and_heating_fuel_metadata(metadata: pl.LazyFrame) -> pl.LazyFrame:
    """Select PUMA and heating-fuel columns from ResStock metadata.

    Expects columns ``bldg_id``, ``in.puma``, ``in.heating_fuel``, and
    ``has_natgas_connection`` (added by ``identify_natgas_connection``).

    Returns a LazyFrame with four columns:

    - ``bldg_id``
    - ``puma`` — last 5 characters of ``in.puma`` (zero-padded Census PUMA ID)
    - ``heating_fuel`` — alias of ``in.heating_fuel``
    - ``has_natgas_connection``
    """
    if "has_natgas_connection" not in metadata.collect_schema().names():
        raise ValueError(
            "Missing required column 'has_natgas_connection'. "
            "Run identify_natgas_connection first to add this column."
        )

    return metadata.select(
        pl.col("bldg_id"),
        pl.col("in.puma").str.slice(-5).alias("puma"),
        pl.col("in.heating_fuel").alias("heating_fuel"),
        pl.col("has_natgas_connection"),
    )
