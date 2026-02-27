"""Copy ResStock data between releases (S3 or local)."""

import argparse
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import boto3


def _get_s3_bucket_and_prefix(s3_uri: str) -> tuple[str, str]:
    """Return (bucket, key_prefix) from s3://bucket/key/prefix."""
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    return bucket, prefix


def copy_dir(
    source_dir: str | Path,
    dest_dir: str | Path,
    *,
    max_workers: int = 32,
) -> int:
    """
    Copy all files from source directory to destination directory.
    Returns the number of files copied.

    Supports:
    - S3: pass s3://bucket/prefix/ for source_dir and dest_dir (same bucket).
    - Local: pass local paths as str or Path.
    """
    src = str(source_dir).rstrip("/")
    dst = str(dest_dir).rstrip("/")

    if src.startswith("s3://") and dst.startswith("s3://"):
        return _copy_s3_dir_to_s3(src, dst, max_workers=max_workers)
    if not src.startswith("s3://") and not dst.startswith("s3://"):
        return _copy_local_dir(Path(src), Path(dst))
    raise ValueError(
        "Source and destination must both be S3 URIs or both be local paths"
    )


def _copy_s3_dir_to_s3(
    source_dir: str,
    dest_dir: str,
    *,
    max_workers: int = 32,
) -> int:
    """Copy all objects under source prefix to dest prefix (server-side). Same bucket."""
    bucket_src, source_prefix = _get_s3_bucket_and_prefix(source_dir)
    bucket_dest, dest_prefix = _get_s3_bucket_and_prefix(dest_dir)
    if bucket_src != bucket_dest:
        raise ValueError("Source and destination must use the same S3 bucket")
    if not source_prefix.endswith("/"):
        source_prefix += "/"
    if not dest_prefix.endswith("/"):
        dest_prefix += "/"

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    to_copy: list[tuple[str, str]] = []

    for page in paginator.paginate(Bucket=bucket_src, Prefix=source_prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if key.endswith("/"):
                continue
            dest_key = dest_prefix + key[len(source_prefix) :]
            to_copy.append((key, dest_key))

    def copy_one(args: tuple[str, str]) -> None:
        src_key, d_key = args
        s3.copy_object(
            CopySource={"Bucket": bucket_src, "Key": src_key},
            Bucket=bucket_src,
            Key=d_key,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(copy_one, to_copy))

    return len(to_copy)


def _copy_local_dir(source_dir: Path, dest_dir: Path) -> int:
    """Copy all files from source_dir to dest_dir (local). Creates dest parents as needed."""
    if not source_dir.is_dir():
        return 0
    count = 0
    for src_file in source_dir.rglob("*"):
        if not src_file.is_file():
            continue
        try:
            rel = src_file.relative_to(source_dir)
        except ValueError:
            continue
        dest_file = dest_dir / rel
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_file, dest_file)
        count += 1
    return count


def _copy_local_resstock_prefix(
    base_path: Path,
    release_from: str,
    release_to: str,
    file_type: str,
    upgrade_id: str,
) -> int:
    """
    Copy files from base_path/release_from/file_type/state=*/upgrade={upgrade_id}/...
    to base_path/release_to/file_type/state=*/upgrade={upgrade_id}/...
    Uses copy_dir for each state subdir so one code path handles the copy.
    """
    source_base = base_path / release_from / file_type
    dest_base = base_path / release_to / file_type
    if not source_base.is_dir():
        return 0
    upgrade_dir = f"upgrade={upgrade_id}"
    total = 0
    for state_dir in source_base.iterdir():
        if not state_dir.is_dir():
            continue
        source_dir = state_dir / upgrade_dir
        if not source_dir.is_dir():
            continue
        dest_dir = dest_base / state_dir.name / upgrade_dir
        total += copy_dir(source_dir, dest_dir)
    return total


def copy_resstock_data(
    data_path: str,
    release_from: str,
    release_to: str,
    state: str,
    upgrade_id: str,
    file_type: str,
) -> int:
    """
    Copy ResStock data for one file_type and one upgrade_id from one release to another.
    Supports S3 (s3://...) or local paths. Structure: release/file_type/state={state}/upgrade={upgrade_id}/.
    Returns number of objects/files copied.
    """
    if data_path.startswith("s3://"):
        bucket, base_prefix = _get_s3_bucket_and_prefix(data_path.rstrip("/"))
        source_dir = f"s3://{bucket}/{base_prefix}/{release_from}/{file_type}/state={state}/upgrade={upgrade_id}/"
        dest_dir = f"s3://{bucket}/{base_prefix}/{release_to}/{file_type}/state={state}/upgrade={upgrade_id}/"
        return copy_dir(source_dir, dest_dir)

    else:
        base_path = Path(data_path.rstrip("/"))
        if not base_path.is_dir():
            raise FileNotFoundError(f"Data path is not a directory: {base_path}")
        return _copy_local_resstock_prefix(
            base_path, release_from, release_to, file_type, upgrade_id
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy ResStock data between releases.")
    parser.add_argument(
        "--data_path",
        required=True,
        help="S3 or local base path (e.g. s3://data.sb/nrel/resstock)",
    )
    parser.add_argument(
        "--release_from",
        required=True,
        help="Source release name",
    )
    parser.add_argument(
        "--release_to",
        required=True,
        help="Destination release name",
    )
    parser.add_argument(
        "--state",
        required=True,
        help="State code (e.g. NY)",
    )
    parser.add_argument(
        "--upgrade_ids",
        required=True,
        help="Space-separated upgrade IDs (e.g. 00 01 02)",
    )
    parser.add_argument(
        "--file_types",
        required=True,
        help="Space-separated file types to copy (e.g. metadata load_curve_hourly)",
    )
    args = parser.parse_args()

    total = 0
    for file_type in args.file_types.split():
        for upgrade_id in args.upgrade_ids.split():
            n = copy_resstock_data(
                data_path=args.data_path,
                release_from=args.release_from,
                release_to=args.release_to,
                state=args.state,
                upgrade_id=upgrade_id,
                file_type=file_type,
            )
            total += n
            if n:
                print(
                    f"Copied {n} objects: {file_type} upgrade={upgrade_id} "
                    f"({args.release_from} -> {args.release_to})"
                )
    print(f"Total objects copied: {total}")
