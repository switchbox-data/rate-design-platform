"""Extract PJM real-time hourly zone-aggregate LMP rows for Maryland utilities.

Reads the raw ``rt_hrl_lmps`` CSV from S3, extracts the ZONE-type aggregate
rows for each MD utility, and returns a tidy DataFrame ready for Hive-
partitioned upload to S3 (partitioned by ``zone`` and ``year``).

**Key data quirk**: In the PJM feed the ``zone`` column is *null* for
ZONE-type aggregate nodes.  The zone name is carried in ``pnode_name``
instead (e.g. ``pnode_name == "BGE"`` for the BGE zone aggregate).
Filtering on ``zone == "BGE"`` alone returns only bus/load/gen nodes inside
that zone — never the zone aggregate.  The correct filter is::

    type == "ZONE"  AND  pnode_name == <pjm_zone_code>

Mapping from Switchbox utility std_name to PJM zone codes (pnode_name for
ZONE-type rows):

    bge            → BGE      (pnode_id 51292)
    pepco          → PEPCO    (pnode_id 51298)
    delmarva       → DPL      (pnode_id 51293)
    potomac_edison → APS      (pnode_id 8394954)

Usage::

    uv run python data/pjm/fetch_lmp.py
    uv run python data/pjm/fetch_lmp.py --s3-path s3://data.sb/pjm/lmp/real_time/rt_hrl_lmps.csv
"""

from __future__ import annotations

import argparse
import io

import boto3
import polars as pl

# ---------------------------------------------------------------------------
# MD utility → PJM zone code mapping
# ---------------------------------------------------------------------------

# Maps Switchbox std_name → PJM zone code that appears as pnode_name for the
# ZONE-type aggregate node.  These codes also appear in the ``zone`` column
# for non-aggregate node types (LOAD, GEN, EHV) within that zone.
MD_UTILITY_ZONE_MAP: dict[str, str] = {
    "bge": "BGE",
    "pepco": "PEPCO",
    "delmarva": "DPL",
    "potomac_edison": "APS",
}

# Canonical output column order
_OUTPUT_COLS = [
    "datetime_beginning_utc",
    "datetime_beginning_ept",
    "pnode_id",
    "pnode_name",
    "zone",
    "total_lmp_rt",
    "system_energy_price_rt",
    "congestion_price_rt",
    "marginal_loss_price_rt",
    "row_is_current",
    "version_nbr",
]

S3_PATH_DEFAULT = "s3://data.sb/pjm/lmp/real_time/rt_hrl_lmps.csv"


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def read_csv_from_s3(s3_path: str) -> pl.DataFrame:
    """Read a CSV from S3 into a Polars DataFrame using boto3."""
    path = s3_path.removeprefix("s3://")
    bucket, key = path.split("/", 1)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pl.read_csv(io.BytesIO(obj["Body"].read()), infer_schema_length=10_000)
    print(f"Read {df.height:,} rows × {df.width} cols from s3://{bucket}/{key}")
    return df


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------


def extract_md_zone_lmps(
    df: pl.DataFrame,
    zone_map: dict[str, str] = MD_UTILITY_ZONE_MAP,
) -> pl.DataFrame:
    """Extract ZONE-type aggregate LMP rows for Maryland utilities.

    For each utility in *zone_map*, finds all rows where:
      - ``type == "ZONE"`` (zone-level aggregate, not an individual bus/node)
      - ``pnode_name == <pjm_zone_code>``  (e.g. "BGE", "PEPCO", "DPL", "APS")

    Note: filtering on the ``zone`` column will NOT find these rows because
    ZONE-type aggregate nodes have ``zone = null`` in the PJM feed.

    Returns a tidy DataFrame with one row per (utility_zone, timestamp), with
    an added ``zone`` column populated from ``pnode_name`` and a ``year``
    column extracted from ``datetime_beginning_utc`` for Hive partitioning.
    """
    pjm_zone_codes = list(zone_map.values())

    result = (
        df.filter(
            (pl.col("type") == "ZONE") & pl.col("pnode_name").is_in(pjm_zone_codes)
        )
        .with_columns(
            # Populate zone from pnode_name (zone col is null for ZONE-type rows)
            pl.col("pnode_name").alias("zone"),
            # Parse timestamps to Datetime for downstream use
            pl.col("datetime_beginning_utc")
            .str.to_datetime("%m/%d/%Y %I:%M:%S %p", strict=False)
            .alias("datetime_beginning_utc"),
            pl.col("datetime_beginning_ept")
            .str.to_datetime("%m/%d/%Y %I:%M:%S %p", strict=False)
            .alias("datetime_beginning_ept"),
        )
        .with_columns(
            # Extract year for Hive partition key
            pl.col("datetime_beginning_utc").dt.year().alias("year"),
        )
        .select(_OUTPUT_COLS + ["year"])
        .sort("zone", "datetime_beginning_utc")
    )

    _print_summary(result, zone_map)
    return result


def _print_summary(df: pl.DataFrame, zone_map: dict[str, str]) -> None:
    pjm_zone_codes = list(zone_map.values())
    print(f"\nExtracted {df.height:,} zone-aggregate rows for MD utilities")
    summary = (
        df.group_by("zone", "year")
        .agg(
            pl.len().alias("n_hours"),
            pl.col("total_lmp_rt").mean().round(2).alias("avg_lmp_rt"),
            pl.col("total_lmp_rt").min().round(2).alias("min_lmp_rt"),
            pl.col("total_lmp_rt").max().round(2).alias("max_lmp_rt"),
        )
        .sort("zone", "year")
    )
    print(summary)

    # Warn about any expected zones with no data
    found = set(df["zone"].unique().to_list())
    missing = set(pjm_zone_codes) - found
    if missing:
        print(f"\nWARNING: no ZONE-type rows found for: {sorted(missing)}")
        print("  Check that the CSV covers the expected date range and zones.")


# ---------------------------------------------------------------------------
# S3 upload (Hive-partitioned by zone and year)
# ---------------------------------------------------------------------------


def upload_to_s3(
    df: pl.DataFrame,
    s3_base: str,
    *,
    dry_run: bool = False,
) -> None:
    """Write zone-LMP DataFrame to Hive-partitioned Parquet on S3.

    Output layout::

        s3://<base>/zone={ZONE}/year={YEAR}/data.parquet

    Args:
        df:       DataFrame returned by :func:`extract_md_zone_lmps`.
        s3_base:  Base S3 prefix (e.g. ``s3://data.sb/pjm/lmp/real_time/zones``).
        dry_run:  If True, print paths without writing.
    """
    s3 = boto3.client("s3")
    base = s3_base.rstrip("/").removeprefix("s3://")
    bucket, prefix = base.split("/", 1)

    zones = sorted(df["zone"].unique().to_list())
    years = sorted(df["year"].unique().to_list())

    for zone in zones:
        for year in years:
            partition = df.filter(
                (pl.col("zone") == zone) & (pl.col("year") == year)
            ).drop("year")

            if partition.is_empty():
                continue

            key = f"{prefix}/zone={zone}/year={year}/data.parquet"
            if dry_run:
                print(
                    f"  [dry-run] would write {partition.height} rows → s3://{bucket}/{key}"
                )
                continue

            buf = io.BytesIO()
            partition.write_parquet(buf)
            buf.seek(0)
            s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
            print(f"  Uploaded {partition.height} rows → s3://{bucket}/{key}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--s3-path",
        default=S3_PATH_DEFAULT,
        help="S3 URI of the source RT LMP CSV (default: %(default)s)",
    )
    parser.add_argument(
        "--s3-output",
        default=None,
        help="S3 base URI for Hive-partitioned output, e.g. s3://data.sb/pjm/lmp/real_time/zones",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print upload paths without writing to S3",
    )
    args = parser.parse_args()

    raw = read_csv_from_s3(args.s3_path)
    lmps = extract_md_zone_lmps(raw)

    if args.s3_output:
        print(f"\nUploading to {args.s3_output} ...")
        upload_to_s3(lmps, args.s3_output, dry_run=args.dry_run)
    elif args.dry_run:
        default_out = "s3://data.sb/pjm/lmp/real_time/zones"
        print(f"\n[dry-run] Would upload to {default_out}")
        upload_to_s3(lmps, default_out, dry_run=True)


if __name__ == "__main__":
    main()
