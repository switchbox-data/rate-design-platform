"""Create revenue requirement YAML from a revenue requirements CSV."""

from __future__ import annotations

import argparse
import io
from pathlib import Path
from urllib.parse import urlparse

import boto3
import polars as pl
import yaml

REVENUE_REQUIREMENT_COL = "revenue_requirement"


def _is_s3_path(path_value: str) -> bool:
    return path_value.startswith("s3://")


def _read_csv(path_revenue_csv: str) -> pl.DataFrame:
    if not _is_s3_path(path_revenue_csv):
        return pl.read_csv(path_revenue_csv)
    parsed = urlparse(path_revenue_csv)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 path: {path_revenue_csv}")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    payload = obj["Body"].read()
    return pl.read_csv(io.BytesIO(payload))


def _normalize_utility(value: object) -> str:
    return str(value).strip().lower()


def _normalize_year(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value))
    raw = str(value).strip()
    if raw == "":
        return ""
    try:
        return str(int(float(raw)))
    except ValueError:
        return raw


def _normalize_col_name(name: str) -> str:
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def _resolve_column_name(columns: list[str], requested_name: str) -> str:
    requested_norm = _normalize_col_name(requested_name)
    matches = [col for col in columns if _normalize_col_name(col) == requested_norm]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(
            f"Ambiguous column match for {requested_name!r}: {matches}. "
            "Please rename columns or pass a more specific column name."
        )
    raise ValueError(
        f"Missing required column {requested_name!r}. Available columns: {columns}"
    )


def _parse_revenue_requirement(value: object) -> float:
    if isinstance(value, int | float):
        return float(value)
    raw = str(value).strip().replace(",", "")
    if raw == "":
        raise ValueError("Revenue requirement value is blank.")
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(
            f"Invalid revenue requirement value {value!r} in column "
            f"{REVENUE_REQUIREMENT_COL!r}."
        ) from exc


def create_revenue_yaml(
    *,
    path_revenue_csv: str,
    path_output_yaml: Path,
    utility: str,
    year: str | int,
    utility_col: str,
    year_col: str,
) -> Path:
    revenue_df = _read_csv(path_revenue_csv)
    available_cols = revenue_df.columns
    resolved_utility_col = _resolve_column_name(available_cols, utility_col)
    resolved_year_col = _resolve_column_name(available_cols, year_col)
    resolved_revenue_col = REVENUE_REQUIREMENT_COL
    if resolved_revenue_col not in available_cols:
        raise ValueError(
            f"Missing required column {REVENUE_REQUIREMENT_COL!r}. "
            f"Available columns: {available_cols}"
        )

    utility_norm = _normalize_utility(utility)
    year_norm = _normalize_year(year)
    matching_rows: list[dict[str, object]] = []
    for row in revenue_df.to_dicts():
        row_utility = _normalize_utility(row[resolved_utility_col])
        row_year = _normalize_year(row[resolved_year_col])
        if row_utility == utility_norm and row_year == year_norm:
            matching_rows.append(row)

    if len(matching_rows) != 1:
        raise ValueError(
            "Expected exactly one revenue requirement row for "
            f"utility={utility_norm!r}, year={year_norm!r}; found {len(matching_rows)}."
        )

    revenue_requirement = _parse_revenue_requirement(matching_rows[0][resolved_revenue_col])
    payload = {
        "utility": utility_norm,
        "revenue_requirement": revenue_requirement,
        "source": (
            f"{path_revenue_csv}:{resolved_revenue_col} "
            f"(utility_col={resolved_utility_col}, year_col={resolved_year_col}, "
            f"utility={utility_norm}, year={year_norm})"
        ),
    }
    path_output_yaml.parent.mkdir(parents=True, exist_ok=True)
    path_output_yaml.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path_output_yaml


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create revenue requirement YAML from a revenue requirements CSV."
    )
    parser.add_argument(
        "--path-revenue-csv",
        required=True,
        help="Input revenue requirements CSV path (local or s3://...).",
    )
    parser.add_argument(
        "--path-output-yaml",
        type=Path,
        required=True,
        help="Output revenue requirement YAML path.",
    )
    parser.add_argument(
        "--utility",
        required=True,
        help="Utility filter value.",
    )
    parser.add_argument(
        "--year",
        required=True,
        help="Year filter value.",
    )
    parser.add_argument(
        "--utility-col",
        required=True,
        help="Column name for utility in the revenue CSV.",
    )
    parser.add_argument(
        "--year-col",
        required=True,
        help="Column name for year in the revenue CSV.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    output_path = create_revenue_yaml(
        path_revenue_csv=args.path_revenue_csv,
        path_output_yaml=args.path_output_yaml,
        utility=args.utility,
        year=args.year,
        utility_col=args.utility_col,
        year_col=args.year_col,
    )
    print(f"Wrote revenue requirement YAML: {output_path}")


if __name__ == "__main__":
    main()
