"""Compare two CAIRO run directories for numerical equivalence.

Used to verify that memory optimizations or refactors produce identical outputs.
Compares key CSV artifacts (BAT values, bills, elasticity tracker) by joining on
stable keys and asserting numeric columns match within tolerance.

Usage::

    uv run python -m utils.post.compare_cairo_runs \\
        --baseline s3://data.sb/.../run15_baseline/ \\
        --challenger s3://data.sb/.../run15_challenger/

    # Compare specific artifacts only:
    uv run python -m utils.post.compare_cairo_runs \\
        --baseline s3://data.sb/.../run15_baseline/ \\
        --challenger s3://data.sb/.../run15_challenger/ \\
        --artifacts bat,bills_elec

    # Strict mode (zero tolerance):
    uv run python -m utils.post.compare_cairo_runs \\
        --baseline s3://data.sb/.../run15_baseline/ \\
        --challenger s3://data.sb/.../run15_challenger/ \\
        --rtol 0 --atol 0
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
import polars as pl

from utils.post.validate.load import _s3_get_bytes, _s3_join

# Artifacts to compare: (short_name, relative_path, join_keys)
_ARTIFACTS: list[tuple[str, str, list[str]]] = [
    ("bat", "cross_subsidization/cross_subsidization_BAT_values.csv", ["bldg_id"]),
    ("bills_elec", "bills/elec_bills_year_run.csv", ["bldg_id", "month"]),
    ("bills_comb", "bills/comb_bills_year_run.csv", ["bldg_id", "month"]),
    ("bills_elec_target", "bills/elec_bills_year_target.csv", ["bldg_id", "month"]),
    ("bills_comb_target", "bills/comb_bills_year_target.csv", ["bldg_id", "month"]),
    ("elasticity", "demand_flex_elasticity_tracker.csv", ["bldg_id"]),
    ("metadata", "customer_metadata.csv", ["bldg_id"]),
]


@dataclass(slots=True)
class ComparisonResult:
    artifact: str
    rows_baseline: int
    rows_challenger: int
    rows_matched: int
    columns_compared: list[str]
    max_abs_diff: float
    max_rel_diff: float
    mismatched_columns: list[str]
    passed: bool
    error: str | None = None


def _read_csv_from_s3(s3_dir: str, rel_path: str) -> pl.DataFrame | None:
    """Read a CSV from S3, returning None if the file doesn't exist."""
    uri = _s3_join(s3_dir, rel_path)
    try:
        raw = _s3_get_bytes(uri)
    except Exception:
        return None
    return pl.read_csv(raw, infer_schema_length=10000, ignore_errors=True)


def _numeric_cols(df: pl.DataFrame, exclude: list[str]) -> list[str]:
    """Return names of numeric columns not in the exclusion list."""
    return [
        c
        for c in df.columns
        if c not in exclude
        and df[c].dtype
        in (
            pl.Float64,
            pl.Float32,
            pl.Int64,
            pl.Int32,
            pl.Int16,
            pl.Int8,
            pl.UInt64,
            pl.UInt32,
            pl.UInt16,
            pl.UInt8,
        )
    ]


def compare_artifact(
    s3_baseline: str,
    s3_challenger: str,
    artifact_name: str,
    rel_path: str,
    join_keys: list[str],
    rtol: float = 1e-9,
    atol: float = 1e-12,
) -> ComparisonResult:
    """Compare a single CSV artifact between baseline and challenger runs."""
    df_base = _read_csv_from_s3(s3_baseline, rel_path)
    df_chal = _read_csv_from_s3(s3_challenger, rel_path)

    if df_base is None and df_chal is None:
        return ComparisonResult(
            artifact=artifact_name,
            rows_baseline=0,
            rows_challenger=0,
            rows_matched=0,
            columns_compared=[],
            max_abs_diff=0.0,
            max_rel_diff=0.0,
            mismatched_columns=[],
            passed=True,
            error="Both missing (OK for non-flex runs)",
        )
    if df_base is None:
        return ComparisonResult(
            artifact=artifact_name,
            rows_baseline=0,
            rows_challenger=df_chal.height if df_chal is not None else 0,
            rows_matched=0,
            columns_compared=[],
            max_abs_diff=0.0,
            max_rel_diff=0.0,
            mismatched_columns=[],
            passed=False,
            error="Baseline missing but challenger exists",
        )
    if df_chal is None:
        return ComparisonResult(
            artifact=artifact_name,
            rows_baseline=df_base.height,
            rows_challenger=0,
            rows_matched=0,
            columns_compared=[],
            max_abs_diff=0.0,
            max_rel_diff=0.0,
            mismatched_columns=[],
            passed=False,
            error="Challenger missing but baseline exists",
        )

    available_keys = [
        k for k in join_keys if k in df_base.columns and k in df_chal.columns
    ]
    if not available_keys:
        return ComparisonResult(
            artifact=artifact_name,
            rows_baseline=df_base.height,
            rows_challenger=df_chal.height,
            rows_matched=0,
            columns_compared=[],
            max_abs_diff=0.0,
            max_rel_diff=0.0,
            mismatched_columns=[],
            passed=False,
            error=f"No join keys found in both DataFrames (tried {join_keys})",
        )

    shared_cols = [
        c for c in df_base.columns if c in df_chal.columns and c not in available_keys
    ]
    num_cols = _numeric_cols(df_base.select(shared_cols), exclude=[])

    joined = df_base.join(
        df_chal,
        on=available_keys,
        how="inner",
        suffix="_chal",
    )

    mismatched: list[str] = []
    overall_max_abs = 0.0
    overall_max_rel = 0.0

    for col in num_cols:
        col_chal = f"{col}_chal"
        if col_chal not in joined.columns:
            continue
        base_vals = joined[col].cast(pl.Float64).fill_null(0.0)
        chal_vals = joined[col_chal].cast(pl.Float64).fill_null(0.0)

        abs_diff = (base_vals - chal_vals).abs()
        denom = base_vals.abs().zip_with(
            base_vals.abs() > 0, pl.Series([1.0] * joined.height)
        )
        rel_diff = abs_diff / denom

        max_abs = abs_diff.max()
        max_rel = rel_diff.max()
        max_abs_f: float = 0.0 if max_abs is None else float(max_abs)  # type: ignore[arg-type]
        max_rel_f: float = 0.0 if max_rel is None else float(max_rel)  # type: ignore[arg-type]

        overall_max_abs = max(overall_max_abs, max_abs_f)
        overall_max_rel = max(overall_max_rel, max_rel_f)

        exceeds = (abs_diff > atol) & (rel_diff > rtol)
        if exceeds.any():
            mismatched.append(col)

    return ComparisonResult(
        artifact=artifact_name,
        rows_baseline=df_base.height,
        rows_challenger=df_chal.height,
        rows_matched=joined.height,
        columns_compared=num_cols,
        max_abs_diff=overall_max_abs,
        max_rel_diff=overall_max_rel,
        mismatched_columns=mismatched,
        passed=len(mismatched) == 0 and df_base.height == df_chal.height,
    )


def compare_tariff_configs(s3_baseline: str, s3_challenger: str) -> ComparisonResult:
    """Compare tariff_final_config.json for exact equality."""
    rel = "tariff_final_config.json"
    try:
        base_raw = _s3_get_bytes(_s3_join(s3_baseline, rel))
        chal_raw = _s3_get_bytes(_s3_join(s3_challenger, rel))
    except Exception as exc:
        return ComparisonResult(
            artifact="tariff_config",
            rows_baseline=0,
            rows_challenger=0,
            rows_matched=0,
            columns_compared=[],
            max_abs_diff=0.0,
            max_rel_diff=0.0,
            mismatched_columns=[],
            passed=False,
            error=f"Failed to load tariff configs: {exc}",
        )
    base_obj = json.loads(base_raw)
    chal_obj = json.loads(chal_raw)
    match = base_obj == chal_obj
    return ComparisonResult(
        artifact="tariff_config",
        rows_baseline=1,
        rows_challenger=1,
        rows_matched=1 if match else 0,
        columns_compared=["json_equality"],
        max_abs_diff=0.0 if match else 1.0,
        max_rel_diff=0.0 if match else 1.0,
        mismatched_columns=[] if match else ["tariff_final_config.json"],
        passed=match,
    )


def compare_runs(
    s3_baseline: str,
    s3_challenger: str,
    artifact_filter: list[str] | None = None,
    rtol: float = 1e-9,
    atol: float = 1e-12,
) -> list[ComparisonResult]:
    """Compare all artifacts between two CAIRO run directories."""
    results: list[ComparisonResult] = []

    for name, rel_path, keys in _ARTIFACTS:
        if artifact_filter and name not in artifact_filter:
            continue
        result = compare_artifact(
            s3_baseline,
            s3_challenger,
            name,
            rel_path,
            keys,
            rtol=rtol,
            atol=atol,
        )
        results.append(result)

    if not artifact_filter or "tariff_config" in artifact_filter:
        results.append(compare_tariff_configs(s3_baseline, s3_challenger))

    return results


def print_results(results: list[ComparisonResult]) -> bool:
    """Print comparison results as a table, return True if all passed."""
    all_passed = True
    print(
        f"\n{'Artifact':<22} {'Rows B':>7} {'Rows C':>7} {'Matched':>7} {'MaxAbs':>12} {'MaxRel':>12} {'Status':<8} {'Detail'}"
    )
    print("-" * 110)
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        if not r.passed:
            all_passed = False
        detail = ""
        if r.error:
            detail = r.error
        elif r.mismatched_columns:
            detail = f"diffs in: {', '.join(r.mismatched_columns[:5])}"
            if len(r.mismatched_columns) > 5:
                detail += f" (+{len(r.mismatched_columns) - 5} more)"
        print(
            f"{r.artifact:<22} {r.rows_baseline:>7} {r.rows_challenger:>7} "
            f"{r.rows_matched:>7} {r.max_abs_diff:>12.2e} {r.max_rel_diff:>12.2e} "
            f"{status:<8} {detail}"
        )
    print()
    return all_passed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare two CAIRO run directories for numerical equivalence."
    )
    parser.add_argument(
        "--baseline",
        required=True,
        help="S3 URI of the baseline run directory",
    )
    parser.add_argument(
        "--challenger",
        required=True,
        help="S3 URI of the challenger run directory",
    )
    parser.add_argument(
        "--artifacts",
        help="Comma-separated artifact names to compare (default: all). "
        f"Available: {', '.join(n for n, _, _ in _ARTIFACTS)}, tariff_config",
    )
    parser.add_argument(
        "--rtol",
        type=float,
        default=1e-9,
        help="Relative tolerance (default: 1e-9)",
    )
    parser.add_argument(
        "--atol",
        type=float,
        default=1e-12,
        help="Absolute tolerance (default: 1e-12)",
    )
    args = parser.parse_args()

    artifact_filter = None
    if args.artifacts:
        artifact_filter = [a.strip() for a in args.artifacts.split(",")]

    results = compare_runs(
        s3_baseline=args.baseline,
        s3_challenger=args.challenger,
        artifact_filter=artifact_filter,
        rtol=args.rtol,
        atol=args.atol,
    )

    all_passed = print_results(results)
    if not all_passed:
        print("COMPARISON FAILED: some artifacts differ beyond tolerance.")
        sys.exit(1)
    else:
        print("COMPARISON PASSED: all artifacts match within tolerance.")


if __name__ == "__main__":
    main()
