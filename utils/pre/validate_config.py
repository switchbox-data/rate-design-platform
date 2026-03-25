"""Validate Justfile configuration against scenario YAML values.

Compares expected values (passed as CLI args from Just variables) against the
canonical scenario YAML.  Run 1 is used for most fields; run 2 (first supply
run) is used for the supply MC paths since delivery runs use zero.parquet.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


def _normalize_data_path(p: str) -> str:
    """Normalize ``s3://data.sb/...`` and ``/data.sb/...`` to a common form.

    Justfile variables use ``s3://`` for native S3 access while scenario YAMLs
    use the FUSE mount (``/data.sb/``) because CAIRO requires local paths.
    Stripping the transport prefix lets us compare the logical path regardless
    of access method.
    """
    p = p.rstrip("/")
    if p.startswith("s3://"):
        return p[len("s3://") :]
    if p.startswith("/"):
        return p.lstrip("/")
    return p


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario-config", required=True, type=Path)
    parser.add_argument("--state", required=True)
    parser.add_argument("--utility", required=True)
    parser.add_argument("--upgrade", required=True)
    parser.add_argument("--year", required=True)
    parser.add_argument("--path-dist-and-sub-tx-mc", required=True)
    parser.add_argument("--path-bulk-tx-mc", default=None)
    parser.add_argument("--path-supply-energy-mc", required=True)
    parser.add_argument("--path-supply-capacity-mc", required=True)
    parser.add_argument("--path-electric-utility-stats", required=True)
    parser.add_argument("--path-resstock-loads", required=True)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    with open(args.scenario_config) as f:
        runs = yaml.safe_load(f)["runs"]

    run1 = runs[1]
    run2 = runs.get(2)

    checks: list[tuple[str, str, str]] = [
        ("state", args.state, str(run1.get("state", ""))),
        ("utility", args.utility, str(run1.get("utility", ""))),
        ("upgrade", args.upgrade, str(run1.get("upgrade", "")).zfill(2)),
        ("year", args.year, str(run1.get("year_run", ""))),
        (
            "path_dist_and_sub_tx_mc",
            _normalize_data_path(args.path_dist_and_sub_tx_mc),
            _normalize_data_path(str(run1.get("path_dist_and_sub_tx_mc", ""))),
        ),
        (
            "path_electric_utility_stats",
            _normalize_data_path(args.path_electric_utility_stats),
            _normalize_data_path(str(run1.get("path_electric_utility_stats", ""))),
        ),
        (
            "path_resstock_loads",
            _normalize_data_path(args.path_resstock_loads),
            _normalize_data_path(str(run1.get("path_resstock_loads", ""))),
        ),
    ]

    # Optional: check path_bulk_tx_mc (NY-only; skipped when arg not supplied)
    if args.path_bulk_tx_mc is not None:
        checks.append(
            (
                "path_bulk_tx_mc",
                _normalize_data_path(args.path_bulk_tx_mc),
                _normalize_data_path(str(run1.get("path_bulk_tx_mc", ""))),
            )
        )

    # Check supply MC paths against run2 (the first supply run).
    # Run1 uses zero.parquet for delivery-only; run2 has the real paths.
    if run2:
        checks.append(
            (
                "path_supply_energy_mc",
                _normalize_data_path(args.path_supply_energy_mc),
                _normalize_data_path(str(run2.get("path_supply_energy_mc", ""))),
            )
        )
        checks.append(
            (
                "path_supply_capacity_mc",
                _normalize_data_path(args.path_supply_capacity_mc),
                _normalize_data_path(str(run2.get("path_supply_capacity_mc", ""))),
            )
        )

    # Cross-check run_includes_subclasses against path_tariffs_electric keys
    for run_num, run_dict in runs.items():
        tariffs = run_dict.get("path_tariffs_electric", {})
        explicit = bool(run_dict.get("run_includes_subclasses", False))
        inferred = isinstance(tariffs, dict) and len(tariffs) > 1
        if explicit != inferred:
            n_keys = len(tariffs) if isinstance(tariffs, dict) else 0
            print(
                f"\u26a0\ufe0f  run {run_num}: run_includes_subclasses={explicit} "
                f"but path_tariffs_electric has {n_keys} key(s)",
                file=sys.stderr,
            )

    mismatches = []
    for name, expected, actual in checks:
        if expected != actual:
            mismatches.append(f"  {name}: justfile={expected!r}  yaml={actual!r}")

    if mismatches:
        banner = "\U0001f6a8" * 3  # 🚨🚨🚨
        print(f"\n{banner}  CONFIG MISMATCH  {banner}", file=sys.stderr)
        print("Justfile variables do not match scenario YAML:", file=sys.stderr)
        for m in mismatches:
            print(m, file=sys.stderr)
        print(file=sys.stderr)
        if args.strict:
            sys.exit(1)
    else:
        print("\u2705 validate-config: all checks passed", file=sys.stderr)


if __name__ == "__main__":
    main()
