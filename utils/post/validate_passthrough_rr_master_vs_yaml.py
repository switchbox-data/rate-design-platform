"""Compare differentiated revenue-requirement YAML passthrough blocks to master bills on S3.

Loads hive-partitioned ``comb_bills_year_target`` for ``run_<delivery>+<supply>`` (default
``1+2``) under ``s3://data.sb/switchbox/cairo/outputs/hp_rates/<state>/all_utilities/<batch>/``
and checks that weighted annual (``upgrade == 0``) aggregates match
``subclass_revenue_requirements.{delivery,supply}.passthrough`` in the local differentiated
YAMLs, using the same subclass column / ``group_value_to_subclass`` mapping as
``resolve_subclass_config`` (``run_num`` 5 for HP vs non-HP, 29 for electric-heat track).

Run as CLI (requires AWS credentials for S3):

  uv run python utils/post/validate_passthrough_rr_master_vs_yaml.py \\
    --batch ny_20260416a_r1-36 --state ny

See also ``tests/validate_supply_passthrough_rr.py``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TextIO, cast

import polars as pl
import yaml

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils import get_project_root
from utils.mid.compute_subclass_rr import parse_group_value_to_subclass
from utils.mid.resolve_subclass_config import resolve_subclass_config

DEFAULT_S3_MASTER_BATCH = "ny_20260416a_r1-36"
DEFAULT_RUN_PAIR = "1+2"
DEFAULT_REL = 1e-5
DEFAULT_ABS = 150.0

NY_UTILITIES_DEFAULT = ("cenhud", "coned", "nimo", "nyseg", "or", "psegli", "rge")


def master_comb_bills_lazy(
    *,
    state_lower: str,
    batch: str,
    run_pair: str,
) -> pl.LazyFrame:
    root = (
        f"s3://data.sb/switchbox/cairo/outputs/hp_rates/{state_lower}/"
        f"all_utilities/{batch}/run_{run_pair}/comb_bills_year_target/"
    )
    opts = get_aws_storage_options()
    return pl.scan_parquet(root, hive_partitioning=True, storage_options=opts)


def _resolve_group_col_for_master(group_col: str) -> str:
    if group_col == "has_hp":
        return "postprocess_group.has_hp"
    if group_col == "heating_type_v2":
        return "postprocess_group.heating_type_v2"
    return group_col


def _subclass_expr(group_col: str, gv_pairs: str) -> pl.Expr:
    gv = parse_group_value_to_subclass(gv_pairs)
    col = _resolve_group_col_for_master(group_col)
    if group_col == "has_hp":
        return (
            pl.when(pl.col(col).cast(pl.Boolean))
            .then(pl.lit(gv["true"]))
            .otherwise(pl.lit(gv["false"]))
        )
    raw = pl.col(col).cast(pl.String)
    expr: pl.Expr | None = None
    for raw_val, alias in gv.items():
        cond = raw == pl.lit(raw_val)
        expr = (
            pl.when(cond).then(pl.lit(alias))
            if expr is None
            else expr.when(cond).then(pl.lit(alias))
        )
    assert expr is not None
    return expr.otherwise(pl.lit("__unmapped__"))


def passthrough_totals_from_master(
    lf: pl.LazyFrame,
    *,
    utility: str,
    group_col: str,
    gv_pairs: str,
) -> tuple[dict[str, float], dict[str, float]]:
    """Per subclass: Σ w·(fixed+delivery) and Σ w·supply (Annual, upgrade 0)."""
    util_col = "sb.electric_utility"
    sub = _subclass_expr(group_col, gv_pairs)
    agg_df = cast(
        pl.DataFrame,
        lf.filter(
            pl.col(util_col) == pl.lit(utility),
            pl.col("month") == pl.lit("Annual"),
            pl.col("upgrade").cast(pl.Int64) == 0,
        )
        .with_columns(_subclass=sub)
        .group_by("_subclass")
        .agg(
            (
                pl.col("weight")
                * (pl.col("elec_fixed_charge") + pl.col("elec_delivery_bill"))
            )
            .sum()
            .alias("del_pass"),
            (pl.col("weight") * pl.col("elec_supply_bill")).sum().alias("sup_pass"),
        )
        .collect(),
    )
    del_out = {str(r["_subclass"]): float(r["del_pass"]) for r in agg_df.to_dicts()}
    sup_out = {str(r["_subclass"]): float(r["sup_pass"]) for r in agg_df.to_dicts()}
    return del_out, sup_out


def print_passthrough_master_vs_yaml(
    *,
    utility: str,
    rev_yaml: Path,
    run_num: int,
    group_col: str,
    run_pair: str,
    del_m: dict[str, float],
    sup_m: dict[str, float],
    yaml_del: dict[str, float],
    yaml_sup: dict[str, float],
    file: TextIO | None = None,
) -> None:
    """Human-readable master vs YAML ``passthrough`` lines."""
    out = sys.stdout if file is None else file
    del_keys = sorted(set(del_m) | set(yaml_del))
    sup_keys = sorted(set(sup_m) | set(yaml_sup))
    print(
        f"\n[passthrough_rr] utility={utility!r} rev_yaml={rev_yaml.name!r} "
        f"run_num={run_num} group_col={group_col!r} (master run_{run_pair} vs YAML passthrough)",
        file=out,
    )
    print("  delivery passthrough - per subclass (master | YAML):", file=out)
    for k in del_keys:
        m = del_m.get(k)
        y = yaml_del.get(k)
        m_s = f"{m:,.6f}" if m is not None else "-"
        y_s = f"{y:,.6f}" if y is not None else "-"
        print(f"    {k}: master={m_s}  yaml={y_s}", file=out)
    print(
        "  delivery passthrough - sum over subclasses: "
        f"master={sum(del_m.values()):,.6f}  yaml={sum(yaml_del.values()):,.6f}",
        file=out,
    )
    print("  supply passthrough - per subclass (master | YAML):", file=out)
    for k in sup_keys:
        m = sup_m.get(k)
        y = yaml_sup.get(k)
        m_s = f"{m:,.6f}" if m is not None else "-"
        y_s = f"{y:,.6f}" if y is not None else "-"
        print(f"    {k}: master={m_s}  yaml={y_s}", file=out)
    print(
        "  supply passthrough - sum over subclasses: "
        f"master={sum(sup_m.values()):,.6f}  yaml={sum(yaml_sup.values()):,.6f}",
        file=out,
    )


def passthrough_values_match(
    actual: float, expected: float, *, rel: float, abs_tol: float
) -> bool:
    """True if ``actual`` is within the same tolerance as ``pytest.approx(expected, ...)``."""
    return abs(actual - expected) <= rel * abs(expected) + abs_tol


def assert_passthrough_yaml_matches_master(
    *,
    utility: str,
    rev_yaml: Path,
    scenario_yaml: Path,
    run_num: int,
    lf_master: pl.LazyFrame,
    run_pair: str = DEFAULT_RUN_PAIR,
    rel: float = DEFAULT_REL,
    abs_tol: float = DEFAULT_ABS,
    print_comparison: bool = True,
) -> None:
    """Raise ``AssertionError`` if master aggregates disagree with YAML passthrough blocks."""
    group_col, gv_pairs = resolve_subclass_config(scenario_yaml, run_num=run_num)
    data = yaml.safe_load(rev_yaml.read_text(encoding="utf-8"))
    sr = data["subclass_revenue_requirements"]
    yaml_del: dict[str, float] = sr["delivery"]["passthrough"]
    yaml_sup: dict[str, float] = sr["supply"]["passthrough"]

    del_m, sup_m = passthrough_totals_from_master(
        lf_master, utility=utility, group_col=group_col, gv_pairs=gv_pairs
    )
    if print_comparison:
        print_passthrough_master_vs_yaml(
            utility=utility,
            rev_yaml=rev_yaml,
            run_num=run_num,
            group_col=group_col,
            run_pair=run_pair,
            del_m=del_m,
            sup_m=sup_m,
            yaml_del=yaml_del,
            yaml_sup=yaml_sup,
        )

    if set(del_m) != set(yaml_del):
        msg = f"{utility}: delivery passthrough subclass keys differ: master={sorted(del_m)} yaml={sorted(yaml_del)}"
        raise AssertionError(msg)
    if set(sup_m) != set(yaml_sup):
        msg = f"{utility}: supply passthrough subclass keys differ: master={sorted(sup_m)} yaml={sorted(yaml_sup)}"
        raise AssertionError(msg)
    for k in yaml_del:
        if not passthrough_values_match(
            del_m[k], yaml_del[k], rel=rel, abs_tol=abs_tol
        ):
            raise AssertionError(
                f"{utility} delivery passthrough[{k!r}]: master={del_m[k]} yaml={yaml_del[k]} "
                f"(rel={rel}, abs_tol={abs_tol})"
            )
    for k in yaml_sup:
        if not passthrough_values_match(
            sup_m[k], yaml_sup[k], rel=rel, abs_tol=abs_tol
        ):
            raise AssertionError(
                f"{utility} supply passthrough[{k!r}]: master={sup_m[k]} yaml={yaml_sup[k]} "
                f"(rel={rel}, abs_tol={abs_tol})"
            )


def _parse_utilities(arg: str | None) -> tuple[str, ...]:
    if arg is None or arg.strip() == "":
        return NY_UTILITIES_DEFAULT
    return tuple(u.strip() for u in arg.split(",") if u.strip())


def main(argv: list[str] | None = None) -> int:
    path_repo = get_project_root()
    default_rev = path_repo / "rate_design/hp_rates/ny/config/rev_requirement"
    default_scen = path_repo / "rate_design/hp_rates/ny/config/scenarios"

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--batch", default=DEFAULT_S3_MASTER_BATCH, help="Master-table batch name"
    )
    p.add_argument(
        "--state", default="ny", help="Lowercase state folder under hp_rates"
    )
    p.add_argument(
        "--run-pair",
        default=DEFAULT_RUN_PAIR,
        help="Run folder suffix, e.g. 1+2 for delivery+supply master comb bills",
    )
    p.add_argument(
        "--path-rev-requirement-dir",
        type=Path,
        default=default_rev,
        help="Directory containing differentiated YAMLs",
    )
    p.add_argument(
        "--path-scenarios-dir",
        type=Path,
        default=default_scen,
        help="Directory containing scenarios_<utility>.yaml",
    )
    p.add_argument(
        "--utilities",
        default=None,
        help=f"Comma-separated utilities (default: {','.join(NY_UTILITIES_DEFAULT)})",
    )
    p.add_argument(
        "--rel",
        type=float,
        default=DEFAULT_REL,
        help="Relative tolerance for float compare",
    )
    p.add_argument(
        "--abs-tol",
        type=float,
        default=DEFAULT_ABS,
        help="Absolute tolerance for float compare",
    )
    p.add_argument(
        "--mode",
        choices=("both", "hp", "elec-heat"),
        default="both",
        help="Which YAML family to validate",
    )
    p.add_argument(
        "--quiet", action="store_true", help="Suppress per-utility comparison printout"
    )
    args = p.parse_args(argv)

    state_lower = args.state.lower().strip() or "ny"
    utilities = _parse_utilities(args.utilities)
    lf = master_comb_bills_lazy(
        state_lower=state_lower, batch=args.batch, run_pair=args.run_pair
    )

    failed = 0
    jobs: list[tuple[str, Path, Path, int]] = []
    for u in utilities:
        scen = args.path_scenarios_dir / f"scenarios_{u}.yaml"
        if args.mode in ("both", "hp"):
            jobs.append(
                (u, args.path_rev_requirement_dir / f"{u}_hp_vs_nonhp.yaml", scen, 5)
            )
        if args.mode in ("both", "elec-heat"):
            jobs.append(
                (
                    u,
                    args.path_rev_requirement_dir
                    / f"{u}_elec_heat_vs_non_elec_heat.yaml",
                    scen,
                    29,
                )
            )

    for utility, rev_path, scen_path, run_num in jobs:
        if not rev_path.is_file():
            print(f"SKIP missing rev yaml: {rev_path}", file=sys.stderr)
            continue
        if not scen_path.is_file():
            print(f"SKIP missing scenarios: {scen_path}", file=sys.stderr)
            continue
        try:
            assert_passthrough_yaml_matches_master(
                utility=utility,
                rev_yaml=rev_path,
                scenario_yaml=scen_path,
                run_num=run_num,
                lf_master=lf,
                run_pair=args.run_pair,
                rel=args.rel,
                abs_tol=args.abs_tol,
                print_comparison=not args.quiet,
            )
        except AssertionError as exc:
            print(f"FAIL {utility} run_num={run_num}: {exc}", file=sys.stderr)
            failed += 1
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR {utility} run_num={run_num}: {exc}", file=sys.stderr)
            failed += 1

    if failed:
        print(f"\nDone: {failed} check(s) failed.", file=sys.stderr)
        return 1
    print("\nAll passthrough checks passed.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
