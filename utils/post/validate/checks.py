"""Validation checks for CAIRO run outputs.

Each check function returns a ``CheckResult`` with a ``status`` of ``"PASS"``,
``"WARN"``, or ``"FAIL"``, a human-readable ``message``, and a ``details`` dict
for programmatic use (e.g. saving as CSV columns).

CAIRO output CSV column conventions:
  bills CSVs : ``bldg_id``, ``month``, ``bill_level``
  BAT CSV    : ``bldg_id``, ``BAT_percustomer``, ``BAT_vol``, ``BAT_peak``
  metadata   : ``bldg_id``, ``weight``, ``postprocess_group.has_hp``,
               ``postprocess_group.heating_type``

Revenue requirement YAML conventions (from :func:`~utils.post.validate.load.load_revenue_requirement`):
  total RR   : ``total_delivery_revenue_requirement``,
               ``total_delivery_and_supply_revenue_requirement``
  subclass RR: ``subclass_revenue_requirements`` →
               ``{subclass: {delivery, supply, total}}`` with keys ``"hp"`` / ``"non-hp"``
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, cast

import boto3
import polars as pl
from botocore.exceptions import ClientError

CheckStatus = Literal["PASS", "WARN", "FAIL"]

# CAIRO output CSV column names
_BLDG_COL = "bldg_id"
_WEIGHT_COL = "weight"
_HP_COL = "postprocess_group.has_hp"
_HEATING_TYPE_COL = "postprocess_group.heating_type"
_MONTH_COL = "month"
_BILL_COL = "bill_level"
_ANNUAL_MONTH = "Annual"

# BAT metric columns present in cross_subsidization_BAT_values.csv
_BAT_COLS = ("BAT_percustomer", "BAT_vol", "BAT_peak")

# All files expected in every run directory
_EXPECTED_FILES: tuple[str, ...] = (
    "bills/elec_bills_year_target.csv",
    "bills/gas_bills_year_target.csv",
    "bills/comb_bills_year_target.csv",
    "bills/elec_bills_year_run.csv",
    "cross_subsidization/cross_subsidization_BAT_values.csv",
    "customer_metadata.csv",
    "tariff_final_config.json",
)


@dataclass
class CheckResult:
    """Outcome of a single validation check.

    Attributes:
        name: Short snake_case identifier (e.g. ``"revenue_neutrality"``).
        status: ``"PASS"``, ``"WARN"``, or ``"FAIL"``.
        message: Human-readable one-line summary suitable for console output.
        details: Structured data for programmatic use (metrics, per-subclass
            breakdowns, lists of diffs, etc.).
    """

    name: str
    status: CheckStatus
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        """``True`` when status is ``"PASS"``."""
        return self.status == "PASS"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _collect(lf: pl.LazyFrame) -> pl.DataFrame:
    return cast(pl.DataFrame, lf.collect())


def _rr_target(rr_config: dict[str, Any], cost_scope: str) -> float:
    """Return the appropriate RR total from a revenue-requirement config dict."""
    key = (
        "total_delivery_and_supply_revenue_requirement"
        if cost_scope == "delivery+supply"
        else "total_delivery_revenue_requirement"
    )
    return float(rr_config[key])


def _weighted_annual_bills(bills: pl.LazyFrame, metadata: pl.LazyFrame) -> pl.LazyFrame:
    """Filter bills to the Annual row and join with metadata weights."""
    return bills.filter(pl.col(_MONTH_COL) == _ANNUAL_MONTH).join(
        metadata.select([_BLDG_COL, _WEIGHT_COL]), on=_BLDG_COL
    )


def _weighted_annual_revenue_by_subclass(
    bills: pl.LazyFrame, metadata: pl.LazyFrame
) -> dict[bool, float]:
    """Return weighted annual electric revenue by HP/non-HP subclass."""
    rows = _collect(
        bills.filter(pl.col(_MONTH_COL) == _ANNUAL_MONTH)
        .join(metadata.select([_BLDG_COL, _WEIGHT_COL, _HP_COL]), on=_BLDG_COL)
        .group_by(_HP_COL)
        .agg((pl.col(_BILL_COL) * pl.col(_WEIGHT_COL)).sum().alias("weighted_bills"))
    )
    return {
        bool(row[_HP_COL]): float(row["weighted_bills"])
        for row in rows.iter_rows(named=True)
    }


def _weighted_annual_revenue_by_named_subclass(
    bills: pl.LazyFrame, metadata: pl.LazyFrame
) -> dict[str, float]:
    """Return weighted annual electric revenue keyed by subclass name."""
    return {
        ("hp" if is_hp else "non-hp"): revenue
        for is_hp, revenue in _weighted_annual_revenue_by_subclass(
            bills, metadata
        ).items()
    }


def _bat_wavg_by_group(bat: pl.LazyFrame, metadata: pl.LazyFrame) -> pl.DataFrame:
    """Compute weighted-average BAT metrics grouped by HP/non-HP.

    Returns a DataFrame with columns: ``postprocess_group.has_hp``,
    ``BAT_percustomer_wavg``, ``BAT_vol_wavg``, ``BAT_peak_wavg``,
    ``customers_weighted``.  Only BAT columns present in ``bat.schema`` are
    included in the aggregation.
    """
    bat_cols = [c for c in _BAT_COLS if c in bat.collect_schema()]
    return _collect(
        bat.join(metadata.select([_BLDG_COL, _WEIGHT_COL, _HP_COL]), on=_BLDG_COL)
        .group_by(_HP_COL)
        .agg(
            *[
                (
                    (pl.col(c) * pl.col(_WEIGHT_COL)).sum() / pl.col(_WEIGHT_COL).sum()
                ).alias(f"{c}_wavg")
                for c in bat_cols
            ],
            pl.col(_WEIGHT_COL).sum().alias("customers_weighted"),
        )
    )


def _flatten_numeric(obj: Any, prefix: str = "") -> dict[str, float]:
    """Recursively extract all numeric leaf values from a nested structure.

    Dict keys and list indices form dotted-path keys in the result
    (e.g. ``"ur_ec_tou_mat[0][4]"``).  String values that parse as floats are
    included; non-numeric strings are silently skipped.  Booleans are excluded.
    """
    if isinstance(obj, bool):
        return {}
    if isinstance(obj, (int, float)):
        return {prefix: float(obj)}
    if isinstance(obj, str):
        try:
            return {prefix: float(obj)}
        except ValueError:
            return {}
    if isinstance(obj, dict):
        out: dict[str, float] = {}
        for k, v in obj.items():
            out |= _flatten_numeric(v, f"{prefix}.{k}" if prefix else k)
        return out
    if isinstance(obj, list):
        out = {}
        for i, v in enumerate(obj):
            out |= _flatten_numeric(v, f"{prefix}[{i}]")
        return out
    return {}


def _primary_vol_rate(tariff: dict[str, Any]) -> float:
    """Extract the primary volumetric rate (``ur_ec_tou_mat[0][4]``) from a tariff dict.

    Uses the first key in the dict (the tariff identifier, e.g. ``"rie_a16"``).
    """
    entry = next(iter(tariff.values()))
    mat = entry.get("ur_ec_tou_mat", [])
    return float(mat[0][4]) if mat else float("nan")


# ---------------------------------------------------------------------------
# Check functions
# ---------------------------------------------------------------------------


def check_revenue_neutrality(
    bills: pl.LazyFrame,
    metadata: pl.LazyFrame,
    rr_config: dict[str, Any],
    cost_scope: str = "delivery",
    tolerance_pct: float = 0.5,
) -> CheckResult:
    """Check that total weighted annual bills recover the topped-up revenue requirement.

    Applies to precalc runs (1-2, 5-6) only; default runs use a large-number RR
    and are not expected to be revenue-neutral.

    Args:
        bills: LazyFrame from :func:`~utils.post.validate.load.load_bills`
            (columns: ``bldg_id``, ``month``, ``bill_level``).
        metadata: LazyFrame with ``bldg_id`` and ``weight`` columns.
        rr_config: Revenue requirement dict from
            :func:`~utils.post.validate.load.load_revenue_requirement`.
        cost_scope: ``"delivery"`` or ``"delivery+supply"`` — selects which RR key
            to compare against.
        tolerance_pct: Allowed deviation from the RR target as a percentage.

    Returns:
        PASS when total weighted bills are within ``tolerance_pct``% of the RR
        target; FAIL otherwise.
    """
    target = _rr_target(rr_config, cost_scope)
    total: float = _collect(
        _weighted_annual_bills(bills, metadata).select(
            (pl.col(_BILL_COL) * pl.col(_WEIGHT_COL)).sum().alias("v")
        )
    )["v"][0]
    pct_diff = (total - target) / target * 100
    return CheckResult(
        name="revenue_neutrality",
        status="PASS" if abs(pct_diff) <= tolerance_pct else "FAIL",
        message=f"Total weighted bills ${total:,.0f} vs RR ${target:,.0f} ({pct_diff:+.2f}%)",
        details={
            "total_weighted_bills": total,
            "target_rr": target,
            "pct_diff": pct_diff,
            "cost_scope": cost_scope,
        },
    )


def check_subclass_revenue_neutrality(
    bills: pl.LazyFrame,
    metadata: pl.LazyFrame,
    subclass_rr: dict[str, Any],
    cost_scope: str = "delivery",
    tolerance_pct: float = 0.5,
) -> CheckResult:
    """Check per-subclass weighted bills against subclass revenue requirements.

    Expected for runs 5-6 where each subclass (HP / non-HP) has its own
    calibrated revenue requirement.  The subclass mapping assumes
    ``postprocess_group.has_hp = True`` → ``"hp"`` and ``False`` → ``"non-hp"``.

    Args:
        bills: LazyFrame from :func:`~utils.post.validate.load.load_bills`.
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.
        subclass_rr: Revenue requirement dict (subclass YAML variant) containing a
            ``subclass_revenue_requirements`` key mapping subclass names
            (``"hp"``, ``"non-hp"``) to ``{delivery, supply, total}`` dicts.
        cost_scope: ``"delivery"`` or ``"delivery+supply"`` — selects the
            ``"delivery"`` or ``"total"`` key within each subclass entry.
        tolerance_pct: Allowed deviation per subclass as a percentage.

    Returns:
        PASS when every subclass is within tolerance; FAIL if any exceed it.
    """
    rr_sub: dict[str, Any] = subclass_rr.get(
        "subclass_revenue_requirements", subclass_rr
    )
    rr_key = "total" if cost_scope == "delivery+supply" else "delivery"
    actual_by_subclass = _weighted_annual_revenue_by_named_subclass(bills, metadata)

    per_subclass: dict[str, dict[str, Any]] = {}
    missing_subclasses: list[str] = []
    for sub, target_cfg in rr_sub.items():
        target = float(target_cfg[rr_key])
        actual = actual_by_subclass.get(sub)
        if actual is None:
            missing_subclasses.append(sub)
            per_subclass[sub] = {
                "actual": None,
                "target": target,
                "pct_diff": None,
                "pass": False,
                "missing": True,
            }
            continue

        pct_diff = (actual - target) / target * 100
        per_subclass[sub] = {
            "actual": actual,
            "target": target,
            "pct_diff": pct_diff,
            "pass": abs(pct_diff) <= tolerance_pct,
        }

    all_pass = not missing_subclasses and all(v["pass"] for v in per_subclass.values())
    summary = "; ".join(
        f"{s}: missing" if v.get("missing") else f"{s}: {float(v['pct_diff']):+.2f}%"
        for s, v in per_subclass.items()
    )
    message = f"Subclass revenue deviations — {summary}"
    if missing_subclasses:
        message += " — FAIL: missing subclasses " + ", ".join(
            sorted(missing_subclasses)
        )
    return CheckResult(
        name="subclass_revenue_neutrality",
        status="PASS" if all_pass else "FAIL",
        message=message,
        details={"subclasses": per_subclass, "cost_scope": cost_scope},
    )


def check_flex_subclass_revenue_expectations(
    bills: pl.LazyFrame,
    metadata: pl.LazyFrame,
    subclass_rr: dict[str, Any],
    cost_scope: str = "delivery",
    nonhp_tolerance_pct: float = 0.5,
    hp_positive_tolerance_pct: float = 0.05,
) -> CheckResult:
    """Check demand-flex subclass revenue behavior against the no-flex baseline.

    Demand-flex runs intentionally do not preserve no-flex subclass revenue
    neutrality. The model holds non-TOU subclasses harmless at their baseline RR
    and lets the TOU subclass absorb the reduction in total RR from load shifting.

    Expected behavior:
    - ``non-hp`` remains approximately revenue-neutral relative to the baseline
    - ``hp`` revenue deviation is non-positive relative to the baseline
    """
    rr_sub: dict[str, Any] = subclass_rr.get(
        "subclass_revenue_requirements", subclass_rr
    )
    rr_key = "total" if cost_scope == "delivery+supply" else "delivery"
    actual_by_subclass = _weighted_annual_revenue_by_named_subclass(bills, metadata)

    per_subclass: dict[str, dict[str, Any]] = {}
    missing_subclasses: list[str] = []
    for sub, target_cfg in rr_sub.items():
        target = float(target_cfg[rr_key])
        actual = actual_by_subclass.get(sub)
        if actual is None:
            missing_subclasses.append(sub)
            per_subclass[sub] = {
                "actual": None,
                "target": target,
                "pct_diff": None,
                "missing": True,
            }
            continue

        pct_diff = (actual - target) / target * 100
        per_subclass[sub] = {
            "actual": actual,
            "target": target,
            "pct_diff": pct_diff,
        }

    nonhp = per_subclass.get("non-hp")
    hp = per_subclass.get("hp")
    nonhp_ok = (
        nonhp is not None
        and not nonhp.get("missing")
        and abs(float(nonhp["pct_diff"])) <= nonhp_tolerance_pct
    )
    hp_ok = (
        hp is not None
        and not hp.get("missing")
        and float(hp["pct_diff"]) <= hp_positive_tolerance_pct
    )

    parts: list[str] = []
    if nonhp is not None:
        parts.append(
            "non-hp: missing"
            if nonhp.get("missing")
            else f"non-hp: {float(nonhp['pct_diff']):+.2f}%"
        )
    if hp is not None:
        parts.append(
            "hp: missing" if hp.get("missing") else f"hp: {float(hp['pct_diff']):+.2f}%"
        )

    failures: list[str] = []
    if missing_subclasses:
        failures.append("missing subclasses " + ", ".join(sorted(missing_subclasses)))
    elif not nonhp_ok and nonhp is not None:
        failures.append(
            f"non-hp should stay within ±{nonhp_tolerance_pct:.2f}% "
            f"(got {float(nonhp['pct_diff']):+.2f}%)"
        )
    if not hp_ok and hp is not None and not hp.get("missing"):
        failures.append(
            f"hp should not exceed baseline RR (got {float(hp['pct_diff']):+.2f}%)"
        )

    return CheckResult(
        name="subclass_revenue_expectations_flex",
        status="PASS" if not failures else "FAIL",
        message="Demand-flex subclass revenue deviations — "
        + "; ".join(parts)
        + (f" — FAIL: {'; '.join(failures)}" if failures else ""),
        details={
            "subclasses": per_subclass,
            "cost_scope": cost_scope,
            "nonhp_tolerance_pct": nonhp_tolerance_pct,
            "hp_positive_tolerance_pct": hp_positive_tolerance_pct,
        },
    )


def check_hp_subclass_revenue_lower_with_flex(
    bills_noflex: pl.LazyFrame,
    bills_flex: pl.LazyFrame,
    metadata_noflex: pl.LazyFrame,
    metadata_flex: pl.LazyFrame,
    run_noflex: int,
    run_flex: int,
) -> CheckResult:
    """Check that demand flex lowers weighted HP subclass revenue.

    Compares a flex run against the matching no-flex run with the same upgrade,
    cost scope, and subclass structure. The HP subclass should collect less
    weighted revenue after elasticity is applied.
    """
    revenue_noflex = _weighted_annual_revenue_by_subclass(bills_noflex, metadata_noflex)
    revenue_flex = _weighted_annual_revenue_by_subclass(bills_flex, metadata_flex)
    if True not in revenue_noflex or True not in revenue_flex:
        missing_runs = [
            str(run)
            for run, revenue in (
                (run_noflex, revenue_noflex),
                (run_flex, revenue_flex),
            )
            if True not in revenue
        ]
        return CheckResult(
            name=f"hp_revenue_lower_with_flex_run{run_noflex}_to_run{run_flex}",
            status="FAIL",
            message="Missing HP subclass revenue for run(s): "
            + ", ".join(missing_runs),
            details={
                "run_noflex": run_noflex,
                "run_flex": run_flex,
                "missing_hp_runs": [int(run) for run in missing_runs],
            },
        )

    hp_noflex = float(revenue_noflex.get(True, float("nan")))
    hp_flex = float(revenue_flex.get(True, float("nan")))
    delta = hp_flex - hp_noflex
    pct_change = delta / hp_noflex * 100 if hp_noflex else float("nan")

    return CheckResult(
        name=f"hp_revenue_lower_with_flex_run{run_noflex}_to_run{run_flex}",
        status="PASS" if hp_flex < hp_noflex else "FAIL",
        message=(
            f"HP weighted revenue run {run_noflex}→{run_flex}: "
            f"${hp_noflex:,.0f} → ${hp_flex:,.0f} ({pct_change:+.2f}%)"
        ),
        details={
            "run_noflex": run_noflex,
            "run_flex": run_flex,
            "hp_revenue_noflex": hp_noflex,
            "hp_revenue_flex": hp_flex,
            "delta": delta,
            "pct_change": pct_change,
        },
    )


def check_subclass_rr_sums_to_total(
    subclass_rr: dict[str, Any],
    total_rr: dict[str, Any],
    cost_scope: str = "delivery",
) -> CheckResult:
    """Check that subclass revenue requirements sum to the total topped-up RR.

    Validates internal consistency between the subclass YAML (produced by
    ``compute-subclass-rr``) and the total RR YAML (from the rate case).

    Args:
        subclass_rr: Revenue requirement dict (subclass YAML variant) containing
            ``subclass_revenue_requirements``.
        total_rr: Total revenue requirement dict (non-subclass YAML).
        cost_scope: ``"delivery"`` or ``"delivery+supply"`` — selects the
            ``"delivery"`` or ``"total"`` key within each subclass entry, and the
            corresponding key in ``total_rr``.

    Returns:
        PASS when the subclass sum matches the total RR within $1 (floating-point
        rounding tolerance); FAIL otherwise.
    """
    rr_sub: dict[str, Any] = subclass_rr.get(
        "subclass_revenue_requirements", subclass_rr
    )
    rr_key = "total" if cost_scope == "delivery+supply" else "delivery"
    sub_sum = sum(float(v[rr_key]) for v in rr_sub.values())
    total = _rr_target(total_rr, cost_scope)
    diff = abs(sub_sum - total)
    return CheckResult(
        name="subclass_rr_sums_to_total",
        status="PASS" if diff <= 1.0 else "FAIL",
        message=f"Subclass RR sum ${sub_sum:,.0f} vs total RR ${total:,.0f} (diff ${diff:,.2f})",
        details={
            "subclass_sum": sub_sum,
            "total_rr": total,
            "diff": diff,
            "cost_scope": cost_scope,
        },
    )


def check_bat_direction(bat: pl.LazyFrame, metadata: pl.LazyFrame) -> CheckResult:
    """Report weighted-average BAT direction by HP/non-HP subclass.

    This is an informational check that always returns PASS; it captures the
    direction and magnitude of cross-subsidization per benchmark so downstream
    reports can verify the expected sign (e.g. HP customers underpaying on the
    volumetric benchmark under a flat rate).

    Positive BAT = the group is overpaying relative to cost causation;
    negative = underpaying.

    Args:
        bat: LazyFrame from :func:`~utils.post.validate.load.load_bat`
            (columns: ``bldg_id``, ``BAT_percustomer``, ``BAT_vol``, ``BAT_peak``).
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        PASS (always); details contain weighted-average BAT per group and benchmark.
    """
    rows = _bat_wavg_by_group(bat, metadata).to_dicts()
    parts = [
        f"{'HP' if r[_HP_COL] else 'Non-HP'}: ${r.get('BAT_percustomer_wavg', float('nan')):+.0f}/cust-yr"
        for r in rows
    ]
    return CheckResult(
        name="bat_direction",
        status="PASS",
        message="BAT direction — " + "; ".join(parts),
        details={"by_subclass": rows},
    )


def check_bat_near_zero(
    bat: pl.LazyFrame,
    metadata: pl.LazyFrame,
    tolerance_usd: float = 5.0,
) -> CheckResult:
    """Check that per-customer BAT is near zero for all subclasses.

    Expected for subclass precalc runs (5-6): each subclass is calibrated to its
    own marginal costs, so per-customer residual cross-subsidization should be
    eliminated across all three benchmarks (volumetric, peak, per-customer).

    Args:
        bat: LazyFrame from :func:`~utils.post.validate.load.load_bat`.
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.
        tolerance_usd: Maximum allowed absolute weighted-average BAT per customer
            ($/customer-year) for a PASS.

    Returns:
        PASS when ``|BAT_percustomer_wavg| ≤ tolerance_usd`` for all groups;
        FAIL otherwise.
    """
    by_group = [
        {
            "group": "HP" if row[_HP_COL] else "Non-HP",
            "BAT_percustomer_wavg": (
                pc := row.get("BAT_percustomer_wavg", float("nan"))
            ),
            "pass": abs(pc) <= tolerance_usd,
        }
        for row in _bat_wavg_by_group(bat, metadata).iter_rows(named=True)
    ]
    parts = [f"{d['group']}: ${d['BAT_percustomer_wavg']:+.1f}" for d in by_group]
    return CheckResult(
        name="bat_near_zero",
        status="PASS" if all(d["pass"] for d in by_group) else "FAIL",
        message=f"Per-customer BAT (tolerance ±${tolerance_usd}) — " + "; ".join(parts),
        details={"by_group": by_group, "tolerance_usd": tolerance_usd},
    )


def _norm_tariff_key(key: str) -> str:
    """Strip the ``_calibrated`` suffix CAIRO appends when copying a tariff.

    Precalc runs emit ``tariff_final_config.json`` with the original key (e.g.
    ``cenhud_flat``), while the default run that inherits that tariff emits the
    same JSON with the key renamed to ``cenhud_flat_calibrated``.  Normalizing
    both sides lets us compare numeric values across that rename.
    """
    return key.removesuffix("_calibrated")


def check_tariff_unchanged(
    input_tariff: dict[str, Any],
    output_tariff: dict[str, Any],
    tolerance: float = 1e-6,
) -> CheckResult:
    """Check that the output tariff rates exactly match the input tariff.

    Used for default runs (3-4, 7-8): their ``tariff_final_config.json`` must be
    identical to the calibrated output of the preceding precalc run because CAIRO
    applies the inherited tariff without modification.  Compares all numeric leaf
    values in the CAIRO tariff JSON recursively using an absolute tolerance.

    Keys are compared after stripping the ``_calibrated`` suffix that CAIRO
    appends when a tariff is copied from a precalc run to a default run, so a
    rename of ``cenhud_flat`` → ``cenhud_flat_calibrated`` does not cause a false
    failure.

    The comparison is anchored on the **output** keys: every tariff key emitted
    by the default run must match the corresponding precalc input key.  Input keys
    that are absent from the output are not flagged — an upgrade 02 run, for
    example, only emits the HP-subclass tariff key and omits the non-HP flat-rate
    key, which is expected behaviour and should not produce a false failure.

    Args:
        input_tariff: Tariff config dict (e.g. from the precalc
            ``tariff_final_config.json`` or a ``*_calibrated.json`` file).
        output_tariff: Tariff config dict from the default run.
        tolerance: Absolute tolerance for numeric comparisons.

    Returns:
        PASS when every numeric value in the output matches the input within
        ``tolerance``; FAIL if any value differs or if an output key has no
        corresponding entry in the input.
    """
    # Build a normalized lookup for the input so output keys (which may carry
    # the _calibrated suffix) can be matched against their precalc counterparts.
    input_by_norm: dict[str, Any] = {
        _norm_tariff_key(k): v for k, v in input_tariff.items()
    }

    diffs: list[dict[str, Any]] = []
    for key in output_tariff:
        norm_key = _norm_tariff_key(key)
        if norm_key not in input_by_norm:
            diffs.append({"path": key, "issue": "key in output not found in input"})
            continue
        in_entry = input_by_norm[norm_key]
        for path, out_val in _flatten_numeric(output_tariff[key]).items():
            in_val = _flatten_numeric(in_entry).get(path)
            if in_val is None:
                diffs.append(
                    {"path": f"{key}.{path}", "input": None, "output": out_val}
                )
            elif abs(in_val - out_val) > tolerance:
                diffs.append(
                    {
                        "path": f"{key}.{path}",
                        "input": in_val,
                        "output": out_val,
                        "diff": out_val - in_val,
                    }
                )
    return CheckResult(
        name="tariff_unchanged",
        status="PASS" if not diffs else "FAIL",
        message=f"Tariff unchanged: {len(diffs)} numeric difference(s) found",
        details={"differences": diffs},
    )


def check_nonhp_calibrated_above_original(
    calibrated_tariff: dict[str, Any],
    original_tariff: dict[str, Any],
) -> CheckResult:
    """Check that the non-HP flat rate increased after subclass calibration (runs 5-6).

    In subclass precalc runs, the non-HP subclass receives a re-calibrated flat
    tariff to recover the non-HP revenue requirement.  Because HP customers are
    removed to their own seasonal subclass, the remaining non-HP RR is spread over
    a set of customers with different load profiles, so the non-HP flat rate should
    be higher than (or equal to) the original undifferentiated flat rate.

    Compares the primary volumetric rate (``ur_ec_tou_mat[0][4]``) from the first
    key of each tariff dict.

    Args:
        calibrated_tariff: Tariff config dict containing the recalibrated non-HP
            entry (first key is used).
        original_tariff: Original undifferentiated tariff config dict (first key is
            used as the baseline).

    Returns:
        PASS when calibrated rate > original; WARN when equal (no change); FAIL
        when the calibrated rate is lower than the original.
    """
    cal_rate = _primary_vol_rate(calibrated_tariff)
    orig_rate = _primary_vol_rate(original_tariff)
    delta = cal_rate - orig_rate
    status: CheckStatus = "PASS" if delta > 0 else ("WARN" if delta == 0 else "FAIL")
    messages = {
        "PASS": f"Non-HP rate increased: ${orig_rate:.6f} → ${cal_rate:.6f} /kWh (+${delta:.6f})",
        "WARN": f"Non-HP rate unchanged at ${cal_rate:.6f} /kWh",
        "FAIL": f"Non-HP rate DECREASED: ${orig_rate:.6f} → ${cal_rate:.6f} /kWh (${delta:.6f})",
    }
    return CheckResult(
        name="nonhp_calibrated_above_original",
        status=status,
        message=messages[status],
        details={
            "calibrated_rate": cal_rate,
            "original_rate": orig_rate,
            "delta": delta,
        },
    )


def check_output_completeness(s3_dir: str) -> CheckResult:
    """Check that all expected output files exist in the run directory.

    Verifies the presence of bills CSVs, the BAT CSV, customer metadata, and the
    tariff config JSON using S3 ``head_object`` calls.

    Args:
        s3_dir: Full ``s3://`` URI to the run directory (no trailing slash).

    Returns:
        PASS when all expected files are present; FAIL if any are missing.
    """
    bucket, _, prefix = s3_dir.removeprefix("s3://").partition("/")
    s3 = boto3.client("s3")

    def _exists(rel: str) -> bool:
        try:
            s3.head_object(Bucket=bucket, Key=f"{prefix}/{rel}")
            return True
        except ClientError:
            return False

    missing = [f for f in _EXPECTED_FILES if not _exists(f)]
    return CheckResult(
        name="output_completeness",
        status="PASS" if not missing else "FAIL",
        message=f"{len(_EXPECTED_FILES) - len(missing)}/{len(_EXPECTED_FILES)} expected files present",
        details={"missing": missing, "expected": list(_EXPECTED_FILES)},
    )


def check_nonhp_customers_in_upgrade02(metadata: pl.LazyFrame) -> CheckResult:
    """Summarize non-HP customers in an upgrade-02 run by heating type.

    In upgrade 02 all HP-eligible buildings are converted from their baseline
    heating type to heat pumps.  This check counts the remaining non-HP customers
    (e.g. gas, resistance, oil) by heating type, which is informational but can
    reveal unexpected data issues such as missing upgrades or metadata mismatches.

    Args:
        metadata: LazyFrame with ``postprocess_group.has_hp``,
            ``postprocess_group.heating_type``, and ``weight`` columns.

    Returns:
        PASS (always); details contain weighted customer counts by heating type.
    """
    rows = _collect(
        metadata.filter(~pl.col(_HP_COL))
        .group_by(_HEATING_TYPE_COL)
        .agg(
            pl.len().alias("count"),
            pl.col(_WEIGHT_COL).sum().alias("customers_weighted"),
        )
        .sort(_HEATING_TYPE_COL)
    ).to_dicts()
    total_weighted: float = sum(r["customers_weighted"] for r in rows)
    summary = ", ".join(
        f"{r[_HEATING_TYPE_COL]}: {r['customers_weighted']:,.0f}" for r in rows
    )
    return CheckResult(
        name="nonhp_customers_in_upgrade02",
        status="PASS",
        message=f"Non-HP customers ({total_weighted:,.0f} weighted total): {summary}",
        details={"by_heating_type": rows, "total_weighted": total_weighted},
    )


# ---------------------------------------------------------------------------
# Cross-run check helpers
# ---------------------------------------------------------------------------

# Month indices for season classification (0 = Jan … 11 = Dec)
_SUMMER_MONTHS: frozenset[int] = frozenset({5, 6, 7, 8})  # Jun–Sep
_WINTER_MONTHS: frozenset[int] = frozenset({11, 0, 1, 2})  # Dec–Mar


def _period_season_map(schedule: list[list[int]]) -> dict[int, str]:
    """Map 1-based CAIRO period IDs to ``'summer'`` or ``'winter'``.

    Args:
        schedule: ``ur_ec_sched_weekday`` — a 12×24 list of 1-based period IDs
            (one row per month, one value per hour).  Months with indices in
            ``_SUMMER_MONTHS`` (Jun–Sep) count toward summer; months in
            ``_WINTER_MONTHS`` (Dec–Mar) count toward winter.  Shoulder months
            (Apr–May, Oct–Nov) are ignored for classification purposes.

    Returns:
        ``{period_id: 'summer' | 'winter'}`` for every period that appears in
        a summer or winter month.  Periods that appear only in shoulder months
        are omitted.
    """
    counts: dict[int, dict[str, int]] = {}
    for month_idx, hours in enumerate(schedule):
        season = (
            "summer"
            if month_idx in _SUMMER_MONTHS
            else "winter"
            if month_idx in _WINTER_MONTHS
            else None
        )
        if season is None:
            continue
        for period_id in set(hours):
            bucket = counts.setdefault(period_id, {"summer": 0, "winter": 0})
            bucket[season] += 1
    return {
        pid: ("summer" if c["summer"] >= c["winter"] else "winter")
        for pid, c in counts.items()
    }


def _season_period_rates(
    schedule: list[list[int]],
    tier1_rates: dict[int, float],
) -> dict[str, list[tuple[int, float]]]:
    """Return tier-1 rates grouped by inferred season."""
    season_map = _period_season_map(schedule)
    grouped: dict[str, list[tuple[int, float]]] = {"winter": [], "summer": []}
    for pid, season in season_map.items():
        if pid in tier1_rates:
            grouped[season].append((pid, tier1_rates[pid]))
    for season in grouped:
        grouped[season].sort(key=lambda item: item[0])
    return grouped


# ---------------------------------------------------------------------------
# Cross-run check functions
# ---------------------------------------------------------------------------


def check_bills_increase_with_supply(
    bills_a: pl.LazyFrame,
    bills_b: pl.LazyFrame,
    metadata: pl.LazyFrame,
    run_a: int,
    run_b: int,
) -> CheckResult:
    """Check that weighted mean combined bills rise for both subclasses when supply is added.

    Expected when comparing delivery+supply run (B) against a delivery-only run (A)
    at the same upgrade level (e.g. run 2 vs run 1, or run 6 vs run 5).  Adding
    supply cost recovery should push bills up for every subclass.

    Args:
        bills_a: Combined (elec+gas) bills LazyFrame for the delivery-only run.
        bills_b: Combined (elec+gas) bills LazyFrame for the delivery+supply run.
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns (same customers in both runs).
        run_a: Run number for the delivery-only run (for logging).
        run_b: Run number for the delivery+supply run (for logging).

    Returns:
        PASS when both HP and non-HP weighted mean annual bills are strictly
        higher in run B; FAIL if either subclass shows equal or lower bills.
    """

    def _wavg_by_subclass(bills: pl.LazyFrame) -> dict[bool, float]:
        rows = _collect(
            bills.filter(pl.col(_MONTH_COL) == _ANNUAL_MONTH)
            .join(metadata.select([_BLDG_COL, _WEIGHT_COL, _HP_COL]), on=_BLDG_COL)
            .group_by(_HP_COL)
            .agg(
                (
                    (pl.col(_BILL_COL) * pl.col(_WEIGHT_COL)).sum()
                    / pl.col(_WEIGHT_COL).sum()
                ).alias("wavg_bill")
            )
        )
        return {row[_HP_COL]: row["wavg_bill"] for row in rows.iter_rows(named=True)}

    a_by_sub = _wavg_by_subclass(bills_a)
    b_by_sub = _wavg_by_subclass(bills_b)

    details: list[dict[str, Any]] = []
    failures: list[str] = []
    for hp_val in sorted(a_by_sub.keys() | b_by_sub.keys()):
        label = "HP" if hp_val else "Non-HP"
        a_val = a_by_sub.get(hp_val, float("nan"))
        b_val = b_by_sub.get(hp_val, float("nan"))
        ok = b_val > a_val
        details.append(
            {
                "subclass": label,
                f"run{run_a}_avg_bill": a_val,
                f"run{run_b}_avg_bill": b_val,
                "increased": ok,
            }
        )
        if not ok:
            failures.append(f"{label}: ${a_val:,.2f} → ${b_val:,.2f}")

    subclass_summary = "; ".join(
        f"{'HP' if d['subclass'] == 'HP' else 'Non-HP'} ${d[f'run{run_a}_avg_bill']:,.2f} → ${d[f'run{run_b}_avg_bill']:,.2f}"
        for d in details
    )
    return CheckResult(
        name=f"bills_increase_run{run_a}_to_run{run_b}",
        status="PASS" if not failures else "FAIL",
        message=f"Combined bills run {run_a}→{run_b} — {subclass_summary}"
        + (f" — FAIL: {'; '.join(failures)}" if failures else ""),
        details={"subclasses": details, "run_a": run_a, "run_b": run_b},
    )


def check_seasonal_winter_below_summer(
    tariff_config: dict[str, Any],
    run_num: int,
) -> CheckResult:
    """Check seasonal tariff ordering for flat and TOU seasonal tariffs.

    Derives the period-to-season mapping from ``ur_ec_sched_weekday`` (CAIRO's
    12×24 1-based schedule) and compares tier-1 rates from ``ur_ec_tou_mat``.
    Prints the full period mapping to stdout so the reviewer can verify it.

    Period numbers are CAIRO's normalized 1-based IDs. They do not imply a
    semantic season order: a source tariff whose raw schedule uses ``0`` for
    summer and ``1`` for winter will appear here as period 1 = summer,
    period 2 = winter after normalization.

    Rules:
    - Seasonal flat: summer rate must exceed winter rate.
    - Seasonal TOU / flex:
      - winter peak > winter off-peak
      - summer peak > summer off-peak
      - summer peak > winter peak

    This intentionally does *not* require every summer period to exceed every
    winter period. In a 4-period seasonal TOU tariff, summer off-peak may be
    below winter peak by design.

    Args:
        tariff_config: CAIRO internal tariff config dict (from
            :func:`~utils.post.validate.load.load_tariff_config`).
        run_num: Run number, used in the check name and log lines.

    Returns:
        PASS when the tariff satisfies the season-appropriate ordering rules;
        FAIL otherwise.
    """
    failures: list[str] = []
    all_period_lines: list[str] = []

    for key, entry in tariff_config.items():
        schedule = entry.get("ur_ec_sched_weekday")
        tou_mat = entry.get("ur_ec_tou_mat", [])
        if not schedule or not tou_mat:
            continue

        season_map = _period_season_map(schedule)

        # Collect tier-1 rates per period (1-based period IDs in ur_ec_tou_mat)
        tier1_rates: dict[int, float] = {
            int(row[0]): float(row[4])
            for row in tou_mat
            if int(row[1]) == 1  # tier == 1
        }

        # Log period → season → rate mapping
        for pid in sorted(season_map):
            season = season_map[pid]
            rate = tier1_rates.get(pid, float("nan"))
            line = f"    run {run_num} {key} period {pid} → {season}: ${rate:.6f}/kWh"
            all_period_lines.append(line)

        season_rates = _season_period_rates(schedule, tier1_rates)
        winter_rates = season_rates["winter"]
        summer_rates = season_rates["summer"]

        # Skip non-seasonal companion tariffs (for example, the non-HP flat tariff
        # that appears alongside HP seasonal/TOU tariffs in subclass runs).
        if not winter_rates or not summer_rates:
            continue

        if len(winter_rates) == 1 and len(summer_rates) == 1:
            winter_pid, winter_rate = winter_rates[0]
            summer_pid, summer_rate = summer_rates[0]
            if winter_rate >= summer_rate:
                failures.append(
                    f"{key}: winter period {winter_pid} (${winter_rate:.6f}) "
                    f"≥ summer period {summer_pid} (${summer_rate:.6f})"
                )
            continue

        if len(winter_rates) == 2 and len(summer_rates) == 2:
            winter_off_pid, winter_off = min(winter_rates, key=lambda item: item[1])
            winter_peak_pid, winter_peak = max(winter_rates, key=lambda item: item[1])
            summer_off_pid, summer_off = min(summer_rates, key=lambda item: item[1])
            summer_peak_pid, summer_peak = max(summer_rates, key=lambda item: item[1])

            if winter_peak <= winter_off:
                failures.append(
                    f"{key}: winter peak period {winter_peak_pid} (${winter_peak:.6f}) "
                    f"≤ winter off-peak period {winter_off_pid} (${winter_off:.6f})"
                )
            if summer_peak <= summer_off:
                failures.append(
                    f"{key}: summer peak period {summer_peak_pid} (${summer_peak:.6f}) "
                    f"≤ summer off-peak period {summer_off_pid} (${summer_off:.6f})"
                )
            if summer_peak <= winter_peak:
                failures.append(
                    f"{key}: summer peak period {summer_peak_pid} (${summer_peak:.6f}) "
                    f"≤ winter peak period {winter_peak_pid} (${winter_peak:.6f})"
                )
            continue

        failures.append(
            f"{key}: unexpected seasonal period structure "
            f"(winter={len(winter_rates)} periods, summer={len(summer_rates)} periods)"
        )

    for line in all_period_lines:
        print(line)

    return CheckResult(
        name=f"seasonal_winter_below_summer_run{run_num}",
        status="PASS" if not failures else "FAIL",
        message=(
            f"Seasonal rates run {run_num}: ordering checks passed"
            if not failures
            else f"Seasonal rate ordering FAIL — {'; '.join(failures)}"
        ),
        details={"failures": failures, "period_mapping": all_period_lines},
    )


def check_hp_bat_increases_with_supply(
    bat_a: pl.LazyFrame,
    bat_b: pl.LazyFrame,
    metadata: pl.LazyFrame,
    run_a: int,
    run_b: int,
) -> CheckResult:
    """Warn if HP cross-subsidy magnitude does not increase when supply is added.

    HP customers underpay under a flat rate (``BAT_percustomer < 0``).  Adding
    supply costs (run B vs run A) exposes additional cost causation, so the
    per-customer cross-subsidy for HP customers should deepen — i.e.
    ``|BAT_percustomer_wavg|`` should be larger in run B.

    This is a sanity signal, not a hard contract: WARN (never FAIL).

    Args:
        bat_a: BAT LazyFrame for the delivery-only run.
        bat_b: BAT LazyFrame for the delivery+supply run.
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns (same customers in both runs).
        run_a: Run number for the delivery-only run (for logging).
        run_b: Run number for the delivery+supply run (for logging).

    Returns:
        PASS when ``|BAT_percustomer_wavg|`` is strictly larger for HP in run B;
        WARN otherwise.
    """
    if (
        "BAT_percustomer" not in bat_a.collect_schema()
        or "BAT_percustomer" not in bat_b.collect_schema()
    ):
        return CheckResult(
            name=f"hp_bat_increases_run{run_a}_to_run{run_b}",
            status="WARN",
            message=f"BAT_percustomer column missing, cannot compare run {run_a} vs {run_b}",
            details={},
        )

    def _hp_bat_wavg(bat: pl.LazyFrame) -> float:
        rows = _collect(
            bat.join(metadata.select([_BLDG_COL, _WEIGHT_COL, _HP_COL]), on=_BLDG_COL)
            .filter(pl.col(_HP_COL))
            .select(
                (
                    (pl.col("BAT_percustomer") * pl.col(_WEIGHT_COL)).sum()
                    / pl.col(_WEIGHT_COL).sum()
                ).alias("wavg")
            )
        )
        return float(rows["wavg"][0])

    bat_a_hp = _hp_bat_wavg(bat_a)
    bat_b_hp = _hp_bat_wavg(bat_b)
    increased = abs(bat_b_hp) > abs(bat_a_hp)
    return CheckResult(
        name=f"hp_bat_increases_run{run_a}_to_run{run_b}",
        status="PASS" if increased else "WARN",
        message=(
            f"HP BAT_percustomer run {run_a}→{run_b}: "
            f"${bat_a_hp:+.2f} → ${bat_b_hp:+.2f} "
            f"(|Δ|={'increased' if increased else 'did NOT increase'})"
        ),
        details={
            f"hp_bat_wavg_run{run_a}": bat_a_hp,
            f"hp_bat_wavg_run{run_b}": bat_b_hp,
            "magnitude_increased": increased,
        },
    )


def check_weights_sum_to_n_customers(metadata: pl.LazyFrame) -> CheckResult:
    """Check that HP + non-HP weights sum to the total weight.

    Validates consistency of CAIRO re-weighted metadata: the sum of weights for
    HP customers plus the sum of weights for non-HP customers should equal the
    total sum of all weights (within floating-point tolerance).

    Args:
        metadata: LazyFrame with ``bldg_id``, ``weight``, and
            ``postprocess_group.has_hp`` columns.

    Returns:
        PASS when HP + non-HP weights sum to total within tolerance (1e-3);
        FAIL otherwise. Details contain ``n_buildings``, ``n_customers_weighted_hp``,
        ``n_customers_weighted_nonhp``, ``n_customers_weighted_total``.
    """
    meta_collected = _collect(metadata)
    total_buildings = len(meta_collected)
    total_weighted = meta_collected[_WEIGHT_COL].sum()

    hp_weighted = meta_collected.filter(pl.col(_HP_COL))[_WEIGHT_COL].sum()
    nonhp_weighted = meta_collected.filter(~pl.col(_HP_COL))[_WEIGHT_COL].sum()

    sum_hp_nonhp = hp_weighted + nonhp_weighted
    diff = abs(sum_hp_nonhp - total_weighted)
    tolerance = 1e-3

    return CheckResult(
        name="weights_sum_to_n_customers",
        status="PASS" if diff <= tolerance else "FAIL",
        message=(
            f"Weight sum check: HP ({hp_weighted:,.0f}) + Non-HP ({nonhp_weighted:,.0f}) "
            f"= {sum_hp_nonhp:,.0f} vs Total ({total_weighted:,.0f}), diff={diff:.6f}"
        ),
        details={
            "n_buildings": total_buildings,
            "n_customers_weighted_hp": hp_weighted,
            "n_customers_weighted_nonhp": nonhp_weighted,
            "n_customers_weighted_total": total_weighted,
            "diff": diff,
        },
    )
