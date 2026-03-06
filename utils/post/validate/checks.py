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
    hp_to_subclass = {True: "hp", False: "non-hp"}

    weighted = _collect(
        bills.filter(pl.col(_MONTH_COL) == _ANNUAL_MONTH)
        .join(metadata.select([_BLDG_COL, _WEIGHT_COL, _HP_COL]), on=_BLDG_COL)
        .group_by(_HP_COL)
        .agg((pl.col(_BILL_COL) * pl.col(_WEIGHT_COL)).sum().alias("weighted_bills"))
    )

    per_subclass: dict[str, dict[str, Any]] = {}
    for row in weighted.iter_rows(named=True):
        sub = hp_to_subclass.get(row[_HP_COL], str(row[_HP_COL]))
        target = float(rr_sub[sub][rr_key])
        actual: float = row["weighted_bills"]
        pct_diff = (actual - target) / target * 100
        per_subclass[sub] = {
            "actual": actual,
            "target": target,
            "pct_diff": pct_diff,
            "pass": abs(pct_diff) <= tolerance_pct,
        }

    all_pass = all(v["pass"] for v in per_subclass.values())
    summary = "; ".join(f"{s}: {v['pct_diff']:+.2f}%" for s, v in per_subclass.items())
    return CheckResult(
        name="subclass_revenue_neutrality",
        status="PASS" if all_pass else "FAIL",
        message=f"Subclass revenue deviations — {summary}",
        details={"subclasses": per_subclass, "cost_scope": cost_scope},
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

    Args:
        input_tariff: Tariff config dict (e.g. from the precalc
            ``tariff_final_config.json`` or a ``*_calibrated.json`` file).
        output_tariff: Tariff config dict from the default run.
        tolerance: Absolute tolerance for numeric comparisons.

    Returns:
        PASS when every numeric value matches within ``tolerance``; FAIL if any
        differ or if a key is missing from the output.
    """
    diffs: list[dict[str, Any]] = []
    for key in input_tariff:
        if key not in output_tariff:
            diffs.append({"path": key, "issue": "key missing from output"})
            continue
        for path, in_val in _flatten_numeric(input_tariff[key]).items():
            out_val = _flatten_numeric(output_tariff[key]).get(path)
            if out_val is None:
                diffs.append({"path": f"{key}.{path}", "input": in_val, "output": None})
            elif abs(in_val - out_val) > tolerance:
                diffs.append(
                    {
                        "path": f"{key}.{path}",
                        "input": in_val,
                        "output": out_val,
                        "diff": in_val - out_val,
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
