"""Run validation framework for HP rate design CAIRO runs.

This package validates CAIRO runs 1-8 for any utility by reading outputs from S3,
running structured checks (revenue neutrality, BAT direction, tariff stability),
and generating plotnine plots + summary CSVs.

See issue #324 for implementation plan.
"""

from utils.post.validate.checks import (
    CheckResult,
    CheckStatus,
    check_bat_direction,
    check_bat_near_zero,
    check_nonhp_calibrated_above_original,
    check_nonhp_customers_in_upgrade02,
    check_output_completeness,
    check_revenue_neutrality,
    check_subclass_revenue_neutrality,
    check_subclass_rr_sums_to_total,
    check_tariff_unchanged,
)
from utils.post.validate.load import (
    load_bat,
    load_bills,
    load_input_tariff,
    load_metadata,
    load_revenue_requirement,
    load_seasonal_discount_inputs,
    load_tariff_config,
)
from utils.post.validate.tables import (
    compute_bill_deltas,
    summarize_bat_by_subclass,
    summarize_bills_by_subclass,
    summarize_nonhp_composition,
    summarize_revenue,
    summarize_tariff_rates,
)

__all__ = [
    # checks
    "CheckResult",
    "CheckStatus",
    "check_bat_direction",
    "check_bat_near_zero",
    "check_nonhp_calibrated_above_original",
    "check_nonhp_customers_in_upgrade02",
    "check_output_completeness",
    "check_revenue_neutrality",
    "check_subclass_revenue_neutrality",
    "check_subclass_rr_sums_to_total",
    "check_tariff_unchanged",
    # load
    "load_bat",
    "load_bills",
    "load_input_tariff",
    "load_metadata",
    "load_revenue_requirement",
    "load_seasonal_discount_inputs",
    "load_tariff_config",
    # tables
    "compute_bill_deltas",
    "summarize_bat_by_subclass",
    "summarize_bills_by_subclass",
    "summarize_nonhp_composition",
    "summarize_revenue",
    "summarize_tariff_rates",
]
