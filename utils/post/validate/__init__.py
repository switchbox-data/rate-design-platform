"""Run validation framework for HP rate design CAIRO runs.

This package validates CAIRO runs 1-8 for any utility by reading outputs from S3,
running structured checks (revenue neutrality, BAT direction, tariff stability),
and generating plotnine plots + summary CSVs.

See issue #324 for implementation plan.
"""
