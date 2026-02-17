"""Validate the EIA-861 yearly sales dataset used by utils/get_utility_stats_from_eia861.py.

Confirms required columns and customer_class values exist so the script's
dynamic aggregation is safe. Uses the same parquet URL as the script.
"""

import polars as pl

# Must match utils/get_utility_stats_from_eia861.py
CORE_EIA861_YEARLY_SALES_URL = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/core_eia861__yearly_sales.parquet"

REQUIRED_COLUMNS = frozenset(
    {
        "utility_id_eia",
        "state",
        "report_date",
        "entity_type",
        "utility_name_eia",
        "business_model",
        "customer_class",
        "sales_mwh",
        "sales_revenue",
        "customers",
    }
)

# Known customer classes in PUDL EIA-861 yearly sales (verified by inspection)
EXPECTED_CUSTOMER_CLASSES = frozenset(
    {
        "commercial",
        "industrial",
        "other",
        "residential",
        "transportation",
    }
)


def test_eia861_yearly_sales_has_required_columns():
    """Dataset must have all columns the utility stats script aggregates on."""
    df = pl.read_parquet(CORE_EIA861_YEARLY_SALES_URL)
    missing = REQUIRED_COLUMNS - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


def test_eia861_yearly_sales_customer_class_values():
    """customer_class must match expected set so per-class columns are predictable.

    Uses exact set equality: fails if the dataset has extra classes (unaccounted for)
    or is missing any expected class.
    """
    df = pl.read_parquet(CORE_EIA861_YEARLY_SALES_URL)
    classes = df.select("customer_class").unique().to_series().cast(pl.Utf8).to_list()
    actual = frozenset(classes)
    assert actual == EXPECTED_CUSTOMER_CLASSES, (
        f"customer_class values changed: expected {EXPECTED_CUSTOMER_CLASSES}, got {actual}"
    )


def test_eia861_yearly_sales_has_investor_owned_data_for_sample_state():
    """At least one state (NY) has Investor Owned rows with all customer classes."""
    df = (
        pl.read_parquet(CORE_EIA861_YEARLY_SALES_URL)
        .filter(pl.col("state") == "NY")
        .filter(pl.col("entity_type") == "Investor Owned")
    )
    assert df.height > 0, "No NY Investor Owned rows in dataset"
    classes_in_data = (
        df.select("customer_class").unique().to_series().cast(pl.Utf8).to_list()
    )
    assert frozenset(classes_in_data) == EXPECTED_CUSTOMER_CLASSES, (
        f"NY IOU data missing some customer classes: got {set(classes_in_data)}"
    )
