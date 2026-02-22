"""Validate the EIA-861 yearly sales dataset used by data/eia/861/fetch_electric_utility_stat_parquets.py.

Confirms required columns and customer_class values exist so the script's
dynamic aggregation is safe. Uses the same parquet URL as the script (stable release).
"""

import subprocess
from io import StringIO
from pathlib import Path

import pytest
import polars as pl

# Must match PUDL_STABLE_VERSION in data/eia/861/fetch_electric_utility_stat_parquets.py
CORE_EIA861_YEARLY_SALES_URL = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/v2026.2.0/core_eia861__yearly_sales.parquet"

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


@pytest.fixture(scope="module")
def eia861_yearly_sales_df() -> pl.DataFrame:
    """Load PUDL EIA-861 yearly sales once per test module to avoid repeated S3 downloads."""
    return pl.read_parquet(CORE_EIA861_YEARLY_SALES_URL)


def test_eia861_yearly_sales_has_required_columns(eia861_yearly_sales_df: pl.DataFrame):
    """Dataset must have all columns the utility stats script aggregates on."""
    missing = REQUIRED_COLUMNS - set(eia861_yearly_sales_df.columns)
    assert not missing, f"Missing columns: {missing}"


def test_eia861_yearly_sales_customer_class_values(
    eia861_yearly_sales_df: pl.DataFrame,
):
    """customer_class must match expected set so per-class columns are predictable.

    Uses exact set equality: fails if the dataset has extra classes (unaccounted for)
    or is missing any expected class.
    """
    classes = (
        eia861_yearly_sales_df.select("customer_class")
        .unique()
        .to_series()
        .cast(pl.Utf8)
        .to_list()
    )
    actual = frozenset(classes)
    assert actual == EXPECTED_CUSTOMER_CLASSES, (
        f"customer_class values changed: expected {EXPECTED_CUSTOMER_CLASSES}, got {actual}"
    )


def test_eia861_yearly_sales_has_investor_owned_data_for_sample_state(
    eia861_yearly_sales_df: pl.DataFrame,
):
    """At least one state (NY) has Investor Owned rows with all customer classes."""
    df = eia861_yearly_sales_df.filter(pl.col("state") == "NY").filter(
        pl.col("entity_type") == "Investor Owned"
    )
    assert df.height > 0, "No NY Investor Owned rows in dataset"
    classes_in_data = (
        df.select("customer_class").unique().to_series().cast(pl.Utf8).to_list()
    )
    assert frozenset(classes_in_data) == EXPECTED_CUSTOMER_CLASSES, (
        f"NY IOU data missing some customer classes: got {set(classes_in_data)}"
    )


def test_utility_code_column_present_and_mapped():
    """Script output includes utility_code; known EIA IDs map to expected std_names."""
    from pathlib import Path

    project_root = Path(__file__).resolve().parent.parent
    result = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "data/eia/861/fetch_electric_utility_stat_parquets.py",
            "NY",
        ],
        capture_output=True,
        text=True,
        cwd=project_root,
    )
    assert result.returncode == 0, f"Script failed: {result.stderr}"

    df = pl.read_csv(StringIO(result.stdout))
    assert "utility_code" in df.columns

    # Known EIA ID -> std_name mappings (IOU + State e.g. LIPA)
    expected = {
        4226: "coned",
        13573: "nimo",
        13511: "nyseg",
        16183: "rge",
        3249: "cenhud",
        14154: "or",
        11171: "psegli",  # Long Island Power Authority (State)
    }
    for eia_id, std_name in expected.items():
        row = df.filter(pl.col("utility_id_eia") == eia_id)
        assert not row.is_empty(), f"EIA ID {eia_id} not in output"
        assert row["utility_code"][0] == std_name, (
            f"EIA ID {eia_id}: expected utility_code {std_name}, got {row['utility_code'][0]}"
        )


def test_ny_csv_has_expected_schema_and_2024_data():
    """Single-state CSV for NY has expected columns and contains 2024 data for known utilities."""
    project_root = Path(__file__).resolve().parent.parent
    result = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "data/eia/861/fetch_electric_utility_stat_parquets.py",
            "NY",
        ],
        capture_output=True,
        text=True,
        cwd=project_root,
    )
    assert result.returncode == 0, f"Script failed: {result.stderr}"
    df = pl.read_csv(StringIO(result.stdout))
    # Schema: year plus script output columns (no state in CSV)
    assert "year" in df.columns
    assert "utility_id_eia" in df.columns
    assert "utility_code" in df.columns
    assert "total_sales_mwh" in df.columns
    assert "total_sales_revenue" in df.columns
    # 2024 is present (dataset coverage through 2024)
    years = df["year"].unique().to_list()
    assert 2024 in years, f"Expected year 2024 in output, got {sorted(years)}"
    # Known NY utilities have 2024 rows with positive sales
    ny_2024 = df.filter(pl.col("year") == 2024)
    known_eia_ids = {4226, 13573, 13511, 16183, 3249, 14154, 11171}
    for eia_id in known_eia_ids:
        row = ny_2024.filter(pl.col("utility_id_eia") == eia_id)
        assert not row.is_empty(), f"EIA ID {eia_id} missing from NY 2024 output"
        assert row["total_sales_mwh"][0] > 0, (
            f"EIA ID {eia_id} has non-positive total_sales_mwh for 2024"
        )
