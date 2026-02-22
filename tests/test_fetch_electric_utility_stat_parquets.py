"""Validate the EIA-861 yearly sales dataset used by data/eia/861/fetch_electric_utility_stat_parquets.py.

Confirms required columns and customer_class values exist so the script's
dynamic aggregation is safe. Uses the same parquet URL as the script (stable release).
"""

import subprocess
from io import StringIO
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal

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


def test_partitioned_parquet_matches_single_state_csv(tmp_path: Path):
    """Partitioned parquet (year=YYYY/state=NY/data.parquet) matches single-state CSV for NY for that year."""
    project_root = Path(__file__).resolve().parent.parent
    # Build partitioned parquet (all years, all states)
    run = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "data/eia/861/fetch_electric_utility_stat_parquets.py",
            "--output-dir",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        cwd=project_root,
    )
    assert run.returncode == 0, f"Parquet build failed: {run.stderr}"
    # Compare one partition: latest year available for NY (e.g. 2024)
    parquet_path = tmp_path / "year=2024" / "state=NY" / "data.parquet"
    assert parquet_path.exists(), f"Expected {parquet_path}"
    # Single-state CSV (all years)
    run_csv = subprocess.run(
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
    assert run_csv.returncode == 0, f"CSV run failed: {run_csv.stderr}"
    df_parquet = (
        pl.read_parquet(parquet_path).drop("year", "state").sort("utility_id_eia")
    )
    df_csv = (
        pl.read_csv(StringIO(run_csv.stdout))
        .filter(pl.col("year") == 2024)
        .drop("year")
        .sort("utility_id_eia")
    )
    assert_frame_equal(df_parquet, df_csv, check_dtypes=False)
