#!/usr/bin/env python3
"""
Convert Cambium 2024 Hourly Balancing Area CSV files to partitioned Parquet format

CAMBIUM DATA STRUCTURE OVERVIEW
================================

Source Files:
- 798 CSV files = 133 balancing areas x 6 projection years (2025, 2030, 2035, 2040, 2045, 2050)
- Each file: ~3.5 MB CSV → ~1.1 MB Parquet (3.3x compression ratio)
- Total: 2.8 GB CSV → 0.86 GB Parquet

CSV File Structure:
- Row 1: Metadata column names (Project, Scenario, Dollar_year, Weather_year, Start_day, r, state, gea, country, tz, t)
- Row 2: Metadata values (constant for entire file, e.g., "Cambium24", "MidCase", "2023$", "2012", ...)
- Row 3: Documentation reference line
- Row 4: Category headers (Load, Emissions, Generation, Costs, Curtailment, etc.)
- Row 5: Units row (MWh, kg/MWh, g/MWh, $/MWh, etc.)
- Row 6+: 8760 rows of hourly data (365 days x 24 hours)

Key Data Characteristics (verified across all 798 files):
- No null values in any columns
- All metadata fields are constant within each file
- Timestamps are hourly in Eastern Time (timestamp) with local timezone versions (timestamp_local)
- No DST gaps/duplicates - all files have exactly 8760 hourly rows
- Float32 provides sufficient precision (max relative error < 1e-6 vs float64)
- Some fields can be negative (e.g., net_load_busbar during high renewable generation periods)

Categorical Fields (dictionary-encoded for efficiency):
- r: 133 unique balancing area IDs (p1, p10, p100, ..., z122)
- state: ~50 US state codes (some BAs span multiple states)
- gea: 13 GEA regions (CAISO, ISONE, MISO, NYISO, PJM, SPP, etc.)
- tz: ~20 timezone abbreviations (ET, CT, MT, PT, AKT, HT)
- marg_gen_tech: ~20 marginal generation technologies (battery, gas-cc, wind-ons, etc.)
- marg_es_tech: ~20 marginal energy source technologies

Geographic Relationships:
- Balancing areas (r) map to states (1:1 or 1:many for BAs spanning state borders)
- GEA regions have many-to-many relationship with states
- Example: MISO spans IL, IN, MI, WI, MN, ND, SD, IA, MO, AR, LA, MS, TX

Partitioning Strategy:
- Partitioned by: scenario/t/gea/r/
- Each partition = 1 Parquet file (~100 KB)
- This mirrors the source CSV structure (1 CSV per BA-year combination)
- Enables efficient predicate pushdown on all dimensions
- Files sorted by timestamp within each partition for optimal row group compression

Schema:
- 91 total columns: 11 metadata + 80 data columns
- All columns are NOT NULL (verified - no nulls exist in the data)
- Field-level metadata encodes units and descriptions
- See cambium_full_schema.csv for complete schema documentation

References:
- NREL Cambium 2024 Documentation: https://www.nrel.gov/docs/fy25osti/93005.pdf
- Data Source: https://scenarioviewer.nrel.gov/?project=5c7bef16-7e38-4094-92ce-8b03dfa93380
- Balancing Area Lookup: cambium_balancing_area_lookup.csv
"""

import sys
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


def create_schema_with_metadata():
    """Create PyArrow schema with field metadata (units, descriptions)"""

    # Define schema with metadata
    fields = [
        # Metadata fields (NOT NULL)
        pa.field(
            "project",
            pa.string(),
            nullable=False,
            metadata={"description": "Project name", "constant": "Cambium24"},
        ),
        pa.field(
            "scenario",
            pa.string(),
            nullable=False,
            metadata={"description": "Scenario name"},
        ),
        pa.field(
            "dollar_year",
            pa.string(),
            nullable=False,
            metadata={"description": "Dollar year for costs", "constant": "2023$"},
        ),
        pa.field(
            "weather_year",
            pa.int16(),
            nullable=False,
            metadata={
                "description": "Weather year used for modeling",
                "constant": "2012",
            },
        ),
        pa.field(
            "start_day",
            pa.string(),
            nullable=False,
            metadata={"description": "First day of week", "constant": "Sunday"},
        ),
        pa.field(
            "r",
            pa.dictionary(pa.int8(), pa.string()),
            nullable=False,
            metadata={"description": "Balancing area ID", "unique_values": "133"},
        ),
        pa.field(
            "state",
            pa.dictionary(pa.int8(), pa.string()),
            nullable=False,
            metadata={"description": "US state code"},
        ),
        pa.field(
            "gea",
            pa.dictionary(pa.int8(), pa.string()),
            nullable=False,
            metadata={"description": "GEA region name"},
        ),
        pa.field(
            "country",
            pa.string(),
            nullable=False,
            metadata={"description": "Country code", "constant": "usa"},
        ),
        pa.field(
            "tz",
            pa.dictionary(pa.int8(), pa.string()),
            nullable=False,
            metadata={"description": "Timezone abbreviation"},
        ),
        pa.field(
            "t",
            pa.int16(),
            nullable=False,
            metadata={
                "description": "Projection year",
                "values": "2025,2030,2035,2040,2045,2050",
            },
        ),
        # Time fields
        pa.field(
            "timestamp",
            pa.timestamp("ms"),
            nullable=False,
            metadata={"description": "Timestamp in ET", "unit": "Datetime"},
        ),
        pa.field(
            "timestamp_local",
            pa.timestamp("ms"),
            nullable=False,
            metadata={"description": "Timestamp in local timezone", "unit": "Datetime"},
        ),
        # Load fields
        pa.field(
            "busbar_load_for_enduse",
            pa.float32(),
            nullable=False,
            metadata={"description": "Busbar load for end use", "unit": "MWh"},
        ),
        pa.field(
            "battery_charging",
            pa.float32(),
            nullable=False,
            metadata={"description": "Battery charging load", "unit": "MWh"},
        ),
        pa.field(
            "phs_charging",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Pumped hydro storage charging load",
                "unit": "MWh",
            },
        ),
        pa.field(
            "trans_losses",
            pa.float32(),
            nullable=False,
            metadata={"description": "Transmission losses", "unit": "MWh"},
        ),
        pa.field(
            "busbar_load",
            pa.float32(),
            nullable=False,
            metadata={"description": "Total busbar load", "unit": "MWh"},
        ),
        pa.field(
            "enduse_load",
            pa.float32(),
            nullable=False,
            metadata={"description": "End use load", "unit": "MWh"},
        ),
        pa.field(
            "net_load_busbar",
            pa.float32(),
            nullable=False,
            metadata={"description": "Net load at busbar", "unit": "MWh"},
        ),
        # Transmission fields
        pa.field(
            "imports",
            pa.float32(),
            nullable=False,
            metadata={"description": "Electricity imports", "unit": "MWh"},
        ),
        pa.field(
            "exports",
            pa.float32(),
            nullable=False,
            metadata={"description": "Electricity exports", "unit": "MWh"},
        ),
        # Operational fields
        pa.field(
            "distloss_rate_avg",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average distribution loss rate",
                "unit": "MWh/MWh",
            },
        ),
        pa.field(
            "distloss_rate_marg",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Marginal distribution loss rate",
                "unit": "MWh/MWh",
            },
        ),
        pa.field(
            "marg_gen_tech",
            pa.dictionary(pa.int8(), pa.string()),
            nullable=False,
            metadata={
                "description": "Marginal generation technology",
                "unit": "unitless",
            },
        ),
        pa.field(
            "marg_es_tech",
            pa.dictionary(pa.int8(), pa.string()),
            nullable=False,
            metadata={
                "description": "Marginal energy source technology",
                "unit": "unitless",
            },
        ),
        # Emissions fields (36 total)
        pa.field(
            "aer_gen_co2_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate gen - CO2 combustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "aer_gen_ch4_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate gen - CH4 combustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "aer_gen_n2o_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate gen - N2O combustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "aer_gen_co2_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate gen - CO2 precombustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "aer_gen_ch4_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate gen - CH4 precombustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "aer_gen_n2o_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate gen - N2O precombustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "aer_gen_co2e_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate gen - CO2e combustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "aer_gen_co2e_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate gen - CO2e precombustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "aer_gen_co2e",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate gen - CO2e total",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "aer_load_co2_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate load - CO2 combustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "aer_load_ch4_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate load - CH4 combustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "aer_load_n2o_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate load - N2O combustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "aer_load_co2_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate load - CO2 precombustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "aer_load_ch4_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate load - CH4 precombustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "aer_load_n2o_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate load - N2O precombustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "aer_load_co2e_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate load - CO2e combustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "aer_load_co2e_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate load - CO2e precombustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "aer_load_co2e",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Average emission rate load - CO2e total",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "lrmer_co2_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Long-run marginal emission rate - CO2 combustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "lrmer_ch4_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Long-run marginal emission rate - CH4 combustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "lrmer_n2o_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Long-run marginal emission rate - N2O combustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "lrmer_co2_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Long-run marginal emission rate - CO2 precombustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "lrmer_ch4_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Long-run marginal emission rate - CH4 precombustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "lrmer_n2o_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Long-run marginal emission rate - N2O precombustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "lrmer_co2e_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Long-run marginal emission rate - CO2e combustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "lrmer_co2e_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Long-run marginal emission rate - CO2e precombustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "lrmer_co2e",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Long-run marginal emission rate - CO2e total",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "srmer_co2_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Short-run marginal emission rate - CO2 combustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "srmer_ch4_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Short-run marginal emission rate - CH4 combustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "srmer_n2o_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Short-run marginal emission rate - N2O combustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "srmer_co2_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Short-run marginal emission rate - CO2 precombustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "srmer_ch4_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Short-run marginal emission rate - CH4 precombustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "srmer_n2o_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Short-run marginal emission rate - N2O precombustion",
                "unit": "g/MWh",
            },
        ),
        pa.field(
            "srmer_co2e_c",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Short-run marginal emission rate - CO2e combustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "srmer_co2e_p",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Short-run marginal emission rate - CO2e precombustion",
                "unit": "kg/MWh",
            },
        ),
        pa.field(
            "srmer_co2e",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Short-run marginal emission rate - CO2e total",
                "unit": "kg/MWh",
            },
        ),
        # Cost fields
        pa.field(
            "energy_cost_busbar",
            pa.float32(),
            nullable=False,
            metadata={"description": "Energy cost at busbar", "unit": "$/MWh"},
        ),
        pa.field(
            "capacity_cost_busbar",
            pa.float32(),
            nullable=False,
            metadata={"description": "Capacity cost at busbar", "unit": "$/MWh"},
        ),
        pa.field(
            "portfolio_cost_busbar",
            pa.float32(),
            nullable=False,
            metadata={"description": "Portfolio cost at busbar", "unit": "$/MWh"},
        ),
        pa.field(
            "total_cost_busbar",
            pa.float32(),
            nullable=False,
            metadata={"description": "Total cost at busbar", "unit": "$/MWh"},
        ),
        pa.field(
            "energy_cost_enduse",
            pa.float32(),
            nullable=False,
            metadata={"description": "Energy cost at end use", "unit": "$/MWh"},
        ),
        pa.field(
            "capacity_cost_enduse",
            pa.float32(),
            nullable=False,
            metadata={"description": "Capacity cost at end use", "unit": "$/MWh"},
        ),
        pa.field(
            "portfolio_cost_enduse",
            pa.float32(),
            nullable=False,
            metadata={"description": "Portfolio cost at end use", "unit": "$/MWh"},
        ),
        pa.field(
            "total_cost_enduse",
            pa.float32(),
            nullable=False,
            metadata={"description": "Total cost at end use", "unit": "$/MWh"},
        ),
        # Curtailment fields
        pa.field(
            "curt_wind_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Wind curtailment", "unit": "MWh"},
        ),
        pa.field(
            "curt_solar_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Solar curtailment", "unit": "MWh"},
        ),
        pa.field(
            "curtailment_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Total curtailment", "unit": "MWh"},
        ),
        # Generation fields
        pa.field(
            "generation",
            pa.float32(),
            nullable=False,
            metadata={"description": "Total generation", "unit": "MWh"},
        ),
        pa.field(
            "variable_generation",
            pa.float32(),
            nullable=False,
            metadata={"description": "Variable generation", "unit": "MWh"},
        ),
        pa.field(
            "battery_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Battery generation", "unit": "MWh"},
        ),
        pa.field(
            "biomass_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Biomass generation", "unit": "MWh"},
        ),
        pa.field(
            "canada_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Canadian imports", "unit": "MWh"},
        ),
        pa.field(
            "coal_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Coal generation", "unit": "MWh"},
        ),
        pa.field(
            "csp_mwh",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Concentrating solar power generation",
                "unit": "MWh",
            },
        ),
        pa.field(
            "distpv_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Distributed PV generation", "unit": "MWh"},
        ),
        pa.field(
            "gas-cc_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Gas combined cycle generation", "unit": "MWh"},
        ),
        pa.field(
            "gas-ct_mwh",
            pa.float32(),
            nullable=False,
            metadata={
                "description": "Gas combustion turbine generation",
                "unit": "MWh",
            },
        ),
        pa.field(
            "geothermal_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Geothermal generation", "unit": "MWh"},
        ),
        pa.field(
            "hydro_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Hydropower generation", "unit": "MWh"},
        ),
        pa.field(
            "nuclear_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Nuclear generation", "unit": "MWh"},
        ),
        pa.field(
            "o-g-s_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Oil-gas-steam generation", "unit": "MWh"},
        ),
        pa.field(
            "phs_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Pumped hydro storage generation", "unit": "MWh"},
        ),
        pa.field(
            "upv_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Utility-scale PV generation", "unit": "MWh"},
        ),
        pa.field(
            "wind-ons_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Onshore wind generation", "unit": "MWh"},
        ),
        pa.field(
            "wind-ofs_mwh",
            pa.float32(),
            nullable=False,
            metadata={"description": "Offshore wind generation", "unit": "MWh"},
        ),
    ]

    return pa.schema(fields)


def convert_csv_to_parquet(csv_file, schema, output_base_dir):
    """Convert a single CSV file to Parquet with proper schema"""

    # Read metadata from rows 1-2
    with open(csv_file) as f:
        header_line = next(f)
        metadata_line = next(f)

    metadata_cols = header_line.strip().split(",")
    metadata_vals = metadata_line.strip().split(",")
    metadata = dict(zip(metadata_cols, metadata_vals, strict=False))

    # Read data (skip first 5 rows: header, metadata, doc, category, units)
    df = pl.read_csv(csv_file, skip_rows=5)

    # Lowercase all column names
    df = df.rename({col: col.lower() for col in df.columns})

    # Add metadata columns at the beginning
    df = df.with_columns(
        [
            pl.lit(metadata.get("Project", "Cambium24")).alias("project"),
            pl.lit(metadata.get("Scenario", "MidCase")).alias("scenario"),
            pl.lit(metadata.get("Dollar_year", "2023$")).alias("dollar_year"),
            pl.lit(int(metadata.get("Weather_year", 2012)))
            .cast(pl.Int16)
            .alias("weather_year"),
            pl.lit(metadata.get("Start_day", "Sunday")).alias("start_day"),
            pl.lit(metadata.get("r", "")).alias("r"),
            pl.lit(metadata.get("state", "")).alias("state"),
            pl.lit(metadata.get("gea", "")).alias("gea"),
            pl.lit(metadata.get("country", "usa")).alias("country"),
            pl.lit(metadata.get("tz", "")).alias("tz"),
            pl.lit(int(metadata.get("t", 0))).cast(pl.Int16).alias("t"),
        ]
    )

    # Convert timestamp columns to datetime
    df = df.with_columns(
        [
            pl.col("timestamp").str.to_datetime(),
            pl.col("timestamp_local").str.to_datetime(),
        ]
    )

    # Convert categorical columns
    for col in ["r", "state", "gea", "tz", "marg_gen_tech", "marg_es_tech"]:
        df = df.with_columns(pl.col(col).cast(pl.Categorical))

    # Sort by timestamp
    df = df.sort("timestamp")

    # Reorder columns to match schema (metadata first, then data columns)
    column_order = [
        "project",
        "scenario",
        "dollar_year",
        "weather_year",
        "start_day",
        "r",
        "state",
        "gea",
        "country",
        "tz",
        "t",
    ] + [
        col
        for col in df.columns
        if col
        not in [
            "project",
            "scenario",
            "dollar_year",
            "weather_year",
            "start_day",
            "r",
            "state",
            "gea",
            "country",
            "tz",
            "t",
        ]
    ]
    df = df.select(column_order)

    # Convert to PyArrow table with schema
    table = df.to_arrow()
    # Cast to our schema with metadata
    table = table.cast(schema)

    # Determine partition path
    scenario = metadata.get("Scenario", "MidCase").replace(" ", "_").replace("-", "_")
    year = metadata.get("t", "0")
    gea = metadata.get("gea", "unknown")
    r = metadata.get("r", "unknown")

    partition_path = (
        output_base_dir / f"scenario={scenario}" / f"t={year}" / f"gea={gea}" / f"r={r}"
    )
    partition_path.mkdir(parents=True, exist_ok=True)

    output_file = partition_path / "data.parquet"

    # Write Parquet file
    pq.write_table(
        table,
        output_file,
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
    )

    return output_file


def main():
    csv_dir = Path("csv/hourly_balancingArea/")
    output_dir = Path("parquet/")

    if not csv_dir.exists():
        print(f"Error: CSV directory not found: {csv_dir}")
        print("Expected structure: csv/hourly_balancingArea/*.csv")
        sys.exit(1)

    csv_files = sorted(csv_dir.glob("*.csv"))

    if len(csv_files) == 0:
        print(f"Error: No CSV files found in {csv_dir}")
        sys.exit(1)

    print("=" * 80)
    print("CAMBIUM CSV TO PARQUET CONVERTER")
    print("=" * 80)
    print(f"\nInput directory:  {csv_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Total CSV files:  {len(csv_files)}")
    print("\nPartitioning: scenario/t/gea/r/")
    print("Schema: 91 columns (11 metadata + 80 data)")
    print("Compression: Snappy")
    print("Sort order: timestamp (ascending)")
    print()

    # Create schema
    schema = create_schema_with_metadata()

    # Convert all files
    print("Converting files...")
    print("-" * 80)

    failed = []

    for csv_file in tqdm(csv_files, desc="Converting", unit="file"):
        try:
            convert_csv_to_parquet(csv_file, schema, output_dir)
        except Exception as e:
            failed.append((csv_file.name, str(e)))
            print(f"\n✗ Error processing {csv_file.name}: {e}")

    print()
    print("=" * 80)
    print("CONVERSION COMPLETE")
    print("=" * 80)

    successful = len(csv_files) - len(failed)
    print(f"\nSuccessful: {successful}/{len(csv_files)} files")

    if failed:
        print(f"\nFailed files ({len(failed)}):")
        for filename, error in failed[:10]:
            print(f"  - {filename}: {error}")
        if len(failed) > 10:
            print(f"  ... and {len(failed) - 10} more")

    # Calculate output size
    print("\nCalculating output size...")
    total_size = sum(f.stat().st_size for f in output_dir.rglob("*.parquet"))
    print(f"Total Parquet size: {total_size / (1024**3):.2f} GB")

    # Count partitions
    partition_dirs = [d for d in output_dir.rglob("r=*") if d.is_dir()]
    print(f"Total partitions: {len(partition_dirs)}")

    print(f"\n✓ Parquet files written to: {output_dir}")


if __name__ == "__main__":
    main()
