########################################################
# Forced Utility Mapping
########################################################
def forced_utility_crosswalk_ri(path_to_rs2024_metadata: str | Path) -> pl.DataFrame:
    """
    Apply forced utility mapping for RI: electric and gas set to rhode_island_energy
    where state is RI; gas only where natural gas consumption > 10.
    """

    USE_THESE_COLUMNS = [
        "bldg_id",
        "in.state",
        "in.heating_fuel",
        "out.natural_gas.total.energy_consumption.kwh",
    ]

    path = Path(path_to_rs2024_metadata)
    if path.is_dir():
        parquet_path = path / "metadata.parquet"
    else:
        parquet_path = path
    out_dir = parquet_path.parent if parquet_path.suffix else path

    bldg_utility_mapping = pl.read_parquet(
        parquet_path, columns=USE_THESE_COLUMNS
    ).with_columns(
        pl.lit(None).cast(pl.Utf8).alias("electric_utility"),
        pl.lit(None).cast(pl.Utf8).alias("gas_utility"),
    )

    ng_col = "out.natural_gas.total.energy_consumption.kwh"
    bldg_utility_mapping = bldg_utility_mapping.with_columns(
        pl.when(pl.col("in.state") == "RI")
        .then(pl.lit("rhode_island_energy"))
        .otherwise(pl.col("electric_utility"))
        .alias("electric_utility"),
        pl.when((pl.col("in.state") == "RI") & (pl.col(ng_col) > 10))
        .then(pl.lit("rhode_island_energy"))
        .otherwise(pl.col("gas_utility"))
        .alias("gas_utility"),
    )

    bldg_utility_mapping.write_ipc(out_dir / "rs2024_bldg_utility_crosswalk.feather")
    bldg_utility_mapping.write_csv(out_dir / "rs2024_bldg_utility_crosswalk.csv")
    return bldg_utility_mapping
