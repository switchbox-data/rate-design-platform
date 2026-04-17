import argparse
import warnings
from pathlib import Path
from typing import Any, cast

import polars as pl
from cloudpathlib import S3Path

from utils import get_aws_region
from utils.types import SBScenario, ElectricUtility

STORAGE_OPTIONS = {"aws_region": get_aws_region()}


def define_electrical_tariff_key(
    SB_scenario: SBScenario,
    electric_utility: ElectricUtility,
    has_hp: pl.Expr,
) -> pl.Expr:
    if SB_scenario.analysis_type == "default":
        return pl.lit(f"{electric_utility}_{SB_scenario}_default")
    elif SB_scenario.analysis_type in [
        "seasonal",
        "seasonal_discount",
        "class_specific_seasonal",
    ]:
        return (
            pl.when(has_hp)
            .then(pl.lit(f"{electric_utility}_{SB_scenario}_HP.csv"))
            .otherwise(pl.lit(f"{electric_utility}_{SB_scenario}_flat.csv"))
        )
    else:
        raise ValueError(f"Invalid SB scenario: {SB_scenario}")


def generate_electrical_tariff_mapping(
    lazy_metadata_has_hp: pl.LazyFrame,
    SB_scenario: SBScenario,
    electric_utility: ElectricUtility,
) -> pl.LazyFrame:
    electrical_tariff_mapping_df = lazy_metadata_has_hp.select(
        pl.col("bldg_id"),
        define_electrical_tariff_key(
            SB_scenario, electric_utility, pl.col("postprocess_group.has_hp")
        ).alias("tariff_key"),
    )

    return electrical_tariff_mapping_df


def map_electric_tariff(
    SB_metadata_df: pl.LazyFrame,
    electric_utility: ElectricUtility,
    SB_scenario: SBScenario,
    state: str,
) -> pl.LazyFrame:
    utility_metadata_df = SB_metadata_df.filter(
        pl.col("sb.electric_utility") == electric_utility
    )

    metadata_has_hp = utility_metadata_df.select(
        pl.col("bldg_id", "postprocess_group.has_hp")
    )

    # Check if there are any rows in the filtered dataframe
    test_sample = cast(pl.DataFrame, metadata_has_hp.head(1).collect())
    if test_sample.is_empty():
        raise ValueError(f"No rows found for electric utility {electric_utility}")

    electrical_tariff_mapping_df = generate_electrical_tariff_mapping(
        metadata_has_hp, SB_scenario, electric_utility
    )

    return electrical_tariff_mapping_df


def generate_tariff_map_from_scenario_keys(
    path_tariffs_electric: dict[str, str],
    bldg_data: pl.DataFrame,
    subclass_config: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Build a bldg_id→tariff_key mapping from keyed path_tariffs_electric.

    Args:
        path_tariffs_electric: Dict mapping subclass keys to tariff path strings.
            Supported forms:
              - ``{"all": "<path>"}`` — every building gets the stem of that path.
                ``subclass_config`` is not required in this case.
              - Any other key set — requires ``subclass_config``. Each key in
                ``path_tariffs_electric`` must appear as a key in
                ``subclass_config["selectors"]``. Buildings whose
                ``postprocess_group.<group_col>`` value appears in the comma-separated
                selector string for a subclass key are assigned the corresponding tariff
                stem.  An arbitrary number of subclasses (2, 3, 5, …) is supported.
        bldg_data: DataFrame with column ``bldg_id`` and the column
            ``postprocess_group.<group_col>`` referenced by ``subclass_config``.
        subclass_config: Optional dict with keys:
            - ``group_col`` (str): the postprocess_group column suffix to read from
              ``bldg_data`` (e.g. ``"has_hp"`` → column
              ``"postprocess_group.has_hp"``).
            - ``selectors`` (dict[str, str]): mapping from subclass key to a
              comma-separated string of ``group_col`` values that belong to that
              subclass (e.g. ``{"hp": "true", "non-hp": "false"}`` or
              ``{"electric_heating": "heat_pump,electrical_resistance",
              "non_electric_heating": "natgas,delivered_fuels,other"}``).
            Required when ``path_tariffs_electric`` has more than one key.

    Returns:
        DataFrame with columns ``bldg_id`` and ``tariff_key``.

    Raises:
        ValueError: If ``path_tariffs_electric`` has more than one key but
            ``subclass_config`` is not provided, or if a tariff key has no
            matching selector entry in ``subclass_config``.
    """
    keys = set(path_tariffs_electric.keys())
    stems = {k: Path(v).stem for k, v in path_tariffs_electric.items()}

    if keys == {"all"}:
        return bldg_data.select(
            pl.col("bldg_id"),
            pl.lit(stems["all"]).alias("tariff_key"),
        )

    if subclass_config is None:
        raise ValueError(
            "subclass_config is required when path_tariffs_electric has more than one "
            f"key; got keys {sorted(keys)}"
        )

    group_col: str = subclass_config["group_col"]
    selectors: dict[str, str] = subclass_config["selectors"]
    col_name = f"postprocess_group.{group_col}"

    missing_keys = keys - set(selectors.keys())
    if missing_keys:
        raise ValueError(
            f"path_tariffs_electric keys {sorted(missing_keys)} have no matching "
            f"entry in subclass_config.selectors (available: {sorted(selectors.keys())})"
        )

    # Build a pl.when(...).then(...).when(...).then(...).otherwise(None) chain
    # over all subclass keys in a deterministic order.
    ordered_keys = sorted(keys)
    first_key = ordered_keys[0]
    first_values = {v.strip() for v in selectors[first_key].split(",")}
    expr = pl.when(pl.col(col_name).cast(pl.Utf8).is_in(first_values)).then(
        pl.lit(stems[first_key])
    )
    for key in ordered_keys[1:]:
        values = {v.strip() for v in selectors[key].split(",")}
        expr = expr.when(pl.col(col_name).cast(pl.Utf8).is_in(values)).then(
            pl.lit(stems[key])
        )
    tariff_key_expr = expr.otherwise(pl.lit(None)).alias("tariff_key")

    result = bldg_data.select(pl.col("bldg_id"), tariff_key_expr)

    unmatched = result.filter(pl.col("tariff_key").is_null()).height
    if unmatched > 0:
        raise ValueError(
            f"{unmatched} building(s) did not match any selector in subclass_config "
            f"for group_col='{group_col}'. Check that all values in column "
            f"'{col_name}' are covered by subclass_config.selectors."
        )

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Utility to help assign electricity tariffs to utility customers."
    )
    parser.add_argument(
        "--metadata_path",
        required=True,
        help="Absolute or s3 path to ResStock metadata",
    )
    parser.add_argument("--state", required=True, help="State code (e.g. RI)")
    parser.add_argument("--upgrade_id", required=True, help="Upgrade id (e.g. 00)")
    parser.add_argument(
        "--electric_utility",
        required=True,
        help="Electric utility std_name (e.g. coned, nyseg, nimo)",
    )
    parser.add_argument(
        "--SB_scenario_type",
        required=True,
        help=(
            "SB scenario type "
            "(e.g. default, seasonal, seasonal_discount, class_specific_seasonal)"
        ),
    )
    parser.add_argument(
        "--SB_scenario_year", required=True, help="SB scenario year (e.g. 1 , 2, 3)"
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Output directory for output CSV",
    )
    args = parser.parse_args()

    try:  # If the metadata path is an S3 path, use the S3Path class.
        base_path = S3Path(args.metadata_path)
        use_s3 = True
    except ValueError:
        base_path = Path(args.metadata_path)
        use_s3 = False

    # Support metadata_utility path (utility_assignment.parquet) or metadata path (metadata-sb.parquet)
    if "metadata_utility" in str(args.metadata_path):
        metadata_path = base_path / f"state={args.state}" / "utility_assignment.parquet"
    else:
        metadata_path = (
            base_path
            / f"state={args.state}"
            / f"upgrade={args.upgrade_id}"
            / "metadata-sb.parquet"
        )

    if use_s3 and not metadata_path.exists():
        raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")
    if not use_s3 and not Path(metadata_path).exists():
        raise FileNotFoundError(f"Metadata path {metadata_path} does not exist")

    storage_opts = STORAGE_OPTIONS if use_s3 else None
    SB_metadata_df = (
        pl.scan_parquet(str(metadata_path), storage_options=storage_opts)
        if storage_opts
        else pl.scan_parquet(str(metadata_path))
    )

    # Use real sb.electric_utility if present; else fall back to synthetic (deprecated)
    schema_cols = SB_metadata_df.collect_schema().names()
    if "sb.electric_utility" in schema_cols:
        SB_metadata_df_with_electric_utility = SB_metadata_df
    else:
        warnings.warn(
            "metadata has no sb.electric_utility column; using synthetic data. "
            "Run assign_utility_ny (data/resstock/) and point --metadata_path to metadata_utility for real data.",
            DeprecationWarning,
            stacklevel=2,
        )
        SB_metadata_df_with_electric_utility = SB_metadata_df.with_columns(
            pl.when(pl.col("bldg_id").hash() % 3 == 0)
            .then(pl.lit("coned"))
            .when(pl.col("bldg_id").hash() % 3 == 1)
            .then(pl.lit("nimo"))
            .otherwise(pl.lit("nyseg"))
            .alias("sb.electric_utility")
        )

    sb_scenario = SBScenario(args.SB_scenario_type, args.SB_scenario_year)
    electrical_tariff_mapping_df = map_electric_tariff(
        SB_metadata_df=SB_metadata_df_with_electric_utility,
        electric_utility=args.electric_utility,
        SB_scenario=sb_scenario,
        state=args.state,
    )
    try:
        base_path = S3Path(args.output_dir)
        output_path = base_path / f"{args.electric_utility}_{sb_scenario}.csv"
        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True)
        electrical_tariff_mapping_df.sink_csv(
            str(output_path), storage_options=STORAGE_OPTIONS
        )
    except ValueError:
        base_path = Path(args.output_dir)
        output_path = base_path / f"{args.electric_utility}_{sb_scenario}.csv"
        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True)
        electrical_tariff_mapping_df.sink_csv(str(output_path))
