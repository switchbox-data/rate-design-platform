"""NYISO bulk transmission marginal cost logic using constraint-group SCR allocation.

Methodology
-----------
Derives scalar costs for paying localities (ROS/LHV/NYC/LI) from NYISO
constraint groups, then allocates each utility's weighted locality costs onto
SCR hours identified from that utility's mapped ICAP localities.

For ISO-NE bulk transmission, see ``bulk_tx_isone.py``.
For the CLI entrypoint, see ``generate_bulk_tx_mc.py``.
"""

from __future__ import annotations

import io
from typing import cast

import polars as pl
from cloudpathlib import S3Path

from utils.pre.marginal_costs.supply_utils import (
    build_cairo_8760_timestamps,
)
from utils.pre.season_config import (
    DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS,
    derive_summer_months,
)

# ── NYISO constants ───────────────────────────────────────────────────────────

DEFAULT_NYISO_BULK_TX_CONSTRAINT_GROUP_TABLE_PATH = (
    "s3://data.sb/nyiso/bulk_tx/ny_bulk_tx_constraint_groups.csv"
)

N_SCR_HOURS_PER_SEASON = 40
DEFAULT_SCR_WINTER_MONTHS: tuple[int, ...] = DEFAULT_SEASONAL_DISCOUNT_WINTER_MONTHS

NESTED_LOCALITY_ZONES: dict[str, list[str]] = {
    "NYCA": [
        "WEST",
        "GENESE",
        "CENTRAL",
        "NORTH",
        "MHK_VL",
        "CAPITL",
        "HUD_VL",
        "MILLWD",
        "DUNWOD",
        "N.Y.C.",
        "LONGIL",
    ],
    "LHV": ["HUD_VL", "MILLWD", "DUNWOD", "N.Y.C."],
    "NYC": ["N.Y.C."],
    "LI": ["LONGIL"],
}
VALID_NESTED_LOCALITIES = frozenset(NESTED_LOCALITY_ZONES)
VALID_PAYING_LOCALITIES = frozenset({"ROS", "LHV", "NYC", "LI"})
ICAP_RAW_TO_NESTED_LOCALITY = {
    "NYCA": "NYCA",
    "GHIJ": "LHV",
    "NYC": "NYC",
    "LI": "LI",
}


# ── Constraint-group table ───────────────────────────────────────────────────


def load_constraint_group_table(path: str) -> pl.DataFrame:
    """Load ny_bulk_tx_constraint_groups.csv from local or S3."""
    if path.startswith("s3://"):
        csv_bytes = S3Path(path).read_bytes()
        df = pl.read_csv(io.BytesIO(csv_bytes))
    else:
        df = pl.read_csv(path)

    required = {
        "nested_localities_str",
        "constraint_group",
        "v_constraint_group_kw_yr",
        "tightest_nested_locality",
        "paying_localities",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Constraint-group table missing columns: {sorted(missing)}")

    bad_nested = sorted(
        set(df["tightest_nested_locality"].cast(pl.String).to_list())
        - set(VALID_NESTED_LOCALITIES)
    )
    if bad_nested:
        raise ValueError(
            "Invalid tightest_nested_locality values: "
            f"{bad_nested}. Expected {sorted(VALID_NESTED_LOCALITIES)}"
        )

    df = df.with_columns(
        pl.col("constraint_group").cast(pl.String),
        pl.col("nested_localities_str").cast(pl.String),
        pl.col("tightest_nested_locality").cast(pl.String),
        pl.col("paying_localities").cast(pl.String),
        pl.col("v_constraint_group_kw_yr").cast(pl.Float64),
    )

    print(f"Loaded constraint-group table with {len(df)} rows from {path}")
    return df


# ── SCR allocation helpers ───────────────────────────────────────────────────


def identify_scr_hours(
    load_df: pl.DataFrame,
    n_hours_per_season: int = N_SCR_HOURS_PER_SEASON,
    winter_months: list[int] | None = None,
) -> pl.DataFrame:
    """Identify top-N SCR hours in each season."""
    if winter_months is None:
        winter_months = list(DEFAULT_SCR_WINTER_MONTHS)
    summer_months = derive_summer_months(winter_months)

    result = load_df.with_columns(
        pl.when(pl.col("timestamp").dt.month().is_in(summer_months))
        .then(pl.lit("summer"))
        .otherwise(pl.lit("winter"))
        .alias("season")
    )

    summer = result.filter(pl.col("season") == "summer").sort(
        "load_mw", descending=True
    )
    winter = result.filter(pl.col("season") == "winter").sort(
        "load_mw", descending=True
    )

    if summer.height < n_hours_per_season or winter.height < n_hours_per_season:
        raise ValueError("Not enough hours to identify SCR windows")

    summer_scr_ts = summer.head(n_hours_per_season)["timestamp"].to_list()
    winter_scr_ts = winter.head(n_hours_per_season)["timestamp"].to_list()

    result = result.with_columns(
        (
            pl.col("timestamp").is_in(summer_scr_ts)
            | pl.col("timestamp").is_in(winter_scr_ts)
        ).alias("is_scr")
    )

    n_scr_total = result.filter(pl.col("is_scr")).height
    expected = 2 * n_hours_per_season
    if n_scr_total != expected:
        raise ValueError(f"Expected {expected} SCR hours, got {n_scr_total}")

    return result


def allocate_bulk_tx_to_hours(
    load_with_scr: pl.DataFrame,
    v_constraint_group_kw_yr: float,
    n_hours_per_season: int = N_SCR_HOURS_PER_SEASON,
) -> pl.DataFrame:
    """Allocate annual value to hours using two-level SCR exceedance weights."""
    season_stats = load_with_scr.group_by("season").agg(
        pl.col("load_mw").max().alias("peak_mw"),
        pl.col("load_mw").top_k(n_hours_per_season + 1).min().alias("threshold_mw"),
    )

    peaks = dict(zip(season_stats["season"], season_stats["peak_mw"], strict=True))
    thresholds = dict(
        zip(season_stats["season"], season_stats["threshold_mw"], strict=True)
    )

    tau_min = min(thresholds.values())
    summer_surplus = peaks["summer"] - tau_min
    winter_surplus = peaks["winter"] - tau_min
    total_surplus = summer_surplus + winter_surplus
    if total_surplus <= 0:
        raise ValueError("Invalid SCR surplus; cannot compute seasonal weights")

    phi_s = summer_surplus / total_surplus
    phi_w = 1.0 - phi_s

    season_phi = season_stats.with_columns(
        pl.when(pl.col("season") == "summer").then(phi_s).otherwise(phi_w).alias("phi")
    ).select("season", "threshold_mw", "phi")

    with_exc = load_with_scr.join(season_phi, on="season").with_columns(
        pl.when(pl.col("is_scr"))
        .then((pl.col("load_mw") - pl.col("threshold_mw")).clip(lower_bound=0.0))
        .otherwise(0.0)
        .alias("exceedance")
    )

    exc_sums = with_exc.group_by("season").agg(
        pl.col("exceedance").sum().alias("exc_sum")
    )
    for exc_sum in exc_sums["exc_sum"].to_list():
        if float(exc_sum) <= 0:
            raise ValueError("Zero seasonal exceedance; cannot allocate SCR weights")

    result = (
        with_exc.join(exc_sums, on="season")
        .with_columns(
            (pl.col("exceedance") / pl.col("exc_sum") * pl.col("phi")).alias("weight")
        )
        .with_columns(
            (pl.col("weight") * v_constraint_group_kw_yr).alias("bulk_tx_cost_enduse")
        )
        .select("timestamp", "bulk_tx_cost_enduse")
    )

    weight_sum = float((result["bulk_tx_cost_enduse"] / v_constraint_group_kw_yr).sum())
    if abs(weight_sum - 1.0) > 1e-4:
        raise ValueError(f"Global SCR weights sum to {weight_sum:.6f}, expected 1.0")

    return result


# ── Constraint-group engine ──────────────────────────────────────────────────


def compute_paying_locality_costs(
    constraint_group_df: pl.DataFrame,
) -> dict[str, float]:
    """Compute mean annual cost per paying locality from constraint groups."""
    exploded = (
        constraint_group_df.with_columns(
            pl.col("paying_localities").str.split("|").alias("paying_locality_list")
        )
        .explode("paying_locality_list")
        .rename({"paying_locality_list": "paying_locality"})
    )

    invalid = sorted(
        set(exploded["paying_locality"].drop_nulls().to_list())
        - set(VALID_PAYING_LOCALITIES)
    )
    if invalid:
        raise ValueError(
            f"Invalid paying_locality values in constraint groups: {invalid}. "
            f"Expected {sorted(VALID_PAYING_LOCALITIES)}"
        )

    locality_costs = (
        exploded.group_by("paying_locality")
        .agg(pl.col("v_constraint_group_kw_yr").mean().alias("mean_cost_kw_yr"))
        .sort("paying_locality")
    )

    result: dict[str, float] = {}
    for row in locality_costs.iter_rows(named=True):
        result[str(row["paying_locality"])] = float(row["mean_cost_kw_yr"])

    return result


def build_nested_locality_load_profiles(
    zone_loads_df: pl.DataFrame,
    nested_localities: list[str],
) -> dict[str, pl.DataFrame]:
    """Build load profile per nested locality from NYISO zone loads."""
    profiles: dict[str, pl.DataFrame] = {}
    for locality in nested_localities:
        zone_names = NESTED_LOCALITY_ZONES[locality]
        profile = (
            zone_loads_df.filter(pl.col("zone").is_in(zone_names))
            .group_by("timestamp")
            .agg(pl.col("load_mw").sum().alias("load_mw"))
            .sort("timestamp")
        )
        if profile.is_empty():
            raise ValueError(
                f"No zone loads found for nested locality {locality} (zones={zone_names})"
            )
        profiles[locality] = profile
    return profiles


def compute_nested_locality_scr_weights(
    load_profile: pl.DataFrame,
    n_hours_per_season: int,
    winter_months: list[int],
) -> pl.DataFrame:
    """Compute locality-specific SCR weights (sum to 1 across all hours)."""
    with_scr = identify_scr_hours(
        load_profile,
        n_hours_per_season=n_hours_per_season,
        winter_months=winter_months,
    )
    one_dollar = allocate_bulk_tx_to_hours(
        with_scr,
        v_constraint_group_kw_yr=1.0,
        n_hours_per_season=n_hours_per_season,
    )
    return one_dollar.rename({"bulk_tx_cost_enduse": "scr_weight"})


def allocate_constraint_group_to_hours(
    row: dict[str, object],
    locality_weights: dict[str, pl.DataFrame],
) -> pl.DataFrame:
    """Allocate one constraint group's annual value using its tightest locality."""
    locality = str(row["tightest_nested_locality"])
    weights = locality_weights[locality]
    value = cast(float, row["v_constraint_group_kw_yr"])

    return weights.with_columns(
        pl.lit(str(row["constraint_group"])).alias("constraint_group"),
        pl.lit(locality).alias("tightest_nested_locality"),
        pl.lit(str(row["paying_localities"])).alias("paying_localities"),
        (pl.col("scr_weight") * value).alias("constraint_group_cost_enduse"),
    ).select(
        "timestamp",
        "constraint_group",
        "tightest_nested_locality",
        "paying_localities",
        "constraint_group_cost_enduse",
    )


def aggregate_paying_locality_hourly_signals_from_constraint_groups(
    constraint_group_hourly: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Average contributing constraint-group hourly signals per paying locality."""
    exploded = (
        constraint_group_hourly.with_columns(
            pl.col("paying_localities").str.split("|").alias("paying_locality_list")
        )
        .explode("paying_locality_list")
        .rename({"paying_locality_list": "paying_locality"})
    )

    invalid = sorted(
        set(exploded["paying_locality"].drop_nulls().to_list())
        - set(VALID_PAYING_LOCALITIES)
    )
    if invalid:
        raise ValueError(
            f"Invalid paying_locality values in constraint groups: {invalid}. "
            f"Expected {sorted(VALID_PAYING_LOCALITIES)}"
        )

    locality_hourly = (
        exploded.group_by("paying_locality", "timestamp")
        .agg(pl.col("constraint_group_cost_enduse").mean().alias("bulk_tx_cost_enduse"))
        .sort("paying_locality", "timestamp")
    )

    result: dict[str, pl.DataFrame] = {}
    for locality in locality_hourly["paying_locality"].unique().to_list():
        result[str(locality)] = locality_hourly.filter(
            pl.col("paying_locality") == locality
        ).select("timestamp", "bulk_tx_cost_enduse")

    return result


def resolve_utility_paying_locality_signal(
    mapping_df: pl.DataFrame,
    utility: str,
    paying_locality_hourly: dict[str, pl.DataFrame],
) -> pl.DataFrame:
    """Blend paying-locality hourly signals into one utility-level signal."""
    utility_weights = (
        mapping_df.filter(pl.col("utility") == utility)
        .select("gen_capacity_zone", "capacity_weight")
        .group_by("gen_capacity_zone")
        .agg(pl.col("capacity_weight").sum().alias("capacity_weight"))
        .rename({"gen_capacity_zone": "paying_locality"})
        .sort("paying_locality")
    )
    if utility_weights.is_empty():
        available = sorted(mapping_df["utility"].unique().to_list())
        raise ValueError(
            f"Utility '{utility}' not found in zone mapping. Available: {available}"
        )

    missing = sorted(
        set(utility_weights["paying_locality"].to_list()) - set(paying_locality_hourly)
    )
    if missing:
        raise ValueError(
            f"No paying-locality hourly signal available for {missing}. "
            "Check constraint-group table coverage."
        )

    total_weight = float(utility_weights["capacity_weight"].sum())
    if total_weight <= 0:
        raise ValueError(f"capacity_weight sum must be positive for utility={utility}")
    utility_weights = utility_weights.with_columns(
        (pl.col("capacity_weight") / total_weight).alias("capacity_weight")
    )

    locality_frames: list[pl.DataFrame] = []
    for row in utility_weights.iter_rows(named=True):
        locality = str(row["paying_locality"])
        weight = float(row["capacity_weight"])
        locality_frames.append(
            paying_locality_hourly[locality].with_columns(
                pl.lit(locality).alias("paying_locality"),
                pl.lit(weight).alias("capacity_weight"),
            )
        )

    blended = (
        pl.concat(locality_frames)
        .with_columns(
            (pl.col("bulk_tx_cost_enduse") * pl.col("capacity_weight")).alias(
                "weighted_cost"
            )
        )
        .group_by("timestamp")
        .agg(pl.col("weighted_cost").sum().alias("bulk_tx_cost_enduse"))
        .sort("timestamp")
    )

    return blended


def compute_utility_bulk_tx_signal(
    utility_icap_rows: pl.DataFrame,
    paying_locality_costs: dict[str, float],
    locality_profiles: dict[str, pl.DataFrame],
    n_scr: int,
    winter_months: list[int],
) -> pl.DataFrame:
    """Compute utility bulk TX hourly signal from utility ICAP locality rows."""
    component_frames: list[pl.DataFrame] = []

    for row in utility_icap_rows.iter_rows(named=True):
        icap_locality_raw = str(row["icap_locality"])
        gen_capacity_zone = str(row["gen_capacity_zone"])
        capacity_weight = float(row["capacity_weight"])

        if icap_locality_raw not in ICAP_RAW_TO_NESTED_LOCALITY:
            raise ValueError(
                f"Unknown icap_locality {icap_locality_raw!r}. "
                f"Expected one of {sorted(ICAP_RAW_TO_NESTED_LOCALITY)}."
            )
        nested_locality = ICAP_RAW_TO_NESTED_LOCALITY[icap_locality_raw]

        if gen_capacity_zone not in paying_locality_costs:
            raise ValueError(
                f"No cost available for paying locality {gen_capacity_zone!r}. "
                f"Available: {sorted(paying_locality_costs)}"
            )
        if nested_locality not in locality_profiles:
            raise ValueError(
                f"No load profile for nested locality {nested_locality!r}. "
                f"Available: {sorted(locality_profiles)}"
            )

        component_value = paying_locality_costs[gen_capacity_zone] * capacity_weight
        load_profile = locality_profiles[nested_locality]
        with_scr = identify_scr_hours(
            load_profile,
            n_hours_per_season=n_scr,
            winter_months=winter_months,
        )
        component_hourly = allocate_bulk_tx_to_hours(
            with_scr,
            v_constraint_group_kw_yr=component_value,
            n_hours_per_season=n_scr,
        )
        component_frames.append(component_hourly)

    if not component_frames:
        raise ValueError("No ICAP locality components found for utility")

    return (
        pl.concat(component_frames)
        .group_by("timestamp")
        .agg(pl.col("bulk_tx_cost_enduse").sum().alias("bulk_tx_cost_enduse"))
        .sort("timestamp")
    )


# ── Output assembly ──────────────────────────────────────────────────────────


def prepare_output(allocated_df: pl.DataFrame, year: int) -> pl.DataFrame:
    """Prepare output on CAIRO 8760 timestamp index."""
    ref_8760 = build_cairo_8760_timestamps(year)

    hourly = allocated_df.with_columns(
        pl.col("timestamp").dt.truncate("1h").alias("timestamp")
    )
    hourly = (
        hourly.group_by("timestamp")
        .agg(pl.col("bulk_tx_cost_enduse").mean())
        .sort("timestamp")
    )

    output = ref_8760.join(hourly, on="timestamp", how="left").with_columns(
        pl.col("bulk_tx_cost_enduse").fill_null(0.0).alias("bulk_tx_cost_enduse")
    )

    if output.height != 8760:
        raise ValueError(f"Output has {output.height} rows, expected 8760")
    if output.filter(pl.col("bulk_tx_cost_enduse").is_null()).height > 0:
        raise ValueError("Output has null bulk_tx_cost_enduse values")

    return output.select("timestamp", "bulk_tx_cost_enduse")


def save_output(
    output_df: pl.DataFrame,
    utility: str,
    year: int,
    output_s3_base: str,
    storage_options: dict[str, str],
) -> None:
    """Write bulk Tx MC parquet to S3 (hive-style utility/year partition path)."""
    output_s3_base = output_s3_base.rstrip("/") + "/"
    output_path = f"{output_s3_base}utility={utility}/year={year}/data.parquet"
    output_df.write_parquet(output_path, storage_options=storage_options)

    print(f"\n✓ Saved bulk Tx MC to {output_path}")
    print(f"  Rows: {len(output_df):,}")


def print_summary(output_df: pl.DataFrame) -> None:
    """Print top-10 hours and summary statistics for a bulk TX output DataFrame."""
    from typing import cast as _cast

    print("\n" + "=" * 60)
    print("SAMPLE: Top 10 hours by bulk Tx cost")
    print("=" * 60)
    print(output_df.sort("bulk_tx_cost_enduse", descending=True).head(10))

    avg_cost = _cast(float, output_df["bulk_tx_cost_enduse"].mean())
    max_cost = _cast(float, output_df["bulk_tx_cost_enduse"].max())
    n_nonzero = output_df.filter(pl.col("bulk_tx_cost_enduse") > 0).height
    print("\nOutput summary:")
    print(f"  avg = ${avg_cost:.6f}/kWh, max = ${max_cost:.6f}/kWh")
    print(f"  {n_nonzero} non-zero hours out of 8760")
