"""Capacity (ICAP) marginal cost computation for NY (NYISO) utility supply MCs."""

from __future__ import annotations

import polars as pl

from utils.pre.marginal_costs.generate_utility_tx_dx_mc import (
    normalize_load_to_cairo_8760,
)
from utils.pre.marginal_costs.supply_utils import load_zone_loads, remap_year_if_needed

N_PEAK_HOURS_PER_MONTH = 8

# These are overlapping by design (NYCA ⊃ LHV ⊃ NYC, and LI=K).
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

ICAP_RAW_TO_NESTED_LOCALITY = {
    "NYCA": "NYCA",
    "GHIJ": "LHV",
    "NYC": "NYC",
    "LI": "LI",
}
NESTED_LOCALITY_TO_ICAP_RAW = {
    nested: raw for raw, nested in ICAP_RAW_TO_NESTED_LOCALITY.items()
}

# Partitioned localities used when applying utility splits to prices.
GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY = {
    "ROS": "NYCA",  # A-F
    "LHV": "LHV",  # G-I
    "NYC": "NYC",  # J
    "LI": "LI",  # K
}

VALID_PARTITIONED_LOCALITIES = frozenset(NESTED_LOCALITY_TO_ICAP_RAW)


def load_icap_spot_prices(
    icap_s3_base: str,
    localities: list[str],
    year: int,
    storage_options: dict[str, str],
) -> pl.DataFrame:
    """Load ICAP Spot prices for partitioned localities and year."""
    raw_localities = sorted({NESTED_LOCALITY_TO_ICAP_RAW[loc] for loc in localities})
    base = icap_s3_base.rstrip("/") + "/"
    collected = (
        pl.scan_parquet(
            base,
            hive_partitioning=True,
            storage_options=storage_options,
        )
        .filter(
            pl.col("year") == year,
            pl.col("auction_type") == "Spot",
            pl.col("locality").is_in(raw_localities),
        )
        .select("month", "locality", "price_per_kw_month")
        .collect()
    )
    if not isinstance(collected, pl.DataFrame):
        raise TypeError("Expected DataFrame from ICAP collect()")

    collected = collected.with_columns(
        pl.col("month").cast(pl.Int32),
        pl.col("locality")
        .cast(pl.Utf8)
        .replace_strict(ICAP_RAW_TO_NESTED_LOCALITY)
        .alias("locality"),
    )
    if collected.is_empty():
        raise FileNotFoundError(
            f"No ICAP Spot data found for localities={raw_localities}, year={year} under {base}"
        )

    locs_found = sorted(collected["locality"].unique().to_list())
    months_found = sorted(collected["month"].unique().to_list())
    print(
        f"Loaded ICAP Spot prices: {len(collected)} rows, localities={locs_found}, months={months_found}"
    )
    return collected


def _resolve_locality_weights(
    utility_mapping: pl.DataFrame,
    source_col: str,
    source_to_locality: dict[str, str] | None,
    valid_localities: frozenset[str],
    purpose: str,
) -> pl.DataFrame:
    locality_expr = pl.col(source_col).cast(pl.Utf8)
    if source_to_locality:
        locality_expr = locality_expr.replace_strict(source_to_locality)

    locality_weights = (
        utility_mapping.select(source_col, "capacity_weight")
        .unique()
        .with_columns(locality_expr.alias("locality"))
        .group_by("locality")
        .agg(pl.col("capacity_weight").sum().alias("capacity_weight"))
        .sort("locality")
    )
    if locality_weights.is_empty():
        raise ValueError(f"No locality weights found for {purpose}")

    invalid = sorted(
        set(locality_weights["locality"].to_list()) - set(valid_localities)
    )
    if invalid:
        raise ValueError(
            f"Unknown localities for {purpose}: {invalid}. "
            f"Expected {sorted(valid_localities)}."
        )
    return locality_weights


def get_partitioned_price_locality_weights(
    utility_mapping: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve partitioned locality weights for ICAP price blending."""
    return _resolve_locality_weights(
        utility_mapping=utility_mapping,
        source_col="gen_capacity_zone",
        source_to_locality=GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY,
        valid_localities=VALID_PARTITIONED_LOCALITIES,
        purpose="partitioned ICAP pricing",
    )


def compute_weighted_icap_prices(
    icap_df: pl.DataFrame,
    locality_weights: pl.DataFrame,
) -> pl.DataFrame:
    """Compute weighted ICAP price per month for a utility."""
    print(f"  ICAP locality weights: {locality_weights.to_dicts()}")
    result = (
        icap_df.join(locality_weights, on="locality", how="inner")
        .group_by("month")
        .agg(
            (pl.col("price_per_kw_month") * pl.col("capacity_weight"))
            .sum()
            .alias("icap_price_per_kw_month")
        )
        .sort("month")
    )

    if result.height != 12:
        months_found = sorted(result["month"].to_list())
        raise ValueError(
            f"Expected 12 months of ICAP prices, got {result.height}: {months_found}"
        )

    total_annual = result["icap_price_per_kw_month"].sum()
    print(f"  Annual ICAP total: ${total_annual:.2f}/kW-yr (sum of 12 monthly prices)")
    return result


def build_locality_load_profiles(
    icap_locality_names: list[str],
    zone_loads_df: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Build raw load profile per nested locality from NYISO zone loads."""
    profiles: dict[str, pl.DataFrame] = {}
    for raw_name in icap_locality_names:
        if raw_name not in ICAP_RAW_TO_NESTED_LOCALITY:
            raise ValueError(
                f"Unknown ICAP locality name: {raw_name!r}. "
                f"Expected one of {sorted(ICAP_RAW_TO_NESTED_LOCALITY)}."
            )
        nested = ICAP_RAW_TO_NESTED_LOCALITY[raw_name]
        if nested in profiles:
            continue

        zone_names = NESTED_LOCALITY_ZONES[nested]
        profile = (
            zone_loads_df.filter(pl.col("zone").is_in(zone_names))
            .group_by("timestamp")
            .agg(pl.col("load_mw").sum().alias("load_mw"))
            .sort("timestamp")
        )
        if profile.is_empty():
            raise ValueError(
                f"No zone load rows found for nested locality={nested!r} "
                f"(zones={zone_names}). Raw ICAP name: {raw_name!r}."
            )
        profiles[nested] = profile
    return profiles


def zone_names_for_localities(
    localities: list[str], locality_zone_map: dict[str, list[str]]
) -> list[str]:
    """Return sorted unique zone names covered by the given localities."""
    return sorted({z for loc in localities for z in locality_zone_map[loc]})


def allocate_icap_to_hours(
    utility_load_df: pl.DataFrame,
    icap_prices: pl.DataFrame,
    n_peak_hours: int = N_PEAK_HOURS_PER_MONTH,
) -> pl.DataFrame:
    """Allocate monthly ICAP $/kW-month to hourly $/kW via threshold exceedance."""
    load_with_month = utility_load_df.with_columns(
        pl.col("timestamp").dt.month().cast(pl.Int32).alias("month")
    )
    all_months: list[pl.DataFrame] = []

    for month_num in range(1, 13):
        month_load = load_with_month.filter(pl.col("month") == month_num)
        if month_load.is_empty():
            raise ValueError(f"No load data for month {month_num}")

        price_row = icap_prices.filter(pl.col("month") == month_num)
        if price_row.is_empty():
            raise ValueError(f"No ICAP price for month {month_num}")
        icap_price = float(price_row["icap_price_per_kw_month"][0])

        sorted_load = month_load.sort("load_mw", descending=True)
        if sorted_load.height < n_peak_hours:
            raise ValueError(
                f"Month {month_num} has only {sorted_load.height} hours, "
                f"need at least {n_peak_hours} for threshold computation"
            )

        top_n = sorted_load.head(n_peak_hours)
        load_nth = float(top_n["load_mw"][-1])
        below = month_load.filter(pl.col("load_mw") < load_nth)["load_mw"]
        threshold = float(below.max()) if not below.is_empty() else 0.0  # type: ignore[arg-type]

        month_result = top_n.with_columns(
            (pl.col("load_mw") - threshold).alias("exceedance")
        )
        total_exceedance = float(month_result["exceedance"].sum())
        if total_exceedance <= 0:
            raise ValueError(
                f"Month {month_num}: total exceedance is zero or negative. "
                f"Threshold={threshold:.2f}, max load={sorted_load['load_mw'][0]:.2f}"
            )

        month_result = month_result.with_columns(
            (pl.col("exceedance") / total_exceedance * icap_price).alias(
                "capacity_cost_per_kw"
            )
        )

        weight_sum = (month_result["exceedance"] / total_exceedance).sum()
        if abs(weight_sum - 1.0) > 1e-6:
            raise ValueError(
                f"Month {month_num}: weights sum to {weight_sum:.6f}, expected 1.0"
            )

        n_nonzero = month_result.filter(pl.col("capacity_cost_per_kw") > 0).height
        print(
            f"  Month {month_num:2d}: ICAP=${icap_price:6.2f}/kW-mo, "
            f"threshold={threshold:,.1f} MW, "
            f"{n_nonzero} peak hours, "
            f"total exceedance={total_exceedance:,.1f} MW"
        )

        all_months.append(month_result.select("timestamp", "capacity_cost_per_kw"))

    return pl.concat(all_months).sort("timestamp")


def compute_components(
    utility_icap_rows: pl.DataFrame,
    icap_df: pl.DataFrame,
    locality_profiles: dict[str, pl.DataFrame],
    n_peak_hours: int = N_PEAK_HOURS_PER_MONTH,
) -> pl.DataFrame:
    """Compute capacity MC by summing per-locality components."""
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

        if gen_capacity_zone not in GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY:
            raise ValueError(
                f"Unknown gen_capacity_zone {gen_capacity_zone!r}. "
                f"Expected one of {sorted(GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY)}."
            )
        partitioned_locality = GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY[
            gen_capacity_zone
        ]

        if nested_locality not in locality_profiles:
            raise ValueError(
                f"No load profile for nested locality {nested_locality!r}. "
                f"Available: {sorted(locality_profiles)}"
            )
        load_profile = locality_profiles[nested_locality]

        component_icap = icap_df.filter(pl.col("locality") == partitioned_locality)
        if component_icap.is_empty():
            raise ValueError(
                f"No ICAP prices found for partitioned locality {partitioned_locality!r}. "
                f"Available: {sorted(icap_df['locality'].unique().to_list())}"
            )
        component_prices = component_icap.select(
            pl.col("month"),
            (pl.col("price_per_kw_month") * capacity_weight).alias(
                "icap_price_per_kw_month"
            ),
        )

        print(
            f"  Component: icap_locality={icap_locality_raw!r} -> "
            f"nested={nested_locality!r}, "
            f"gen_capacity_zone={gen_capacity_zone!r} -> "
            f"partitioned={partitioned_locality!r}, "
            f"weight={capacity_weight:.4f}"
        )
        component_frames.append(
            allocate_icap_to_hours(load_profile, component_prices, n_peak_hours)
        )

    if not component_frames:
        raise ValueError("No ICAP locality components found for utility")

    return (
        pl.concat(component_frames)
        .group_by("timestamp")
        .agg(pl.col("capacity_cost_per_kw").sum().alias("capacity_cost_per_kw"))
        .sort("timestamp")
    )


def validate_allocation(capacity_df: pl.DataFrame, icap_prices: pl.DataFrame) -> None:
    """Validate 1 kW constant load recovers annual ICAP total."""
    expected_annual = float(icap_prices["icap_price_per_kw_month"].sum())
    actual_annual = float(capacity_df["capacity_cost_per_kw"].sum())
    error = abs(actual_annual - expected_annual)
    error_pct = (error / expected_annual * 100) if expected_annual > 0 else 0.0

    print("\n" + "=" * 60)
    print("VALIDATION: 1 kW Constant Load -> ICAP Recovery")
    print("=" * 60)
    print(f"  Expected (sum of 12 ICAP Spot prices): ${expected_annual:.4f}/kW-yr")
    print(f"  Actual (sum of hourly allocations):     ${actual_annual:.4f}/kW-yr")
    print(f"  Error: ${error:.6f} ({error_pct:.6f}%)")

    tolerance = 0.01
    if error_pct > tolerance:
        print("  Validation FAILED")
        print("=" * 60)
        raise ValueError(
            f"ICAP validation failed: error {error_pct:.6f}% exceeds {tolerance}%. "
            f"Expected ${expected_annual:.4f}/kW-yr, got ${actual_annual:.4f}/kW-yr."
        )
    print("  Validation PASSED")
    print("=" * 60)


def compute_supply_capacity_mc(
    utility_mapping: pl.DataFrame,
    utility: str,
    icap_s3_base: str,
    zone_loads_s3_base: str,
    price_year: int,
    storage_options: dict[str, str],
    peak_hours: int = N_PEAK_HOURS_PER_MONTH,
    capacity_load_year: int | None = None,
) -> pl.DataFrame:
    """Compute hourly utility-level supply capacity MC from ICAP Spot data."""
    capacity_load_year = (
        price_year if capacity_load_year is None else capacity_load_year
    )

    utility_icap_rows = utility_mapping.select(
        "icap_locality", "gen_capacity_zone", "capacity_weight"
    ).unique()
    print(f"  Utility ICAP rows:\n{utility_icap_rows}")

    partitioned_localities = sorted(
        {
            GEN_CAPACITY_ZONE_TO_PARTITIONED_LOCALITY[z]
            for z in utility_icap_rows["gen_capacity_zone"].to_list()
        }
    )
    print(f"  Loading ICAP prices for partitioned localities: {partitioned_localities}")

    icap_df = load_icap_spot_prices(
        icap_s3_base, partitioned_localities, price_year, storage_options
    )

    icap_locality_names = utility_icap_rows["icap_locality"].to_list()
    nested_localities = sorted(
        {ICAP_RAW_TO_NESTED_LOCALITY[raw] for raw in icap_locality_names}
    )
    zones_needed = zone_names_for_localities(nested_localities, NESTED_LOCALITY_ZONES)
    print(
        f"\n  Building locality load profiles for year {capacity_load_year}..."
        f"\n  Nested localities: {nested_localities}"
        f"\n  Zones needed: {zones_needed}"
    )

    zone_loads_df = load_zone_loads(
        zone_loads_s3_base,
        zones_needed,
        capacity_load_year,
        storage_options,
    )
    raw_profiles = build_locality_load_profiles(icap_locality_names, zone_loads_df)

    locality_profiles = {
        loc: normalize_load_to_cairo_8760(profile, utility, capacity_load_year)
        for loc, profile in raw_profiles.items()
    }

    print("\n  Computing capacity MC (component-by-component):")
    capacity_df = compute_components(
        utility_icap_rows,
        icap_df,
        locality_profiles,
        peak_hours,
    )

    price_locality_weights = get_partitioned_price_locality_weights(utility_mapping)
    icap_prices_for_validation = compute_weighted_icap_prices(
        icap_df, price_locality_weights
    )
    validate_allocation(capacity_df, icap_prices_for_validation)

    if capacity_load_year != price_year:
        print(
            f"\n  Remapping capacity timestamps: {capacity_load_year} -> {price_year}"
        )
        capacity_df = remap_year_if_needed(
            capacity_df,
            "timestamp",
            capacity_load_year,
            price_year,
        )

    return capacity_df
