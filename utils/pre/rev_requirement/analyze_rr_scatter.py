"""Analyze revenue requirements scatter plots and create additional analysis outputs.

This script:
1. Analyzes why utilities (especially ConEd) diverge from expected patterns
2. Creates scatter plots for RIE revenue requirement components vs kWh and customer count
3. Produces analysis outputs explaining the behavior
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import cast

import polars as pl
import yaml
from plotnine import (
    aes,
    facet_wrap,
    geom_point,
    geom_text,
    ggplot,
    labs,
    scale_color_manual,
    theme_minimal,
)

from data.eia.hourly_loads.eia_region_config import get_aws_storage_options
from utils.scenario_config import get_residential_customer_count_from_utility_stats

UtilityData = dict[str, str | float]


def _is_standard_yaml(path: Path) -> bool:
    """Return True if path is a standard utility YAML (exclude special variants)."""
    name = path.name
    if name.startswith("delivery_") or name.startswith("supply_"):
        return False
    if "_hp_vs_nonhp" in name or "_large_number" in name:
        return False
    return name.endswith(".yaml") and path.is_file()


def _load_utility_yamls(path_rev_req: Path) -> list[UtilityData]:
    """Load all standard utility YAML files from the directory."""
    yamls = []
    for yaml_path in sorted(path_rev_req.glob("*.yaml")):
        if not _is_standard_yaml(yaml_path):
            continue
        with yaml_path.open() as f:
            data = cast(UtilityData, yaml.safe_load(f))
        yamls.append(data)
    return yamls


def _load_all_utilities(
    path_ny_rev_req: Path, path_ri_rev_req: Path
) -> list[tuple[UtilityData, str]]:
    """Load all NY and RI utilities, returning (data, state) tuples."""
    utilities = []
    for data in _load_utility_yamls(path_ny_rev_req):
        utilities.append((data, "NY"))
    for data in _load_utility_yamls(path_ri_rev_req):
        utilities.append((data, "RI"))
    return utilities


def _get_customer_counts(
    utilities: list[tuple[UtilityData, str]],
    path_ny_stats: str,
    path_ri_stats: str,
) -> dict[str, int]:
    """Get residential customer counts for all utilities."""
    storage_options = get_aws_storage_options()
    customer_counts: dict[str, int] = {}

    for data, state in utilities:
        utility = str(data["utility"])
        if utility in customer_counts:
            continue

        path_stats = path_ny_stats if state == "NY" else path_ri_stats
        try:
            count = get_residential_customer_count_from_utility_stats(
                path_stats, utility, storage_options=storage_options
            )
            customer_counts[utility] = count
        except Exception as e:
            print(f"Warning: Could not get customer count for {utility}: {e}")
            customer_counts[utility] = 0

    return customer_counts


def _analyze_coned_divergence(
    utilities: list[tuple[UtilityData, str]], output_path: Path
) -> None:
    """Analyze why ConEd diverges from expected patterns and write analysis."""
    rows = []
    for data, state in utilities:
        utility = str(data["utility"])
        total_kwh = float(data["total_residential_kwh"])
        delivery_rr = float(data["total_delivery_revenue_requirement"])
        delivery_and_supply_rr = float(
            data["total_delivery_and_supply_revenue_requirement"]
        )

        # Calculate rates per kWh
        delivery_rate_per_kwh = delivery_rr / total_kwh if total_kwh > 0 else 0
        total_rate_per_kwh = delivery_and_supply_rr / total_kwh if total_kwh > 0 else 0

        rows.append(
            {
                "utility": utility,
                "state": state,
                "total_residential_kwh": total_kwh,
                "delivery_rr": delivery_rr,
                "delivery_and_supply_rr": delivery_and_supply_rr,
                "delivery_rate_per_kwh": delivery_rate_per_kwh,
                "total_rate_per_kwh": total_rate_per_kwh,
            }
        )

    df = pl.DataFrame(rows)

    # Calculate statistics
    coned_row = df.filter(pl.col("utility") == "coned")
    if coned_row.height > 0:
        coned_kwh = coned_row["total_residential_kwh"].item()
        coned_delivery_rr = coned_row["delivery_rr"].item()
        coned_total_rr = coned_row["delivery_and_supply_rr"].item()
        coned_delivery_rate = coned_row["delivery_rate_per_kwh"].item()
        coned_total_rate = coned_row["total_rate_per_kwh"].item()

        # Compare to other utilities
        other_utilities = df.filter(pl.col("utility") != "coned")
        avg_delivery_rate = other_utilities["delivery_rate_per_kwh"].mean()
        avg_total_rate = other_utilities["total_rate_per_kwh"].mean()

        # Find utilities with similar kWh
        similar_kwh = df.filter(
            (pl.col("total_residential_kwh") >= coned_kwh * 0.5)
            & (pl.col("total_residential_kwh") <= coned_kwh * 1.5)
            & (pl.col("utility") != "coned")
        )

        analysis = f"""# Revenue Requirements Analysis: Why ConEd Diverges

## Summary

ConEd (Consolidated Edison) shows significant divergence from other utilities in revenue requirements per kWh, particularly in delivery rates.

## Key Metrics

### ConEd Values
- **Total Residential kWh**: {coned_kwh:,.0f}
- **Delivery Revenue Requirement**: ${coned_delivery_rr:,.2f}
- **Total (Delivery + Supply) Revenue Requirement**: ${coned_total_rr:,.2f}
- **Delivery Rate per kWh**: ${coned_delivery_rate:.6f}
- **Total Rate per kWh**: ${coned_total_rate:.6f}

### Comparison to Other Utilities
- **Average Delivery Rate per kWh (excluding ConEd)**: ${avg_delivery_rate:.6f}
- **Average Total Rate per kWh (excluding ConEd)**: ${avg_total_rate:.6f}
- **ConEd Delivery Rate vs Average**: {((coned_delivery_rate / avg_delivery_rate - 1) * 100):+.1f}%
- **ConEd Total Rate vs Average**: {((coned_total_rate / avg_total_rate - 1) * 100):+.1f}%

## Possible Explanations for Divergence

1. **Urban Density and Infrastructure Costs**: ConEd serves New York City, one of the most densely populated urban areas in the US. Urban utilities face:
   - Higher infrastructure costs (underground distribution, complex routing)
   - Higher land costs for substations and equipment
   - More complex grid management requirements
   - Higher maintenance costs in dense urban environments

2. **Regulatory Environment**: NY PSC may have different rate-setting methodologies or cost recovery mechanisms that result in higher delivery charges.

3. **Service Territory Characteristics**:
   - High-rise buildings require more complex distribution infrastructure
   - Higher peak demand per customer
   - More complex load patterns

4. **Historical Rate Case Outcomes**: ConEd's rate cases may have resulted in higher approved revenue requirements relative to kWh sales.

## Utilities with Similar kWh (for comparison)
"""

        if similar_kwh.height > 0:
            analysis += (
                "\n| Utility | State | kWh | Delivery RR | Delivery Rate/kWh |\n"
            )
            analysis += "|---------|-------|-----|-------------|-------------------|\n"
            for row in similar_kwh.iter_rows(named=True):
                analysis += f"| {row['utility']} | {row['state']} | {row['total_residential_kwh']:,.0f} | ${row['delivery_rr']:,.2f} | ${row['delivery_rate_per_kwh']:.6f} |\n"
        else:
            analysis += "\nNo other utilities found with similar kWh volumes.\n"

        analysis += """

## All Utilities Summary

| Utility | State | kWh | Delivery RR | Delivery Rate/kWh | Total Rate/kWh |
|---------|-------|-----|-------------|-------------------|----------------|
"""

        for row in df.sort("total_residential_kwh", descending=True).iter_rows(
            named=True
        ):
            analysis += f"| {row['utility']} | {row['state']} | {row['total_residential_kwh']:,.0f} | ${row['delivery_rr']:,.2f} | ${row['delivery_rate_per_kwh']:.6f} | ${row['total_rate_per_kwh']:.6f} |\n"

        output_path.write_text(analysis)
        print(f"Saved analysis to {output_path}")
    else:
        print("Warning: ConEd data not found, skipping analysis")


def _plot_rie_components(
    rie_data: UtilityData,
    customer_count: int,
    output_path: Path,
) -> None:
    """Plot RIE revenue requirement components vs kWh and customer count.

    Creates two panels: one with kWh on x-axis, one with customer count on x-axis.
    Each panel shows all three revenue requirement components as separate points.
    """
    total_kwh = float(rie_data["total_residential_kwh"])

    # Extract the three revenue requirement values
    delivery_from_rate_case = float(
        rie_data["delivery_revenue_requirement_from_rate_case"]
    )
    delivery_topups = float(rie_data["delivery_revenue_requirement_topups"])
    supply_topups = float(rie_data["supply_revenue_requirement_topups"])

    # Create data for plotting - two panels with different x variables
    plot_data = []
    for metric_name, metric_value in [
        ("Delivery RR\n(Rate Case)", delivery_from_rate_case),
        ("Delivery RR\n(Topups)", delivery_topups),
        ("Supply RR\n(Topups)", supply_topups),
    ]:
        # Panel 1: vs kWh
        plot_data.append(
            {
                "metric": metric_name,
                "value": metric_value,
                "x_var": "Total Residential kWh",
                "x_value": total_kwh,
            }
        )
        # Panel 2: vs customer count
        plot_data.append(
            {
                "metric": metric_name,
                "value": metric_value,
                "x_var": "Customer Count",
                "x_value": float(customer_count),
            }
        )

    plot_df = pl.DataFrame(plot_data)
    max_value = cast(float, plot_df["value"].max())

    # Create the plot with two panels
    p = (
        ggplot(plot_df, aes(x="x_value", y="value", color="metric"))
        + geom_point(size=6, alpha=0.7)
        + geom_text(
            aes(label="metric"),
            nudge_y=0.08 * max_value,
            size=7,
        )
        + facet_wrap("x_var", scales="free_x", ncol=2)
        + scale_color_manual(
            values={
                "Delivery RR\n(Rate Case)": "#56B4E9",
                "Delivery RR\n(Topups)": "#E69F00",
                "Supply RR\n(Topups)": "#009E73",
            }
        )
        + labs(
            x="",
            y="Revenue Requirement ($)",
            title="RIE Revenue Requirement Components",
            color="Component",
        )
        + theme_minimal()
    )

    p.save(output_path, dpi=150, width=14, height=7)
    print(f"Saved RIE components plot to {output_path}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze revenue requirements scatter plots and create additional outputs.",
    )
    parser.add_argument(
        "--path-ny-rev-req",
        type=Path,
        required=True,
        help="Path to NY config/rev_requirement/ directory.",
    )
    parser.add_argument(
        "--path-ri-rev-req",
        type=Path,
        required=True,
        help="Path to RI config/rev_requirement/ directory.",
    )
    parser.add_argument(
        "--path-ny-stats",
        type=str,
        default="s3://data.sb/eia/861/electric_utility_stats/year=2024/state=NY/data.parquet",
        help="Path to NY utility stats parquet file.",
    )
    parser.add_argument(
        "--path-ri-stats",
        type=str,
        default="s3://data.sb/eia/861/electric_utility_stats/year=2024/state=RI/data.parquet",
        help="Path to RI utility stats parquet file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write output files.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if not args.path_ny_rev_req.is_dir():
        raise ValueError(
            f"NY rev_requirement directory not found: {args.path_ny_rev_req}"
        )
    if not args.path_ri_rev_req.is_dir():
        raise ValueError(
            f"RI rev_requirement directory not found: {args.path_ri_rev_req}"
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)

    utilities = _load_all_utilities(args.path_ny_rev_req, args.path_ri_rev_req)

    if not utilities:
        raise ValueError(
            f"No standard utility YAML files found in {args.path_ny_rev_req} or {args.path_ri_rev_req}"
        )

    # Analyze ConEd divergence
    _analyze_coned_divergence(
        utilities, args.output_dir / "coned_divergence_analysis.md"
    )

    # Get customer counts
    customer_counts = _get_customer_counts(
        utilities, args.path_ny_stats, args.path_ri_stats
    )

    # Find RIE data
    rie_data = None
    for data, state in utilities:
        if str(data["utility"]) == "rie":
            rie_data = data
            break

    if rie_data is None:
        print("Warning: RIE data not found, skipping RIE component plots")
    else:
        rie_customer_count = customer_counts.get("rie", 0)
        if rie_customer_count == 0:
            print("Warning: Could not get RIE customer count, using 0")
        _plot_rie_components(
            rie_data, rie_customer_count, args.output_dir / "rie_components_scatter.png"
        )


if __name__ == "__main__":
    main()
