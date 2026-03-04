"""Plot scatter plots of revenue requirements vs residential kWh for NY and RI utilities.

Reads standard per-utility YAML files (excluding *_hp_vs_nonhp, *_large_number,
delivery_*, supply_*) from NY and RI config/rev_requirement/ directories and produces
two scatter plots:
1. Delivery revenue requirement vs total residential kWh
2. Delivery and supply revenue requirement vs total residential kWh

Points are labeled by utility name and colored by state (NY vs RI).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import cast

import polars as pl
import yaml
from plotnine import (
    aes,
    geom_point,
    geom_text,
    ggplot,
    labs,
    scale_color_manual,
    theme_minimal,
)

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


def _plot_delivery_vs_kwh(
    utilities: list[tuple[UtilityData, str]], output_path: Path
) -> None:
    """Plot delivery revenue requirement vs total residential kWh."""
    rows = []
    for data, state in utilities:
        rows.append(
            {
                "utility": str(data["utility"]),
                "state": state,
                "total_residential_kwh": float(data["total_residential_kwh"]),
                "total_delivery_revenue_requirement": float(
                    data["total_delivery_revenue_requirement"]
                ),
            }
        )

    df = pl.DataFrame(rows)

    p = (
        ggplot(
            df, aes(x="total_residential_kwh", y="total_delivery_revenue_requirement")
        )
        + geom_point(aes(color="state"), size=4)
        + geom_text(
            aes(label="utility"),
            nudge_x=0,
            nudge_y=0.02 * df["total_delivery_revenue_requirement"].max(),
            size=9,
        )
        + scale_color_manual(values={"NY": "#56B4E9", "RI": "#E69F00"})
        + labs(
            x="Total Residential kWh",
            y="Total Delivery Revenue Requirement ($)",
            title="Delivery Revenue Requirement vs Residential kWh",
        )
        + theme_minimal()
    )

    p.save(output_path, dpi=150, width=10, height=7)
    print(f"Saved chart to {output_path}")


def _plot_delivery_and_supply_vs_kwh(
    utilities: list[tuple[UtilityData, str]], output_path: Path
) -> None:
    """Plot delivery and supply revenue requirement vs total residential kWh."""
    rows = []
    for data, state in utilities:
        rows.append(
            {
                "utility": str(data["utility"]),
                "state": state,
                "total_residential_kwh": float(data["total_residential_kwh"]),
                "total_delivery_and_supply_revenue_requirement": float(
                    data["total_delivery_and_supply_revenue_requirement"]
                ),
            }
        )

    df = pl.DataFrame(rows)

    p = (
        ggplot(
            df,
            aes(
                x="total_residential_kwh",
                y="total_delivery_and_supply_revenue_requirement",
            ),
        )
        + geom_point(aes(color="state"), size=4)
        + geom_text(
            aes(label="utility"),
            nudge_x=0,
            nudge_y=0.02 * df["total_delivery_and_supply_revenue_requirement"].max(),
            size=9,
        )
        + scale_color_manual(values={"NY": "#56B4E9", "RI": "#E69F00"})
        + labs(
            x="Total Residential kWh",
            y="Total Delivery and Supply Revenue Requirement ($)",
            title="Delivery and Supply Revenue Requirement vs Residential kWh",
        )
        + theme_minimal()
    )

    p.save(output_path, dpi=150, width=10, height=7)
    print(f"Saved chart to {output_path}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot scatter plots of revenue requirements vs residential kWh.",
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
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write output PNG files.",
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

    _plot_delivery_vs_kwh(utilities, args.output_dir / "rr_delivery_vs_kwh.png")
    _plot_delivery_and_supply_vs_kwh(
        utilities, args.output_dir / "rr_delivery_and_supply_vs_kwh.png"
    )


if __name__ == "__main__":
    main()
