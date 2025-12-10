#!/usr/bin/env python
"""Generate NY heat pump adoption scenarios with cumulative adoption."""

from pathlib import Path

from utils.mixed_adoption_trajectory import (
    build_adoption_trajectory,
    fetch_baseline_sample,
)

# Base data directory for NY HP rates (git-ignored raw/processed, configs versioned)
BASE_DATA_DIR = Path("rate_design/ny/hp_rates/data")

# Configuration
CONFIG = {
    # ResStock release parameters
    "release_year": "2024",
    "weather_file": "tmy3",
    "release_version": "2",
    "state": "NY",
    # Heat pump upgrade ID (adjust based on your ResStock release)
    "hp_upgrade_id": "1",
    # Download settings
    "output_dir": BASE_DATA_DIR / "buildstock_raw",
    "max_workers": 5,
    # Sampling settings
    "sample_size": 1000,  # Number of buildings to sample
    "sample_seed": 123,  # Seed for sampling reproducibility (determines building ordering)
    # Adoption scenario settings
    "adoption_fractions": [0.1, 0.2, 0.3, 0.5, 0.8, 1.0],
    # Output settings
    "processed_dir": BASE_DATA_DIR / "buildstock_processed",
}


def main():
    """Run the complete workflow to generate adoption scenarios."""
    print("=" * 80)
    print("NY Heat Pump Cumulative Adoption Scenario Generator")
    print("=" * 80)
    print("\nConfiguration:")
    for key, value in CONFIG.items():
        print(f"  {key}: {value}")
    print("\n")

    # Step 1: Fetch baseline sample and establish building ID ordering
    print("\n" + "=" * 80)
    print("STEP 1: Fetching baseline sample")
    print("=" * 80)
    print(f"Fetching {CONFIG['sample_size']} baseline buildings (seed={CONFIG['sample_seed']})")

    baseline_metadata_path, building_ids = fetch_baseline_sample(
        sample_size=CONFIG["sample_size"],
        random_seed=CONFIG["sample_seed"],
        release_year=CONFIG["release_year"],
        weather_file=CONFIG["weather_file"],
        release_version=CONFIG["release_version"],
        state=CONFIG["state"],
        output_dir=CONFIG["output_dir"],
        max_workers=CONFIG["max_workers"],
    )

    print(f"\n✓ Fetched {len(building_ids)} baseline buildings")
    print(f"✓ Baseline metadata: {baseline_metadata_path}")
    print(f"✓ Building ID ordering established (deterministic from seed)")

    # Step 2: Build adoption trajectory
    print("\n" + "=" * 80)
    print("STEP 2: Building adoption trajectory")
    print("=" * 80)
    print(f"Creating scenarios for adoption fractions: {CONFIG['adoption_fractions']}")
    print("Note: Upgrade data will be fetched incrementally for each fraction")

    scenario_paths = build_adoption_trajectory(
        baseline_metadata_path=baseline_metadata_path,
        baseline_building_ids=building_ids,
        adoption_fractions=CONFIG["adoption_fractions"],
        upgrade_id=CONFIG["hp_upgrade_id"],
        release_year=CONFIG["release_year"],
        weather_file=CONFIG["weather_file"],
        release_version=CONFIG["release_version"],
        state=CONFIG["state"],
        output_dir=CONFIG["output_dir"],
        max_workers=CONFIG["max_workers"],
        output_processed_dir=CONFIG["processed_dir"],
    )

    # Summary
    print("\n" + "=" * 80)
    print("COMPLETE - Scenario Summary")
    print("=" * 80)
    print(f"\nGenerated {len(scenario_paths)} adoption scenarios:")
    for fraction, path in sorted(scenario_paths.items()):
        n_adopters = int(round(fraction * len(building_ids)))
        print(f"  {fraction*100:3.0f}% adoption ({n_adopters:4d} buildings) → {path.name}")


if __name__ == "__main__":
    main()
