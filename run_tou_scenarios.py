"""
TOU Parameter Scenario Analysis Script

This script runs multiple TOU parameter configurations and saves results to organized output folders.
It explores different peak rates, switching costs, and peak hour configurations to analyze
their impact on consumer behavior and cost savings.
"""

import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from rate_design_platform.plotting import plot_all_comparisons
from rate_design_platform.second_pass import (
    TOUParameters,
    run_full_simulation,
    save_monthly_results,
)


def create_scenario_configurations() -> dict[str, TOUParameters]:
    """
    Create different TOU parameter scenarios to explore.

    Returns:
        Dictionary mapping scenario names to TOUParameters
    """
    scenarios = {}

    # Baseline scenario (current parameters)
    scenarios["baseline"] = TOUParameters(
        r_on=0.48, r_off=0.12, c_switch=3.0, alpha=0.15, peak_start_hour=12, peak_end_hour=20
    )

    # Peak rate scenarios based on off-peak multipliers
    off_peak_rate = 0.12
    scenarios["peak_2x_off"] = TOUParameters(
        r_on=2 * off_peak_rate,  # 0.24
        r_off=off_peak_rate,
        c_switch=3.0,
        alpha=0.15,
        peak_start_hour=12,
        peak_end_hour=20,
    )

    scenarios["peak_6x_off"] = TOUParameters(
        r_on=6 * off_peak_rate,  # 0.72
        r_off=off_peak_rate,
        c_switch=3.0,
        alpha=0.15,
        peak_start_hour=12,
        peak_end_hour=20,
    )

    # Higher switching cost scenarios
    for cost in [10.0, 20.0, 30.0]:
        scenarios[f"higher_switch_{cost:.0f}"] = TOUParameters(
            r_on=0.48, r_off=0.12, c_switch=cost, alpha=0.15, peak_start_hour=12, peak_end_hour=20
        )

    # Different peak hour scenarios
    scenarios["shortened_peaks"] = TOUParameters(
        r_on=0.48, r_off=0.12, c_switch=3.0, alpha=0.15, peak_start_hour=14, peak_end_hour=18
    )

    scenarios["extended_peaks"] = TOUParameters(
        r_on=0.48, r_off=0.12, c_switch=3.0, alpha=0.15, peak_start_hour=10, peak_end_hour=22
    )

    return scenarios


def setup_house_args() -> dict:
    """
    Set up house arguments for simulation.

    Returns:
        Dictionary of house arguments
    """
    # Input/Output file paths
    bldg_id = 72
    upgrade_id = 3
    weather_station = "G3400270"
    name = f"bldg{bldg_id:07d}-up{upgrade_id:02d}"

    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    input_path = os.path.join(base_path, "rate_design_platform", "inputs")
    xml_path = os.path.join(input_path, f"{name}.xml")
    weather_path = os.path.join(input_path, f"{weather_station}.epw")
    schedule_path = os.path.join(input_path, f"{name}_schedule.csv")

    # Check that files exist
    for file_path in [xml_path, weather_path, schedule_path]:
        if not Path(file_path).exists():
            msg = f"Required input file not found: {file_path}"
            raise FileNotFoundError(msg)

    # Simulation parameters
    year = 2018
    month = 1
    start_date = 1
    start_time = datetime(year, month, start_date, 0, 0)
    duration = timedelta(days=365)
    time_step = timedelta(minutes=15)
    end_time = start_time + duration
    initialization_time = timedelta(days=1)

    return {
        "name": name,
        "start_time": start_time,
        "end_time": end_time,
        "time_res": time_step,
        "duration": duration,
        "initialization_time": initialization_time,
        "save_results": True,
        "verbosity": 9,
        "metrics_verbosity": 7,
        "hpxml_file": xml_path,
        "hpxml_schedule_file": schedule_path,
        "weather_file": weather_path,
    }


def run_scenario(
    scenario_name: str, tou_params: TOUParameters, house_args: dict, sims_output_path: str
) -> tuple[list, dict]:
    """
    Run a single scenario and save results.

    Args:
        scenario_name: Name of the scenario
        tou_params: TOU parameters for this scenario
        house_args: House arguments
        sims_output_path: Base output path for simulations

    Returns:
        Tuple of (monthly_results, annual_metrics)
    """
    print(f"\n=== Running Scenario: {scenario_name} ===")

    # Create scenario-specific output directory
    scenario_output_path = os.path.join(sims_output_path, scenario_name)
    os.makedirs(scenario_output_path, exist_ok=True)

    # Update house args with scenario-specific output path
    scenario_house_args = house_args.copy()
    scenario_house_args["output_path"] = scenario_output_path
    scenario_house_args["name"] = house_args["name"] + f"_{scenario_name}"

    # Run the simulation
    monthly_results, annual_metrics = run_full_simulation(tou_params, scenario_house_args)

    # Save monthly results
    monthly_results_file = os.path.join(scenario_output_path, f"{scenario_name}_monthly_results.csv")
    save_monthly_results(monthly_results, monthly_results_file)

    # Save annual metrics
    annual_metrics_file = os.path.join(scenario_output_path, f"{scenario_name}_annual_metrics.csv")
    annual_df = pd.DataFrame([annual_metrics])
    annual_df.to_csv(annual_metrics_file, index=False)

    # Save TOU parameters for reference
    tou_params_file = os.path.join(scenario_output_path, f"{scenario_name}_tou_params.csv")
    tou_df = pd.DataFrame([
        {
            "r_on": tou_params.r_on,
            "r_off": tou_params.r_off,
            "c_switch": tou_params.c_switch,
            "alpha": tou_params.alpha,
            "peak_start_hour": tou_params.peak_start_hour,
            "peak_end_hour": tou_params.peak_end_hour,
        }
    ])
    tou_df.to_csv(tou_params_file, index=False)

    # Generate plots for this scenario
    try:
        building_id = scenario_house_args["name"]
        power_fig, temp_fig, bills_fig = plot_all_comparisons(building_id, Path(scenario_output_path), days=7)

        # Save plots
        plots_dir = os.path.join(scenario_output_path, "plots")
        os.makedirs(plots_dir, exist_ok=True)

        power_fig.savefig(
            os.path.join(plots_dir, f"{scenario_name}_water_heating_comparison.png"), dpi=300, bbox_inches="tight"
        )
        temp_fig.savefig(
            os.path.join(plots_dir, f"{scenario_name}_temperature_comparison.png"), dpi=300, bbox_inches="tight"
        )
        bills_fig.savefig(os.path.join(plots_dir, f"{scenario_name}_monthly_bills.png"), dpi=300, bbox_inches="tight")

        plt.close(power_fig)
        plt.close(temp_fig)
        plt.close(bills_fig)

    except Exception as e:
        print(f"Warning: Could not generate plots for {scenario_name}: {e}")

    print(f"Scenario {scenario_name} completed. Results saved to {scenario_output_path}")

    return monthly_results, annual_metrics


def create_summary_analysis(scenarios: dict[str, TOUParameters], sims_output_path: str) -> None:
    """
    Create summary analysis comparing all scenarios.

    Args:
        scenarios: Dictionary of scenario configurations
        sims_output_path: Base output path for simulations
    """
    print("\n=== Creating Summary Analysis ===")

    summary_dir = os.path.join(sims_output_path, "summary")
    os.makedirs(summary_dir, exist_ok=True)

    # Collect all annual metrics
    all_annual_metrics = []
    all_monthly_results = []

    for scenario_name in scenarios:
        scenario_output_path = os.path.join(sims_output_path, scenario_name)

        # Load annual metrics
        annual_metrics_file = os.path.join(scenario_output_path, f"{scenario_name}_annual_metrics.csv")
        if os.path.exists(annual_metrics_file):
            annual_df = pd.read_csv(annual_metrics_file)
            annual_df["scenario"] = scenario_name
            all_annual_metrics.append(annual_df)

        # Load monthly results
        monthly_results_file = os.path.join(scenario_output_path, f"{scenario_name}_monthly_results.csv")
        if os.path.exists(monthly_results_file):
            monthly_df = pd.read_csv(monthly_results_file)
            monthly_df["scenario"] = scenario_name
            all_monthly_results.append(monthly_df)

    # Combine all results
    if all_annual_metrics:
        combined_annual = pd.concat(all_annual_metrics, ignore_index=True)
        combined_annual.to_csv(os.path.join(summary_dir, "all_scenarios_annual_metrics.csv"), index=False)

    if all_monthly_results:
        combined_monthly = pd.concat(all_monthly_results, ignore_index=True)
        combined_monthly.to_csv(os.path.join(summary_dir, "all_scenarios_monthly_results.csv"), index=False)

    # Create summary comparison plots
    create_summary_plots(combined_annual, combined_monthly, summary_dir)


def create_summary_plots(annual_df: pd.DataFrame, monthly_df: pd.DataFrame, summary_dir: str) -> None:
    """
    Create summary comparison plots across all scenarios.

    Args:
        annual_df: Combined annual metrics DataFrame
        monthly_df: Combined monthly results DataFrame
        summary_dir: Directory to save summary plots
    """
    plots_dir = os.path.join(summary_dir, "plots")
    os.makedirs(plots_dir, exist_ok=True)

    # 1. Annual metrics comparison
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle("Annual Metrics Comparison Across Scenarios", fontsize=16)

    # TOU adoption rate
    axes[0, 0].bar(annual_df["scenario"], annual_df["tou_adoption_rate_percent"])
    axes[0, 0].set_title("TOU Adoption Rate (%)")
    axes[0, 0].tick_params(axis="x", rotation=45)

    # Net annual benefit
    axes[0, 1].bar(annual_df["scenario"], annual_df["net_annual_benefit"])
    axes[0, 1].set_title("Net Annual Benefit ($)")
    axes[0, 1].tick_params(axis="x", rotation=45)

    # Total annual bills
    axes[1, 0].bar(annual_df["scenario"], annual_df["total_annual_bills"])
    axes[1, 0].set_title("Total Annual Bills ($)")
    axes[1, 0].tick_params(axis="x", rotation=45)

    # Annual switches
    axes[1, 1].bar(annual_df["scenario"], annual_df["annual_switches"])
    axes[1, 1].set_title("Annual Switches")
    axes[1, 1].tick_params(axis="x", rotation=45)

    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "annual_metrics_comparison.png"), dpi=300, bbox_inches="tight")
    plt.close()

    # 2. Monthly bill comparison for select scenarios
    fig, ax = plt.subplots(figsize=(15, 8))

    # Select key scenarios to plot
    key_scenarios = ["baseline", "peak_2x_off", "peak_6x_off", "higher_switch_30", "morning_peaks"]

    for scenario in key_scenarios:
        if scenario in monthly_df["scenario"].values:
            scenario_data = monthly_df[monthly_df["scenario"] == scenario]
            month_labels = [f"{row['year']}-{row['month']:02d}" for _, row in scenario_data.iterrows()]
            ax.plot(range(len(scenario_data)), scenario_data["bill"], "o-", label=scenario, linewidth=2, markersize=6)

    ax.set_xlabel("Month")
    ax.set_ylabel("Monthly Bill ($)")
    ax.set_title("Monthly Bill Comparison - Key Scenarios")
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Set x-axis labels
    if len(month_labels) > 0:
        ax.set_xticks(range(0, len(month_labels), 2))  # Show every 2nd month
        ax.set_xticklabels([month_labels[i] for i in range(0, len(month_labels), 2)], rotation=45, ha="right")

    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "monthly_bills_comparison.png"), dpi=300, bbox_inches="tight")
    plt.close()


def main():
    """
    Main function to run all TOU parameter scenarios.
    """
    print("=== TOU Parameter Scenario Analysis ===")

    # Set up output directory
    base_path = os.path.abspath(os.path.dirname(__file__))
    sims_output_path = os.path.join(base_path, "sims")

    # Clean and create output directory
    if os.path.exists(sims_output_path):
        shutil.rmtree(sims_output_path)
    os.makedirs(sims_output_path)

    # Set up house arguments
    house_args = setup_house_args()

    # Create scenario configurations
    scenarios = create_scenario_configurations()

    print(f"Created {len(scenarios)} scenarios:")
    for name in scenarios:
        print(f"  - {name}")

    # Run all scenarios
    all_results = {}
    for scenario_name, tou_params in scenarios.items():
        try:
            monthly_results, annual_metrics = run_scenario(scenario_name, tou_params, house_args, sims_output_path)
            all_results[scenario_name] = {"monthly_results": monthly_results, "annual_metrics": annual_metrics}
        except Exception as e:
            print(f"Error running scenario {scenario_name}: {e}")
            continue

    # Create summary analysis
    create_summary_analysis(scenarios, sims_output_path)

    # Create README
    readme_content = f"""# TOU Parameter Scenario Analysis Results

Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Scenarios Analyzed

{len(scenarios)} scenarios were analyzed with different TOU parameter configurations:

"""

    for scenario_name, tou_params in scenarios.items():
        readme_content += f"### {scenario_name}\n"
        readme_content += f"- Peak Rate: ${tou_params.r_on:.2f}/kWh\n"
        readme_content += f"- Off-Peak Rate: ${tou_params.r_off:.2f}/kWh\n"
        readme_content += f"- Switching Cost: ${tou_params.c_switch:.1f}\n"
        readme_content += f"- Peak Hours: {tou_params.peak_start_hour}:00 - {tou_params.peak_end_hour}:00\n\n"

    readme_content += """
## Output Structure

- Each scenario has its own folder with:
  - Monthly results CSV
  - Annual metrics CSV
  - TOU parameters CSV
  - Individual plots (if generated successfully)
- `summary/` folder contains:
  - Combined results across all scenarios
  - Comparison plots

## Key Files

- `summary/all_scenarios_annual_metrics.csv`: Annual metrics for all scenarios
- `summary/all_scenarios_monthly_results.csv`: Monthly results for all scenarios
- `summary/plots/`: Comparison plots across scenarios
"""

    with open(os.path.join(sims_output_path, "README.md"), "w") as f:
        f.write(readme_content)

    print("\n=== Analysis Complete ===")
    print(f"Results saved to: {sims_output_path}")
    print(f"Successfully completed {len(all_results)} out of {len(scenarios)} scenarios")

    # Print summary of key metrics
    if all_results:
        print("\n=== Key Results Summary ===")
        for scenario_name, results in all_results.items():
            metrics = results["annual_metrics"]
            print(f"\n{scenario_name}:")
            print(f"  TOU Adoption Rate: {metrics['tou_adoption_rate_percent']:.1f}%")
            print(f"  Net Annual Benefit: ${metrics['net_annual_benefit']:.2f}")
            print(f"  Annual Switches: {metrics['annual_switches']}")


if __name__ == "__main__":
    main()
