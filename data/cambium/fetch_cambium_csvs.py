#!/usr/bin/env python3
"""
Cambium file downloader - interactive or CLI mode.

Supports multiple release years (--year 2024, 2023, ...). Use --output to write
ZIPs to a directory that includes the year (e.g. zips/2024) so different years
don't clobber each other.

Interactive mode:
    uv run python fetch_cambium_csvs.py

CLI mode (for Justfile):
    uv run python fetch_cambium_csvs.py --year 2024 --output zips/2024 --id 69177 69178
    uv run python fetch_cambium_csvs.py --year 2024 --output zips/2024 --scenario "Mid-case" --resolution "Hourly"
    uv run python fetch_cambium_csvs.py --year 2024 --output zips/2024 --all
"""

import argparse
import subprocess
import sys
from pathlib import Path

import questionary
import requests
from rich.console import Console
from rich.table import Table

PROJECT_UUIDS = {
    "2024": "5c7bef16-7e38-4094-92ce-8b03dfa93380",
    "2023": "0f92fe57-3365-428a-8fe8-0afc326b3b43",
    "2022": "82460f06-548c-4954-b2d9-b84ba92d63e2",
    "2021": "a3e2f719-dd5a-4c3e-9bbf-f24fef563f45",
    "2020": "579698fe-5a38-4d7c-8611-d0c5969b2e54",
}

API_BASE_URL = "https://scenarioviewer.nrel.gov"
FILE_LIST_API = f"{API_BASE_URL}/api/file-list/"
DOWNLOAD_API = f"{API_BASE_URL}/api/download/"

console = Console()


def format_bytes(bytes_val):
    """Format bytes into human-readable format."""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes_val < 1024.0:
            return f"{bytes_val:.0f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.1f} TB"


def fetch_file_list(year="2024"):
    """Fetch the list of available files from the API."""
    project_uuid = PROJECT_UUIDS.get(year)
    if not project_uuid:
        console.print(
            f"[red]Invalid year: {year}. Available years: {', '.join(PROJECT_UUIDS.keys())}[/red]"
        )
        return None

    response = requests.post(
        FILE_LIST_API,
        data={"project_uuid": project_uuid},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if response.status_code == 200:
        return response.json()["files"], project_uuid
    else:
        console.print(f"[red]✗ Error fetching file list: {response.status_code}[/red]")
        return None, None


def get_s3_url(file_id, project_uuid):
    """Get the presigned S3 URL for a file."""
    try:
        response = requests.post(
            DOWNLOAD_API,
            data={"file_ids": file_id, "project_uuid": project_uuid},
            allow_redirects=False,
        )

        if response.status_code == 302:
            return response.headers.get("Location")
        return None
    except Exception as e:
        console.print(f"[red]Error getting URL for file {file_id}: {e}[/red]")
        return None


def display_files_table(files, year="2024"):
    """Display files in a rich table."""
    project_uuid = PROJECT_UUIDS[year]
    console.print(f"\n[bold cyan]Cambium {year} Datasets from NREL[/bold cyan]")
    console.print(
        "[dim]The following datasets are available for download from the National Renewable Energy Laboratory[/dim]"
    )
    console.print(
        f"[dim]Source: https://scenarioviewer.nrel.gov/?project={project_uuid}&mode=download[/dim]\n"
    )

    table = Table(
        title=f"Cambium {year} Available Files",
        show_header=True,
        header_style="bold cyan",
    )

    table.add_column("#", style="dim", width=4)
    table.add_column("Scenario", style="magenta", max_width=30)
    table.add_column("Year", width=8)
    table.add_column("Metric", style="green", max_width=20)
    table.add_column("Time\nResolution", style="yellow", width=12)
    table.add_column("Location", style="blue", max_width=18)
    table.add_column("Type", width=6)
    table.add_column("Size", justify="right", width=8)

    for i, f in enumerate(files, 1):
        table.add_row(
            str(i),
            f["scenario"],
            f["year"],
            f["metric"],
            f["time_resolution"],
            f["location_type"],
            f["file_type"],
            format_bytes(f["size"]),
        )

    console.print(table)


def download_files(
    files_to_download,
    project_uuid,
    year="2024",
    output_dir=None,
    show_cli_command=False,
):
    """Download selected files to output_dir."""
    if not files_to_download:
        console.print("[yellow]No files to download[/yellow]")
        return

    if output_dir is None:
        output_dir = Path(f"zips/{year}")
    else:
        output_dir = Path(output_dir)

    # Show CLI command to reproduce this selection
    if show_cli_command:
        console.print("\n[dim]To reproduce this download via CLI:[/dim]")

        # Try to generate a readable filter-based command
        if len(files_to_download) == 1:
            f = files_to_download[0]
            parts = [f"--year {year}", f"--output {output_dir}"]
            if f["scenario"] != "ALL":
                parts.append(f'--scenario "{f["scenario"]}"')
            if f["time_resolution"] != "ALL":
                parts.append(f'--resolution "{f["time_resolution"]}"')
            if f["location_type"] not in ["ALL", "GEA Regions 2023"]:
                parts.append(f'--location "{f["location_type"]}"')
            if f["metric"] != "ALL":
                parts.append(f'--metric "{f["metric"]}"')

            if len(parts) > 2:  # More than year + output
                cli_command = f"uv run python fetch_cambium_csvs.py {' '.join(parts)}"
            else:
                cli_command = (
                    f"uv run python fetch_cambium_csvs.py --year {year} --output"
                    f" {output_dir} --id {f['id']}"
                )
        else:
            # Multiple files - use IDs
            file_ids = [str(f["id"]) for f in files_to_download]
            cli_command = (
                f"uv run python fetch_cambium_csvs.py --year {year} --output"
                f" {output_dir} --id {' '.join(file_ids)}"
            )

        console.print(f"[cyan]{cli_command}[/cyan]\n")

    console.print(
        f"[cyan]Fetching S3 URLs for {len(files_to_download)} file(s)...[/cyan]\n"
    )

    # Get S3 URLs
    with console.status("[bold cyan]Getting presigned URLs...") as status:
        for i, file in enumerate(files_to_download, 1):
            status.update(f"[bold cyan]Getting URL {i}/{len(files_to_download)}...")
            file["s3_url"] = get_s3_url(file["id"], project_uuid)

    successful = [f for f in files_to_download if f.get("s3_url")]
    console.print(f"[green]✓ Got {len(successful)} URL(s)[/green]\n")

    # Download files
    download_dir = output_dir
    download_dir.mkdir(parents=True, exist_ok=True)

    for i, file in enumerate(successful, 1):
        filename = f"cambium{year}_{file['id']}_{file['scenario']}_{file['time_resolution']}_{file['location_type']}.zip"
        filename = filename.replace(" ", "_").replace("/", "-")
        filepath = download_dir / filename

        console.print(f"[cyan]Downloading {i}/{len(successful)}: {filename}[/cyan]")

        try:
            subprocess.run(
                ["curl", "-L", "-o", str(filepath), file["s3_url"]],
                check=True,
                capture_output=True,
            )
            console.print(
                f"[green]  ✓ Downloaded ({format_bytes(file['size'])})[/green]"
            )
        except subprocess.CalledProcessError:
            console.print("[red]  ✗ Failed to download[/red]")

    console.print(f"\n[green]✓ Files saved to: {download_dir.absolute()}[/green]\n")


def filter_files(
    files,
    scenario=None,
    year=None,
    metric=None,
    resolution=None,
    location=None,
    file_ids=None,
):
    """Filter files by criteria."""
    filtered = files

    if file_ids:
        filtered = [f for f in filtered if f["id"] in file_ids]
    if scenario:
        filtered = [f for f in filtered if scenario.lower() in f["scenario"].lower()]
    if year:
        filtered = [f for f in filtered if year.lower() in f["year"].lower()]
    if metric:
        filtered = [f for f in filtered if metric.lower() in f["metric"].lower()]
    if resolution:
        filtered = [
            f for f in filtered if resolution.lower() in f["time_resolution"].lower()
        ]
    if location:
        filtered = [
            f for f in filtered if location.lower() in f["location_type"].lower()
        ]

    return filtered


def interactive_mode(files, project_uuid, year="2024"):
    """Interactive mode - show table and let user select."""
    console.print(
        "\n[bold cyan]═══════════════════════════════════════════════════════[/bold cyan]"
    )
    console.print(f"[bold cyan]   Cambium {year} Interactive Downloader[/bold cyan]")
    console.print(
        "[bold cyan]═══════════════════════════════════════════════════════[/bold cyan]\n"
    )

    display_files_table(files, year)

    console.print(
        "\n[dim]Enter the numbers of the files you want to download (e.g., 1 3 5 or 1-5 or 1,3,5)[/dim]"
    )
    console.print("[dim]Press Enter without typing anything to cancel[/dim]\n")

    selection = questionary.text(
        "Select files:",
        validate=lambda text: True,  # Allow empty input
    ).ask()

    if not selection or selection.strip() == "":
        console.print("\n[yellow]Cancelled[/yellow]\n")
        return

    # Parse selection (supports: "1 3 5", "1-5", "1,3,5")
    selected_indices = []
    try:
        parts = selection.replace(",", " ").split()
        for part in parts:
            if "-" in part:
                start, end = part.split("-", 1)
                selected_indices.extend(range(int(start) - 1, int(end)))
            else:
                selected_indices.append(int(part) - 1)

        # Validate indices
        selected_indices = [i for i in selected_indices if 0 <= i < len(files)]

        if not selected_indices:
            console.print("\n[red]Invalid selection[/red]\n")
            return

        # Remove duplicates and sort
        selected_indices = sorted(set(selected_indices))

    except ValueError:
        console.print(
            "\n[red]Invalid input. Please enter numbers like: 1 3 5 or 1-5[/red]\n"
        )
        return

    selected_files = [files[i] for i in selected_indices]
    console.print(f"\n[green]Selected {len(selected_files)} file(s)[/green]")
    output_dir = Path(f"zips/{year}")
    download_files(
        selected_files, project_uuid, year, output_dir=output_dir, show_cli_command=True
    )


def cli_mode(files, project_uuid, year, args):
    """CLI mode - filter and download based on arguments."""
    output_dir = Path(args.output) if args.output else Path(f"zips/{year}")

    if args.all:
        console.print(f"\n[cyan]Downloading all {len(files)} files...[/cyan]")
        download_files(files, project_uuid, year, output_dir=output_dir)
        return

    # Filter files
    filtered = filter_files(
        files,
        scenario=args.scenario,
        year=args.year_filter,
        metric=args.metric,
        resolution=args.resolution,
        location=args.location,
        file_ids=args.id,
    )

    if not filtered:
        console.print("[red]No files match the specified criteria[/red]")
        sys.exit(1)

    console.print(f"\n[cyan]Found {len(filtered)} matching file(s):[/cyan]\n")
    display_files_table(filtered, year)

    download_files(filtered, project_uuid, year, output_dir=output_dir)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download Cambium files - interactive or CLI mode",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--year",
        default="2024",
        choices=list(PROJECT_UUIDS.keys()),
        help="Cambium data year (default: 2024)",
    )
    parser.add_argument(
        "--id", type=int, nargs="+", help="Download specific file ID(s)"
    )
    parser.add_argument("--scenario", help="Filter by scenario (e.g., 'Mid-case')")
    parser.add_argument(
        "--year-filter", dest="year_filter", help="Filter by year within dataset"
    )
    parser.add_argument("--metric", help="Filter by metric")
    parser.add_argument(
        "--resolution", help="Filter by time resolution (e.g., 'Hourly')"
    )
    parser.add_argument(
        "--location", help="Filter by location type (e.g., 'GEA Regions')"
    )
    parser.add_argument("--all", action="store_true", help="Download all files")
    parser.add_argument(
        "--output",
        "-o",
        metavar="DIR",
        help="Output directory for downloaded ZIP files (default: zips/<year>)",
    )

    args = parser.parse_args()

    # Determine if CLI mode or interactive mode
    is_cli_mode = any(
        [
            args.id,
            args.scenario,
            args.year_filter,
            args.metric,
            args.resolution,
            args.location,
            args.all,
        ]
    )

    # If not CLI mode and year is default, ask user to select year
    year = args.year
    if not is_cli_mode and year == "2024":
        available_years = sorted(PROJECT_UUIDS.keys(), reverse=True)
        year = questionary.select(
            "Select Cambium data year:", choices=available_years, default="2024"
        ).ask()

        if not year:
            console.print("\n[yellow]Cancelled[/yellow]\n")
            return 0

    # Fetch file list
    console.print(
        f"[cyan]Fetching file list from NREL API for Cambium {year}...[/cyan]"
    )
    result = fetch_file_list(year)
    if result is None:
        console.print("[red]Failed to fetch file list[/red]")
        return 1

    files, project_uuid = result
    console.print(f"[green]✓ Found {len(files)} files[/green]")

    # Run appropriate mode
    if is_cli_mode:
        cli_mode(files, project_uuid, year, args)
    else:
        interactive_mode(files, project_uuid, year)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        console.print("\n\n[yellow]Interrupted by user[/yellow]\n")
        sys.exit(0)
