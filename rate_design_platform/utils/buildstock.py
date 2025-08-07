from pathlib import Path

from buildstock_fetch.main import fetch_bldg_data, fetch_bldg_ids  # type: ignore[import-untyped]


def download_bldg_files(
    product: str,
    release_year: str,
    weather_file: str,
    release_version: str,
    upgrade_id: str,
    state: str,
    num_bldgs: int,
    file_type: tuple[str, ...],
    output_dir: Path,
) -> tuple[list[Path], list[str]]:
    bldg_ids = fetch_bldg_ids(
        product=product,
        release_year=release_year,
        weather_file=weather_file,
        release_version=release_version,
        upgrade_id=upgrade_id,
        state=state,
    )
    downloaded_paths, failed_downloads = fetch_bldg_data(bldg_ids[:num_bldgs], file_type, output_dir)
    return downloaded_paths, failed_downloads


if __name__ == "__main__":
    downloaded_paths, failed_downloads = download_bldg_files(
        product="resstock",
        release_year="2024",
        weather_file="amy2018",
        release_version="2",
        upgrade_id="0",
        state="NY",
        num_bldgs=10,
        file_type=("hpxml", "schedule", "metadata"),
        output_dir=Path("data"),
    )
    print(downloaded_paths)
