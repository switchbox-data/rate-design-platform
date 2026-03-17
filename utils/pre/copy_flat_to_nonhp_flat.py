"""Copy default flat tariffs to nonhp_flat and nonhp_flat_supply variants.

Used before subclass precalc runs so CAIRO calibrates a dedicated non-HP flat
tariff instead of overwriting the system-wide flat tariff outputs.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

_SOURCE_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("_flat.json", "_nonhp_flat.json"),
    ("_flat_supply.json", "_nonhp_flat_supply.json"),
)


def _load_tariff_payload(path_tariff: Path) -> dict[str, Any]:
    """Load a tariff JSON payload and validate the primary item exists."""
    payload = json.loads(path_tariff.read_text(encoding="utf-8"))
    items = payload.get("items")
    if not items:
        raise ValueError(f"{path_tariff}: missing or empty 'items'")
    if not isinstance(items[0], dict):
        raise ValueError(f"{path_tariff}: items[0] must be a JSON object")
    return payload


def _derive_nonhp_name(source_name: str) -> str:
    """Convert ``*_flat`` labels/names to their non-HP equivalents."""
    if source_name.endswith("_flat_supply"):
        return source_name.removesuffix("_flat_supply") + "_nonhp_flat_supply"
    if source_name.endswith("_flat"):
        return source_name.removesuffix("_flat") + "_nonhp_flat"
    raise ValueError(f"Unexpected flat tariff label/name: {source_name}")


def _copy_with_nonhp_label(path_source: Path, path_dest: Path) -> None:
    """Copy a flat tariff and retag its primary item for the non-HP subclass."""
    payload = _load_tariff_payload(path_source)
    item = payload["items"][0]

    label = item.get("label")
    if not isinstance(label, str):
        raise ValueError(f"{path_source}: items[0].label must be a string")
    item["label"] = _derive_nonhp_name(label)

    if "name" in item:
        name = item.get("name")
        if not isinstance(name, str):
            raise ValueError(f"{path_source}: items[0].name must be a string")
        item["name"] = _derive_nonhp_name(name)

    path_dest.parent.mkdir(parents=True, exist_ok=True)
    path_dest.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Created {path_dest}")


def _discover_utilities(path_output_dir: Path) -> list[str]:
    """Discover utilities that have paired flat and flat_supply source tariffs."""
    utilities = {
        path_tariff.name.removesuffix("_flat.json")
        for path_tariff in path_output_dir.glob("*_flat.json")
        if not path_tariff.name.endswith("_nonhp_flat.json")
        and (path_output_dir / f"{path_tariff.stem}_supply.json").exists()
    }
    return sorted(utilities)


def _copy_utility_tariffs(path_output_dir: Path, utility: str) -> None:
    """Create non-HP delivery and delivery+supply flat tariffs for one utility."""
    for source_suffix, dest_suffix in _SOURCE_SUFFIXES:
        path_source = path_output_dir / f"{utility}{source_suffix}"
        if not path_source.exists():
            raise FileNotFoundError(
                f"Source not found: {path_source} (run create-flat-tariffs first)"
            )
        _copy_with_nonhp_label(path_source, path_output_dir / f"{utility}{dest_suffix}")


def _parse_args() -> argparse.Namespace:
    """Parse CLI args for copying flat tariffs to non-HP tariff variants."""
    parser = argparse.ArgumentParser(
        description="Copy default flat tariffs to nonhp_flat / nonhp_flat_supply with updated labels"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Electric tariffs directory (e.g. config/tariffs/electric)",
    )
    utility_scope = parser.add_mutually_exclusive_group(required=True)
    utility_scope.add_argument("--utility", help="Utility identifier (e.g. rie).")
    utility_scope.add_argument(
        "--all",
        action="store_true",
        help="Create nonhp_flat / nonhp_flat_supply for every utility that has flat source tariffs in output-dir.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    path_output_dir = args.output_dir.resolve()
    if not path_output_dir.is_dir():
        raise FileNotFoundError(f"Output directory not found: {path_output_dir}")

    if args.all:
        utilities = _discover_utilities(path_output_dir)
        if not utilities:
            raise FileNotFoundError(
                "No paired *_flat.json / *_flat_supply.json files found in "
                f"{path_output_dir} (run create-flat-tariffs first)"
            )
        for utility in utilities:
            _copy_utility_tariffs(path_output_dir, utility)
        return

    _copy_utility_tariffs(path_output_dir, args.utility)


if __name__ == "__main__":
    main()
