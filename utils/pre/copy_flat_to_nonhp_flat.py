"""Copy tariffs to non-HP variants (e.g. flat -> nonhp_flat, default -> nonhp_default).

Used before subclass precalc runs so CAIRO calibrates a dedicated non-HP
tariff instead of overwriting the system-wide tariff outputs.  The --pattern
arg controls which suffix pair to copy (default: 'flat').
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


def _suffixes_for_pattern(pattern: str) -> tuple[tuple[str, str], ...]:
    """Return (source_suffix, dest_suffix) pairs for a given tariff pattern."""
    return (
        (f"_{pattern}.json", f"_nonhp_{pattern}.json"),
        (f"_{pattern}_supply.json", f"_nonhp_{pattern}_supply.json"),
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


def _derive_nonhp_name(source_name: str, pattern: str = "flat") -> str:
    """Convert ``*_{pattern}`` labels/names to their non-HP equivalents."""
    supply_suffix = f"_{pattern}_supply"
    base_suffix = f"_{pattern}"
    if source_name.endswith(supply_suffix):
        return source_name.removesuffix(supply_suffix) + f"_nonhp_{pattern}_supply"
    if source_name.endswith(base_suffix):
        return source_name.removesuffix(base_suffix) + f"_nonhp_{pattern}"
    raise ValueError(f"Unexpected tariff label/name: {source_name} (pattern={pattern})")


def _copy_with_nonhp_label(
    path_source: Path, path_dest: Path, pattern: str = "flat"
) -> None:
    """Copy a tariff and retag its primary item for the non-HP subclass."""
    payload = _load_tariff_payload(path_source)
    item = payload["items"][0]

    label = item.get("label")
    if not isinstance(label, str):
        raise ValueError(f"{path_source}: items[0].label must be a string")
    item["label"] = _derive_nonhp_name(label, pattern)

    if "name" in item:
        name = item.get("name")
        if not isinstance(name, str):
            raise ValueError(f"{path_source}: items[0].name must be a string")
        item["name"] = _derive_nonhp_name(name, pattern)

    path_dest.parent.mkdir(parents=True, exist_ok=True)
    path_dest.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Created {path_dest}")


def _discover_utilities(path_output_dir: Path, pattern: str = "flat") -> list[str]:
    """Discover utilities that have paired source tariffs for the given pattern."""
    suffix = f"_{pattern}.json"
    nonhp_suffix = f"_nonhp_{pattern}.json"
    utilities = {
        path_tariff.name.removesuffix(suffix)
        for path_tariff in path_output_dir.glob(f"*{suffix}")
        if not path_tariff.name.endswith(nonhp_suffix)
        and (path_output_dir / f"{path_tariff.stem}_supply.json").exists()
    }
    return sorted(utilities)


def _copy_utility_tariffs(
    path_output_dir: Path, utility: str, pattern: str = "flat"
) -> None:
    """Create non-HP delivery and delivery+supply tariffs for one utility."""
    suffixes = _suffixes_for_pattern(pattern)
    for source_suffix, dest_suffix in suffixes:
        path_source = path_output_dir / f"{utility}{source_suffix}"
        if not path_source.exists():
            raise FileNotFoundError(
                f"Source not found: {path_source} (run create-{pattern}-tariffs first)"
            )
        _copy_with_nonhp_label(
            path_source, path_output_dir / f"{utility}{dest_suffix}", pattern
        )


def _parse_args() -> argparse.Namespace:
    """Parse CLI args for copying tariffs to non-HP variants."""
    parser = argparse.ArgumentParser(
        description="Copy tariffs to nonhp variants with updated labels"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Electric tariffs directory (e.g. config/tariffs/electric)",
    )
    parser.add_argument(
        "--pattern",
        default="flat",
        help="Tariff suffix pattern to copy (default: 'flat'). "
        "E.g. 'default' copies *_default.json -> *_nonhp_default.json",
    )
    utility_scope = parser.add_mutually_exclusive_group(required=True)
    utility_scope.add_argument("--utility", help="Utility identifier (e.g. rie).")
    utility_scope.add_argument(
        "--all",
        action="store_true",
        help="Create nonhp variants for every utility that has source tariffs matching --pattern in output-dir.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    path_output_dir = args.output_dir.resolve()
    pattern = args.pattern
    if not path_output_dir.is_dir():
        raise FileNotFoundError(f"Output directory not found: {path_output_dir}")

    if args.all:
        utilities = _discover_utilities(path_output_dir, pattern)
        if not utilities:
            raise FileNotFoundError(
                f"No paired *_{pattern}.json / *_{pattern}_supply.json files found in "
                f"{path_output_dir} (run create-{pattern}-tariffs first)"
            )
        for utility in utilities:
            _copy_utility_tariffs(path_output_dir, utility, pattern)
        return

    _copy_utility_tariffs(path_output_dir, args.utility, pattern)


if __name__ == "__main__":
    main()
