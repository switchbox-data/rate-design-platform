"""Copy tariff JSONs to variant names while retagging the primary item."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _suffix_pairs(
    source_pattern: str,
    dest_pattern: str,
) -> tuple[tuple[str, str], tuple[str, str]]:
    return (
        (f"_{source_pattern}.json", f"_{dest_pattern}.json"),
        (f"_{source_pattern}_supply.json", f"_{dest_pattern}_supply.json"),
    )


def _load_tariff_payload(path_tariff: Path) -> dict[str, Any]:
    payload = json.loads(path_tariff.read_text(encoding="utf-8"))
    items = payload.get("items")
    if not items:
        raise ValueError(f"{path_tariff}: missing or empty 'items'")
    if not isinstance(items[0], dict):
        raise ValueError(f"{path_tariff}: items[0] must be a JSON object")
    return payload


def _derive_variant_name(
    source_name: str,
    source_pattern: str,
    dest_pattern: str,
) -> str:
    for source_suffix, dest_suffix in (
        (f"_{source_pattern}_supply", f"_{dest_pattern}_supply"),
        (f"_{source_pattern}", f"_{dest_pattern}"),
    ):
        if source_name.endswith(source_suffix):
            return source_name.removesuffix(source_suffix) + dest_suffix
    raise ValueError(
        f"Unexpected tariff label/name: {source_name} "
        f"(source_pattern={source_pattern}, dest_pattern={dest_pattern})"
    )


def _copy_variant(
    path_source: Path,
    path_dest: Path,
    source_pattern: str,
    dest_pattern: str,
) -> None:
    payload = _load_tariff_payload(path_source)
    item = payload["items"][0]

    for field in ("label", "name"):
        value = item.get(field)
        if value is None:
            continue
        if not isinstance(value, str):
            raise ValueError(f"{path_source}: items[0].{field} must be a string")
        item[field] = _derive_variant_name(value, source_pattern, dest_pattern)

    path_dest.parent.mkdir(parents=True, exist_ok=True)
    path_dest.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Created {path_dest}")


def _discover_utilities(path_output_dir: Path, source_pattern: str) -> list[str]:
    suffix = f"_{source_pattern}.json"
    candidates = {
        path_tariff.name.removesuffix(suffix)
        for path_tariff in path_output_dir.glob(f"*{suffix}")
        if (path_output_dir / f"{path_tariff.stem}_supply.json").exists()
    }
    return sorted(
        candidate
        for candidate in candidates
        if not any(
            candidate.startswith(f"{other}_")
            for other in candidates
            if other != candidate
        )
    )


def _copy_utility_tariffs(
    path_output_dir: Path,
    utility: str,
    source_pattern: str,
    dest_pattern: str,
) -> None:
    for source_suffix, dest_suffix in _suffix_pairs(source_pattern, dest_pattern):
        path_source = path_output_dir / f"{utility}{source_suffix}"
        if not path_source.exists():
            raise FileNotFoundError(
                "Source not found: "
                f"{path_source} (run create-{source_pattern}-tariffs first)"
            )
        _copy_variant(
            path_source,
            path_output_dir / f"{utility}{dest_suffix}",
            source_pattern,
            dest_pattern,
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy tariff JSONs to variant names with updated labels"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Electric tariffs directory (e.g. config/tariffs/electric)",
    )
    parser.add_argument(
        "--source-pattern",
        default="flat",
        help="Tariff suffix pattern to copy from (default: 'flat'). "
        "E.g. 'default' reads *_default.json and *_default_supply.json",
    )
    parser.add_argument(
        "--dest-pattern",
        default="",
        help="Destination tariff suffix pattern. Defaults to nonhp_<source-pattern>. "
        "E.g. --source-pattern default --dest-pattern non_electric_heating "
        "creates *_non_electric_heating.json from *_default.json",
    )
    utility_scope = parser.add_mutually_exclusive_group(required=True)
    utility_scope.add_argument("--utility", help="Utility identifier (e.g. rie).")
    utility_scope.add_argument(
        "--all",
        action="store_true",
        help="Create variants for every utility that has matching source tariffs.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    path_output_dir = args.output_dir.resolve()
    source_pattern = args.source_pattern
    dest_pattern = args.dest_pattern or f"nonhp_{source_pattern}"
    if not path_output_dir.is_dir():
        raise FileNotFoundError(f"Output directory not found: {path_output_dir}")

    if args.all:
        utilities = _discover_utilities(path_output_dir, source_pattern)
        if not utilities:
            raise FileNotFoundError(
                "No paired "
                f"*_{source_pattern}.json / *_{source_pattern}_supply.json files found in "
                f"{path_output_dir} (run create-{source_pattern}-tariffs first)"
            )
        for utility in utilities:
            _copy_utility_tariffs(
                path_output_dir, utility, source_pattern, dest_pattern
            )
        return

    _copy_utility_tariffs(path_output_dir, args.utility, source_pattern, dest_pattern)


if __name__ == "__main__":
    main()
