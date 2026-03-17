"""Copy default flat tariffs to nonhp_flat and nonhp_flat_supply with updated labels.

Used before run 5 (and run 6) so precalc has a dedicated non-HP flat tariff to
calibrate, producing nonhp_flat_calibrated and nonhp_flat_supply_calibrated.
Source files are the pre-calibrated flat tariffs from create-flat-tariffs
(utility_flat.json and utility_flat_supply.json).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def _copy_with_label(source_path: Path, dest_path: Path, new_label: str) -> None:
    """Read tariff JSON, set items[0].label and optional name, write to dest."""
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    items = payload.get("items")
    if not items:
        raise ValueError(f"{source_path}: missing or empty 'items'")
    items[0]["label"] = new_label
    if "name" in items[0]:
        items[0]["name"] = new_label
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Created {dest_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy default flat tariffs to nonhp_flat / nonhp_flat_supply with updated labels"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Electric tariffs directory (e.g. config/tariffs/electric)",
    )
    parser.add_argument(
        "--utility",
        help="Utility identifier (e.g. rie). Omit if using --all.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Create nonhp_flat / nonhp_flat_supply for every utility that has *_flat.json in output-dir.",
    )
    args = parser.parse_args()
    out = args.output_dir.resolve()
    if not out.is_dir():
        raise FileNotFoundError(f"Output directory not found: {out}")

    if args.all:
        # Discover all {utility}_flat.json (exclude *_flat_supply, *_nonhp_flat, etc.).
        flat_files = sorted(out.glob("*_flat.json"))
        utilities = []
        for p in flat_files:
            stem = p.stem
            # Only {utility}_flat (e.g. rie_flat, coned_flat), not rie_nonhp_flat or rie_flat_supply.
            if stem.endswith("_flat") and "nonhp" not in stem and stem != "flat":
                utilities.append(stem.removesuffix("_flat"))
        if not utilities:
            raise FileNotFoundError(
                f"No *_flat.json (e.g. rie_flat.json) found in {out} (run create-flat-tariffs first)"
            )
        for util in utilities:
            _process_one(out, util)
        return

    util = args.utility
    if not util:
        raise ValueError("Either --utility or --all is required")
    _process_one(out, util)


def _process_one(out: Path, util: str) -> None:
    """Create nonhp_flat and nonhp_flat_supply for one utility."""
    flat_src = out / f"{util}_flat.json"
    flat_supply_src = out / f"{util}_flat_supply.json"
    if not flat_src.exists():
        raise FileNotFoundError(
            f"Source not found: {flat_src} (run create-flat-tariffs first)"
        )
    if not flat_supply_src.exists():
        raise FileNotFoundError(
            f"Source not found: {flat_supply_src} (run create-flat-tariffs first)"
        )
    _copy_with_label(flat_src, out / f"{util}_nonhp_flat.json", f"{util}_nonhp_flat")
    _copy_with_label(
        flat_supply_src,
        out / f"{util}_nonhp_flat_supply.json",
        f"{util}_nonhp_flat_supply",
    )


if __name__ == "__main__":
    main()
