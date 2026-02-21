"""Create flat-rate URDB v7 tariff JSON files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from utils.pre.create_tariff import (
    create_default_flat_tariff,
    load_tariff_json,
    write_tariff_json,
)


def _write_json(path: Path, payload: dict) -> str:
    written = write_tariff_json(payload, path)
    return str(written)


def _derive_supply_default_path(default_tariff_path: Path) -> Path:
    return default_tariff_path.with_name(f"{default_tariff_path.stem}_supply.json")


def _load_template_item(tariff_path: Path) -> dict[str, Any]:
    payload = load_tariff_json(tariff_path)
    if not isinstance(payload, dict):
        raise ValueError(f"Tariff at {tariff_path} must be a JSON object.")
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError(f"Tariff at {tariff_path} must contain non-empty 'items'.")
    item = items[0]
    if not isinstance(item, dict):
        raise ValueError(f"Tariff at {tariff_path} has invalid first item.")
    rate_structure = item.get("energyratestructure")
    if not isinstance(rate_structure, list) or not rate_structure:
        raise ValueError(
            f"Tariff at {tariff_path} must contain non-empty 'energyratestructure'."
        )
    return item


def _build_labeled_tariff_from_item(item: dict[str, Any], label: str) -> dict[str, Any]:
    new_item = json.loads(json.dumps(item))
    new_item["label"] = label
    new_item["name"] = label
    return {"items": [new_item]}


def _write_template_outputs(
    default_tariff_path: Path,
    supply_default_tariff_path: Path,
    flat_label: str,
    flat_supply_label: str,
    output_dir: Path,
) -> tuple[str, str]:
    if not supply_default_tariff_path.exists():
        raise FileNotFoundError(
            "Supply default tariff not found at "
            f"{supply_default_tariff_path}. Provide --supply-default-tariff-json "
            "or add a matching <default_stem>_supply.json file."
        )
    default_item = _load_template_item(default_tariff_path)
    supply_item = _load_template_item(supply_default_tariff_path)
    flat_tariff = _build_labeled_tariff_from_item(default_item, flat_label)
    flat_supply_tariff = _build_labeled_tariff_from_item(supply_item, flat_supply_label)
    flat_path = output_dir / f"{flat_label}.json"
    flat_supply_path = output_dir / f"{flat_supply_label}.json"
    written_flat = _write_json(flat_path, flat_tariff)
    written_supply = _write_json(flat_supply_path, flat_supply_tariff)
    return written_flat, written_supply


def _validate_cli_mode(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> str:
    legacy_args = [
        args.label,
        args.volumetric_rate,
        args.fixed_charge,
        args.output_path,
    ]
    template_args = [
        args.default_tariff_json,
        args.supply_default_tariff_json,
        args.flat_label,
        args.flat_supply_label,
        args.output_dir,
    ]
    legacy_requested = any(v is not None for v in legacy_args)
    template_requested = any(v is not None for v in template_args)

    if legacy_requested and template_requested:
        parser.error("Specify either legacy args or template-mode args, not both.")
    if not legacy_requested and not template_requested:
        parser.error(
            "No mode selected. Use legacy args for single tariff output, or "
            "template-mode args to generate flat + flat_supply outputs."
        )
    return "legacy" if legacy_requested else "template"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a flat-rate URDB v7 format tariff JSON file"
    )
    parser.add_argument("--label", help="Tariff label/identifier")
    parser.add_argument(
        "--volumetric-rate",
        type=float,
        help="Volumetric rate in $/kWh",
    )
    parser.add_argument(
        "--fixed-charge",
        type=float,
        help="Fixed monthly charge in $",
    )
    parser.add_argument(
        "--adjustment",
        type=float,
        default=0.0,
        help="Rate adjustment in $/kWh (default: 0.0)",
    )
    parser.add_argument(
        "--utility",
        default="GenericUtility",
        help="Utility name (default: GenericUtility)",
    )
    parser.add_argument(
        "--output-path",
        help="Output JSON file path.",
    )
    parser.add_argument(
        "--default-tariff-json",
        type=Path,
        help="Default utility tariff JSON used to create flat output.",
    )
    parser.add_argument(
        "--supply-default-tariff-json",
        type=Path,
        default=None,
        help="Optional supply default tariff JSON. "
        "If omitted, infer <default_stem>_supply.json.",
    )
    parser.add_argument(
        "--flat-label",
        help="Label for generated flat tariff in template mode.",
    )
    parser.add_argument(
        "--flat-supply-label",
        help="Label for generated flat_supply tariff in template mode.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Shared output directory for template-mode outputs.",
    )

    args = parser.parse_args()
    mode = _validate_cli_mode(parser, args)

    if mode == "legacy":
        missing = [
            name
            for name, value in (
                ("--label", args.label),
                ("--volumetric-rate", args.volumetric_rate),
                ("--fixed-charge", args.fixed_charge),
                ("--output-path", args.output_path),
            )
            if value is None
        ]
        if missing:
            parser.error(
                "Legacy mode requires: --label --volumetric-rate --fixed-charge "
                f"--output-path. Missing: {', '.join(missing)}"
            )
        tariff = create_default_flat_tariff(
            label=args.label,
            volumetric_rate=args.volumetric_rate,
            fixed_charge=args.fixed_charge,
            adjustment=args.adjustment,
            utility=args.utility,
        )
        output_path = Path(args.output_path)
        written_path = _write_json(output_path, tariff)
        print(f"Created tariff file: {written_path}")
        return

    missing = [
        name
        for name, value in (
            ("--default-tariff-json", args.default_tariff_json),
            ("--flat-label", args.flat_label),
            ("--flat-supply-label", args.flat_supply_label),
            ("--output-dir", args.output_dir),
        )
        if value is None
    ]
    if missing:
        parser.error(
            "Template mode requires: --default-tariff-json --flat-label "
            f"--flat-supply-label --output-dir. Missing: {', '.join(missing)}"
        )
    supply_default = args.supply_default_tariff_json or _derive_supply_default_path(
        args.default_tariff_json
    )
    written_flat, written_supply = _write_template_outputs(
        default_tariff_path=args.default_tariff_json,
        supply_default_tariff_path=supply_default,
        flat_label=args.flat_label,
        flat_supply_label=args.flat_supply_label,
        output_dir=args.output_dir,
    )
    print(f"Created tariff files: {written_flat}, {written_supply}")


if __name__ == "__main__":
    main()
