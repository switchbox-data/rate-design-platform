"""Ensure all tariff JSONs in a directory have the URDB v7 ``{"items": [...]}`` envelope.

Gas tariffs fetched from Rate Acuity are written as bare tariff dicts. CAIRO's
``__initialize_tariff__`` expects the standard URDB v7 wrapper
``{"items": [<tariff>]}``. This script checks every JSON file in the given
directory and adds the wrapper in-place when it is missing.

Usage:
    uv run python utils/pre/ensure_urdb_envelope.py <directory>
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def ensure_envelope(path: Path) -> bool:
    """Wrap *path* in ``{"items": [...]}`` if missing. Return True if modified."""
    with path.open() as f:
        data = json.load(f)

    if not isinstance(data, dict):
        log.warning("Skipping %s: top-level value is not a JSON object", path.name)
        return False

    if "items" in data:
        return False

    wrapped = {"items": [data]}
    path.write_text(json.dumps(wrapped, indent=2) + "\n")
    return True


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <tariff-directory>", file=sys.stderr)
        sys.exit(1)

    directory = Path(sys.argv[1])
    if not directory.is_dir():
        log.error("Not a directory: %s", directory)
        sys.exit(1)

    json_files = sorted(directory.glob("*.json"))
    if not json_files:
        log.info("No JSON files in %s", directory)
        return

    modified = 0
    for p in json_files:
        if ensure_envelope(p):
            log.info("Wrapped %s", p.name)
            modified += 1

    log.info(
        "Checked %d files in %s — %d wrapped, %d already OK",
        len(json_files),
        directory,
        modified,
        len(json_files) - modified,
    )


if __name__ == "__main__":
    main()
