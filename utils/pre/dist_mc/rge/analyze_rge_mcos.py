"""Compute marginal costs from the RG&E CRA-prepared MCOS workbook.

Thin wrapper around the shared NERA-style project-level analysis logic in
``nyseg/analyze_nyseg_mcos.py``, parameterized for RG&E's 4 divisions
and system peak.

See the NYSEG script's docstring and ``nyseg/README.md`` for full
methodology documentation.  Key differences for RG&E:

  - 4 divisions (Canandaigua, Central, Fillmore, Sodus) vs. NYSEG's 13
  - System peak: 1,428.52 MW (2035 forecast)

Usage (via Justfile):
    just analyze-rge
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "nyseg"))

from analyze_nyseg_mcos import CRAConfig, run_analysis  # noqa: E402  # ty: ignore[unresolved-import]

RGE_DIVISIONS = ["Canandaigua", "Central", "Fillmore", "Sodus"]


def main() -> None:
    parser = argparse.ArgumentParser(description="RG&E MCOS analysis")
    parser.add_argument("--path-xlsx", required=True)
    parser.add_argument("--system-peak-mw", type=float, required=True)
    parser.add_argument("--path-output-dir", required=True)
    args = parser.parse_args()

    config = CRAConfig(
        utility="rge",
        display_name="RG&E",
        system_peak_mw=args.system_peak_mw,
        divisions=RGE_DIVISIONS,
    )

    run_analysis(args.path_xlsx, config, Path(args.path_output_dir))


if __name__ == "__main__":
    main()
