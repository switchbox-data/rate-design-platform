"""Compute marginal costs from the RG&E CRA-prepared MCOS workbook.

Thin wrapper around the shared CRA analysis logic in
``nyseg/analyze_nyseg_mcos.py``, parameterized for RG&E's 4 divisions
and workbook sheet names.

See the NYSEG script's docstring and ``nyseg/README.md`` for full
methodology documentation.  Key differences for RG&E:

  - 4 divisions (Canandaigua, Central, Fillmore, Sodus) vs. NYSEG's 13
  - System peak: 1,428.52 MW (2035 forecast)
  - T4a/T4B sheet names differ from NYSEG's T4/T4B
  - T5/T6 sheets have a different layout (not per-division);
    division-level verification is limited to upstream (T4a/T4b)

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
    parser = argparse.ArgumentParser(description="RG&E MCOS analysis (CRA)")
    parser.add_argument("--path-xlsx", required=True)
    parser.add_argument("--system-peak-mw", type=float, required=True)
    parser.add_argument("--path-output-dir", required=True)
    args = parser.parse_args()

    config = CRAConfig(
        utility="rge",
        display_name="RG&E",
        divisions=RGE_DIVISIONS,
        system_peak_mw=args.system_peak_mw,
        t4_pattern="T4a",
        t4b_pattern="T4B",
        has_division_t5_t6=False,
    )

    run_analysis(args.path_xlsx, config, Path(args.path_output_dir))


if __name__ == "__main__":
    main()
