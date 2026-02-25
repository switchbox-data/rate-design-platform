"""Update Google Sheet with formulas for path_supply_energy_mc, path_supply_capacity_mc, and path_tou_supply_mc.

This script reads the spreadsheet structure and fills in formulas instead of values.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import gspread
from dotenv import load_dotenv

load_dotenv()

DEFAULT_SHEET_ID = "14naAchDw95hom88a9tdw4Y8DVxfqhx948x2UbBPujuI"

# Constants for formula paths
ZERO_MC_PATH = "s3://data.sb/nrel/cambium/zero_marginal_costs.csv"
RI_CAMBIUM_PATH = "s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet"
NY_SUPPLY_ENERGY_BASE = (
    "s3://data.sb/switchbox/marginal_costs/ny/supply/energy/utility="
)
NY_SUPPLY_CAPACITY_BASE = (
    "s3://data.sb/switchbox/marginal_costs/ny/supply/capacity/utility="
)
SUPPLY_MC_YEAR = "2025"


def _get_column_letter(col_num: int) -> str:
    """Convert 1-based column number to Excel column letter (A, B, ..., Z, AA, AB, ...)."""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(65 + (col_num % 26)) + result
        col_num //= 26
    return result


def main() -> None:
    """Update Google Sheet with formulas."""
    client_id = os.getenv("G_CLIENT_ID")
    client_secret = os.getenv("G_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError(
            "Google Sheets auth requires G_CLIENT_ID and G_CLIENT_SECRET in .env"
        )

    app_creds = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "project_id": os.getenv("G_PROJECT_ID", ""),
            "auth_uri": os.getenv(
                "G_AUTH_URI", "https://accounts.google.com/o/oauth2/auth"
            ),
            "token_uri": os.getenv(
                "G_TOKEN_URI", "https://oauth2.googleapis.com/token"
            ),
            "redirect_uris": ["http://localhost"],
        }
    }

    # Use cached token if available
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    else:
        base = Path.home() / ".config"
    token_path = base / "gspread" / "authorized_user_rate_design.json"

    saved_token: dict[str, object] | None = None
    if token_path.exists():
        try:
            saved_token = json.loads(token_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass

    if saved_token is not None:
        gc, authorized_user = gspread.oauth_from_dict(
            credentials=app_creds, authorized_user_info=saved_token
        )
    else:
        gc, authorized_user = gspread.oauth_from_dict(credentials=app_creds)

    # Persist token
    if authorized_user:
        token_path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(authorized_user, dict):
            token_path.write_text(
                json.dumps(authorized_user, indent=2), encoding="utf-8"
            )
        else:
            token_path.write_text(str(authorized_user), encoding="utf-8")

    sh = gc.open_by_key(DEFAULT_SHEET_ID)
    ws = sh.sheet1

    # Get header row to find column positions
    headers = ws.row_values(1)
    print("Reading column structure...")

    # Find column indices - print all headers first for debugging
    print("\nAll headers:")
    for i, header in enumerate(headers, start=1):
        print(f"  {_get_column_letter(i)} ({i}): {header}")

    col_indices = {}
    for i, header in enumerate(headers, start=1):
        header_lower = str(header).lower().strip()
        if header_lower == "state":
            col_indices["state"] = i
        elif header_lower == "utility":
            col_indices["utility"] = i
        elif header_lower == "num":
            col_indices["num"] = i
        elif header_lower == "path_supply_energy_mc":
            col_indices["path_supply_energy_mc"] = i
        elif header_lower == "path_supply_capacity_mc":
            col_indices["path_supply_capacity_mc"] = i
        elif (
            "tou" in header_lower and "supply" in header_lower and "mc" in header_lower
        ):
            # Track both W and X columns (path_tou_supply_capacity_mc)
            if "path_tou_supply_capacity_mc_1" not in col_indices:
                col_indices["path_tou_supply_capacity_mc_1"] = i  # W
            else:
                col_indices["path_tou_supply_capacity_mc_2"] = i  # X
        elif header_lower == "supply":
            # Column E is "supply" which indicates add_supply_revenue_requirement (X = true)
            col_indices["supply"] = i

    print(f"Found columns: {col_indices}")

    # Verify we found all needed columns
    required = [
        "state",
        "utility",
        "num",
        "path_supply_energy_mc",
        "path_supply_capacity_mc",
    ]
    missing = [k for k in required if k not in col_indices]
    if missing:
        print(f"Warning: Missing columns: {missing}")
        print("Available headers:")
        for i, h in enumerate(headers, start=1):
            print(f"  {_get_column_letter(i)} ({i}): {h}")

    # Get all data rows (skip header row 1)
    all_values = ws.get_all_values()
    if len(all_values) < 2:
        print("No data rows found")
        return

    # Get column letters - use correct mappings based on user's spreadsheet
    # A = state, B = utility, C = num, E = supply (X means add supply revenue requirement)
    state_col = _get_column_letter(col_indices["state"])  # A
    utility_col = _get_column_letter(col_indices["utility"])  # B
    num_col = _get_column_letter(col_indices["num"])  # C
    energy_mc_col = _get_column_letter(col_indices["path_supply_energy_mc"])  # U
    capacity_mc_col = _get_column_letter(col_indices["path_supply_capacity_mc"])  # V
    # Column E is "supply" which indicates add_supply_revenue_requirement (X = true)
    supply_col = _get_column_letter(
        col_indices.get("supply", 5)
    )  # E (default to 5 if not found)
    tou_mc_col_1 = _get_column_letter(
        col_indices.get("path_tou_supply_capacity_mc_1", 23)
    )  # W
    tou_mc_col_2 = _get_column_letter(
        col_indices.get("path_tou_supply_capacity_mc_2", 24)
    )  # X

    print("\nColumn mappings:")
    print(f"  State: {state_col} (A)")
    print(f"  Utility: {utility_col} (B)")
    print(f"  Num: {num_col} (C)")
    print(f"  Supply (add_supply_revenue_requirement): {supply_col} (E)")
    print(f"  path_supply_energy_mc: {energy_mc_col} (U)")
    print(f"  path_supply_capacity_mc: {capacity_mc_col} (V)")
    print(f"  path_tou_supply_capacity_mc (1): {tou_mc_col_1} (W)")
    print(f"  path_tou_supply_capacity_mc (2): {tou_mc_col_2} (X)")

    # Prepare formulas for each data row (starting from row 2)
    energy_formulas = []
    capacity_formulas = []
    tou_energy_formulas = []
    tou_capacity_formulas = []

    for row_num in range(2, len(all_values) + 1):
        # Formula for path_supply_energy_mc:
        # For NY supply runs: use separate energy file
        # For NY delivery-only runs: use zero_marginal_costs.csv (cambium)
        # For RI supply runs: use cambium ISONE path
        # For RI delivery-only runs: use zero_marginal_costs.csv (cambium)
        energy_formula = (
            f'=IF(AND(${state_col}{row_num}="NY", ${supply_col}{row_num}="X"), '
            f'"{NY_SUPPLY_ENERGY_BASE}" & LOWER(${utility_col}{row_num}) & "/year={SUPPLY_MC_YEAR}/data.parquet", '
            f'IF(${state_col}{row_num}="NY", "{ZERO_MC_PATH}", '
            f'IF(AND(${state_col}{row_num}="RI", ${supply_col}{row_num}="X"), "{RI_CAMBIUM_PATH}", '
            f'IF(${state_col}{row_num}="RI", "{ZERO_MC_PATH}", ""))))'
        )
        energy_formulas.append(energy_formula)

        # Formula for path_supply_capacity_mc:
        # For NY supply runs: use separate capacity file
        # For NY delivery-only runs: use zero_marginal_costs.csv (cambium)
        # For RI supply runs: use cambium ISONE path
        # For RI delivery-only runs: use zero_marginal_costs.csv (cambium)
        capacity_formula = (
            f'=IF(AND(${state_col}{row_num}="NY", ${supply_col}{row_num}="X"), '
            f'"{NY_SUPPLY_CAPACITY_BASE}" & LOWER(${utility_col}{row_num}) & "/year={SUPPLY_MC_YEAR}/data.parquet", '
            f'IF(${state_col}{row_num}="NY", "{ZERO_MC_PATH}", '
            f'IF(AND(${state_col}{row_num}="RI", ${supply_col}{row_num}="X"), "{RI_CAMBIUM_PATH}", '
            f'IF(${state_col}{row_num}="RI", "{ZERO_MC_PATH}", ""))))'
        )
        capacity_formulas.append(capacity_formula)

        # Formula for path_tou_supply_capacity_mc (W) - energy MC for TOU (only for runs where num = 13 or 14):
        # For NY runs with num=13 or 14: use separate energy file
        # For RI runs with num=13 or 14: use cambium ISONE path
        tou_energy_formula = (
            f'=IF(AND(${state_col}{row_num}="NY", OR(${num_col}{row_num}=13, ${num_col}{row_num}=14)), '
            f'"{NY_SUPPLY_ENERGY_BASE}" & LOWER(${utility_col}{row_num}) & "/year={SUPPLY_MC_YEAR}/data.parquet", '
            f'IF(AND(${state_col}{row_num}="RI", OR(${num_col}{row_num}=13, ${num_col}{row_num}=14)), "{RI_CAMBIUM_PATH}", '
            f'""))'
        )
        tou_energy_formulas.append(tou_energy_formula)

        # Formula for path_tou_supply_capacity_mc (X) - capacity MC for TOU (only for runs where num = 13 or 14):
        # For NY runs with num=13 or 14: use separate capacity file
        # For RI runs with num=13 or 14: use cambium ISONE path
        tou_capacity_formula = (
            f'=IF(AND(${state_col}{row_num}="NY", OR(${num_col}{row_num}=13, ${num_col}{row_num}=14)), '
            f'"{NY_SUPPLY_CAPACITY_BASE}" & LOWER(${utility_col}{row_num}) & "/year={SUPPLY_MC_YEAR}/data.parquet", '
            f'IF(AND(${state_col}{row_num}="RI", OR(${num_col}{row_num}=13, ${num_col}{row_num}=14)), "{RI_CAMBIUM_PATH}", '
            f'""))'
        )
        tou_capacity_formulas.append(tou_capacity_formula)

    # Update the sheet with formulas
    print(f"\nUpdating {len(energy_formulas)} rows with formulas...")

    # Update path_supply_energy_mc column (U)
    energy_range = f"{energy_mc_col}2:{energy_mc_col}{len(all_values)}"
    ws.update(
        values=[[f] for f in energy_formulas],
        range_name=energy_range,
        raw=False,
    )
    print(f"✓ Updated {energy_mc_col} column (path_supply_energy_mc)")

    # Update path_supply_capacity_mc column (V)
    capacity_range = f"{capacity_mc_col}2:{capacity_mc_col}{len(all_values)}"
    ws.update(
        values=[[f] for f in capacity_formulas],
        range_name=capacity_range,
        raw=False,
    )
    print(f"✓ Updated {capacity_mc_col} column (path_supply_capacity_mc)")

    # Update path_tou_supply_capacity_mc columns (W and X) if they exist
    if "path_tou_supply_capacity_mc_1" in col_indices:
        tou_range_1 = f"{tou_mc_col_1}2:{tou_mc_col_1}{len(all_values)}"
        ws.update(
            values=[[f] for f in tou_energy_formulas],
            range_name=tou_range_1,
            raw=False,
        )
        print(f"✓ Updated {tou_mc_col_1} column (path_tou_supply_capacity_mc - energy)")

    if "path_tou_supply_capacity_mc_2" in col_indices:
        tou_range_2 = f"{tou_mc_col_2}2:{tou_mc_col_2}{len(all_values)}"
        ws.update(
            values=[[f] for f in tou_capacity_formulas],
            range_name=tou_range_2,
            raw=False,
        )
        print(
            f"✓ Updated {tou_mc_col_2} column (path_tou_supply_capacity_mc - capacity)"
        )

    print("\n✓ All formulas updated successfully!")


if __name__ == "__main__":
    main()
