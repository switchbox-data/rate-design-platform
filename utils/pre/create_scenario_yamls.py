"""Create scenario YAMLs from the Runs & Charts Google Sheet.

Reads the sheet, groups by (state, utility), and writes
rate_design/<state>/hp_rates/config/scenarios_<utility>.yaml for each group.

Note on path_cambium_marginal_costs column:
    For NY supply runs (runs with add_supply_revenue_requirement=true), the
    path_cambium_marginal_costs column should point to NYISO-derived supply MCs:
        s3://data.sb/switchbox/marginal_costs/ny/supply/utility={utility}/year=2025/data.parquet
    
    For NY delivery-only runs (add_supply_revenue_requirement=false), use:
        s3://data.sb/nrel/cambium/zero_marginal_costs.csv
    
    For RI runs, continue using Cambium paths:
        s3://data.sb/nrel/cambium/2024/scenario=MidCase/t=2025/gea=ISONE/r=p133/data.parquet
    
    To automatically update the Google Sheet for NY supply runs, use:
        uv run python utils/pre/create_scenario_yamls.py --update-sheet
    
    Or after manual updates, run: just create-scenario-yamls
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

import gspread
import yaml
from dotenv import load_dotenv

from utils import get_project_root


# Cached OAuth token path; reuse to avoid browser prompt on every run.
# Use standard gspread config dir so it works regardless of project root / cwd.
def _gspread_token_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    else:
        base = Path.home() / ".config"
    return base / "gspread" / "authorized_user_rate_design.json"


DEFAULT_SHEET_ID = "14naAchDw95hom88a9tdw4Y8DVxfqhx948x2UbBPujuI"


def _normalize_header(name: str) -> str:
    """Lowercase, strip, spaces and hyphens to underscores, remove ?."""
    s = str(name).strip().lower()
    s = re.sub(r"[\s\-]+", "_", s)
    s = s.replace("?", "").rstrip("_")
    return s


def _path_tariffs_to_dict(comma_separated: str) -> dict[str, str]:
    """Convert 'key: path' pairs (comma-separated) to dict.

    Supports:
        - key: path  e.g. all: tariffs/electric/rie_flat_supply_calibrated.json
    """
    if not str(comma_separated).strip():
        return {}
    out: dict[str, str] = {}
    for part in str(comma_separated).split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            key, path_str = part.split(":", 1)
            key = key.strip()
            path_str = path_str.strip()
            if not key or not path_str:
                raise ValueError(
                    f"path_tariffs segment must be 'key: path', got {part!r}"
                )
        else:
            raise ValueError(
                "path_tariffs segment must be 'key: path'; "
                f"bare path is not allowed: {part!r}"
            )
        if key in out:
            raise ValueError(
                f"path_tariffs duplicate key {key!r} from {out[key]!r} and {path_str!r}"
            )
        out[key] = path_str
    return out


def _parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in (0, 1):
            return bool(value)
        raise ValueError(f"Invalid boolean integer {value!r}; expected 0 or 1")
    if isinstance(value, float):
        if value in (0.0, 1.0):
            return bool(int(value))
        raise ValueError(f"Invalid boolean float {value!r}; expected 0.0 or 1.0")
    s = str(value).strip().upper()
    if s in ("TRUE", "1", "YES", "Y"):
        return True
    if s in ("FALSE", "0", "NO", "N"):
        return False
    raise ValueError(
        f"Invalid boolean value {value!r}; expected one of "
        "TRUE/FALSE, 1/0, YES/NO, Y/N."
    )


def _row_to_run(row: dict[str, str], headers: list[str]) -> dict[str, object]:
    """Convert one sheet row to a run dict for YAML."""

    norm_to_header: dict[str, str] = {}
    for h in headers:
        norm = _normalize_header(h)
        if norm in norm_to_header:
            raise ValueError(
                f"Duplicate normalized header {norm!r} from "
                f"{norm_to_header[norm]!r} and {h!r}"
            )
        norm_to_header[norm] = h

    def get(key: str) -> str:
        normalized = _normalize_header(key)
        header = norm_to_header.get(normalized)
        if header is None:
            raise ValueError(f"Required column missing: {key!r}")
        val = row.get(header, "")
        return str(val).strip() if val else ""

    def get_optional(key: str) -> str:
        normalized = _normalize_header(key)
        header = norm_to_header.get(normalized)
        if header is None:
            return ""
        val = row.get(header, "")
        return str(val).strip() if val else ""

    def require_non_empty(key: str) -> str:
        value = get(key)
        if not value:
            raise ValueError(f"Required value is blank for {key!r}")
        return value

    def parse_required_int(key: str) -> int:
        value = require_non_empty(key)
        try:
            return int(value)
        except ValueError as exc:
            raise ValueError(f"{key!r} must be an integer, got {value!r}") from exc

    def parse_required_float(key: str) -> float:
        value = require_non_empty(key)
        try:
            return float(value)
        except ValueError as exc:
            raise ValueError(f"{key!r} must be a float, got {value!r}") from exc

    run: dict[str, object] = {}

    run["run_name"] = get("run_name")

    run["state"] = get("state").upper()

    run["utility"] = get("utility").lower()

    run["run_type"] = get("run_type")

    run["upgrade"] = get("upgrade")

    for key in (
        "path_tariff_maps_electric",
        "path_tariff_maps_gas",
        "path_resstock_metadata",
        "path_resstock_loads",
        "path_cambium_marginal_costs",
        "path_td_marginal_costs",
        "path_utility_assignment",
        "path_tariffs_gas",
        "path_outputs",
    ):
        run[key] = get(key)

    run["path_tariffs_electric"] = _path_tariffs_to_dict(
        require_non_empty("path_tariffs_electric")
    )

    run["utility_delivery_revenue_requirement"] = get(
        "utility_delivery_revenue_requirement"
    )

    run["add_supply_revenue_requirement"] = _parse_bool(
        require_non_empty("add_supply_revenue_requirement")
    )

    run["path_electric_utility_stats"] = get("path_electric_utility_stats")

    path_tou_supply_mc = get_optional("path_tou_supply_mc")
    if path_tou_supply_mc:
        run["path_tou_supply_mc"] = path_tou_supply_mc

    run["solar_pv_compensation"] = require_non_empty("solar_pv_compensation")

    run["year_run"] = parse_required_int("year_run")

    run["year_dollar_conversion"] = parse_required_int("year_dollar_conversion")

    run["process_workers"] = parse_required_int("process_workers")

    sample_size = get("sample_size")
    if sample_size:
        try:
            run["sample_size"] = int(sample_size)
        except ValueError:
            run["sample_size"] = sample_size

    run["elasticity"] = parse_required_float("elasticity")

    return run


def _insert_blank_lines_between_runs(yaml_str: str) -> str:
    """Insert a blank line before run keys 2+, not before the first run key."""
    lines = yaml_str.splitlines()
    out: list[str] = []
    seen_run_key = False
    for line in lines:
        stripped = line.strip()
        is_run_key = (
            line.startswith("  ") and stripped.endswith(":") and stripped[:-1].isdigit()
        )
        if is_run_key and seen_run_key and (not out or out[-1] != ""):
            out.append("")
        if is_run_key:
            seen_run_key = True
        out.append(line)
    return "\n".join(out) + ("\n" if yaml_str.endswith("\n") else "")


def _cell_to_str(value: object) -> str:
    """Convert sheet cell to string while preserving numeric zeros."""
    if value is None:
        return ""
    return str(value).strip()


def _col_num_to_letter(col_num: int) -> str:
    """Convert 1-based column number to Excel column letter (A, B, ..., Z, AA, ...)."""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(65 + (col_num % 26)) + result
        col_num //= 26
    return result


def _get_worksheet(
    gc: gspread.Client, sheet_id: str, name: str | None, index: int | None
):  # noqa: ANN001
    """Open spreadsheet and return worksheet by name or index."""
    sh = gc.open_by_key(sheet_id)
    if name is not None:
        return sh.worksheet(name)
    if index is not None:
        return sh.get_worksheet(index)
    return sh.sheet1


def _update_ny_supply_mc_paths(
    ws: gspread.Worksheet,
    headers: list[str],
    norm_to_header: dict[str, str],
    rows_as_dicts: list[dict[str, str]],
    state_key: str,
    utility_key: str,
) -> int:
    """Update path_cambium_marginal_costs for NY supply runs in the Google Sheet.
    
    Args:
        ws: The worksheet to update.
        headers: List of original header names.
        norm_to_header: Mapping from normalized header to original header.
        rows_as_dicts: All data rows from the sheet (with original row positions).
        state_key: Original header name for state column.
        utility_key: Original header name for utility column.
    
    Returns:
        Number of cells updated.
    """
    # Find column indices
    path_mc_key = norm_to_header.get("path_cambium_marginal_costs")
    add_supply_key = norm_to_header.get("add_supply_revenue_requirement")
    
    if not path_mc_key or not add_supply_key:
        print("Warning: Missing required columns for sheet update. Skipping update.")
        return 0
    
    # Find column index (1-based for gspread)
    path_mc_col_idx = headers.index(path_mc_key) + 1
    
    # Get all values to find row numbers (header row is 1, data starts at 2)
    all_values = ws.get_all_values()
    if len(all_values) < 2:
        return 0
    
    updates: list[dict[str, object]] = []
    updated_count = 0
    prev_utility = ""
    
    # Process each row (row index in sheet = row_index + 2: 1 for header, 1-based)
    for row_idx, row in enumerate(rows_as_dicts):
        sheet_row = row_idx + 2  # 1 for header, 1-based indexing
        
        state_val = (row.get(state_key) or "").strip().upper()
        if not state_val:
            continue
        
        utility_val = (row.get(utility_key) or "").strip().lower()
        if utility_val and not utility_val.isdigit():
            prev_utility = utility_val
        elif prev_utility:
            utility_val = prev_utility
        
        # Check if this is a NY supply run
        if state_val != "NY":
            continue
        
        add_supply_val = (row.get(add_supply_key) or "").strip()
        try:
            is_supply_run = _parse_bool(add_supply_val)
        except (ValueError, KeyError):
            continue
        
        if not is_supply_run:
            continue
        
        # Generate new path
        new_path = f"s3://data.sb/switchbox/marginal_costs/ny/supply/utility={utility_val}/year=2025/data.parquet"
        
        # Check current value - only update if different
        current_path = (row.get(path_mc_key) or "").strip()
        if current_path == new_path:
            continue
        
        # Add to batch update (A1 notation: e.g., "A2", "B5")
        col_letter = _col_num_to_letter(path_mc_col_idx)
        cell_address = f"{col_letter}{sheet_row}"
        updates.append({
            "range": cell_address,
            "values": [[new_path]]
        })
        updated_count += 1
        current_display = current_path[:60] + "..." if len(current_path) > 60 else current_path
        new_display = new_path[:60] + "..." if len(new_path) > 60 else new_path
        print(f"  Row {sheet_row} ({utility_val}): {current_display} → {new_display}")
    
    # Batch update all cells at once
    if updates:
        ws.batch_update(updates)
        print(f"\n✓ Updated {updated_count} NY supply run paths in Google Sheet")
    else:
        print("  No NY supply runs found that need updating")
    
    return updated_count


def run(
    sheet_id: str = DEFAULT_SHEET_ID,
    worksheet_name: str | None = None,
    worksheet_index: int | None = None,
    output_dir: Path | None = None,
    update_sheet: bool = False,
) -> None:
    """Fetch sheet, group by (state, utility), write scenario YAMLs.
    
    Args:
        sheet_id: Google Spreadsheet ID.
        worksheet_name: Worksheet name (default: first sheet).
        worksheet_index: Worksheet index 0-based (default: first sheet).
        output_dir: Output root directory (default: project root).
        update_sheet: If True, update path_cambium_marginal_costs for NY supply runs.
    """
    load_dotenv()

    client_id = os.getenv("G_CLIENT_ID")
    client_secret = os.getenv("G_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError(
            "Google Sheets auth requires G_CLIENT_ID and G_CLIENT_SECRET in .env "
            "(from a Google Cloud OAuth 2.0 client ID for a desktop app)."
        )
    # Standard Google OAuth2 endpoints; override with G_AUTH_URI / G_TOKEN_URI if set
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
    token_path = _gspread_token_path()
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
    # Persist token so next run can skip the browser (gspread returns JSON str or dict)
    if authorized_user:
        token_path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(authorized_user, dict):
            token_path.write_text(
                json.dumps(authorized_user, indent=2), encoding="utf-8"
            )
        else:
            token_path.write_text(str(authorized_user), encoding="utf-8")

    ws = _get_worksheet(gc, sheet_id, worksheet_name, worksheet_index)
    all_rows = ws.get_all_records()
    if not all_rows:
        return

    raw_headers = list(all_rows[0].keys()) if all_rows else []
    headers = [str(h) for h in raw_headers]
    # Build row list as list of dicts with original header keys.
    # Preserve numeric zeros (0/0.0) instead of treating them as blank.
    rows_as_dicts: list[dict[str, str]] = []
    for row in all_rows:
        rows_as_dicts.append({str(k): _cell_to_str(v) for k, v in row.items()})

    # Normalized header -> original header
    norm_to_header: dict[str, str] = {}
    for h in headers:
        norm_to_header[_normalize_header(h)] = h

    state_key = norm_to_header.get("state")
    utility_key = norm_to_header.get("utility")
    num_key = norm_to_header.get("num")
    if not state_key or not utility_key or not num_key:
        raise ValueError(
            "Sheet must have columns state, utility, and num. "
            f"Normalized headers: {list(norm_to_header)}"
        )

    # Filter to data rows with state set; forward-fill utility
    data_rows: list[dict[str, str]] = []
    prev_utility = ""
    for row in rows_as_dicts:
        state_val = (row.get(state_key) or "").strip()
        if not state_val:
            continue
        utility_val = (row.get(utility_key) or "").strip()
        if utility_val and not utility_val.isdigit():
            prev_utility = utility_val
        elif prev_utility:
            row[utility_key] = prev_utility
        data_rows.append(row)
    
    # Update sheet if requested
    if update_sheet:
        print("\n" + "=" * 60)
        print("UPDATING GOOGLE SHEET: NY Supply MC Paths")
        print("=" * 60)
        _update_ny_supply_mc_paths(ws, headers, norm_to_header, rows_as_dicts, state_key, utility_key)
        print("=" * 60 + "\n")

    # Group by (state, utility)
    groups: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in data_rows:
        s = (row.get(state_key) or "").strip().upper()
        u = (row.get(utility_key) or "").strip().lower()
        if not s or not u:
            continue
        key = (s, u)
        if key not in groups:
            groups[key] = []
        groups[key].append(row)

    out_root = output_dir or get_project_root()

    for (state, utility), group_rows in groups.items():
        # Sort by num
        def num_val(r: dict[str, str]) -> int:
            n = (r.get(num_key) or "").strip()
            try:
                return int(n)
            except ValueError:
                return 0

        group_rows.sort(key=num_val)
        runs: dict[int, dict[str, object]] = {}
        for row in group_rows:
            n = num_val(row)
            if n <= 0:
                continue
            run_dict = _row_to_run(row, headers)
            runs[n] = run_dict

        if not runs:
            continue

        out_path = (
            out_root
            / "rate_design"
            / state.lower()
            / "hp_rates"
            / "config"
            / "scenarios"
            / f"scenarios_{utility}.yaml"
        )

        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"runs": runs}
        yaml_str = yaml.dump(
            payload,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
        yaml_str = _insert_blank_lines_between_runs(yaml_str)
        out_path.write_text(yaml_str, encoding="utf-8")
        print(f"Wrote {out_path} ({len(runs)} runs)")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create scenario YAMLs from the Runs & Charts Google Sheet."
    )
    parser.add_argument(
        "--sheet-id",
        default=DEFAULT_SHEET_ID,
        help="Google Spreadsheet ID (default: Runs & Charts sheet).",
    )
    parser.add_argument(
        "--sheet-name",
        help="Worksheet name (default: first sheet).",
    )
    parser.add_argument(
        "--worksheet-index",
        type=int,
        help="Worksheet index 0-based (default: first sheet).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output root directory (default: project root).",
    )
    parser.add_argument(
        "--update-sheet",
        action="store_true",
        help="Update path_cambium_marginal_costs for NY supply runs in the Google Sheet.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    run(
        sheet_id=args.sheet_id,
        worksheet_name=args.sheet_name,
        worksheet_index=args.worksheet_index,
        output_dir=args.output_dir,
        update_sheet=args.update_sheet,
    )


if __name__ == "__main__":
    main()
