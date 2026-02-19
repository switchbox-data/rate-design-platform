"""Create scenario YAMLs from the Runs & Charts Google Sheet.

Reads the sheet, groups by (state, utility), and writes
rate_design/<state>/hp_rates/config/scenarios_<utility>.yaml for each group.
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
    """Convert comma-separated paths to dict keyed by filename stem."""
    if not str(comma_separated).strip():
        return {}
    out: dict[str, str] = {}
    for part in str(comma_separated).split(","):
        path_str = part.strip()
        if not path_str:
            continue
        stem = Path(path_str).stem
        if stem in out:
            raise ValueError(
                f"path_tariffs duplicate stem {stem!r} from {out[stem]!r} and {path_str!r}"
            )
        out[stem] = path_str
    return out


def _parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().upper()
    if s in ("TRUE", "1", "YES", "Y"):
        return True
    if s in ("FALSE", "0", "NO", "N"):
        return False
    return False


def _row_to_run(row: dict[str, str], headers: list[str]) -> dict[str, object]:
    """Convert one sheet row to a run dict for YAML."""
    def get(key: str, default: str | None = None) -> str:
        normalized = _normalize_header(key)
        for h in headers:
            if _normalize_header(h) == normalized:
                val = row.get(h, "")
                return str(val).strip() if val else (default or "")
        return default or ""

    run: dict[str, object] = {}

    run_name = get("run_name")
    if run_name:
        run["run_name"] = run_name

    state = get("state")
    if state:
        run["state"] = state.upper()

    utility = get("utility")
    if utility:
        run["utility"] = utility.lower()

    run_type = get("run_type")
    if run_type:
        run["run_type"] = run_type

    upgrade = get("upgrade")
    if upgrade:
        run["upgrade"] = upgrade

    for key in (
        "path_tariff_maps_electric",
        "path_tariff_maps_gas",
        "path_resstock_metadata",
        "path_resstock_loads",
        "path_cambium_marginal_costs",
        "path_td_marginal_costs",
        "path_utility_assignment",
        "path_tariffs_gas",
    ):
        v = get(key)
        if v:
            run[key] = v

    path_tariffs_elec = get("path_tariffs_electric")
    if path_tariffs_elec:
        run["path_tariffs_electric"] = _path_tariffs_to_dict(path_tariffs_elec)

    rr = get("utility_delivery_revenue_requirement")
    if rr:
        run["utility_delivery_revenue_requirement"] = rr

    add_supply = get("add_supply_revenue_requirement")
    if add_supply:
        run["add_supply_revenue_requirement"] = _parse_bool(add_supply)

    ucc = get("utility_customer_count")
    if ucc:
        run["utility_customer_count"] = ucc

    solar = get("solar_pv_compensation")
    run["solar_pv_compensation"] = solar or "net_metering"

    year_run = get("year_run")
    run["year_run"] = int(year_run) if year_run.isdigit() else 2025

    year_dollar = get("year_dollar_conversion")
    run["year_dollar_conversion"] = int(year_dollar) if year_dollar.isdigit() else 2025

    workers = get("process_workers")
    run["process_workers"] = int(workers) if workers.isdigit() else 20

    return run


def _get_worksheet(gc: gspread.Client, sheet_id: str, name: str | None, index: int | None):  # noqa: ANN001
    """Open spreadsheet and return worksheet by name or index."""
    sh = gc.open_by_key(sheet_id)
    if name is not None:
        return sh.worksheet(name)
    if index is not None:
        return sh.get_worksheet(index)
    return sh.sheet1


def run(
    sheet_id: str = DEFAULT_SHEET_ID,
    worksheet_name: str | None = None,
    worksheet_index: int | None = None,
    output_dir: Path | None = None,
    dry_run: bool = False,
) -> None:
    """Fetch sheet, group by (state, utility), write scenario YAMLs."""
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
            "auth_uri": os.getenv("G_AUTH_URI", "https://accounts.google.com/o/oauth2/auth"),
            "token_uri": os.getenv("G_TOKEN_URI", "https://oauth2.googleapis.com/token"),
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
    # Build row list as list of dicts with original header keys
    rows_as_dicts: list[dict[str, str]] = []
    for row in all_rows:
        rows_as_dicts.append({str(k): str(v or "").strip() for k, v in row.items()})

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
            / f"scenarios_{utility}.yaml"
        )

        if dry_run:
            print(f"Would write {len(runs)} runs to {out_path}")
            print(f"  First run keys: {list(runs[min(runs)].keys())}")
            continue

        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"runs": runs}
        yaml_str = yaml.dump(
            payload,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
        # Insert blank line before each run key for readability
        yaml_str = re.sub(r"\n(  \d+:)", r"\n\n\1", yaml_str)
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
        "--dry-run",
        action="store_true",
        help="Print what would be written, do not write files.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    run(
        sheet_id=args.sheet_id,
        worksheet_name=args.sheet_name,
        worksheet_index=args.worksheet_index,
        output_dir=args.output_dir,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
