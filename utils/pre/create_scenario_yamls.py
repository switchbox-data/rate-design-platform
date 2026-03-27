"""Create scenario YAMLs from the Runs & Charts Google Sheet.

Reads the sheet, groups by (state, utility), and writes
rate_design/hp_rates/<state>/config/scenarios_<utility>.yaml for each group.

Supply MC Google Sheet column formulas
--------------------------------------

path_supply_energy_mc (required):
    NY supply runs (add_supply_revenue_requirement=TRUE):
        "s3://data.sb/switchbox/marginal_costs/ny/supply/energy/utility=" & LOWER($B18) & "/year=2025/data.parquet"
    NY delivery-only runs:
        "s3://data.sb/switchbox/marginal_costs/ny/supply/energy/utility=" & LOWER($B18) & "/year=2025/zero.parquet"
    RI supply runs (add_supply_revenue_requirement=TRUE):
        "s3://data.sb/switchbox/marginal_costs/ri/supply/energy/utility=" & LOWER($B18) & "/year=2025/data.parquet"
    RI delivery-only runs:
        "s3://data.sb/switchbox/marginal_costs/ri/supply/energy/utility=" & LOWER($B18) & "/year=2025/zero.parquet"

    Full formula (where E18 = add_supply_revenue_requirement column, X = TRUE):
    =IF(AND($A18="NY", E18="X"),
        "s3://data.sb/switchbox/marginal_costs/ny/supply/energy/utility=" & LOWER($B18) & "/year=2025/data.parquet",
        IF(AND($A18="NY", E18<>"X"),
            "s3://data.sb/switchbox/marginal_costs/ny/supply/energy/utility=" & LOWER($B18) & "/year=2025/zero.parquet",
            IF(AND($A18="RI", E18="X"),
                "s3://data.sb/switchbox/marginal_costs/ri/supply/energy/utility=" & LOWER($B18) & "/year=2025/data.parquet",
                "s3://data.sb/switchbox/marginal_costs/ri/supply/energy/utility=" & LOWER($B18) & "/year=2025/zero.parquet")))

    Note: RI supply energy MC uses ISO-NE real-time zonal LMP data (not Cambium).
    Zero parquets are placeholders for delivery-only runs where supply MCs are not needed.

path_supply_capacity_mc (required):
    NY supply runs (add_supply_revenue_requirement=TRUE):
        "s3://data.sb/switchbox/marginal_costs/ny/supply/capacity/utility=" & LOWER($B18) & "/year=2025/data.parquet"
    NY delivery-only runs:
        "s3://data.sb/switchbox/marginal_costs/ny/supply/capacity/utility=" & LOWER($B18) & "/year=2025/zero.parquet"
    RI supply runs (add_supply_revenue_requirement=TRUE):
        "s3://data.sb/switchbox/marginal_costs/ri/supply/capacity/utility=" & LOWER($B18) & "/year=2025/data.parquet"
    RI delivery-only runs:
        "s3://data.sb/switchbox/marginal_costs/ri/supply/capacity/utility=" & LOWER($B18) & "/year=2025/zero.parquet"

    Full formula (where E18 = add_supply_revenue_requirement column, X = TRUE):
    =IF(AND($A18="NY", E18="X"),
        "s3://data.sb/switchbox/marginal_costs/ny/supply/capacity/utility=" & LOWER($B18) & "/year=2025/data.parquet",
        IF(AND($A18="NY", E18<>"X"),
            "s3://data.sb/switchbox/marginal_costs/ny/supply/capacity/utility=" & LOWER($B18) & "/year=2025/zero.parquet",
            IF(AND($A18="RI", E18="X"),
                "s3://data.sb/switchbox/marginal_costs/ri/supply/capacity/utility=" & LOWER($B18) & "/year=2025/data.parquet",
                "s3://data.sb/switchbox/marginal_costs/ri/supply/capacity/utility=" & LOWER($B18) & "/year=2025/zero.parquet")))

    Note: Zero parquets are ONLY placeholders for capacity in delivery-only runs.
    For supply runs, actual ISO-NE supply MCs (energy from LMP, capacity from FCA) should be loaded.
    RI capacity MC uses data.parquet for supply runs (FCA-based allocation) and zero.parquet for delivery-only runs.
    NY uses separate NYISO LBMP + ICAP parquets for supply runs, and zero-filled parquets for delivery-only runs.

path_supply_ancillary_mc (optional):
    RI supply runs (add_supply_revenue_requirement=TRUE):
        "s3://data.sb/switchbox/marginal_costs/ri/supply/ancillary/utility=" & LOWER($B18) & "/year=2025/data.parquet"
    RI delivery-only runs:
        "" (empty, not used)
    NY runs:
        "" (empty, not used)

    Full formula (where E18 = add_supply_revenue_requirement column, X = TRUE):
    =IF(AND($A18="RI", E18="X"),
        "s3://data.sb/switchbox/marginal_costs/ri/supply/ancillary/utility=" & LOWER($B18) & "/year=2025/data.parquet",
        "")

    Note: RI ancillary MC uses ISO-NE regulation clearing prices (reg_service_price + reg_capacity_price).

path_tou_supply_mc formula (for runs where num = 13 or 14):
    =IF(AND($A18="NY", OR($C18=13, $C18=14)),
        "s3://data.sb/switchbox/marginal_costs/ny/supply/energy/utility=" & LOWER($B18) & "/year=2025/data.parquet",
        "")

    Where:
    - $A18 is the state column (NY)
    - $B18 is the utility column
    - $C18 is the num column (run number)

    path_dist_and_sub_tx_mc formula (required for NY and RI):
    =IF($A18="NY",
        "s3://data.sb/switchbox/marginal_costs/ny/dist_and_sub_tx/utility=" & LOWER($B18) & "/year=2025/data.parquet",
        IF($A18="RI",
            "s3://data.sb/switchbox/marginal_costs/ri/dist_and_sub_tx/utility=" & LOWER($B18) & "/year=2025/data.parquet",
            ""))

    path_bulk_tx_mc formula (required for NY and RI):
    =IF($A18="NY",
        "s3://data.sb/switchbox/marginal_costs/ny/bulk_tx/utility=" & LOWER($B18) & "/year=2025/data.parquet",
        IF($A18="RI",
            "s3://data.sb/switchbox/marginal_costs/ri/bulk_tx/utility=" & LOWER($B18) & "/year=2025/data.parquet",
            ""))

    Where:
    - $A18 is the state column (NY or RI)
    - $B18 is the utility column

    Column naming conventions:
    - Use path_dist_and_sub_tx_mc (not path_dist_and_sub_tx_marginal_costs or path_td_marginal_costs)
    - Use path_bulk_tx_mc (not path_bulk_tx_marginal_costs or path_transmission_marginal_costs)
    - Backward compatibility: old column names path_td_marginal_costs and
      path_dist_and_sub_tx_marginal_costs are still supported

    path_dist_and_sub_tx_mc formula (NY):
    ="s3://data.sb/switchbox/marginal_costs/ny/dist_and_sub_tx/utility=" & LOWER($C18) & "/year=2025/data.parquet"

    path_dist_and_sub_tx_mc formula (RI):
    ="s3://data.sb/switchbox/marginal_costs/ri/dist_and_sub_tx/utility=" & LOWER($C18) & "/year=2025/data.parquet"

    Where:
    - $C18 is the utility column

    After updating the Google Sheet, run: just create-scenario-yamls
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
        "path_dist_and_sub_tx_mc",
        "path_utility_assignment",
        "path_tariffs_gas",
        "path_outputs",
    ):
        run[key] = get(key)

    run["path_supply_energy_mc"] = require_non_empty("path_supply_energy_mc")
    run["path_supply_capacity_mc"] = require_non_empty("path_supply_capacity_mc")

    run["path_tariffs_electric"] = _path_tariffs_to_dict(
        require_non_empty("path_tariffs_electric")
    )

    run["utility_revenue_requirement"] = get("utility_revenue_requirement")

    # Accept either the new column name or the old one for backward compatibility
    supply_raw = get_optional("run_includes_supply")
    if not supply_raw:
        supply_raw = require_non_empty("add_supply_revenue_requirement")
    run["run_includes_supply"] = _parse_bool(supply_raw)

    # Derive run_includes_subclasses from path_tariffs_electric keys
    tariffs_dict = run.get("path_tariffs_electric")
    run["run_includes_subclasses"] = (
        isinstance(tariffs_dict, dict) and len(tariffs_dict) > 1
    )

    run["path_electric_utility_stats"] = get("path_electric_utility_stats")

    path_tou_supply_mc = get_optional("path_tou_supply_mc")
    if path_tou_supply_mc:
        run["path_tou_supply_mc"] = path_tou_supply_mc

    path_bulk_tx_marginal_costs = get_optional("path_bulk_tx_mc")
    # Only include if not empty/blank (formula returns "" for non-NY utilities)
    if path_bulk_tx_marginal_costs and path_bulk_tx_marginal_costs.strip():
        run["path_bulk_tx_mc"] = path_bulk_tx_marginal_costs

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

    elasticity_raw = require_non_empty("elasticity")
    try:
        run["elasticity"] = float(elasticity_raw)
    except ValueError:
        state = str(run["state"]).lower()
        config_dir = get_project_root() / "rate_design" / "hp_rates" / state / "config"
        periods_path = config_dir / elasticity_raw
        if not periods_path.exists():
            raise FileNotFoundError(
                f"Elasticity path {elasticity_raw!r} resolved to "
                f"{periods_path} which does not exist"
            )
        with open(periods_path) as f:
            periods_data = yaml.safe_load(f)

        enabling_tech = get_optional("enabling_tech").strip().lower()
        use_with_tech = enabling_tech not in ("false", "no", "0")
        yaml_key = "elasticity_with_tech" if use_with_tech else "elasticity"
        if yaml_key not in periods_data:
            raise ValueError(f"No '{yaml_key}' key in {periods_path}")
        run["elasticity"] = periods_data[yaml_key]

    return run


def _cell_to_str(value: object) -> str:
    """Convert sheet cell to string while preserving numeric zeros."""
    if value is None:
        return ""
    return str(value).strip()


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


def run(
    sheet_id: str = DEFAULT_SHEET_ID,
    worksheet_name: str | None = None,
    worksheet_index: int | None = None,
    output_dir: Path | None = None,
    state_filter: str | None = None,
) -> None:
    """Fetch sheet, group by (state, utility), write scenario YAMLs.

    Args:
        sheet_id: Google Spreadsheet ID.
        worksheet_name: Worksheet name (default: first sheet).
        worksheet_index: Worksheet index 0-based (default: first sheet).
        output_dir: Output root directory (default: project root).
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
        if state_filter and state.upper() != state_filter.upper():
            continue

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
            / "hp_rates"
            / state.lower()
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
        "--state",
        type=str,
        default=None,
        help="Two-letter state code to filter (e.g. NY, RI). Omit to generate all.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    run(
        sheet_id=args.sheet_id,
        worksheet_name=args.sheet_name,
        worksheet_index=args.worksheet_index,
        output_dir=args.output_dir,
        state_filter=args.state,
    )


if __name__ == "__main__":
    main()
