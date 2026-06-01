"""Pre-run warning checks for the ResStock data pipeline.

Each function checks for a class of argument/file-type mismatches that would
cause a pipeline step to be silently skipped.  All checks print immediately so
the user sees them at the top of the run log, and return the warning messages
so the caller can attach them to the manifest run record.
"""

from __future__ import annotations

from data.resstock.constants import SB_EXCLUDED_FILE_TYPES


def collect_run_warnings(
    *,
    file_types: list[str],
    upgrade_ids: list[str],
    approximate_non_hp_load: bool,
    approx_upgrade: str,
    adjust_mf_electricity: bool,
    mf_adj_upgrades: list[str],
    assign_utility: bool,
    add_monthly_loads: bool,
) -> list[str]:
    """Run all pre-run argument/file-type mismatch checks.

    Prints each warning immediately and returns the full list of warning
    messages so the caller can attach them to the manifest run record.

    Parameters
    ----------
    file_types:
        File types requested via ``--file-types``.
    upgrade_ids:
        Upgrade IDs requested via ``--upgrade-ids``.
    approximate_non_hp_load:
        Whether the non-HP load approximation step is enabled.
    approx_upgrade:
        The upgrade ID that the approximation step targets (from config).
    adjust_mf_electricity:
        Whether the MF electricity adjustment step is enabled.
    mf_adj_upgrades:
        Upgrade IDs targeted by the MF electricity adjustment (from config).
    assign_utility:
        Whether the utility assignment step is enabled.
    add_monthly_loads:
        Whether the monthly load aggregation step is enabled.
    """
    warnings: list[str] = []

    def _warn(msg: str) -> None:
        print(f"WARNING: {msg}", flush=True)
        warnings.append(msg)

    for ft in SB_EXCLUDED_FILE_TYPES:
        if ft in file_types:
            _warn(
                f"'{ft}' will be fetched for the raw release but is NOT copied to "
                f"the _sb release. The _sb release has no post-modification annual "
                f"equivalent; use load_curve_monthly (derived from load_curve_hourly) "
                f"for month-level aggregations of the modified _sb data."
            )

    if approximate_non_hp_load and "load_curve_hourly" not in file_types:
        _warn(
            "--approximate-non-hp-load is enabled but 'load_curve_hourly' is not in "
            "--file-types. The approximation step will be skipped. Add "
            "'load_curve_hourly' to --file-types if you want non-HP load curves approximated."
        )

    if approximate_non_hp_load and approx_upgrade not in [
        u.zfill(2) for u in upgrade_ids
    ]:
        _warn(
            f"--approximate-non-hp-load is enabled but upgrade {approx_upgrade!r} is not "
            f"in --upgrade-ids. The approximation step will be skipped. Add "
            f"upgrade {approx_upgrade!r} to --upgrade-ids if you want non-HP load curves approximated."
        )

    if adjust_mf_electricity and "load_curve_hourly" not in file_types:
        _warn(
            "--adjust-mf-electricity is enabled but 'load_curve_hourly' is not in "
            "--file-types. The MF electricity adjustment step will be skipped. Add "
            "'load_curve_hourly' to --file-types if you want MF non-HVAC electricity adjusted."
        )

    if adjust_mf_electricity and not any(
        u.zfill(2) in mf_adj_upgrades for u in upgrade_ids
    ):
        _warn(
            f"--adjust-mf-electricity is enabled but none of the requested upgrade IDs "
            f"are in mf_adj_upgrade_ids {mf_adj_upgrades}. The MF electricity adjustment "
            f"step will be skipped. Check --upgrade-ids or mf_adj_upgrade_ids in config.yaml."
        )

    if assign_utility and "metadata" not in file_types:
        _warn(
            "--assign-utility is enabled but 'metadata' is not in --file-types. "
            "The utility assignment step will be skipped. Add 'metadata' to "
            "--file-types if you want utilities assigned."
        )

    if add_monthly_loads and "load_curve_hourly" not in file_types:
        _warn(
            "--add-monthly-loads is enabled but 'load_curve_hourly' is not in "
            "--file-types. The monthly aggregation step will be skipped. Add "
            "'load_curve_hourly' to --file-types if you want monthly load curves generated."
        )

    return warnings
