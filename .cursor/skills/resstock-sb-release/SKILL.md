---
name: resstock-sb-release
description: >-
  Prepare or upload a Switchbox ResStock `_sb` release in
  rate-design-platform. Use when running data/resstock/main.py or the
  run-pipeline recipe, including fetch, clone, metadata, utility assignment,
  non-HP approximation, multifamily adjustment, monthly and annual loads, and
  S3 upload.
---

# ResStock `_sb` release

One invocation of `data/resstock/main.py` builds the release on local EBS and uploads it. Do not build a full release by chaining the legacy per-step recipes, and do not `aws s3 sync` the `_sb` tree between steps. The pipeline writes to EBS and uploads at the end.

Step internals, CLI flags, and constants (`SB_CLONE_EXCLUDED_FILE_TYPES`, column sets, `config.yaml`, `state_configs.yaml`, sample mode, `path_raw` vs `path_sb`) are in [resstock_sb_release_pipeline_main_py.md](../../../context/code/data/resstock_sb_release_pipeline_main_py.md). The legacy Justfile recipes for a partial re-run are in [resstock_data_preparation_run_order.md](../../../context/code/data/resstock_data_preparation_run_order.md). Open a file when a flag, a constant, or one step's internals are the question. Do not open either file to launch.

## Output

```text
/ebs/data/nrel/resstock/<release>/
/ebs/data/nrel/resstock/<release>_sb/
s3://data.sb/nrel/resstock/<release>_sb/
```

Release names come from `data/resstock/config.yaml`. Today they are `res_2024_amy2018_2` and `res_2024_amy2018_2_sb`. The `_sb` tree has `metadata` (`metadata-sb.parquet`), `metadata_utility`, `load_curve_hourly`, `load_curve_monthly`, and a generated `load_curve_annual`. Raw NREL annual is fetched for the unmodified release and is not cloned onto `_sb`.

## Procedure

1. **Launch one command** from the repo root. NY and RI pin upgrades `0` and `2` (the baseline and the heat-pump package) on `resstock-run-pipeline`. Every other state uses `run-pipeline`, which downloads upgrades `0` through `5` from `config.yaml` unless `--upgrade-ids` is passed.

```bash
just -f rate_design/hp_rates/ny/Justfile resstock-run-pipeline
just -f rate_design/hp_rates/ri/Justfile resstock-run-pipeline
just -f data/resstock/Justfile run-pipeline <STATE>
```

NY or RI through the generic recipe still needs the upgrade list: `just -f data/resstock/Justfile run-pipeline NY --upgrade-ids 0 2`.

A production run leaves `--sample` at 0. `--sample N` is a development run: the gas factor and the multifamily ratios are computed on the sample, and the real-world checks are skipped.

2. **Leave the order inside `main.py` alone.**

   1. Fetch the raw release onto EBS.
   2. Clone it to `_sb`. `load_curve_annual` is not cloned.
   3. Metadata on `_sb`: heat-pump customers, heating type, natural-gas connection (annual read from the raw release), and vulnerability columns when that state's config says so.
   4. Utility assignment from upgrade 00, written to `metadata_utility` and uploaded immediately. A state runs only when `state_configs.yaml` has a `utility_assignment` entry. Adding a state is [utility_assignment_resstock.md](../../../context/code/data/utility_assignment_resstock.md).
   5. Non-HP load approximation for `approx_upgrade_ids` (01 and 02 today).
   6. Multifamily non-HVAC electricity adjustment for `mf_adj_upgrade_ids` (00 and 02 today).
   7. One statewide gas scale: EIA-176 residential sales plus transport, divided by weighted upgrade-00 raw gas, applied to the hourly gas of every requested upgrade.
   8. Monthly and annual loads from the modified hourly, then upload those aggregates. How the annual file is built is [resstock_sb_annual_load_curves.md](../../../context/code/data/resstock_sb_annual_load_curves.md).
   9. Upload the raw release and the cloned `_sb` file types. The aggregates are already on S3 from the previous step.
   10. Real-world checks against EIA-861, EIA-176, and ACS PUMS. They warn and do not fail the run. On an existing release: `just -f data/resstock/Justfile check-real-world <STATE>`.

3. **A partial re-run** uses the individual recipes in `data/resstock/Justfile`, or the `resstock-*` wrappers in the NY and RI Justfiles. The run-order doc maps each recipe to a step. A full release is step 1 of this procedure, not that chain.

4. **A non-zero exit is recorded.** The `run-pipeline` recipe marks the manifest `crashed` with the exit code. Without the recipe, `just -f data/resstock/Justfile mark-crashed` does the same. The next run also marks a stale `in_progress` record at startup.

## Done

You can point to the `_sb` prefix for the state on EBS and on S3, and say whether the real-world checks warned.
