# Fixed-tariff quartets and candidate-tariff subclass RR

How to bill one subgroup on an **already-built** tariff (no redesign) against a
subclass revenue requirement taken from a **candidate-tariff** run, then read
subclass over/underpayment from BAT.

BGE's `hp_rd_vs_default` is the first production use: today's heat-pump
customers (upgrade 00, `has_hp=true`) on posted Schedule RD, everyone else on
class-calibrated R/RL. That is **not** "switch to a heat pump, then move onto
RD."

This is the Prefect path added for that question. Seasonal / flat HP rates still
use `multi_rate_collapsed` + `depends_on` + `derive_tariffs`. This path uses
`depends_on` (a list of prerequisite scenarios) + two prep tasks + `multi_rate_fixed`.

## Why a separate path

`multi_rate_collapsed` derives new HP tariffs (seasonal, flat) from `default`
bills and a BAT-based subclass split. The RD question needs:

1. **Posted** Schedule RD as the HP tariff (CAIRO must not solve RD to class
   RR).
2. A subclass RR whose HP delivery pot is **what HP customers actually paid on
   that posted RD**, not a BAT residual subtraction from R/RL bills.
3. A quartet that **copies** those JSONs, writes the subclass RR YAML, then
   runs CAIRO `precalc` so each subgroup's tariff is scaled to its own target.

An earlier sketch used `default_rd` as an ordinary `single_rate` quartet
(everyone on RD, CAIRO precalc to the **same class RR** as `default`). That
calibrates RD to class revenue, so the HP copy was class-calibrated RD, not
posted RD. `single_rate_uncalibrated` replaced that.

## End-to-end flow (BGE)

Declared in
`rate_design/hp_rates/md/config/scenarios/pipeline_bge.yaml`.

```text
preflight
  │
  ├─ default                          (single_rate)
  │    R/RL, class RR, CAIRO precalc then calibrated
  │
  ├─ default_rd_uncalibrated          (single_rate_uncalibrated)
  │    posted Schedule RD, run_type: default both stages,
  │    large-number RR YAML, no tariff promotion
  │
  └─ hp_rd_vs_default                 (multi_rate_fixed)
       depends_on: [default, default_rd_uncalibrated]
       bat_allocation_scenario: default
       │
       ├─ compute_candidate_tariff_rr_for_fixed
       │    HP delivery RR ← weighted HP annual bills on
       │      default_rd_uncalibrated upgrade-00 delivery
       │    non-HP delivery RR ← class delivery RR − HP
       │    class supply pot (testimony YAML) split by
       │      candidate_tariff_supply_method (BGE: passthrough)
       │
       ├─ prepare_fixed_tariffs
       │    HP     ← copy posted RD   (default_rd_uncalibrated)
       │    non-HP ← copy calibrated R/RL (default)
       │    relabel onto this scenario's stems; do not rewrite rates
       │
       └─ run_quartet
            CAIRO precalc those copies to the subclass targets,
            then calibrated (upgrade 02, large-number RR)
```

One command (must list the three names, or the seasonal/flat quartets run too):

```bash
uv run python -m rate_design.hp_rates.run_pipeline \
  --yaml rate_design/hp_rates/md/config/scenarios/pipeline_bge.yaml \
  --batch md_YYYYMMDD_rd_hp \
  --scenarios default default_rd_uncalibrated hp_rd_vs_default
```

Then master tables:

```bash
cd rate_design/hp_rates
just s md build-all-master-prefect md_YYYYMMDD_rd_hp
```

Read **`hp_rd_vs_default_precalc`** for subclass BAT. Calibrated-segment BAT is
dominated by the large-number RR; use it for bill changes on upgrade 02, not
cross-subsidy. `bill_change_baseline` is `default` / `precalc`.

Completed batch: `md_20260828_rd_hp` under
`/data.sb/switchbox/cairo/outputs/hp_rates/md/`. That batch's on-disk run
dirs still use the old scenario name `default_uncalibrated_rd`; newer runs
use `default_rd_uncalibrated`.

## `single_rate_uncalibrated`

Same four-run quartet shape as `single_rate` (upgrade 00 then 02, delivery +
supply). Differences:

|                  | `single_rate`                   | `single_rate_uncalibrated`   |
| ---------------- | ------------------------------- | ---------------------------- |
| CAIRO `run_type` | precalc then default            | **default both stages**      |
| RR YAML          | class RR, then large-number     | **large-number both stages** |
| Tariff files     | posted then `*_calibrated.json` | **posted JSON both stages**  |
| Promotion seam   | writes `*_calibrated.json`      | **skipped**                  |

`run_type` stays the CAIRO binary `precalc` | `default`. "Uncalibrated" lives
on the quartet kind, posted JSON, and large-number RR — not a third YAML
`run_type`.

BGE: `tariff_base: rd_default` → `bge_rd_default.json` /
`bge_rd_default_supply.json`.

**Check that CAIRO did not solve rates:** compare posted
`bge_rd_default.json` energy rates to
`tariff_final_config.json` → `bge_rd_default` → `ur_ec_tou_mat` column 4 in
each of the four `default_rd_uncalibrated` output dirs (via `.runs/*.path`).
They must match exactly, including the stage named `calibrated`.

Do **not** use `bge_rd_default_calibrated.json` for that check. Promotion is
skipped, so that file is leftover from the old class-calibrated `default_rd`
quartet if it still exists on disk.

Do **not** compare `bge_hp_base_candidate_tariff_candidate_tariff.json` to
`…_calibrated.json`. The first is the posted-RD copy (prep). The second is
CAIRO precalc of `hp_rd_vs_default` to the HP subclass target; those rates
**should** differ.

`copy_from: default_rd_uncalibrated` copies the **posted** stem
(`calibrated=False`). `copy_from: default` copies `*_calibrated.json`.

## Candidate-tariff subclass RR

Implemented in `utils/mid/compute_subclass_rr.py`
(`compute_candidate_tariff_subclass_rr`) and called from
`compute_candidate_tariff_rr_for_fixed` in `run_pipeline.py`.

This is **not** the BAT-subtraction formula in
[`subclass_revenue_requirement_utility.md`](subclass_revenue_requirement_utility.md)
(`sum(bills) − sum(BAT_* )`).

**Delivery** (always from the candidate-tariff **upgrade-00 delivery** run):

- HP = weighted sum of HP annual electric target bills
- non-HP = class delivery RR (testimony YAML) − HP

**Supply** is the class supply pot already in that YAML
(`total_delivery_and_supply_rr − total_delivery_rr`), split by
`candidate_tariff_supply_method`:

| Method              | Split using                                                                                                                                                                        |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `passthrough` (BGE) | Each building's share of **actual supply bills on the candidate tariff** (delivery+supply run minus delivery-only run). Must come from `candidate_tariff_scenario`, not `default`. |
| `percustomer`       | Sample weights on `default` (or another allocation run with the same buildings)                                                                                                    |
| `volumetric`        | `customer_level_residual_share_volumetric` from that allocation run's BAT CSV                                                                                                      |
| `epmc`              | `customer_level_economic_burden` from that allocation run                                                                                                                          |

Subclass targets add back to the class pot. The written YAML is the usual
multi-rate path (`rev_requirement/{utility}_{alias1}_vs_{alias2}.yaml`).

## `multi_rate_fixed`

CAIRO still **precalcs** the copied tariffs to those subclass targets. "Fixed"
means **no structure derivation** (no new seasonal/flat/TOU JSON), not "leave
cents/kWh unchanged."

Uses `depends_on` (a list of prerequisite scenarios). Prep is two tasks so `run_quartet`
does not invent RR or rewrite rates:

1. `compute_candidate_tariff_rr_for_fixed`
2. `prepare_fixed_tariffs` (relabel-copy only)

`residual_allocation: {delivery: candidate_tariff, supply: candidate_tariff}`
is the method **name** in YAML and in tariff stems. For BGE both slots are
`candidate_tariff`, which is why stems look like
`bge_hp_base_candidate_tariff_candidate_tariff`.

Calibrated stage of a collapsed/fixed quartet still promotes the `promote:`
subgroup's solved tariff for upgrade 02 (large-number RR), same as other
multi-rate kinds.

### Inputs: copy from scenarios, or manual paths

Each subgroup must use **exactly one** tariff source:

| Field                                          | Meaning                                                                                                                                           |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `copy_from: <scenario>`                        | Copy that scenario's tariff (`*_calibrated.json` unless the source is `single_rate_uncalibrated`, then posted). Scenario must be in `depends_on`. |
| `tariff_json_path` + `tariff_json_supply_path` | Use those JSON files verbatim (paths relative to the state config dir, or absolute). No scenario dependency for that subgroup.                    |

Subclass RR must use **exactly one** of:

| Field                           | Meaning                                                                            |
| ------------------------------- | ---------------------------------------------------------------------------------- |
| `candidate_tariff_scenario`     | Derive RR from that required scenario's upgrade-00 bills. Must be in `depends_on`. |
| `candidate_tariff_rr_yaml_path` | Copy a pre-computed subclass RR YAML to the canonical destination. No derivation.  |

When the RR is derived, `bat_allocation_scenario` (usually `default`, and also
in `depends_on`) names the scenario whose precalc outputs supply the BAT-based
allocation methods written alongside the candidate-tariff RR, plus the
allocation shares for non-passthrough supply methods. It is required in that
case — the pipeline never infers it from `depends_on` order.

`depends_on` may be omitted only when **both** the RR YAML and **every**
subgroup's tariff JSONs are supplied by path (fully manual feed). The quartet
is still not an "independent" `single_rate` scenario; it always goes through
the fixed prep tasks.

BGE production YAML uses `copy_from` + `candidate_tariff_scenario` (not manual
paths). Manual paths exist so a later run can reuse a frozen RR YAML and/or
hand-edited JSONs without re-running the two default quartets.

Example of a fully manual declaration (illustrative):

```yaml
hp_rd_vs_default:
  quartet: multi_rate_fixed
  candidate_tariff_rr_yaml_path: rev_requirement/bge_hp_vs_non-hp.yaml
  promote: hp
  residual_allocation:
    delivery: candidate_tariff
    supply: candidate_tariff
  subclass_config:
    group_col: has_hp
    subgroups:
      hp:
        values: ["true"]
        structure: base
        tariff_json_path: tariffs/electric/bge_rd_default.json
        tariff_json_supply_path: tariffs/electric/bge_rd_default_supply.json
      non-hp:
        values: ["false"]
        structure: base
        tariff_json_path: tariffs/electric/bge_default_calibrated.json
        tariff_json_supply_path: tariffs/electric/bge_default_supply_calibrated.json
```

## Code map

| Piece                                 | Where                                                        |
| ------------------------------------- | ------------------------------------------------------------ |
| Quartet kinds, YAML validation, stems | `rate_design/hp_rates/pipeline_config.py`                    |
| Prep tasks + `depends_on` dispatch    | `rate_design/hp_rates/run_pipeline.py`                       |
| Candidate-tariff RR math              | `utils/mid/compute_subclass_rr.py`                           |
| BGE wiring                            | `rate_design/hp_rates/md/config/scenarios/pipeline_bge.yaml` |
| Generic Prefect vocabulary            | [`prefect_pipeline.md`](prefect_pipeline.md)                 |
