# CT Eversource (CL&P) sub-TX + distribution marginal cost: plan of action

**Status: PLAN — not yet implemented.** This document lays out how to derive the `sub_tx_and_dist`
BAT marginal-cost input for Eversource CT (CL&P) from the two MCOS exhibits, and how to allocate it
to an 8760 hourly signal. No config CSV, allocator code, or S3 output exists yet for CT distribution
MC (CT's `rate_design/hp_rates/ct/config/` has no `marginal_costs/` subdirectory today — contrast
with `ny/`, `ri/`, `md/`). Only CT **bulk transmission** MC is implemented so far
([ct_bulk_transmission_marginal_cost.md](ct_bulk_transmission_marginal_cost.md)).

For the underlying LRMC framework and cross-state definition choice, see
[dist_mc_definition_choice.md](dist_mc_definition_choice.md). For the BGE/RI implementation pattern
this plan follows, see [md_bge_dist_mc_methodology.md](md_bge_dist_mc_methodology.md). Source
documents: [`exhibit_clp_mcos_1.md`](../../sources/mcos/exhibit_clp_mcos_1.md) (Nieto testimony,
narrative + methodology) and [`exhibit_clp_mcos_2.md`](../../sources/mcos/exhibit_clp_mcos_2.md)
(BRG workbook, Tables 1–8).

---

## 1. What CL&P's MCOS actually covers (recap)

CL&P's 2026 MCOS (Docket 26-05-10) covers **"upstream distribution"** — bulk + non-bulk distribution
substations and primary trunkline feeders — plus separately, local distribution facilities
(line transformers/secondary, MCOS-2 Table 4/6) and customer/meter costs (Table 7). "Upstream
distribution" is the CT-specific label for what our platform's `sub_tx_and_dist` bucket represents in
other states (CT does not break out a separate sub-transmission voltage tier the way NY does; see the
MCOS-1/README discussion already captured in `exhibit_clp_mcos_1.md`). This plan is scoped to
**only** the upstream distribution station + trunkline component (MCOS-2 Tables 2A/2B/3).

Local facilities and customer costs (Tables 4–7) are **not** implemented as a parallel hourly BAT
marginal-cost input anywhere in the platform today — there is no `local_facilities_mc` /
`customer_mc` loader alongside `load_dist_and_sub_tx_marginal_costs` /
`load_bulk_tx_marginal_costs` in `utils/cairo.py`, and no NY/RI/MD methodology doc treats them that
way either. They land in the **residual** by construction: the BAT's two-part decomposition
(`bat_lrmc_residual_allocation_methodology.md` §1, Eq. 5) prices everything through the hourly
$$MC_h$$

term; anything not in that term falls into $$R$$

algebraically. Customer/facilities cost
doesn't vary hourly (it scales with customer count / design demand, not with $$L_{i,h}$$

), so it has
no slot in an $$MC_h$$

signal. This is not an economic judgment that these costs are
embedded/sunk — CL&P's MCOS-2 computes them as genuine forward-looking marginal costs. It's a
structural consequence of the platform's architecture.

---

## 2. Stage A — the annualized `$/kW-yr` figure

### 2.1 Candidate values (MCOS-2 Table 3, 2026$)

|                                                        | System-wide average (diluted) | Locational (expansion areas only) |
| ------------------------------------------------------ | ----------------------------: | --------------------------------: |
| **Total annualized marginal station + trunkline cost** |            **`$20.17`/kW-yr** |                **`$86.58`/kW-yr** |

- **Locational (`$86.58`)** = the actual per-kW cost of the specific substation/feeder capacity
  additions planned 2026–2031 (the AIC-style figure for the capacity-constrained slice of the
  system).
- **System-wide (`$20.17`)** = the locational figure diluted by the share of total 2031 system peak
  load that sits in areas requiring expansion — **~24% for substations, ~14% for feeders**
  (MCOS-1 testimony). This dilution-by-load-share mechanic is the same structural move as NY's
  diluted FLIC (see [dist_mc_definition_choice.md](dist_mc_definition_choice.md) §1). **CT is
  therefore closer to NY's FLIC convention than to RI/BGE's published avoided-cost-scalar
  convention**, even though the dollar magnitude (`$20.17`) is roughly comparable to RI/BGE.

### 2.2 Recommendation: use the system-wide diluted figure (`$20.17/kW-yr`)

1. **It's CL&P's own primary framing.** Table 1 (comparing current rates to marginal cost) and the
   rate-design testimony (MCOS-1 §IV) both use the system-wide figure as the reference marginal cost
   when arguing current volumetric rates recover far more than marginal cost. Using the same figure
   CL&P uses against itself keeps the intervention grounded in the utility's own numbers (same logic
   as the BGE precedent in `dist_mc_definition_choice.md` §5).
2. **Consistent with the platform's dilution convention.** Diluting by expansion-area load share is
   exactly the mechanic NY's FLIC values use; adopting it keeps CT internally consistent with NY
   rather than introducing a third convention.
3. Carry `$86.58/kW-yr` as a documented **sensitivity/upper bound** (parallel to how BGE's `$203–258`
   E3 figure and NY's undiluted numbers are retained as sensitivities elsewhere).

### 2.3 Config file (to create)

Following the RI/MD pattern (`ri_marginal_costs_2025.csv`, `md_marginal_costs_2025.csv`), create
`rate_design/hp_rates/ct/config/marginal_costs/ct_marginal_costs_2025.csv`:

```csv
utility,sub_tx_and_dist_mc_kw_yr,dollar_year
ct_eversource,20.17,2026
```

`ct_ui` (United Illuminating) is **not** covered by this MCOS — it needs its own source (see
[Open questions](#4-open-questions--decisions-needed) below). The optional `dollar_year=2026` lets
the existing CPI-inflation logic in `generate_utility_tx_dx_mc.py` handle any run year other than
2026 with no code change.

---

## 3. Stage B — hourly allocation via Probability of Peak (PoP)

### 3.1 Method: standard PoP allocation (same as NY, RI, MD)

Use the same Probability of Peak (PoP) allocation method already implemented in
[`generate_utility_tx_dx_mc.py`](../../../utils/data_prep/marginal_costs/generate_utility_tx_dx_mc.py)
and used for NY, RI, and MD. The method:

1. Load hourly utility load data for the target year.
2. Rank all 8760 hours by load (descending).
3. Select the top `K` hours (default `K=100`).
4. Assign each top-`K` hour a weight proportional to its load share within those `K` hours:
   $$w_h = \frac{\text{Load}_h}{\sum_{h' \in \text{top-}K} \text{Load}_{h'}}$$
5. Allocate the annualized `$/kW-yr` cost: $$MC^{\text{dist}}_h = w_h \times \text{MC}_{\text{annual}}$$
6. All non-top-`K` hours get $$MC^{\text{dist}}_h = 0$$
   .
7. Validate: for a flat 1 kW load, $$\sum_h MC^{\text{dist}}_h = \text{MC}_{\text{annual}}$$
   (exact,
   by construction).

This is the standard approach across all states in the platform. It allocates the annual marginal cost
to the hours most likely to drive capacity investment, weighted by load magnitude in those hours.

### 3.2 CL&P's own method (context)

CL&P's MCOS used an hourly probability-of-peak analysis (MCOS-1, p. 12) with:

- Substation-level hourly distribution load (2022–2025), normalized for customer-growth trend
- Forward adjustments for BTM solar and heat-pump adoption through 2031
- Results summarized into TOU-period buckets (MCOS-2 Table 2A/2B)

We don't have CL&P's underlying hourly PoP curve (the MCOS-2 back-up tables, including Back-Up 36
"Probabilities of peak by month and time of day period," were listed in the table of contents but not
included in the 17-page exhibit we have). However, the standard PoP method on CT zone load is a
reasonable proxy — CT is summer-peaking (CL&P's testimony confirms ~80% of annual peak probability
falls in July–August, ~20% in June/September, <1% winter), which our PoP allocator will naturally
reproduce from the load data.

### 3.3 Load data source

Use the CT zone hourly load already on S3 for the CT bulk-TX pipeline:
`s3://data.sb/isone/hourly_demand/zones/`, zone label `CT`. Both `ct_eversource` and `ct_ui` map to
the single `CT` ISO-NE zone via `ISONE_UTILITY_ZONES` in `supply_utils.py`. The existing
`generate_utility_tx_dx_mc.py` loads utility-level data from
`s3://data.sb/isone/hourly_demand/utilities/utility=ct_eversource/...` — this is already wired via
the `--utility-load-s3-base` argument.

**Caveat**: this is CT-zone transmission-level load (both utilities combined), not CL&P's
substation-level distribution load. It doesn't reflect CL&P's BTM-solar/HP-adoption forward
adjustments. This is the same trade-off as in other states where we use zone-level rather than
utility-specific substation data — sufficient for the BAT, and matches the approach used for CT
bulk-TX MC.

### 3.4 Implementation steps

1. **Add CT to `generate_utility_tx_dx_mc.py`**: add `"CT"` to the `--state` choices (currently
   `["NY", "RI", "MD"]`).
2. **Create the config CSV** (§2.3).
3. **Run the standard PoP allocation**:
   ```bash
   uv run python utils/data_prep/marginal_costs/generate_utility_tx_dx_mc.py \
       --state CT --utility ct_eversource --year 2025 \
       --mc-table-path rate_design/hp_rates/ct/config/marginal_costs/ct_marginal_costs_2025.csv \
       --utility-load-s3-base s3://data.sb/isone/hourly_demand/utilities/ \
       --output-s3-base s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/ \
       --upload
   ```
4. **Validate**: the built-in validation check (`validate_allocation`) confirms the flat-1-kW annual
   sum equals the input `$/kW-yr` (exact by construction). Additionally, sanity-check the seasonal
   distribution — top-`K` hours should cluster overwhelmingly in summer (Jun–Sep), consistent with
   CL&P's own finding.
5. **Output**: same schema as all other states (`timestamp, utility, year, mc_total_per_kwh`) at
   `s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/utility=ct_eversource/year=YYYY/data.parquet`.
   Downstream CAIRO wiring is unchanged.
6. **Wire a `just` recipe** in `rate_design/hp_rates/ct/Justfile` (e.g. `create-dist-mc-data`),
   mirroring `create-bulk-tx-mc-data`.
7. **Add tests** mirroring `tests/test_ri_bulk_tx_mc.py` for the CT PoP allocator (8760-hour
   coverage, annual reconciliation, seasonal concentration check).

### 3.5 Primary vs. secondary voltage column (Table 2A reference values)

Table 2A gives near-identical Primary and Secondary `$/kWh` columns (e.g. system-wide annual average
is `$0.00241` primary vs `$0.00242` secondary). The difference is negligible (loss-adjustment). Since
we're using the PoP method with the `$20.17/kW-yr` Table 3 figure directly (not the Table 2A `$/kWh`
rates), the primary/secondary distinction doesn't affect the implementation. Table 2A's values remain
useful as a cross-check: our PoP-allocated 8760 should produce similar seasonal concentration to
what Table 2A shows (nearly all cost in summer, near-zero winter).

### 3.6 Back-Up 36 (if obtainable)

MCOS-2's table of contents lists **Back-Up 36 "Probabilities of peak by month and time of day
period"** and a **"Monthly Probability of Distribution Peak, System-wide" chart** — these would
contain CL&P's actual hourly allocation weights. They were not included in the 17-page exhibit PDF.
If obtained (via PURA docket portal for Docket 26-05-10, discovery responses, or the native
`CLP_MCOS Exhibit 2.xlsx` workbook), they could serve as a validation benchmark for our PoP
allocation or as a direct replacement. This is a nice-to-have, not a blocker for implementation.

---

## 4. Open questions / decisions needed

- **`ct_ui` (United Illuminating).** This MCOS is CL&P-only. UI's own MCOS/rate-case filing needed
  before UI has a `sub_tx_and_dist` value — flagged as a document gap, same status as the UI revenue
  requirement gap noted in
  [ct_residential_charges_in_bat.md](../bat_mc_residual/ct_residential_charges_in_bat.md).
- **Locational sensitivity.** Confirm whether we want `$86.58/kW-yr` carried as a formal sensitivity
  run (§2.2 point 3) or just documented here.
- **`--n-hours` parameter.** Start with the platform default of `100` and check that the resulting
  seasonal split shows the same ~80% Jul–Aug concentration CL&P reports in the testimony. Adjust if
  needed.
- **Whether "marginal customer/facilities cost" should ever get its own BAT MC term.** Not a
  CT-specific question, but CT's MCOS happens to compute Bonbright-style customer/facilities marginal
  costs explicitly (Tables 4–7), which makes the gap visible. Worth raising with the team as a
  cross-cutting design question (see §1).

---

## 5. Concrete task list

1. Add `"CT"` to `--state` choices in `generate_utility_tx_dx_mc.py`.
2. Create `rate_design/hp_rates/ct/config/marginal_costs/ct_marginal_costs_2025.csv` (§2.3).
3. Wire a `just` recipe in `rate_design/hp_rates/ct/Justfile` (e.g. `create-dist-mc-data`), mirroring
   `create-bulk-tx-mc-data`.
4. Run PoP allocation and validate (§3.4).
5. Add tests (8760-hour coverage, annual reconciliation, seasonal concentration check).
6. Update this doc and [dist_mc_definition_choice.md](dist_mc_definition_choice.md) §2–3 once
   implemented, adding CT to the source-number and per-state tables.
