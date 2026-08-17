# CT Eversource (CL&P) sub-TX + distribution marginal cost: plan of action

**Status: IMPLEMENTED.** This document lays out how the `sub_tx_and_dist` BAT marginal-cost input
for Eversource CT (CL&P) is derived from the two MCOS exhibits, and how it is allocated to an 8760
hourly signal. The config CSV, allocator wiring, and S3 output described below exist and have been
run; see §3.4 for the implementation notes (including two upstream data gaps — ISO-NE utility-level
load and 2026 CPI data — that had to be filled first). Only CT **bulk transmission** MC was
implemented before this
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

- **Locational (`$86.58`)** [DocumentCloud p. 7](https://www.documentcloud.org/documents/28540599-exhibit-clp-mcos-2/#document/p7/a2826757)
  = the actual per-kW cost of the specific substation/feeder capacity additions planned
  2026–2031 (the AIC-style figure for the capacity-constrained slice of the system).
- **System-wide (`$20.17`)** [DocumentCloud p. 7](https://www.documentcloud.org/documents/28540599-exhibit-clp-mcos-2/#document/p7/a2826034)
  = the locational figure diluted by the share of total 2031 system peak load that sits in
  areas requiring expansion — **~24% for substations, ~14% for feeders** (MCOS-1 testimony) [DocumentCloud p. 13](https://www.documentcloud.org/documents/28540606-exhibit-clp-mcos-1/#document/p13/a2826758).
  This dilution-by-load-share mechanic is the same structural move as NY's diluted FLIC
  (see [dist_mc_definition_choice.md](dist_mc_definition_choice.md) §1). **CT is therefore
  closer to NY's FLIC convention than to RI/BGE's published avoided-cost-scalar
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

### 2.3 Config file

Following the RI/MD pattern (`ri_marginal_costs_2025.csv`, `md_marginal_costs_2025.csv`),
`rate_design/hp_rates/ct/config/marginal_costs/ct_marginal_costs_2025.csv` contains:

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

CL&P's MCOS used an hourly probability-of-peak analysis (MCOS-1, p. 14)(https://www.documentcloud.org/documents/28540606-exhibit-clp-mcos-1/#document/p14/a2826761) with:

- Substation-level hourly distribution load (2022–2025), normalized for customer-growth trend
- Forward adjustments for BTM solar and heat-pump adoption through 2031
- Results summarized into TOU-period buckets (MCOS-2 Table 2A/2B)

We don't have CL&P's underlying hourly PoP curve (the MCOS-2 back-up tables, including Back-Up 36
"Probabilities of peak by month and time of day period," were listed in the table of contents but not
included in the 17-page exhibit we have). However, the standard PoP method on CT zone load is a
reasonable proxy — CT is summer-peaking (CL&P's testimony confirms ~80% of annual peak probability
falls in July–August, ~20% in June/September, <1% winter) [DocumentCloud p. 20](https://www.documentcloud.org/documents/28540606-exhibit-clp-mcos-1/#document/p20/a2826759), which our PoP allocator will naturally
reproduce from the load data.

### 3.3 Load data source

Use the CT zone hourly load already on S3 for the CT bulk-TX pipeline:
`s3://data.sb/isone/hourly_demand/zones/`, zone label `CT`. Both `ct_eversource` and `ct_ui` map to
the single `CT` ISO-NE zone. `generate_utility_tx_dx_mc.py` loads **utility-level** (not zone-level)
data from `s3://data.sb/isone/hourly_demand/utilities/utility=ct_eversource/...` via the
`--utility-load-s3-base` argument — the same layout NY/RI/MD use.

That utility-level partition **did not exist** before this implementation (only `utility=rie` was
present under `s3://data.sb/isone/hourly_demand/utilities/`); CT bulk-TX reads zone-level data
directly via `load_isone_zone_loads()` and never needed the utility-level aggregation. To fill the
gap:

1. Added `ct_eversource` and `ct_ui` rows (zone `CT`, location 4004) to
   `data/isone/zone_mapping/generate_zone_mapping_csv.py` and regenerated
   `data/isone/zone_mapping/csv/isone_utility_zone_mapping.csv`.
2. Ran `data/isone/hourly_demand/aggregate_isone_utility_loads.py` (via the
   `aggregate-utility-loads` Justfile recipe) for `ct_eversource` and `ct_ui`, year 2025 — a 1:1
   zone→utility relabel (both utilities share the single CT zone, same as RI's `rie`→`RI`).
3. Uploaded the resulting `utility=ct_eversource/year=2025/` and `utility=ct_ui/year=2025/`
   partitions to `s3://data.sb/isone/hourly_demand/utilities/`.

**Caveat**: this is CT-zone transmission-level load (both utilities combined), not CL&P's
substation-level distribution load. It doesn't reflect CL&P's BTM-solar/HP-adoption forward
adjustments. This is the same trade-off as in other states where we use zone-level rather than
utility-specific substation data — sufficient for the BAT, and matches the approach used for CT
bulk-TX MC.

### 3.4 Implementation notes

1. **Added CT to `generate_utility_tx_dx_mc.py`**: `"CT"` is now in the `--state` choices
   (`["NY", "RI", "MD", "CT"]`).
2. **Created the config CSV** (§2.3).
3. **Filled the utility-level load gap** (§3.3) — `ct_eversource`/`ct_ui` zone-mapping rows, ran the
   ISO-NE utility aggregation for 2025, uploaded to S3.
4. **Refreshed CPI data through 2026**: the MCOS-2 Table 3 figure is filed in 2026$
   (`dollar_year=2026` in the config CSV), but `data/fred/cpi/parquet/` only had annual averages
   through 2025. Ran `just -f data/fred/cpi/Justfile fetch-cpi CPIAUCSL 2019 2026` (2026 is a
   partial-year average — 6 months as of this run — since FRED lags by ~1 month) and uploaded.
   `$20.17 → $19.65/kW-yr` in 2025$ (CPI factor 0.9740).
5. **Ran the standard PoP allocation** via the new `just -f ct/Justfile create-dist-mc-data 2025
   --upload` recipe (§3.5):
   ```bash
   uv run python utils/data_prep/marginal_costs/generate_utility_tx_dx_mc.py \
       --state CT --utility ct_eversource --year 2025 \
       --mc-table-path rate_design/hp_rates/ct/config/marginal_costs/ct_marginal_costs_2025.csv \
       --utility-load-s3-base s3://data.sb/isone/hourly_demand/utilities/ \
       --output-s3-base s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/ \
       --upload
   ```
6. **Validated**: the built-in `validate_allocation`-equivalent 1-kW-constant-load check passed
   exactly ($19.6460/kW-yr, 0.0000% error). The top-100 PoP hours for 2025 fall entirely in
   June–August (30 in June, 62 in July, 8 in August, 0 elsewhere) — consistent with CL&P's own
   finding of concentrated summer peak-probability, though the specific split differs somewhat from
   CL&P's multi-year-normalized ~80% Jul–Aug / ~20% Jun–Sep because this uses a single year (2025)
   of CT zone load rather than CL&P's normalized 2022–2025 substation analysis.
7. **Output**: same schema as all other states (`timestamp, utility, year, mc_total_per_kwh`) at
   `s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/utility=ct_eversource/year=2025/data.parquet`.
   Downstream CAIRO wiring is unchanged.
8. **Wired a `just` recipe**, `create-dist-mc-data`, in `rate_design/hp_rates/ct/Justfile`
   (mirrors `create-bulk-tx-mc-data`'s style, but scoped to `ct_eversource` only — see §3.5).
9. **Added tests** in `tests/test_ct_dist_mc.py`: ISO-NE zone-mapping coverage for both CT
   utilities, config-CSV schema/value checks, and an end-to-end PoP allocation on a synthetic
   summer-peaking CT load profile (8760-hour coverage, exact 1-kW annual reconciliation, peak-hour
   seasonal concentration in Jun–Sep with none in Nov–Mar).

### 3.5 Why `create-dist-mc-data` doesn't loop over both CT utilities

Unlike `create-bulk-tx-mc-data-all` (which applies the same AESC PTF value to every ISO-NE utility
in `state.env`'s `UTILITIES` list), the dist+sub-TX MC value is CL&P-specific — `ct_ui` has no row in
`ct_marginal_costs_2025.csv` (see [Open questions](#4-open-questions--decisions-needed)). Looping
over `UTILITIES=ct_eversource,ct_ui` the way the generic `create-dist-and-sub-tx-mc-data-all` shared
recipe does would raise `ValueError: No marginal cost data found for ct_ui`. `create-dist-mc-data`
therefore takes an explicit year argument and always targets `ct_eversource`.

### 3.6 Primary vs. secondary voltage column (Table 2A reference values)

Table 2A gives near-identical Primary and Secondary `$/kWh` columns (e.g. system-wide annual average
is `$0.00241` primary vs `$0.00242` secondary [DocumentCloud p. 5](https://www.documentcloud.org/documents/28540599-exhibit-clp-mcos-2/#document/p5/a2826760)). The difference is negligible (loss-adjustment). Since
we're using the PoP method with the `$20.17/kW-yr` Table 3 figure directly (not the Table 2A `$/kWh`
rates), the primary/secondary distinction doesn't affect the implementation. Table 2A's values remain
useful as a cross-check: our PoP-allocated 8760 should produce similar seasonal concentration to
what Table 2A shows (nearly all cost in summer, near-zero winter).

### 3.7 Back-Up 36 (if obtainable)

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
  [ct_residential_charges_in_bat.md](../bat_mc_residual/ct_residential_charges_in_bat.md). UI's
  ISO-NE zone-mapping and utility-level load data (§3.3) are in place if/when a value is found;
  only the config-CSV row is missing.
- **Locational sensitivity.** Confirm whether we want `$86.58/kW-yr` carried as a formal sensitivity
  run (§2.2 point 3) or just documented here. Not yet run.
- **`--n-hours` parameter.** Ran with the platform default of `100` for 2025: the resulting top-100
  hours fall entirely in June–August (30/62/8 split), directionally consistent with but not an exact
  match to CL&P's normalized ~80% Jul–Aug / ~20% Jun–Sep testimony finding (see §3.4 point 6 for why
  the exact split differs). Not tuned further; revisit if a closer match to CL&P's own split matters
  for a given analysis.
- **Whether "marginal customer/facilities cost" should ever get its own BAT MC term.** Not a
  CT-specific question, but CT's MCOS happens to compute Bonbright-style customer/facilities marginal
  costs explicitly (Tables 4–7), which makes the gap visible. Worth raising with the team as a
  cross-cutting design question (see §1).
- **2026 CPI is a partial-year average.** The CPI inflation factor (§3.4 point 4) uses a 2026 annual
  average computed from only the months FRED had published as of this implementation. As more 2026
  months are published, re-running `just fetch-cpi` will shift the 2026 average slightly, which
  would change the `$20.17 → $19.65` inflated value by a small amount. Not expected to matter
  materially, but worth knowing if the output value changes on a future re-run.

---

## 5. Concrete task list

1. ~~Add `"CT"` to `--state` choices in `generate_utility_tx_dx_mc.py`.~~ Done.
2. ~~Create `rate_design/hp_rates/ct/config/marginal_costs/ct_marginal_costs_2025.csv` (§2.3).~~ Done.
3. ~~Wire a `just` recipe in `rate_design/hp_rates/ct/Justfile` (`create-dist-mc-data`).~~ Done.
4. ~~Run PoP allocation and validate (§3.4).~~ Done — output at
   `s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/utility=ct_eversource/year=2025/data.parquet`.
5. ~~Add tests (8760-hour coverage, annual reconciliation, seasonal concentration check).~~ Done —
   `tests/test_ct_dist_mc.py`.
6. ~~Update this doc and [dist_mc_definition_choice.md](dist_mc_definition_choice.md) §2–3, adding CT
   to the source-number and per-state tables.~~ Done.
7. **Remaining**: wire `path_dist_and_sub_tx_mc` into a CT scenario config (`scenarios_ct_eversource.yaml`
   or equivalent) once CT scenario YAMLs exist, so a CAIRO run actually consumes this MC output. No CT
   scenario configs exist yet in `rate_design/hp_rates/ct/config/scenarios/` — that's a separate,
   larger piece of CT onboarding beyond this MC-generation task.
