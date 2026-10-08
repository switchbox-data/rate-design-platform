# CT Eversource (CL&P) sub-TX + distribution marginal cost: plan of action

**Status: IMPLEMENTED.** Three 8760s can be built for Eversource CT (CL&P) distribution
marginal cost. `create-dist-mc-data` runs the first. The other two stay available in
`generate_utility_tx_dx_mc.py`.

1. **Month-hour expansion (default).** Eversource's OCC-863 Attachment 2 secondary month-hour
   `$/kWh` tables are looked up onto one calendar year. Saturday and Sunday take the
   weekend-and-holiday table. Every other Monday–Friday takes the weekday table, except the
   eleven federal holidays in 5 U.S.C. § 6103 when `include_holidays` is true (the default):
   a Saturday holiday is observed Friday, and a Sunday holiday is observed Monday. `--year` is
   that calendar, and the same year is stamped on `timestamp`, the `year` column, and the Hive
   partition. The recipe passes a `dist_and_sub_tx/` output base; the script rewrites it to
   `dist_and_sub_tx_eversource_derived/`. Detail is in §7. This path does not use the annual
   `$/kW-yr` scalar or a load curve.
2. **`$21.22/kW-yr` on Eversource's substation load.** The scalar is Table 3's `$20.17/kW-yr`
   times the OCC-863 secondary loss factor (§2.2). Probability-of-peak (§3.1) spreads it over
   the top 100 hours of OCC-863 Attachment 3 page 2, the distribution-substation load
   (`system_load.parquet`). It is written to `dist_and_sub_tx_occ863/`. There is no CPI
   adjustment: the CT analysis is rate year 1, in the filing's own 2026 dollars, not the test
   year, so `--target-dollar-year 2026` matches the CSV and the allocator keeps `$21.22`
   (§2.3). The `create-dist-mc-data` recipe does not pass this load or this output base; the
   Python invocation is in §3.4.
3. **ISO-NE Connecticut zone load (oldest, not preferred).** The file that exists at
   `dist_and_sub_tx/utility=ct_eversource/year=2025/` allocated Table 3's `$20.17/kW-yr` after
   CPI deflation onto the 2025 load year (about `$19.65`), ranked on ISO-NE CT zone aggregate
   load. That was the best allocation before the OCC-863 substation load and the loss-adjusted
   scalar were in hand. It is not the same file as a fresh zone-load run: the CSV is now
   `$21.22` with no CPI, so `use_eversource_marginal_cost=false` would allocate `$21.22` on
   that zone load into `dist_and_sub_tx/` and would not reproduce the stored parquet.

Only CT **bulk transmission** MC was implemented before this
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

### 2.2 Recommendation: system-wide diluted figure, grossed up to secondary (`$21.22/kW-yr`)

Table 3's `$20.17/kW-yr` is the cost per kW of upstream peak capability, before line losses.
Residential customers take service at secondary voltage, so a kW measured at the meter requires
more than a kW of that upstream capacity. Eversource's response to OCC-863, Attachment 2, page 1
("Average hourly marginal cost by month, secondary service") applies the gross-up in cell J3:

$$20.17 \times 1.051845136935937 = 21.2157\ldots$$

where `1.051845136935937` is the secondary loss factor in cell N3. Rounded to the cent, that is
**`$21.22/kW-yr`**, and that is the scalar in the config CSV.

The same workbook's page 2 uses the primary loss factor 1.0460400426767955, which gives
$21.10/kW-yr. We use secondary because the BAT population is residential. The choice also
matches MCOS-2 Table 2A: the secondary "Annual Average" row is $1.768/kW-mo, and
$1.768 × 12 = $21.22/kW-yr.

Three reasons for starting from the system-wide diluted base rather than the locational `$86.58`:

1. **It's CL&P's own primary framing.** Table 1 (comparing current rates to marginal cost) and the
   rate-design testimony (MCOS-1 §IV) both use the system-wide figure as the reference marginal cost
   when arguing current volumetric rates recover far more than marginal cost. Using the same figure
   CL&P uses against itself keeps the intervention grounded in the utility's own numbers (same logic
   as the BGE precedent in `dist_mc_definition_choice.md` §5). The loss gross-up is likewise
   CL&P's own, from the OCC-863 workbook rather than a factor we estimated.
2. **Consistent with the platform's dilution convention.** Diluting by expansion-area load share is
   exactly the mechanic NY's FLIC values use; adopting it keeps CT internally consistent with NY
   rather than introducing a third convention.
3. Carry `$86.58/kW-yr` as a documented **sensitivity/upper bound** (parallel to how BGE's `$203–258`
   E3 figure and NY's undiluted numbers are retained as sensitivities elsewhere). That sensitivity
   stays at the Table 3 locational figure; we do not also loss-adjust it.

### 2.3 Config file

Following the RI/MD pattern (`ri_marginal_costs_2025.csv`, `md_marginal_costs_2025.csv`),
`rate_design/hp_rates/ct/config/marginal_costs/ct_marginal_costs_2025.csv` contains:

```csv
utility,sub_tx_and_dist_mc_kw_yr,dollar_year
ct_eversource,21.22,2026
```

`$21.22` is Table 3's `$20.17` times the OCC-863 secondary loss factor, rounded to the cent (§2.2).

`ct_ui` (United Illuminating) is **not** covered by this MCOS — it needs its own source (see
[Open questions](#4-open-questions--decisions-needed) below). `dollar_year=2026` records that
`$21.22` is the rate year 1 figure, in the filing's own dollars. The CT recipe passes
`--target-dollar-year 2026`, which matches that column, so `generate_utility_tx_dx_mc.py` allocates
`$21.22` as provided. It does not CPI-adjust the scalar onto the load year. RI and MD still inflate
from their `dollar_year` to the run year; this skip is CT-only.

---

## 3. Stage B — hourly allocation via Probability of Peak (PoP)

The default recipe does not run this allocator. It expands the month-hour tables (§7). The
two probability-of-peak 8760s differ in which load picks the top 100 hours. The substation-load
run allocates `$21.22/kW-yr` with no CPI adjustment and writes `dist_and_sub_tx_occ863/`
(§3.4). The stored zone-load file at `dist_and_sub_tx/` allocated the pre-loss `$20.17`
after CPI deflation. `use_eversource_marginal_cost=false` still runs zone-load
probability-of-peak into `dist_and_sub_tx/`, using the current CSV scalar.

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
included in the 17-page exhibit we have). The PoP run uses the substation series from OCC-863
Attachment 3 page 2 (§3.3). CT is summer-peaking (CL&P's testimony confirms ~80% of annual peak
probability falls in July–August, ~20% in June/September, <1% winter) [DocumentCloud p. 20](https://www.documentcloud.org/documents/28540606-exhibit-clp-mcos-1/#document/p20/a2826759).

### 3.3 Load data source

PoP ranks the top 100 hours of Eversource's distribution-substation load: OCC-863 Attachment 3
page 2, stored as `system_load.parquet` (`timestamp`, `load_mw`, 2022–2025). The Python
invocation in §3.4 passes that file as `--path-utility-load`. The `create-dist-mc-data` recipe
does not. `normalize_load_to_cairo_8760` keeps `--load-year`.
The hive reader `--utility-load-s3-base` stays what NY, RI, and MD use. The OCC-863 prefix is one
multi-year file, so it is not a valid value for that flag.

This is the substation-sum series behind CL&P's own probability-of-peak (MCOS-1, p. 14). The
`$21.22/kW-yr` scalar prices upstream station and trunkline capacity, which is added when that
system load peaks. On the 2025 series the top 100 hours are 28 in June, 66 in July, and 6 in August.

Attachment 4 (Rates 1, 5, and 7 summed) is the residential class shape. We do not rank it. The
upstream investment is sized to the substation sum, which includes non-residential load, so the
system series is the cost driver. The rate-class parquets are stored on the same local clock as
`system_load.parquet` (see
[ct_occ863_system_load.md](../../code/data/ct_occ863_system_load.md)). On that clock the two 2025
top-100 lists share 84 hours. The residential class peaks at 17:00 on June 24. The system peaks
at 18:00 on July 29. The rate-class parquets stay available for class-shape work.

The first run ranked ISO-NE Connecticut zone load
(`s3://data.sb/isone/hourly_demand/utilities/utility=ct_eversource/`). That series is
transmission-level load for both CT utilities combined. It is still the bulk-transmission load.
Building it required a `ct_eversource` / `ct_ui` zone-mapping row (zone `CT`, location 4004) and
`aggregate_isone_utility_loads.py` for 2025, because that hive partition did not exist before. It
does not include CL&P's BTM-solar or heat-pump forward adjustments, and neither does the
substation series: OCC-863 says no forecasted hourly system loads are available.

### 3.4 Implementation notes

1. **Added CT to `generate_utility_tx_dx_mc.py`**: `"CT"` is now in the `--state` choices
   (`["NY", "RI", "MD", "CT"]`).
2. **Created the config CSV** (§2.3).
3. **Filled the utility-level load gap** (§3.3) — `ct_eversource`/`ct_ui` zone-mapping rows, ran the
   ISO-NE utility aggregation for 2025, uploaded to S3.
4. **Rate year 1 uses `$21.22` with no CPI adjustment.** An earlier run deflated the pre-loss
   Table 3 scalar from 2026$ to the 2025 load year (`$20.17 → $19.65`, CPI factor 0.9740). The
   same factor on `$21.22` would be about `$20.67`. The CT analysis is rate year 1, so the recipe
   now passes `--target-dollar-year 2026` and the allocator keeps `$21.22`. The earlier run's
   parquet keeps the deflated pre-loss scalar (point 7).
5. **Ran the standard PoP allocation** via the new `just -f ct/Justfile create-dist-mc-data 2025
   --upload` recipe (§3.5):
   ```bash
   uv run python utils/data_prep/marginal_costs/generate_utility_tx_dx_mc.py \
       --state CT --utility ct_eversource --year 2025 \
       --target-dollar-year 2026 \
       --mc-table-path rate_design/hp_rates/ct/config/marginal_costs/ct_marginal_costs_2025.csv \
       --path-utility-load s3://data.sb/switchbox/sources/ct/eversource/docket-26-05-10/occ-863/parquet/system_load.parquet \
       --output-s3-base s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx_occ863/ \
       --upload
   ```
6. **Validated** (against the pre-loss `$20.17` scalar; re-run after the `$21.22` change): the
   built-in `validate_allocation`-equivalent 1-kW-constant-load check passed
   exactly ($19.6460/kW-yr, 0.0000% error). The top-100 PoP hours for 2025 fall entirely in
   June–August (30 in June, 62 in July, 8 in August, 0 elsewhere) — consistent with CL&P's own
   finding of concentrated summer peak-probability, though the specific split differs somewhat from
   CL&P's multi-year-normalized ~80% Jul–Aug / ~20% Jun–Sep because that run used a single year
   (2025) of ISO-NE CT zone load. The recipe now ranks the OCC-863 substation series instead;
   its 2025 top 100 hours are 28 in June, 66 in July, and 6 in August (§3.3).
7. **Output**: same schema as all other states (`timestamp, utility, year, mc_total_per_kwh`).
   There are two CT datasets, so the OCC-863 run does not overwrite the earlier one:

   | Dataset                                                            | Scalar                                | Allocation load                        |
   | ------------------------------------------------------------------ | ------------------------------------- | -------------------------------------- |
   | `s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/`        | `$20.17` deflated to `$19.65` (2025$) | ISO-NE CT zone load, 2025              |
   | `s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx_occ863/` | `$21.22`, rate year 1, no CPI         | OCC-863 Attachment 3 system load, 2025 |

   Both use `utility=ct_eversource/year=2025/data.parquet`. The `_occ863` suffix names the
   source of both changes: the secondary loss factor (Attachment 2) and the system load
   (Attachment 3). It follows the sibling-prefix pattern of `dist_and_sub_tx_load{year}/`, so a
   recursive scan of one dataset never picks up the other's 8760. `create-dist-mc-data` now
   writes the month-hour file under `dist_and_sub_tx_eversource_derived/` (§7), not either of
   these. The zone-load parquet cannot be rebuilt from the current inputs, since the config
   CSV and the preferred load both changed. Point a scenario's `path_dist_and_sub_tx_mc` at
   whichever of the three 8760s that run should read.
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

### 3.6 Secondary loss adjustment, and Table 2A as a cross-check

The annual scalar is grossed up by the secondary loss factor before PoP allocation (§2.2), so every
allocated hour carries the same gross-up. We still do not use Table 2A's `$/kWh` rates as the hourly
signal. PoP spreads the annual scalar across the top load hours.

Table 2A remains the seasonal cross-check, and it is also where the primary/secondary split shows up
as a rate. The system-wide annual average is `$0.00241/kWh` primary vs `$0.00242/kWh` secondary
[DocumentCloud p. 5](https://www.documentcloud.org/documents/28540599-exhibit-clp-mcos-2/#document/p5/a2826760).
The secondary "Annual Average" `$1.768/kW-mo` row is the same `$21.22/kW-yr` we store
(`$1.768 × 12`). Our PoP-allocated 8760 should still produce the seasonal concentration Table 2A
shows (nearly all cost in summer, near-zero winter). The hours that concentration lands on come from
the OCC-863 substation series (§3.3), not from Table 2A's own probability-of-peak weights.

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
- **`--n-hours` parameter.** Platform default of `100`. On the 2025 OCC-863 substation series the
  top 100 hours are 28 in June, 66 in July, and 6 in August (§3.3). That is close to, but not
  the same as, CL&P's normalized ~80% Jul–Aug / ~20% Jun–Sep testimony finding, which pools
  2022–2025. Not tuned further; revisit if a closer match to CL&P's own split matters for a given
  analysis.
- **Whether "marginal customer/facilities cost" should ever get its own BAT MC term.** Not a
  CT-specific question, but CT's MCOS happens to compute Bonbright-style customer/facilities marginal
  costs explicitly (Tables 4–7), which makes the gap visible. Worth raising with the team as a
  cross-cutting design question (see §1).
- **No CPI adjustment on the CT scalar.** Rate year 1 uses `$21.22/kW-yr` as filed (§2.3, §3.4
  point 4). A future FRED update to the partial-year 2026 CPI average does not change this figure.

---

## 5. Concrete task list

1. ~~Add `"CT"` to `--state` choices in `generate_utility_tx_dx_mc.py`.~~ Done.
2. ~~Create `rate_design/hp_rates/ct/config/marginal_costs/ct_marginal_costs_2025.csv` (§2.3).~~ Done.
3. ~~Wire a `just` recipe in `rate_design/hp_rates/ct/Justfile` (`create-dist-mc-data`).~~ Done.
4. ~~Run PoP allocation and validate (§3.4).~~ Done for the earlier zone-load run, at
   `s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/utility=ct_eversource/year=2025/data.parquet`.
5. ~~Add tests (8760-hour coverage, annual reconciliation, seasonal concentration check).~~ Done —
   `tests/test_ct_dist_mc.py`.
6. ~~Update this doc and [dist_mc_definition_choice.md](dist_mc_definition_choice.md) §2–3, adding CT
   to the source-number and per-state tables.~~ Done.
7. ~~Write the substation-load 8760 to `dist_and_sub_tx_occ863/` without replacing the zone-load
   file at `dist_and_sub_tx/`.~~ Done (§3.4).
8. ~~Point a CT scenario's `path_dist_and_sub_tx_mc` at a built 8760.~~ Scenario configs exist.
   Which of the three files a run reads is that path.

---

## 6. OCC-863 source files

OCC-863 (Docket 26-05-10, filed October 6, 2026) asked for three historical years and three
forecast years of: (a) average hourly system load by month, (b) average hourly marginal cost by
month, (c) maximum monthly system peak, and (d) an 8760 for the residential class. Eversource
answered with four workbooks. Subparts (a) and (c) are historical only: both are built from hourly
distribution-substation loads for the last three years, and the response says no forecasted hourly
system loads are available. Subpart (b) is the system-wide marginal distribution substation and
trunkline cost per kW, assigned to each hour type within the month. That workbook is also where the
secondary loss factor behind the `$21.22/kW-yr` scalar comes from (§2.2).

The raw workbooks and the discovery-request PDF live at
`s3://data.sb/switchbox/sources/ct/eversource/docket-26-05-10/occ-863/`, mirrored locally at
`/ebs/data/switchbox/sources/ct/eversource/docket-26-05-10/occ-863/`.

The hourly series we keep is Attachment 3 page 2, not Attachment 1. Page 2 is the substation sum
for 2022–2025. Attachment 1 covers 2023–2025 and, on the three fall-back Sundays, stores twice the
page 2 value at 1:00 a.m. Page 1's monthly "Max MW" matches page 2, including those Novembers.
`parquet/system_load.parquet` (`timestamp`, `load_mw`) is page 2. `parquet/system_monthly_peak_mw.parquet`
(`year`, `month`, `peak_mw`) is page 1. 2024 includes February 29 (8784 hours). Attachment 4, the
2025 residential class 8760, is `parquet/rate_1_load.parquet`, `rate_5_load.parquet`, and
`rate_7_load.parquet` (`timestamp`, `load_kw`, `load_mw`). Interval-ending stamps are shifted
back one hour, then mapped from Eastern Standard Time onto local wall-clock time.

The §3.4 invocation reads `system_load.parquet` through `--path-utility-load` and keeps the
load year inside `normalize_load_to_cairo_8760` (§3.3). The rate-class files are not the PoP
load. `--utility-load-s3-base` still expects the hive `utility`/`year` layout the other states
use. How to rebuild the parquets:
[ct_occ863_system_load.md](../../code/data/ct_occ863_system_load.md).

---

## 7. OCC-863 Attachment 2 month-hour 8760 (current default)

`create-dist-mc-data` defaults to `--use-eversource-marginal-cost true`. It does not
probability-of-peak allocate the `$21.22/kW-yr` scalar. It looks up each hour of one calendar year in Eversource's
OCC-863 Attachment 2 secondary month-hour `$/kWh` tables (Docket 26-05-10) and writes an 8760
in the same schema as the PoP file (`timestamp`, `utility`, `year`, `mc_total_per_kwh`).

The tables live at
`s3://data.sb/switchbox/sources/ct/eversource/docket-26-05-10/occ-863/parquet/`:

- `month_hour_marginal_costs_weekday_secondary.parquet`
- `month_hour_marginal_costs_weekend_holidays_secondary.parquet`

Each has 288 rows (`month` 1–12, `hour` 0–23, `num_days`, `mc_total_per_kwh`). The recipe uses
the **secondary** voltage pair. Implementation is `expand_month_hour_mc_to_8760` in
`utils/data_prep/marginal_costs/generate_utility_tx_dx_mc.py`.

### 7.1 Day type

Saturday and Sunday take the weekend-and-holiday table. Every other Monday–Friday takes the
weekday table, except federal holidays when holidays are included.

`include_holidays` defaults to `true`. Those holidays are the eleven dates in 5 U.S.C. § 6103.
A fixed-date holiday that falls on Saturday is observed the Friday before, and one that falls
on Sunday is observed the Monday after. With `include_holidays=false`, a holiday on
Monday–Friday stays on the weekday table; Saturday and Sunday are unchanged.

```bash
just -f ct/Justfile create-dist-mc-data 2025 --upload
just -f ct/Justfile include_holidays=false create-dist-mc-data 2025 --upload
INCLUDE_HOLIDAYS=false just s ct create-dist-mc-data 2025 --upload
```

### 7.2 One year for the calendar, the timestamps, and the partition

`--year` (the recipe's `year_arg`) is the only year. It chooses the calendar used to classify
weekdays, weekends, and holidays, and it is stamped on `timestamp`, on the `year` column, and
on the Hive partition `year=YYYY`. There is no separate partition year.

CAIRO does not join this file to the run on the timestamp year. When both series have 8,760
hours, `utils/cairo.py`'s `_align_mc_to_index` copies the marginal-cost values by position onto
the run's hours. Hour 0 of the file lands on January 1 at midnight of the run. For weekends and
holidays to fall on the right days, `--year` has to be the run's calendar. The CT pipeline year
is 2025. A leap year still emits 8,760 rows (December 31 is dropped), but its positions diverge
from a non-leap run after February 28.

### 7.3 Where it is written

With the flag true, the script rewrites an output base that ends in `dist_and_sub_tx/` to
`dist_and_sub_tx_eversource_derived/`. The recipe still passes the `dist_and_sub_tx/` base; the
redirect is what keeps this file off the PoP path.

```text
s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx_eversource_derived/utility=ct_eversource/year=YYYY/data.parquet
/ebs/data/switchbox/marginal_costs/ct/dist_and_sub_tx_eversource_derived/utility=ct_eversource/year=YYYY/data.parquet
```

`use_eversource_marginal_cost=false` leaves the base alone and writes the §3 PoP file:

```text
s3://data.sb/switchbox/marginal_costs/ct/dist_and_sub_tx/utility=ct_eversource/year=YYYY/data.parquet
```

```bash
just -f ct/Justfile use_eversource_marginal_cost=false create-dist-mc-data 2025 --upload
USE_EVERSOURCE_MARGINAL_COST=false just s ct create-dist-mc-data 2025 --upload
```

Tests for the lookup, the observed-holiday rule, the holiday toggle, leap-year length, and the
output-base rewrite are in `tests/test_expand_month_hour_mc.py`.
