# MD Tariff Fetch Plan

Working document for fetching default electric and gas tariffs for all Maryland utilities.
See `utils/pre/rev_requirement/fetch_electric_tariffs_genability.py` and
`utils/pre/fetch_gas_tariffs_rateacuity.py` for the fetch scripts.

---

## MD utilities in scope

### Electric (10 utilities — from HIFLD + utility_codes.py)

| std_name          | Display name             | Type                 | EIA ID | Arcadia availability |
| ----------------- | ------------------------ | -------------------- | ------ | -------------------- |
| `bge`             | Baltimore Gas & Electric | IOU                  | 1167   | Confirmed            |
| `pepco`           | Pepco                    | IOU                  | 15270  | Confirmed            |
| `poted`           | Potomac Edison           | IOU                  | 15263  | Confirmed            |
| `dpl`             | Delmarva Power           | IOU                  | 5027   | Confirmed            |
| `smeco`           | SMECO                    | Co-op                | 17637  | Likely yes           |
| `choptank`        | Choptank Electric        | Co-op                | 3503   | Likely yes           |
| `somerset_rec`    | Somerset REC             | Small co-op          | 40167  | Confirmed            |
| `berlin_muni`     | Town of Berlin           | Municipal            | 1615   | Confirmed            |
| `hagerstown_muni` | Hagerstown Light Dept    | Municipal            | 7908   | Confirmed            |
| `easton_muni`     | Easton Utilities         | Municipal (elec+gas) | 5625   | Confirmed            |

### Gas (8 utilities — from HIFLD + utility_codes.py)

| std_name               | Display name             | Type         | Notes                   |
| ---------------------- | ------------------------ | ------------ | ----------------------- |
| `bge`                  | Baltimore Gas & Electric | IOU          | Largest MD gas LDC      |
| `washington_gas`       | Washington Gas           | IOU          | D.C. metro area         |
| `columbia_gas_md`      | Columbia Gas of Maryland | IOU          | NiSource subsidiary     |
| `chesapeake_utilities` | Chesapeake Utilities     | Mid-size LDC | Eastern MD / Shore      |
| `easton_muni`          | Easton Utilities         | Municipal    | Small muni              |
| `sandpiper`            | Sand-Piper Energy        | Small LDC    | Lower Shore             |
| `elkton_gas`           | Elkton Gas               | Small LDC    | Cecil County            |
| `ugi_central_penn`     | UGI Central Penn Gas     | LDC          | Smaller footprint in MD |

---

## Phase 1: Electric default tariffs (snapshot + URDB) — DONE

**Source:** Arcadia/Genability API (`ARCADIA_APP_ID`, `ARCADIA_APP_KEY`)

**Output files per utility (two URDB JSONs + one snapshot JSON):**

- `config/tariffs/electric/{std_name}_default.json` — delivery-only URDB (DISTRIBUTION + TRANSMISSION + OTHER)
- `config/tariffs/electric/{std_name}_default_supply.json` — delivery + supply URDB (adds SUPPLY + CONTRACTED)
- `config/rev_requirement/top-ups/default_tariffs/{std_name}_default_2025-01-01.json` — raw Genability snapshot (not URDB; reference for revenue-requirement pipeline)

All 10 electric utilities converted successfully on 2026-06-24. All 20 URDB files are present in
`config/tariffs/electric/`. All 10 Genability snapshots are in `top-ups/default_tariffs/`.

### Step 1.1 — Config (done)

`config/rev_requirement/top-ups/tariffs_by_utility.yaml` created with all 10 utilities.

### Step 1.2 — Run fetch (done)

```bash
# From rate_design/hp_rates/
just -f md/Justfile fetch-default-electric-tariffs
```

This runs `fetch_electric_tariffs_genability.py` with `--urdb`, writing:

- Snapshot → `top-ups/default_tariffs/`
- URDB delivery + supply → `config/tariffs/electric/`

#### tariff_fetch library bugs patched

Three utilities (BGE, Pepco, DPL) initially failed URDB conversion due to bugs in the
`tariff_fetch` library. Both bugs are patched via monkey-patches in
`fetch_electric_tariffs_genability.py`. See
`context/code/data/tariff_rates_and_genability.md` for full details.

**BGE — empty band list from BOOLEAN applicability filtering:**
BGE has a "Competitive Billing" FIXED_PRICE rate whose single band carries
`applicabilityValue=true`. When the non-interactive resolver sets `competitiveBilling=False`,
`rate_filter_bands` skips the band and returns `[]`. The original code then checks
`"COST_PER_UNIT" not in set()` — always true — and raises a misleading error. Fix: return `0.0`
when `rate_filter_bands` returns an empty list, because an empty list means the rate simply
doesn't apply to this customer scenario.

**Pepco / DPL — `calculation_factor` on GRT and DSIC charges:**
Arcadia models percentage-based regulatory charges (Pepco's 2.04% DC Gross Receipts Tax and
DPL's 4.23% Distribution System Improvement Charge) as companion rates where `rateAmount` is
the base being taxed and `calculationFactor` is the percentage multiplier. The effective charge
is `rateAmount × calculationFactor`. `tariff_fetch` raises unconditionally on any band with
`calculation_factor`. Fix: patch `rate_filter_bands` to fold the factor into `rateAmount`
before the band is processed, producing a normal flat-rate band at the effective dollar amount.

### Step 1.3 — Small-utility availability (all confirmed)

All four small utilities (`somerset_rec`, `berlin_muni`, `hagerstown_muni`, `easton_muni`) were
found in Arcadia and converted successfully. No null tariff fallback was needed.

### Step 1.4 — Inspect outputs (pending)

Check each `*_default.json` has `energyratestructure` populated. Check `*_default_supply.json`
has both delivery and supply charges. Spot-check BGE's filed rate structure against PSC filings.

---

## Phase 2: Gas default tariffs (URDB from RateAcuity) — DONE

**Source:** RateAcuity web scrape (`RATEACUITY_USERNAME`, `RATEACUITY_PASSWORD`)

**Output files per utility/schedule** (all present in `config/tariffs/gas/`):

| File                                          | Utility              | Notes                                        |
| --------------------------------------------- | -------------------- | -------------------------------------------- |
| `bge_residential.{csv,json}`                  | BGE                  | Single flat rate                             |
| `chesapeake_utilities_res1.{csv,json}`        | Chesapeake Utilities | RES-1 raw CSV + blended JSON (not used)      |
| `chesapeake_utilities_res2.{csv,json}`        | Chesapeake Utilities | RES-2 raw CSV + blended JSON (not used)      |
| `chesapeake_utilities_res1_phase2.{csv,json}` | Chesapeake Utilities | Phase 2 RES-1 (--year 2027, Cecil NFEC only) |
| `chesapeake_utilities_res2_phase2.{csv,json}` | Chesapeake Utilities | Phase 2 RES-2 (--year 2027, Cecil NFEC only) |
| `chesapeake_main_res1.json`                   | Chesapeake Utilities | County-split: Main territory, RES-1          |
| `chesapeake_main_res2.json`                   | Chesapeake Utilities | County-split: Main territory, RES-2          |
| `chesapeake_cecil_res1.json`                  | Chesapeake Utilities | County-split: Cecil Co, RES-1 (levelized)    |
| `chesapeake_cecil_res2.json`                  | Chesapeake Utilities | County-split: Cecil Co, RES-2 (levelized)    |
| `chesapeake_worcester_res1.json`              | Chesapeake Utilities | County-split: Worcester Co, RES-1 (+SIR)     |
| `chesapeake_worcester_res2.json`              | Chesapeake Utilities | County-split: Worcester Co, RES-2 (+SIR)     |
| `columbia_gas_md_residential.{csv,json}`      | Columbia Gas MD      | Single flat rate                             |
| `easton_muni_residential.{csv,json}`          | Easton Utilities     | Single flat rate, per ccf                    |
| `ugi_central_penn_residential.{csv,json}`     | UGI (Emmitsburg)     | Single class, 5-tier declining block         |
| `washington_gas_nonheating.{csv,json}`        | Washington Gas       | Non-heating/non-cooling sub-class            |
| `washington_gas_heating.{csv,json}`           | Washington Gas       | Heating and/or cooling sub-class             |

All steps (utility discovery → schedule discovery → `rateacuity_tariffs.yaml` →
`fetch-gas-tariffs` including Phase 2 fetch and county split) completed 2026-06-27.
All URDB JSONs are present. The six Chesapeake county-specific JSONs are the authoritative
tariff files; the blended `chesapeake_utilities_res{1,2}.json` are artifacts of the
standard fetch and should not be used directly.

### Step 2.1–2.5 — Completed

Steps 2.1 (list utilities), 2.2 (add `rate_acuity_utility_names` to `utility_codes.py`),
2.3 (discover schedule names), 2.4 (create `rateacuity_tariffs.yaml`), and 2.5 (run fetch)
are all done. The `rateacuity_tariffs.yaml` is at `md/config/tariffs/gas/rateacuity_tariffs.yaml`.

### Step 2.6 — Gas rate classification review — DONE (findings below)

All 8 MD gas utilities have been reviewed against their PSC tariff filings to determine whether
they have distinct rate classes or tiered structures that require ResStock building segmentation.
See **§ Gas rate classification findings** below.

---

## Gas rate classification findings (reviewed 2026-06-25)

Each utility was verified against its current PSC tariff filing. Key question: does the utility
have multiple residential rate classes or within-bill tiers that require different treatment of
ResStock buildings?

### BGE — Single flat rate, no sub-classes

**Schedule D** (confirmed from PSC-filed
[GasScheduleD.pdf](https://azure-na-assets.contentstack.com/v3/assets/blt71bfe6e8a1c2d265/blt968aea18d44b9968/GasScheduleD.pdf)).
One rate applies to all residential customers regardless of usage or end use: `$0.8963/therm`
delivery + `$15.85/month` customer charge. **No segmentation needed.**

### Columbia Gas of Maryland — Single flat rate, no sub-classes

**Rate RS** (confirmed from Columbia Gas
[Maryland tariff PDF](https://www.columbiagasmd.com/docs/librariesprovider13/rates-and-tariffs/maryland-tariff.pdf)).
Single residential schedule: `$1.237/therm` distribution + `$16.50/month` system charge. Note
seasonal variation (Apr–Dec vs. Jan–Mar per CSV). **No segmentation needed.**

### Easton Utilities — Single flat rate, no sub-classes

**Rate R.** Simple flat rate: `$0.5791/ccf` + `$12.00/month`. No tiering, no class splits.
**No segmentation needed.**

### Elkton Gas — Single flat rate, no sub-classes (pre-merger tariff)

**Rate R.** Flat rate: `$0.3447/therm` + `$6.00/month` + `$2.00/month` STRIDE. No tiers.
**Merger note:** Elkton Gas merged into Chesapeake Utilities on April 19, 2025. The fetched
RateAcuity data reflects the pre-merger tariff (effective February 2019). Cecil County
buildings are now on Chesapeake's consolidated RES-1/RES-2 tariff. For Amy2018 simulations
(weather year pre-2025), the pre-merger Elkton tariff is likely the more accurate choice. Same
data quality caveat as Chesapeake main territory: winter 2025 (Jan–Apr) is uncovered by the
current tariff's effective date.

### Sandpiper Energy — Two annual-consumption-based rate classes (pre-merger tariff)

Like Chesapeake, Sandpiper had two residential rate classes assigned based on **annual
consumption**:

- **RS-1** (`sandpiper_nonheating`): lower annual consumption class → `$1.779/ccf` delivery +
  `$6.50/month` customer charge
- **RS-2** (`sandpiper_heating`): higher annual consumption class → `$1.573/ccf` delivery +
  `$8.00/month` customer charge

Pattern is the same as Chesapeake: lower volumetric rate but higher fixed charge for the
higher-use class. Classification was annual, reviewed at calendar year-end.

The RateAcuity schedule name strings embed numbers (`---87` for RS-1, `--88-349` for RS-2)
that appear to indicate the annual ccf thresholds, but the old Sandpiper tariff is not
publicly available online to confirm these exact values. Treat the 87/88 ccf figures as
inferred from RateAcuity labels, not primary-source confirmed.

**Merger note:** Sandpiper (Worcester County) also merged into Chesapeake on April 19, 2025.
The RateAcuity data is the old Sandpiper tariff (effective August 2024). The inferred RS-1
threshold (~87 ccf/yr) differs substantially from the current Chesapeake threshold (150
therms/yr).

**Modeling:** For Amy2018 simulations, decide whether to use the old Sandpiper tariff or the
new Chesapeake consolidated tariff. The old tariff is more accurate for weather years before
April 2025, but has the same Jan–Apr gap in the current year. Requires the same ResStock
segmentation approach as Chesapeake main territory: split buildings by estimated annual gas
consumption.

### UGI Utilities (Emmitsburg) — Single rate class, within-bill declining blocks

**Rate B.** One rate class applies to all customers, but bills using a **5-tier declining block**
volumetric structure based on **monthly consumption**. No fixed customer charge.

| Tier | Monthly usage | Rate          |
| ---- | ------------- | ------------- |
| 1    | 0–8 ccf       | `$0.74/ccf`   |
| 2    | 9–20 ccf      | `$0.3989/ccf` |
| 3    | 21–500 ccf    | `$0.3231/ccf` |
| 4    | 501–1,000 ccf | `$0.3168/ccf` |
| 5    | 1,001+ ccf    | `$0.3104/ccf` |

This is a true within-bill declining block — every customer, regardless of appliances or annual
usage, pays the same tiers applied to their monthly consumption. The URDB JSON was built
correctly by RateAcuity with all 5 tiers in `energyratestructure`. CAIRO falls back to its native
engine for tiered gas rates and handles this correctly. **No customer segmentation needed.**

### Washington Gas — Two rate sub-classes based on end use (not consumption)

**Rate Schedule No. 1** has two sub-classes defined in Section 1A of the General Service
Provisions:

- **Heating and/or Cooling** (`washington_gas_heating`): gas supplies the **principal space
  heating or cooling** of the dwelling. Rate: `$0.4621/therm` flat + `$11.85/month` system charge
  - `$1.99/month` STRIDE.
- **Non-Heating and Non-Cooling** (`washington_gas_nonheating`): gas used for other purposes
  only (e.g., cooking, water heating). Rate: `$0.418/therm` flat + `$11.85/month` system charge
  - `$1.30/month` STRIDE.

**Key distinction from Chesapeake/Sandpiper:** This is a **permanent end-use classification**,
not an annual consumption threshold. Customers are assigned at signup based on their appliances;
no annual reclassification occurs. This maps directly and cleanly from ResStock:

- `heats_with_natgas = True` → `washington_gas_heating`
- gas present but `heats_with_natgas = False` (gas cooking / water heater only) → `washington_gas_nonheating`

**No ambiguity issue.** The two sub-classes are already fetched and present in URDB form.

### Chesapeake Utilities — Two annual-consumption-based rate classes

Already investigated in the prior session. See notes above and the tariff filing at
[chpkgas.com](https://www.chpkgas.com/wp-content/uploads/2025/07/Maryland-Consolidated-Tariffs_07.18.2025_ADA_COMPLIANT.pdf).

- **RES-1** (`chesapeake_utilities_res1`): annual consumption ≤ 150 therms/year
- **RES-2** (`chesapeake_utilities_res2`): annual consumption > 150 therms/year

Applies to the entire consolidated territory (Caroline, Dorchester, Somerset, Wicomico, Cecil,
and Worcester counties) with county-specific non-fuel energy charges. The classification is
reviewed at calendar year-end. Cecil County has phased non-fuel energy rates; the split
script computes the levelized (Phase 3) rate from Phase 1 and Phase 2 fetches. See
Step 2.7 for full details.

---

## Step 2.7 — Chesapeake / Sandpiper / Elkton tariff strategy — DECIDED

**Decision (2026-06-26):** Use the post-consolidation Chesapeake tariff for all three former
utilities. The analysis targets 2025 and is forward-oriented, so the current consolidated
rates are the correct ones to use. The pre-merger tariffs (Sandpiper 2015, Elkton 2018,
Chesapeake MD 2006) are stale by many years and no longer in effect.

### Implementation plan

The Chesapeake RES-1 and RES-2 CSVs already contain county-specific rates in the `location`
column, but the RateAcuity → URDB converter averages across all location rows, producing
incorrect blended rates. The fix is a **post-processing script** (not a monkey-patch of the
converter) that splits the already-fetched CSVs by county group.

#### Source data (fetched via `just fetch-gas-tariffs`)

- `chesapeake_utilities_res1.csv` — Phase 1 RES-1 (≤ 150 therms/yr), all counties (`--year 2025`)
- `chesapeake_utilities_res2.csv` — Phase 1 RES-2 (> 150 therms/yr), all counties (`--year 2025`)
- `chesapeake_utilities_res1_phase2.csv` — Phase 2 RES-1, all counties (`--year 2027`)
- `chesapeake_utilities_res2_phase2.csv` — Phase 2 RES-2, all counties (`--year 2027`)

#### Output: 6 county-specific URDB JSONs

| County group                             | Former utility | RES-1 JSON                       | RES-2 JSON                       |
| ---------------------------------------- | -------------- | -------------------------------- | -------------------------------- |
| Caroline, Dorchester, Somerset, Wicomico | Chesapeake MD  | `chesapeake_main_res1.json`      | `chesapeake_main_res2.json`      |
| Cecil County                             | Elkton Gas     | `chesapeake_cecil_res1.json`     | `chesapeake_cecil_res2.json`     |
| Worcester County                         | Sandpiper      | `chesapeake_worcester_res1.json` | `chesapeake_worcester_res2.json` |

#### Steps

1. **DONE — Post-processing script** — `utils/data_prep/tariffs/split_chesapeake_gas_tariffs.py`
   reads the Phase 1 and Phase 2 Chesapeake CSVs, filters rows by `location` column for each
   county group, sums per-therm charges (non-fuel energy + GSR + franchise tax + SIR),
   converts from $/therm to $/kWh (matching the `tariff_fetch` library convention), and
   writes the 6 output JSONs to `config/tariffs/gas/`. For Cecil County, it computes the
   levelized (Phase 3) non-fuel energy charge from Phase 1 and Phase 2 values (see below).

2. **DONE — Phase 2 fetch** — `rateacuity_tariffs_chesapeake_phase2.yaml` fetches the same
   Chesapeake RES-1 and RES-2 schedules with `--year 2027` to capture the Phase 2 non-fuel
   energy charge for Cecil County. The `fetch-chesapeake-gas-phase2` Justfile recipe handles
   this; it is called automatically by `fetch-gas-tariffs`.

3. **DONE — Justfile recipes** — `fetch-gas-tariffs` now runs three steps in sequence:
   (a) fetch all MD gas tariffs (Phase 1, `--year 2025`), (b) fetch Chesapeake Phase 2
   (`--year 2027`), (c) run `split-chesapeake-gas-tariffs`. The split recipe passes both
   Phase 1 and Phase 2 CSV paths to the script.

4. **Update tariff maps (separate PR)** — assign each ResStock building to the correct
   county-group JSON. The `sb.gas_utility` assignment (from HIFLD shapefiles) will produce
   one of `chesapeake_utilities`, `sandpiper`, or `elkton_gas`. Map each to the corresponding
   county-group JSON:
   - `chesapeake_utilities` → `chesapeake_main_res{1,2}`
   - `sandpiper` → `chesapeake_worcester_res{1,2}`
   - `elkton_gas` → `chesapeake_cecil_res{1,2}`
     The RES-1 vs. RES-2 assignment requires estimating each building's annual gas consumption
     from ResStock load data and applying the 150 therms/year threshold.

5. **DONE — Clean up old files** — removed `sandpiper_nonheating.*`, `sandpiper_heating.*`,
   `elkton_gas_residential.*`, `chesapeake_utilities_nonheating.*`,
   `chesapeake_utilities_heating.*`, and `chesapeake_utilities_residential.*`. Updated
   `rateacuity_tariffs.yaml` to use `_res1`/`_res2` keys and removed separate Sandpiper
   and Elkton entries.

#### Cecil County phased non-fuel energy rates

The consolidated tariff (PSC Md. No. 1, Sheets 7.101/7.103) has a phased non-fuel energy
charge schedule for Cecil County (former Elkton Gas territory):

| Phase   | Period                       | Duration | RES-1 NFEC | RES-2 NFEC |
| ------- | ---------------------------- | -------- | ---------- | ---------- |
| Phase 1 | 4/19/2025 – 4/18/2026        | 1 year   | `$1.02598` | `$0.52068` |
| Phase 2 | 4/19/2026 – 4/18/2030        | 4 years  | `$1.47362` | `$0.66844` |
| Phase 3 | 4/19/2030 onward (permanent) | —        | `$1.38409` | `$0.63889` |

Phase 3 is the time-weighted average of Phases 1 and 2:
`(1yr × Phase1 + 4yr × Phase2) / 5yr`. This was verified against the tariff document.

**Implementation:** The script fetches Phase 1 (via `--year 2025`) and Phase 2 (via
`--year 2027`) CSVs from RateAcuity. It extracts the Cecil County non-fuel energy charge
from each, computes the levelized Phase 3 rate, and applies it as a constant for all 12
months in the Cecil County URDB JSONs. All other charges (GSR, franchise tax) come from
the Phase 1 CSV.

#### Per-therm charge components by county group

All six county-specific JSONs include these per-therm charges summed together, then
converted from $/therm to $/kWh (÷ 29.3001):

| Charge                        | Main | Cecil         | Worcester | Notes                               |
| ----------------------------- | ---- | ------------- | --------- | ----------------------------------- |
| Non-Fuel Energy Charge (NFEC) | ✓    | ✓ (levelized) | ✓         | Delivery; varies by county          |
| Gas Sales Service Rate (GSR)  | ✓    | ✓             | ✓         | Supply; varies by quarter           |
| Maryland Franchise Tax Rider  | ✓    | ✓             | ✓         | `$0.00402/therm`, uniform           |
| System Improvement Rate (SIR) | —    | —             | ✓         | Worcester only; non-Ocean City rate |

The customer charge is written separately as `fixedchargefirstmeter` (`$8/month` RES-1,
`$10/month` RES-2).

---

## Phase 3: Validation

### Electric

Check each URDB JSON manually:

- `energyratestructure` — volumetric energy tiers present
- `fixedchargefirstmeter` — fixed charge in `$/month`
- `demandweekdayschedule` / `demandweekendschedule` — present for demand-tariff utilities
- `utility` / `name` — metadata populated

Automated: run `just -f md/Justfile validate-config` once scenarios exist (requires
`scenarios/`, `periods/`, and MC data — deferred to the full pre-run setup phase).

### Gas

Check each URDB JSON:

- `energyratestructure` — monthly gas rates present
- `fixedchargefirstmeter` — customer charge in `$/month`
- Spot-check BGE's filed residential gas rate against PSC records

---

## Remaining work (not in scope here)

After tariffs are fetched and the Chesapeake county-specific JSONs are built (see Step 2.7),
the following remain before CAIRO runs:

1. **Gas tariff maps** — assign each ResStock building to the correct gas tariff key.
   Chesapeake territory: map by `sb.gas_utility` → county-group JSON, then split by
   estimated annual consumption (150 therms/yr) for RES-1 vs. RES-2. Washington Gas:
   split by `heats_with_natgas`. UGI, BGE, Columbia Gas, Easton are single-key.
2. Research and classify BGE/Pepco/etc. charges → `charge_decisions/` JSONs (the revenue-requirement pipeline)
3. Fetch monthly rates → `monthly_rates/` YAMLs
4. Add rate-case delivery revenue requirements
5. Run `compute-rr` → `rev_requirement/bge.yaml` etc.
6. Add `periods/bge.yaml` (winter months, TOU window, elasticity)
7. Add MD row to Runs & Charts Google Sheet → run `create-scenario-yamls`
8. Run `all-pre` to generate tariff maps and derived HP-rate tariff variants
9. Run ResStock utility assignment for MD (already implemented in `assign_utility_md.py`)
