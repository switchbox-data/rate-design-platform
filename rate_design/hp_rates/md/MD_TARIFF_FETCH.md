# MD Electric and Gas Tariff Fetch

Reference document for electric and gas tariff data fetched for Maryland utilities.
Covers the fetch scripts, source data, rate classification decisions, and per-utility
findings for all 10 electric and 8 gas LDCs in scope. See
`utils/pre/rev_requirement/fetch_electric_tariffs_genability.py` and
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

## Electric tariffs (Genability/Arcadia API) — completed 2026-06-24

**Source:** Arcadia/Genability API (`ARCADIA_APP_ID`, `ARCADIA_APP_KEY`)

**Output files per utility (two URDB JSONs + one snapshot JSON):**

- `config/tariffs/electric/{std_name}_default.json` — delivery-only URDB (DISTRIBUTION + TRANSMISSION + OTHER)
- `config/tariffs/electric/{std_name}_default_supply.json` — delivery + supply URDB (adds SUPPLY + CONTRACTED)
- `config/rev_requirement/top-ups/default_tariffs/{std_name}_default_2025-01-01.json` — raw Genability snapshot (not URDB; reference for revenue-requirement pipeline)

All 10 electric utilities converted successfully. All 20 URDB files are present in
`config/tariffs/electric/`. All 10 Genability snapshots are in `top-ups/default_tariffs/`.

### Fetch config

`config/rev_requirement/top-ups/tariffs_by_utility.yaml` contains all 10 utilities and
their Genability tariff IDs (populated automatically by the fetch script).

```bash
# From rate_design/hp_rates/
just -f md/Justfile fetch-default-electric-tariffs
```

This runs `fetch_electric_tariffs_genability.py` with `--urdb`, writing:

- Snapshot → `top-ups/default_tariffs/`
- URDB delivery + supply → `config/tariffs/electric/`

### Delivery vs. supply split

For electric tariffs, CAIRO requires separate files:

- `*_default.json` — used in the **delivery run** (covers delivery costs only: DISTRIBUTION + TRANSMISSION + OTHER charge classes)
- `*_default_supply.json` — used in the **delivery+supply run** (all charges including SUPPLY + CONTRACTED)

The Genability filter maps `chargeClass` to one of these two output files. A charge appears
in the supply file if it is classified `SUPPLY` or `CONTRACTED`; all other charges go into
the delivery-only file (and are also present in the supply file).

### Genability bug patches

Four utilities (BGE, Pepco, DPL, Hagerstown) were affected by bugs in `tariff_fetch`. All
three bugs are patched via monkey-patches in `fetch_electric_tariffs_genability.py`. See
`context/code/data/tariff_rates_and_genability.md` for full details.

**BGE — empty band list from BOOLEAN applicability filtering:**
BGE has a "Competitive Billing" FIXED_PRICE rate whose single band carries
`applicabilityValue=true`. When the non-interactive resolver sets `competitiveBilling=False`,
`rate_filter_bands` skips the band and returns `[]`. The original code then checks
`"COST_PER_UNIT" not in set()` — always true — and raises a misleading error. Fix: return `0.0`
when `rate_filter_bands` returns an empty list.

**Pepco / DPL — `calculation_factor` on GRT and DSIC charges:**
Arcadia models percentage-based regulatory charges (Pepco's 2.04% DC Gross Receipts Tax and
DPL's 4.23% Distribution System Improvement Charge) as companion rates where `rateAmount` is
the base being taxed and `calculationFactor` is the percentage multiplier. Fix: patch
`rate_filter_bands` to fold the factor into `rateAmount` before band processing.

**BGE / Pepco / DPL / Hagerstown — duplicate rate from inline + rider traversal:**
Arcadia inlines certain rider rates into the parent tariff AND keeps the rider pointer. Without
deduplication, `tariff_iter_rates_for_dt` yields the same `tariff_rate_id` twice (once from the
inline entry, once from the rider). Fix: deduplicate by `tariff_rate_id` during iteration. This
corrected BGE's fixed charge from `$10.29` → `$9.97` (Universal Service Charge was counted
twice at `$0.32` each).

### Small-utility availability (all confirmed)

All four small utilities (`somerset_rec`, `berlin_muni`, `hagerstown_muni`, `easton_muni`)
were found in Arcadia and converted successfully.

---

## Gas tariffs (RateAcuity web scrape) — completed 2026-06-27

**Source:** RateAcuity web scrape (`RATEACUITY_USERNAME`, `RATEACUITY_PASSWORD`)

### Delivery and supply in MD gas tariffs

**Most MD gas tariffs are fully bundled (delivery + supply + riders).** BGE, Columbia Gas,
Washington Gas, and Chesapeake all produce correct fully-loaded $/kWh values in the JSON.
The RateAcuity CSV rows include non-fuel energy charges, gas supply service rates (commodity
pass-through), franchise taxes, GSRs, and other riders — summed to a single $/therm value
per month. The `fetch_gas_tariffs_rateacuity.py` script converts per-therm sums to $/kWh
(÷ 29.3001) for CAIRO compatibility.

**UGI and Easton are distribution-only; commodity charges must be added separately.** Their
RateAcuity CSVs only capture the base distribution tariff — the gas commodity pass-through
is filed separately as a monthly adjustment (PAPUC Rider B for UGI; PSC Md. Purchased Gas
Adjustment for Easton) and is not in RateAcuity. The distribution-only URDB JSON rates
significantly understate total gas cost:

| Utility      | JSON rate (distribution only) | Commodity component | Fully loaded (est.) |
| ------------ | ----------------------------- | ------------------- | ------------------- |
| UGI (Tier 1) | `$0.025/kWh`                  | ~`$0.023/kWh` avg   | ~`$0.048/kWh`       |
| Easton       | `$0.020/kWh`                  | ~`$0.032/kWh` avg   | ~`$0.052/kWh`       |

Commodity rates for both utilities are stored in separate CSVs (see below) and must be
applied to the URDB JSONs before CAIRO runs.

### Output files

All URDB JSONs are present in `config/tariffs/gas/`:

| File                                    | Utility              | Notes                                                            |
| --------------------------------------- | -------------------- | ---------------------------------------------------------------- |
| `bge_residential.json`                  | BGE                  | Single rate class; bundled delivery + supply                     |
| `columbia_gas_md_residential.json`      | Columbia Gas MD      | Single rate class; bundled delivery + supply                     |
| `easton_muni_residential.json`          | Easton Utilities     | **Distribution-only**; supply in `easton_muni_pgc.csv`           |
| `ugi_central_penn_residential.json`     | UGI (Emmitsburg)     | **Distribution-only**; supply in `ugi_central_penn_pgc.csv`      |
| `washington_gas_heating.json`           | Washington Gas       | Heating/cooling sub-class; bundled delivery + supply             |
| `washington_gas_nonheating.json`        | Washington Gas       | Non-heating sub-class; bundled delivery + supply                 |
| `chesapeake_main_res1.json`             | Chesapeake Utilities | Main territory, RES-1 (≤150 therms/yr); bundled                  |
| `chesapeake_main_res2.json`             | Chesapeake Utilities | Main territory, RES-2 (>150 therms/yr); bundled                  |
| `chesapeake_cecil_res1.json`            | Chesapeake (Cecil)   | Cecil Co, RES-1; levelized Phase-3 NFEC; bundled                 |
| `chesapeake_cecil_res2.json`            | Chesapeake (Cecil)   | Cecil Co, RES-2; levelized Phase-3 NFEC; bundled                 |
| `chesapeake_worcester_res1.json`        | Chesapeake (Worcs.)  | Worcester Co, RES-1; includes SIR; bundled                       |
| `chesapeake_worcester_res2.json`        | Chesapeake (Worcs.)  | Worcester Co, RES-2; includes SIR; bundled                       |
| `chesapeake_utilities_res1.csv`         | Chesapeake Utilities | Phase 1 raw CSV (`--year 2025`); do not use JSON directly        |
| `chesapeake_utilities_res2.csv`         | Chesapeake Utilities | Phase 1 raw CSV; do not use JSON directly                        |
| `chesapeake_utilities_res1_phase2.csv`  | Chesapeake Utilities | Phase 2 raw CSV (`--year 2027`); used for Cecil levelization     |
| `chesapeake_utilities_res2_phase2.csv`  | Chesapeake Utilities | Phase 2 raw CSV; used for Cecil levelization                     |
| `chesapeake_utilities_res1.json`        | Chesapeake Utilities | Blended (all-county avg); artifact of standard fetch; **unused** |
| `chesapeake_utilities_res2.json`        | Chesapeake Utilities | Blended (all-county avg); artifact of standard fetch; **unused** |
| `chesapeake_utilities_res1_phase2.json` | Chesapeake Utilities | Blended Phase 2; artifact; **unused**                            |
| `chesapeake_utilities_res2_phase2.json` | Chesapeake Utilities | Blended Phase 2; artifact; **unused**                            |
| `ugi_central_penn_pgc.csv`              | UGI (Emmitsburg)     | Monthly commodity rates (Rider B); see UGI section               |
| `easton_muni_pgc.csv`                   | Easton Utilities     | Monthly CGA+ACA rates from PSC filing; see Easton section        |

### Fetch commands

```bash
# Fetch all gas tariffs (all steps in sequence):
just -f md/Justfile fetch-gas-tariffs

# Individual steps (run automatically by fetch-gas-tariffs):
just -f md/Justfile fetch-chesapeake-gas-phase2   # Phase 2 Chesapeake fetch
just -f md/Justfile split-chesapeake-gas-tariffs  # county-split script
just -f md/Justfile fetch-ugi-pgc                 # UGI commodity rates

# UGI PGC with custom date range:
just -f md/Justfile fetch-ugi-pgc 2025-01 2025-12
```

---

## Gas rate classification by utility

### BGE — Single rate class, no sub-classes

**Schedule D** (PSC-filed
[GasScheduleD.pdf](https://azure-na-assets.contentstack.com/v3/assets/blt71bfe6e8a1c2d265/blt968aea18d44b9968/GasScheduleD.pdf)).
Single residential rate for all customers regardless of usage or end use. Bundled delivery +
supply + riders. **No customer segmentation needed.**

### Columbia Gas of Maryland — Single rate class, no sub-classes

**Rate RS** (Columbia Gas
[Maryland tariff PDF](https://www.columbiagasmd.com/docs/librariesprovider13/rates-and-tariffs/maryland-tariff.pdf)).
Single residential schedule with seasonal variation (Apr–Dec vs. Jan–Mar per CSV). Bundled
delivery + supply + system charge. **No customer segmentation needed.**

### Washington Gas — Two sub-classes by end use

**Rate Schedule No. 1** has two permanent sub-classes defined by end use, not consumption:

- **Heating and/or Cooling** (`washington_gas_heating`): gas supplies the **principal space
  heating or cooling** of the dwelling. Bundled rate; higher supply + system charge.
- **Non-Heating and Non-Cooling** (`washington_gas_nonheating`): gas used for other purposes
  only (e.g., cooking, water heating only). Lower bundled rate.

**Key distinction:** Permanent end-use classification, not an annual consumption threshold.
Customers are assigned at signup based on their appliances. This maps directly from ResStock:

- `heats_with_natgas = True` → `washington_gas_heating`
- gas present but `heats_with_natgas = False` (cooking/water heater only) → `washington_gas_nonheating`

Both sub-class files are present and verified.

### Chesapeake Utilities — RES-1 / RES-2 by annual consumption + county split

The consolidated post-merger Chesapeake tariff (PSC Md. No. 1) has two residential classes
based on **annual consumption** reviewed at calendar year-end:

- **RES-1** (≤ 150 therms/year): lower volumetric rate, lower fixed charge (`$8/month`)
- **RES-2** (> 150 therms/year): lower volumetric rate but higher fixed charge (`$10/month`)

The "lower rate, higher fixed charge" pattern at higher usage is consistent with Maryland
rate design practice: high-use customers (predominantly space-heating customers) pay more
in fixed charges to reduce volumetric cross-subsidization.

Additionally, non-fuel energy charges (NFEC) vary by county because Chesapeake acquired
separate utilities in different regulatory dockets, each with their own approved rates. The
consolidated tariff carries **county-specific NFEC columns** in the RateAcuity CSV. The
blended JSON (averaging all counties) is incorrect for any individual county; the six
county-group JSONs are the authoritative tariff files.

**County groups and their former utilities:**

| County group                             | Former utility | RES-1 JSON                       | RES-2 JSON                       |
| ---------------------------------------- | -------------- | -------------------------------- | -------------------------------- |
| Caroline, Dorchester, Somerset, Wicomico | Chesapeake MD  | `chesapeake_main_res1.json`      | `chesapeake_main_res2.json`      |
| Cecil County                             | Elkton Gas     | `chesapeake_cecil_res1.json`     | `chesapeake_cecil_res2.json`     |
| Worcester County                         | Sandpiper      | `chesapeake_worcester_res1.json` | `chesapeake_worcester_res2.json` |

**Per-therm charge components by county group** (all summed, then ÷ 29.3001 for $/kWh):

| Charge                        | Main | Cecil         | Worcester | Notes                          |
| ----------------------------- | ---- | ------------- | --------- | ------------------------------ |
| Non-Fuel Energy Charge (NFEC) | ✓    | ✓ (levelized) | ✓         | Delivery; varies by county     |
| Gas Sales Service Rate (GSR)  | ✓    | ✓             | ✓         | Supply; quarterly commodity    |
| Maryland Franchise Tax Rider  | ✓    | ✓             | ✓         | `$0.00402/therm`, uniform      |
| System Improvement Rate (SIR) | —    | —             | ✓         | Worcester only; non-Ocean City |

### Sandpiper Energy → Chesapeake Worcester County

Sandpiper (Worcester County lower shore) merged into Chesapeake Utilities on April 19, 2025.
Its RateAcuity tariff (effective August 2024) reflects the pre-merger rate structure with two
consumption-based classes (`---87` = RS-1 / lower annual use; `--88-349` = RS-2 / higher
annual use) at thresholds inferred as ~87 ccf/year — substantially different from the
current Chesapeake 150 therms/year threshold.

**Why we use the post-merger Chesapeake tariff for Sandpiper territory:** The analysis
targets 2025 and is forward-oriented. The Sandpiper tariff was last effective August 2024
and the rates have not been updated since the merger; the utility is no longer separately
regulated. Worcester County customers are now on the Chesapeake consolidated tariff
(PSC Md. No. 1, effective 2025), which is the correct rate for 2025 simulations.

The Worcester County JSONs (`chesapeake_worcester_res1.json`, `chesapeake_worcester_res2.json`)
include the Worcester-specific NFEC plus the System Improvement Rate (SIR), which applies
to the former Sandpiper service area (excluding Ocean City).

### Elkton Gas → Chesapeake Cecil County

Elkton Gas (Cecil County) also merged into Chesapeake Utilities on April 19, 2025. Its
RateAcuity tariff (effective February 2019) is similarly stale (single flat rate, no
tiering). Cecil County customers are now on the Chesapeake consolidated tariff with
county-specific NFEC that was phased in under a settlement approved at merger.

**Why we use the post-merger Chesapeake tariff for Elkton territory:** Same rationale as
Sandpiper — the pre-merger tariff is stale by 6 years and no longer in effect. The
consolidated tariff is correct for 2025.

#### Cecil County phased non-fuel energy rates (Phase 1 / Phase 2 / Phase 3)

The consolidated tariff has a three-phase NFEC schedule for Cecil County, reflecting a
rate step-up over time as former Elkton customers converge to the MD-system rate:

| Phase   | Period                       | Duration | RES-1 NFEC | RES-2 NFEC |
| ------- | ---------------------------- | -------- | ---------- | ---------- |
| Phase 1 | 4/19/2025 – 4/18/2026        | 1 year   | `$1.02598` | `$0.52068` |
| Phase 2 | 4/19/2026 – 4/18/2030        | 4 years  | `$1.47362` | `$0.66844` |
| Phase 3 | 4/19/2030 onward (permanent) | —        | `$1.38409` | `$0.63889` |

Phase 3 is the time-weighted average of Phases 1 and 2:
`(1yr × Phase1 + 4yr × Phase2) / 5yr`. This formula is verified against the tariff document.

**Implementation:** `split_chesapeake_gas_tariffs.py` fetches Phase 1 (via `--year 2025`)
and Phase 2 (via `--year 2027`) CSVs from RateAcuity. It extracts the Cecil County NFEC
from each, computes the levelized Phase 3 rate, and applies it as a constant for all 12
months in the Cecil County URDB JSONs. All other charges (GSR, franchise tax) come from the
Phase 1 CSV.

#### Why not interpolate phase values?

For CAIRO simulations (Amy2018 weather year, targeting 2025 rates), a single representative
annual rate is needed. Phase 3 is the correct steady-state rate. Simulating with the Phase 1
transitional rate (effective only April 2025 – April 2026) would understate the long-term
cost. The levelized Phase 3 rate is the standard approach used in regulated utility
modeling when a phase-in is underway.

---

## UGI Central Penn Gas — distribution-only + programmatic PGC fetch

**Rate B.** One rate class with a 5-tier declining-block volumetric structure based on
**monthly consumption**. No fixed customer charge.

| Tier | Monthly usage | Rate          |
| ---- | ------------- | ------------- |
| 1    | 0–8 ccf       | `$0.74/ccf`   |
| 2    | 9–20 ccf      | `$0.3989/ccf` |
| 3    | 21–500 ccf    | `$0.3231/ccf` |
| 4    | 501–1,000 ccf | `$0.3168/ccf` |
| 5    | 1,001+ ccf    | `$0.3104/ccf` |

**No customer segmentation needed** — all customers pay the same tier structure. CAIRO
handles tiered gas rates natively.

### Why UGI is distribution-only

RateAcuity only captured UGI's base distribution tariff (Rate B). The purchased gas
adjustment ("price to compare") is filed quarterly with the Pennsylvania Public Utility
Commission (PAPUC) under Rider B (Purchased Gas Cost), Rider D (Merchant Function), and
Rider E (Gas Procurement), and is not embedded in Rate B. RateAcuity's CSV notes
"purchased gas adjustment applies" without a dollar value. For 2025, the commodity
component is roughly equal to the distribution charge:

| Component    | Source                              | 2025 avg rate     |
| ------------ | ----------------------------------- | ----------------- |
| Distribution | `ugi_central_penn_residential.json` | `~$0.025/kWh`     |
| Commodity    | `ugi_central_penn_pgc.csv`          | `~$0.023/kWh`     |
| **Total**    | —                                   | **`~$0.048/kWh`** |

### Programmatic PGC fetch

UGI's PAPUC-filed "price to compare" is published monthly by a third-party supplier
comparison site ([pennsylvaniaenergy.com/pricing-history](https://www.pennsylvaniaenergy.com/pricing-history/)),
which tracks the full commodity pass-through (Rider B + Rider D + Rider E). The site
publishes a rolling ~24-month window of static HTML.

The fetch is automated via `utils/data_prep/tariffs/fetch_ugi_pgc.py` and the `fetch-ugi-pgc` Justfile
recipe, which runs automatically as the final step of `fetch-gas-tariffs`. The script
merges new rows into any existing CSV content so re-running updates only the requested
window without discarding rows outside it.

```bash
# Default: 2024-01 through current month
just -f md/Justfile fetch-ugi-pgc

# Custom date range (YYYY-MM, inclusive):
just -f md/Justfile fetch-ugi-pgc 2025-01 2025-12
```

Requesting dates older than ~24 months (or ahead of the latest published month) logs a
`WARNING` and clips the range to what is available on the site; existing rows in the CSV
outside that window are preserved by the merge. For historical data before the rolling
window, check PAPUC quarterly filings (Docket R-series under Section 1307(f)) or the MD
PSC eDocket (Case No. 9516).

**Remaining work:** A post-processing script (not yet written) must read
`ugi_central_penn_pgc.csv`, compute the annual-average commodity rate for the simulation
year, convert $/ccf → $/kWh (÷ 29.3001), and add it to each tier in the distribution-only
URDB JSON before CAIRO runs.

---

## Easton Utilities — distribution-only + PSC filing (manual fetch)

**Rate R.** Single flat rate: `$0.5791/ccf` distribution + `$12.00/month` customer charge.
**No customer segmentation needed.**

### Why Easton is distribution-only

RateAcuity only captured Easton's base distribution tariff. The gas commodity charge
(Purchased Gas Adjustment) is set monthly by the MD PSC under an annual rate case cycle.
The relevant dockets are:

- **Case No. 9502** — _Continuing Investigation of the Purchased Gas Adjustment Charges of
  The Easton Utilities Commission_ — gas PGA/PGC case.
- **Case No. 9501** — parallel Electric Fuel Cost Adjustment case (not relevant).

Annual hearing cycle: notices go out in September, Easton files Direct Testimony in
January–February, final orders issued in April. Each annual cycle is suffixed with a letter.

### Structure of Easton's commodity charge

| Component                        | What it is                                                               | 2025 range                                    |
| -------------------------------- | ------------------------------------------------------------------------ | --------------------------------------------- |
| **CGA** (Cost of Gas Adjustment) | Monthly prospective pass-through of projected gas supply costs           | `$0.47`–`$1.23`/ccf                           |
| **ACA** (Actual Cost Adjustment) | Annual reconciliation factor (over/under-collection for prior 12 months) | `+$0.032`/ccf in 2025, `+$0.012`/ccf for 2026 |

The CGA is set monthly; the ACA is set annually (effective January 1 based on 12-month
reconciliation through November 30). Total commodity = CGA + ACA.

### Why files must be fetched manually

There is no programmatic API for Easton's PSC filings. The CGA and ACA are extracted from
the **Direct Testimony of Carrie B. Manuel** filed in the relevant annual cycle and
uploaded to the MD PSC Document Management System. The DMS docket index is public at
[webpscxb.pscmaryland.com/DMS/case/9502](https://webpscxb.pscmaryland.com/DMS/case/9502).

The most recent completed cycle is **9502(t)**, Final Order No. 92298 issued 04/17/2026.
The testimony and all exhibit tables from Case 9502(t) are transcribed in
`config/tariffs/gas/easton_muni_pgc_source.md` (the original PDF is not committed — it
exceeds the 600 KB git limit). The rates are also in `config/tariffs/gas/easton_muni_pgc.csv`.

### Coverage and December 2025 gap

`easton_muni_pgc.csv` covers **Dec 2024 – Nov 2025** (13 months from the 9502(t) cycle).
**December 2025 is not available** — it falls in the 9502(u) cycle, which was not yet filed
as of mid-2026. For annual-average purposes, the 2025 Jan–Nov mean total commodity supply
is approximately `$0.93/ccf` (~`$0.032/kWh` at 29.3001 kWh/ccf), compared to the
distribution charge of `$0.020/kWh`.

| Component                | Source                         | Rate              |
| ------------------------ | ------------------------------ | ----------------- |
| Distribution             | `easton_muni_residential.json` | `$0.020/kWh`      |
| Commodity (11-month avg) | `easton_muni_pgc.csv`          | `~$0.032/kWh`     |
| **Total (est.)**         | —                              | **`~$0.052/kWh`** |

**Remaining work:** A post-processing script (not yet written) must read
`easton_muni_pgc.csv`, convert $/ccf → $/kWh (÷ 29.3001), and add the monthly or
annual-average commodity rate to the distribution URDB JSON. For the missing December 2025,
use the Jan–Nov average or estimate from the prior year's December rate as a fallback.

---

## Gas rate verification findings (2026-06-27)

All JSON rates were verified against the raw RateAcuity CSVs and, where available, against
PSC tariff filings.

**BGE** — `$1.76/therm` (`$0.060/kWh`), `$15.85/month` fixed. Verified against PSC
Schedule D. Bundled delivery + supply.

**Columbia Gas MD** — `$1.53/therm` (`$0.052/kWh`), `$16.42/month` fixed (annual average
of charges that change mid-year — correct behavior from `build_urdb`). Bundled delivery +
supply.

**Washington Gas heating** — `$1.06/therm` (`$0.036/kWh`), `$13.78/month` fixed. Bundled
delivery + supply + STRIDE. Washington Gas non-heating: `$1.01/therm`, `$13.12/month`.

**Chesapeake main RES-1** — `$2.08/therm` (`$0.071/kWh`), `$8/month` fixed. Chesapeake
main RES-2: `$1.64/therm`, `$10/month` fixed. Cecil and Worcester county groups differ by
NFEC (see above).

**UGI (Tier 1)** — `$0.74/ccf` (`$0.025/kWh`), no fixed charge. Distribution-only;
commodity not included.

**Easton** — `$0.58/ccf` (`$0.020/kWh`), `$12/month` fixed. Distribution-only; commodity
not included.

---

## Remaining work (not in scope for this PR)

1. **Add commodity charges to UGI and Easton URDB JSONs** — write post-processing script
   to read `ugi_central_penn_pgc.csv` and `easton_muni_pgc.csv`, compute annual-average
   commodity rates, convert $/ccf → $/kWh, and add to tier rates in the distribution-only
   URDB JSONs.

2. **Gas tariff maps** — assign each ResStock building to the correct gas tariff key.
   - Chesapeake territory: map by `sb.gas_utility` → county-group JSON, then split by
     estimated annual consumption (150 therms/yr) for RES-1 vs. RES-2.
   - Washington Gas: split by `heats_with_natgas`.
   - BGE, Columbia Gas, Easton, UGI: single-key (no segmentation needed).

3. **Electric charge decisions and revenue requirements** — classify BGE/Pepco/etc. charges
   → `charge_decisions/` JSONs (revenue-requirement pipeline).

4. **Fetch monthly electric rates** → `monthly_rates/` YAMLs.

5. **Add rate-case delivery revenue requirements**, run `compute-rr`, add period/scenario
   YAMLs, run `all-pre`.

6. **Run MD ResStock utility assignment** (`assign_utility_md.py` already implemented).
