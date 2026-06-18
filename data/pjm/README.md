# PJM data

PJM datasets feeding supply-capacity marginal costs for the Bill Alignment Test. The design is **region-first**: the two curated capacity datasets cover all PJM transmission zones; utility specificity lives only in `zone_mapping/`, which filters region-wide data down to one utility. Each sub-product is a self-contained pipeline with its own Justfile and scripts.

Both capacity datasets are **reproducible from committed, reviewable source intermediates**. PJM publishes 5CP only as PDFs and RPM only as per-DY Excel files (with drifting names/paths, no API); these are transcribed once into per-period markdown intermediates under each product's `sources/` directory — carrying the original PJM source URL(s) in a citation header — and a `convert` recipe deterministically rebuilds the CSV from them. The workflow for any update is **fetch source → edit intermediate → `just convert` → review CSV diff → `just validate` → commit → `just upload`**.

For methodology (5CP capacity obligation, RPM pricing, decision rationale for BGE), see [`context/domain/marginal_costs/pjm_supply_capacity_marginal_cost.md`](../../context/domain/marginal_costs/pjm_supply_capacity_marginal_cost.md).

## Delivery-year calendar

PJM's capacity market runs on **delivery years (DY): June 1 – May 31** (e.g. DY 2025/26 = Jun 1, 2025 – May 31, 2026). These datasets store per-DY values; consumers that need calendar-year prices blend two DYs with a 5/7 split (Jan–May from the DY ending that year, Jun–Dec from the DY starting that year).

## Zone nomenclature crosswalk

PJM publishes the same zone under different labels depending on the data product. Both curated CSVs store **canonical labels** (normalized at transcription time); the per-source aliases live in `zone_mapping/`. This table is the source of truth (mirrored in the validate scripts):

| Canonical (`zone`) | Data Miner code | 5CP PDF label | RPM XLS label | Notes                             |
| ------------------ | --------------- | ------------- | ------------- | --------------------------------- |
| AECO               | AE              | AE            | AE            | Atlantic City Electric            |
| AEP                | AEP             | AEP           | AEP           |                                   |
| APS                | AP              | APS           | APS           | Allegheny / Potomac Edison parent |
| ATSI               | ATSI            | ATSI          | ATSI          |                                   |
| BGE                | BC              | BGE           | BGE           | **legacy Data Miner code `BC`**   |
| COMED              | CE              | COMED         | COMED         |                                   |
| DAY                | DAY             | DAYTON        | DAYTON        |                                   |
| DEOK               | DEOK            | DEOK          | DEOK          |                                   |
| DOM                | DOM             | DOM           | DOM           |                                   |
| DPL                | DPL             | DPL           | DPL           |                                   |
| DUQ                | DUQ             | DLCo          | DLCO          | Duquesne Light; two aliases       |
| EKPC               | EKPC            | EKPC          | EKPC          |                                   |
| JCPL               | JC              | JCPL          | JCPL          |                                   |
| METED              | ME              | METED         | METED         |                                   |
| PECO               | PE              | PECO          | PECO          |                                   |
| PENELEC            | PN              | PENLC         | PENLC         |                                   |
| PEPCO              | PEP             | PEPCO         | PEPCO         |                                   |
| PPL                | PL              | PPL-EU        | PL            |                                   |
| PSEG               | PS              | PS            | PS            |                                   |
| RECO               | RECO            | RECO          | RECO          | tiny zone                         |
| UGI                | UGI             | UGI           | (in PPL LDA)  | tiny zone; absent from RPM files  |

Excluded from the curated CSVs: **OVEC** (generation-owning entity, no retail LSE load; appears in 5CP PDFs from 2019 and RPM files from DY 2022/23) and the 5CP PDF sub-zone rows **EASTON** (inside DPL), **SMECO** (inside PEPCO), **Vineland** (inside AECO).

### LDA nesting

RPM Locational Deliverability Areas nest; constrained children clear at or above their parent:

```
RTO
├── MAAC
│   ├── EMAAC ── PSEG ── PS-NORTH;  DPL-S;  JCPL
│   └── SWMAAC ── BGE;  PEPCO
├── COMED
├── ATSI ── ATSI-C
├── DAY, DEOK, DOM, ...
```

The `lda` column in `rpm_capacity_prices.csv` records the most-specific LDA that cleared separately for the zone that DY — membership effects change by DY (e.g. BGE cleared at the RTO price in DY 2018/19 but as a constrained LDA in 2025/26), so it is a row attribute, not a static property.

## Data products

### `capacity/rpm/` — RPM capacity prices

BRA Resource Clearing Prices (per LDA) and Final Zonal Capacity Prices (per zone, IA-inclusive), one row per (delivery_year, zone).

|                    |                                                                                                                                                                                                                                         |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Source**         | Per-DY Excel files on the [PJM RPM page](https://pjm.com/markets-and-operations/rpm.aspx): "Final Zonal UCAP Obligations, Capacity Prices and CTR Credit Rates" (→ `source_url`) + "Base Residual Auction Results" (→ `bra_source_url`) |
| **Format**         | Committed markdown intermediates (`sources/rpm_YYYY_YY.md`) → CSV (`just convert`) → Parquet (`just upload`); no API                                                                                                                    |
| **Coverage**       | DY 2018/19 – 2026/27, 20 zones (no UGI/OVEC, see crosswalk)                                                                                                                                                                             |
| **S3 path**        | `s3://data.sb/pjm/capacity/rpm/data.parquet`                                                                                                                                                                                            |
| **Update cadence** | New BRA ~annually; IA true-ups within a DY                                                                                                                                                                                              |

**How to update**: (1) Download the two Excel files for the new DY (Final Zonal + BRA results). (2) Transcribe into a new `sources/rpm_YYYY_YY.md` (citation header with both URLs + a per-zone `lda / bra / final / notes` table; normalize zone labels to canonical). (3) When adding a new DY, also bump `EXPECTED_DYS` in `validate_rpm_reference.py` (and ideally add a `CROSS_CHECK_VALUES` pin from the new report); otherwise the new DY reports as an unexpected-period WARN. (4) `just convert` to rebuild `rpm_capacity_prices.csv`. (5) Review the `git diff` of the CSV. (6) `just validate`. (7) Commit intermediate + CSV. (8) `just upload`. IA true-up: edit the intermediate in place, bump **Final price as of**, re-convert.

**Reproducibility**: the per-DY markdown intermediates under `sources/` are the **source of record**; `convert_rpm_md_to_csv.py` deterministically rebuilds the CSV from them. The Excel→intermediate step is done once and reviewed (the spreadsheets are binaries with drifting names/paths and are not committed). The LDA-assignment-per-zone editorial judgment lives, reviewable, in the intermediate tables.

**Gotchas**:

- **DY = Jun 1–May 31.** Calendar-year consumers blend 5/7; this dataset just stores per-DY.
- **Two price concepts per row, two citations**: the BRA price is per-LDA at auction time (cited by `bra_source_url`); the Final Zonal price is per-zone after Incremental Auctions and locational adders (cited by `source_url`). They differ whenever IAs cleared or the zone sits in a constrained LDA — and the final price can drop _below_ the system BRA price after IA true-downs (e.g. PPL in 2018/19). Only the Final Zonal price carries a tracked `final_price_as_of` (it drifts as IAs settle); the BRA price's as-of is the fixed BRA posting date implied by the DY, so it has no separate column.
- **LDA membership changes by DY** — see LDA nesting above.
- **Excel formats and URLs vary across DYs** (sheet names, layouts, paths) — hence the committed-intermediate approach. From DY 2023/24, the FZSF-FZCP sheet ships inside the "3IA Results" XLSX instead of a standalone Final Zonal file; BRA file stems also drift (`…-base-residual-auction-results.xlsx`, the older `…-base-residual-auction-results-xls.xls`, the newest `…-bra-results.xlsx`).
- **Sub-LDA blending**: DPL-S (inside the DPL zone) cleared separately in 2023/24 and 2024/25; the zone-level `lda` stays at the containing LDA and the final zonal price blends sub-LDA pricing (noted in `notes`; flagged by the `Final vs BRA` WARN).
- **BRA schedule compression**: recent BRAs run much closer to the DY; expect new DYs to appear with BRA-only prices first, then drift as IAs settle. Git history is the revision trail.

### `capacity/5cp/` — Summer 5CP peaks

The 5 highest non-holiday-weekday RTO unrestricted daily peaks per summer (PJM's capacity-obligation hours), plus zonal unrestricted MW coincident with each peak. One row per (summer_year, rank, zone); `zone == "RTO"` rows are what the MC consumer reads.

|                    |                                                                                                                                                            |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Source**         | Annual "Summer YYYY Peaks and 5CPs" PDF, [PJM Planning → Load Forecast](https://www.pjm.com/planning/resource-adequacy-planning/load-forecast-dev-process) |
| **Format**         | Committed markdown intermediates (`sources/5cp_YYYY.md`) → CSV (`just convert`) → Parquet (`just upload`); no API                                          |
| **Coverage**       | Summers 2021–2025, 5 ranks × (21 zones + RTO) = 550 rows                                                                                                   |
| **S3 path**        | `s3://data.sb/pjm/capacity/5cp/data.parquet`                                                                                                               |
| **Update cadence** | Mid-October posting + possible November revision                                                                                                           |

**How to update**: (1) Download the new PDF. (2) Extract its peak tables to a new `sources/5cp_YYYY.md` intermediate (citation header + an RTO peaks table + a by-zone MW table; normalize zone labels to canonical). Use the repo `extract-pdf-to-markdown` command as a starting point. (3) When adding a new summer, also bump `EXPECTED_SUMMERS` in `validate_fivecp_reference.py` (and ideally add a `CROSS_CHECK_VALUES` pin from the new PDF); otherwise the new summer reports as an unexpected-period WARN. (4) `just convert` to rebuild `fivecp_peaks.csv`. (5) Review the `git diff` of the CSV. (6) `just validate`. (7) Commit intermediate + CSV. (8) `just upload`. For a November revision, edit the intermediate's MW in place, bump its **As of**, re-convert.

**Reproducibility**: the per-summer markdown intermediates under `sources/` are the **source of record**; `convert_5cp_md_to_csv.py` deterministically rebuilds the CSV from them. PJM publishes 5CP only as PDFs and pulls old ones from pjm.com, so the PDF→text step is done once and captured as a committed, reviewable intermediate rather than re-parsed live.

**Gotchas**:

- **Hour-ending EPT**: PDFs report hour-ending (e.g. "HE 17" = 16:00–17:00 EPT), stored as published; the MC consumer converts to hour-beginning to match platform convention. EPT = prevailing Eastern (EDT all summer in practice).
- **November revisions** restate load-drop add-backs ("Revised MM/DD/YYYY"): MW values change, timestamps almost never do. Update the intermediate's MW in place, bump its **As of**; git history is the revision trail.
- **Zone label drift**: PDFs use `DLCo`, `PENLC`, `PPL-EU` (and `PL-EU` in 2025) — normalize to canonical in the intermediate; never store PDF-native labels.
- **Scope**: only summers feeding 2025+ runs are retained (2021–2025). Earlier summers (2018–2020, since removed from pjm.com) were dropped; recover from the Wayback Machine if ever needed.
- **Unrestricted ≠ metered**: these MW include load-drop add-backs and are not exactly reconcilable against Data Miner `hrl_load_metered` (see Future work).
- **`notes` is file-level here**: the 5CP `notes` is a single value per summer (from the intermediate's `**Notes:**` header, may be empty) copied onto every row, unlike RPM where `notes` is per-row in the price table.

### `zone_mapping/` — utility → zone crosswalk

One row per (utility, zone, weight); the single place the per-source zone aliases (`dataminer_zone`, `fivecp_zone_label`, `price_zone`) live. MC scripts use it to filter the region-wide datasets down to one utility.

|                    |                                                                  |
| ------------------ | ---------------------------------------------------------------- |
| **Source**         | Hardcoded rows in `generate_zone_mapping_csv.py` (nyiso pattern) |
| **Format**         | Generated CSV (uploaded as CSV, not parquet)                     |
| **Coverage**       | MD utilities: bge, pepco, dpl, potomac-edison                    |
| **S3 path**        | `s3://data.sb/pjm/zone_mapping/pjm_utility_zone_mapping.csv`     |
| **Update cadence** | When onboarding a utility                                        |

**How to update**: Edit `_MAPPING_ROWS` in `generate_zone_mapping_csv.py`, then `just prepare && just upload`. Validation cross-checks every mapped zone against both capacity CSVs (the single enforcement point of zone-code integrity between the three datasets).

**Registry compatibility**: column names map 1:1 onto the planned cross-ISO utility registry (`utility`, `state`, `dataminer_zone` → `load_zones`, `price_zone`, `capacity_weight`; `iso=pjm` implicit by location). When the registry lands, these rows migrate into it and this pipeline can be retired or become a thin export.

**Gotchas**:

- **Three naming systems, one crosswalk**: Data Miner legacy codes (`BC`, `PEP`, `AP`) vs PDF/XLS labels (`BGE`, `PEPCO`, `APS`) — this CSV is the only place the mapping lives.
- **Zone ≠ retail territory**: the PEPCO and DPL zones span MD + DC/DE; `state` here is the _analysis_ state, customer filtering happens upstream in ResStock utility assignment.
- **`potomac-edison`** is a retail brand inside the APS zone — zone-level data includes WV/PA load.
- **UGI / PPL-folded zones**: UGI has no separate row in the RPM price files (it sits inside the PPL LDA), so a mapping row with `price_zone=UGI` will fail the RPM cross-check. Set `price_zone=PPL` for such utilities (UGI is still valid for `fivecp_zone_label` and `dataminer_zone`, which both carry a UGI row).

## Historical source URLs

The authoritative per-DY URLs (both Final Zonal and BRA) live in the citation header of each `sources/rpm_YYYY_YY.md` and are copied into the CSV `source_url` / `bra_source_url` columns. Listed here for quick reference; all under `https://www.pjm.com/-/media/DotCom/markets-ops/rpm/rpm-auction-info/`.

Final Zonal (→ `source_url`):

| DY      | File                                                                                         |
| ------- | -------------------------------------------------------------------------------------------- |
| 2018/19 | `2018-2019-final-zonal-ucap-obligations-capacity-prices-ctr-credit-rates.xlsx`               |
| 2019/20 | `2019-2020/2019-2020-final-zonal-ucap-obligations-capacity-prices-ctr-credit-rates.xls`      |
| 2020/21 | `2020-2021/2020-2021-final-zonal-ucap-obligations-capacity-prices-ctr-credit-rates.xlsx`     |
| 2021/22 | `2021-2022/2021-2022-final-zonal-ucap-obligations-capacity-prices-and-ctr-credit-rates.xlsx` |
| 2022/23 | `2022-2023/2022-2023-final-zonal-ucap-obligations-capacity-prices-and-ctr-rates.xlsx`        |
| 2023/24 | `2023-2024/2023-2024-3ia-results.xlsx` (FZSF-FZCP sheet)                                     |
| 2024/25 | `2024-2025/2024-2025-3ia-results.xlsx` (FZSF-FZCP sheet)                                     |
| 2025/26 | `2025-2026/2025-2026-3ia-results.xlsx` (FZSF-FZCP sheet)                                     |
| 2026/27 | `2026-2027/2026-2027-3ia-results.xlsx` (FZSF-FZCP sheet)                                     |

BRA Resource Clearing Prices (→ `bra_source_url`):

| DY      | File                                                     |
| ------- | -------------------------------------------------------- |
| 2018/19 | `2018-2019-base-residual-auction-results-xls.xls`        |
| 2019/20 | `2019-2020-base-residual-auction-results-xls.xls`        |
| 2020/21 | `2020-2021-base-residual-auction-results.xlsx`           |
| 2021/22 | `2021-2022/2021-2022-base-residual-auction-results.xlsx` |
| 2022/23 | `2022-2023/2022-2023-base-residual-auction-results.xlsx` |
| 2023/24 | `2023-2024/2023-2024-base-residual-auction-results.xlsx` |
| 2024/25 | `2024-2025/2024-2025-base-residual-auction-results.xlsx` |
| 2025/26 | `2025-2026/2025-2026-base-residual-auction-results.xlsx` |
| 2026/27 | `2026-2027/2026-2027-bra-results.xlsx`                   |

### `capacity/5cp/` (annual PDFs)

Each summer's PDF URL lives in the citation header of `sources/5cp_YYYY.md` and the CSV `source_url`. All retained summers are on live pjm.com:

| Summer    | URL                                                                                                 |
| --------- | --------------------------------------------------------------------------------------------------- |
| 2021–2025 | `https://www.pjm.com/-/media/DotCom/planning/res-adeq/load-forecast/summer-YYYY-peaks-and-5cps.pdf` |

## `hourly_demand/` — zonal + utility hourly loads

Data Miner 2 [`hrl_load_metered`](https://dataminer2.pjm.com/feed/hrl_load_metered/definition) pipeline (`data/pjm/hourly_demand/`): hourly metered load by transmission zone, summed from the feed's `load_area` grain up to the zone, Hive-partitioned `zone={dataminer_zone}/year={YYYY}/data.parquet` at `s3://data.sb/pjm/hourly_demand/zones/`, plus utility-level profiles at `s3://data.sb/pjm/hourly_demand/utilities/`. Zone labels are the legacy `dataminer_zone` codes (BGE = `BC`), matching the mapping CSV's `dataminer_zone` column. Needs `PJM_API_PRIMARY_KEY` in `.env`. Shared Data Miner client (`data/pjm/dataminer.py`) handles the archive/standard 731-day boundary, pagination, and retries. Timestamps are tz-aware `America/New_York` built from `datetime_beginning_utc`; the calendar year is derived from `datetime_beginning_ept`. Zones are a faithful raw mirror (with a `value_flag` column marking bad load-area values, e.g. a load area reporting `0.0` at a DST transition); those flagged hours are interpolated only when building the curated utility profiles, which carry an `interpolated` boolean marking the cleaned hours. See `context/code/data/pjm_hourly_loads.md` for the full design. Used for sub-TX/DX (and later bulk-TX) load shapes, exceedance sensitivity runs, and zonal-weighting cross-checks against 5CP MW. **Not needed for v1 capacity MC** — PJM publishes the 5 peak hours directly.
