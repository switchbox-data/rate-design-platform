# PJM data

PJM datasets feeding supply-capacity marginal costs for the Bill Alignment Test. The design is **region-first**: the two curated capacity datasets cover all PJM transmission zones; utility specificity lives only in `zone_mapping/`, which filters region-wide data down to one utility. Each sub-product is a self-contained pipeline with its own Justfile and scripts.

For methodology (5CP capacity obligation, RPM pricing, decision rationale for BGE), see [`context/domain/marginal_costs/pjm_supply_capacity_marginal_cost.md`](../../context/domain/marginal_costs/pjm_supply_capacity_marginal_cost.md).

## Delivery-year calendar

PJM's capacity market runs on **delivery years (DY): June 1 – May 31** (e.g. DY 2025/26 = Jun 1, 2025 – May 31, 2026). These datasets store per-DY values; consumers that need calendar-year prices blend two DYs with a 5/7 split (Jan–May from the DY ending that year, Jun–Dec from the DY starting that year).

## Zone nomenclature crosswalk

PJM publishes the same zone under different labels depending on the data product. Both curated CSVs store **canonical labels** (normalized at transcription time); the per-source aliases live in `zone_mapping/`. This table is the source of truth (mirrored in the validate scripts):

| Canonical (`zone`) | Data Miner code | 5CP PDF label | RPM XLS label | Notes                              |
| ------------------ | --------------- | ------------- | ------------- | ---------------------------------- |
| AECO               | AE              | AE            | AE            | Atlantic City Electric             |
| AEP                | AEP             | AEP           | AEP           |                                    |
| APS                | AP              | APS           | APS           | Allegheny / Potomac Edison parent  |
| ATSI               | ATSI            | ATSI          | ATSI          |                                    |
| BGE                | BC              | BGE           | BGE           | **legacy Data Miner code `BC`**    |
| COMED              | CE              | COMED         | COMED         |                                    |
| DAY                | DAY             | DAYTON        | DAYTON        |                                    |
| DEOK               | DEOK            | DEOK          | DEOK          |                                    |
| DOM                | DOM             | DOM           | DOM           |                                    |
| DPL                | DPL             | DPL           | DPL           |                                    |
| DUQ                | DUQ             | DLCo          | DLCO          | Duquesne Light; two aliases        |
| EKPC               | EKPC            | EKPC          | EKPC          |                                    |
| JCPL               | JC              | JCPL          | JCPL          |                                    |
| METED              | ME              | METED         | METED         |                                    |
| PECO               | PE              | PECO          | PECO          |                                    |
| PENELEC            | PN              | PENLC         | PENLC         |                                    |
| PEPCO              | PEP             | PEPCO         | PEPCO         |                                    |
| PPL                | PL              | PPL-EU        | PL            |                                    |
| PSEG               | PS              | PS            | PS            |                                    |
| RECO               | RECO            | RECO          | RECO          | tiny zone                          |
| UGI                | UGI             | UGI           | (in PPL LDA)  | tiny zone; absent from RPM files   |

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

|                    |                                                                                                                                                                            |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Source**         | Per-DY Excel files on the [PJM RPM page](https://pjm.com/markets-and-operations/rpm.aspx): "Final Zonal UCAP Obligations, Capacity Prices and CTR Credit Rates" + BRA results |
| **Format**         | Curated CSV → Parquet (no API)                                                                                                                                                |
| **Coverage**       | DY 2018/19 – 2026/27, 20 zones (no UGI/OVEC, see crosswalk)                                                                                                                   |
| **S3 path**        | `s3://data.sb/pjm/capacity/rpm/data.parquet`                                                                                                                                  |
| **Update cadence** | New BRA ~annually; IA true-ups within a DY                                                                                                                                    |

**How to update**: Manual. (1) Download both files for the new DY from the RPM page. (2) Add one row per zone to `rpm_capacity_prices.csv` following the `# MAINTENANCE` instructions in the CSV header. (3) `just validate`. (4) Commit CSV. (5) `just upload`.

**Gotchas**:

- **DY = Jun 1–May 31.** Calendar-year consumers blend 5/7; this dataset just stores per-DY.
- **Two price concepts per row**: the BRA price is per-LDA at auction time; the Final Zonal price is per-zone after Incremental Auctions and locational adders. They differ whenever IAs cleared or the zone sits in a constrained LDA — and the final price can drop *below* the system BRA price after IA true-downs (e.g. PPL in 2018/19).
- **LDA membership changes by DY** — see LDA nesting above.
- **Excel formats and URLs vary across DYs** (sheet names, layouts, paths) — hence manual transcription; each XLS URL is recorded in `source_url`. From DY 2023/24, the FZSF-FZCP sheet ships inside the "3IA Results" XLSX instead of a standalone Final Zonal file.
- **Sub-LDA blending**: DPL-S (inside the DPL zone) cleared separately in 2023/24 and 2024/25; the zone-level `lda` stays at the containing LDA and the final zonal price blends sub-LDA pricing (noted in `notes`; flagged by the `Final vs BRA` WARN).
- **BRA schedule compression**: recent BRAs run much closer to the DY; expect new DYs to appear with BRA-only prices first, then drift as IAs settle. IA true-ups: update rows in place, bump `final_price_as_of`, note the IA in `notes` — git history is the revision trail.

### `capacity/5cp/` — Summer 5CP peaks

The 5 highest non-holiday-weekday RTO unrestricted daily peaks per summer (PJM's capacity-obligation hours), plus zonal unrestricted MW coincident with each peak. One row per (summer_year, rank, zone); `zone == "RTO"` rows are what the MC consumer reads.

|                    |                                                                                                                                 |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| **Source**         | Annual "Summer YYYY Peaks and 5CPs" PDF, [PJM Planning → Load Forecast](https://www.pjm.com/planning/resource-adequacy-planning/load-forecast-dev-process) |
| **Format**         | Curated CSV → Parquet (PDF source, no API)                                                                                       |
| **Coverage**       | Summers 2018–2025, 5 ranks × (21 zones + RTO) = 880 rows                                                                         |
| **S3 path**        | `s3://data.sb/pjm/capacity/5cp/data.parquet`                                                                                     |
| **Update cadence** | Mid-October posting + possible November revision                                                                                 |

**How to update**: Manual. (1) Download the new PDF. (2) Add rows to `fivecp_peaks.csv` following the `# MAINTENANCE` instructions in the CSV header. (3) `just validate`. (4) Commit CSV. (5) `just upload`.

**Gotchas**:

- **Hour-ending EPT**: PDFs report hour-ending (e.g. "HE 17" = 16:00–17:00 EPT), stored as published; the MC consumer converts to hour-beginning to match platform convention. EPT = prevailing Eastern (EDT all summer in practice).
- **November revisions** restate load-drop add-backs ("Revised MM/DD/YYYY"): MW values change, timestamps almost never do. Update `mw_unrestricted` in place, bump `source_as_of`; git history is the revision trail.
- **Zone label drift**: PDFs use `DLCo`, `PENLC`, `PPL-EU` (and `PL-EU` in 2025) — normalize to canonical at transcription; never store PDF-native labels.
- **Older years (2018–2020)** were removed from pjm.com; `source_url` points to Wayback Machine archives of the original postings (see Historical source URLs below).
- **Unrestricted ≠ metered**: these MW include load-drop add-backs and are not exactly reconcilable against Data Miner `hrl_load_metered` (see Future work).

### `zone_mapping/` — utility → zone crosswalk

One row per (utility, zone, weight); the single place the per-source zone aliases (`dataminer_zone`, `fivecp_zone_label`, `price_zone`) live. MC scripts use it to filter the region-wide datasets down to one utility.

|                    |                                                                          |
| ------------------ | ------------------------------------------------------------------------ |
| **Source**         | Hardcoded rows in `generate_zone_mapping_csv.py` (nyiso pattern)         |
| **Format**         | Generated CSV (uploaded as CSV, not parquet)                             |
| **Coverage**       | MD utilities: bge, pepco, dpl, potomac-edison                            |
| **S3 path**        | `s3://data.sb/pjm/zone_mapping/pjm_utility_zone_mapping.csv`             |
| **Update cadence** | When onboarding a utility                                                |

**How to update**: Edit `_MAPPING_ROWS` in `generate_zone_mapping_csv.py`, then `just prepare && just upload`. Validation cross-checks every mapped zone against both capacity CSVs (the single enforcement point of zone-code integrity between the three datasets).

**Registry compatibility**: column names map 1:1 onto the planned cross-ISO utility registry (`utility`, `state`, `dataminer_zone` → `load_zones`, `price_zone`, `capacity_weight`; `iso=pjm` implicit by location). When the registry lands, these rows migrate into it and this pipeline can be retired or become a thin export.

**Gotchas**:

- **Three naming systems, one crosswalk**: Data Miner legacy codes (`BC`, `PEP`, `AP`) vs PDF/XLS labels (`BGE`, `PEPCO`, `APS`) — this CSV is the only place the mapping lives.
- **Zone ≠ retail territory**: the PEPCO and DPL zones span MD + DC/DE; `state` here is the *analysis* state, customer filtering happens upstream in ResStock utility assignment.
- **`potomac-edison`** is a retail brand inside the APS zone — zone-level data includes WV/PA load.

## Historical source URLs

### `capacity/rpm/` (per-DY Final Zonal / 3IA results, under `https://www.pjm.com/-/media/DotCom/markets-ops/rpm/rpm-auction-info/`)

| DY      | File                                                                              |
| ------- | --------------------------------------------------------------------------------- |
| 2018/19 | `2018-2019-final-zonal-ucap-obligations-capacity-prices-ctr-credit-rates.xlsx`     |
| 2019/20 | `2019-2020/2019-2020-final-zonal-ucap-obligations-capacity-prices-ctr-credit-rates.xls` |
| 2020/21 | `2020-2021/2020-2021-final-zonal-ucap-obligations-capacity-prices-ctr-credit-rates.xlsx` |
| 2021/22 | `2021-2022/2021-2022-final-zonal-ucap-obligations-capacity-prices-and-ctr-credit-rates.xlsx` |
| 2022/23 | `2022-2023/2022-2023-final-zonal-ucap-obligations-capacity-prices-and-ctr-rates.xlsx` |
| 2023/24 | `2023-2024/2023-2024-3ia-results.xlsx` (FZSF-FZCP sheet)                           |
| 2024/25 | `2024-2025/2024-2025-3ia-results.xlsx` (FZSF-FZCP sheet)                           |
| 2025/26 | `2025-2026/2025-2026-3ia-results.xlsx` (FZSF-FZCP sheet)                           |
| 2026/27 | `2026-2027/2026-2027-3ia-results.xlsx` (FZSF-FZCP sheet; BRA-only so far)          |

BRA Resource Clearing Prices come from the matching `<DY>-base-residual-auction-results` XLS(X) (`2026-2027-bra-results.xlsx` for 2026/27).

### `capacity/5cp/` (annual PDFs)

| Summer    | URL                                                                                               |
| --------- | -------------------------------------------------------------------------------------------------- |
| 2021–2025 | `https://www.pjm.com/-/media/DotCom/planning/res-adeq/load-forecast/summer-YYYY-peaks-and-5cps.pdf` |
| 2020      | Wayback: `web.archive.org/web/20210625134332/https://www.pjm.com/-/media/planning/res-adeq/load-forecast/summer-2020-peaks-and-5cps.ashx` |
| 2019      | Wayback: `web.archive.org/web/20210625131044/https://www.pjm.com/-/media/planning/res-adeq/load-forecast/summer-2019-peaks-and-5cps.ashx` |
| 2018      | Wayback: `web.archive.org/web/20210625122746/https://www.pjm.com/-/media/planning/res-adeq/load-forecast/20181017-summer-2018-peaks-and-5cps.ashx` |

## Future work (deferred): `hourly_demand/zones/`

Data Miner 2 [`hrl_load_metered`](https://dataminer2.pjm.com/feed/hrl_load_metered/definition) pipeline — all zones + RTO, hourly metered load, Hive-partitioned `zone={dataminer_zone}/year={YYYY}/data.parquet` at `s3://data.sb/pjm/hourly_demand/zones/`. Needs `PJM_API_KEY` (free registration). Model on `data/isone/hourly_demand/` (fetch + update-to-latest watermark). Required only for exceedance sensitivity runs, zonal-weighting cross-checks against 5CP MW, and future PJM components (supply energy, bulk TX). The mapping CSV's `dataminer_zone` column already anticipates it. **Not needed for v1 capacity MC** — PJM publishes the 5 peak hours directly.
