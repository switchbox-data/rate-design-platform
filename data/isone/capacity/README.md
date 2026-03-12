# ISO-NE Forward Capacity Market data

Four data products covering ISO-NE's Forward Capacity Market (FCM), the mechanism through which New England procures generation capacity to meet reliability requirements. Each sub-product is a self-contained pipeline with its own Justfile and scripts.

For a comparison of ISO-NE's FCM with NYISO's ICAP market and implications for the Bill Alignment Test, see [`context/domain/capacity_market_comparison_nyiso_isone.md`](../../../context/domain/capacity_market_comparison_nyiso_isone.md).

## Data products

### `fca/` — Forward Capacity Auction clearing prices

Primary auction results. The FCA runs once per year (results in February), procuring capacity three years forward.

|                    |                                                                                                                                                                                              |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Source**         | [FCA Results Report PDF](https://www.iso-ne.com/static-assets/documents/2018/05/fca-results-report.pdf), [official summary table](https://www.iso-ne.com/about/key-stats/markets#fcaresults) |
| **Format**         | Curated CSV → Parquet (PDF source, no API)                                                                                                                                                   |
| **Coverage**       | FCA 1 (CCP 2010-11) through FCA 18 (CCP 2027-28), all capacity zones                                                                                                                         |
| **S3 path**        | `s3://data.sb/isone/capacity/fca/data.parquet`                                                                                                                                               |
| **Update cadence** | Once/year (new FCA in February)                                                                                                                                                              |

**How to update**: Manual. (1) Download FCA Results Report PDF from ISO-NE. (2) Add rows to `fca_clearing_prices.csv` following the `# MAINTENANCE` instructions in the CSV header. (3) `just validate`. (4) Commit CSV. (5) `just upload`.

**Gotchas**: Zone structure changed over time — FCA 1-6 had a single system-wide price, FCA 7 introduced NEMA/Boston as an import-constrained zone, FCA 8-9 had administrative pricing (different prices for existing vs. new resources), and FCA 15+ use the current 4-zone structure (Rest-of-Pool, Maine, NNE, SENE). The `resource_status` column captures these distinctions. See the CSV header and validation script for details.

### `mra/` — Monthly Reconfiguration Auction clearing prices

Monthly adjustments to capacity obligations during a commitment period. The closest ISO-NE analog to NYISO's Spot auction.

|                    |                                                                                                            |
| ------------------ | ---------------------------------------------------------------------------------------------------------- |
| **Source**         | [ISO Express CSV](https://www.iso-ne.com/isoexpress/web/reports/auctions/-/tree/fcmmra) (no auth required) |
| **Format**         | CSV → Hive-partitioned Parquet (`year={YYYY}/month={M}/data.parquet`)                                      |
| **Coverage**       | September 2018 through present (~91 months), all zones + external interfaces                               |
| **S3 path**        | `s3://data.sb/isone/capacity/mra/`                                                                         |
| **Update cadence** | Monthly (new MRA results ~2 months ahead of the auction month)                                             |

**How to update**: `cd data/isone/capacity/mra/ && just update`. Discovers the latest month on S3, fetches newer months, validates, and uploads. For a full rebuild: `just prepare && just upload`.

**Gotchas**: Zone structure changed across commitment periods — CP 2018-19 used zones 8501 (CT), 8502 (NEMA-Boston), 8504 (SEMA-RI), while later CPs use 8503 (Maine), 8505 (NNE), 8506 (SENE). The ISO Express endpoint returns empty responses for CPs before 2018-19. The CSV format uses "C"/"H"/"D"/"T" row-type markers; the fetch script parses only "D" rows.

### `ara/` — Annual Reconfiguration Auction clearing prices

Annual adjustments to capacity obligations before a commitment period begins. Up to 3 ARAs per commitment period (ARA1, ARA2, ARA3).

|                    |                                                                                                            |
| ------------------ | ---------------------------------------------------------------------------------------------------------- |
| **Source**         | [ISO Express CSV](https://www.iso-ne.com/isoexpress/web/reports/auctions/-/tree/fcmara) (no auth required) |
| **Format**         | CSV → Hive-partitioned Parquet (`cp={cp}/ara_number={1\|2\|3}/data.parquet`)                               |
| **Coverage**       | CP 2019-20 through CP 2027-28 (22 partitions across 9 CPs)                                                 |
| **S3 path**        | `s3://data.sb/isone/capacity/ara/`                                                                         |
| **Update cadence** | Up to 3 per commitment period, at specific pre-commitment milestones                                       |

**How to update**: `cd data/isone/capacity/ara/ && just update`. Discovers the latest CP/ARA on S3, fetches newer ARAs. For a full rebuild: `just prepare && just upload`.

**Gotchas**: Not all CPs have all 3 ARAs (e.g., CP 2027-28 only has ARA1 so far). CPs before 2019-20 return header-only CSVs with no data rows. ARA CSVs include `iso_supply_mw` / `iso_demand_mw` columns that MRA CSVs lack (though these are currently null in all observed data).

### `cso/` — Capacity Supply Obligations from CELT reports

State-level capacity obligations extracted from CELT (Capacity, Energy, Loads, and Transmission) Excel files, Table 4.1.

|                    |                                                                                      |
| ------------------ | ------------------------------------------------------------------------------------ |
| **Source**         | [CELT Excel files](https://www.iso-ne.com/system-planning/system-plans-studies/celt) |
| **Format**         | Excel → Hive-partitioned Parquet (`celt_year={YYYY}/data.parquet`)                   |
| **Coverage**       | CELT editions 2020-2025 (skipping 2024), covering CCPs 2019-20 through 2027-28       |
| **S3 path**        | `s3://data.sb/isone/capacity/cso/`                                                   |
| **Update cadence** | Annually (new CELT published in April/May)                                           |

**How to update**: Semi-manual. (1) Find the new CELT Excel URL at [iso-ne.com/celt](https://www.iso-ne.com/system-planning/system-plans-studies/celt) — the URL pattern changes between years. (2) Add the URL to the `CELT_URLS` dict in `fetch_isone_celt_cso.py`. (3) `just prepare && just upload`.

**Gotchas**: CSOs change between CELT editions because reconfiguration auctions adjust them — the same FCA's CSO will differ between the 2024 and 2025 CELT editions. Each CELT edition is a snapshot, so we version by `celt_year`. The 2024 CELT is not available online. Sheet name changed from "4.1 Sum of CSOs" (2020-2022) to "4.1 Summary of CSOs" (2023+). CELT Excels have merged cells and formula cells that require careful parsing. Resource-type subtotals (DCR Total, Gen Total, Total) are verified against component rows during validation.

## Historical CELT URLs

| CELT year | URL                                                      |
| --------- | -------------------------------------------------------- |
| 2025      | `/static-assets/documents/100023/2025_celt.xlsx`         |
| 2024      | Not available online                                     |
| 2023      | `/static-assets/documents/2023/05/2023_celt_report.xlsx` |
| 2022      | `/static-assets/documents/2022/04/2022_celt_report.xlsx` |
| 2021      | `/static-assets/documents/2021/04/2021_celt_report.xlsx` |
| 2020      | `/static-assets/documents/2020/04/2020_celt_report.xlsx` |

Pre-2020 CELTs only have `forecast_data_YYYY.xlsx` files (load forecasts, no CSO data in Table 4.1 format).
