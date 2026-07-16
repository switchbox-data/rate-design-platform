# Easton Utilities — Gas PGC Source Filing

**Source:** Direct Testimony of Carrie B. Manuel, Vice President of Finance, Easton
Utilities Commission.
**Filed:** January 21, 2026. **Docket:** PSC Case No. 9502(t) — _Continuing Investigation
of the Gas Fuel Cost Adjustment Charges of The Easton Utilities Commission._
**Original document:** PSC Case No. 9502(t) CBM Direct Testimony.pdf (not committed;
exceeds 600 KB git limit). Available from the MD PSC DMS at
[webpscxb.pscmaryland.com/DMS/case/9502](https://webpscxb.pscmaryland.com/DMS/case/9502).

The rates in `easton_muni_pgc.csv` are sourced directly from Exhibit CBM-1 of this filing.

---

## How the PGC and ACA work (from testimony)

The **Purchased Gas Charge (PGC)** is assessed monthly to recover the cost of gas sold.
It is calculated as: `(demand costs + commodity costs + propane air plant costs) / projected
CCF sales for the month ± current ACA`.

The **Actual Cost Adjustment (ACA)** reconciles PGC collections against actual gas costs
over the 12 months ending November 30 each year. The ACA factor = `under/(over) collection
balance / forecasted annual sales for the following year`. It is charged or credited to
customers January–December of that year as part of the monthly PGC.

**ACA for 2026 (effective January 1, 2026):** For the 12 months ended November 30, 2025,
EUC had an under-collection of `$61,675.54`. Based on forecasted 2026 sales of 5,011,958
CCF, the ACA factor is **`$0.012/CCF`** (charged to customers).

**ACA for 2025 (effective January 1, 2025):** `$0.032/CCF` (charged), as shown in
Schedule 3.

**ACA for December 2024:** `$(0.007)/CCF` (credit — the prior cycle resulted in a small
over-collection).

---

## Exhibit CBM-1, Page 1 — ACA Calculation Summary

ACA period: December 2024 – November 2025 (12 months). Effective in customer bills:
January 2026 – December 2026.

| Line | Item                                               | Amount           |
| ---- | -------------------------------------------------- | ---------------- |
| 1    | Total cost of gas (Sch. 1)                         | `$4,038,384.96`  |
| 2    | Net under/(over) collected from prior ACA          | `$158,140.20`    |
| 3    | Total costs (1 + 2)                                | `$4,196,525.16`  |
| 4    | Collections via CGA (Sch. 2)                       | `$3,977,747.65`  |
| 5    | Collections via ACA (Sch. 3)                       | `$146,075.87`    |
| 6    | Supplier refunds                                   | `$0.00`          |
| 7    | Billing adjustments (Sch. 5)                       | `$11,026.10`     |
| 8    | Total collections (4+5+6+7)                        | `$4,134,849.62`  |
| 9    | Net under-collection to apply in 2026 (3 − 8)      | `$61,675.54`     |
| 10   | Estimated retail sales for 2026 (CCF) (Sch. 6)     | `5,011,958`      |
| 11   | **ACA rate per CCF (9/10) — charged to customers** | **`$0.012/CCF`** |

---

## Exhibit CBM-1, Schedule 1 — Cost of Gas Supply

| Month     | Year | Gross cost of gas supply | Propane plant cost | Total cost of gas   |
| --------- | ---- | ------------------------ | ------------------ | ------------------- |
| December  | 2024 | `$481,960.99`            | `$4,471.58`        | `$486,432.57`       |
| January   | 2025 | `$758,016.77`            | `$14,944.07`       | `$772,960.84`       |
| February  | 2025 | `$539,699.41`            | `$4,429.74`        | `$544,129.15`       |
| March     | 2025 | `$388,065.91`            | `$0.00`            | `$388,065.91`       |
| April     | 2025 | `$313,905.25`            | `$0.00`            | `$313,905.25`       |
| May       | 2025 | `$208,919.57`            | `$0.00`            | `$208,919.57`       |
| June      | 2025 | `$175,777.09`            | `$0.00`            | `$175,777.09`       |
| July      | 2025 | `$174,445.09`            | `$0.00`            | `$174,445.09`       |
| August    | 2025 | `$171,128.72`            | `$0.00`            | `$171,128.72`       |
| September | 2025 | `$163,877.86`            | `$0.00`            | `$163,877.86`       |
| October   | 2025 | `$235,671.78`            | `$0.00`            | `$235,671.78`       |
| November  | 2025 | `$396,907.13`            | `$6,164.00`        | `$403,071.13`       |
| **Total** |      | **`$4,008,375.57`**      | **`$30,009.39`**   | **`$4,038,384.96`** |

---

## Exhibit CBM-1, Schedule 2 — CGA Revenue (monthly CGA rates)

These are the **filed monthly CGA rates** billed to customers (the key input for
`easton_muni_pgc.csv`). No supplier refunds in this period (Schedule 4 is all zeros).

| Month     | Year | CGA rate (`$/CCF`) | Actual retail sales (CCF) | CGA revenue         |
| --------- | ---- | ------------------ | ------------------------- | ------------------- |
| December  | 2024 | `$0.856`           | 581,220                   | `$497,524.32`       |
| January   | 2025 | `$0.684`           | 931,507                   | `$637,150.79`       |
| February  | 2025 | `$0.510`           | 1,026,883                 | `$523,710.33`       |
| March     | 2025 | `$0.470`           | 813,171                   | `$382,190.37`       |
| April     | 2025 | `$0.786`           | 469,391                   | `$368,941.33`       |
| May       | 2025 | `$0.867`           | 253,871                   | `$220,106.16`       |
| June      | 2025 | `$1.052`           | 203,978                   | `$214,584.86`       |
| July      | 2025 | `$1.159`           | 159,891                   | `$185,313.67`       |
| August    | 2025 | `$1.079`           | 146,956                   | `$158,565.52`       |
| September | 2025 | `$1.016`           | 165,775                   | `$168,427.40`       |
| October   | 2025 | `$1.120`           | 185,747                   | `$208,036.64`       |
| November  | 2025 | `$1.234`           | 334,843                   | `$413,196.26`       |
| **Total** |      |                    | **5,273,233**             | **`$3,977,747.65`** |

---

## Exhibit CBM-1, Schedule 3 — ACA Revenue (monthly ACA rates)

These confirm the ACA rate billed in each month (`$0.032/CCF` for Jan–Nov 2025,
`$(0.007)/CCF` for Dec 2024, i.e., a small credit).

| Month     | Year | ACA rate (`$/CCF`) | Actual retail sales (CCF) | ACA revenue       |
| --------- | ---- | ------------------ | ------------------------- | ----------------- |
| December  | 2024 | `$(0.007)`         | 581,220                   | `$(4,068.54)`     |
| January   | 2025 | `$0.032`           | 931,507                   | `$29,808.22`      |
| February  | 2025 | `$0.032`           | 1,026,883                 | `$32,860.26`      |
| March     | 2025 | `$0.032`           | 813,171                   | `$26,021.47`      |
| April     | 2025 | `$0.032`           | 469,391                   | `$15,020.51`      |
| May       | 2025 | `$0.032`           | 253,871                   | `$8,123.87`       |
| June      | 2025 | `$0.032`           | 203,978                   | `$6,527.30`       |
| July      | 2025 | `$0.032`           | 159,891                   | `$5,116.51`       |
| August    | 2025 | `$0.032`           | 146,956                   | `$4,702.59`       |
| September | 2025 | `$0.032`           | 165,775                   | `$5,304.80`       |
| October   | 2025 | `$0.032`           | 185,747                   | `$5,943.90`       |
| November  | 2025 | `$0.032`           | 334,843                   | `$10,714.98`      |
| **Total** |      |                    | **5,273,233**             | **`$146,075.87`** |

---

## Exhibit CBM-1, Schedule 6 — Estimated Retail Sales for 2026

Based on a 3-year average. Used to compute the 2026 ACA factor.

| Month     | Estimated CCF |
| --------- | ------------- |
| January   | 583,911       |
| February  | 839,130       |
| March     | 920,042       |
| April     | 706,116       |
| May       | 515,007       |
| June      | 265,571       |
| July      | 198,420       |
| August    | 162,981       |
| September | 157,142       |
| October   | 163,092       |
| November  | 189,888       |
| December  | 310,660       |
| **Total** | **5,011,958** |

---

## Coverage note

This filing (9502(t)) covers CGA rates for **December 2024 – November 2025** and sets the
ACA for **January 2026 – December 2026**. **December 2025 CGA is not available here** —
it falls in the 9502(u) cycle (filed ~January 2027). For annual-average simulation
purposes, use the Jan–Nov 2025 mean CGA or estimate December from the prior year's rate.
