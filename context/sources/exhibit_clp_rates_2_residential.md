# Exhibit CLP-RATES-2 — Rate Design and Revenue Workpapers (Residential classes only: Rate 1, 5, 7; MRCC)

**Source**: `Exhibit CLP-RATES-2.xlsx` (179-tab workbook; `Exhibit CLP-RATES-2.pdf` is the same workbook "printed to PDF" and was not used as a source here)
**Docket**: Connecticut PURA Docket No. 26-05-10 — Application of The Connecticut Light and Power Company d/b/a Eversource Energy to Amend Its Rate Schedules
**Exhibit**: CLP-RATES-2 (Rate Design and Revenue), sub-exhibits CLP-RATES-2.1, 2.3, 2.4, 2.13

**Extraction method**: Read directly via `openpyxl` (`data_only=True`, i.e. cached calculated values, not formulas). This is a **scoped extract**: the full workbook covers all rate classes (Rate 1 through Rate 119, EV classes, street lighting) across 14 sub-exhibits (`CLP-RATES-2.1` through `2.14`); only the tabs and rows relevant to the **residential classes** (Rate 1 — Residential Electric Service, Rate 5 — Residential Electric Heating, Rate 7 — Residential Time-of-Day) and the Maximum Residential Customer Charge (MRCC, `Exhibit CLP-RATES-2.13`) were transcribed. Rate 6 (new Optional Residential Space Heating rate) has no workpaper in `CLP-RATES-2.4` because it is a new rate with no current-rate billing history; its tariff structure is captured separately in `exhibit_clp_rates_3_residential_tariffs.md`. Dollar figures below are in `$(000)'s` unless noted; per-unit rates are `$` (customer charge) or `$/kWh`.

---

## 1. Exhibit CLP-RATES-2.1 — Comparison of Distribution Revenue at Current vs. Proposed Rates

Tab `Exh 2.1, 1 of 2`. Rate Year ending 6/30/2028. Columns: `A` = Rate Year Billed Sales (MWh), `B` = Current Rate Distribution (Rev `$000`), `C` = GET Refund (Rev `$000`), `D = B - C` = Adjusted Distribution, `E = D x Test Year Avg GET Rate` = GET Revenue, `F = D + E`... [formula labels per the sheet's own header row; see column headers below]. Residential rows only:

| Rate                  | Billed Sales (MWh) | Current Rate Distribution Rev (`$000`) | GET Refund (`$000`) | Adjusted D (`$000`) | GET Revenue (`$000`) | Distribution Rev w/o GET (`$000`) | Allocation to Avg | Proposed D Increase (`$000`) | Proposed Rate Target Revenue (`$000`) | Proposed Distribution Revenue (`$000`) |
| --------------------- | ------------------ | -------------------------------------- | ------------------- | ------------------- | -------------------- | --------------------------------- | ----------------- | ---------------------------- | ------------------------------------- | -------------------------------------- |
| 1                     | 8,716,958.14       | 811,715.87                             | -0.41               | 811,716.28          | -60,036.91           | 751,679.37                        | —                 | 388,008.83                   | 1,139,688.20                          | 1,139,699.00                           |
| 7                     | 9,126.89           | 776.47                                 | 0                   | 776.47              | -57.43               | 719.04                            | —                 | 371.16                       | 1,090.20                              | 1,121.84                               |
| **Total Rates 1 & 7** | 8,726,085.03       | 812,492.34                             | -0.41               | 812,492.74          | -60,094.34           | 752,398.41                        | 1.02              | 388,379.99                   | 1,140,778.40                          | 1,140,820.84                           |
| 5                     | 1,593,008.20       | 127,306.10                             | 0                   | 127,306.10          | -9,415.93            | 117,890.17                        | 1.20              | 71,592.52                    | 189,482.69                            | 189,480.14                             |

Company-wide totals from the same sheet (for context): Test Year Average Gross Earnings Tax Rate = `7.396292%` (`Exhibit TRP-2, page 2 of 2`); Proposed RY Revenue Increase = `$728,594` thousand (`Schedule A-1.0, Line 34`); Proposed RY Increase w/o GET = `$677,221` thousand; Average Rate Change % = `50.6068%`; RY Billed Sales = `20,457,506.84` MWh; Average Rate Change per kWh = `3.3104¢/kWh`.

Tab `Exh 2.1, 2 of 2` (CL&P Test Year 2025 Average GET Rate build-up): Distribution Revenue `(A)` = `$1,124,494`k; Distribution GET Mfg. Credits `(B)` = `$3,121.37`k; Distribution Revenue plus Credits `C = A + B` = `$1,127,615.37`k; GET Liability `(D)` = `$83,401.73`k; Average GET Rate `E = D / C` = `7.39629%`. Source: Company Billing Records.

---

## 2. Exhibit CLP-RATES-2.4 — Billing Determinants / Test Year and Rate Year Workpapers

Each rate has 3 tabs: (1) Billing Determinants Workpaper, (2) Test Year Ending 2025 Workpapers, (3) Rate Year Workpapers. All figures below are the **Rate Year** (ending 6/30/2028) columns from workpaper (3) unless noted; workpaper (2)'s Test Year figures are given separately where they differ materially (GET rate and a few normalization adjustments).

### 2a. Rate 1 — Residential Electric Service (tabs `Exh 2.4, R1 1–3 of 75`)

**Billing determinants** (Rate Year): Average # Bills Rendered = `1,076,695.40`; Billed Sales = `8,716,958.14` MWh; Primary Metering Credit = `12.70` MWh; Total Sales = `8,716,970.84` MWh.

**Rate Year current vs. proposed rate build** (tab 3, `Page 3 of 75`):

| Price block                       | Units (kWh or count)  | Current Rate `$` | Current Revenue (`$000`) | Proposed Rate `$` | Proposed Revenue (`$000`) | GET Adj. (6.8%) Rate `$` | GET Revenue (`$000`) | Proposed + GET Rate `$` | Proposed + GET Revenue (`$000`) |
| --------------------------------- | --------------------- | ---------------- | ------------------------ | ----------------- | ------------------------- | ------------------------ | -------------------- | ----------------------- | ------------------------------- |
| Customer Charge                   | 12,920,344.76 (bills) | 9.62             | 124,293.72               | 11.52             | 148,842.37                | 0.84                     | 10,853.09            | 12.36                   | 159,695.46                      |
| Distribution (per kWh)            | 8,716,958.14          | 0.05844          | 509,419.03               | 0.11367           | 990,856.63                | 0.00829                  | 72,263.58            | 0.12196                 | 1,063,120.21                    |
| Electric System Improvements      | "                     | 0.02031          | 177,041.42               | 0                 | 0                         | —                        | 0                    | 0                       | 0                               |
| Revenue Decoupling Mechanism      | "                     | 0.00011          | 958.87                   | 0                 | 0                         | —                        | —                    | 0                       | 0                               |
| Transmission                      | "                     | 0.0505           | 440,206.39               | 0.0505            | 440,206.39                | —                        | —                    | 0.0505                  | 440,206.39                      |
| Conservation Adjustment Mech.     | "                     | 0.006            | 52,301.75                | 0.006             | 52,301.75                 | —                        | —                    | 0.006                   | 52,301.75                       |
| Renewable Energy                  | "                     | 0.001            | 8,716.96                 | 0.001             | 8,716.96                  | —                        | —                    | 0.001                   | 8,716.96                        |
| Systems Benefits Charge           | "                     | -0.00196         | -17,085.24               | -0.00196          | -17,085.24                | —                        | —                    | -0.00196                | -17,085.24                      |
| Competitive Transition Assessment | "                     | 0.00496          | 43,236.11                | 0.00496           | 43,236.11                 | —                        | —                    | 0.00496                 | 43,236.11                       |
| FMCC-Delivery                     | "                     | -0.01911         | -166,581.07              | -0.01911          | -166,581.07               | —                        | —                    | -0.01911                | -166,581.07                     |
| FMCC-Generation                   | "                     | -0.0015          | -13,075.44               | -0.0015           | -13,075.44                | —                        | —                    | -0.0015                 | -13,075.44                      |
| Generation Services               | "                     | 0.12791          | 1,114,986.12             | 0.12791           | 1,114,986.12              | —                        | —                    | 0.12791                 | 1,114,986.12                    |
| GET Refund                        | —                     | ~0               | -0.41                    | —                 | —                         | ~0                       | -0.79                | ~0                      | -0.79                           |
| **Total**                         |                       |                  | **2,274,418.20**         |                   | **2,602,404.58**          |                          | **83,115.89**        |                         | **2,685,520.46**                |

`% Change` (Proposed+GET vs. Current) = `18.075%`.

**By functional category, Rate Year** (Current Rate / Proposed / Proposed+GET / Difference, revenue `$000` and `cents/kWh`):

| Functional category               | Current Rev      | Current ¢/kWh | Proposed Rev     | Proposed ¢/kWh | Proposed+GET Rev | Proposed+GET ¢/kWh | Difference Rev | Difference ¢/kWh |
| --------------------------------- | ---------------- | ------------- | ---------------- | -------------- | ---------------- | ------------------ | -------------- | ---------------- |
| Distribution                      | 633,712.34       | 7.2699        | 1,139,699.00     | 13.0745        | 1,222,814.89     | 14.0280            | 589,102.55     | 6.7581           |
| Electric System Improvements      | 177,041.42       | 2.031         | 0                | 0              | 0                | 0                  | -177,041.42    | -2.031           |
| Revenue Decoupling Mechanism      | 958.87           | 0.011         | 0                | 0              | 0                | 0                  | -958.87        | -0.011           |
| Transmission                      | 440,206.39       | 5.05          | 440,206.39       | 5.05           | 440,206.39       | 5.05               | 0              | 0                |
| Conservation Adjustment Mech.     | 52,301.75        | 0.6           | 52,301.75        | 0.6            | 52,301.75        | 0.6                | 0              | 0                |
| Renewable Energy                  | 8,716.96         | 0.1           | 8,716.96         | 0.1            | 8,716.96         | 0.1                | 0              | 0                |
| Systems Benefits Charge           | -17,085.24       | -0.196        | -17,085.24       | -0.196         | -17,085.24       | -0.196             | 0              | 0                |
| Competitive Transition Assessment | 43,236.11        | 0.496         | 43,236.11        | 0.496          | 43,236.11        | 0.496              | 0              | 0                |
| FMCC-Delivery                     | -166,581.07      | -1.911        | -166,581.07      | -1.911         | -166,581.07      | -1.911             | 0              | 0                |
| FMCC-Generation                   | -13,075.44       | -0.15         | -13,075.44       | -0.15          | -13,075.44       | -0.15              | 0              | 0                |
| Generation Services               | 1,114,986.12     | 12.791        | 1,114,986.12     | 12.791         | 1,114,986.12     | 12.791             | 0              | 0                |
| **Total**                         | **2,274,418.20** | **26.0919**   | **2,602,404.58** | **29.8545**    | **2,685,520.46** | **30.8080**        | **411,102.26** | **4.7161**       |

Total `% Change` = `18.075%` (matches the customer-charge-block total above to within rounding; the workpaper computes it twice, once per price-block build and once per functional-category rollup, and the two differ in the 3rd decimal — `18.075%` vs `18.079%` in the Test Year tab — due to normalization-adjustment timing between Actual and Normalized sales).

### 2b. Rate 5 — Residential Electric Heating Service (tabs `Exh 2.4, R5 4–6 of 75`)

**Billing determinants** (Rate Year): Average # Bills Rendered = `135,073.2`; Billed Sales = `1,593,008.20` MWh; Total Sales = `1,593,009.92` MWh.

**Rate Year current vs. proposed rate build**:

| Price block                       | Current Rate `$` | Current Revenue (`$000`) | Proposed Rate `$` | Proposed Revenue (`$000`) | GET Adj. Rate `$` | GET Revenue (`$000`) | Proposed+GET Rate `$` | Proposed+GET Revenue (`$000`) |
| --------------------------------- | ---------------- | ------------------------ | ----------------- | ------------------------- | ----------------- | -------------------- | --------------------- | ----------------------------- |
| Customer Charge                   | 23.75            | 38,495.86                | 30.00             | 48,626.35                 | —                 | —                    | 30.00                 | 48,626.35                     |
| Distribution (per kWh)            | 0.03847          | 61,283.03                | 0.08842           | 140,853.78                | 0.00868           | 13,824.73            | 0.0971                | 154,681.10                    |
| Electric System Improvements      | 0.01717          | 27,351.95                | 0                 | 0                         | —                 | 0                    | 0                     | 0                             |
| Revenue Decoupling Mechanism      | 0.00011          | 175.23                   | 0                 | 0                         | —                 | —                    | 0                     | 0                             |
| Transmission                      | 0.04901          | 78,073.33                | 0.04901           | 78,073.33                 | —                 | —                    | 0.04901               | 78,073.33                     |
| Conservation Adjustment Mech.     | 0.006            | 9,558.05                 | 0.006             | 9,558.05                  | —                 | —                    | 0.006                 | 9,558.05                      |
| Renewable Energy                  | 0.001            | 1,593.01                 | 0.001             | 1,593.01                  | —                 | —                    | 0.001                 | 1,593.01                      |
| Systems Benefits Charge           | -0.00196         | -3,122.30                | -0.00196          | -3,122.30                 | —                 | —                    | -0.00196              | -3,122.30                     |
| Competitive Transition Assessment | 0.00496          | 7,901.32                 | 0.00496           | 7,901.32                  | —                 | —                    | 0.00496               | 7,901.32                      |
| FMCC-Delivery                     | -0.0184          | -29,311.35               | -0.0184           | -29,311.35                | —                 | —                    | -0.0184               | -29,311.35                    |
| FMCC-Generation                   | -0.0015          | -2,389.51                | -0.0015           | -2,389.51                 | —                 | —                    | -0.0015               | -2,389.51                     |
| Generation Services               | 0.12791          | 203,761.68               | 0.12791           | 203,761.68                | —                 | —                    | 0.12791               | 203,761.68                    |
| **Total**                         |                  | **393,370.30**           |                   | **455,544.37**            |                   | **13,824.73**        |                       | **469,371.68**                |

`% Change` = `19.3206%`. Note Rate 5 gets **no distribution GET gross-up adjustment on the customer charge** in this tab (unlike Rate 1/7, whose customer charge shows a GET add-on) — the `30.00` proposed customer charge already appears to be the final billed value.

### 2c. Rate 7 — Residential Time-of-Day (tabs `Exh 2.4, R7 7–9 of 75`)

**Billing determinants** (Rate Year): Average # Bills Rendered = `752.35`; On-Peak = `2,122.41` MWh; Off-Peak = `7,004.48` MWh; Billed Sales = `9,126.89` MWh.

**Rate Year current vs. proposed rate build**:

| Price block                                | Current Rate `$` | Current Revenue (`$000`) | Proposed Rate `$` | Proposed Revenue (`$000`) | GET Adj. Rate `$` | GET Revenue (`$000`) | Proposed+GET Rate `$` | Proposed+GET Revenue (`$000`) |
| ------------------------------------------ | ---------------- | ------------------------ | ----------------- | ------------------------- | ----------------- | -------------------- | --------------------- | ----------------------------- |
| Customer Charge                            | 9.62             | 86.85                    | 11.52             | 104.01                    | 0.84              | 7.59                 | 12.36                 | 111.59                        |
| On-Peak Distribution                       | 0.05513          | 117.01                   | 0.11152           | 236.69                    | 0.00814           | 17.27                | 0.11966               | 253.97                        |
| On-Peak Electric System Improvements       | 0.02031          | 43.11                    | 0                 | 0                         | —                 | 0                    | 0                     | 0                             |
| On-Peak Revenue Decoupling Mechanism       | 0.00011          | 0.23                     | 0                 | 0                         | —                 | —                    | 0                     | 0                             |
| On-Peak Transmission                       | 0.11741          | 249.19                   | 0.11741           | 249.19                    | —                 | —                    | 0.11741               | 249.19                        |
| On-Peak Conservation Adj. Mech.            | 0.006            | 12.73                    | 0.006             | 12.73                     | —                 | —                    | 0.006                 | 12.73                         |
| On-Peak Renewable Energy                   | 0.001            | 2.12                     | 0.001             | 2.12                      | —                 | —                    | 0.001                 | 2.12                          |
| On-Peak Systems Benefits Charge            | -0.00196         | -4.16                    | -0.00196          | -4.16                     | —                 | —                    | -0.00196              | -4.16                         |
| On-Peak Competitive Transition Assessment  | 0.00496          | 10.53                    | 0.00496           | 10.53                     | —                 | —                    | 0.00496               | 10.53                         |
| On-Peak FMCC-Delivery                      | -0.04443         | -94.30                   | -0.04443          | -94.30                    | —                 | —                    | -0.04443              | -94.30                        |
| On-Peak FMCC-Generation                    | -0.0015          | -3.18                    | -0.0015           | -3.18                     | —                 | —                    | -0.0015               | -3.18                         |
| On-Peak Generation Services                | 0.15396          | 326.77                   | 0.15396           | 326.77                    | —                 | —                    | 0.15396               | 326.77                        |
| Off-Peak Distribution                      | 0.05513          | 386.16                   | 0.11152           | 781.14                    | 0.00814           | 56.99                | 0.11966               | 838.16                        |
| Off-Peak Electric System Improvements      | 0.02031          | 142.26                   | 0                 | 0                         | —                 | 0                    | 0                     | 0                             |
| Off-Peak Revenue Decoupling Mechanism      | 0.00011          | 0.77                     | 0                 | 0                         | —                 | —                    | 0                     | 0                             |
| Off-Peak Transmission                      | 0.02525          | 176.86                   | 0.02525           | 176.86                    | —                 | —                    | 0.02525               | 176.86                        |
| Off-Peak Conservation Adj. Mech.           | 0.006            | 42.03                    | 0.006             | 42.03                     | —                 | —                    | 0.006                 | 42.03                         |
| Off-Peak Renewable Energy                  | 0.001            | 7.00                     | 0.001             | 7.00                      | —                 | —                    | 0.001                 | 7.00                          |
| Off-Peak Systems Benefits Charge           | -0.00196         | -13.73                   | -0.00196          | -13.73                    | —                 | —                    | -0.00196              | -13.73                        |
| Off-Peak Competitive Transition Assessment | 0.00496          | 34.74                    | 0.00496           | 34.74                     | —                 | —                    | 0.00496               | 34.74                         |
| Off-Peak FMCC-Delivery                     | -0.00955         | -66.89                   | -0.00955          | -66.89                    | —                 | —                    | -0.00955              | -66.89                        |
| Off-Peak FMCC-Generation                   | -0.0015          | -10.51                   | -0.0015           | -10.51                    | —                 | —                    | -0.0015               | -10.51                        |
| Off-Peak Generation Services               | 0.11896          | 833.25                   | 0.11896           | 833.25                    | —                 | —                    | 0.11896               | 833.25                        |
| **Total**                                  |                  | **2,278.85**             |                   | **2,624.30**              |                   | **81.85**            |                       | **2,706.17**                  |

`% Change` = `18.752%`.

**By functional category, Rate Year** (Distribution row only, others match the On/Off-Peak components summed): Current Rev `590.02` (`6.4646¢/kWh`), Proposed Rev `1,121.84` (`12.2915¢/kWh`), Proposed+GET Rev `1,203.71` (`13.1886¢/kWh`), Difference `613.70` (`6.7240¢/kWh`).

---

## 3. Exhibit CLP-RATES-2.13 — Calculation of Maximum Residential Customer Charge (MRCC)

Tab `Exh 2.13, 1 of 19` ("Summary of MRCC by Item"), which per the exhibit's own cover/TOC is the top-level summary rolling up Rate Base (Services, Meters, Office Furniture & Equipment, Transportation Equipment, Stores Equipment, Tools/Shop/Garage Equipment, Laboratory Equipment, Power Operated Equipment, Communication Equipment, Miscellaneous Equipment, Rate Base Adds/Deducts) → Expenses (Distribution Supervision & Engineering, Meter Expenses, Overhead Lines Expense-Services, Customer Accounting Supervision, Meter Reading, Customer Records and Collection, Miscellaneous Customer Accounts, Customer Assistance, Miscellaneous Customer Service, Salaries, Office Supplies, Admin Expenses & Transfer Credit, Outside Services, Property Insurance, Injuries & Damages, Pension & Benefit, Misc. General, Depreciation & Amortization, Property Taxes, Payroll Taxes) → Taxes (Current Income Taxes, Gross Receipts Tax, Deferred Income Taxes):

| Item                      | Rate Base / Expense (`$`) | MRCC (`$`/month) |
| ------------------------- | ------------------------- | ---------------- |
| Total Return on Rate Base | 26,258.11                 | 2.0674           |
| Total Expenses            | 103,256.57                | 8.1298           |
| Total Taxes               | 11,342.39                 | 0.8930           |
| **Total**                 | **140,857.07**            | **11.0902**      |

Footnotes on this tab: (a) Number of Customers used for this calculation = `1,058,422.5` (source: `Exhibit CLP-ACOS-2, Appendix 1.b Customers - (Rates 1 & 7)`); (b) Return = `7.74%` ("Proposed total capitalization"). Column formula per the sheet header: `MRCC ($/Month) = (Rate Base/Expense) / (a) / 12 months × 1000`.

**Discrepancy to flag**: this tab's computed **Total MRCC is `$11.09`/month**, whereas `Exhibit CLP-RATES-1` (testimony) states "the Company completed the MRCC calculation, as provided in Exhibit CLP-RATES-2.13. This calculation produced a customer charge of `$12.36` for Rate 1 and Rate 7," and the `Exh 2.4, R1`/`R7` rate-design tabs use a pre-GET proposed customer charge of `$11.52` (which grosses up to `$12.36` with the `6.8%` distribution GET, i.e. `11.52 / (1 - 0.068) ≈ 12.36`). The `$11.09` MRCC-tab total and the `$11.52` pre-GET figure actually used in rate design do not reconcile in the pages transcribed here (Test Year cost/customer-count basis vs. whatever basis produced `$11.52` is unclear from this tab alone). This has **not** been resolved — it would need the remaining `Exh 2.13, 2–19 of 19` backup pages (Rate Base detail, Expense detail by FERC account, footnote (b) capitalization detail) to trace, which were not extracted in this scoped pass.

---

## Notes on scope

This is a **residential-only** extract. Not transcribed here (available in the source workbook if needed): `Exh 2.2` (current vs. proposed rate comparison, all classes), `Exh 2.4` for all other rate classes (5 non-residential + EV classes + street lighting + Rate 119), `Exh 2.5` (typical bills, 23 tabs), `Exh 2.6` (Last Resort typical bills), `Exh 2.7` (street lighting), `Exh 2.8` (Rate 7 TOD calculations — note: this may contain the actual on/off-peak period definitions and could be worth pulling if TOU period boundaries are needed), `Exh 2.9`–`2.12` (test year actual revenue, current/proposed rate revenue schedules E-2.1/E-2.0, decoupling targets), `Exh 2.13` pages 2–19 (MRCC backup detail), `Exh 2.14` (pole attachment).
