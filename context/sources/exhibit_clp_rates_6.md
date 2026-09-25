# Exhibit CLP-RATES-6 — Rate 6 Optional Residential Space Heating Rate (Rate Design Worksheet and Bill Impacts)

**Source**: `Exhibit CLP-RATES-6.pdf` (Excel print of `Exhibit CLP-RATES-6.xlsx`)
**Pages**: 3 total (cover/TOC + worksheet + rates/bill-impacts)
**Date**: Printed July 13, 2026 (PDF metadata `creationDate`); filed with the July 14, 2026 rate-case application
**Author(s)**: Kimberly Parsons (PDF metadata author)
**Author affiliations**: None on the exhibit (supporting schedules to Davis & Parsons joint testimony, Exhibit CLP-RATES-1)
**Docket**: Connecticut PURA Docket No. 26-05-10 — Application of The Connecticut Light and Power Company d/b/a Eversource Energy to Amend Its Rate Schedules
**Exhibit**: CLP-RATES-6

**Extraction method**: PyMuPDF text + word coordinates, then page renders to reconstruct the two-column worksheet (page 2) and the Rate 6 vs Rate 1 layout plus embedded bar chart (page 3). Excel row numbers in the left margin (1–49 on page 2; 1–64 on page 3) are omitted. Dollar signs that the print placed in a separate column are shown with the values.

Note: Minor typographical errors in the source (double closing parenthesis in `(l=k/h))`, `Energy_Charge` underscore) are preserved where they appear in labels. Substantive formula notes are annotated inline.

**How to read this exhibit:** Page 2 is a two-column Excel print. The **left** column builds the Rate 6 customer charge from marginal customer cost + GET, then backs a class-level energy-charge revenue requirement out of proposed Rate 1 customer-charge and energy-charge revenues, yielding Block 1 `$0.09431/kWh` and winter Block 2 `$0.07517/kWh`. The **right** column (“MC Based Distribution EC Rate Detail”) shows why that winter tail rate equals unit facilities-cost recovery + energy MC + a coincidence-factor-reduced residual. Page 3 applies those rates to an illustrative monthly kWh profile versus Rate 1.

GET throughout is **6.80%** (gross earnings tax), applied as a mark-up on pre-GET unit costs.

---

## Cover (page 1 of 3)

The Connecticut Light and Power Company\
d/b/a Eversource Energy\
Docket No. 26-05-10\
Exhibit CLP-RATES-6\
Page 1 of 3

**EXHIBIT CLP-RATES-6**\
**Rate 6 - Optional Residential Space Heating Rate**

| Contents                                  | Page |
| ----------------------------------------- | ---- |
| Rate 6 Rate Design Worksheet              | 2    |
| Rate 6 Distribution Rate and Bill Impacts | 3    |

---

## Page 2 of 3 — Rate 6 Rate Design Worksheet

The Connecticut Light & Power Company dba Eversource Energy\
Docket No. 26-05-10

### Customer Charge

| Line | Item                       | Value               |
| ---- | -------------------------- | ------------------- |
| 9    | Marginal Customer Cost     | $28.98 /cust-mo     |
| 10   | GET                        | 6.80%               |
| 11   | Rate 6 Customer Charge (a) | **$30.95** /cust-mo |

Rate 6 customer charge (a) is the GET-grossed marginal customer cost: `$28.98 × 1.0680 = $30.95`.

### Distribution Energy Charge (left column)

Class-level (Rate 1 / residential) billing determinants and proposed revenues are used to set the Block 1 energy charge, then a stated discount produces winter Block 2.

| Line | Item                                      | Value             |
| ---- | ----------------------------------------- | ----------------- |
| 14   | Customer Billing Months (b)               | 12,555,775        |
| 15   | Adjusted CC Revenue (c=a×b)               | $388,609,278      |
| 17   | Proposed CC Revenue (d)                   | $155,195,848      |
| 18   | Energy Charge Adj. (e=d−c)                | $(233,413,430)    |
| 19   | Proposed EC Revenue (f)                   | $1,029,467,601    |
| 20   | Energy Charge Adj. (e)                    | $(233,413,430)    |
| 21   | Rate 6 EC Revenue Requirement (g=f−e)     | $796,054,171      |
| 23   | RY Billed kWh (h)                         | 8,440,783,007     |
| 24   | Rate 6 Block 1 Energy Charge (i=g/h)      | **$0.09431** /kWh |
| 26   | Distribution Energy Charge Discount (j)   | $(0.01914) /kWh   |
| 27   | Rate 6 Block 2 Winter Energy Charge (i+j) | **$0.07517** /kWh |

Notes on the left-column arithmetic (source formulas kept):

- Line 11 (a) × line 14 (b) is `$30.95 × 12,555,775 ≈ $388,601,236`; the printed (c) is `$388,609,278` (small rounding / input difference; transcribed as printed).
- (e=d−c) = `$155,195,848 − $388,609,278 = $(233,413,430)`.
- The label (g=f−e) uses the **absolute** energy-charge adjustment: `$1,029,467,601 − $233,413,430 = $796,054,171` (not `f` minus the signed negative e).
- (i=g/h) = `$796,054,171 / 8,440,783,007 = $0.09431/kWh`.
- Winter Block 2 = `$0.09431 + (−$0.01914) = $0.07517/kWh`.

### MC Based Distribution EC Rate Detail (right column)

This column is the cost-based build of the same winter Block 2 rate. Letter labels (j), (h), etc. reuse letters that also appear in the left column with different meanings — do not mix the two columns’ letter keys.

#### A. Base FC Recovery

| Line | Item                         | Value             |
| ---- | ---------------------------- | ----------------- |
| 15   | Marginal Facilities Cost (j) | $24.51            |
| 16   | Customer Billing Months (b)  | 12,555,775        |
| 17   | MFC Revenue (k)              | 307,742,050.2     |
| 18   | RY Billed kWh (h)            | 8,440,783,007     |
| 19   | Unit FC Recovery (l=k/h)     | $0.03646          |
| 20   | GET                          | 6.80%             |
| 21   | Unit FC Recovery (w/GET) (m) | **$0.03894** /kWh |

Source label on line 19 is `(l=k/h))` (extra closing parenthesis). (k) ≈ `$24.51 × 12,555,775`. (m) = `$0.03646 × 1.0680 = $0.03894/kWh`.

#### B. Marginal Cost

| Line | Item             | Value             |
| ---- | ---------------- | ----------------- |
| 24   | MC Energy \*\*\* | $0.00242          |
| 25   | GET              | 6.80%             |
| 26   | MC w/GET (o)     | **$0.00258** /kWh |

#### C. Subtotal (A + B)

**$0.04152 /kWh**  (`$0.03894 + $0.00258`)

#### D. Reconciliation Adjustment

| Line | Item                            | Value             |
| ---- | ------------------------------- | ----------------- |
| 31   | Rate 6 EC (block 1) (i)         | $0.09431 /kWh     |
| 32   | Unit FC Recovery (m)            | −$0.03894 /kWh    |
| 33   | Difference (n)                  | $0.05537 /kWh     |
| 35   | MC Energy (o)                   | $0.00258 /kWh     |
| 37   | Incr above MC (p = n − o)       | $0.05279 /kWh     |
| 38   | Coinc. Fct. Adj. \* (q)         | 63.7%             |
| 39   | Reduced Reconciliation Adj. (r) | **$0.03365** /kWh |

(n) = `$0.09431 − $0.03894`. (p) = `$0.05537 − $0.00258`. (r) = `$0.05279 × 63.7% ≈ $0.03365`.

#### E. Winter Block 2 Rate

**$0.07517 /kWh**  (`C + r` = `$0.04152 + $0.03365`)

This matches left-column line 27.

#### Coincidence-factor notes (lines 43–49)

\* **System Peak CF Adj**

| Item                  | Value |
| --------------------- | ----- |
| CF w/ space htg \*\*  | 25.6% |
| CF w/o space htg \*\* | 40.1% |
| Ratio (q)             | 63.7% |

Ratio = `25.6% / 40.1% ≈ 63.8%`, printed as **63.7%**.

\* source: System Peak CF Adj (see table above)\
\*\* source: Load Research\
\*\*\* source: MCOS Study

---

## Page 3 of 3 — Rate 6 Distribution Rate and Bill Impacts

The Connecticut Light & Power Company dba Eversource Energy\
Docket No. 26-05-10

### Proposed distribution rates (Rate 6 vs Rate 1)

Rate 6 is the **only** rate on this page with a seasonal 700 kWh block. Rate 1 is a single volumetric distribution energy charge.

#### Rate 6 Distribution Rates

**Customer Charge:** $30.95 /month

**Distribution Energy Charge ($/kWh)**

|                     | Heating (Nov–Mar) | Heating (Nov–Mar) | Non-Heating (Apr–Oct) | Non-Heating (Apr–Oct) |
| ------------------- | ----------------- | ----------------- | --------------------- | --------------------- |
|                     | Block 1           | Block 2           | Block 1               | Block 2               |
| Threshold           | ≤ 700 kWh         | > 700 kWh         | ≤ 700 kWh             | > 700 kWh             |
| Distribution energy | **$0.09431**      | **$0.07517**      | **$0.09431**          | **$0.09431**          |

#### Rate 1 Distribution Rates

**Customer Charge:** $12.36 /month\
**Energy Charge:** $0.12196 /kWh

### Rate Changes (Rate 6 versus Rate 1)

Boxed comparison on the source page:

| Item                      | Rate 6 − Rate 1 |
| ------------------------- | --------------- |
| Customer Charge           | $18.59 /month   |
| Energy_Charge — All Hours | $(0.02765) /kWh |
| Energy_Charge — Winter    | $(0.04679) /kWh |

`$30.95 − $12.36 = $18.59`. All-hours (Block 1) delta = `$0.09431 − $0.12196 = $(0.02765)`. Winter Block 2 delta = `$0.07517 − $0.12196 = $(0.04679)`.

### Illustrative monthly bill-impact table

Columns after the kWh split are **dollar bill deltas vs Rate 1** for the same kWh: the extra customer charge, the all-hours (non-heating / Block 1) energy delta, the winter Block 2 (heating) energy delta, and the net.

Heating months (1–3, 11–12 / Nov–Mar in a calendar-year layout) put the first 700 kWh in “Block 1 & Block 2 (Non-heating)” and kWh above 700 in “Block 2 (Heating)”. Non-heating months (4–10) put all kWh in the non-heating column; the heating-dollar column is a dash.

| Month | Total (kWh) | Block 1 & Block 2 (Non-heating) (kWh) | Block 2 (Heating) (kWh) | CC ($) | Block 1 & Block 2 (Non-heating) ($) | Block 2 (Heating) ($) | Net Bill Impact ($) |
| ----- | ----------- | ------------------------------------- | ----------------------- | ------ | ----------------------------------- | --------------------- | ------------------- |
| 1     | 1,550       | 700                                   | 850                     | 18.59  | (19.36)                             | (39.77)               | (40.54)             |
| 2     | 1,300       | 700                                   | 600                     | 18.59  | (19.36)                             | (28.08)               | (28.84)             |
| 3     | 1,000       | 700                                   | 300                     | 18.59  | (19.36)                             | (14.04)               | (14.81)             |
| 4     | 800         | 800                                   | —                       | 18.59  | (22.12)                             | —                     | (3.53)              |
| 5     | 800         | 800                                   | —                       | 18.59  | (22.12)                             | —                     | (3.53)              |
| 6     | 900         | 900                                   | —                       | 18.59  | (24.89)                             | —                     | (6.30)              |
| 7     | 1,500       | 1,500                                 | —                       | 18.59  | (41.48)                             | —                     | (22.89)             |
| 8     | 1,500       | 1,500                                 | —                       | 18.59  | (41.48)                             | —                     | (22.89)             |
| 9     | 1,000       | 1,000                                 | —                       | 18.59  | (27.65)                             | —                     | (9.06)              |
| 10    | 800         | 800                                   | —                       | 18.59  | (22.12)                             | —                     | (3.53)              |
| 11    | 1,200       | 700                                   | 500                     | 18.59  | (19.36)                             | (23.40)               | (24.16)             |
| 12    | 1,450       | 700                                   | 750                     | 18.59  | (19.36)                             | (35.10)               | (35.86)             |
| Total | 13,800      | 10,800                                | 3,000                   | 223.08 | (298.65)                            | (140.38)              | (215.95)            |

Annual net: extra customer charges of `$223.08` (`$18.59 × 12`) are more than offset by volumetric reductions of `$298.65 + $140.38`, for a **$(215.95)** illustrative annual distribution-bill decrease versus Rate 1.

Spot checks against the stated rate deltas: heating-month Block 1 dollars = `700 × $(0.02765) = $(19.355) ≈ $(19.36)`; month 1 heating Block 2 = `850 × $(0.04679) = $(39.77)`; month 7 all-hours = `1,500 × $(0.02765) = $(41.475) ≈ $(41.48)`.

The monthly kWh profile is the same shape as the CLF-001 “700 kWh annual-average Rate 1” months scaled up (winter and summer peaks), with heating-season kWh above 700 treated as incremental space-heating load.

### Chart (page 3, bottom)

The source introduces the figure under the bill-impact table (Excel rows 47–64). Title and series names are in the embedded chart image, not in the selectable worksheet text.

[DIAGRAM DESCRIPTION: Residential Rate 6 — Illustrative Regular and Incremental Consumption Profile]

A vertical stacked-bar chart of monthly kWh for calendar months 1–12 (x-axis). The y-axis is kWh from 0 to 1800 with major ticks every 200. A horizontal **black dashed line** at **700 kWh** is the Block 1 / Block 2 **Breakpoint**.

Each month is a stacked bar with up to three series (legend, left to right):

- **Regular w/o HP** — dark blue (bottom of each bar). In every month this segment reaches about the 700 kWh breakpoint (Block 1).
- **Incremental Heating Consumption** — cyan/teal. Appears only in heating months **1, 2, 3, 11, 12** (Nov–Mar), stacked above the blue segment. Bar totals match the table: ~1,550; 1,300; 1,000; 1,200; 1,450 kWh.
- **Incremental HP Consumption** — green. Appears in non-heating months **4–10** (Apr–Oct), stacked above the blue segment. Totals match the table: ~800; 800; 900; **1,500; 1,500**; 1,000; 800 kWh. Months 7–8 are the tallest green stacks (summer cooling / other incremental HP use). Months 4, 5, and 10 are only slightly above the 700 kWh line.

The chart is the visual counterpart of the kWh columns in the bill-impact table: winter usage above 700 kWh is labeled incremental **heating**; summer usage above 700 kWh is labeled incremental **HP** (not priced at the winter Block 2 discount).

[→ See original PDF page 3 for visual rendering]
