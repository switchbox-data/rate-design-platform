# New York's Energy Affordability Program: a complete guide

**New York operates the nation's most expansive state-level utility discount program for low- and moderate-income households, delivering roughly $500 million annually in monthly bill credits to over one million families.**

The Energy Affordability Program (EAP), administered under PSC Case 14-M-0565, applies fixed monthly credits to participating customers' utility bills with the goal of capping energy costs at **6% of household income**. As of January 2026, the program expanded significantly with the launch of the Enhanced Energy Affordability Program (EEAP), a two-year pilot extending eligibility to an additional **1.6 million moderate-income households** earning below their area's median income. Despite these expansions, an estimated 1.5 million eligible low-income households remain unenrolled — a participation gap that state officials and advocates continue to work to close.

---

## How the bill credit mechanism actually works

The EAP does not use a percentage discount, special rate class, or reduced tariff. Instead, it provides a **fixed monthly bill credit** — labeled "Energy Assistance Credit (EAP)" or "Enhanced Affordability Credit (EEAP)" on the bill — applied directly against the customer's delivery charges each month.

Credit amounts are **utility-specific**, **recalculated annually** by each utility based on average low-income customer bills in its service territory, and approved by the PSC. The program's design target is to reduce the energy burden of participating households to no more than **6% of gross annual income**, split roughly 3% electric and 3% gas.

Several structural features distinguish the program. Enrollment lasts **18 months** per cycle, after which customers must re-qualify. All participants are **automatically enrolled in budget billing** (levelized monthly payments), though they may opt out.

Credits differ dramatically along four dimensions: the utility serving the customer, the fuel type (electric vs. gas), whether the household uses that fuel for space heating, and the customer's tier placement. A Con Edison gas-heating customer at the highest tier receives **$189.83 per month**, while a non-heating gas customer at the same utility receives just **$3.00** — reflecting the reality that heating drives the vast majority of low-income energy burden.

The program's annual budget is capped at **2.5% of each utility's revenues** from end-use sales (raised from 2% in the July 2025 EEAP order). Con Edison alone spent approximately **$247 million** on EAP credits in the 2024–2025 program year.

---

## Seven tiers now span low-income through moderate-income households

The original EAP established four tiers for low-income customers. The EEAP, launched January 13, 2026, added three more tiers for moderate-income households, creating a comprehensive seven-tier structure.

### Traditional EAP (Tiers 1–4): how HEAP grants determine tier placement

Tier placement for the traditional EAP (Tiers 1–4) is ultimately driven by the federal **Home Energy Assistance Program (HEAP)**, which provides a one-time annual heating benefit to eligible low-income households. When a HEAP benefit posts to a customer's utility account, the customer is automatically enrolled in EAP. The dollar amount of the HEAP grant determines which EAP tier the customer lands in — and that dollar amount is itself a mechanical function of two household characteristics: **income level** and **vulnerable member status**.

#### How HEAP calculates the grant amount for utility customers

HEAP has its own two-tier income structure (distinct from EAP tiers). For households of 1–12 members, HEAP income eligibility maxes out at **60% of State Median Income**. Within that ceiling, HEAP distinguishes between lower-income and higher-income eligible households:

- **HEAP Tier I**: Gross monthly income ≤ 130% of the Federal Poverty Level (FPL), OR the household is **categorically eligible** because at least one member receives ongoing Temporary Assistance (TA/FA), SNAP, or Code A SSI.
- **HEAP Tier II**: Gross monthly income between 131% FPL and 60% SMI (the HEAP maximum).

For utility-heated customers (natural gas, PSC-regulated electric heat, or municipal electric), HEAP calculates the Regular benefit as a **base amount plus add-ons**. For the 2024–2025 and 2025–2026 program years, the structure is:

- **Base benefit** \= $400 (uniform for all utility-heated customers)
- **Tier I add-on** \= \+$61 (applied if the household qualifies as HEAP Tier I)
- **Vulnerable member add-on** \= \+$35 (applied if the household contains a **vulnerable individual**: a child under age 6, a person age 60 or older, or a person who is permanently disabled)

These add-ons are cumulative: a household can receive zero, one, or both. This produces exactly **four possible HEAP grant amounts** for utility customers:

| HEAP income tier                  | Vulnerable member? | Grant calculation  | HEAP grant |
| :-------------------------------- | :----------------- | :----------------- | :--------- |
| Tier II (131–60% SMI)             | No                 | $400               | **$400**   |
| Tier II (131–60% SMI)             | Yes                | $400 \+ $35        | **$435**   |
| Tier I (≤130% FPL or categorical) | No                 | $400 \+ $61        | **$461**   |
| Tier I (≤130% FPL or categorical) | Yes                | $400 \+ $61 \+ $35 | **$496**   |

#### How HEAP grant amounts map to EAP tiers

The PSC's May 2016 Order establishing the EAP defined tiers based on the number of **HEAP "add-on benefits"** a customer receives. As Con Edison's annual EAP filing to the PSC states: an add-on benefit is provided to HEAP recipients if their household income is at or below 130% FPL, or if their household contains a vulnerable individual; a customer can receive two add-on benefits if both conditions apply. This definition applies uniformly across all PSC-regulated utilities.

The mapping is:

| HEAP add-ons received | HEAP grant (utility) | EAP Tier   | Plain-language meaning                                                                                 |
| :-------------------- | :------------------- | :--------- | :----------------------------------------------------------------------------------------------------- |
| 0 add-ons             | $400                 | **Tier 1** | Income 131% FPL–60% SMI, no vulnerable members                                                         |
| 1 add-on              | $435 or $461         | **Tier 2** | EITHER income ≤ 130% FPL, OR has a vulnerable member (but not both)                                    |
| 2 add-ons             | $496                 | **Tier 3** | Income ≤ 130% FPL AND has a vulnerable member                                                          |
| N/A (DSS pays bill)   | N/A                  | **Tier 4** | Bills paid via Direct Vendor (DV) or Utility Guarantee (UGL) through the Department of Social Services |

#### Programmer-readable logic table: household characteristics → EAP tier

The following table consolidates the full chain from household characteristics to EAP/EEAP tier assignment. It covers all seven tiers and is intended to be precise enough to implement in code. **This logic applies to all PSC-regulated utilities.** PSEG Long Island operates a separate flat-rate program (see below).

| If income is...                          | And...                                                                                                       | Then tier is... | Enrollment pathway                                                |
| :--------------------------------------- | :----------------------------------------------------------------------------------------------------------- | :-------------- | :---------------------------------------------------------------- |
| ≤ 60% SMI                                | Bills paid by DSS via Direct Vendor or Utility Guarantee                                                     | **Tier 4**      | DSS enrollment (highest priority; overrides HEAP-based placement) |
| ≤ 130% FPL (or categorical: SNAP/TA/SSI) | Has 1+ vulnerable member (age \<6, age ≥60, or permanently disabled)                                         | **Tier 3**      | HEAP grant \= $496 (2 add-ons)                                    |
| ≤ 130% FPL (or categorical: SNAP/TA/SSI) | No vulnerable members                                                                                        | **Tier 2**      | HEAP grant \= $461 (1 add-on: income)                             |
| 131% FPL – 60% SMI                       | Has 1+ vulnerable member                                                                                     | **Tier 2**      | HEAP grant \= $435 (1 add-on: vulnerable)                         |
| 131% FPL – 60% SMI                       | No vulnerable members                                                                                        | **Tier 1**      | HEAP grant \= $400 (0 add-ons)                                    |
| ≤ 60% SMI                                | Enrolled in qualifying program (SNAP, Medicaid, SSI, TANF, etc.) but no HEAP grant posted to utility account | **Tier 1**      | Categorical enrollment / automated file match                     |
| ≤ 60% SMI                                | Emergency HEAP only (no regular HEAP grant)                                                                  | **Tier 1**      | Emergency HEAP enrollment                                         |
| ≤ 60% SMI                                | Heats with non-utility fuel (oil, propane, wood) — HEAP grant goes to fuel vendor, not utility               | **Tier 1**      | Categorical enrollment only (HEAP grant doesn't post to utility)  |
| \> 60% SMI, ≤ 60% SMI/AMI                | (EEAP) Income \< 60% SMI or AMI                                                                              | **Tier 5**      | EEAP application; credits \= Tier 1 amounts                       |
| \> 60% SMI/AMI                           | (EEAP) Income 60–80% SMI/AMI                                                                                 | **Tier 6**      | EEAP application                                                  |
| \> 80% SMI/AMI                           | (EEAP) Income 80–100% SMI/AMI                                                                                | **Tier 7**      | EEAP application; credits \= $1.00/month                          |

**Important caveats on this logic table:**

1. **Tier 4 takes priority.** If a customer's bills are paid through DSS Direct Vendor or Utility Guarantee arrangements, they are placed in Tier 4 regardless of HEAP grant amount. Tier 4 credits are typically slightly below Tier 3 (e.g., $121.84 vs. $126.21 for Con Edison electric heat).

2. **The HEAP grant thresholds ($400/$435/$461/$496) are for the 2024–2025 and 2025–2026 program years.** The base benefit ($400) and add-on amounts ($61 Tier I, $35 vulnerable) are set annually by OTDA and can change. If OTDA changes these amounts, the dollar thresholds shift but the add-on-counting logic remains the same.

3. **Some utilities express the tier mapping using dollar thresholds rather than add-on counts.** Con Edison's tariff uses thresholds of $435 and $496. NYSEG and O\&R use ranges ($21–$400 for Tier 1, $385–$461 for Tier 2, $446–$496 for Tier 3). National Grid describes it as "regular HEAP" (Tier 1), "HEAP \+ 1 add-on" (Tier 2), "HEAP \+ 2 add-ons" (Tier 3). These are all equivalent representations of the same underlying logic.

4. **HEAP Tier I categorical eligibility** means the household qualifies automatically if any member receives SNAP, Temporary Assistance (federally funded FA or Safety Net), or Code A SSI — regardless of current income. This is a common path into EAP Tier 2 or 3\.

5. **The vulnerable member definition for HEAP** is: a child under age 6, a person age 60 or older, or a permanently disabled person (typically evidenced by SSI receipt). This is defined in Chapter 9 of the HEAP Manual and is uniform statewide.

6. **EEAP (Tiers 5–7) uses AMI in New York City and Nassau County**, and SMI elsewhere. In AMI territories, the income thresholds are higher. Some downstate National Grid entities (KEDNY, KEDLI) also use AMI-based thresholds.

7. **For deliverable-fuel households** (oil, propane, wood), the HEAP base benefit is $900 (not $400), and the grant is paid to the fuel vendor rather than the utility. These households can only enroll in EAP through categorical program participation (SNAP, Medicaid, etc.), which places them in Tier 1\. They do not receive Tier 2 or 3 placement through HEAP because the HEAP grant doesn't post to a utility account.

#### HEAP income limits for the 2024–2025 program year (monthly gross income)

| Household size | HEAP Tier I (≤130% FPL) | HEAP Tier II (131% FPL – 60% SMI) |
| :------------- | :---------------------- | :-------------------------------- |
| 1              | $0 – $1,631             | $1,632 – $3,322                   |
| 2              | $0 – $2,214             | $2,215 – $4,345                   |
| 3              | $0 – $2,797             | $2,798 – $5,367                   |
| 4              | $0 – $3,380             | $3,381 – $6,390                   |
| 5              | $0 – $3,962             | $3,963 – $7,412                   |
| 6              | $0 – $4,545             | $4,546 – $8,434                   |

Source: [OTDA HEAP Desk Guide (LDSS-5005), 2024–2025](https://otda.ny.gov/policy/directives/2024/LCM/24-LCM-15-Attachment-4.pdf). Categorical eligibility (SNAP/TA/SSI receipt) places the household in HEAP Tier I regardless of income.

### Enhanced EAP / EEAP (Tiers 5–7): income-based enrollment

The EEAP pilot targets households that earn too much to qualify for HEAP or other traditional assistance programs but still face energy affordability challenges. Eligibility is based on **income as a percentage of State Median Income (SMI)**, or **Area Median Income (AMI)** in New York City and Nassau County, where living costs are higher.

- **Tier 5**: Income below 60% of SMI/AMI — receives credits equivalent to EAP Tier 1
- **Tier 6**: Income 60–80% of SMI/AMI — receives moderate credits
- **Tier 7**: Income 80–100% of SMI/AMI — receives nominal credits ($1.00/month), primarily securing enrollment in budget billing and the broader affordability infrastructure

EEAP requires self-identification and proof of income for all household members. Applications are processed through a centralized portal at **nyeeap.com** or directly through each utility.

### EEAP income thresholds by household size

Income limits vary by geography. In Con Edison's territory (NYC/Westchester), the AMI-based thresholds for a family of four are: Tier 5 up to **$97,200**, Tier 6 up to **$129,600**, and Tier 7 up to **$162,000**. In upstate and suburban territories using SMI, the same family of four would qualify at: Tier 5 up to **$80,165**, Tier 6 up to **$106,887**, and Tier 7 up to **$133,609** (Orange & Rockland thresholds). The following table shows Con Edison's full EEAP income schedule:

| Household size | Tier 5 (\<60% AMI) | Tier 6 (60–80% AMI) | Tier 7 (80–100% AMI) |
| :------------- | :----------------- | :------------------ | :------------------- |
| 1              | Up to $68,040      | $68,041–$90,720     | $90,721–$113,400     |
| 2              | Up to $77,760      | $77,761–$103,680    | $103,681–$129,600    |
| 3              | Up to $87,480      | $87,481–$116,640    | $116,641–$145,800    |
| 4              | Up to $97,200      | $97,201–$129,600    | $129,601–$162,000    |
| 5              | Up to $105,000     | $105,001–$140,000   | $140,001–$175,000    |

For utilities using statewide SMI (O\&R, NYSEG, RG\&E, Central Hudson, National Grid Upstate, National Fuel Gas), maximum 100% SMI for a household of four is **$133,609**, with 60% and 80% breakpoints proportionally lower.

---

## Credit amounts vary dramatically across utilities

Each utility calculates its own credit schedule based on local delivery rates and average low-income customer bills, producing wide disparities. The tables below present the most current credit amounts for every PSC-regulated utility. Each table includes a source link column pointing to the official document or utility page where the figures were obtained. Where values could not be confirmed, a question mark (?) is shown.

### Con Edison (effective February 1, 2026\)

| Tier     | Elec. non-heat | Elec. heat | Gas non-heat | Gas heat | Source                                                                                                             |
| :------- | :------------- | :--------- | :----------- | :------- | :----------------------------------------------------------------------------------------------------------------- |
| 1        | $33.47         | $33.47     | $3.00        | $135.24  | [Con Ed EAP page](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program) |
| 2        | $50.66         | $80.58     | $3.00        | $167.01  | [Con Ed EAP page](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program) |
| 3        | $73.47         | $126.21    | $3.00        | $189.83  | [Con Ed EAP page](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program) |
| 4        | $71.29         | $121.84    | $3.00        | $187.65  | [Con Ed EAP page](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program) |
| 5 (EEAP) | $33.47         | $33.47     | $3.00        | $135.24  | [Con Ed EAP page](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program) |
| 6 (EEAP) | $3.00          | $3.00      | $3.00        | $44.50   | [Con Ed EAP page](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program) |
| 7 (EEAP) | $1.00          | $1.00      | $1.00        | $1.00    | [Con Ed EAP page](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program) |

A Con Edison customer who heats with both electric and gas at Tier 3 could receive combined credits exceeding **$316 per month**.

### National Grid NYC Metro / KEDNY (gas only, effective December 1, 2025\)

| Tier | Gas heat | Gas non-heat | Source                                                                                                 |
| :--- | :------- | :----------- | :----------------------------------------------------------------------------------------------------- |
| 1    | $77.27   | $1.92        | [NG NYC Metro EAP page](https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program) |
| 2    | $112.97  | $1.92        | [NG NYC Metro EAP page](https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program) |
| 3    | $138.67  | $1.92        | [NG NYC Metro EAP page](https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program) |
| 4    | $139.38  | $1.92        | [NG NYC Metro EAP page](https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program) |

National Grid's NYC Metro territory (KEDNY / Brooklyn Union) is gas-only and carries the **highest gas credits** among all New York utilities, reflecting elevated gas delivery costs in the NYC metro area. EEAP credit amounts for KEDNY have not yet been published separately.

### National Grid Long Island / KEDLI (gas only, effective December 1, 2025\)

| Tier | Gas heat | Gas non-heat | Source                                                                                                               |
| :--- | :------- | :----------- | :------------------------------------------------------------------------------------------------------------------- |
| 1    | $63.76   | $3.00        | [NG Long Island EAP page](https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program) |
| 2    | $95.52   | $3.00        | [NG Long Island EAP page](https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program) |
| 3    | $118.33  | $3.00        | [NG Long Island EAP page](https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program) |
| 4    | $116.14  | $3.00        | [NG Long Island EAP page](https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program) |

National Grid Long Island (KEDLI) serves gas customers in Nassau, Suffolk, and the Rockaway Peninsula. EEAP credit amounts for KEDLI have not yet been published separately.

### National Grid Upstate / Niagara Mohawk (effective December 1, 2025\)

| Tier | Elec. heat | Elec. non-heat | Gas heat | Gas non-heat | Source                                                                                                                  |
| :--- | :--------- | :------------- | :------- | :----------- | :---------------------------------------------------------------------------------------------------------------------- |
| 1    | $22.46     | $22.46         | $3.00    | $3.00        | [NG Upstate EAP page](https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program) |
| 2    | $39.64     | $39.64         | $3.00    | $3.00        | [NG Upstate EAP page](https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program) |
| 3    | $62.45     | $62.45         | $22.49   | $3.00        | [NG Upstate EAP page](https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program) |
| 4    | $60.26     | $60.26         | $20.30   | $3.00        | [NG Upstate EAP page](https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program) |

National Grid Upstate provides **identical electric credits for heating and non-heating customers** at each tier.

### NYSEG (current)

| Tier     | Elec. heat | Elec. non-heat | Gas heat | Gas non-heat | Source                                                                                |
| :------- | :--------- | :------------- | :------- | :----------- | :------------------------------------------------------------------------------------ |
| 1        | $47.26     | $47.26         | $3.00    | $3.00        | [NYSEG EAP page](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap) |
| 2        | $67.01     | $67.01         | $5.83    | $3.00        | [NYSEG EAP page](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap) |
| 3        | $88.99     | $88.99         | $28.64   | $3.00        | [NYSEG EAP page](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap) |
| 4        | $86.16     | $86.16         | $26.45   | $3.00        | [NYSEG EAP page](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap) |
| 5 (EEAP) | $47.26     | $47.26         | $3.00    | $3.00        | [NYSEG EAP page](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap) |
| 6 (EEAP) | $18.53     | $18.53         | $3.00    | $3.00        | [NYSEG EAP page](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap) |
| 7 (EEAP) | $0.80      | $0.80          | $1.00    | $1.00        | [NYSEG EAP page](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap) |

### RG\&E / Rochester Gas and Electric (current)

| Tier     | Elec. heat | Elec. non-heat | Gas heat | Gas non-heat | Source                                                                                         |
| :------- | :--------- | :------------- | :------- | :----------- | :--------------------------------------------------------------------------------------------- |
| 1        | $25.04     | $25.04         | $3.00    | $3.00        | [RG\&E EAP page](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap) |
| 2        | $42.21     | $42.21         | $3.00    | $3.00        | [RG\&E EAP page](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap) |
| 3        | $65.03     | $65.03         | $18.14   | $3.00        | [RG\&E EAP page](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap) |
| 4        | $62.84     | $62.84         | $15.64   | $3.00        | [RG\&E EAP page](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap) |
| 5 (EEAP) | $25.04     | $25.04         | $3.00    | $3.00        | [RG\&E EAP page](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap) |
| 6 (EEAP) | $3.00      | $3.00          | $3.00    | $3.00        | [RG\&E EAP page](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap) |
| 7 (EEAP) | $1.00      | $1.00          | $1.00    | $1.00        | [RG\&E EAP page](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap) |

RG\&E and NYSEG are both Avangrid subsidiaries and follow the same tier logic, but their credit amounts differ because each calculates discounts from its own average low-income customer bills.

### Orange & Rockland (effective January 13, 2026\)

| Tier     | Elec. heat | Elec. non-heat | Gas heat | Gas non-heat | Source                                                                                                    |
| :------- | :--------- | :------------- | :------- | :----------- | :-------------------------------------------------------------------------------------------------------- |
| 1        | $76.81     | $76.81         | $9.79    | $2.40        | [O\&R EAP page](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability) |
| 2        | $92.67     | $92.67         | $37.52   | $2.40        | [O\&R EAP page](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability) |
| 3        | $114.52    | $114.52        | $69.42   | $2.40        | [O\&R EAP page](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability) |
| 4        | $111.36    | $111.36        | $76.33   | $2.40        | [O\&R EAP page](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability) |
| 5 (EEAP) | $76.81     | $76.81         | $9.79    | $2.40        | [O\&R EAP page](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability) |
| 6 (EEAP) | $43.44     | $43.44         | $23.00   | $3.00        | [O\&R EAP page](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability) |
| 7 (EEAP) | $1.00      | $1.00          | $1.00    | $1.00        | [O\&R EAP page](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability) |

### Central Hudson Gas & Electric (effective December 1, 2024\)

| Tier     | Elec. heat | Elec. non-heat | Gas heat | Gas non-heat | Source                                                                                                                                     |
| :------- | :--------- | :------------- | :------- | :----------- | :----------------------------------------------------------------------------------------------------------------------------------------- |
| 1        | $60.46     | $60.46         | $28.61   | $3.00        | [CenHud DPS filing, Jan 2025](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D) |
| 2        | $75.84     | $75.84         | $58.58   | $3.00        | [CenHud DPS filing, Jan 2025](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D) |
| 3        | $110.75    | $97.48         | $80.22   | $3.00        | [CenHud DPS filing, Jan 2025](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D) |
| 4        | $104.03    | $94.13         | $76.86   | $3.00        | [CenHud DPS filing, Jan 2025](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D) |
| 5 (EEAP) | ?          | ?              | ?        | ?            | EEAP launched Jan 2026; exact amounts not yet publicly posted                                                                              |
| 6 (EEAP) | ?          | ?              | ?        | ?            | [EEAP range: $20–$68/mo](https://highlandscurrent.org/2026/01/27/central-hudson-offers-new-discount/)                                      |
| 7 (EEAP) | $1.00      | $1.00          | $1.00    | $1.00        | [Highlands Current](https://highlandscurrent.org/2026/01/27/central-hudson-offers-new-discount/)                                           |

Central Hudson is the **only utility where Tier 3 electric-heating credits exceed electric non-heating credits** ($110.75 vs. $97.48), likely reflecting higher average usage among electric-heating customers in its territory. Application portal: [cenhud.nyeeap.com](https://cenhud.nyeeap.com/program).

### National Fuel Gas (gas only, Western NY)

| Tier | Description                        | Gas heat | Gas non-heat | Source                                                                                                                                          |
| :--- | :--------------------------------- | :------- | :----------- | :---------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | HEAP grant ≤$400                   | $1.92    | $1.92        | [NFG EAP page](https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/) |
| 2    | HEAP grant $435 or $461 (see note) | $21.73   | $1.92        | [NFG EAP page](https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/) |
| 3    | HEAP grant \= $496                 | $34.70   | $1.92        | [NFG EAP page](https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/) |
| 4    | DSS Direct Voucher                 | $22.14   | $1.92        | [NFG EAP page](https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/) |

National Fuel Gas has the **lowest credits among all utilities**, reflecting lower gas delivery costs in Western New York. Officially, NFG splits Tier 2 into sub-tiers 2.1 (HEAP $435, vulnerable add-on → $1.92) and 2.2 (HEAP $461, income add-on → $21.73). **For platform implementation we use a single Tier 2 credit of $21.73** (the higher value) and do not distinguish 2.1 vs 2.2. The company also operates LICAAP, offering discounts up to 70% off the residential rate with matching debt forgiveness.

---

## PSEG Long Island operates outside the PSC framework entirely

LIPA/PSEG Long Island is a **public power authority** governed by its own board, not regulated by the PSC. Its low-income assistance program differs fundamentally from the EAP in several ways:

The program is called the **Household Assistance Program (HAP)**, not EAP. It provides a **flat $45/month discount** to all qualifying customers with no tier system and no variation based on heating status. PSEG Long Island is electric-only (no gas service), so there is no gas component. Approximately **40,000 residential customers** are currently enrolled. Eligibility mirrors EAP qualifying programs (HEAP, SNAP, SSI, Medicaid, etc.) plus customers meeting HEAP income guidelines.

This simplified structure means PSEG Long Island customers receive less generous assistance than they would under the tiered PSC-regulated EAP. A Tier 3 electric-heating customer at Con Edison receives **$126.21/month** — nearly three times the PSEG Long Island flat benefit. PSEG Long Island was **not included** in the initial EEAP pilot launch, though it does participate in the separate Energy Affordability Guarantee pilot. The utility also operates REAP (Residential Energy Affordability Partnership), providing free home energy surveys and efficiency installations for income-eligible customers.

---

## Combined credit tables across all PSC-regulated IOUs, all tiers

The following tables compare monthly credit amounts across all utilities at every tier. This allows direct assessment of how geography affects the benefit a household receives. Gas-only utilities show "N/A" for electric credits and vice versa. PSEG Long Island's flat $45/month HAP credit is included for reference. Where EEAP values have not been published, cells show "?".

### Electric heating credits ($/month) by utility and tier

| Tier     | Con Ed  | NG Upstate | NYSEG  | RG\&E  | O\&R    | Cen. Hudson | PSEG LI |
| :------- | :------ | :--------- | :----- | :----- | :------ | :---------- | :------ |
| 1        | $33.47  | $22.46     | $47.26 | $25.04 | $76.81  | $60.46      | $45.00  |
| 2        | $80.58  | $39.64     | $67.01 | $42.21 | $92.67  | $75.84      | $45.00  |
| 3        | $126.21 | $62.45     | $88.99 | $65.03 | $114.52 | $110.75     | $45.00  |
| 4        | $121.84 | $60.26     | $86.16 | $62.84 | $111.36 | $104.03     | $45.00  |
| 5 (EEAP) | $33.47  | ?          | $47.26 | $25.04 | $76.81  | ?           | N/A     |
| 6 (EEAP) | $3.00   | ?          | $18.53 | $3.00  | $43.44  | ?           | N/A     |
| 7 (EEAP) | $1.00   | ?          | $0.80  | $1.00  | $1.00   | $1.00       | N/A     |

Notes: At most utilities, electric heating and non-heating credits are **identical** (Con Edison is the major exception — its non-heating electric credits are substantially lower: $33.47/$50.66/$73.47/$71.29 for Tiers 1–4). Central Hudson is the only utility where electric-heating exceeds non-heating credits at Tiers 3–4 ($110.75 vs. $97.48 at Tier 3). PSEG Long Island provides a flat $45/month regardless of tier or heating status.

### Electric non-heating credits ($/month) — showing only utilities where they differ from heating

| Tier     | Con Ed (non-heat) | Cen. Hudson (non-heat) |
| :------- | :---------------- | :--------------------- |
| 1        | $33.47            | $60.46                 |
| 2        | $50.66            | $75.84                 |
| 3        | $73.47            | $97.48                 |
| 4        | $71.29            | $94.13                 |
| 5 (EEAP) | $33.47            | ?                      |
| 6 (EEAP) | $3.00             | ?                      |
| 7 (EEAP) | $1.00             | $1.00                  |

For all other electric utilities (NG Upstate, NYSEG, RG\&E, O\&R), non-heating credits equal the heating credits shown in the table above.

### Gas heating credits ($/month) by utility and tier

| Tier     | Con Ed  | NG KEDNY | NG KEDLI | NG Upstate | NYSEG  | RG\&E  | O\&R   | Cen. Hudson | Nat. Fuel |
| :------- | :------ | :------- | :------- | :--------- | :----- | :----- | :----- | :---------- | :-------- |
| 1        | $135.24 | $77.27   | $63.76   | $3.00      | $3.00  | $3.00  | $9.79  | $28.61      | $1.92     |
| 2        | $167.01 | $112.97  | $95.52   | $3.00      | $5.83  | $3.00  | $37.52 | $58.58      | $21.73    |
| 3        | $189.83 | $138.67  | $118.33  | $22.49     | $28.64 | $18.14 | $69.42 | $80.22      | $34.70    |
| 4        | $187.65 | $139.38  | $116.14  | $20.30     | $26.45 | $15.64 | $76.33 | $76.86      | $22.14    |
| 5 (EEAP) | $135.24 | ?        | ?        | ?          | $3.00  | $3.00  | $9.79  | ?           | ?         |
| 6 (EEAP) | $44.50  | ?        | ?        | ?          | $3.00  | $3.00  | $23.00 | ?           | ?         |
| 7 (EEAP) | $1.00   | ?        | ?        | ?          | $1.00  | $1.00  | $1.00  | $1.00       | ?         |

Notes: For National Fuel we use a single Tier 2 credit of $21.73 (we do not distinguish sub-tiers 2.1 vs 2.2). Gas non-heating credits are uniformly minimal across all utilities ($1.92–$3.00/month at all tiers), so a separate table is not warranted.

### Key takeaways from the combined tables

**Geographic disparities are enormous and widen at higher tiers.** At Tier 3, gas heating credits range from $189.83 (Con Edison) down to $34.70 (National Fuel Gas) — a 5.5x difference. Electric heating credits range from $126.21 (Con Edison) to $22.46 (National Grid Upstate at Tier 1\) — though the Tier 3 electric range narrows somewhat ($62.45 to $126.21). These disparities reflect real differences in local delivery costs and average low-income bills, not arbitrary policy choices.

**Combined dual-fuel heating credits** at Tier 3 reach their maximum at Con Edison ($126.21 electric \+ $189.83 gas \= **$316.04**), followed by Central Hudson ($110.75 \+ $80.22 \= **$190.97**), O\&R ($114.52 \+ $69.42 \= **$183.94**), and NYSEG ($88.99 \+ $28.64 \= **$117.63**). For customers served by both a gas-only utility and PSEG Long Island, the combined maximum would be PSEG LI ($45) \+ National Grid KEDNY ($138.67) \= **$183.67**.

**Tier 5 (EEAP) mirrors Tier 1 by design** — the PSC order set Tier 5 credits equal to Tier 1 amounts. This means a moderate-income household just above the HEAP ceiling receives the same credit as the lowest-benefit traditional EAP participant.

**Tier 6 credits vary dramatically in generosity.** O\&R gives $43.44/month electric at Tier 6, while Con Edison and RG\&E give just $3.00. This reflects utility-specific calculations and the PSC's allowance for each utility to set its own Tier 6 discount level.

**Tier 7 is nominal everywhere** — $1.00/month (or $0.80 at NYSEG) across all fuel types. Its primary value is enrollment in budget billing and protections, not the credit itself.

---

## One million enrolled, but 1.5 million more remain eligible

Approximately **1 million households** currently receive traditional EAP discounts statewide. State officials estimate roughly **1.5 million additional households** are eligible but not enrolled, yielding a participation rate of approximately **40%**. When the program launched in 2016, the PSC estimated 2.3 million low-income households could ultimately qualify, and the original order aimed to reach 1.65 million.

Con Edison's territory alone accounts for a substantial share: its January 2026 annual EAP report shows roughly **519,000 accounts** receiving credits (including overlap for dual-service customers). The breakdown reveals that the overwhelming majority — **367,146 of 402,393 electric non-heating accounts** — sit in Tier 1, suggesting most enrollees qualify through categorical programs rather than high HEAP grants. Only 4,528 Con Edison customers are classified as electric-heating EAP participants.

The broader energy debt context underscores the urgency of closing this participation gap. Over **1.2 million New York families** are two or more months behind on electric bills, amounting to nearly **$2 billion in energy debt** as of 2025\. One in four state residents is "energy burdened" (spending 6% or more of income on utilities), with the figure reaching **34% of households in the Bronx**.

Recent legislation aims to boost enrollment through automated systems. Laws of 2023, Chapter 764 requires automated identification of affordability program participants, and Laws of 2024, Chapter 78 authorizes automated file matching between utilities and OTDA to identify eligible customers without requiring individual applications.

---

## Protections, pilots, and programs beyond the monthly credit

### The Energy Affordability Guarantee pilot

Approved by PSC order on August 15, 2024, and funded by a **$50 million** state budget appropriation, the Energy Affordability Guarantee (EAG) pilot guarantees that participating low-income households who have **fully electrified** their homes through NYSERDA's EmPower+ program pay no more than 6% of annual income toward electricity. The pilot targets approximately 1,000 households and launched its application portal at portal.nyeag.com in October 2025\. Participating utilities include Central Hudson, Con Edison, National Grid Upstate, PSEG Long Island, NYSEG, RG\&E, and O\&R.

### Disconnection protections and the disenrollment pause

New York's Home Energy Fair Practices Act (HEFPA) provides baseline protections for all residential customers: 15-day written notice before shutoff, cold-weather rules from November 1 through April 15 requiring utilities to verify that termination won't endanger health or safety, a holiday moratorium on disconnections, and special protections for elderly, blind, or disabled customers. Medical emergencies trigger 30-day renewable protection.

In November 2025, the PSC issued an emergency order **pausing all EAP disenrollments through November 30, 2026**, after the federal government shutdown disrupted HEAP funding and threatened to knock an estimated **97,000+ customers** off EAP rolls. The order also extended the HEAP eligibility lookback period from 12 to 18 months and required automatic re-enrollment of anyone disenrolled since October 1, 2025\. EAP customers also receive reconnection fee waivers — Con Edison eliminated these fees entirely for EAP customers as of February 1, 2026\.

### Arrears forgiveness and energy efficiency

Multiple arrears forgiveness mechanisms exist alongside EAP. The state's COVID-19 Electric and Gas Bill Relief Program delivered approximately **$672 million** in arrears relief across two phases, serving roughly 311,000 utility accounts in Phase 1 alone. Individual utilities operate their own arrears programs: Central Hudson's Powerful Opportunity Program forgives up to $2,400 over 24 months, National Grid's AffordAbility Payment Plan couples affordable monthly payments with arrearage forgiveness, and National Fuel's LICAAP matches timely payments with debt forgiveness for up to 24 months.

NYSERDA's **EmPower+** program provides no-cost energy efficiency upgrades (insulation, air sealing, heat pumps, refrigerator replacement) to households below 60% SMI, with coverage up to $10,000 per home — and up to $24,000 with supplemental IRA HEAR funding. Utilities actively refer EAP customers to EmPower+; Con Edison alone sends 500 electric and 100 gas EAP referrals every two weeks. However, NYSERDA has warned of significant budget reductions, with EmPower+ funding projected to drop from approximately **$220 million to $80 million** by 2027, potentially reducing production from 30,000 units per year to 10,000–11,000.

---

## Conclusion: an expanding but still incomplete safety net

New York's Energy Affordability Program represents one of the most structurally sophisticated utility assistance frameworks in the country — seven income-based tiers, utility-specific credit calculations targeting a uniform 6% energy burden, and integration with HEAP, EmPower+, and the new Energy Affordability Guarantee. The January 2026 EEAP expansion was a watershed moment, potentially doubling the eligible population by extending benefits to households earning up to median income.

Three critical tensions define the program's current trajectory. First, the **participation gap** remains enormous — with roughly 60% of eligible low-income households still unenrolled, automated file matching and outreach improvements are essential but unproven at scale. Second, the **wide disparity in credit amounts across utilities** means that a low-income household's effective benefit depends heavily on geography: a Tier 3 gas-heating customer receives $189.83/month from Con Edison but just $34.70 from National Fuel Gas. Third, the program's funding sustainability faces pressure from multiple directions — the budget cap increase to 2.5% of revenues accommodates EEAP expansion, but federal funding uncertainty for HEAP (which drives EAP enrollment) and projected cuts to EmPower+ threaten the broader ecosystem on which energy affordability depends.

---

## Appendix: comprehensive credit lookup table (dataframe-ready)

The table below is designed to be loaded directly into a pandas DataFrame (or equivalent) for programmatic bill calculations. Every row is a unique combination of `utility` and `tier`. The four credit columns (`elec_heat`, `elec_nonheat`, `gas_heat`, `gas_nonheat`) give monthly credit amounts in dollars. Cells show `N/A` where the utility does not provide that service, and `?` where EEAP amounts have not yet been published.

**National Fuel Gas and PSEG Long Island** appear in separate tables below: NFG is gas-only and we use a single Tier 2 credit ($21.73) instead of sub-tiers 2.1/2.2; PSEG LI is a flat-rate non-PSC program.

### Main reference table: PSC-regulated IOUs (standard tier structure)

| utility | tier | elec\_heat | elec\_nonheat | gas\_heat | gas\_nonheat | effective\_date | source\_url                                                                                                                                                                                                                |
| :------ | :--- | :--------- | :------------ | :-------- | :----------- | :-------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| coned   | 1    | 33.47      | 33.47         | 135.24    | 3.00         | 2026-02-01      | [https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program)                         |
| coned   | 2    | 80.58      | 50.66         | 167.01    | 3.00         | 2026-02-01      | [https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program)                         |
| coned   | 3    | 126.21     | 73.47         | 189.83    | 3.00         | 2026-02-01      | [https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program)                         |
| coned   | 4    | 121.84     | 71.29         | 187.65    | 3.00         | 2026-02-01      | [https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program)                         |
| coned   | 5    | 33.47      | 33.47         | 135.24    | 3.00         | 2026-02-01      | [https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program)                         |
| coned   | 6    | 3.00       | 3.00          | 44.50     | 3.00         | 2026-02-01      | [https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program)                         |
| coned   | 7    | 1.00       | 1.00          | 1.00      | 1.00         | 2026-02-01      | [https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program](https://www.coned.com/en/accounts-billing/payment-plans-assistance/energy-affordability-program)                         |
| kedny   | 1    | N/A        | N/A           | 77.27     | 1.92         | 2025-12-01      | [https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program](https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program)                                                             |
| kedny   | 2    | N/A        | N/A           | 112.97    | 1.92         | 2025-12-01      | [https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program](https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program)                                                             |
| kedny   | 3    | N/A        | N/A           | 138.67    | 1.92         | 2025-12-01      | [https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program](https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program)                                                             |
| kedny   | 4    | N/A        | N/A           | 139.38    | 1.92         | 2025-12-01      | [https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program](https://www.nationalgridus.com/NY-Home/Bill-Help/Energy-Affordability-Program)                                                             |
| kedli   | 1    | N/A        | N/A           | 63.76     | 3.00         | 2025-12-01      | [https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program](https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program)                                     |
| kedli   | 2    | N/A        | N/A           | 95.52     | 3.00         | 2025-12-01      | [https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program](https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program)                                     |
| kedli   | 3    | N/A        | N/A           | 118.33    | 3.00         | 2025-12-01      | [https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program](https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program)                                     |
| kedli   | 4    | N/A        | N/A           | 116.14    | 3.00         | 2025-12-01      | [https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program](https://www.nationalgridus.com/Long-Island-NY-Home/Bill-Help/Energy-Affordability-Program)                                     |
| nimo    | 1    | 22.46      | 22.46         | 3.00      | 3.00         | 2025-12-01      | [https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program](https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program)                       |
| nimo    | 2    | 39.64      | 39.64         | 3.00      | 3.00         | 2025-12-01      | [https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program](https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program)                       |
| nimo    | 3    | 62.45      | 62.45         | 22.49     | 3.00         | 2025-12-01      | [https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program](https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program)                       |
| nimo    | 4    | 60.26      | 60.26         | 20.30     | 3.00         | 2025-12-01      | [https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program](https://www.nationalgridus.com/Upstate-NY-Home/Monthly-Bill-Credits/Energy-Affordability-Program)                       |
| nyseg   | 1    | 47.26      | 47.26         | 3.00      | 3.00         | 2025-12-01      | [https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap)                                                                                 |
| nyseg   | 2    | 67.01      | 67.01         | 5.83      | 3.00         | 2025-12-01      | [https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap)                                                                                 |
| nyseg   | 3    | 88.99      | 88.99         | 28.64     | 3.00         | 2025-12-01      | [https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap)                                                                                 |
| nyseg   | 4    | 86.16      | 86.16         | 26.45     | 3.00         | 2025-12-01      | [https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap)                                                                                 |
| nyseg   | 5    | 47.26      | 47.26         | 3.00      | 3.00         | 2026-01-13      | [https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap)                                                                                 |
| nyseg   | 6    | 18.53      | 18.53         | 3.00      | 3.00         | 2026-01-13      | [https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap)                                                                                 |
| nyseg   | 7    | 0.80       | 0.80          | 1.00      | 1.00         | 2026-01-13      | [https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap](https://www.nyseg.com/account/waystopay/help-with-bill/eap-and-eeap)                                                                                 |
| rge     | 1    | 25.04      | 25.04         | 3.00      | 3.00         | 2025-12-01      | [https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap)                                                               |
| rge     | 2    | 42.21      | 42.21         | 3.00      | 3.00         | 2025-12-01      | [https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap)                                                               |
| rge     | 3    | 65.03      | 65.03         | 18.14     | 3.00         | 2025-12-01      | [https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap)                                                               |
| rge     | 4    | 62.84      | 62.84         | 15.64     | 3.00         | 2025-12-01      | [https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap)                                                               |
| rge     | 5    | 25.04      | 25.04         | 3.00      | 3.00         | 2026-01-13      | [https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap)                                                               |
| rge     | 6    | 3.00       | 3.00          | 3.00      | 3.00         | 2026-01-13      | [https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap)                                                               |
| rge     | 7    | 1.00       | 1.00          | 1.00      | 1.00         | 2026-01-13      | [https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap](https://www.rge.com/en/web/rge/account/waystopay/help-with-bill/eap-and-eeap)                                                               |
| or      | 1    | 76.81      | 76.81         | 9.79      | 2.40         | 2026-01-13      | [https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability)                                       |
| or      | 2    | 92.67      | 92.67         | 37.52     | 2.40         | 2026-01-13      | [https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability)                                       |
| or      | 3    | 114.52     | 114.52        | 69.42     | 2.40         | 2026-01-13      | [https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability)                                       |
| or      | 4    | 111.36     | 111.36        | 76.33     | 2.40         | 2026-01-13      | [https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability)                                       |
| or      | 5    | 76.81      | 76.81         | 9.79      | 2.40         | 2026-01-13      | [https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability)                                       |
| or      | 6    | 43.44      | 43.44         | 23.00     | 3.00         | 2026-01-13      | [https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability)                                       |
| or      | 7    | 1.00       | 1.00          | 1.00      | 1.00         | 2026-01-13      | [https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability](https://www.oru.com/en/accounts-billing/payment-assistance/new-york/energy-affordability)                                       |
| cenhud  | 1    | 60.46      | 60.46         | 28.61     | 3.00         | 2024-12-01      | [https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D) |
| cenhud  | 2    | 75.84      | 75.84         | 58.58     | 3.00         | 2024-12-01      | [https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D) |
| cenhud  | 3    | 110.75     | 97.48         | 80.22     | 3.00         | 2024-12-01      | [https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D) |
| cenhud  | 4    | 104.03     | 94.13         | 76.86     | 3.00         | 2024-12-01      | [https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D](https://documents.dps.ny.gov/public/Common/ViewDoc.aspx?DocRefId=%7B30DDB194-0000-C335-A7B8-3015D0864D00%7D) |
| cenhud  | 5    | ?          | ?             | ?         | ?            | 2026-01-13      | [https://highlandscurrent.org/2026/01/27/central-hudson-offers-new-discount/](https://highlandscurrent.org/2026/01/27/central-hudson-offers-new-discount/)                                                                 |
| cenhud  | 6    | ?          | ?             | ?         | ?            | 2026-01-13      | [https://highlandscurrent.org/2026/01/27/central-hudson-offers-new-discount/](https://highlandscurrent.org/2026/01/27/central-hudson-offers-new-discount/)                                                                 |
| cenhud  | 7    | 1.00       | 1.00          | 1.00      | 1.00         | 2026-01-13      | [https://highlandscurrent.org/2026/01/27/central-hudson-offers-new-discount/](https://highlandscurrent.org/2026/01/27/central-hudson-offers-new-discount/)                                                                 |

### National Fuel Gas (gas only, Western NY) — implementation uses integer tiers

National Fuel officially has Tier 2 sub-tiers 2.1 ($435 HEAP → $1.92) and 2.2 ($461 HEAP → $21.73). **We use a single Tier 2 credit of $21.73** and integer tier 1–4, so NFG fits the same schema as other utilities. This utility is gas-only. Gas non-heat uses $1.92 where NFG does not publish a distinct value.

| utility | tier | gas\_heat | gas\_nonheat | effective\_date | source\_url                                                                                                                                                                                                                                                        |
| :------ | :--- | :-------- | :----------- | :-------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| nfg     | 1    | 1.92      | 1.92         | 2025-12-01      | [https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/](https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/) |
| nfg     | 2    | 21.73     | 1.92         | 2025-12-01      | [https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/](https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/) |
| nfg     | 3    | 34.70     | 1.92         | 2025-12-01      | [https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/](https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/) |
| nfg     | 4    | 22.14     | 1.92         | 2025-12-01      | [https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/](https://www.nationalfuel.com/utility/payment-assistance-programs/statewide-low-income-program-nys-energy-affordability-program/) |

### PSEG Long Island — Household Assistance Program (HAP), separate table

PSEG Long Island is a public power authority outside PSC jurisdiction. It uses a flat monthly credit with no tier structure and no fuel-type or heating-status variation. Electric-only service.

| utility | program | credit\_monthly | heating\_variation | tier\_structure | effective\_date | source\_url                                                                                                                                                                              |
| :------ | :------ | :-------------- | :----------------- | :-------------- | :-------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| psegli  | HAP     | 45.00           | None (flat)        | None (flat)     | 2025-12-01      | [https://www.psegliny.com/myaccount/customersupport/financialassistance/householdassistance](https://www.psegliny.com/myaccount/customersupport/financialassistance/householdassistance) |

### Usage notes for dataframe loading

**Column types.** In the main table and the NFG table, `tier` is an integer (1–7). `elec_heat`, `elec_nonheat`, `gas_heat`, and `gas_nonheat` should be parsed as nullable floats. Values of `N/A` indicate the utility does not provide that service (the column is structurally inapplicable). Values of `?` indicate the value exists but has not been published; treat as null/NaN for calculations.

**Lookup logic.** To calculate a customer's total monthly EAP credit, filter the table to their `utility` and `tier`, then sum the applicable credit columns. A dual-service customer (e.g., Con Edison electric \+ gas) sums the electric column matching their heating status plus the gas column matching their heating status. A customer served by two different utilities (e.g., PSEG LI electric \+ National Grid KEDNY gas) looks up each utility separately and sums the results.

**Effective dates.** Credits are recalculated annually by each utility. The `effective_date` column reflects the most recent update available at time of research (February 2026). Tiers 1–4 typically update December 1 each year when the new HEAP season begins. EEAP tiers (5–7) launched January 13, 2026 and may update on different cycles.

**Completeness.** This table covers all nine PSC-regulated investor-owned utilities plus PSEG Long Island. It does not include municipal electric utilities (e.g., Freeport, Jamestown, Plattsburgh), which are not part of the PSC's EAP framework. The three National Grid entities (KEDNY, KEDLI, Upstate/Niagara Mohawk) have EEAP Tiers 5–7 values pending publication and are omitted from the main table for those tiers to avoid false precision.
