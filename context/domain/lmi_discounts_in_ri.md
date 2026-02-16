## **I. Current RIE Residential Rate Classes**

### **Electric Rate Classes and Example Rates**

Rhode Island Energy’s publicly posted residential electric delivery rate schedules show the following delivery charges for standard residential customers. These are _delivery service_ components; _supply costs_ (the cost of electricity itself) are in addition and vary by market/season.

| Rate Code | Description                     | Fixed & Example Volumetric Rates (Delivery)                                                                                                                                                                             |
| :-------- | :------------------------------ | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A-16**  | Basic residential delivery      | **Customer Charge:** $6.00/month **Distribution:** 5.379¢/kWh **Transmission:** 4.773¢/kWh **Renewable Dist:** 1.985¢/kWh **RE Growth:** $3.22/month **LIHEAP Charge:** $0.78/month **Energy Efficiency:** \~0.195¢/kWh |
| **A-60**  | Residential low-income delivery | Same delivery structure as A-16 but eligible for low-income **discount program**.                                                                                                                                       |

As an example of how these combine with supply costs, RIE filed a _Last Resort Service_ supply price of **14.77 ¢/kWh (winter 2025–26)** for customers who do not shop for supply. That supply cost is added on top of the delivery charges above.

### 

### **Natural Gas Rate Classes and Example Rates**

The natural gas tariff includes distinct heating and non-heating residential categories with monthly customer charges and _per-therm_ volumetric charges.

The volumetric charges in the heating rates (12 and 13\) are usually slightly lower than those in the non-heating rates (10 and 11); while the low-income rates (11 and 13\) apply the total bill discounts described below.

The following table summarizes these gas rates:

| Rate Code   | Category                           | Customer Charge (per month) | Examples of Volumetric Rates (per therm)                                                                                   |
| :---------- | :--------------------------------- | :-------------------------- | :------------------------------------------------------------------------------------------------------------------------- |
| **Rate 10** | Residential Non-Heating            | $14.00                      | Distribution Adjustment Charge **\~0.6145** $/therm; Gas Cost Recovery \~0.4382 $/therm; LIHEAP Enhancement \~0.79 $/therm |
| **Rate 11** | Low-Income Residential Non-Heating | $14.00                      | Same per-therm components as Rate 10                                                                                       |
| **Rate 12** | Residential Heating                | $14.00                      | Distribution Adjustment Charge **\~0.5933** $/therm; Gas Cost Recovery \~0.4382 $/therm; LIHEAP Enhancement \~0.79 $/therm |
| **Rate 13** | Low-Income Residential Heating     | $14.00                      | Same per-therm components as Rate 12                                                                                       |

Rates 11 and 13 are the **low-income natural gas codes** used for discount application.

---

## **II. Current Low-Income Discount Program**

Rhode Island Energy offers a _bill discount_ for qualifying low-income customers on applicable electric (A-60) and gas (Rates 11 and 13\) codes. To qualify, customers must participate in certain assistance programs and provide documentation.

### **Eligibility**

Customers may qualify for low-income discounts if they participate in programs such as:

- SNAP (food stamps)
- LIHEAP
- SSI
- Medicaid
- Rhode Island Works
- Public Assistance

Documentation must be provided to RIE.

### **Discount Levels**

Current low-income discount levels applied to total monthly bills are:

| Qualification                                      | Discount on Total Bill |
| :------------------------------------------------- | :--------------------- |
| SNAP, LIHEAP, or SSI                               | **25%**                |
| Medicaid, Rhode Island Works, or Public Assistance | **30%**                |

These discounts reduce what the customer owes on their total monthly bill _after delivery and supply charges are combined_.

---

## **III. RIE Proposed LIDR+ Redesign in Docket 25-45-GE**

In its November 26, 2025 filing with the Rhode Island Public Utilities Commission (RIPUC), RIE proposed revisions to the current low-income discount program, referred to as the **Revised Low-Income Discount Rate (LIDR+)**, as part of the base distribution rate case (Docket No. 25-45-GE).

It proposes expanding the number of discount tiers from two to three, and basing the tiers based on the customer’s income level rather than which programs they participate in.

### **1\. Tier Structure Based on Federal Poverty Level (FPL)**

RIE’s direct testimony (Table 7 in Blazunas testimony) defines three income tiers based on household income expressed as a percentage of the Federal Poverty Level (FPL). Those tiers are associated with increasing discount levels.

| Tier                  | Household Income (% of FPL) | Approx. Discount (Reported) |
| --------------------- | --------------------------- | --------------------------- |
| **Tier 3 (Deepest)**  | ≤ 75% of FPL                | \~**60%**                   |
| **Tier 2 (Middle)**   | 76 – 150% of FPL            | \~**30%**                   |
| **Tier 1 (Smallest)** | 151 – 250% of FPL           | \~**10%**                   |

Press coverage of the filing describes the company’s intention to tie discounts roughly to these levels, with deeper bill discounts for the lowest income households and smaller discounts for higher low-income bands. The filed testimony table itself labels the tiers (“Deepest,” “Middle,” “Smallest”) with corresponding income bands; the numeric values above are from reporting summarizing the filing.

### **2\. How LIDR+ Works**

Under the proposed redesign:

- A customer must still **enroll in a qualifying assistance program** to be eligible to enter the low-income discount program.
- Once enrolled, the customer would be **placed into an income tier** (as defined by % of FPL) based on income verification.
- The LIDR+ discount would be applied as a **percentage reduction of the total bill** (after delivery and supply charges), with the percentage reduction being based on their tier.
- The redesign does **not change the underlying structure of electric or gas tariffs**, including the distinction between electric delivery codes and gas heating vs. non-heating rate codes; it changes _how much discount_ is applied to a qualifying customer’s bill.
- For **electric service**, the LIDR+ discounts would apply to customers on the low-income electric delivery rate (A-60) who qualify and are placed into a tier.
- For **natural gas service**, discounts would apply to customers on low-income gas rate codes (Rate 11 and Rate 13\) based on their income tier. The discount percentage would be applied after volumetric and fixed charges are calculated under those codes, and volumetric rates would still be lower for heating customers.

### **3\. Legislative Context (Separate from the Rate Case)**

Separate legislative proposals in Rhode Island (e.g., bills in the General Assembly) have contemplated low-income discount programs structured around _caps on energy costs as a percentage of income (e.g., 3% for gas heating, 3% for electric non-heating, and 6% for electric heating)_. However, those legislative texts are _not the same as the LIDR+ proposal filed in Docket 25-45-GE_. The rate case filing itself uses **tiered percentage discounts** in the structure described above; it does not propose the specific fixed dollar credits or percentage-of-income caps.

### **4\. Cost-recovery rider (postprocessing)**

When modeling LIDR+ with a cost-recovery rider (so that total revenue is unchanged), the platform applies a **volumetric** allocation: the total discount cost (sum of discounts given to participants, by fuel) is divided by total non-participant consumption to obtain a rider rate.

- **Electric:** rider ($/kWh) = total electric discount cost ÷ total non-participant kWh. The rider is added to each non-participant’s electric bill in proportion to their annual kWh (from ResStock metadata).
- **Gas:** rider ($/therm) = total gas discount cost ÷ total non-participant therms. Gas consumption is taken from ResStock in kWh and converted to therms (1 therm ≈ 29.3 kWh); the rider is added to each non-participant’s gas bill in proportion to their annual therms.

This matches the volumetric cost-recovery approach used in CAIRO’s native LMI logic (see `context/tools/cairo_lmi_and_bat_analysis.md`).
