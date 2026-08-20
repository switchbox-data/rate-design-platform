# Maryland low-income / energy affordability programs

**Status:** OHEP **MEAP / EUSP is implemented** for master bills from the FY26 benefit matrices, DHS brochure, and income guidelines. Both legacy and Prefect master-bill builders dispatch the MD implementation with `--calculate-lmi`. **LIM is not implemented**: its tier → `%` / `$` schedules still require final Work Group / utility tariff inputs. A current real-world OHEP participation rate is also still needed.

**Utilities in scope:** Statewide OHEP and LIM apply to MD utilities generally; this repo currently emphasizes `bge` (`UTILITIES=bge` in `rate_design/hp_rates/md/state.env`). Also relevant: Pepco, Delmarva (DPL), SMECO, Potomac Edison, Columbia Gas, UGI, WGL.

**Sibling docs:** [lmi_discounts_in_ny.md](lmi_discounts_in_ny.md), [lmi_discounts_in_ri.md](lmi_discounts_in_ri.md). Cost recovery of assistance on the retail bill (e.g. Universal Service / EUSP) is touched in [md_residential_charges_in_bat.md](../methods/bat_mc_residual/md_residential_charges_in_bat.md).

### Citation key

| Short cite                 | Document                                                                          | Extract / public URL                                                                                                                                                                            | DocumentCloud                                                                                  |
| -------------------------- | --------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| **PC 59 Order**            | MD PSC Order No. 92190, _Order on Limited Income Mechanism_, PC 59 (Feb 12, 2026) | [Order_LIM-PC59.md](../../sources/Order_LIM-PC59.md) · [PSC PDF](https://psc.maryland.gov/wp-content/uploads/2026/02/Order_LIM-PC59.pdf)                                                        | [doc 28564978](https://www.documentcloud.org/documents/28564978-order-lim-pc59/)               |
| **FY26 MEAP matrix**       | OHEP FY26 MEAP heating grant schedule                                             | [FY26-MEAP-Benefit-Matrix.md](../../sources/FY26-MEAP-Benefit-Matrix.md) · [OHEP PDF](https://dhs.maryland.gov/documents/OHEP/Advisory%20Board/FY26-MEAP-Benefit-Matrix-2-1-1.pdf)              | [doc 28564977](https://www.documentcloud.org/documents/28564977-fy26-meap-benefit-matrix/)     |
| **FY26 EUSP matrix**       | OHEP FY26 EUSP electric grant schedule                                            | [FY26-EUSP-Benefit-Matrix.md](../../sources/FY26-EUSP-Benefit-Matrix.md) · [OHEP DOCX](https://dhs.maryland.gov/documents/OHEP/Advisory%20Board/FY26-EUSP-Benefit-Matrix-Updated-7.7.25-2.docx) | [doc 28564979](https://www.documentcloud.org/documents/28564979-fy26-eusp-benefit-matrixdocx/) |
| **OHEP brochure FY26**     | DHS OHEP English brochure (Jul 2025–Jun 2026)                                     | [OHEP PDF](https://dhs.maryland.gov/documents/OHEP/OHEP_Englishbrochure_2026.pdf)                                                                                                               | [doc 28564984](https://www.documentcloud.org/documents/28564984-ohep-englishbrochure-2026/)    |
| **Income guidelines FY26** | OHEP 200% FPG weekly / monthly / annual limits                                    | [OHEP PDF](https://dhs.maryland.gov/documents/OHEP/Advisory%20Board/Income-Guidelines-FY2026-Updated-7.9.2025.pdf)                                                                              | —                                                                                              |
| **DHS About OHEP**         | Plain-language MEAP / EUSP / arrearage / USPP overview                            | [DHS page](https://dhs.maryland.gov/office-of-home-energy-programs/about-energy-assistance/)                                                                                                    | —                                                                                              |

Inline pins use `([DocumentCloud p. N](…#document/pN/a…))` after the fact they support.

**Framing:** Maryland has (a) **today’s OHEP grant stack** (MEAP / EUSP / arrearage / USPP) and (b) a forthcoming **Limited Income Mechanism (LIM)** approved in design under PC 59, targeted for implementation **before Jan 1, 2027**.

**LIM sits on top of OHEP (not a replacement):** OHEP remains the eligibility gateway and continues to pay MEAP/EUSP grants; LIM is an **additional** utility on-bill discount/credit sized so that, **after netting existing OHEP assistance**, the customer’s remaining energy burden approaches the target (≈6%). Private charity funds and arrearage grants are **excluded** from that netting (PC 59 Order §III.3).

**Implemented modeling stance:** Default path = **MEAP + EUSP** (live programs with concrete FY26 `$` matrices). The two components can be disabled independently for sensitivity analysis, while the public result is a single net `*_lmi_*` bill per fuel and participation rate. **LIM is absent from the calculation**, rather than represented by a placeholder toggle, until final schedules are available.

---

## 1. Program landscape overview

| Program                                                                   | Role                                                                                        | Modeling relevance                               |
| ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| **OHEP**                                                                  | DHS Office of Home Energy Programs — enrollment / certification gateway                     | Eligibility backbone for MEAP, EUSP, and LIM     |
| **MEAP**                                                                  | Heating assistance **grant** (paid to heating vendor / utility)                             | **Default on** — subtract from heating-fuel bill |
| **EUSP**                                                                  | Electric assistance **grant**                                                               | **Default on** — subtract from electric bill     |
| **Arrearage assistance** (ARA / GARA etc.)                                | Past-due balance grants                                                                     | **Out of scope** for current-bill LMI            |
| **USPP**                                                                  | Winter shutoff protection + budget billing for MEAP-eligible                                | Protections only — **not** a `$` discount        |
| **LIM (PC 59)**                                                           | Forthcoming utility **on-bill discount/credit** toward ~6% energy burden **on top of** OHEP | **Toggle** — off until schedules are final       |
| **Utility-specific** (e.g. Columbia AMP / HeatShare; UGI Operation Share) | Company hardship / arrearage offerings                                                      | Out of initial BGE-first scope                   |

**MEAP and EUSP in one paragraph:** Both are **OHEP grants** administered by DHS (DHS About OHEP). **MEAP** (Maryland Energy Assistance Program / LIHEAP) helps with **home heating** bills; the grant is paid to the fuel supplier or utility once per program year (July–June) (OHEP brochure FY26). **EUSP** (Electric Universal Service Program) helps with **electric** bills; also once per program year; recipients may optionally enroll in utility budget billing. Households **can and commonly do** receive both; the PC 59 Order notes that only ~**0.5%** of MEAP recipients lack EUSP ([DocumentCloud p. 8](https://www.documentcloud.org/documents/28564978-order-lim-pc59/#document/p8/a2826957)).

**Who is generally eligible (brochure):** Renters, homeowners, residents of public housing, sub-metered homes, and roomers/boarders. Primary screens: **household size** and **income from the last 30 days** ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28564984-ohep-englishbrochure-2026/#document/p1/a2826961)). FY26 income limits for MEAP and EUSP are published at **200% of the federal poverty guidelines** by household size (OHEP brochure FY26; Income guidelines FY26). Households receiving **SNAP** or **TCA** in Maryland are **categorically eligible** and need not complete a separate application (OHEP brochure FY26).

**Forthcoming LIM (one paragraph):** Under PUA § 4-309 and PC 59 Order, utilities must adopt a Limited Income Mechanism. The Commission approves a tiered, OHEP-based design aiming toward ~**6%** energy burden ([DocumentCloud p. 7](https://www.documentcloud.org/documents/28564978-order-lim-pc59/#document/p7/a2826958)), implementable **prior to January 1, 2027** ([DocumentCloud p. 4](https://www.documentcloud.org/documents/28564978-order-lim-pc59/#document/p4/a2826960)). LIM **supplements** OHEP; it does not replace MEAP/EUSP (PC 59 Order §I, §III, §VI.1).

---

## 2. Eligibility criteria

### 2.1 OHEP / MEAP / EUSP (current)

#### Overall eligibility (both programs)

| Fact                     | Value                                                                                 | Cite                                                                             |
| ------------------------ | ------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Administering agency     | Maryland DHS **OHEP**                                                                 | DHS About OHEP                                                                   |
| Program year             | **July 1 – June 30** (FY26 brochure covers Jul 2025–Jun 2026)                         | OHEP brochure FY26                                                               |
| Who may apply            | Renters, homeowners, public housing, sub-metered, roomers/boarders                    | OHEP brochure FY26                                                               |
| Income screen (standard) | ≤ **200% FPG** by household size (same published table for MEAP and EUSP)             | OHEP brochure FY26; Income guidelines FY26                                       |
| Income measurement       | **Last 30 days** of household income (brochure)                                       | OHEP brochure FY26                                                               |
| Categorical path         | **SNAP** or **TCA** recipients in MD — benefits issued after SNAP/TCA redetermination | OHEP brochure FY26                                                               |
| Form of benefit          | Fixed `$` **annual grant**, not a % of the bill                                       | FY26 MEAP / EUSP matrices; brochure (“all energy assistance dollars are grants”) |

**FY26 monthly income limits (200% FPG)** — OHEP brochure FY26 ([DocumentCloud p. 2](https://www.documentcloud.org/documents/28564984-ohep-englishbrochure-2026/#document/p2/a2826956)):

| HH size     | 1     | 2     | 3     | 4     | 5     | 6     | 7     | 8     |
| ----------- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| Monthly `$` | 2,608 | 3,525 | 4,441 | 5,358 | 6,275 | 7,191 | 8,108 | 9,025 |

(Annual limits also published; for HH of 11+, income guidelines note use of **60% SMI** — see Income guidelines FY26.)

#### Poverty / Benefit Levels 1–7

OHEP assigns a **Poverty Level** (also called Benefit Level on the matrices). Levels **1–5** are FPL% bands. Levels **6** and **7** are **not** sequential FPL bands — they are special categories.

| Level | Label on FY26 matrices        | MEAP column label          | EUSP column label                                              |
| ----- | ----------------------------- | -------------------------- | -------------------------------------------------------------- |
| **1** | 0–25% FPL                     | same                       | same                                                           |
| **2** | 26–50% FPL                    | same                       | same                                                           |
| **3** | 51–100% FPL                   | same                       | same                                                           |
| **4** | 101–150% FPL                  | same                       | same                                                           |
| **5** | 151–200% FPL                  | same                       | same                                                           |
| **6** | **Not an FPL band**           | “Subsidized / Sub-metered” | “Subsidized / Roomer / Boarder and Sub-metered” (all HH sizes) |
| **7** | **Not a standard ≤200% band** | “Over 200% FPL”            | “>200% FPL, Categorically Eligible Only” (all HH sizes)        |

**Programmer-readable default (Levels 1–5 only):**

```
if   fpl_pct <= 25:  level = 1
elif fpl_pct <= 50:  level = 2
elif fpl_pct <= 100: level = 3
elif fpl_pct <= 150: level = 4
elif fpl_pct <= 200: level = 5
else:                ineligible for L1–L5 path  # see §5 for L6/L7
```

**Still open (Ops Manual):** exact landlord / “responsibility for energy costs” rules; whether MEAP and EUSP ever use different Poverty Level assignments for the same household; full categorical list beyond SNAP/TCA.

#### MEAP-specific

| Fact                | Value                                       | Cite                     |
| ------------------- | ------------------------------------------- | ------------------------ |
| Purpose             | Home **heating** assistance                 | DHS About OHEP; brochure |
| Frequency           | **Once per program year**                   | Brochure                 |
| Payment             | Delivered to **heating or utility company** | Brochure                 |
| Lookup keys for `$` | `(Poverty Level, heating fuel)`             | FY26 MEAP matrix         |
| Fuels on matrix     | Electric, Gas, Oil, Propane, Wood/Coal      | FY26 MEAP matrix         |

#### EUSP-specific

| Fact                | Value                                                            | Cite                     |
| ------------------- | ---------------------------------------------------------------- | ------------------------ |
| Purpose             | **Electric** bill assistance                                     | DHS About OHEP; brochure |
| Frequency           | **Once per program year**                                        | Brochure                 |
| Budget billing      | Optional for EUSP recipients                                     | Brochure                 |
| Lookup keys for `$` | `(Poverty Level, primary heat source, annual electric kWh band)` | FY26 EUSP matrix         |
| kWh bands           | 0–4,000; 4,001–8,000; 8,001–12,000; >12,000                      | FY26 EUSP matrix         |

#### Stacking MEAP and EUSP

- Both are OHEP grants; a household **may receive both** in the same program year.
- Order evidence that dual receipt is the norm: only ~**0.5%** of MEAP recipients lack EUSP ([DocumentCloud p. 8](https://www.documentcloud.org/documents/28564978-order-lim-pc59/#document/p8/a2826957)).
- We do **not** have a cite that every income-eligible household _must_ get both; model “both” as the default for dual-eligible homes, with an optional sensitivity for MEAP-only.

### 2.2 Limited Income Mechanism (LIM / PC 59)

**Eligibility (quoted):**

> “The mechanism proposed by the Work Group will be available to all residential utility customers who are certified by the OHEP as eligible, and is in keeping with the Commission’s directive for the model mechanism’s standard for eligibility. This proposed eligibility includes residential utility customers categorized by OHEP as Poverty Level 6 or lower, which is effectively 200 percent of the federal Poverty Limit (“FPL”) or lower.”
>
> — PC 59 Order §III.1 ([DocumentCloud p. 8](https://www.documentcloud.org/documents/28564978-order-lim-pc59/#document/p8/a2826959))

**Coverage gaps when tying LIM to OHEP (quoted):**

> “…approximately 0.5 percent of customers receiving benefits through … MEAP do not receive … EUSP benefits, so they may not show up in the utility’s system. This also means that, at this time, the proposed LIM does not extend to master meter customers who lack a unique utility account….”
>
> — PC 59 Order §III.1 ([DocumentCloud p. 8](https://www.documentcloud.org/documents/28564978-order-lim-pc59/#document/p8/a2826957))

| Fact                   | Value                                                                   | Cite                                                                                                                             |
| ---------------------- | ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Who                    | Residential customers **certified by OHEP**                             | PC 59 Order §III.1                                                                                                               |
| Income / Poverty Level | Poverty Level **6 or lower** ≈ **≤200% FPL**; Level 7 excluded from LIM | PC 59 Order §III.1 ([DocumentCloud p. 8](https://www.documentcloud.org/documents/28564978-order-lim-pc59/#document/p8/a2826959)) |
| Enrollment             | Driven by OHEP certification                                            | PC 59 Order §III.1                                                                                                               |
| Tiers                  | OHEP Poverty Level **and heating source**                               | PC 59 Order §III.2, §VI.1                                                                                                        |
| Master-metered         | **Not** covered initially                                               | PC 59 Order §III.1, §VI.10                                                                                                       |

**Still needed for LIM code:** Work Group report / Fall 2026 tariffs for tier → `$/kWh` or flat credit.

### 2.3 Utility-specific pathways (LIM)

| Fact               | Value                                                   | Cite               |
| ------------------ | ------------------------------------------------------- | ------------------ |
| SMECO vs others    | **Percent-of-rate** except SMECO → **flat bill credit** | PC 59 Order §VI.2  |
| Columbia Gas / UGI | Not exempted yet; must file alternative plans           | PC 59 Order §VI.9  |
| Retail choice      | Shopping does **not** change LIM credit size            | PC 59 Order §III.3 |

---

## 3. How discounts / benefits are applied

### 3.1 MEAP and EUSP grant amounts (FY26)

Both programs pay a **fixed annual** `$` **grant** (not a percentage of the bill). The implementation allocates each annual grant across the relevant fuel's monthly master-bill rows **in proportion to each month's share of that fuel's annual bill**. It does not divide the grant into equal twelfths. For a fuel with annual grant `G`, monthly bills `B_m`, and annual monthly-bill sum `B`, the discounted month is:

```
fraction remaining = max(0, 1 - G / B)
discounted month   = B_m * fraction remaining
```

This uses the sum of Jan–Dec as `B`, then rebuilds the Annual row as the sum of the 12 discounted months. Therefore the full grant is consumed when `G <= B`, no month becomes negative, and the maximum modeled assistance when `G > B` is the full annual bill.

#### MEAP annual `$` by Level × heating fuel

Source: [FY26-MEAP-Benefit-Matrix.md](../../sources/FY26-MEAP-Benefit-Matrix.md) ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28564977-fy26-meap-benefit-matrix/#document/p1/a2826952))

| Fuel      | L1 (0–25%) | L2 (26–50%) | L3 (51–100%) | L4 (101–150%) | L5 (151–200%) | L6 (sub/sub-metered) | L7 (>200%) |
| --------- | ---------- | ----------- | ------------ | ------------- | ------------- | -------------------- | ---------- |
| Electric  | 100        | 100         | 100          | 100           | 100           | 100                  | 25         |
| Gas       | 550        | 475         | 400          | 360           | 300           | 225                  | 25         |
| Oil       | 1,100      | 990         | 880          | 770           | 650           | 225                  | 25         |
| Propane   | 1,000      | 930         | 830          | 725           | 600           | 225                  | 25         |
| Wood/Coal | 550        | 475         | 400          | 360           | 300           | 225                  | 25         |

**Fuel split:** apply MEAP against the **heating** bill (gas / oil / propane / electric heat as appropriate). Electric-heat MEAP is small (`$100` for L1–L5); electric-heat households typically also receive a larger **EUSP** grant.

#### EUSP annual `$` by Level × primary heat × kWh band

Source: [FY26-EUSP-Benefit-Matrix.md](../../sources/FY26-EUSP-Benefit-Matrix.md). Keyed by `(Poverty Level, primary heat source, annual electric kWh band)`. Within every level the four non-electric fuels (Gas / Oil-Kerosene / Propane / Wood-Coal) carry **identical** amounts, so they are shown as one row.

kWh bands: **B1** = 0–4,000 · **B2** = 4,001–8,000 · **B3** = 8,001–12,000 · **B4** = >12,000.

**Level 1 — 0–25% FPL** ([DocumentCloud p. 1](https://www.documentcloud.org/documents/28564979-fy26-eusp-benefit-matrixdocx/#document/p1/a2826953))

| Primary heat                    | B1  | B2  | B3  | B4    |
| ------------------------------- | --- | --- | --- | ----- |
| Electric                        | 875 | 900 | 950 | 1,000 |
| Gas / Oil / Propane / Wood-Coal | 350 | 400 | 450 | 500   |

**Level 2 — 26–50% FPL**

| Primary heat                    | B1  | B2  | B3  | B4  |
| ------------------------------- | --- | --- | --- | --- |
| Electric                        | 800 | 850 | 900 | 950 |
| Gas / Oil / Propane / Wood-Coal | 300 | 350 | 400 | 450 |

**Level 3 — 51–100% FPL**

| Primary heat                    | B1  | B2  | B3  | B4  |
| ------------------------------- | --- | --- | --- | --- |
| Electric                        | 750 | 800 | 850 | 900 |
| Gas / Oil / Propane / Wood-Coal | 250 | 300 | 350 | 400 |

**Level 4 — 101–150% FPL** ([DocumentCloud p. 2](https://www.documentcloud.org/documents/28564979-fy26-eusp-benefit-matrixdocx/#document/p2/a2826954))

| Primary heat                    | B1  | B2  | B3  | B4  |
| ------------------------------- | --- | --- | --- | --- |
| Electric                        | 700 | 750 | 800 | 850 |
| Gas / Oil / Propane / Wood-Coal | 225 | 250 | 300 | 350 |

**Level 5 — 151–200% FPL**

| Primary heat                    | B1  | B2  | B3  | B4  |
| ------------------------------- | --- | --- | --- | --- |
| Electric                        | 650 | 700 | 750 | 800 |
| Gas / Oil / Propane / Wood-Coal | 175 | 200 | 250 | 300 |

**Level 6 — Subsidized / Roomer / Boarder and Sub-metered (all HH sizes)** ([DocumentCloud p. 3](https://www.documentcloud.org/documents/28564979-fy26-eusp-benefit-matrixdocx/#document/p3/a2826955))

| Primary heat                    | B1  | B2  | B3  | B4  |
| ------------------------------- | --- | --- | --- | --- |
| Electric                        | 550 | 600 | 650 | 700 |
| Gas / Oil / Propane / Wood-Coal | 125 | 150 | 200 | 250 |

**Level 7 — >200% FPL, Categorically Eligible Only (all HH sizes)**

| Primary heat                    | B1 | B2 | B3 | B4 |
| ------------------------------- | -- | -- | -- | -- |
| Electric                        | 25 | 25 | 25 | 25 |
| Gas / Oil / Propane / Wood-Coal | 25 | 25 | 25 | 25 |

**Fuel split:** apply EUSP against the **electric** bill only. Electric primary heat pays substantially more than other fuels at the same level and band (e.g. L1 electric `$875`–`$1,000` vs L1 non-electric `$350`–`$500`).

#### Other OHEP programs — not in current-bill LMI

| Program                          | Why exclude from `*_lmi_*`                                                                       |
| -------------------------------- | ------------------------------------------------------------------------------------------------ |
| ARA / GARA                       | Past-due balance grants, not current charges (brochure); Order excludes arrearages from LIM calc |
| USPP                             | Shutoff protection + budget billing — no grant `$` off current charges                           |
| Private charity (e.g. Fuel Fund) | Explicitly excluded from LIM benefit calculation (Order §III.3)                                  |

| Fact                 | Value                                                                                                             | Cite               |
| -------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------ |
| Interaction with LIM | LIM benefits determined **after considering** EUSP, MEAP, supplemental OHEP; exclude private funds and arrearages | PC 59 Order §III.3 |

### 3.2 Limited Income Mechanism (LIM) bill mechanics

**Structure and formula (quoted):**

> “The proposed mechanism employs a tiered discount structure that groups customers by OHEP Poverty Level group identification and heating source… The discount or credit is calculated by subtracting the Target Energy Burden Threshold … from the Applicable Bill Net of OHEP Assistance…”
>
> — PC 59 Order §III.2

```
Target Energy Burden Threshold   = Average Income                × Energy Burden Percentage
Applicable Bill Net of OHEP       = Average Applicable Charges    − Average Existing OHEP Assistance
Discount Needed (LIM credit)      = Applicable Bill Net of OHEP   − Target Energy Burden Threshold
```

Where all quantities are **group averages** (per Poverty Level × heating-source group); Energy Burden Percentage is the target (≈ 6%); and Average Existing OHEP Assistance = MEAP + EUSP + supplemental.

| Fact                  | Value                                      | Cite                                                                                                                             |
| --------------------- | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| Form (most utilities) | **Percent-of-rate** (`$/kWh` or `$/therm`) | PC 59 Order §VI.2                                                                                                                |
| Form (SMECO)          | **Flat bill credit**                       | PC 59 Order §VI.2                                                                                                                |
| Bill base             | **Supply + distribution**                  | PC 59 Order §VI.7                                                                                                                |
| Timeline              | Implement **before Jan 1, 2027**           | PC 59 Order §VI.10 ([DocumentCloud p. 4](https://www.documentcloud.org/documents/28564978-order-lim-pc59/#document/p4/a2826960)) |

**Still needed:** tier → `$/kWh` (or flat `$`) from Work Group report / utility tariffs.

### 3.3 Cost recovery on non-participant bills

| Fact         | Value                                                                                                                                   | Cite                               |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------- |
| LIM recovery | All ratepayers; soft cap deferred; residential vs C&I split deferred                                                                    | PC 59 Order §III.8–9, §VI.5, §VI.8 |
| EUSP today   | Universal Service Charge on bills — see [md_residential_charges_in_bat.md](../methods/bat_mc_residual/md_residential_charges_in_bat.md) | In-repo BAT notes                  |

---

## 4. Participation in the real world today

| Fact                                   | Value                                                                                          | Cite                                                                                                                             |
| -------------------------------------- | ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Dual MEAP+EUSP                         | ~**0.5%** of MEAP recipients lack EUSP (implies dual receipt is typical among MEAP recipients) | PC 59 Order §III.1 ([DocumentCloud p. 8](https://www.documentcloud.org/documents/28564978-order-lim-pc59/#document/p8/a2826957)) |
| LIM pool (future)                      | OHEP-certified, Poverty Level ≤6                                                               | PC 59 Order §III.1, §VI.1                                                                                                        |
| **Enrollment / eligible take-up rate** | **Still needed** — no statewide participation % in sources used so far                         | —                                                                                                                                |

**Modeling until rates are known:** support NY-style **p100** (all income-eligible) and a parameterized `participation_rate`; fill the real rate when OHEP / PC 53 / Jul 2026 PC 59 utility count filings are available.

---

## 5. Programmatic implementation

### 5.1 Files and responsibilities

- **Benefit configuration — `utils/post/data/md_ohep_benefits.yaml`:** executable FY26 program year, 2025 FPL guideline year, modeled Poverty Levels 1–5, EUSP kWh bands, and complete MEAP/EUSP annual grant matrices. The file links to the source extracts.
- **Shared helpers — `utils/post/lmi_common.py`:** loads and minimally validates the YAML; assigns MD OHEP levels and EUSP kWh bands; flattens both benefit matrices into join-ready Polars DataFrames; and supplies shared FPL, CPI, and participation helpers.
- **MD application — `utils/post/apply_md_ohep_to_master_bills.py`:** builds one OHEP profile per building, samples participation, joins profiles to monthly master bills, applies MEAP/EUSP to the correct fuels, rebuilds Annual rows, validates the result, and provides a standalone CLI.
- **Automatic builders — `utils/post/build_master_bills.py` and `utils/post/build_master_bills_prefect.py`:** dispatch to `apply_md_ohep_to_master()` for state MD when `--calculate-lmi` is set.
- **Tests — `tests/test_md_ohep_discounts.py`:** cover tier and kWh boundaries, source-matrix lookups, heating-fuel mapping, MEAP/EUSP stacking and independent toggles, proportional allocation, bill floors, participation behavior, and both builder dispatch paths.

### 5.2 Inputs and profile construction

For every `bldg_id` in a segment, the implementation reads:

1. `metadata-sb.parquet` for the segment's ResStock upgrade: occupants, representative income, vacancy, heating fuel, and heat-pump status.
2. The consolidated `load_curve_annual` parquet for the same state and upgrade: `out.electricity.total.energy_consumption.kwh`.
3. CPI data from `s3://data.sb/fred/cpi/` to convert ResStock's 2019-dollar income to the selected FPL guideline year.
4. The FPL guideline and FY26 benefit YAMLs.

Vacant units are excluded from profile assignment. The remaining profiles compute household-size FPL thresholds, FPL%, OHEP Poverty Level 1–5, primary heating fuel, and EUSP kWh band. Heat-pump homes are treated as electric-heated even when `in.heating_fuel` retains the pre-retrofit fuel.

### 5.3 Per-building grant and bill algorithm

1. **Filter** vacant units; require ResStock income + occupants ([resstock_lmi_metadata_guide.md](../../code/data/resstock_lmi_metadata_guide.md)).
2. **Compute FPL%** from `representative_income` + `occupants`, after inflating income to the selected FPL guideline year.
3. **Assign Poverty Level 1–5** via the band table in §2.1. **Do not assign L6/L7** on the default path (see §6).
4. **Determine primary heating fuel** from `in.heating_fuel`, overriding heat-pump homes to Electric, then map to Electric / Gas / Oil-Kerosene / Propane / Wood-Coal YAML keys.
5. **MEAP:** join `(level, primary heating fuel)` to `meap_annual_benefit`. Apply it to the matching heating-fuel bill.
6. **EUSP:** assign the annual electric kWh band; join `(level, primary heating fuel, kWh band)` to `eusp_annual_benefit`. Apply it to the electric bill.
7. **Stack:** electric-heated homes receive `EUSP + MEAP` on electric. Non-electric homes receive EUSP on electric and MEAP on gas, oil, or propane.
8. **Participation:** sample the eligible pool at each requested rate. `uniform` gives eligible buildings equal probability; `weighted` favors lower FPL% using inverse-FPL weights. A fixed seed makes selection deterministic.
9. **Allocate annual grants:** for each participating building and fuel, calculate one fraction from the sum of Jan–Dec bills and its annual grant, multiply every month by that fraction, and rebuild Annual from the discounted months.
10. **Validate:** reject null/negative LMI bills, LMI bills above base bills, Annual/monthly inconsistencies, fuel-total inconsistencies, and p100 electric eligibility/application mismatches.

`elec_lmi_tier` / `is_lmi_elec` mean that a modeled grant lands on the electric bill: positive EUSP, or positive MEAP for electric heat. The gas/oil/propane equivalents mean that MEAP lands on that fuel. `is_lmi_any` is the union used as the participation pool.

### 5.4 ResStock field map

| Need         | ResStock / derived                                                                                                                    |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| Income       | `in.representative_income` (+ inflation)                                                                                              |
| HH size      | `in.occupants`                                                                                                                        |
| FPL%         | computed                                                                                                                              |
| Heating fuel | `in.heating_fuel`; `postprocess_group.has_hp` overrides to electric                                                                   |
| Annual kWh   | `load_curve_annual/.../{STATE}_upgrade{UPGRADE}_metadata_and_annual_results.parquet` → `out.electricity.total.energy_consumption.kwh` |
| Vacancy      | `in.vacancy_status`                                                                                                                   |

### 5.5 Running the integrated Prefect build

From `rate_design/hp_rates/`, build every completed segment in one Prefect batch and append p100 and p40 OHEP columns:

```bash
just s md build-master-bills-prefect <batch> \
  --calculate-lmi \
  --lmi-participation-rates 1.0 0.4 \
  --lmi-participation-mode weighted \
  --lmi-calculation-type monthly
```

`--lmi-calculation-type monthly` identifies the monthly output mode; MD always uses the proportional annual-grant allocation described above. The command reconstructs master bills from CAIRO source outputs and rewrites the existing `comb_bills_year_target/` prefixes for the batch. CAIRO run outputs are not modified.

The Prefect builder currently applies LMI **after** writing each per-utility table but **before** writing the Hive-partitioned `all_utilities` table. Therefore the LMI columns are present at:

```
s3://data.sb/switchbox/cairo/outputs/hp_rates/md/all_utilities/<batch>/<segment>/comb_bills_year_target/
```

They are not present in the per-utility copy written during the same build. Analysis of MD OHEP results must use `all_utilities`.

The standalone Just recipes can also augment an existing master table:

```bash
just s md apply-md-ohep-to-master-bills <batch> <segment>
```

or `apply-md-ohep-to-existing-master-bills` with an explicit input/output path.

### 5.6 Output columns

- **Eligibility/profile:** `ohep_poverty_level`, `primary_heating_fuel`, `annual_electric_kwh`, `eusp_kwh_band`, fuel-specific `*_lmi_tier`, fuel-specific `is_lmi_*`, `is_lmi_any`, and `has_unmodeled_meap_fuel`.
- **Per participation rate:** `elec_total_bill_lmi_{pct}`, `gas_total_bill_lmi_{pct}`, `oil_total_bill_lmi_{pct}`, `propane_total_bill_lmi_{pct}`, `energy_total_bill_lmi_{pct}`, and fuel-specific `applied_discount_*_{pct}`.
- **Optional standalone debug columns:** `meap_annual_credit_{pct}` and `eusp_annual_credit_{pct}` with `--keep-component-columns`.
- **Component toggles:** standalone `--exclude-meap` and `--exclude-eusp`; both are enabled by default.

LIM has no output columns and is not applied.

---

## 6. Limitations and modeling flags

| Topic                                      | Limitation / decision                                                                                                                                                                                                                                                                                                                                        | Severity                    |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------- |
| **Levels 6 and 7**                         | Matrix labels are housing/metering (L6) or >200% + categorical-only (L7), **not** FPL bands. ResStock does not cleanly identify “subsidized / roomer / boarder / sub-metered” for L6, nor categorical-only >200% for L7. **Default: assign only L1–L5 from FPL%; treat L6/L7 as** `$0` **/ out of scope** until Ops Manual + housing fields justify a proxy. | High                        |
| **Stacking**                               | Model **both** MEAP and EUSP for dual-eligible homes as the common case; ~0.5% MEAP-without-EUSP exists (Order). Optional sensitivity: MEAP-only.                                                                                                                                                                                                            | Medium                      |
| **Fuel split**                             | MEAP → heating fuel bill; EUSP → electric (amount depends on heat source + kWh). Wrong fuel assignment double-counts or misses assistance.                                                                                                                                                                                                                   | High                        |
| **Wood/coal heating bills**                | ResStock `Other Fuel` is used as the closest proxy for the matrix's Wood/Coal row, but master bills have no wood/coal bill column. The script still applies EUSP and flags `has_unmodeled_meap_fuel`; it does not subtract the scheduled MEAP amount from an unrelated fuel bill.                                                                            | Medium                      |
| **Exclude from current-bill LMI**          | ARA/GARA (arrears), USPP (protection only), private charity.                                                                                                                                                                                                                                                                                                 | High                        |
| **Income timing**                          | Brochure uses **last 30 days** income; ResStock `representative_income` is an **annual** ACS-based proxy (2019$ in 2024.2). Document as approximation.                                                                                                                                                                                                       | Medium                      |
| **Sub-metered / master-meter**             | Brochure: sub-metered / roomers eligible for OHEP grants. LIM initially **does not** cover master-meter customers without unique utility accounts (Order). ResStock may not flag master-meter cleanly.                                                                                                                                                       | Medium                      |
| **Annual-to-month allocation**             | OHEP grants are annual. The model spreads each grant proportionally across Jan–Dec so no credit is lost to a low-bill month; this is a modeling allocation, not a claim that OHEP posts 12 proportional credits operationally.                                                                                                                               | Medium                      |
| **Participation rate**                     | Unknown — ship p100 + parameterized rate.                                                                                                                                                                                                                                                                                                                    | High for “real world today” |
| **LIM not implemented**                    | Do not include LIM until tariffs / `$/kWh` (or flat credits) are final. There is currently no `include_lim` code path.                                                                                                                                                                                                                                       | High                        |
| **Order wording vs matrices on “Level 6”** | Order ≈ “Poverty Level 6 or lower ≈ ≤200% FPL” for LIM eligibility; matrices use Level 6 for subsidized/sub-metered. Treat Order’s “≤6 ≈ ≤200% FPL” as referring to the **income-eligible OHEP pool through L5 (+ whatever OHEP calls level 6 in eligibility systems)** — reconcile carefully when wiring LIM.                                               | Medium                      |

---

## 7. Implications for rate-design modeling

| Topic                      | Implication                                                           | Confidence     |
| -------------------------- | --------------------------------------------------------------------- | -------------- |
| OHEP implementation        | **MEAP + EUSP** annual grants → proportional monthly bill reductions  | Implemented    |
| LIM                        | Not implemented until schedules are final                             | High           |
| Eligibility proxy          | FPL% ≤ 200% → Levels 1–5                                              | High for L1–L5 |
| Discount shape (OHEP)      | Fixed annual `$` grants allocated proportionally across monthly bills | High           |
| Discount shape (LIM later) | Closer to RI % of rate                                                | High           |
| Net bill column            | Single `*_lmi_*`; internal MEAP/EUSP/LIM columns                      | High           |
| Script                     | `apply_md_ohep_to_master_bills.py` → master bills via `lmi_common.py` | Implemented    |

---

## 8. Key open questions

1. Real-world **OHEP participation / take-up** rate (eligible vs enrolled).
2. Ops Manual confirmation of operational **EUSP posting** and **L6/L7** assignment rules; the model's proportional allocation is an analytical choice.
3. ResStock (or other) proxy for **sub-metered / subsidized / roomer** if L6 is ever in scope.
4. LIM **tier →** `$/kWh` **/ flat** `$` from Work Group report / Fall 2026 tariffs.
5. Soft-cap / cost-allocation outcomes for LIM non-participant riders.

---

## Appendix A. Research sources

### A.1 Current OHEP (MEAP / EUSP)

| Source                                                                                                                                   | Why look here                                             |
| ---------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| [DHS OHEP hub](https://dhs.maryland.gov/office-of-home-energy-programs/)                                                                 | How to apply, program year                                |
| [About energy assistance](https://dhs.maryland.gov/office-of-home-energy-programs/about-energy-assistance/)                              | MEAP / EUSP / arrearage / USPP overview                   |
| [OHEP Brochure FY26 (PDF)](https://dhs.maryland.gov/documents/OHEP/OHEP_Englishbrochure_2026.pdf)                                        | Income limits; once-per-year grants; categorical SNAP/TCA |
| [Income Guidelines FY2026 (PDF)](https://dhs.maryland.gov/documents/OHEP/Advisory%20Board/Income-Guidelines-FY2026-Updated-7.9.2025.pdf) | 200% FPG weekly / monthly / annual                        |
| **[FY26 MEAP extract](../../sources/FY26-MEAP-Benefit-Matrix.md)**                                                                       | Heating grant `$` table — **used above**                  |
| **[FY26 EUSP extract](../../sources/FY26-EUSP-Benefit-Matrix.md)**                                                                       | Electric grant `$` tables — **used above**                |
| [OHEP Operations Manual (PDF)](https://dhs.maryland.gov/documents/OHEP/OHEP-Operations-Manual.pdf)                                       | Still needed for L6/L7 admin + EUSP posting detail        |
| OHEP public data / PC 53 / Jul 2026 PC 59 data order filings                                                                             | Participation counts                                      |

### A.2 LIM design

| Source                                                                                                                                         | Why look here                                              |
| ---------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| **[PC 59 Order extract](../../sources/Order_LIM-PC59.md)** / [PSC PDF](https://psc.maryland.gov/wp-content/uploads/2026/02/Order_LIM-PC59.pdf) | Commission decisions — **used above**                      |
| **[PC 59 Work Group Report — Oct 1, 2025 (PDF)](https://www.nclc.org/wp-content/uploads/2026/02/PC59-WG-Report-Oct-1-2025-Final.pdf)**         | Detailed LIM math / scenarios — still needed for schedules |
| [PSC press release](https://psc.maryland.gov/news/2026/psc-advances-discounted-rate-mechanism-for-limited-income-utility-customers/)           | Short FAQ                                                  |
| [Jul 8, 2026 PC 59 data/funds order](https://psc.maryland.gov/wp-content/uploads/2026/07/Order_DataPropRelatingDistFunds-PC-59.pdf)            | Utility EUSP/MEAP count reporting                          |
| Future utility LIM tariffs (Fall 2026)                                                                                                         | Company-specific `$/kWh` or flat credits                   |

### A.3 In-repo context

| File                                                                                            | Role                            |
| ----------------------------------------------------------------------------------------------- | ------------------------------- |
| [Order_LIM-PC59.md](../../sources/Order_LIM-PC59.md)                                            | LIM design source               |
| [FY26-MEAP-Benefit-Matrix.md](../../sources/FY26-MEAP-Benefit-Matrix.md)                        | MEAP `$`                        |
| [FY26-EUSP-Benefit-Matrix.md](../../sources/FY26-EUSP-Benefit-Matrix.md)                        | EUSP `$`                        |
| [md_residential_charges_in_bat.md](../methods/bat_mc_residual/md_residential_charges_in_bat.md) | EUSP cost recovery on BGE bills |
| [resstock_lmi_metadata_guide.md](../../code/data/resstock_lmi_metadata_guide.md)                | ResStock → FPL%                 |
| [lmi_master_bills_workflow.md](../../code/orchestration/lmi_master_bills_workflow.md)           | NY wiring pattern for MD script |

### A.4 Remaining research order

1. Pull **participation** stats (OHEP / PC 53 / Jul 2026 filings).
2. Skim **Ops Manual** for L6/L7 and operational EUSP posting.
3. Obtain final utility LIM schedules and implement LIM as a separate component after OHEP.
4. Decide whether Prefect per-utility master tables should also receive LMI columns; the integrated path currently writes them only to `all_utilities`.
