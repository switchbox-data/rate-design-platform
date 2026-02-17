# ResStock Metadata for LMI Program Modeling

A guide to what's available in the ResStock 2024.2 parquet metadata for assigning customers to RI and NY low-income discount tiers, and what gaps remain.

---

## 1. Columns CAIRO's Native LMI Code Uses

CAIRO's `EligibilityMapper` class (in `low_income_assistance.py`, lines 41–53) pulls these columns from building stock metadata. In CAIRO's internal naming convention these use a `build_existing_model.` prefix; in the ResStock 2024.2 parquet files they use an `in.` prefix.

| CAIRO name (`build_existing_model.X`) | Parquet name (`in.X`)            | Type   | Purpose in CAIRO's LMI logic                                                                                                          |
| ------------------------------------- | -------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| `tract`                               | **Not present**                  | —      | Census tract — used for tract-level participation targeting. No direct equivalent in 2024.2 parquet; closest is `in.county_and_puma`. |
| `tenure`                              | `in.tenure`                      | String | `Owner` / `Renter` / `Not Available` — splits customers into generic vs. renter sub-programs                                          |
| `area_median_income`                  | `in.area_median_income`          | String | AMI band — loaded but not directly used in CAIRO's generic eligibility check                                                          |
| `disadvantaged_community`             | **Not present**                  | —      | Loaded by CAIRO but not used in eligibility; absent from 2024.2 parquet                                                               |
| `federal_poverty_level`               | `in.federal_poverty_level`       | String | FPL band — loaded by CAIRO, but eligibility is actually determined via `income` + `occupants`                                         |
| `geometry_building_type_recs`         | `in.geometry_building_type_recs` | String | Building type (SFD, SFA, MF 2–4, MF 5+, Mobile Home) — used to route certain renters to a separate sub-program                        |
| `vintage_acs`                         | `in.vintage_acs`                 | String | Loaded but not used in eligibility logic                                                                                              |
| `income`                              | `in.income`                      | String | Household income band (20 bins, `<10000` to `200000+`) — mapped to a numeric level, then compared against per-occupant thresholds     |
| `occupants`                           | `in.occupants`                   | String | Number of occupants (`1` through `10+`) — used to look up household-size-specific income eligibility thresholds                       |

CAIRO's native LMI was built for a simplified California-style bill assistance model. It supports a single income eligibility threshold (no tiers), and its tract-based participation targeting relies on a column (`tract`) that isn't present in the 2024.2 parquet. We won't be using CAIRO's native LMI for RI or NY — we'll do tier assignment and discount application in postprocessing.

---

## 2. The Key Column: `in.representative_income`

**This is new in ResStock 2024.2.** In the 2022 releases, income was only available as banded strings (`in.income`), and we had to roll our own band-to-float conversions. The 2024.2 release adds `in.representative_income` — a continuous float representing a point estimate of household income in dollars.

From the [ResStock 2024.2 documentation](https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_tmy3_release_2/resstock_documentation_2024_release_2.pdf), Section 3.3:

> To determine the `representative_income` value the team created a series of lookup tables derived from **2019 5-year American Community Survey data**. The lookup tables provide the **weighted median income** within a specific set of characteristics: income bin, occupants, FPL, tenure, building type, and different geographic resolutions starting with the intersection of PUMA and county.

This is significantly better than a naive band midpoint — it's a geographically informed weighted median conditional on multiple household characteristics.

**Caveats:**

- **Dollar year:** `representative_income` is in **2019 USD** (matching the ACS vintage). If comparing against current-year FPL or SMI thresholds, you may want to inflate.
- **Vacant units:** Set to **0.0** for vacant dwellings (identifiable via `in.vacancy_status == "Vacant"`). Always filter these out before income-based analysis.
- **188 unique values** in the NY file (33,790 rows). It's not fully continuous — multiple dwellings share the same representative income — but it's far more granular than the 20 income bands.

---

## 3. Computing FPL% and SMI% for Tier Assignment

### FPL% (needed for both RI and NY)

The [HHS Federal Poverty Guidelines](https://aspe.hhs.gov/topics/poverty-economic-mobility/poverty-guidelines) define thresholds by household size. Using the 2024 guidelines:

```
FPL_threshold = $15,060 + (occupants - 1) × $5,380
FPL_pct = representative_income / FPL_threshold × 100
```

This works well. Verified against the banded `in.federal_poverty_level` column — computed values align with the band assignments. Example from RI:

| bldg_id | representative_income | occupants | computed FPL% | banded FPL |
| ------- | --------------------- | --------- | ------------- | ---------- |
| 15676   | $4,849                | 1         | 32.2%         | 0–100%     |
| 22034   | $17,504               | 3         | 67.8%         | 0–100%     |
| 26268   | $12,655               | 2         | 61.9%         | 0–100%     |
| 27535   | $11,140               | 1         | 74.0%         | 0–100%     |

### SMI% (needed for NY)

NY's EAP tiers use State Median Income (SMI), not FPL. The NY HEAP program publishes monthly income limits by household size and tier. For the 2025–2026 HEAP season:

| HH Size | 60% SMI (monthly) | 80% SMI (monthly) | 100% SMI (monthly) |
| ------- | ----------------- | ----------------- | ------------------ |
| 1       | $3,473            | $4,631            | $5,789             |
| 2       | $4,542            | $6,056            | $7,571             |
| 3       | $5,611            | $7,481            | $9,352             |
| 4       | $6,680            | $8,907            | $11,134            |

To compute SMI%, annualize these thresholds (× 12) and compare against `representative_income`. Note: `representative_income` is in 2019 USD, while these SMI thresholds are in 2025 USD. You'll need to either inflate `representative_income` to 2025 dollars or deflate the SMI thresholds to 2019 dollars for a consistent comparison.

**Important:** `in.area_median_income` (AMI) is **not** the same as SMI. AMI is local (varies by metro area), while SMI is statewide. The AMI bands in ResStock may be useful as a rough cross-check, but tier assignment should be computed from `representative_income` against published SMI thresholds.

---

## 4. NY EAP Tiers 1–3: Eligibility Criteria and What ResStock Can Represent

The NY EAP tier assignment for Tiers 1–3 depends on three factors: income relative to FPL and SMI thresholds, presence of "vulnerable" household members, and HEAP enrollment pathway. The detailed criteria are:

| Income condition   | Demographic condition                                                       | Tier       | HEAP grant                  |
| ------------------ | --------------------------------------------------------------------------- | ---------- | --------------------------- |
| < 130% FPL         | 1+ vulnerable member (age <6, age ≥60, or permanently disabled)             | **Tier 3** | $496 (2 add-ons)            |
| < 130% FPL         | No vulnerable members                                                       | **Tier 2** | $461 (1 add-on: income)     |
| 131% FPL – 60% SMI | 1+ vulnerable member                                                        | **Tier 2** | $435 (1 add-on: vulnerable) |
| 131% FPL – 60% SMI | No vulnerable members                                                       | **Tier 1** | $400 (0 add-ons)            |
| ≤ 60% SMI          | Categorical enrollment (SNAP, Medicaid, SSI, TANF) but no HEAP grant posted | **Tier 1** | —                           |
| ≤ 60% SMI          | Emergency HEAP only                                                         | **Tier 1** | —                           |
| ≤ 60% SMI          | Heats with non-utility fuel (oil, propane, wood)                            | **Tier 1** | —                           |

### What ResStock can represent

| Criterion                                 | Feasibility | How                                                                                           |
| ----------------------------------------- | ----------- | --------------------------------------------------------------------------------------------- |
| Income < 130% FPL                         | **Yes**     | Compute FPL% from `representative_income` + `occupants`                                       |
| Income 131% FPL – 60% SMI                 | **Yes**     | Compute FPL% and SMI% from `representative_income` + `occupants` against published thresholds |
| Vulnerable member (age <6, ≥60, disabled) | **No**      | ResStock tracks `in.occupants` (count) but has **no age distribution or disability data**     |
| SNAP/Medicaid/SSI/TANF enrollment         | **No**      | ResStock does not track public assistance program enrollment                                  |
| HEAP grant amount                         | **No**      | Not in ResStock — it's an output of the eligibility determination, not an input               |
| Heating fuel type (non-utility fuel)      | **Yes**     | `in.heating_fuel` distinguishes Natural Gas, Electricity, Fuel Oil, Propane, Other Fuel       |

### Practical approach for Tiers 1–3

Since ResStock lacks vulnerability and program enrollment data, a reasonable simplification is:

1. **Compute FPL% and SMI%** from `representative_income` + `occupants`.
2. **Assign tiers based on income thresholds alone:**
   - FPL% < 130% → **Tier 2** (conservative default — blends the Tier 2/Tier 3 distinction that we can't resolve without vulnerability data)
   - 130% FPL < income ≤ 60% SMI → **Tier 1**
3. **Optionally, probabilistically assign vulnerability** to split Tier 2 customers into Tier 2 vs. Tier 3. ACS PUMS data for NY can provide the share of low-income households with a member age <6, ≥60, or disabled, by PUMA. Apply that share as a probability to "promote" some Tier 2 customers to Tier 3.

This is imperfect but defensible. The income thresholds are the primary driver of tier assignment; vulnerability is a modifier that shifts some customers one tier deeper. Sensitivity analysis (e.g., running with all-Tier-2 vs. all-Tier-3 for the < 130% FPL group) can bound the impact.

---

## 5. RI LIDR+ Tiers: Straightforward

RI's proposed LIDR+ tiers are defined purely by FPL%, with no vulnerability or program-enrollment conditions:

| Tier              | FPL% range | Discount |
| ----------------- | ---------- | -------- |
| Tier 3 (Deepest)  | ≤ 75%      | ~60%     |
| Tier 2 (Middle)   | 76–150%    | ~30%     |
| Tier 1 (Smallest) | 151–250%   | ~10%     |

This can be computed directly from `representative_income` + `occupants` with no approximations needed. Every occupied ResStock dwelling can be definitively assigned to a tier or marked as ineligible (> 250% FPL).

---

## 6. Other Useful Columns for LMI Modeling

| Column                           | Type    | Unique (NY) | Why it matters                                                                                                            |
| -------------------------------- | ------- | ----------- | ------------------------------------------------------------------------------------------------------------------------- |
| `in.representative_income`       | Float64 | 188         | Continuous income for FPL%/SMI% computation                                                                               |
| `in.occupants`                   | String  | 11          | Household size for FPL/SMI threshold lookup                                                                               |
| `in.federal_poverty_level`       | String  | 7           | Banded FPL — useful for cross-checks, too coarse for RI tiers                                                             |
| `in.area_median_income`          | String  | 8           | Banded AMI — useful for cross-checks, not the same as SMI                                                                 |
| `in.heating_fuel`                | String  | 6           | Natural Gas / Electricity / Fuel Oil / Propane / Other / None — needed for heating vs. non-heating credit assignment (NY) |
| `in.hvac_heating_type`           | String  | 5           | Identifies heat pump customers (Ducted/Non-Ducted Heat Pump)                                                              |
| `in.tenure`                      | String  | 3           | Owner / Renter — relevant for some program design considerations                                                          |
| `in.geometry_building_type_recs` | String  | 5           | SFD, SFA, MF 2–4, MF 5+, Mobile Home                                                                                      |
| `in.county`                      | String  | 62 (NY)     | County FIPS — needed to map customers to utility territories in NY                                                        |
| `in.county_and_puma`             | String  | 171 (NY)    | Finer-grained geography for utility territory mapping                                                                     |
| `in.vacancy_status`              | String  | 2           | Filter out vacant units (3,945 in NY, 233 in RI) — these have zero income and "Not Available" for all demographic fields  |
| `weight`                         | Float64 | —           | Sample weight for scaling to actual household counts                                                                      |

### NY heating vs. non-heating classification

NY EAP credits differ substantially between heating and non-heating accounts. Use `in.heating_fuel` to classify:

- **Electric Heating:** `in.heating_fuel == "Electricity"` (includes heat pumps and electric baseboard)
- **Gas Heating:** `in.heating_fuel == "Natural Gas"`
- **Non-Heating (electric account):** Any customer whose `in.heating_fuel` is not `"Electricity"` — their electric bill covers lights, appliances, cooling, but not space heating
- **Non-Heating (gas account):** Customers with `in.heating_fuel == "Natural Gas"` who use gas only for cooking/hot water — harder to distinguish in ResStock since gas consumption includes all end uses. A pragmatic approach: if a customer has gas service but `in.heating_fuel != "Natural Gas"`, they are a gas non-heating customer.

### NY utility territory mapping

NY has multiple utilities with different EAP credit amounts. ResStock doesn't include a utility identifier, but `in.county` can be mapped to utility service territories. Example mappings (not exhaustive):

| Counties (FIPS)                                                                           | Utility                         |
| ----------------------------------------------------------------------------------------- | ------------------------------- |
| Kings (047), Queens (081), New York (061), Bronx (005), Westchester (119), Richmond (085) | Con Edison                      |
| Erie (029), Niagara (063), Chautauqua (013)                                               | National Fuel Gas               |
| Nassau (059), Suffolk (103)                                                               | PSEG Long Island / Nat. Grid LI |
| Albany (001), Schenectady (093), Saratoga (091)                                           | National Grid Upstate           |

A proper crosswalk would use EIA Form 861 utility service territory data or a county-to-utility mapping table.

---

## 7. Summary: What We Can and Can't Model

| Program feature                 | RI LIDR+                                       | NY EAP (Tiers 1–3)                        | ResStock support                                                |
| ------------------------------- | ---------------------------------------------- | ----------------------------------------- | --------------------------------------------------------------- |
| Income-based tier assignment    | FPL% thresholds (75%, 150%, 250%)              | FPL% (130%) and SMI% (60%) thresholds     | **Full** — compute from `representative_income` + `occupants`   |
| Vulnerability modifier          | Not used                                       | Shifts customers between Tiers 2 and 3    | **None** — no age or disability data; must approximate or bound |
| Program enrollment (SNAP, etc.) | Required for program entry, not tier placement | Alternative pathway to Tier 1             | **None** — not in ResStock                                      |
| Heating vs. non-heating         | Different base rates, same discount %          | Different credit amounts                  | **Partial** — `in.heating_fuel` covers most cases               |
| Utility territory               | Single utility (RIE)                           | Multiple utilities with different credits | **Feasible** — via county-to-utility crosswalk                  |
| Discount mechanism              | % of total bill                                | Fixed monthly credit                      | N/A (applied in postprocessing, not a data question)            |
