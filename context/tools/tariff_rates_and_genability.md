# Tariff structure, terminology, and Genability representation

This doc covers three things:

1. What tariffs, rates, riders, and tariff books actually are — the regulatory concepts, why they exist, and how they work.
2. How Genability/Arcadia represents these concepts in its data model and APIs.
3. How to reconstruct what a customer was actually charged in a given historical month — the API sequence, why naive approaches fail, and the subtleties of date boundaries.

## Part 1: Regulatory concepts

### Tariff, rate, charge

**Tariff** = a complete rate schedule. It's a document filed with the PSC that says "here are all the terms, conditions, and prices for this class of customer." ConEd EL1 ("Residential and Religious") is a tariff. PSEG-LI 194 is a tariff. A rider is also a tariff — a modular one that attaches to other tariffs. A tariff contains many rates.

**Rate** = a single line item within a tariff. "Customer Charge," "Summer Delivery Energy Charge," "System Benefits Charge," "MSC Rate - Zone J" — each is a rate. A rate has a name, a charge type (fixed, consumption-based, demand-based, etc.), possibly a season and/or TOU period, and one or more rate bands (tiers).

A rate can take many numerical forms:

- $10/month (fixed)
- $0.05/kWh (flat volumetric)
- $0.02/kWh below 250 kWh + $0.03/kWh above (tiered — two rate bands)
- $10/kW (demand-based)
- $0.10/kWh summer + $0.08/kWh winter (seasonal — two separate rates sharing a group)
- $0.22/kWh summer on-peak + $0.05/kWh summer off-peak + ... (seasonal × TOU — four separate rates)

Each tier within a single rate (e.g. the $0.02 and $0.03 in the tiered example) is a **rate band**. Each season × TOU combination is its own rate (not a band within one rate).

**Charge** = loosely, the dollar amount that results from applying a rate to actual usage. The rate is $0.05/kWh; the charge is $0.05 × 500 kWh = $25.00. A rate is the price; a charge is the bill line item after multiplication. In practice "rate" and "charge" are used interchangeably — utility tariff books say "Delivery Charges" as a header for a group of rates, and Arcadia uses `chargeType` as a property of a rate. The distinction only matters when you need to differentiate the per-unit price from the total dollar amount.

### Tariff types: default, alternative, rider

A customer's bill comes from one base tariff plus zero or more riders.

**Default** = the tariff a customer is automatically placed on when they sign up for service. Every residential customer in ConEd territory starts on EL1. There's typically one default tariff per customer class per utility.

**Alternative** = a tariff the customer can opt into instead of the default (e.g. ConEd EL1-TOU, EL1-Demand). The customer switches because it might save them money given their usage pattern. The utility offers alternatives because regulators want to encourage certain behaviors (demand flexibility, off-peak EV charging).

**Rider** = a modular bolt-on, not a standalone tariff a customer can be "on." Riders attach to whatever base tariff the customer is on. Some are unconditional (DLM Surcharge, EV Make Ready — all residential customers pay), some are conditional (CBC only if you have solar, Low Income Discount only if you qualify). The customer doesn't choose riders; they apply automatically based on rules.

### Why riders exist

Riders exist because of how utility regulation works procedurally.

**Rate cases** are the main proceedings — they happen every 3-5 years, take 12-18 months, involve the utility, PSC staff, consumer advocates, environmental groups, industrial intervenors, etc. arguing about the revenue requirement, cost allocation, and rate design. The base tariff (default and alternative rate schedules) comes out of this. It's expensive, slow, and comprehensive.

**Riders** come from narrower, targeted proceedings that can happen anytime:

- The legislature passes a law (Climate Leadership and Community Protection Act) → PSC issues an order → utility files a rider to implement the mandated surcharge
- COVID hits → PSC issues emergency moratorium → later creates an arrears recovery mechanism → rider
- PSC decides to incentivize EVs → issues an EV Make Ready order → rider
- Federal tax reform passes overnight → PSC orders utilities to pass savings back → rider

If you had to reopen the full rate case every time one of these things happened, nothing would get done. Riders let the PSC create a targeted cost recovery mechanism through a focused proceeding, attach it to all relevant tariffs, and update it on whatever cadence makes sense (monthly for PSEG-LI Power Supply, annually for EV Make Ready, one-time for storm bonds) — all without touching the base tariff.

Each rate or charge on a customer's bill needs a legal basis — a PSC order authorizing it, a specific docket number, a filing. The base tariff has its authorization from the rate case order. Each rider has its own authorization from its own order. They're legally separate instruments that happen to show up on the same bill. This is also why riders can sunset — the authorizing order can specify an end date or a total dollar cap, and the rider goes away without needing to reopen the base tariff.

### Tariff books and the leaf system

The physical/administrative structure is a **tariff book** (formally "tariff schedule" or "schedule of electric rates") — the master document filed with the PSC containing everything for a utility's service type. A single utility has one electric tariff book, one gas tariff book, etc.

A tariff book contains:

- **Rate schedules / service classifications** — SC1, SC2, SC9, etc. (the base tariffs for each customer class)
- **Rider statements** — separate numbered documents for each bolt-on charge. The NY PSC glossary defines a statement as "a document, which is not part of the general tariff schedule, that provides for the automatic adjustment of rates and charges."
- **General information** — terms of service, definitions, service territory maps, etc.

#### Versioning: the leaf/revision system

Tariff books are not re-published wholesale when something changes. They use a **loose-leaf binder** system with page-level versioning. The unit of versioning is the **leaf** — a single page in the tariff book. Each leaf has:

- **Leaf number** — its position/address in the book (stable, not re-numbered when other pages change)
- **Revision number** — how many times this specific leaf has been updated (1, 2, 3...)
- **Superseding revision number** — which prior revision this one replaces
- **Effective date** — when this version of the leaf takes effect
- **Status** — Pending → Effective → Cancelled

When a rate case concludes and delivery charges change, the utility files **replacement leaves** — just the specific pages that changed, with incremented revision numbers. Everything else stays as-is. When a rider proceeding concludes, the utility files revised leaves for that statement only.

Leaf numbers are pre-allocated with gaps (like old BASIC line numbers 10, 20, 30) and can use decimal or suffixed numbering (47.1, 47.2, 47A) to insert new content without renumbering downstream pages. A rate schedule might occupy leaves 45, 46, 47, 47.1, 47.2, 48, 50 — the numbers are stable identifiers, not sequential positions.

The identity of any tariff page is:

```
PSC No. [schedule number] → Leaf [number] → Revision [number] → Effective [date]
```

For example:

```
PSC NO. 120 - ELECTRICITY
LEAF: 47
REVISION: 12
SUPERSEDING REVISION: 11
EFFECTIVE DATE: 2025-08-01
```

NY tariffs are filed and stored electronically in the PSC's Electronic Tariff System (ETS) at [ets.dps.ny.gov](https://ets.dps.ny.gov). Utilities register as submitters and file replacement PDF leaves, which are date-stamped on submission.

#### Etymology: "schedule" and "tariff"

"Schedule" in "rate schedule" means a tabular list or appendix (Latin _schedula_, a slip of paper) — the same usage as Schedule A/B in tax filings, a schedule of assets in bankruptcy, or a schedule of fees. It's "a structured, itemized document appended to a legal instrument," not a timetable.

"Tariff" comes from Arabic _taʿrīfa_ (notification, price list) via Italian _tariffa_. Originally used for customs duties (a schedule of import/export taxes), it was adopted by regulated industries (telecom, electricity, gas) for any filed schedule of prices and terms. In utility regulation, "tariff" can mean the specific rate schedule (ConEd EL1), the tariff book (the whole filing), or loosely the rate itself.

---

## Part 2: Genability / Arcadia representation

Arcadia (formerly Genability) maintains a database of utility tariff data and provides APIs to query it. Their data model maps closely to the regulatory concepts above.

### Data hierarchy

```
Tariff (header: metadata about the rate plan)
├── properties[]      → Input parameters needed for bill calculations
└── rates[]           → Array of TariffRate objects (line items)
    └── rateBands[]   → Array of TariffRateBand objects (tiers/bands)
```

### Tariff object

The top-level object. Key fields:

| Field            | What it is                                                              |
| ---------------- | ----------------------------------------------------------------------- |
| `tariffId`       | Unique ID for _this version_ of the tariff (changes with each revision) |
| `masterTariffId` | Persistent ID across all revisions — the stable identity                |
| `tariffCode`     | Utility's shortcode (e.g. "EL1", "SC1", "194")                          |
| `tariffName`     | Official name (e.g. "Residential and Religious")                        |
| `lseId`          | Load Serving Entity (utility) ID                                        |
| `tariffType`     | `DEFAULT`, `ALTERNATIVE`, `RIDER`, or `OPTIONAL_EXTRA`                  |
| `customerClass`  | `RESIDENTIAL`, `GENERAL`, `SPECIAL_USE`, `PROPOSED`                     |
| `effectiveDate`  | When this version became effective                                      |
| `endDate`        | When this version is no longer effective (null = open-ended)            |

`tariffId` vs `masterTariffId`: think of `masterTariffId` as the leaf number (stable identity) and `tariffId` as a specific revision of that leaf. ConEd EL1 has `masterTariffId=809`; each time rates change, a new `tariffId` is issued, but `masterTariffId` stays 809.

### TariffRate object (rates)

Individual charges within the tariff. Key fields:

| Field             | What it is                                                                                                                                    |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `tariffRateId`    | Unique ID for this rate                                                                                                                       |
| `rateName`        | Human-readable name (e.g. "Summer Delivery Energy Charge")                                                                                    |
| `rateGroupName`   | Category grouping (e.g. "Delivery Charges")                                                                                                   |
| `chargeType`      | `FIXED_PRICE` ($/month), `CONSUMPTION_BASED` ($/kWh), `DEMAND_BASED` ($/kW), `QUANTITY` (per-item or percentage), `MINIMUM`, `MAXIMUM`, `TAX` |
| `chargeClass`     | `DISTRIBUTION`, `SUPPLY`, `TRANSMISSION`, `TAX`, `CONTRACTED`, etc.                                                                           |
| `chargePeriod`    | Usually `MONTHLY`                                                                                                                             |
| `season`          | Present for seasonal rates (summer/winter)                                                                                                    |
| `timeOfUse`       | Present for TOU rates (on-peak/off-peak)                                                                                                      |
| `territory`       | Present for zone-specific rates                                                                                                               |
| `variableRateKey` | If present, `rateAmount` is a placeholder (0.0); actual values come from the Lookups API                                                      |
| `riderId`         | If present, this is a rider _reference_ (pointer, no charge data)                                                                             |
| `riderTariffId`   | If present, this is a rider _implementation_ (resolved charge data)                                                                           |
| `rateBands[]`     | The actual rate amounts / tier structure                                                                                                      |

### TariffRateBand object (rate bands)

The tier structure within a rate. Key fields:

| Field                   | What it is                                        |
| ----------------------- | ------------------------------------------------- |
| `rateAmount`            | The actual per-unit price                         |
| `rateUnit`              | `COST_PER_UNIT` or `PERCENTAGE`                   |
| `consumptionUpperLimit` | For tiered rates, the kWh threshold for this tier |
| `rateSequenceNumber`    | Tier order (1, 2, 3...)                           |
| `isCredit`              | True if this band is a credit (reduces the bill)  |

Simple rates have one band. Tiered rates have multiple bands with consumption limits.

### How riders appear in the rates array

When you fetch a base tariff with `populateRates=true`, each attached rider appears as **two entries** in the `rates[]` array:

**1. Rider reference** — a pointer with `riderId` (the rider's `masterTariffId`):

```json
{
  "tariffRateId": 20918185,
  "riderId": 3401705,
  "rateName": "Electric Vehicle Make Ready Surcharge - SC1",
  "chargeType": null,
  "rateBands": []
}
```

**2. Rider implementation** — resolved data with `riderTariffId` (the rider's current-version `tariffId`):

```json
{
  "tariffRateId": 20903427,
  "riderTariffId": 3536446,
  "rateName": "Electric Vehicle Make Ready Surcharge",
  "chargeType": "CONSUMPTION_BASED",
  "rateBands": [{ "rateAmount": 0.002, "rateUnit": "COST_PER_UNIT" }]
}
```

`riderId` and `riderTariffId` are **different numbers** for the same rider (unless it has never been revised). `riderId` is the `masterTariffId` (stable); `riderTariffId` is the current `tariffId` (version-specific). `populateRates=true` resolves all attached riders — every `riderId` reference has a corresponding `riderTariffId` implementation with actual charge data.

Since a rider is just a tariff with `tariffType: "RIDER"`, you can also fetch it directly via `GET /tariffs/{riderId}` and get its full rate structure independently.

### Variable rates vs. fixed rates vs. versioned riders

Rates in Genability get their values through three mechanisms:

**1. Fixed in the tariff** — `rateAmount` in `rateBands` is the actual value, no `variableRateKey`. Changes only when a new tariff version is issued (rate case update, periodic rider revision). Examples: Customer Charge, base delivery energy charges, EV Make Ready Surcharge, VDER Cost Recovery, CBC.

**2. Variable via Lookups API** — has `variableRateKey`; `rateAmount` is `0.0` (placeholder). Actual values change frequently (often monthly) and are retrieved from lookup tables. Examples: ConEd MSC rates, MAC adjustment, Tax Sur-Credit, DLM Surcharge, NatGrid zone-specific supply charges.

**3. Versioned rider tariffs** — riders without `variableRateKey` where the `rateAmount` is fixed within a version, but the rider tariff gets re-versioned periodically. The Tariff History API shows the version timeline. Examples: PSEG-LI Power Supply Charge (re-versioned monthly — 35 versions from Apr 2023 to Feb 2026), NYSEG Recovery Charge (3 versions since Feb 2025), ConEd EV Make Ready (5 annual versions since 2022).

In our NY/RI tariff JSONs, the split is roughly: most rider implementations have no `variableRateKey` (they're category 1 or 3). The main exception is NatGrid, where nearly all rider rates are variable (category 2).

### Tariff properties

The `properties[]` array is metadata that says: "if you want to use the rates in this tariff to calculate a real bill, here are the questions you need to answer first." Many rates are conditional or parameterized — they depend on who the customer is, where they are, or how much they use. Properties enumerate those input requirements.

| Property            | Why you need it                                                                                                                                            |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `consumption`       | Most rates are $/kWh — you need to know how many kWh to multiply by                                                                                        |
| `demand`            | Demand rates are $/kW — you need the customer's peak demand                                                                                                |
| `territoryId`       | ConEd has zone-specific MSC and GRT rates (Zone H, I, J); NatGrid has 6 zones. Without this, you don't know which set of rates applies to a given customer |
| `systemSize`        | CBC only applies to solar customers. If `systemSize = 0` (no solar), CBC doesn't show up on the bill                                                       |
| `lowIncomeCustomer` | Low income discounts only apply if the customer qualifies. If `false`, discount rates don't apply                                                          |
| `chargeClass`       | Lets you filter to just SUPPLY, DISTRIBUTION, etc.                                                                                                         |

Each property has a `dataType` (DECIMAL, CHOICE, BOOLEAN) and, for CHOICE types, a list of valid `choices` with display names and values (e.g., `territoryId` has choices "Zone H" = 3632, "Zone I" = 3633, "Zone J" = 3634). Some properties have defaults (e.g., `systemSize` defaults to 0, meaning no solar; `lowIncomeCustomer` defaults to false).

This matters in two places:

- **Calculate API**: you pass `propertyInputs` with values for these properties, and the API uses them to filter which rates apply and compute the bill. If you don't pass `territoryId` for ConEd, it uses whatever default is set.
- **Processing the rates array yourself**: properties tell you which rates are conditional and what conditions they depend on. Without this context, you'd see 3 sets of zone-specific MSC rates and not know you should only pick one per customer.

### API endpoint

#### Get Tariff: `GET /rest/public/tariffs/{masterTariffId}`

Returns the current (or `effectiveOn`-date) version of a tariff with optional population of rates, properties, and documents.

Key parameters:

| Parameter                  | What it does                                                                |
| -------------------------- | --------------------------------------------------------------------------- |
| `populateRates=true`       | Include all rates (with resolved rider implementations)                     |
| `populateProperties=true`  | Include input parameter definitions                                         |
| `effectiveOn={date}`       | Return the version effective on this date (for historical data)             |
| `lookupVariableRates=true` | Resolve variable rate values inline (instead of returning 0.0 placeholders) |

#### Get Tariff List: `GET /rest/public/tariffs`

Search for tariffs by various criteria (LSE, zip code, customer class, tariff type, etc.). Useful for discovering what tariffs exist for a utility.

Key parameters: `lseId`, `customerClasses`, `tariffTypes` (DEFAULT, ALTERNATIVE, RIDER), `effectiveOn`, `populateRates`, `populateProperties`.

You can also pass `riderId={masterTariffId}` to find all base tariffs a given rider is attached to.

#### Tariff History: `GET /rest/public/tariffs/{masterTariffId}/history`

Returns the complete version history for a tariff. The response includes:

- **`tariffVersions[]`** — each version's `tariffId`, `effectiveDate`, `endDate`, and `lastUpdatedDate`
- **`riderVersions[]`** (within each tariff version) — version history for attached riders
- **`lookupVariableRates[]`** (within each tariff version) — summary of which `variableRateKey` properties have data (property key, date range, entry count)

This is the key endpoint for building time series of fixed-rate charges. Get the version timeline, then fetch each version via `GET /tariffs/{masterTariffId}?effectiveOn={date}&populateRates=true`.

#### Lookups: `GET /rest/public/properties/{variableRateKey}/lookups`

Returns the time series of values for a variable rate. Each entry has:

- `fromDateTime` / `toDateTime` — the effective period
- `bestValue` — the value to use
- `actualValue` — finalized value (may be null for future periods)
- `forecastValue` — Arcadia's forecast
- `lseForecastValue` — utility's forecast

One entry per effective period (not per month); periods may span multiple months if the rate didn't change. To get monthly data, expand each period to fill the months it covers.

#### Calculate: `POST /rest/v1/ondemand/calculate`

Runs a bill calculation for a specific scenario. You provide `masterTariffId`, date range, and `propertyInputs` (consumption, territory, etc.). Returns line items with resolved rates for that scenario.

Key differences from Lookups:

- Calculate resolves **all** applicable rates for the scenario (including riders), filtering by territory, eligibility, etc.
- Lookups returns raw rate values regardless of applicability
- Calculate requires a consumption assumption; Lookups doesn't

The Calculate API is useful for validation (sanity-checking that individual rate values sum to the correct total bill) but less useful for building charge-level time series.

### How Arcadia versioning maps to regulatory versioning

| Regulatory concept                                | Arcadia equivalent                                                                 |
| ------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Tariff book (the whole filing)                    | Not directly represented; each rate schedule and rider is a separate tariff object |
| Rate schedule / service classification (SC1, EL1) | `Tariff` with `tariffType: DEFAULT` or `ALTERNATIVE`                               |
| Rider statement                                   | `Tariff` with `tariffType: RIDER`                                                  |
| Leaf number (stable page identity)                | `masterTariffId` (stable across revisions)                                         |
| Leaf revision (specific version of a page)        | `tariffId` (unique per version)                                                    |
| Effective date on a leaf                          | `effectiveDate` on the tariff                                                      |
| Superseding revision                              | `priorTariffId` (previous version's `tariffId`)                                    |
| PSC schedule number                               | `tariffCode` (e.g. "EL1", "SC1")                                                   |

---

## Part 3: Reconstructing historical monthly charges

Parts 1 and 2 describe tariff structure in the abstract — what rates are, how they're organized, and where they live in Genability's data model. But the question we actually need to answer in practice is concrete: **what were the exact charges on a ConEd residential bill in March 2023?**

This turns out to be harder than it looks. A tariff isn't a single, static price list — it's a composite of base rates, rider rates, and variable rates that each change on different timescales for different regulatory reasons. A snapshot downloaded today only reflects today's values. To reconstruct a historical month accurately, you need to know which version of each rate was in effect during that month and what its numerical value was. This section explains why, and then walks through the exact API calls to do it.

### Three kinds of rates, three change cadences

Every rate on a customer's bill falls into one of three categories based on how and when its numerical value changes:

**1. Base rates** — fixed within a tariff version. These are the rates defined directly in the base tariff: Customer Charge ($20/month), Summer Delivery Energy Charge ($0.16107/kWh), Billing and Payment Processing Charge ($1.28/month). They change only when the base tariff is re-versioned, which happens when a rate case concludes — roughly every 3-5 years for NY utilities. Between rate cases, these numbers don't move. (In Genability: no `variableRateKey`, rate sits directly in the base tariff's `rateBands`.)

**2. Monthly (and daily) variable rates** — change frequently by administrative filing, not by formal proceeding. The utility files updated values with the PSC on a set schedule, and they take effect automatically. Examples:

- **Monthly**: MAC (Monthly Adjustment Clause), RDM (Revenue Decoupling Mechanism), Reconciliation Rate, Merchant Function Charge, System Benefits Charge. These change because the underlying cost inputs (fuel costs, wholesale market prices, reconciliation balances) change monthly.
- **Daily**: MSC (Market Supply Charge). ConEd's MSC varies by day because it reflects daily wholesale electricity market prices. Each day of the billing month can have a different $/kWh MSC rate.

These rates exist as separate lookup tables in Genability, keyed by `variableRateKey`. The tariff JSON itself just has a placeholder `rateAmount: 0.0`; the actual values are retrieved from the Lookups API.

**3. Versioned rider rates** — fixed within a rider version, but the rider gets re-versioned on its own schedule. Unlike base rates (which change every 3-5 years with a rate case) and variable rates (which change monthly/daily), riders change on whatever cadence their authorizing proceeding dictates:

- PSEG-LI Power Supply Charge: re-versioned monthly (35 versions from Apr 2023 to Feb 2026)
- ConEd EV Make Ready Surcharge: re-versioned annually (5 versions since 2022)
- NYSEG Recovery Charge: re-versioned as needed (3 versions since Feb 2025)
- ConEd CBC (Customer Benefit Contribution): re-versioned ~annually based on NY-Sun program updates

When a rider is re-versioned, only the rider tariff gets a new `tariffId` — the base tariff is unaffected. (In Genability: no `variableRateKey`, rate sits in a rider tariff's `rateBands`. The base tariff references it via `riderId`.)

The reason these three categories exist is fundamentally procedural. Base rates require a full rate case (formal proceeding, 12-18 months, multiple parties). Variable rates are administratively filed by the utility on a pre-authorized schedule. Rider rates come from targeted proceedings with their own authorization and timeline. Each mechanism allows rates to change at the cadence that matches the underlying cost driver — wholesale energy costs change daily, program surcharges change annually, base cost allocation changes every few years.

### Why you can't use a static tariff snapshot

If you downloaded `coned_default.json` today (February 2026) and tried to use it for all historical months, you'd get three things wrong:

**Base rates would be wrong for past tariff versions.** The Customer Charge is $20/month today. Was it $20 in January 2023? Maybe not — if a rate case concluded between then and now, the amount changed. Today's snapshot only has today's version of the base rates. For ConEd, the base tariff has had 4 versions since 2021 (visible via the Tariff History API), each with potentially different base rate amounts.

**Rider rates would be wrong for past rider versions.** Today's snapshot resolves riders to their _current_ `riderTariffId`. The EV Make Ready Surcharge might be $0.0008/kWh today but was $0.002/kWh in 2023 (a different rider version). The snapshot gives you the 2026 rider amount, not the 2023 one. Riders that didn't exist in 2023 might show up; riders that have since been sunset might be missing.

**Variable rates are always wrong.** The snapshot has `rateAmount: 0.0` for every rate with a `variableRateKey`. These are placeholders — the actual values live in lookup tables and depend on the month. Without hitting the Lookups API (or using `lookupVariableRates=true`), you have no MSC, no MAC, no RDM, no Merchant Function Charge. These are often the largest charges on a bill.

### The solution: one API call per month

Everything you need for a given month comes from a single call:

```http
GET /rest/public/tariffs/{masterTariffId}
  ?populateRates=true
  &effectiveOn={YYYY-MM-01}
  &lookupVariableRates=true
```

This call does three things simultaneously:

1. **Returns the base tariff version effective on that date.** If you pass `effectiveOn=2023-03-01`, you get the tariff version whose `effectiveDate ≤ 2023-03-01 < endDate`. Base rates (Customer Charge, delivery energy charges) will have the amounts that were in effect in March 2023 — not today's amounts.

2. **Resolves all rider implementations to the rider version effective on that date.** Each rider's `riderTariffId` in the response is the version of that rider that was active on March 1, 2023. If the EV Make Ready Surcharge was $0.002/kWh in March 2023 (and later changed to $0.0008), you get $0.002.

3. **Looks up all variable rate values for that month.** With `lookupVariableRates=true`, the API fills in the actual values from the lookup tables instead of returning 0.0 placeholders. MSC comes back as ~30 daily entries (one per day of the month). MAC, RDM, Reconciliation Rate, etc. come back with their monthly values.

The result is a complete, self-consistent snapshot of every rate that was in effect during that month — base, rider, and variable — in a single API call.

### Handling date-boundary subtleties

Not all variable rates align with calendar month boundaries. In practice:

**Daily rates (MSC)** return one entry per day of the month, each with a different `rateAmount`. For a flat-load approximation (equal consumption every day), the simple average of the daily values gives you the effective monthly rate. For hourly load profiles, use each day's rate for that day's hours.

**Mid-month boundary rates.** Several ConEd variable rates (Reconciliation Rate, Uncollectible Bill Expense, MSC I Adjustment, MSC II Adjustment) have effective periods that run from the **15th of one month to the 16th of the next**, not 1st-to-1st. When you fetch March 2023, you'll get two entries for these rates:

- Entry A: `from=2023-02-15, to=2023-03-16`, `rateAmount=X` (the "February cycle" value)
- Entry B: `from=2023-03-16, to=2023-04-16`, `rateAmount=Y` (the "March cycle" value)

For a billing period of March 1-31, the first 15 days use rate X and the remaining 16 days use rate Y. The correct monthly rate is a **day-weighted average**: `(15 × X + 16 × Y) / 31`. Ignoring the weighting and picking just one entry can produce errors of 50-400%.

**Standard monthly rates** (MAC, RDM, Delivery Revenue Surcharge) have two entries as well — one for the prior period and one for the current — but they align with calendar months. Pick the entry whose `fromDateTime` matches the first of the target month.

### Validating with the Calculate API

To verify that your reconstruction is correct, you can compare it against the Calculate API:

```json
// POST /rest/v1/ondemand/calculate
{
  "masterTariffId": 809,
  "fromDateTime": "2023-03-01",
  "toDateTime": "2023-04-01",
  "detailLevel": "RATE",
  "groupBy": "MONTH",
  "propertyInputs": [
    {"keyName": "consumption", "dataValue": "1"},
    {"keyName": "territoryId", "dataValue": "3634"}
  ]
}
```

Passing 1 kWh makes the math trivial: for any $/kWh rate, `cost = rate × 1 = rate`. The response gives you individual line items with `tariffRateId`, `rateAmount`, and `cost` — directly comparable to the rates you reconstructed from the Get Tariff approach.

In testing across all 25 ConEd rates for every month of 2025, the two approaches matched to within 0.5% for 291 out of 300 rate-month comparisons. The 9 remaining discrepancies were all FIXED_PRICE charges where the Calculate API pro-rates by days-in-month/30 — a billing normalization artifact, not a data disagreement.

### Why use Get Tariff instead of Calculate for rate reconstruction

Both approaches produce the same rates — validated to <0.5% across 300 rate-month comparisons for ConEd. But Get Tariff with `effectiveOn` is the right choice for building rate time series. Here's why.

**Cost.** Arcadia bills API calls and calculations on separate meters with very different pricing:

| Meter                    | Free tier     | Overage rate  | Per-unit cost |
| ------------------------ | ------------- | ------------- | ------------- |
| API calls (Get Tariff)   | 100,000/month | $0.01 per 100 | $0.0001       |
| Calculations (Calculate) | 2,500/month   | $0.12 each    | $0.12         |

That's a 1,200x difference per unit at overage rates. For our workload — 9 utilities x 72 months (2020-2025) = 648 calls — both fit within free tiers today. But the headroom story is very different: 648 API calls barely dents the 100K free tier, while 648 calculations burns through a quarter of the 2,500 calculation budget. At higher volume (more utilities, more years, iterating on methodology), calculations get expensive fast.

"Unique tariffs" — the third billing meter — count by `masterTariffId`, not by snapshot or version. Fetching ConEd tariff 809 with `effectiveOn=2020-01-01` and again with `effectiveOn=2025-12-01` counts as one unique tariff, not two. Both approaches use the same `masterTariffId`s, so this cost is a wash.

**Granularity.** Get Tariff returns the raw rate structure — individual daily MSC values, separate seasonal rate entries, each rider's rate bands. Calculate returns a single time-weighted average per rate for the billing period. If you need daily supply cost variation (for hourly load matching or TOU analysis), only Get Tariff gives it to you. Calculate collapses it.

**No consumption assumption needed.** Get Tariff returns rates ($/kWh, $/month, %) independent of usage. Calculate requires a consumption input — you have to pick a kWh value and back out the per-unit rate from the computed charge. With 1 kWh this is trivial, but it's an unnecessary indirection. And for percentage-based rates (GRT), the charge depends on other charges, so backing out the percentage from a 1-kWh bill requires knowing the base amounts it applies to.

**What Calculate is good for.** Validation. Run it occasionally with 1 kWh and `detailLevel=RATE` to confirm that your Get Tariff reconstruction matches Arcadia's own bill engine. It's the ground truth for "did I interpret the rate structure correctly?" But for production use — fetching rate time series for all utilities across multiple years — Get Tariff is cheaper, more granular, and more direct.
