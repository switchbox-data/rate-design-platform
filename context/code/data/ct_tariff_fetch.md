# CT Gas Tariff Fetch (RateAcuity)

Reference for Connecticut residential gas tariffs: which RateAcuity schedules we
fetch, why they cover the full residential customer class for each modeled LDC,
and what we intentionally exclude. Fetch script:
`utils/pre/fetch_gas_tariffs_rateacuity.py`. Config:
`rate_design/hp_rates/ct/config/tariffs/gas/rateacuity_tariffs.yaml`.

**Ticket scope:** produce URDB gas tariff JSONs only — RateAcuity fetch, copy
`null_gas_tariff.json`, and run `ensure-gas-tariff-envelope`. Tariff maps,
`gas_tariff_mapper.py` branches, and assignment validation are follow-up work
(documented under Mapping below for later use, not this ticket).

---

## CT gas utilities in scope

PURA regulates three investor-owned gas LDCs
([PURA Gas](https://portal.ct.gov/pura/gas/gas),
[PURA Rates](https://portal.ct.gov/pura/industries/rates)); a fourth municipal
LDC (Norwich) is also included in the RateAcuity fetch because its tariffs
are available there:

| std_name          | Display name                     | Owner      | RateAcuity dropdown name           |
| ----------------- | -------------------------------- | ---------- | ---------------------------------- |
| `ct_natural_gas`  | Connecticut Natural Gas          | Avangrid   | `Connecticut Natural Gas`          |
| `southern_ct_gas` | Southern Connecticut Gas         | Avangrid   | `Southern Connecticut Gas`         |
| `yankee_gas`      | Yankee Gas Services (Eversource) | Eversource | `Yankee Gas Services (Eversource)` |
| `norwich_muni`    | Norwich Public Utilities         | Municipal  | `Norwich Public Utilities`         |

**Norwich is included, not excluded.** The ticket only permits excluding
municipal gas when tariffs are unavailable; Norwich's residential tariffs are
present in RateAcuity (and filed publicly), so we fetch them. See
[Norwich residential classes](#norwich-residential-classes) below.

---

## Residential class coverage

Coverage differs by utility type. The three IOUs each have three residential
classes; Norwich (municipal) has two. Heating vs non-heating alone is **not**
complete for any of them.

### IOUs: three classes each

Each IOU’s PURA-approved tariff book defines **three** residential classes:

| Class          | Who it covers                                                                                         | CNG / SCG schedule | Yankee schedule |
| -------------- | ----------------------------------------------------------------------------------------------------- | ------------------ | --------------- |
| Non-heating    | Single-family and multi-family **≤5** dwelling units on one meter, gas **not** used for space heating | RSG                | Rate 01         |
| Heating        | Same dwelling sizes, gas used for **space heating**                                                   | RSH                | Rate 02         |
| Multi-dwelling | Multi-dwellings with **6+** units served through a **single meter**                                   | RMDS               | Rate 03         |

### Evidence (filed / PURA-approved sources)

**Yankee Gas (Eversource)** —
[Summary of Residential Gas Rates (CT) (Zotero)](https://www.zotero.org/groups/5319234/switchbox/collections/4GJTUQRF/items/PQV37J4T/attachment/23A9H449/reader)
(PURA-approved summary; also on
[eversource.com](https://www.eversource.com/content/docs/default-source/rates-tariffs/ct-gas/ct-residential-gas-rates.pdf?sfvrsn=7281e8d4_9)):

- **01 – Residential Non-Heating Service:** “all single family residential
  dwellings, and multi-family residential dwellings serving five or less units
  from a single meter, where the use of natural gas is for purposes other than
  space heating.”
- **02 – Residential Heating Firm Service:** same dwelling sizes that “use
  natural gas for space heating.”
- **03 – Residential Multi-Dwelling Firm Service:** “property owners of all
  multi-dwellings with six or more units served through a single meter.”

**Connecticut Natural Gas** — 2025 rate brochure (three residential rates:
RSG / RSH / RMDS) and filed Rate RSG tariff
([CNG Rate RSG — Residential Service General (Zotero)](https://www.zotero.org/groups/5319234/switchbox/collections/4GJTUQRF/items/BXCDI38X/reader);
brochure also in Zotero as
[CNG An Avangrid Company 2025 Rate Schedule](https://www.zotero.org/groups/5319234/switchbox/collections/4GJTUQRF/items/YR8QTXQ3/reader)):

- Brochure: “Residential customers have **three** separate rates: RSG … RSH …
  RMDS …”
- Rate RSG availability (effective 05/01/2025): “For all single-family
  residential customers and all multi-family residential customers where the
  number of dwelling units is five (5) or less, where the use of natural gas is
  for other than space heating. Residential customers where the number of
  dwelling units is five (5) or less and use natural gas for space heating will
  be served under the Company’s Rate RSH.” RMDS is the 6+ single-meter
  multi-dwelling class.

**Southern Connecticut Gas** — filed Rate RSG and Rate RSH tariffs
([SCG Rate RSG — Residential Service General (Zotero)](https://www.zotero.org/groups/5319234/switchbox/collections/4GJTUQRF/items/WCBLJQFG/reader);
[SCG Rate RSH — Residential Service Heating (Zotero)](https://www.zotero.org/groups/5319234/switchbox/collections/4GJTUQRF/items/AYU63JWP/reader)):

- Rate RSG mirrors CNG almost verbatim: “Available every day of the year to
  residential Customers for all uses other than space heating where the number
  of dwelling units supplied through one meter is five or less. Residential
  customers … that use natural gas for space heating will be served under the
  Company’s … Rate RSH.”
- Rate RSH is the ≤5-unit space-heating companion class; RMDS (6+ units, single
  meter) is the third residential class on SCG’s pricing page. CNG and SCG share
  the Avangrid rate redesign (PURA Docket 23-11-02).

### Mapping to ResStock / CAIRO (follow-up; not this ticket)

These rules are the intended assignment once tariff maps are built. This ticket
only fetches the URDB keys below; mapper / map generation is separate work.

| ResStock situation                                  | Tariff key suffix | Rationale      |
| --------------------------------------------------- | ----------------- | -------------- |
| `heats_with_natgas = False`, not master-metered 6+  | `_nonheating`     | RSG / Rate 01  |
| `heats_with_natgas = True`, not master-metered 6+   | `_heating`        | RSH / Rate 02  |
| Master-metered multi-dwelling (6+ units, one meter) | `_mf`             | RMDS / Rate 03 |

The fetch list in this doc is the complete residential URDB set those future
mapper rules must resolve into.

---

## RateAcuity schedules we fetch

Company-supply / firm sales schedules only (baseline bundled path). Exact
strings must match the RateAcuity gas-history dropdown.

| tariff_key                   | RateAcuity schedule name                                                   |
| ---------------------------- | -------------------------------------------------------------------------- |
| `ct_natural_gas_nonheating`  | `RSG-RESIDENTIAL SERVICE GENERAL---`                                       |
| `ct_natural_gas_heating`     | `RSH-RESIDENTIAL SERVICE HEATING---`                                       |
| `ct_natural_gas_mf`          | `RMDS-RESIDENTIAL MULTI-DWELLING SERVICE-Company Supply Service-5001-`     |
| `yankee_gas_nonheating`      | `01-RESIDENTIAL NON-HEATING SERVICE---`                                    |
| `yankee_gas_heating`         | `02-RESIDENTIAL HEATING FIRM SERVICE---`                                   |
| `yankee_gas_mf`              | `03-RESIDENTIAL MULTI-DWELLING FIRM SERVICE-Company Standard Gas Supply--` |
| `southern_ct_gas_nonheating` | `RSG-RESIDENTIAL SERVICE GENERAL---`                                       |
| `southern_ct_gas_heating`    | `RSH-RESIDENTIAL SERVICE HEATING---`                                       |
| `southern_ct_gas_mf`         | `RMDS-RESIDENTIAL MULTI-DWELLING SERVICE-Company Supply Service--`         |
| `norwich_muni_general`       | `GRES-RESIDENTIAL---`                                                      |
| `norwich_muni_mf`            | `GSHRES-RESIDENTIAL SPACE HEATING---`                                      |

### Intentionally not fetched (kept for reference only)

Not needed for ResStock / CAIRO baseline analysis — existing stock is modeled on
standard firm residential classes (RSG / RSH / RMDS, or Yankee 01 / 02 / 03). The
two "SE" labels below are **different products**; do not conflate them.

| Variant                                                                                               | What it is                                                                                                                                                                                                                                                                    | Why omit from fetch / analysis                                                                                                                                             | Source (Zotero)                                                                                                                                                                                                                        |
| ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Rate SE — Seasonal Gas Service** (CNG; also C/I SE)                                                 | Separate class under "Seasonal and Interruptible Customers," not RSG/RSH/RMDS. **Value-of-service** pricing tied to alternate-fuel prices: winter delivery is a filed `$/Ccf`; summer delivery is **"Market Conditions."** Distinct from year-round firm residential heating. | Not a standard firm residential class; not how ResStock gas-heated homes are modeled.                                                                                      | [CNG An Avangrid Company 2025 Rate Schedule](https://www.zotero.org/groups/5319234/switchbox/collections/4GJTUQRF/items/YR8QTXQ3/reader)                                                                                               |
| **`*-SE` / System Expansion** (e.g. RSH-SE, RMDS-SE, Yankee 02-SE / 03-SE; often on-main vs off-main) | Higher rates for **new premises** added under the utility's gas system expansion program; customers typically remain on SE rates for **10 years**. Still firm residential heating / multi-dwelling — a surcharge path for expansion customers, not "seasonal" service.        | Applies to post-cutover new connects; existing ResStock buildings map to standard RSG/RSH/RMDS (or 01/02/03).                                                              | [Summary of Residential Gas Rates (CT)](https://www.zotero.org/groups/5319234/switchbox/collections/4GJTUQRF/items/PQV37J4T/attachment/23A9H449/reader) (Yankee/Eversource 02-SE / 03-SE); CNG/SCG SE brochures cover RSH-SE / RMDS-SE |
| Third-party / Operator Gas Supply RMDS (or Rate 03) variants                                          | Competitive supply path                                                                                                                                                                                                                                                       | Residential single-family sales remain with the LDC under CT deregulation norms ([PURA Gas](https://portal.ct.gov/pura/gas/gas)). Company-supply RMDS/03 covers the class. | —                                                                                                                                                                                                                                      |
| Commercial/industrial (SGS, MGS, LGS, Rate 10/20/30, etc.)                                            | Non-residential                                                                                                                                                                                                                                                               | Out of residential scope.                                                                                                                                                  | —                                                                                                                                                                                                                                      |

**Note:** Existing firm residential customers still pay a **System Expansion
Reconciliation (SER)** rider on standard RSG/RSH/RMDS — that is a separate
volumetric (or MDQ-billed on MF) surcharge recovering expansion costs from the
broader base, not the System Expansion rate class itself. SER appears in the
fetched RateAcuity rows; on MF tariffs the MDQ-billed SER rows are among those
dropped with demand charges (see Known limitation below).

Together, the nine IOU schedules plus Norwich's two are the company-supply
versions of every **standard firm** residential class published for the four
modeled LDCs — so every modeled residential customer maps to one of the fetched
keys. Seasonal Rate SE and System Expansion `*-SE` are documented above for
reference only.

---

## Norwich residential classes

Filed rates ([NPU Gas Rates](https://norwichpublicutilities.com/DocumentCenter/View/342/Gas-Rates),
verified against the live document);
present in RateAcuity, so fetched (not excluded):

| tariff_key             | RateAcuity name                       | Filed class | Availability (verbatim from tariff)                                                                                                                                                                                         |
| ---------------------- | ------------------------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `norwich_muni_general` | `GRES-RESIDENTIAL---`                 | GRES        | "Gas service for year-round use in single family residential dwellings and appurtenances and multi-family residential dwellings serving **five or less units** from a single meter **for all its household requirements**." |
| `norwich_muni_mf`      | `GSHRES-RESIDENTIAL SPACE HEATING---` | GSHRES      | "Gas service for year-round use available to multi-family residential customers serving **six or more units** from a single meter."                                                                                         |

**Why `_mf` and not `_heating` despite the "Space Heating" title:** GSHRES's
availability clause has no heating-use restriction — it qualifies purely on unit
count (6+, single meter). Contrast the _commercial_ GSH rate in the same filing,
which explicitly requires "the entire building is heated with gas" for non-heating
uses to qualify. NPU kept the legacy "Space Heating" name on GSHRES, but
functionally it is the multi-dwelling class, directly analogous to RMDS (CNG/SCG)
and Rate 03 (Yankee). `_mf` reflects what the rate actually does, not its title.

**Coverage is complete, by construction:** every residential dwelling is either
≤5 units on a single meter (→ GRES) or 6+ units on a single meter (→ GSHRES) —
the two availability clauses partition all residential customers with no gap and
no overlap. GRES covers **both** heating and non-heating uses for ≤5-unit
dwellings (there is no separate Norwich heating/non-heating split, unlike the
IOUs). Intended future mapper routing (not this ticket): any ≤5-unit dwelling
(regardless of `heats_with_natgas`) → `norwich_muni_general`; master-metered 6+
unit buildings → `norwich_muni_mf`.

| ResStock situation (Norwich)                        | Tariff key             | Rationale                           |
| --------------------------------------------------- | ---------------------- | ----------------------------------- |
| Not master-metered 6+ (any heating status)          | `norwich_muni_general` | GRES = all household uses, ≤5 units |
| Master-metered multi-dwelling (6+ units, one meter) | `norwich_muni_mf`      | GSHRES = 6+ units on a single meter |

---

## Ticket deliverables (fetch only)

1. **Fetch** RateAcuity URDB JSONs for every `tariff_key` in
   `rateacuity_tariffs.yaml` (eleven residential schedules across the four
   LDCs).
2. **Copy** `null_gas_tariff.json` into
   `rate_design/hp_rates/ct/config/tariffs/gas/` (same zero-rate envelope used
   by other states; fallback for buildings with no gas connection).
3. **Envelope** — run `ensure-gas-tariff-envelope` so every JSON under that
   directory has the CAIRO URDB wrapper shape.

```bash
# From rate_design/hp_rates/:
just -f ct/Justfile fetch-gas-tariffs
# or via dispatch: just s ct fetch-gas-tariffs

# Copy null_gas_tariff.json from another state (e.g. RI or MD), then:
just s ct ensure-gas-tariff-envelope
# or: just -f ct/Justfile ensure-gas-tariff-envelope  (if env already loaded)
```

Requires `RATEACUITY_USERNAME` and `RATEACUITY_PASSWORD` (already set in the
repo-root `.env`, loaded automatically by the fetch script via `dotenv`).
Writes one `{tariff_key}.json` + `.csv` per schedule row above.
`utils/utility_codes.py` must include `gas_tariff_key` and
`rate_acuity_utility_names` for each shortcode (set for `ct_natural_gas`,
`yankee_gas`, `southern_ct_gas`, `norwich_muni`).

**Fetch run:** completed successfully — wrote all 11 tariff files. See
[Known limitation: demand charges](#known-limitation-demand-charges-silently-dropped-for-mf-tariffs)
below for the one real gap in the output.

### Out of scope for this ticket

- CT `gas_tariff_mapper.py` branches / `EXPECTED_GAS_UTILITIES`
- `create-gas-tariff-maps-all` / tariff-map CSVs
- `validate-config` against assigned `sb.gas_utility` values
- Fixing the demand-charge gap below (library/engine work, not a fetch step)

---

## Known limitation: demand charges silently dropped for MF tariffs

**What these "demand charges" are (and why only the MF class has them).** This is
not a naming artifact of the unit string — the utilities themselves call it a
**Demand Charge** on the filed rate sheet. It is a **peak-capacity charge**, not
a volumetric (per-Ccf-used) charge. It bills on the customer's _billing demand_
— their peak-day gas draw — rather than total gas consumed, and it recovers the
cost of sizing the distribution system (and, for the "Sales Services Demand
Charge", the LNG/supply assets) to serve that peak. From Eversource's Rate 03
(Residential Multi-Dwelling) summary:

> Rate 03 customers have a Demand Charge on their bill which takes into account
> individual customer peak demands – or what is required by Yankee Gas dba
> Eversource Energy's distribution system to serve them. In addition, the
> customer is billed a demand based rate called the Sales Services Demand Charge
> … which recovers gas supply-related costs associated with … [the] Liquefied
> Natural Gas ("LNG") facility.

Source: [Summary of Residential Gas Rates (CT), Eversource](https://www.eversource.com/content/docs/default-source/rates-tariffs/ct-gas/ct-residential-gas-rates.pdf?sfvrsn=7281e8d4_9).
CNG/SCG express the same thing as `Demand Charge/MDQ` (MDQ = Maximum Daily
Quantity, the max daily volume the company agrees to deliver); see the
[CNG rate schedule](https://www.cngcorp.com/documents/d/cng/cng_2025_rateschedule_rev-10-28-25).

Only the multi-dwelling class (Rate 03 / RMDS, 6+ units on a single meter) has
it: the ≤5-unit residential classes (Rate 01/02, RSG/RSH) bill on volume plus a
flat customer charge, whereas one master meter serving 6+ units can place a large
peak-day load on the distribution system, so those costs are recovered through a
demand charge instead of being folded into a higher volumetric `$/Ccf` rate. The
related MF-only riders we also drop — **SER** (System Expansion Reconciliation),
**DIMP** (Distribution Integrity Management Program), and the **Sales/Transportation
Services Demand Charge** — are likewise billed per unit of billing demand / MDQ,
not per Ccf used.

The fetch script logs `WARNING: Validation: {...}` for rows it can't parse.
These aren't cosmetic — `tariff_fetch`'s `HistoryData.rows()` (the method that
actually builds the URDB JSON) silently drops any row that fails validation
(`contextlib.suppress(RowValidationError)` in
`tariff_fetch/urdb/rateacuity_history_gas/history_data.py`). Verified directly
against the written JSONs (not just the log) — four files are affected:

| File                        | Rows silently dropped                                                              | Real charge?                                                                                                                                                           |
| --------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ct_natural_gas_mf.json`    | Demand charge, SER charge, DIMP                                                    | Yes — filed, substantial                                                                                                                                               |
| `yankee_gas_mf.json`        | Demand charge, system expansion reconciliation, sales services demand charge, DIMP | Yes — filed, substantial (confirmed verbatim against the Eversource rate summary: e.g. "Demand Charge (per Ccf of billing demand): $0.7107")                           |
| `southern_ct_gas_mf.json`   | Demand charge, DIMP, system expansion reconciliation                               | Yes — filed, substantial                                                                                                                                               |
| `norwich_muni_general.json` | One "demand added" row                                                             | No — `min_psig: 123456`, `max_psig: 32165`, `charge_type: 'charge type'` are nonsensical placeholder values, not a real filed field. RateAcuity data-quality artifact. |

**Root cause:** `tariff_fetch`'s `RateDeterminant` enum
(`tariff_fetch/urdb/rateacuity_history_gas/types.py`) only recognizes `per ccf`,
`per therm`, `per month(*)`, and `percent`. It has no case for demand-based
determinants like `"per ccf of maximum daily demand"` (CNG/SCG) or `"per
billing demand ccf"` (Yankee), so those rows fail Pydantic validation and get
dropped before ever reaching the URDB output.

**Not a pre-existing, accepted gap — genuinely new territory.** Checked every
other state:

- RI: no multi-dwelling/demand-metered gas class fetched at all.
- MD: no multi-dwelling gas class fetched either (all single residential
  schedules).
- NY: does fetch MF gas classes (`kedli_mf`, `kedny_mf`), but their raw
  RateAcuity data only contains `percent`/`per month`/`per therm` — those
  tariffs simply don't bill via demand charges.

CT's IOU RMDS/Rate-03 tariffs are the first demand-metered gas class this
pipeline has ever fetched, so there's no established pattern of intentionally
excluding gas demand charges — they've just never come up before.

**Why we're not patching them in manually.** Checked CAIRO's URDB parser
(`cairo/rates_tool/tariffs.py`, `try_get_demand_structure`): it hardcodes a
unit check —

```python
if "unit" in entry.keys():
    if entry["unit"].lower() != "kW".lower():
        raise RuntimeError(
            "UtilityRateDatabase error: unrecognized unit in rate structure"
        )
```

CAIRO bills gas the same way as electricity, through PySAM's `UtilityRate5`
engine (there is no separate gas code path in `customer_bill_calculation.py` or
`tariffs.py` — it's why volumetric gas charges are already stored as
`unit: "kWh"` in `energyratestructure`). A manually-added gas demand charge
would have to be either:

1. Labeled with a real unit (e.g. `"Ccf"`) → hits the check above → CAIRO
   raises `RuntimeError` and the run fails.
2. Labeled `"kW"` (or given no `unit` key) without actually converting "Ccf of
   maximum daily demand" into a kW-equivalent magnitude → CAIRO runs fine and
   silently computes a wrong bill component — worse than dropping the charge.

Correctly supporting this would require `tariff_fetch` to convert the gas
demand determinant into a kWh/day-equivalent capacity value (analogous to the
existing `KWH_PER_THERM` conversion for volumetric charges) before CAIRO ever
sees it — real library/engine work, out of scope for a fetch-only ticket.

**Disposition:** demand charges omitted from all four affected tariffs (same
treatment as Norwich's junk row, but for a different reason — these are real
filed charges we can't safely represent yet, not artifacts). `_mf` tariffs for
CNG, Yankee, and SCG currently understate multi-dwelling building bills by the
value of their (dropped) demand charge. Follow-up work needed before these
tariffs are used for any analysis sensitive to MF-building bill accuracy:
extend `tariff_fetch`'s rate-determinant handling for gas demand charges, with
a kWh/day-equivalent conversion, then re-fetch.
