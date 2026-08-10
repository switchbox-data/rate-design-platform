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

**Verification note:** `eversource.com`, `portal.ct.gov`, and
`norwichpublicutilities.com` load fine for a direct fetch (confirmed live in
this session). `cngcorp.com` and `soconngas.com` (both Avangrid) time out on
every direct fetch attempt — the bare domain, not just these deep PDF links —
which looks like bot/crawler protection on Avangrid's side rather than a dead
link. If these links don't load in your browser either, that's consistent with
what we're seeing here, not a problem specific to you. The CNG/SCG quotes below
are corroborated via search-engine-indexed snippets of the actual filed PDFs
(verbatim text, consistent across independent hits) rather than a direct fetch
by us — open them in a normal browser (not an automated tool) if you need to
inspect the PDFs yourself.

**Yankee Gas (Eversource)** — [Summary of Residential Gas Rates (CT)](https://www.eversource.com/content/docs/default-source/rates-tariffs/ct-gas/ct-residential-gas-rates.pdf?sfvrsn=7281e8d4_9)
(PURA-approved summary, last updated April 1, 2026; fetched directly):

- **01 – Residential Non-Heating Service:** “all single family residential
  dwellings, and multi-family residential dwellings serving five or less units
  from a single meter, where the use of natural gas is for purposes other than
  space heating.”
- **02 – Residential Heating Firm Service:** same dwelling sizes that “use
  natural gas for space heating.”
- **03 – Residential Multi-Dwelling Firm Service:** “property owners of all
  multi-dwellings with six or more units served through a single meter.”

**Connecticut Natural Gas** — CNG 2025 rate brochure and Rate RSG tariff
([CNG rates](https://www.cngcorp.com/documents/d/cng/cng_2025_rateschedule_rev-10-28-25);
[Rate RSG PDF](https://www.cngcorp.com/documents/40122/29053211/CNG+01-RSG+%28Residential+Service+General%29+doc.pdf/a3919d4d-91bf-5d5f-646c-b58e23f4ab55?t=1743616748465);
not directly fetchable, see verification note above):

- Brochure: “Residential customers have **three** separate rates: RSG … RSH …
  RMDS …”
- Rate RSG availability (effective 05/01/2025): “For all single-family
  residential customers and all multi-family residential customers where the
  number of dwelling units is five (5) or less, where the use of natural gas is
  for other than space heating. Residential customers where the number of
  dwelling units is five (5) or less and use natural gas for space heating will
  be served under the Company’s Rate RSH.” RMDS is the 6+ single-meter
  multi-dwelling class.

**Southern Connecticut Gas** — [SCG Pricing](https://www.soconngas.com/account/understandyourbill/pricing)
lists RATE RSG, RATE RSH, and RATE RMDS as distinct residential tariffs
(not directly fetchable, see verification note above); the
[Rate RSG PDF](https://www.soconngas.com/documents/40142/28538985/SCG+01-RSG.pdf/703d2714-f840-2a9a-5e09-d74f84d899bb?t=1746539159747)
mirrors CNG almost verbatim: “Available every day of the year to residential
Customers for all uses other than space heating where the number of dwelling
units supplied through one meter is five or less. Residential customers …
that use natural gas for space heating will be served under the Company’s …
Rate RSH.” CNG and SCG share the Avangrid rate redesign (PURA Docket 23-11-02).

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

### Intentionally not fetched (same residential class, different variant)

| Variant                                                      | Why omit                                                                                                                                                                                            |
| ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `*-SE` / System Expansion (on-main / off-main)               | Applies to new service after the SE cutover date; existing ResStock stock is modeled on standard RSG/RSH/RMDS (or 01/02/03). Documented on Eversource Rate 02-SE / 03-SE and CNG/SCG SE brochures.  |
| Third-party / Operator Gas Supply RMDS (or Rate 03) variants | Competitive supply path; residential single-family sales remain with the LDC under CT deregulation norms ([PURA Gas](https://portal.ct.gov/pura/gas/gas)). Company-supply RMDS/03 covers the class. |
| Commercial/industrial (SGS, MGS, LGS, Rate 10/20/30, etc.)   | Out of residential scope.                                                                                                                                                                           |

Together, the nine IOU schedules plus Norwich's two are the company-supply
versions of every residential class published for the four modeled LDCs — so
every residential customer maps to one of the fetched keys.

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
# From rate_design/hp_rates/ (after adding a CT fetch-gas-tariffs recipe, or):
uv run python utils/pre/fetch_gas_tariffs_rateacuity.py \
  rate_design/hp_rates/ct/config/tariffs/gas/rateacuity_tariffs.yaml \
  rate_design/hp_rates/ct/config/tariffs/gas

# Copy null_gas_tariff.json from another state (e.g. RI or MD), then:
just s ct ensure-gas-tariff-envelope
# or: just -f ct/Justfile ensure-gas-tariff-envelope  (if env already loaded)
```

Requires `RATEACUITY_USERNAME` and `RATEACUITY_PASSWORD`. Writes one
`{tariff_key}.json` + `.csv` per schedule row above. `utils/utility_codes.py`
must include `gas_tariff_key` and `rate_acuity_utility_names` for each shortcode
(set for `ct_natural_gas`, `yankee_gas`, `southern_ct_gas`, `norwich_muni`).

### Out of scope for this ticket

- CT `gas_tariff_mapper.py` branches / `EXPECTED_GAS_UTILITIES`
- `create-gas-tariff-maps-all` / tariff-map CSVs
- `validate-config` against assigned `sb.gas_utility` values
- Adding a CT `fetch-gas-tariffs` Justfile recipe (nice-to-have wiring; not
  required if the fetch script is invoked directly as above)
