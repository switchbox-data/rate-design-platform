# CT Eversource custom rates (Docket 26-05-10)

Hand-encoded proposed CL&P/Eversource residential tariffs from **PURA Docket 26-05-10**, used for
heat-pump rate-design analysis. Two tariffs are defined as delivery + `_supply` JSON pairs under
`rate_design/hp_rates/ct/config/tariffs/electric/`, each with a matching rev-requirement top-up YAML
under `rate_design/hp_rates/ct/config/rev_requirement/top-ups/monthly_rates/`:

| Tariff key                     | What it is                                                              |
| ------------------------------ | ----------------------------------------------------------------------- |
| `ct_eversource_rate6`          | Proposed **Rate 6** (Optional Residential Space Heating) — the HP rate. |
| `ct_eversource_rate1_proposed` | Proposed **Rate 1** (Residential) — a matched comparison baseline.      |

These are **distinct** from the current-vintage `ct_eversource` default (fetched from RateAcuity, see
[`ct_tariff_fetch.md`](ct_tariff_fetch.md)). The current default is left untouched.

> [!NOTE]
> The matching **revenue-requirement** targets (what CAIRO calibrates to) live in a separate set of
> whole-residential-class files documented in
> [`ct_eversource_class_rev_requirement.md`](ct_eversource_class_rev_requirement.md).

> [!WARNING]
> **Do not use these tariffs as a CAIRO pre-calc (calibration) target.** They are fixed,
> hand-encoded rates at filed values with **no calibrated class revenue requirement**. CAIRO's
> pre-calc mode scales all volumetric rates proportionally to hit a target revenue requirement
> (see [`tariff_generation_pipeline.md`](tariff_generation_pipeline.md), "CAIRO calibration"),
> which would rescale these away from their filed values and destroy the point of the comparison.
> Use them **only in fixed / bill-calc runs**, where the encoded rates are applied as-is.

## Why a proposed Rate 1 exists

Downstream analysis needs to decompose the **delivery bill (inclusive of transmission)** for Rate 1
vs Rate 6 on a _matched_ basis. Rate 6 is defined in the filed exhibits entirely as a set of deltas
against the proposed Rate 1 (same customer class, same rate case), so the only components that differ
between the two are the **customer charge** and the **core distribution energy charge**. Every other
component — transmission, CTA, public benefits (SBC/CAM/RE), FMCC-Delivery, and the supply commodity —
is identical and cancels in the Rate 6 − Rate 1 delta. Encoding a proposed Rate 1 on the _same filed
vintage_ lets us attribute bill differences to distribution rate design rather than to a vintage
mismatch.

## Decisions

- **Filed 2026 delivery vintage.** All delivery components of both rates come from the filed rate-case
  exhibits (`CLP-RATES-3` filed tariff, `CLP-RATES-2.4` rate build, `CLP-RATES-6` Rate 6 worksheet),
  not from RateAcuity. Docket 26-05-10 is a **distribution** case: it changes only distribution + the
  customer charge; transmission, public benefits, and supply are pass-throughs printed at filing
  vintage.
- **ESI and RDM rolled into distribution.** In the proposed rates the Electric System Improvement
  (ESI) mechanism sunsets and its capital, plus NBFMCC/SBC capital, roll into base distribution;
  the revenue decoupling mechanism (RDM/RAM) continues but its revenue is set inside base distribution.
  We therefore **zero ESI and RDM** in the top-up YAMLs (this also fixes an ESI double-count in the
  earlier draft).
- **SBC included as a real bill component.** The System Benefits Charge (`-$0.00196/kWh`, a credit) is
  **included** here because these are bill-calc / comparison tariffs and we encode what customers
  actually pay. (The current `ct_eversource` RateAcuity top-up instead drops SBC via
  `exclude_eligibility`, which is a BAT-classification choice, not a billing one.)
- **CTA and FMCC-Delivery keep filed values, including negatives.** `CTA = $0.00496`,
  `FMCC-Delivery = -$0.01911`; the negative credit nets into the delivery revenue requirement rather
  than being floored at zero.
- **Supply from Genability 2025 seasonal commodity.** The rate case does not set supply, so both
  tariffs reuse the same bundled supply commodity as `ct_eversource_default_supply.json`:
  `$0.1119/kWh` (Jan–Jun) and `$0.09748/kWh` (Jul–Dec).
- **Distinct rate key.** The comparison Rate 1 is keyed `ct_eversource_rate1_proposed`; the current
  `ct_eversource` default is not modified.

## Rate structure

### Shared delivery riders (flat, identical for both rates)

| Rider                             | Rate ($/kWh) |
| --------------------------------- | ------------ |
| Transmission                      | 0.05050      |
| Competitive Transition Assessment | 0.00496      |
| Systems Benefits Charge (credit)  | -0.00196     |
| Conservation Adjustment Mechanism | 0.00600      |
| Renewable Energy                  | 0.00100      |
| FMCC-Delivery (credit)            | -0.01911     |
| **Sum (derived)**                 | **0.04139**  |

ESI and RDM are `0` (rolled into distribution). Sum of shared riders = `0.05050 + 0.00496 - 0.00196

- 0.00600 + 0.00100 - 0.01911 = 0.04139/kWh` (**derived**).

### Rate 6 (`ct_eversource_rate6`)

- **Customer charge:** $30.95/month.
- **Distribution:** winter (Nov–Mar) tiered `$0.09431` (≤700 kWh) / `$0.07517` (>700 kWh);
  non-heating (Apr–Oct) flat `$0.09431`.
- **Delivery energy** = distribution + `0.04139` riders (**derived**):

| Season / block        | Delivery ($/kWh) | + supply → supply-JSON ($/kWh) |
| --------------------- | ---------------- | ------------------------------ |
| Winter ≤700 (Jan–Mar) | 0.13570          | 0.24760 (+0.1119)              |
| Winter >700 (Jan–Mar) | 0.11656          | 0.22846 (+0.1119)              |
| Non-heating (Apr–Jun) | 0.13570          | 0.24760 (+0.1119)              |
| Non-heating (Jul–Oct) | 0.13570          | 0.23318 (+0.09748)             |
| Winter ≤700 (Nov–Dec) | 0.13570          | 0.23318 (+0.09748)             |
| Winter >700 (Nov–Dec) | 0.11656          | 0.21404 (+0.09748)             |

The delivery JSON has 2 periods (winter tiered / non-heating flat); the supply JSON has 4 periods
because the seasonal distribution block (winter tier) and the seasonal supply commodity (Jan–Jun vs
Jul–Dec) cut the year differently.

### Rate 1 (`ct_eversource_rate1_proposed`)

- **Customer charge:** $12.36/month.
- **Distribution:** flat `$0.12196/kWh` (incl. GET, ESI/RDM folded in).
- **Delivery energy** = `0.12196 + 0.04139 = 0.16335/kWh` (**derived**, flat all year):

| Period  | Delivery ($/kWh) | + supply → supply-JSON ($/kWh) |
| ------- | ---------------- | ------------------------------ |
| Jan–Jun | 0.16335          | 0.27525 (+0.1119)              |
| Jul–Dec | 0.16335          | 0.26083 (+0.09748)             |

## Per-number provenance

Every value is traced to its source exhibit (under `context/sources/`), the specific location, and
the derivation where the exhibit itself derives it. Values we compute are marked **derived** with the
arithmetic shown from cited components.

| Component                         | Value                                     | Source (exhibit + location)                                                                                                  | Derivation / notes                                                                                                                                                                                  |
| --------------------------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Rate 6 customer charge            | $30.95/mo                                 | `exhibit_clp_rates_6.md` p. 2, lines 48/50/52; also p. 3 line 159                                                            | Marginal customer cost $28.98 × 1.0680 GET = $30.95                                                                                                                                                 |
| Rate 6 distribution Block 1       | $0.09431/kWh                              | `exhibit_clp_rates_6.md` p. 2, lines 66–68, 77; also p. 3 line 167                                                           | Class EC RR $796,054,171 / RY billed kWh 8,440,783,007 = $0.09431                                                                                                                                   |
| Rate 6 winter >700 kWh            | $0.07517/kWh                              | `exhibit_clp_rates_6.md` p. 2, lines 69–70, 78; also p. 3 line 167                                                           | Block 1 $0.09431 + discount ($0.01914) = $0.07517                                                                                                                                                   |
| Rate 1 customer charge            | $12.36/mo                                 | `exhibit_clp_rates_6.md` p. 3 line 171; `exhibit_clp_rates_2_residential.md` line 72, 184                                    | CLP-RATES-2.4 Proposed+GET; = pre-GET $11.52 / (1 − 0.068). (MRCC-tab $11.09 discrepancy flagged in exhibit line 184.)                                                                              |
| Rate 1 distribution               | $0.12196/kWh                              | `exhibit_clp_rates_6.md` p. 3 line 172, 184; `exhibit_clp_rates_2_residential.md` line 167                                   | Value the filed Rate 6 exhibit uses for the Rate 1 all-hours distribution charge. CLP-RATES-2.4 functional summary shows 12.2915¢ pre-GET / 13.1886¢ +GET (slightly different normalization basis). |
| Transmission                      | $0.05050/kWh                              | `exhibit_clp_rates_3_residential_tariffs.md` lines 34, 128                                                                   | Filed tariff line; current = proposed (pass-through)                                                                                                                                                |
| Competitive Transition Assessment | $0.00496/kWh                              | `exhibit_clp_rates_3_residential_tariffs.md` lines 28, 122                                                                   | Filed tariff line                                                                                                                                                                                   |
| Systems Benefits Charge (credit)  | -$0.00196/kWh                             | `exhibit_clp_rates_3_residential_tariffs.md` lines 40, 134                                                                   | Filed tariff line                                                                                                                                                                                   |
| Conservation Adjustment Mechanism | $0.00600/kWh                              | `exhibit_clp_rates_3_residential_tariffs.md` lines 41, 135                                                                   | Filed tariff line                                                                                                                                                                                   |
| Renewable Energy                  | $0.00100/kWh                              | `exhibit_clp_rates_3_residential_tariffs.md` line 42                                                                         | Filed tariff line                                                                                                                                                                                   |
| FMCC-Delivery (credit)            | -$0.01911/kWh                             | `exhibit_clp_rates_3_residential_tariffs.md` lines 43, 137                                                                   | Filed tariff line                                                                                                                                                                                   |
| ESI / RDM                         | 0                                         | `exhibit_clp_revreq_1.md` lines 111, 137, 215, 217, 219                                                                      | ESI sunsets and its capital + NBFMCC/SBC capital roll into base distribution; RDM continues inside base distribution. Rate effective July 1, 2027.                                                  |
| Generation (reference only)       | $0.12791/kWh                              | `exhibit_clp_rates_3_residential_tariffs.md` lines 49, 143                                                                   | Not used — supply comes from Genability                                                                                                                                                             |
| FMCC-Generation (reference only)  | -$0.00150/kWh                             | `exhibit_clp_rates_3_residential_tariffs.md` lines 51, 145                                                                   | Not used — supply comes from Genability                                                                                                                                                             |
| Supply commodity                  | 0.1119 / 0.09748                          | Genability masterTariffId 614 via `utils/pre/rev_requirement/fetch_monthly_rates.py` (calendar 2025, day-weighted per month) | Encoded in `ct_eversource_monthly_rates_2025.yaml` `add_to_srr`; reconstructable as `ct_eversource_default_supply.json` energy − `ct_eversource_default.json` delivery energy                       |
| **Shared riders sum**             | **0.04139**                               | derived                                                                                                                      | transmission + CTA + SBC + CAM + RE + FMCC-Del                                                                                                                                                      |
| **Rate 1 delivery energy**        | **0.16335**                               | derived                                                                                                                      | 0.12196 + 0.04139                                                                                                                                                                                   |
| **Rate 6 delivery, Block 1**      | **0.13570**                               | derived                                                                                                                      | 0.09431 + 0.04139                                                                                                                                                                                   |
| **Rate 6 delivery, winter >700**  | **0.11656**                               | derived                                                                                                                      | 0.07517 + 0.04139                                                                                                                                                                                   |
| **Rate 1 supply-JSON periods**    | **0.27525 / 0.26083**                     | derived                                                                                                                      | 0.16335 + 0.1119 / 0.16335 + 0.09748                                                                                                                                                                |
| **Rate 6 supply-JSON periods**    | **0.24760 / 0.22846 / 0.23318 / 0.21404** | derived                                                                                                                      | delivery block + seasonal supply component (see Rate 6 table)                                                                                                                                       |

## Transmission vintage note

The filed transmission rate is **$0.05050/kWh** (`exhibit_clp_rates_3_residential_tariffs.md` line 34,
matching CLP-RATES-2.4). The current-vintage RateAcuity top-up
(`ct_eversource_monthly_rates_2025.yaml`) instead carries transmission at **0.03401** (winter) /
**0.04433** (summer). This is **not** a bucketing error: distribution and the customer charge match
exactly across the two sources; only the pass-through riders differ, because they reset periodically
(transmission via the Company's Transmission Adjustment Clause, FMCC via reconciliation). The gap is a
~1-year vintage difference — the rate-case testimony is dated **July 14, 2026** (`exhibit_clp_rates_1`)
with a **July 1, 2027** rate effective date (`exhibit_clp_revreq_1.md` line 215), whereas the
RateAcuity fetch reflects calendar-2025 effective rates. For the matched Rate 1 vs Rate 6 comparison we
take transmission (and all riders) as a single filed-2026 set so the two rates share the same vintage.

## Files

- `rate_design/hp_rates/ct/config/tariffs/electric/ct_eversource_rate6.json` (+ `_supply.json`)
- `rate_design/hp_rates/ct/config/tariffs/electric/ct_eversource_rate1_proposed.json` (+ `_supply.json`)
- `rate_design/hp_rates/ct/config/rev_requirement/top-ups/monthly_rates/ct_eversource_rate6_monthly_rates_2025.yaml`
- `rate_design/hp_rates/ct/config/rev_requirement/top-ups/monthly_rates/ct_eversource_rate1_proposed_monthly_rates_2025.yaml`
