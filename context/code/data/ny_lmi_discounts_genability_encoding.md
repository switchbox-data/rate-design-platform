# EAP / LMI discount encoding in NY Genability tariffs

This note evaluates whether each NY utility’s **Genability** tariff (electric, in `rate_design/ny/hp_rates/config/tariffs/electric/genability/`) encodes the Energy Affordability Program (EAP) / low-income discounts as described in [lmi_discounts_in_ny.md](../domain/lmi_discounts_in_ny.md). For each utility we report: **program concept**, **tier structure**, **heating split** (if any), **credit levels**, and **divergences** from our reference doc.

**Scope:** Electric tariffs only. Gas-only utilities (KEDNY, KEDLI, National Fuel) are not present in this electric genability folder; their EAP credits are documented in the LMI doc but not evaluated here.

---

## Summary table

| Utility               | EAP in Genability? | Tier structure     | Heating split               | Amounts vs doc             |
| --------------------- | ------------------ | ------------------ | --------------------------- | -------------------------- |
| **Central Hudson**    | Yes                | 4 tiers (EAP)      | Yes (elec heat vs non-heat) | Different (newer schedule) |
| **RG&E**              | Yes                | 4 tiers            | No (same for heat/non-heat) | Match                      |
| **O&R**               | Yes                | 4 tiers + EEAP 5–7 | No (electric same)          | Match                      |
| **NIMO** (NG Upstate) | Partial            | Not EAP tiers      | Heat vs non-heat only       | Different program          |
| **NYSEG**             | No                 | —                  | —                           | —                          |
| **Con Edison**        | No                 | —                  | —                           | —                          |
| **PSEG Long Island**  | No                 | —                  | —                           | —                          |

---

## 1. Central Hudson (cenhud_default.json)

**Encoded:** Yes.

- **Rate:** "Low Income Discount"; applicabilityKey `lowIncomeBillDiscount2033`.
- **Program concept:** Fixed monthly bill credit (FIXED_PRICE, MONTHLY); aligns with EAP.
- **Tier structure:** Four tiers (1–4) for traditional EAP.
- **Heating split:** Yes. Two customer types: "Electric Heat Customer" and "Non-Electric Heat Customer," each with four tier amounts. Matches the LMI doc (CenHud is the only NY electric utility where Tier 3/4 elec heat ≠ elec non-heat).
- **Effective date in Genability:** 2025-07-01.

**Amounts (Genability) vs doc (effective Dec 2024):**

| Tier | Doc: elec heat / elec non-heat | Genability: Electric Heat / Non-Electric Heat |
| ---- | ------------------------------ | --------------------------------------------- |
| 1    | $60.46 / $60.46                | $67.99 / $67.99                               |
| 2    | $75.84 / $75.84                | $85.17 / $85.17                               |
| 3    | $110.75 / $97.48               | $126.10 / $107.98                             |
| 4    | $104.03 / $94.13               | $121.72 / $105.80                             |

**Divergence:** Structure is the same; dollar amounts are higher in Genability and the tariff is effective July 2025. Either a newer annual update or a different source. EEAP (Tiers 5–7) is not present in this Genability tariff.

---

## 2. RG&E (rge_default.json)

**Encoded:** Yes.

- **Rate:** "Low Income Program Discount"; applicabilityKey `lowIncomeHEAPTier`.
- **Program concept:** Fixed monthly credit (FIXED_PRICE, MONTHLY); HEAP tier language in applicability matches EAP.
- **Tier structure:** Four tiers: None, tier1, tier2, tier3, tier4.
- **Heating split:** No. Single set of credits for electric (doc: RG&E uses same elec heat and elec non-heat for all tiers).
- **Effective date in Genability:** 2025-05-01.

**Amounts (Genability) vs doc:**

| Tier | Doc (elec heat = elec non-heat) | Genability |
| ---- | ------------------------------- | ---------- |
| 1    | $25.04                          | $25.04     |
| 2    | $42.21                          | $42.21     |
| 3    | $65.03                          | $65.03     |
| 4    | $62.84                          | $62.84     |

**Divergence:** None. Full match. EEAP (Tiers 5–7) is not in this tariff.

---

## 3. Orange & Rockland (or_default.json)

**Encoded:** Yes (EAP Tiers 1–4 and EEAP Tiers 5–7).

- **Rates:**
  - "Low Income Bill Credit"; applicabilityKey `lowIncomeHEAPTier` (Tiers 1–4).
  - "Enhanced Energy Affordability Credits"; applicabilityKey `lowIncome2BillCredits691` (Tiers 5–7; description matches EEAP income thresholds).
- **Program concept:** Fixed monthly credits; HEAP tier + EEAP tier logic matches the LMI doc.
- **Tier structure:** 4 HEAP tiers + 3 EEAP tiers (5, 6, 7).
- **Heating split:** No for electric (doc: O&R elec heat = elec non-heat).
- **Effective date in Genability:** 2026-01-13.

**Amounts (Genability) vs doc:**

**HEAP (Tiers 1–4):**

| Tier | Doc (elec) | Genability |
| ---- | ---------- | ---------- |
| 1    | $76.81     | $76.81     |
| 2    | $92.67     | $92.67     |
| 3    | $114.52    | $114.52    |
| 4    | $111.36    | $111.36    |

**EEAP (Tiers 5–7):**

| Tier | Doc (elec) | Genability (applicabilityValue)                              |
| ---- | ---------- | ------------------------------------------------------------ |
| 5    | $76.81     | incomeLessThanSixtyPercentOfSMI → $76.81                     |
| 6    | $43.44     | incomeGreaterThanOrEqualToSixty…ButLessThanEighty… → $43.44  |
| 7    | $1.00      | incomeGreaterThanOrEqualToEighty…ButLessThanHundred… → $1.00 |

**Divergence:** None. Full match for both EAP and EEAP electric credits.

---

## 4. National Grid Upstate / Niagara Mohawk (nimo_default.json)

**Encoded:** Only in a different, limited form — not the full EAP.

- **Rate:** "Income Eligible Basic Service Credit"; applicabilityKey `lowIncomeCustomerType`.
- **Program concept:** Fixed monthly credit, but only two customer types (no HEAP tier 1–4 structure).
- **Tier structure:** Not EAP. Three applicability values: `None` (0), `Non-Electric Heat Customer` ($5), `Electric Heat Customer` ($15).
- **Heating split:** Yes (heat vs non-heat), but with only these two credit levels.
- **Effective date in Genability:** 2025-09-01.

**Doc (EAP) vs Genability:**

| Tier      | Doc (NG Upstate): elec heat = elec non-heat | Genability                                |
| --------- | ------------------------------------------- | ----------------------------------------- |
| 1         | $22.46                                      | —                                         |
| 2         | $39.64                                      | —                                         |
| 3         | $62.45                                      | —                                         |
| 4         | $60.26                                      | —                                         |
| (no tier) | —                                           | Non-Electric Heat: $5; Electric Heat: $15 |

**Divergence:** Genability does **not** encode the full EAP (no HEAP tiers, no tier amounts). It encodes a separate "Income Eligible Basic Service Credit" with only $5 (non-heat) and $15 (heat). For full EAP credits (e.g. for bill or BAT work), use the LMI doc / lookup table, not this Genability rate.

---

## 5. NYSEG (nyseg_default.json)

**Encoded:** No.

- No "Low Income," "EAP," "HEAP," or "affordability" rate or applicability key in the tariff.
- Only applicability found that is discount-like: "Residential Agricultural Discount" (`isRadProgramParticipant562`).
- **Divergence:** EAP credits for NYSEG (e.g. $47.26, $67.01, $88.99, $86.16 for elec Tiers 1–4, plus EEAP) must be taken from the LMI doc / lookup table; they are not in Genability.

---

## 6. Con Edison (coned_default.json)

**Encoded:** No.

- `hasRateApplicability: false`; no LMI/EAP/HEAP applicability key in the tariff properties or rate names.
- **Divergence:** EAP credits for Con Ed (e.g. elec heat vs non-heat by tier, gas, EEAP) must be taken from the LMI doc; they are not in Genability.

---

## 7. PSEG Long Island (psegli_default.json)

**Encoded:** No.

- `hasRateApplicability: false`; no LMI/EAP or HAP applicability key.
- The LMI doc describes PSEG LI's **Household Assistance Program (HAP)** — flat $45/month, no tiers, outside PSC EAP. That structure is not present in this Genability tariff.
- **Divergence:** HAP credit must be taken from the LMI doc; it is not in Genability.

---

## Gas-only utilities (KEDNY, KEDLI, National Fuel)

Not evaluated here. The Genability files in `rate_design/ny/hp_rates/config/tariffs/electric/genability/` are electric only; gas EAP credits for these utilities are documented in [lmi_discounts_in_ny.md](../domain/lmi_discounts_in_ny.md) only.

---

## Charge map (charge_map.json)

The following Genability tariff rate IDs are mapped to `"master_charge": "Low-income discounts"` with `"decision": "skip"` (i.e. excluded from the main BAT/rate flow but identified for eligibility):

- **20812636** — RG&E Low Income Program Discount
- **20715136** — NIMO (placeholder / rider; actual credit is "Income Eligible Basic Service Credit")
- **20799335** — O&R Low Income Bill Credit
- **20904391** — O&R Enhanced Energy Affordability Credits
- **20871983** — Central Hudson Low Income Discount

So the platform explicitly skips LMI discount _charges_ in the main tariff run and applies LMI via a separate lookup (e.g. post-processing or LMI doc table); the Genability encoding is still useful for structure and, where it matches, for validating or sourcing credit amounts.
