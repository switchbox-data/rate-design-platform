# FY26 MEAP Benefit Matrix

**Program**: Maryland Energy Assistance Program (MEAP) — OHEP heating assistance grant
**Fiscal / program year**: FY26 (July 1, 2025 – June 30, 2026)
**Source PDF**: `dev/md_lmi_sources/FY26-MEAP-Benefit-Matrix.pdf`
**Original URL**: https://dhs.maryland.gov/documents/OHEP/Advisory%20Board/FY26-MEAP-Benefit-Matrix-2-1-1.pdf
**Publisher**: Maryland Department of Human Services, Office of Home Energy Programs (OHEP)
**Pages**: 1

Amounts below are **annual grant dollars** by OHEP Poverty / Benefit Level and primary heating fuel. Extracted from the source PDF via `pdftotext -layout`.

---

## Benefit table

| Fuel      | Level 1<br>0–25% FPL | Level 2<br>26–50% FPL | Level 3<br>51–100% FPL | Level 4<br>101–150% FPL | Level 5<br>151–200% FPL | Level 6<br>Subsidized / Sub-metered | Level 7<br>Over 200% FPL |
| --------- | -------------------- | --------------------- | ---------------------- | ----------------------- | ----------------------- | ----------------------------------- | ------------------------ |
| Electric  | $100                 | $100                  | $100                   | $100                    | $100                    | $100                                | $25                      |
| Gas       | $550                 | $475                  | $400                   | $360                    | $300                    | $225                                | $25                      |
| Oil       | $1,100               | $990                  | $880                   | $770                    | $650                    | $225                                | $25                      |
| Propane   | $1,000               | $930                  | $830                   | $725                    | $600                    | $225                                | $25                      |
| Wood/Coal | $550                 | $475                  | $400                   | $360                    | $300                    | $225                                | $25                      |

Header labels in the source PDF: `FY26 LIHEAP/MEAP` with columns Level 1 … Level 7 as above.

---

## Notes for modeling

- MEAP is a **heating** grant paid to the fuel supplier / utility on the customer’s behalf (see DHS OHEP “About energy assistance”).
- Lookup keys: **(Poverty Level, heating fuel)**. No usage band — unlike EUSP.
- Level 6 in this matrix is labeled **Subsidized/Sub-metered**, not an FPL band. Level 7 is **Over 200% FPL**.
- Related extract: [FY26-EUSP-Benefit-Matrix.md](FY26-EUSP-Benefit-Matrix.md). Domain writeup: [lmi_discounts_in_md.md](../domain/charges/lmi_discounts_in_md.md).
