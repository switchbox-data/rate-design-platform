# FY26 EUSP Benefit Matrix

**Program**: Electric Universal Service Program (EUSP) — OHEP electric assistance grant
**Fiscal / program year**: FY26 (July 1, 2025 – June 30, 2026)
**Source file**: `dev/md_lmi_sources/FY26-EUSP-Benefit-Matrix.docx`
**Original URL**: https://dhs.maryland.gov/documents/OHEP/Advisory%20Board/FY26-EUSP-Benefit-Matrix-Updated-7.7.25-2.docx
**Publisher**: Maryland Department of Human Services, Office of Home Energy Programs (OHEP)

Amounts below are **annual grant dollars** by OHEP Poverty / Benefit Level, primary heat source, and annual electric usage band. Extracted from the source `.docx` via `pandoc -t plain`. Source table headers sometimes show typographical artifacts (e.g. `12,0000kw`); usage bands are interpreted as:

| Band label in source                 | Interpreted annual kWh |
| ------------------------------------ | ---------------------- |
| `0-4,000kw`                          | 0–4,000 kWh            |
| `4,001-8,000kw`                      | 4,001–8,000 kWh        |
| `8,001-12,0000kw` / `8,001-12,000kw` | 8,001–12,000 kWh       |
| `> 12,000kw`                         | > 12,000 kWh           |

---

## Level 1 — 0–25% FPL

| Primary heat source | 0–4,000 kWh | 4,001–8,000 kWh | 8,001–12,000 kWh | > 12,000 kWh |
| ------------------- | ----------- | --------------- | ---------------- | ------------ |
| Electric            | $875        | $900            | $950             | $1,000       |
| Gas                 | $350        | $400            | $450             | $500         |
| Oil/Kerosene        | $350        | $400            | $450             | $500         |
| Propane             | $350        | $400            | $450             | $500         |
| Wood/Coal           | $350        | $400            | $450             | $500         |

## Level 2 — 26–50% FPL

| Primary heat source | 0–4,000 kWh | 4,001–8,000 kWh | 8,001–12,000 kWh | > 12,000 kWh |
| ------------------- | ----------- | --------------- | ---------------- | ------------ |
| Electric            | $800        | $850            | $900             | $950         |
| Gas                 | $300        | $350            | $400             | $450         |
| Oil/Kerosene        | $300        | $350            | $400             | $450         |
| Propane             | $300        | $350            | $400             | $450         |
| Wood/Coal           | $300        | $350            | $400             | $450         |

## Level 3 — 51–100% FPL

| Primary heat source | 0–4,000 kWh | 4,001–8,000 kWh | 8,001–12,000 kWh | > 12,000 kWh |
| ------------------- | ----------- | --------------- | ---------------- | ------------ |
| Electric            | $750        | $800            | $850             | $900         |
| Gas                 | $250        | $300            | $350             | $400         |
| Oil/Kerosene        | $250        | $300            | $350             | $400         |
| Propane             | $250        | $300            | $350             | $400         |
| Wood/Coal           | $250        | $300            | $350             | $400         |

## Level 4 — 101–150% FPL

| Primary heat source | 0–4,000 kWh | 4,001–8,000 kWh | 8,001–12,000 kWh | > 12,000 kWh |
| ------------------- | ----------- | --------------- | ---------------- | ------------ |
| Electric            | $700        | $750            | $800             | $850         |
| Gas                 | $225        | $250            | $300             | $350         |
| Oil/Kerosene        | $225        | $250            | $300             | $350         |
| Propane             | $225        | $250            | $300             | $350         |
| Wood/Coal           | $225        | $250            | $300             | $350         |

## Level 5 — 151–200% FPL

| Primary heat source | 0–4,000 kWh | 4,001–8,000 kWh | 8,001–12,000 kWh | > 12,000 kWh |
| ------------------- | ----------- | --------------- | ---------------- | ------------ |
| Electric            | $650        | $700            | $750             | $800         |
| Gas                 | $175        | $200            | $250             | $300         |
| Oil/Kerosene        | $175        | $200            | $250             | $300         |
| Propane             | $175        | $200            | $250             | $300         |
| Wood/Coal           | $175        | $200            | $250             | $300         |

## Level 6 — Subsidized / Roomer / Boarder and Sub-metered (all household sizes)

| Primary heat source | 0–4,000 kWh | 4,001–8,000 kWh | 8,001–12,000 kWh | > 12,000 kWh |
| ------------------- | ----------- | --------------- | ---------------- | ------------ |
| Electric            | $550        | $600            | $650             | $700         |
| Gas                 | $125        | $150            | $200             | $250         |
| Oil/Kerosene        | $125        | $150            | $200             | $250         |
| Propane             | $125        | $150            | $200             | $250         |
| Wood/Coal           | $125        | $150            | $200             | $250         |

## Level 7 — >200% FPL, Categorically Eligible Only (all household sizes)

| Primary heat source | 0–4,000 kWh | 4,001–8,000 kWh | 8,001–12,000 kWh | > 12,000 kWh |
| ------------------- | ----------- | --------------- | ---------------- | ------------ |
| Electric            | $25         | $25             | $25              | $25          |
| Gas                 | $25         | $25             | $25              | $25          |
| Oil/Kerosene        | $25         | $25             | $25              | $25          |
| Propane             | $25         | $25             | $25              | $25          |
| Wood/Coal           | $25         | $25             | $25              | $25          |

---

## Notes for modeling

- EUSP is OHEP’s **electric** assistance grant (once per program year). Recipients may choose budget billing (DHS OHEP “About energy assistance”).
- Lookup keys: **(Poverty Level, primary heat source, annual electric kWh band)**.
- Level 6 is a **housing / metering category** (subsidized / roomer / boarder / sub-metered), not an FPL band. Level 7 is **>200% FPL, categorically eligible only**.
- Related extract: [FY26-MEAP-Benefit-Matrix.md](FY26-MEAP-Benefit-Matrix.md). Domain writeup: [lmi_discounts_in_md.md](../domain/charges/lmi_discounts_in_md.md).
