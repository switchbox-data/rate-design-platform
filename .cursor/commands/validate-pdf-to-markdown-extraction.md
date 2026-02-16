---
description: Validate a PDF-to-markdown extraction by comparing the extract to the source PDF and producing a consistent quality report
argument-hint: <path-to-extract.md> [path-to-source.pdf]
---

# Validate PDF-to-Markdown Extraction

You are comparing an **existing markdown extract** to its **source PDF** to assess how well the extraction succeeded across all major categories. Your output is a **structured validation report** that other agents or humans can use to judge quality and decide on follow-up actions.

**Inputs:** The user provides the path to the markdown file (e.g. `context/papers/bill_alignment_test.md`). The source PDF is either provided as a second argument or inferred from the markdown path by replacing the file with the same base name and `.pdf` extension in the same directory (e.g. `context/papers/bill_alignment_test.pdf`). Read both the PDF (or its text representation) and the markdown to perform the comparison.

---

## Major Extraction Categories

Evaluate the extract against the PDF for **every** category below. Use the guidance in the next section to assign a verdict and note issues.

1. **Equations** – Display equations (numbered or unnumbered), inline math in prose, equation labels, appendix equations. Check correctness and completeness; note if equations were garbled in PDF text and had to be reconstructed.
2. **Citations** – In-text references (e.g. [1], [2], [3]–[6], (Author et al., Year)), reference list / bibliography at end, DOIs/URLs, journal or book titles in refs, special characters in author names.
3. **Headings and nesting** – Document title (H1), section numbers and titles (H2/H3/…), appendix headings, abstract/front/back section headings. Hierarchy and numbering should match the source.
4. **Figures and diagrams** – Every figure has a verbal description (and optional "see PDF" pointer). Descriptions should be detailed enough to understand content without the image; no figure in the PDF should be missing from the extract.
5. **Tables** – All tables present, headers and cells match source, captions preserved. Note if layout was reordered for readability (e.g. section header rows).
6. **Footnotes** – All footnote content preserved either as markdown footnotes `[^n]:` or inlined (e.g. **Note**: or in-body). No substantive footnote text dropped.
7. **Lists** – Numbered and bulleted lists preserved; inline numbered lists (e.g. "1) … 2) … 5)") converted to markdown lists where the extraction command requests it.
8. **Text formatting** – Bold/italic/code used to mirror source emphasis; variable names and short math in prose use inline math `$...$` where the extraction command requests it; abbreviations and key terms consistent.
9. **Front matter** – Title, authors, **author affiliations** (correspondence, institutions, addresses), abstract, keywords, date/source/version if present. Check that affiliations were not omitted when present in the PDF.
10. **Back matter** – Funding, author contributions, data statement, acknowledgments, appendices. All present and complete.
11. **Other** – En/em dashes, symbols (e.g. ÷), equation numbers in labels, URLs wrapped or linked; no stray PDF artifacts (e.g. page numbers in body).

---

## How to Evaluate Extraction Quality

- **Presence** – Is the content there at all? (e.g. every figure has some description; every table is in the markdown.)
- **Fidelity** – Does it match the source? (e.g. equation semantics correct even if reconstructed; reference list entries complete; section numbers correct.)
- **Completeness** – Is nothing meaningful dropped? (e.g. no missing footnotes; no truncated tables; affiliations included when in PDF.)

For each category:

- **Verdict**: Use **A** (complete and accurate), **B** (minor gaps or fixable issues), **C** (substantial gaps or errors), or **D** (missing or wrong).
- **Notes**: One to three sentences on what was done well and what (if anything) failed or was omitted. Cite line numbers or section names in the extract when helpful.

PDF text extraction often **garbles equations** (encoding/symbols). If the extract reconstructed equations from surrounding prose and labels, that is acceptable and counts as successful provided the reconstructed math is correct. Similarly, **figures** cannot be reproduced literally; a detailed verbal description plus optional "[→ See original PDF page X]" is success.

---

## Prescribed Report Output Format

Produce your assessment **exactly** in the following structure. Use the headings and subsections as given; fill in content and tables. **Section 6 (TL;DR Summary)** is at the end so readers can jump to it first and then decide whether to consult the full report above.

```markdown
# PDF-to-Markdown Extraction Validation Report

## 1. Overview

| Field                        | Value          |
| ---------------------------- | -------------- |
| Source PDF                   | [path or name] |
| Extract                      | [path or name] |
| PDF pages                    | [N]            |
| Extract word count (approx.) | [optional]     |

**Verdict:** [One sentence: overall success, partial success, or material gaps.]

---

## 2. Category-by-Category Assessment

| Category             | Verdict | Notes         |
| -------------------- | ------- | ------------- |
| Equations            | A/B/C/D | [Brief note.] |
| Citations            | A/B/C/D | [Brief note.] |
| Headings and nesting | A/B/C/D | [Brief note.] |
| Figures and diagrams | A/B/C/D | [Brief note.] |
| Tables               | A/B/C/D | [Brief note.] |
| Footnotes            | A/B/C/D | [Brief note.] |
| Lists                | A/B/C/D | [Brief note.] |
| Text formatting      | A/B/C/D | [Brief note.] |
| Front matter         | A/B/C/D | [Brief note.] |
| Back matter          | A/B/C/D | [Brief note.] |
| Other                | A/B/C/D | [Brief note.] |

### Detail (optional)

[Add short prose for any category that needs more than one line—e.g. equation reconstruction, table layout changes, footnote handling.]

---

## 3. Summary: Tricky Categories

Summarize how the extract fared on categories that are often hard to get right:

- **Equations** – Were display and inline equations correct and complete? If the PDF text was garbled, was reconstruction from context successful?
- **Figures/diagrams** – Were all figures covered with useful descriptions? Any too terse or missing?
- **Footnotes** – All content preserved? Any linter-driven inlining or link conversion that changed behavior?
- **Front matter (affiliations)** – If the PDF had an affiliation block, was it extracted?

[2–4 sentences total.]

---

## 4. Improvements for This Extract

List **concrete, actionable improvements** that would make _this specific_ extract better. These are things a human or agent could do by editing the markdown or re-running the extractor with different instructions. Be specific (e.g. "Add author affiliations from PDF p.1", "Fix Equation (B4) denominator to match Appendix B", "Convert inline '1) … 5)' in Section 2.2 to a numbered list").

- [ ] Improvement 1
- [ ] Improvement 2
- …

(If nothing material, say "None; extract meets expectations.")

---

## 5. Recommendations for the Extraction Command

Suggest **only** changes to the **extract-pdf-to-markdown** slash command (or the instructions it embodies) that would improve **future, out-of-sample** extractions—not one-off fixes for this PDF.

- **Be conservative.** Do not suggest command edits that are specific to this document (e.g. "add a rule for BAT acronym" or "always use Table 1 layout for two-column metric tables"). Prefer general rules (e.g. "extract author affiliations when present", "use inline math for variables in prose").
- If an issue in this extract is due to a **general** gap in the command (e.g. no instruction for affiliations, or no instruction for inline math), and the same gap would likely affect other PDFs, then recommend an addition or tweak to the command.
- If an issue is **idiosyncratic** (e.g. this PDF’s equations are unusual, or this table layout is rare), do **not** recommend a command change; instead, list it only in Section 4 (Improvements for This Extract) so the user can fix it manually or leave it.

List recommendations in brief bullet form. If there are no generalizable command improvements, say: "No command changes recommended; issues are specific to this extract."

---

## 6. TL;DR Summary (read this first)

**Purpose:** A self-contained summary so the user can read this section first (e.g. by jumping to it) and decide whether to consult the full report above. Place this section **at the end** of the report.

Use the following structure. Keep the whole section short (one short paragraph per subpart unless the extract is unusually complex).

### Overall score

[One of: **Excellent** / **Good** / **Adequate** / **Needs work**.] One sentence justifying the score (e.g. "All categories A/B; equations and figures reconstructed correctly; only minor front-matter gaps.")

### Document profile: heavy-use categories

[1–3 sentences.] Which extraction categories did _this_ document lean on most? (e.g. "Equation-heavy (main text + appendices). Multiple figures with detailed captions. Dense reference list and many footnotes.") This sets context for the qualitative assessment.

### Qualitative assessment

[2–4 sentences.] How did the extraction perform overall, especially in those heavy-use categories? (e.g. "Equations were reconstructed from context and are correct; display and inline math are consistent. Figure descriptions are detailed and standalone. Footnotes were inlined as **Note**: in places; content preserved. Tables and structure match the source.")

### What could be improved

- **Fix manually (this extract):** [Bullet list of concrete edits to the markdown or one-off fixes that are specific to this document. Empty if nothing.]
- **Consider improving the extract command:** [Bullet list of generalizable command changes that might help future extractions. Empty if none.]
```

---

## Process

1. **Locate inputs** – Resolve the markdown path from the user’s argument. Resolve the PDF path from the second argument or by same directory + same base name + `.pdf`.
2. **Read both** – Read the markdown extract in full. Read or sample the PDF (or its text) enough to compare structure, equations, figures, tables, front/back matter, and citations.
3. **Assess each category** – For each of the 11 categories, assign a verdict (A/B/C/D) and notes using the evaluation guidance above.
4. **Fill the report** – Output the report using exactly the prescribed format (Overview through Section 6 TL;DR Summary). Section 6 must appear at the end and summarize overall score, document profile (heavy-use categories), qualitative assessment, and what to fix manually vs. via the extract command.
5. **Be conservative in Section 5 and in the TL;DR** – Only recommend extraction-command changes that would generalize to other PDFs; leave document-specific fixes to Section 4 and to "Fix manually" in the TL;DR.
