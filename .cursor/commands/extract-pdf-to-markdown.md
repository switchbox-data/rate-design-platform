---
description: Extract PDF to markdown preserving structure, formatting, tables, equations, and visual descriptions
argument-hint: <path-to-pdf-file>
---

# PDF to Markdown Extraction

You are extracting a technical PDF into a **standalone, fully-formatted markdown document** that preserves as much information as possible while remaining readable and useful without the original PDF.

## Core Principles

1. **Primary audience is LLMs/agents** consuming the markdown as context. Prefer clarity and completeness over brevity: keep equation labels, explicit section structure, and full figure descriptions. Human-friendly rendering is secondary but desirable (e.g. LaTeX for math, proper lists).
2. **Fidelity over brevity**: Preserve every meaningful piece of information
3. **Structure preservation**: Keep original document hierarchy, sections, subsections
4. **Formatting in markdown**: Use markdown formatting (bold, italics, code blocks, etc.) to mirror original emphasis
5. **Impossible content handling**: For diagrams, charts, images—provide detailed textual descriptions inline
6. **Standalone design**: A reader should be able to work from this markdown alone; the PDF is emergency reference only
7. **LLM-friendly markers**: Use clear, parseable markers when you must indicate "see PDF for visual"

## Extraction Instructions

### Structure & Hierarchy

- Preserve all heading levels (H1→H2→H3, etc.)
- Maintain table of contents structure if present
- Keep section numbers, subsection labels, and organizational schemes from original. **When the source has a table of contents**, verify that numbered section titles and counts in the extract match the TOC (e.g. the last numbered section in the extract should correspond to the last TOC entry; unnumbered headings on the same page as a numbered section may stay unnumbered).
- Preserve logical document sections with `---` between them if they marked distinct parts

### Text & Formatting

- **Bold** for emphasis (use `**text**`)
- _Italics_ for terms and citations; for variable names and short math in prose use inline math `$...$` (see Equations below)
- `code` for technical terms, file paths, code snippets
- Preserve numbered and bulleted lists with correct nesting. **When the source has an inline numbered list** (e.g. "commissions should 1) … 2) … 5)" or "(1) … (2)" in one paragraph), **convert it to a markdown list** (numbered or bullet) unless that would break the flow of the paragraph.
- Keep paragraph structure and grouping exactly as in original
- Convert hyperlinks to markdown format: `[link text](URL)`

### Tables

- Convert **ALL** tables to markdown table format
- Preserve column headers and cell content exactly
- **Multi-column factor tables**: For tables with many numbered or lettered columns (e.g. (1)–(13), or A, B, C…), **verify column alignment for every row**, including the last. If one row has fewer or more populated cells than others (e.g. due to PDF layout or wrapping), ensure the extract preserves the same number of columns per row and does not drop or merge columns; add or align cells so each row matches the header.
- **When table content cannot be extracted** from the PDF (e.g. exhibit or appendix pages yield only titles or the table is image-based), do not invent cell data. Add an explicit note in the extract: _"Table content was not extractable from the PDF; description below is inferred from surrounding text. See original PDF for data."_ Then provide a short description of the table’s purpose and structure based on the narrative, and a "[→ See original PDF page X]" pointer.
- If a table is complex/wide, add a description before it explaining its structure
- Complex tables that don't render cleanly: provide both a prose description and the markdown version
- Keep alignment indicators if relevant (left, center, right aligned)
- **Table footnotes**: When the source has footnotes or notes tied to a table (e.g. superscripts in cells, "Notes:" or "Source:" below the table), handle them consistently: either (a) use markdown footnote refs in cells and `[^n]:` definitions below the table, or (b) fold the footnote text into the table caption or a single "Note:" or "Notes:" line immediately below the table. Choose one approach per document and apply it to all tables with footnotes.

### Equations & Mathematical Content

- **In running text**: Use **inline math** `$...$` for variable names and short expressions (e.g. $r_i$, $MC_h$, $\epsilon$, $ProposedRate_h$). This keeps math unambiguous for LLMs and renders well for humans.
- **Display equations**: Use **block math** `$$...$$` with LaTeX. Keep an optional label line above (e.g. `**Equation (1):**`) so the equation is tied to the text.
- Keep equations in original notation (LaTeX preferred). If equation is explained in surrounding text, include that text.
- For critical visual elements where the equation is unreadable in the PDF: `[EQUATION: describe what it shows]` before/after the transcribed formula.
- Reconstruct equations from surrounding prose and equation numbers when the PDF text extraction yields garbled symbols.

### Diagrams, Charts, Figures, Images

**For EVERY diagram/chart/figure in the PDF:**

1. **Provide a detailed, comprehensive verbal description** placed inline where the diagram appears in the original
2. **Use this format:**

```
   [DIAGRAM DESCRIPTION: <exact title from PDF>]

   <detailed prose description including:>
   - What type of diagram it is (flowchart, map, graph, schematic, etc.)
   - All labeled elements and components
   - Spatial relationships and connections between elements
   - For charts: axes labels, units, scale, data ranges, trend lines
   - For maps: geographic regions shown, color/pattern meanings, scale
   - Color coding, shading patterns, line styles if relevant to interpretation
   - What insight, relationship, or information the diagram conveys
   - Any legends or keys explained inline

   [→ See original PDF page X for visual rendering]
```

3. **Real example:**

```
   [DIAGRAM DESCRIPTION: Continental US Power Flow Coloring Scheme]

   A geographic map showing North America with transmission corridors overlaid.
   Transmission lines are color-coded by primary power flow direction: red
   indicates southward flow, blue indicates northward flow. Line thickness
   represents transmission capacity, with thicker lines indicating higher capacity.
   The diagram reveals regional patterns: Great Plains shows primarily north-to-south
   flows from wind generation, the Northeast shows complex multi-directional flows
   reflecting its interconnected grid, and the Southwest shows strong south-to-north
   flows. This visualization demonstrates how transmission constraints vary regionally
   and which directions are capacity-constrained.

   [→ See original PDF page 42 for visual rendering]
```

4. **For screenshots/UI mockups**: Describe layout, button locations, form fields, displayed data, visual hierarchy

5. **For flowcharts/process diagrams**: Describe flow path, decision points, inputs/outputs, process steps in order

### Citations & References

- Preserve citation format exactly: `(Author et al., Year)` or `[1]`, `[2]`, etc.
- Keep full reference entries at end of document in "References" section
- Maintain all URLs, DOIs, publication details
- If citation appears multiple times, keep it consistent throughout

### Footnotes & Endnotes

- **Locate and preserve every footnote** in the source. Do not drop footnote text, definitions, or URLs. Output each footnote either as a markdown footnote (`[^n]` in body with `[^n]: content` in a **Footnotes** section) or inlined at the reference point; in either case, the full content (including any URLs, citations, or definitions) must appear in the extract.
- Preserve ALL footnote content—nothing drops. If a footnote reference triggers a linter (e.g. "unused reference definition"), you may **inline the footnote content** into the body at the reference point and remove the footnote definition, provided no content is lost.
- Keep numbering/order from original (or renumber from 1 if the source uses different numbering).

### Content You Cannot Fully Extract

**Logos/branding graphics**: Skip unless essential

**Decorative photographs**: Skip

**Letterhead/covers**: Extract text only

**Hand-drawn annotations**: Describe if meaningful

**Color-critical diagrams**: Provide description, note to see PDF for precise colors

**High-precision technical drawings**: Describe relationships and elements, note PDF shows exact proportions

### Output Structure

```
# [Document Title]

**Source**: [Original filename]
**Pages**: [X total pages]
**Date**: [Publication/modification date if present]
**Author(s)**: [Names; if the PDF has superscript numbers, list names with affiliations below]
**Author affiliations**: Extract from the first page: correspondence author (e.g. "Author for correspondence", email), and each numbered affiliation (institution, address). Omit only if the PDF has no affiliation block.

---

[Document content here, preserving original hierarchy]

---

## References

[Complete reference list]
```

When the source PDF contains them, include short back-matter sections such as **Acknowledgments**, **Funding**, **Author Contributions**, **Data Statement**, **Conflicts of interest**, or similar—preserve their headings and full text.

### Quality Checklist

- [ ] All heading levels preserved (H1, H2, H3, etc.)
- [ ] Text formatting (bold, italics, code, links) matches original emphasis
- [ ] **ALL tables extracted to markdown table format**
- [ ] **Every figure, diagram, and chart has detailed verbal description**
- [ ] Equations preserved in original notation
- [ ] All citations and references complete and linked
- [ ] All footnotes and endnotes preserved
- [ ] No orphaned content (mentioned but not included)
- [ ] Section numbers, titles, labels preserved exactly
- [ ] Document reads as complete, standalone resource
- [ ] "See PDF" markers only for visual elements (colors, layouts, photos)
- [ ] Markdown syntax valid and renders cleanly
- [ ] List nesting correct

## When to Say "See PDF"

**DO reference for:**

- Color differentiation that's semantically important
- Precise visual layouts requiring exact positioning
- Photographic content (describe what's shown; see PDF for image)
- Hand-drawn elements
- Multi-layer overlapping visuals

**DO NOT reference for:**

- Any textual information (describe instead)
- Table content (extract it)
- Equation meaning (transcribe it)
- Document structure (recreate it)
- Citations (extract them)
- Text emphasis (apply markdown)
- Conceptual diagrams (describe in prose)

## Output filename and location

- **Filename**: The markdown file **must** use the same base name as the PDF, with a `.md` extension. Example: `bill_alignment_test.pdf` → `bill_alignment_test.md`.
- **Location**: Save under `context/docs/` for technical documentation (e.g. Cambium, ResStock) or `context/papers/` for academic papers (e.g. Bill Alignment Test). Update `context/README.md` when adding or changing files.

## Process

The PDF file path is provided as: **$ARGUMENTS**

1. Examine PDF structure first (TOC, sections, page count). From the first page, extract author affiliations and correspondence if present. If the document has a TOC, note the section numbers and page mapping so the extract’s heading numbers match (e.g. last numbered section = last TOC entry).
2. Preserve structure exactly in markdown (include **Author affiliations** in metadata when available). Verify section numbers against the TOC before finalizing.
3. Extract section by section, applying all rules (inline math in prose, markdown lists for inline-numbered lists, equation labels).
4. For visual elements: stop and write detailed description
5. For complex tables: show prose description + markdown version
6. Build complete References section
7. Do final quality check against checklist
8. Output complete markdown as ready-to-use document
9. **Save the file** under `context/docs/` or `context/papers/` using the **same base name as the PDF** (e.g. `path/to/foo.pdf` → `context/.../foo.md`). Update `context/README.md` if needed.

**Provide the extracted markdown in full, ready to save to context/ with the matching filename and commit.**
