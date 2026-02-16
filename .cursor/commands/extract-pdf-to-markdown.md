---
description: Extract PDF to markdown preserving structure, formatting, tables, equations, and visual descriptions
argument-hint: <path-to-pdf-file>
---

# PDF to Markdown Extraction

You are extracting a technical PDF into a **standalone, fully-formatted markdown document** that preserves as much information as possible while remaining readable and useful without the original PDF.

## Core Principles

1. **Fidelity over brevity**: Preserve every meaningful piece of information
2. **Structure preservation**: Keep original document hierarchy, sections, subsections
3. **Formatting in markdown**: Use markdown formatting (bold, italics, code blocks, etc.) to mirror original emphasis
4. **Impossible content handling**: For diagrams, charts, images—provide detailed textual descriptions inline
5. **Standalone design**: A reader should be able to work from this markdown alone; the PDF is emergency reference only
6. **LLM-friendly markers**: Use clear, parseable markers when you must indicate "see PDF for visual"

## Extraction Instructions

### Structure & Hierarchy

- Preserve all heading levels (H1→H2→H3, etc.)
- Maintain table of contents structure if present
- Keep section numbers, subsection labels, and organizational schemes from original
- Preserve logical document sections with `---` between them if they marked distinct parts

### Text & Formatting

- **Bold** for emphasis (use `**text**`)
- _Italics_ for terms, citations, variable names (use `*text*`)
- `code` for technical terms, variable names, file paths, code snippets
- Preserve numbered and bulleted lists with correct nesting
- Keep paragraph structure and grouping exactly as in original
- Convert hyperlinks to markdown format: `[link text](URL)`

### Tables

- Convert **ALL** tables to markdown table format
- Preserve column headers and cell content exactly
- If a table is complex/wide, add a description before it explaining its structure
- Complex tables that don't render cleanly: provide both a prose description and the markdown version
- Keep alignment indicators if relevant (left, center, right aligned)

### Equations & Mathematical Content

**For inline formulas**: Use backticks or notation: `E = mc²`

**For block equations**: Use code blocks with math notation

**Handling strategy:**

- Keep equations in original notation (LaTeX, plain text, mathematical symbols)
- If equation is explained in surrounding text, include that text
- For critical visual elements: `[EQUATION: describe what it shows]` before/after
- Example: For the Lorentz force: `[EQUATION: Force on a charged particle in electromagnetic field]` followed by `F = q(E + v × B)`

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

- Convert footnotes to markdown: `[^1]` with `[^1]: content` at bottom
- Or integrate as inline text: `(Note: ...)`
- Preserve ALL footnote content—nothing drops
- Keep numbering/order from original

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
**Author(s)**: [If applicable]

---

[Document content here, preserving original hierarchy]

---

## References

[Complete reference list]
```

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

1. Examine PDF structure first (TOC, sections, page count)
2. Preserve structure exactly in markdown
3. Extract section by section, applying all rules
4. For visual elements: stop and write detailed description
5. For complex tables: show prose description + markdown version
6. Build complete References section
7. Do final quality check against checklist
8. Output complete markdown as ready-to-use document
9. **Save the file** under `context/docs/` or `context/papers/` using the **same base name as the PDF** (e.g. `path/to/foo.pdf` → `context/.../foo.md`). Update `context/README.md` if needed.

**Provide the extracted markdown in full, ready to save to context/ with the matching filename and commit.**
