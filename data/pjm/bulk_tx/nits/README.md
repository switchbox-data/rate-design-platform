# PJM NITS Rates Reference Data

Network Integration Transmission Service (NITS) rates for MD-relevant transmission zones (BGE, DPL, PEPCO, APS). Used as the bulk transmission marginal cost input for Maryland BAT runs.

## Files

| File                     | Purpose                                                  |
| ------------------------ | -------------------------------------------------------- |
| `nits_rates.csv`         | Generated CSV: one row per (year, effective_date, zone)  |
| `sources/nits_{year}.md` | Per-year markdown intermediates (citation + rate tables) |
| `fetch_nits_pdfs.py`     | Download NITS rate PDFs from PJM website                 |
| `validate_nits_rates.py` | Schema and value validation                              |
| `Justfile`               | `fetch` and `validate` recipes                           |
| `_local_pdfs/`           | Local PDF cache (gitignored)                             |

## Workflow

### Fetch PDFs (on-demand)

```bash
just fetch YEARS="2025"           # single year
just fetch YEARS="2021-2025"      # year range
just fetch YEARS="2021 2022 2025" # specific years
```

PDFs are saved to `_local_pdfs/` (gitignored) for extraction or verification.

### Manual transcription (current approach)

1. Download PDFs (or view online)
2. Transcribe rates to markdown intermediate: `sources/nits_{year}.md`
3. Update `nits_rates.csv` with new rows
4. Run `just validate` to verify

This approach is preferred because:

- PJM PDF layouts vary across years
- Some years are no longer accessible (2018-2020 return 404)
- Manual transcription is verifiable and catches extraction errors
- Similar to the RPM capacity prices workflow

### Validation

```bash
just validate
```

Checks:

- Required columns and dtypes
- All 4 MD zones present per year
- Each year has exactly 2 effective dates (Jan and Jun)
- NITS rates are positive and `$/kW-yr = $/MW-yr / 1000`
- Source URLs are non-empty

## Data coverage

**Current:** 2021-2025 (40 rows: 4 zones × 2 periods × 5 years)

**Source quality:**

- **2021-2023**: High confidence, extracted directly from PJM PDFs
- **2024-2025**: High confidence, Jan from CAPS Handbook, Jun from PJM PDFs / ETCC table

**Historical (2018-2020):** PJM PDFs no longer accessible. Use ETCC historical table + inference if needed for pre-2021 BAT runs.

## Rate update patterns

- **APS**: Updates in January, rate constant through June
- **BGE, DPL**: Update in June, January rate = previous June carryforward
- **PEPCO**: Updates at both January (NSPL reset) and June (new ATRR)

## Calendar-year blending

PJM bills NITS daily per Manual 27 §5.2.2. For calendar-year BAT runs:

```
blended_rate = (151 × jan_rate + 214 × jun_rate) / 365  # non-leap
blended_rate = (152 × jan_rate + 214 × jun_rate) / 366  # leap
```

## Storage

NITS rates are **in-repo reference data** (like PJM RPM capacity prices), not on S3. Rationale:

- Small dataset (< 10 KB)
- Should be versioned in git for reproducibility
- Changes infrequently (2x per year per zone)
- Human-verifiable markdown source of truth

Compare:

- **In-repo**: Small reference data (utility codes, zone mappings, capacity prices, NITS rates)
- **S3**: Large datasets (ResStock loads, LMP time series, CAIRO outputs)

## Adding a new year

1. Fetch PDFs: `just fetch YEARS="2026"`
2. Create `sources/nits_2026.md` from the PDFs (transcribe Jan and Jun tables)
3. Add 8 rows to `nits_rates.csv` (4 zones × 2 periods)
4. Run `just validate` to verify
5. Commit all files (markdown source, updated CSV)

## References

- PJM NITS rate postings: https://www.pjm.com/markets-and-operations/billing-settlements-and-credit/formula-rates
- CAPS Transmission Handbook: https://dgardiner.com/ (published twice yearly)
- ETCC historical rates: http://electricitytransmissioncompetitioncoalition.org/
- Methodology: `context/methods/marginal_costs/md_bulk_transmission.md`
