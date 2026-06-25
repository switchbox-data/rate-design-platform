# MD Tariff Fetch Plan

Working document for fetching default electric and gas tariffs for all Maryland utilities.
See `utils/pre/rev_requirement/fetch_electric_tariffs_genability.py` and
`utils/pre/fetch_gas_tariffs_rateacuity.py` for the fetch scripts.

---

## MD utilities in scope

### Electric (10 utilities — from HIFLD + utility_codes.py)

| std_name          | Display name             | Type                 | EIA ID | Arcadia availability |
| ----------------- | ------------------------ | -------------------- | ------ | -------------------- |
| `bge`             | Baltimore Gas & Electric | IOU                  | 1167   | Confirmed            |
| `pepco`           | Pepco                    | IOU                  | 15270  | Confirmed            |
| `poted`           | Potomac Edison           | IOU                  | 15263  | Confirmed            |
| `dpl`             | Delmarva Power           | IOU                  | 5027   | Confirmed            |
| `smeco`           | SMECO                    | Co-op                | 17637  | Likely yes           |
| `choptank`        | Choptank Electric        | Co-op                | 3503   | Likely yes           |
| `somerset_rec`    | Somerset REC             | Small co-op          | 84     | May not exist        |
| `berlin_muni`     | Town of Berlin           | Municipal            | 1615   | May not exist        |
| `hagerstown_muni` | Hagerstown Light Dept    | Municipal            | 7908   | May not exist        |
| `easton_muni`     | Easton Utilities         | Municipal (elec+gas) | 5625   | May not exist        |

### Gas (8 utilities — from HIFLD + utility_codes.py)

| std_name               | Display name             | Type         | Notes                   |
| ---------------------- | ------------------------ | ------------ | ----------------------- |
| `bge`                  | Baltimore Gas & Electric | IOU          | Largest MD gas LDC      |
| `washington_gas`       | Washington Gas           | IOU          | D.C. metro area         |
| `columbia_gas_md`      | Columbia Gas of Maryland | IOU          | NiSource subsidiary     |
| `chesapeake_utilities` | Chesapeake Utilities     | Mid-size LDC | Eastern MD / Shore      |
| `easton_muni`          | Easton Utilities         | Municipal    | Small muni              |
| `sandpiper`            | Sand-Piper Energy        | Small LDC    | Lower Shore             |
| `elkton_gas`           | Elkton Gas               | Small LDC    | Cecil County            |
| `ugi_central_penn`     | UGI Central Penn Gas     | LDC          | Smaller footprint in MD |

---

## Phase 1: Electric default tariffs (snapshot + URDB)

**Source:** Arcadia/Genability API (`ARCADIA_APP_ID`, `ARCADIA_APP_KEY`)

**Output files per utility (two URDB JSONs + one snapshot JSON):**

- `config/tariffs/electric/{std_name}_default.json` — delivery-only URDB (DISTRIBUTION + TRANSMISSION + OTHER)
- `config/tariffs/electric/{std_name}_default_supply.json` — delivery + supply URDB (adds SUPPLY + CONTRACTED)
- `config/rev_requirement/top-ups/default_tariffs/{std_name}_default_2025-01-01.json` — raw Genability snapshot (not URDB; reference for revenue-requirement pipeline)

### Step 1.1 — Config (done)

`config/rev_requirement/top-ups/tariffs_by_utility.yaml` created with all 10 utilities.

### Step 1.2 — Run fetch

```bash
# From rate_design/hp_rates/
just -f md/Justfile fetch-default-electric-tariffs
```

This runs `fetch_electric_tariffs_genability.py` with `--urdb`, writing:

- Snapshot → `top-ups/default_tariffs/`
- URDB delivery + supply → `config/tariffs/electric/`

### Step 1.3 — Handle small-utility failures

Small co-ops and municipals (`somerset_rec`, `berlin_muni`, `hagerstown_muni`, `easton_muni`) may not
exist in Arcadia. If the fetch raises for them:

1. Remove them from `tariffs_by_utility.yaml` temporarily
2. Search Arcadia interactively: `tariff-fetch --state md --provider genability`
3. If found by name substring or masterTariffId, update the YAML entry
4. If not in Arcadia, use OpenEI as fallback: `tariff-fetch ni openei <eia_id> residential`
5. If not available anywhere, these buildings will need a `null_electric_tariff.json`

### Step 1.4 — Inspect outputs

Check each `*_default.json` has `energyratestructure` populated. Check `*_default_supply.json`
has both delivery and supply charges. Spot-check BGE's filed rate structure against PSC filings.

---

## Phase 2: Gas default tariffs (URDB from RateAcuity)

**Source:** RateAcuity web scrape (`RATEACUITY_USERNAME`, `RATEACUITY_PASSWORD`)

**Output files per utility/schedule:**

- `config/tariffs/gas/{tariff_key}.json` — URDB gas tariff

### Step 2.1 — Discover RateAcuity utility names

List all MD gas utilities in the RateAcuity dropdown. You need a minimal placeholder
`rateacuity_tariffs.yaml` (just `state: MD`) to use `--list-utilities`:

```bash
# Create minimal placeholder
echo "state: MD" > md/config/tariffs/gas/rateacuity_tariffs.yaml

# From rate_design/hp_rates/
uv run python ../../utils/pre/fetch_gas_tariffs_rateacuity.py \
  md/config/tariffs/gas/rateacuity_tariffs.yaml /tmp/unused --list-utilities
```

This prints the exact dropdown strings. Map each one to the correct `std_name`.

### Step 2.2 — Add `rate_acuity_utility_names` to utility_codes.py

For each MD gas utility discovered in the dropdown, add:

```python
{
    "std_name": "bge",
    ...
    "gas_tariff_key": "bge",
    "rate_acuity_utility_names": ["<exact dropdown string>"],
},
```

### Step 2.3 — Discover schedule names per utility

For each utility, run the script interactively to see available schedules:

```bash
tariff-fetch gas --state md
```

or inspect via the web portal. Record the **exact** schedule name for:

- Residential non-heating
- Residential heating (if separate)

### Step 2.4 — Create `rateacuity_tariffs.yaml`

```yaml
state: MD
bge:
  bge_nonheating: "<exact RateAcuity schedule name>"
  bge_heating: "<exact RateAcuity schedule name>"
washington_gas:
  washington_gas_nonheating: "<exact schedule name>"
  # etc.
```

### Step 2.5 — Run fetch

```bash
# From rate_design/hp_rates/
just -f md/Justfile fetch-gas-tariffs
```

### Step 2.6 — Handle small-utility failures

`sandpiper`, `elkton_gas`, `columbia_gas_md`, `ugi_central_penn` may not be on RateAcuity.
For buildings assigned these utilities, use `null_gas_tariff.json`:

```bash
cp ri/config/tariffs/gas/null_gas_tariff.json md/config/tariffs/gas/null_gas_tariff.json
```

---

## Phase 3: Validation

### Electric

Check each URDB JSON manually:

- `energyratestructure` — volumetric energy tiers present
- `fixedchargefirstmeter` — fixed charge in `$/month`
- `demandweekdayschedule` / `demandweekendschedule` — present for demand-tariff utilities
- `utility` / `name` — metadata populated

Automated: run `just -f md/Justfile validate-config` once scenarios exist (requires
`scenarios/`, `periods/`, and MC data — deferred to the full pre-run setup phase).

### Gas

Check each URDB JSON:

- `energyratestructure` — monthly gas rates present
- `fixedchargefirstmeter` — customer charge in `$/month`
- Spot-check BGE's filed residential gas rate against PSC records

---

## Remaining work (not in scope here)

After tariffs are fetched, the following remain before CAIRO runs:

1. Research and classify BGE/Pepco/etc. charges → `charge_decisions/` JSONs (the revenue-requirement pipeline)
2. Fetch monthly rates → `monthly_rates/` YAMLs
3. Add rate-case delivery revenue requirements
4. Run `compute-rr` → `rev_requirement/bge.yaml` etc.
5. Add `periods/bge.yaml` (winter months, TOU window, elasticity)
6. Add MD row to Runs & Charts Google Sheet → run `create-scenario-yamls`
7. Run `all-pre` to generate tariff maps and derived HP-rate tariff variants
8. Run ResStock utility assignment for MD (already implemented in `assign_utility_md.py`)
