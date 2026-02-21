# Gas tariff fetch (Rate Acuity) – checklist for browser environment

Use this when running the fetch on an environment that has a browser (and Rate Acuity credentials). The fetch script was not run in CI/dev without a browser.

**Credentials:** Add `RATEACUITY_USERNAME` and `RATEACUITY_PASSWORD` to `.env` (see `.env.example`). The fetch script loads `.env` from the project root automatically.

## 1. Get actual Rate Acuity dropdown names

The crosswalk in `utils/utility_codes.py` (`rate_acuity_utility_names` per utility) was copied from the old fetch script, not verified against the live Rate Acuity UI.

**Do this first:**

```bash
# From repo root, with RATEACUITY_USERNAME and RATEACUITY_PASSWORD set (e.g. in .env)
uv run python utils/pre/fetch_gas_tariffs_rateacuity.py --state NY --list-utilities
uv run python utils/pre/fetch_gas_tariffs_rateacuity.py --state RI --list-utilities
```

- Capture the printed lists (exact strings as shown in the gas history dropdown).
- In `utils/utility_codes.py`, for each gas utility that we fetch (NY: coned, cenhud, kedny, kedli, nimo, nfg, nyseg, or, rge; RI: rie), set `rate_acuity_utility_names` to the **exact** name(s) that appear in that list (or add candidates so at least one matches). Update any mismatches.

## 2. Run a single-utility fetch (smoke test)

Pick one utility and run the fetch; it writes one JSON per tariff_key (e.g. `coned_sf.json`, `coned_mf_lowrise.json`) into `--output-dir`:

```bash
mkdir -p /tmp/gas_out
uv run python utils/pre/fetch_gas_tariffs_rateacuity.py --state NY --utility coned --output-dir /tmp/gas_out
```

- Confirm no interactive prompts and no errors.
- Confirm expected tariff_key files exist (e.g. for ConEd: `coned_sf.json`, `coned_mf_lowrise.json`, `coned_mf_highrise.json` if those schedules were fetched). Rates that do not match a mapping in `rateacuity_tariff_to_gas_tariff_key.py` are skipped.

## 3. Convert existing staging (optional)

If you have an existing directory of `rateacuity_*.json` or `*.urdb.*.json` (e.g. from an old run or tariff_fetch CLI), you can convert it to tariff_key files without re-fetching:

```bash
uv run python utils/pre/install_ny_gas_tariffs_from_staging.py /tmp/gas_staging --state NY --output-dir /tmp/gas_out
```

## 4. RI smoke test (optional)

If RI gas is in scope:

```bash
uv run python utils/pre/fetch_gas_tariffs_rateacuity.py --state RI --utility rie --output-dir /tmp/gas_out_ri
```

- Confirm `rie_heating.json` and `rie_nonheating.json` (or whatever the RI mapping produces). Adjust `utils/pre/rateacuity_tariff_to_gas_tariff_key.py` or `rate_acuity_utility_names` for rie if needed.

## 5. Justfile flow (NY)

From `rate_design/ny/hp_rates/`:

- `just fetch-gas-coned` (or another utility) → writes tariff_key JSON files (e.g. `coned_sf.json`, `coned_mf_lowrise.json`) directly into the configured gas dir (`config/tariffs/gas/`; see `Justfile.tasks`). No separate extract step needed for a fresh fetch.
- To convert an existing staging dir (e.g. old `rateacuity_*.json` files) to tariff_key files, use `just extract-gas-tariffs-ny <staging_path> <output_path>`.

## 6. Things to fix if they break

| Symptom                                   | Likely fix                                                                                                                                                             |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "Utility X not found for state Y"         | Update `rate_acuity_utility_names` for that utility in `utils/utility_codes.py` to match `--list-utilities` output.                                                    |
| Fetch writes no (or few) tariff_key files | Rate names from Rate Acuity don't match the patterns in `rateacuity_tariff_to_gas_tariff_key.py`. Add or relax (utility substring, rate name regex) → tariff_key rows. |
| Browser / login errors                    | Ensure RATEACUITY_USERNAME and RATEACUITY_PASSWORD are set; run on a machine where the tariff-fetch browser flow can run.                                              |

## 7. After verification

- If you had to change `rate_acuity_utility_names` or the mapping tables, commit those updates.
- You can delete this checklist file or move it elsewhere once the process is stable.
