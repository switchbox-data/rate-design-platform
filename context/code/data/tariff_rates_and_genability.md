# Tariff rates and the Genability/Arcadia data model

Reference for working with electric tariffs from the Arcadia/Genability API, including the
data model, how `tariff_fetch` converts them to URDB, known library bugs and their patches,
and how to reconstruct historical monthly charges.

---

## Terminology

| Term               | Meaning                                                                                                                                                                                                                                                     |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Tariff**         | The filed rate schedule for a utility (e.g. BGE Residential). Identified by `masterTariffId`. May have multiple versions over time, each with a different `tariffId` and `effectiveDate`.                                                                   |
| **Rate**           | A single line-item charge within a tariff (e.g. "Delivery Service Customer Charge"). Has a `chargeType` (FIXED_PRICE, CONSUMPTION_BASED, DEMAND, QUANTITY), a `chargePeriod` (MONTHLY, etc.), and one or more `rateBands`.                                  |
| **Rate band**      | A tier within a rate. Most residential rates have one band. Has `rateAmount`, `rateUnit`, and optional properties like `calculationFactor`, `applicabilityValue`, `hasConsumptionLimit`, etc.                                                               |
| **Rider**          | A supplemental tariff that "rides" on top of a base tariff, filed separately (e.g. BGE's "Energy Cost Adjustment" rider). In Arcadia, base tariff rates reference riders via `riderTariffId`. At runtime the library fetches and inlines the rider's rates. |
| **masterTariffId** | Stable identifier for a tariff across all versions. Use this to look up a tariff.                                                                                                                                                                           |
| **tariffId**       | Identifier for a specific effective version of a tariff. Changes when rates are updated.                                                                                                                                                                    |

---

## Arcadia rate band fields relevant to URDB conversion

### `rateUnit`

Two possible values:

- `COST_PER_UNIT` — the rate amount is a flat dollar figure per unit ($/month for FIXED_PRICE,
  $/kWh for CONSUMPTION_BASED). This is the standard case.
- `PERCENTAGE` — the rate amount is a percentage of some implicit base (e.g. percentage of the
  total bill). The `tariff_fetch` URDB converter does not support this unit for FIXED_PRICE or
  CONSUMPTION_BASED charge types. For `QUANTITY` charge type it is silently skipped.

### `calculationFactor`

A decimal multiplier to be applied to `rateAmount`. The effective charge is
`rateAmount × calculationFactor`. Arcadia uses this to model percentage-based regulatory
surcharges where:

- `rateAmount` = the base dollar amount being taxed
- `calculationFactor` = the surcharge rate as a decimal (e.g. `0.020408` for 2.04%)
- Effective charge = `rateAmount × calculationFactor`

Examples in MD:

- Pepco "Customer Charge Component - Gross Receipts Tax": `rateAmount=8.44`,
  `calculationFactor=0.020408` → `$0.172/month` DC GRT on the customer charge
- DPL "Customer Charge - Component": `rateAmount=13.50`, `calculationFactor=0.0423` →
  `$0.571/month` Delaware DSIC on the customer charge

### `applicabilityValue`

When set, a band only applies if the scenario property named by `applicabilityKey` matches this
value. Used for opt-in charges (e.g. smart meter opt-out fee, competitive billing surcharge).
`rate_filter_bands` uses `continue` to skip non-matching bands. If all bands in a rate are
skipped, the function returns an empty list `[]`.

---

## `tariff_fetch` library bugs and patches

`fetch_electric_tariffs_genability.py` monkey-patches the `tariff_fetch` library to fix two
bugs that surface with MD utility tariffs. The patches follow the same pattern as the existing
patches already in that file (xdrlib stub, 403 rider cache, non-interactive property resolver).

### Bug 1 — Empty band list from BOOLEAN applicability filtering (affects BGE)

**Root cause:** BGE's "Competitive Billing" and "Smart Meter Opt-Out Charge" rates are
`FIXED_PRICE` with a single band each carrying `applicabilityValue=true`. These charges only
apply to customers on competitive/third-party supply or who have opted out of the smart meter
program. The non-interactive property resolver sets these BOOLEAN properties to `False` (correct
default for the vast majority of residential customers).

`rate_filter_bands` in `rateutils.py` skips non-matching applicability bands via `continue`,
returning an empty list `[]`. The downstream `get_rate_fixed_charge_at_dt` in `fixedcharge.py`
then does:

```python
bands = ru.rate_filter_bands(rate, scenario, library)
band_rate_units = {band["rate_unit"] for band in bands}  # empty set when bands=[]
if rate["charge_type"] != "FIXED_PRICE":
    return 0
if "COST_PER_UNIT" not in band_rate_units:               # always True for empty set
    raise RateConversionError(rate, "Fixed price rate bands units should be COST_PER_UNIT")
```

The error message ("Fixed price rate bands units should be COST_PER_UNIT") is misleading — the
real problem is that all bands were filtered out, not that the unit type is wrong.

**Fix:** Patch `get_rate_fixed_charge_at_dt` to return `0.0` when `rate_filter_bands` returns
an empty list. An empty list means the rate does not apply to this customer scenario — its
contribution to the fixed charge is zero.

```python
def _get_rate_fixed_charge_at_dt_empty_bands_safe(scenario, library, rate, dt):
    bands = _ru_mod.rate_filter_bands(rate, scenario, library)
    if not bands:
        return 0.0
    return _original_get_rate_fixed_charge_at_dt(scenario, library, rate, dt)
```

**Correctness:** Customers on the standard BGE residential service (non-competitive, no opt-out)
genuinely do not pay these charges. Returning 0 is correct for CAIRO's representative customer.

### Bug 2 — `calculation_factor` raises unconditionally (affects Pepco and DPL)

**Root cause:** The `rate_filter_bands` function in `rateutils.py` raises `RateConversionError`
for any band with a `calculation_factor` field, without attempting to handle it:

```python
if band.get("calculation_factor"):
    raise RateConversionError(rate, "Bands with property calculation_factor are not supported")
```

This causes the entire URDB conversion to abort for Pepco and DPL, which have DC Gross Receipts
Tax and Delaware Distribution System Improvement Charge rates modeled with `calculationFactor`.

**Fix:** Patch `rate_filter_bands` to fold the factor into `rateAmount` before the band is
processed:

```python
def _rate_filter_bands_fold_calc_factor(rate, scenario, library):
    bands = rate.get("rate_bands") or []
    has_cf = any(b.get("calculation_factor") is not None for b in bands)
    if not has_cf:
        return _original_rate_filter_bands(rate, scenario, library)
    folded_bands = []
    for band in bands:
        cf = band.get("calculation_factor")
        if cf is not None:
            base = band.get("rate_amount") or 0.0
            band = {**band, "rate_amount": base * cf, "calculation_factor": None}
        folded_bands.append(band)
    modified_rate: TariffRateExtended = {**rate, "rate_bands": folded_bands}
    return _original_rate_filter_bands(modified_rate, scenario, library)
```

**Correctness:** The Arcadia API docs define `calculationFactor` as "a factor to be applied to
the cost of the rate." Folding it into `rateAmount` produces a flat-rate band at the effective
dollar/kWh amount, which is what the customer actually pays. The resulting URDB tariff
correctly includes GRT and DSIC charges.

**Edge case:** Pepco's "Transmission Charge Component - Gross Receipts Tax" has a
`variableRateKey`, so `rate_band_get_amount_at_datetime` uses a live variable lookup rather
than `rateAmount`. The fold sets `rateAmount = 0.0 × 0.020408 = 0.0`, which is then ignored
by the variable lookup. This means the GRT on the variable transmission charge is not applied.
The understatement is minor (2% of a small per-kWh variable rate) and is the inherent
limitation of the URDB format, which cannot express multiplicative variable adjustments.

### Why NY utilities did not trigger these bugs

NY utilities have similar data patterns but hit different code paths:

- **ConEd, PSEG-LI:** Have `PERCENTAGE` rateUnit bands, but on `QUANTITY` charge-type rates.
  `tariff_fetch` only processes `FIXED_PRICE` and `CONSUMPTION_BASED` rates for URDB
  conversion. `QUANTITY` rates are skipped, so the PERCENTAGE check never fires.
- **NiMo:** Has `calculationFactor` bands, but on `chargeClass=SUPPLY,CONTRACTED` rates.
  When building the delivery-only URDB, supply-class rates are filtered out by
  `rate_is_applied_to_charge_classes` before `rate_filter_bands` is ever called.
- MD utilities (Pepco, DPL): GRT and DSIC bands are `chargeClass=DISTRIBUTION` and
  `chargeType=FIXED_PRICE` or `CONSUMPTION_BASED`. They are not filtered by charge class,
  so they reach `rate_filter_bands` and trigger the bug.

---

## Arcadia tariff versioning and date-boundary behavior

A `masterTariffId` may have multiple versions during a year (different `tariffId` values with
different `effectiveDate` / `endDate`). BGE had three versions in 2025:

- Jan 1 – Mar 1: `tariffId=3491377`
- Mar 1 – Dec 31: `tariffId=3517176`

When `get_fixed_charge_value` iterates over the year with 12-hour intervals, `get_tariff_at_date`
fetches the appropriate version for each date via the Arcadia search endpoint
(`effective_on=dt`). This means both tariff versions are used during a single annual URDB
conversion, and a rate change mid-year will affect the averaged fixed charge output.

The direct GET endpoint (`tariffs/{tariffId}`) used by our `_fetch_tariff_direct` patch is
only for rider tariff lookups by ID, not for version-by-date lookups. Rider tariff IDs are
stable across tariff versions and do not require date-based resolution.

---

## Reconstructing historical monthly charges

To fetch actual monthly rate amounts (for the revenue-requirement top-up pipeline) rather than
URDB tariff structure, use `fetch_monthly_rates.py` (see `tariff_generation_pipeline.md`).
The monthly rates pipeline queries the Arcadia Calculate API with representative usage amounts
for each month and extracts the per-component dollar values. This is how variable rates
(like BGE's energy cost adjustment) are resolved to actual monthly $/kWh values.
