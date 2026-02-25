# Gas kWh→therms double-conversion in CAIRO

**Date:** 2026-02-23
**Reported by:** Switchbox
**CAIRO version:** (please fill in)

---

## Summary

When a caller pre-loads gas data and passes it into `simulate()` as
`customer_gas_load`, CAIRO's internal `_adjust_gas_loads` function applies
the kWh→therms conversion factor a second time on data that is already in
therms. The result is gas bills that are approximately **29× smaller than
the correct value** (factor = 0.0341214116, so 1 / 0.0341… ≈ 29.3×).

---

## How it happens

### The intended flow

CAIRO normally reads gas loads from parquet files inside `_return_load()`,
which returns data in kWh. `_adjust_gas_loads` then converts to therms
before billing. This is correct.

### The broken flow (pre-loaded data)

When a caller passes pre-processed gas data directly into `simulate()` via
the `customer_gas_load` argument, the data is already in therms. Inside
`_calculate_gas_bills`, CAIRO detects the pre-loaded DataFrame (via the
`"load_data"` column check) and skips the `_return_load()` call. However,
it then passes the pre-loaded DataFrame straight into
`_return_preaggregated_load`, which unconditionally calls
`process_building_demand_by_period`, which calls `aggregate_load_worker` per
building (via Dask), which calls `_load_worker`, which calls
`_adjust_gas_loads` — applying the kWh→therms factor a second time on data
that is already in therms.

### Call chain

```
_calculate_gas_bills(raw_load=<already therms>)
  │
  ├─ detects "load_data" column → skips _return_load()  ✓
  │
  └─ _return_preaggregated_load(raw_load)
       └─ load_funcs.process_building_demand_by_period(prepassed_load=raw_load)
            └─ aggregate_load_worker(...)     [Dask, once per building]
                 └─ _load_worker(...)
                      └─ _adjust_gas_loads(df)   ← applies × 0.0341214116 again  ✗
```

### Relevant code in `_calculate_gas_bills`

```python
if "load_data" in raw_load.columns:
    raw_load_gas = raw_load          # already processed — skips _return_load ✓
else:
    raw_load_gas = load_funcs._return_load(load_type="gas", ...)

# ... but then:
agg_load, agg_solar = _return_preaggregated_load(
    raw_load=raw_load_gas,           # passed here — still gets double-converted ✗
    ...
)
```

---

## Magnitude

| Value                                                     | Result                       |
| --------------------------------------------------------- | ---------------------------- |
| Conversion factor applied twice                           | 0.0341214116² ≈ 0.00116      |
| Under-billing factor                                      | 1 / 0.0341214116 ≈ **29.3×** |
| Typical RI residential annual gas bill (correct)          | ~$500–$1,500                 |
| Typical RI residential annual gas bill (double-converted) | ~$17–$51                     |

We confirmed this in production data: before the fix, RI residential annual
gas bills were in the $17–$51 range; after, $500–$1,500.

---

## Conditions that trigger the bug

The bug is triggered when **all three** of the following are true:

1. The caller passes `customer_gas_load` to `simulate()` (i.e., pre-loaded gas data).
2. The pre-loaded DataFrame has a `"load_data"` column (which causes the
   `"load_data" in raw_load.columns` branch to be taken in `_calculate_gas_bills`).
3. The data in `"load_data"` is already in therms (not kWh).

Callers who do not pass `customer_gas_load`, or who pass raw kWh data, are
not affected.

---

## Our workaround

We monkey-patch `_calculate_gas_bills` on `MeetRevenueSufficiencySystemWide`
to bypass `_return_preaggregated_load` entirely and call our own vectorized
aggregation path, which does not apply `_adjust_gas_loads` to pre-loaded data.
This is working correctly in production for our RI runs but is fragile — it
relies on CAIRO internals staying stable.

---

## Suggested fix

The cleanest fix is a guard in `_load_worker` (or in `aggregate_load_worker`)
that skips `_adjust_gas_loads` when the data has already been converted. A
boolean flag on the call path would be sufficient:

```python
# Option A: flag on _return_preaggregated_load
def _return_preaggregated_load(raw_load, ..., gas_already_therms=False):
    ...
    # pass flag down to aggregate_load_worker / _load_worker

# Option B: detect units from a metadata column on raw_load
# (e.g., add a "units" column or attribute)

# Option C: expose process_building_demand_by_period with a skip_gas_conversion flag
```

Option A would let `_calculate_gas_bills` pass `gas_already_therms=True` when
it detects that pre-loaded therms data is being used (the existing
`"load_data" in raw_load.columns` check already performs this detection).

---

## Reproducing the bug

```python
import pandas as pd
from cairo.rates_tool.loads import _return_load, process_building_demand_by_period

# Load gas data via _return_load — this returns kWh, then _adjust_gas_loads
# converts to therms inside aggregate_load_worker.
raw_gas = _return_load(load_type="gas", target_year=2025, ...)
# raw_gas["load_data"] is now in therms

# Pass the therms data back in as prepassed_load.
# _adjust_gas_loads runs again inside aggregate_load_worker → double conversion.
agg, _ = process_building_demand_by_period(
    load_col_key="total_fuel_gas",
    prepassed_load=raw_gas,   # already therms, but will be converted again
    ...
)

# agg["load_data"] is now in therms² (0.034× of what it should be)
```

A minimal test to detect the bug — the annual gas load for an RI residential
building should be 200–1,500 therms; after double-conversion it will be 7–51:

```python
annual_therms = raw_gas["load_data"].sum()
assert annual_therms > 100, f"Looks double-converted: {annual_therms:.1f} therms"
```
