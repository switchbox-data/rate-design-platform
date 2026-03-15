# Applying percentage-of-bill charges in CAIRO (utility_tax_rate)

CAIRO has a built-in mechanism for applying a flat percentage surcharge to all customer bills **after bill calculation but before the Bill Alignment Test (BAT)**. This is the `utility_tax_rate` parameter on `MeetRevenueSufficiencySystemWide.simulate()`.

## When to use this

Use `utility_tax_rate` for charges that are:

1. **Percentage-of-bill** — computed as a percentage of total charges, not flat $/kWh
2. **Fixed-pool / cross-subsidy-creating** — the underlying budget is fixed and the percentage rate adjusts to hit a revenue target, so higher-consumption customers pay a disproportionate share (the same mechanic as any "fixed pool ÷ volumetric" charge)
3. **Applied uniformly** — same rate for all customers in the run

Common examples: state regulatory assessments (PSL §18-a), gross earnings taxes that fund fixed budgets, settlement surcharges computed as a fraction of revenue.

## How it works

### Pipeline order

```
1. Calculate base bills from tariff (energy charges, customer charges, demand charges)
2. Precalc: adjust energy charges to meet revenue requirement
3. Apply utility_tax_rate  ← percentage of total bill, applied here
4. Apply LMI bill assistance (discounts, rider)
5. Run BAT (bill alignment test uses the taxed bills)
```

Because the tax is applied before BAT, the cross-subsidy from the percentage charge is captured in the BAT results. If you instead applied it in post-processing after CAIRO, bill calcs would be correct but BAT would not reflect the charge.

### CAIRO implementation

In `cairo.rates_tool.customer_bill_calculation`:

```python
def _apply_utility_tax(self, utility_tax_rate, customer_bills_df):
    customer_bills_df *= 1 + utility_tax_rate
    return customer_bills_df
```

Called from `MeetRevenueSufficiencySystemWide._customer_bill_postprocessing()`.

### simulate() signature

```python
bs.simulate(
    ...
    utility_tax_rate=0.0,       # float, 0.0–1.0 (e.g. 0.004 for 0.4%)
    energy_surcharge_rate=0.0,  # separate: flat $/kWh surcharge (e.g. CA energy surcharge)
    ...
)
```

Both default to 0.0. `energy_surcharge_rate` is volumetric ($/kWh × load), not percentage-of-bill; it's a separate mechanism.

## Wiring in run_scenario.py

Currently `run_scenario.py` does not pass `utility_tax_rate` to `simulate()`, so it defaults to 0.0. To enable it:

1. **Scenario YAML**: add `utility_tax_rate: <float>` to the run config
2. **ScenarioSettings**: add `utility_tax_rate: float = 0.0` field
3. **_build_settings_from_yaml_run**: parse it from the YAML
4. **run()**: pass `utility_tax_rate=settings.utility_tax_rate` to `bs.simulate()`

## Key limitation: single rate for all customers

`utility_tax_rate` is a single float applied uniformly to every customer in the run. This is accurate when the percentage charge applies equally to all customers (e.g. a statewide assessment at a single rate). It does **not** handle:

- **Territory-specific charges** — e.g. a surcharge that applies only to customers in one county
- **Multiple percentage charges at different rates** — you'd have to combine them into one effective rate, which is only correct if they all apply to the same base (total bill)
- **Charges that are a percentage of a subset of bill components** (e.g. percentage of delivery charges only, not supply)

For the last case, using total-bill percentage is an approximation. The error depends on how different the delivery-to-supply ratio is across customers.

## Example: PSEG-LI (NY)

PSEG-LI has two percentage-of-bill charges that create cross-subsidies and should ideally be captured in BAT:

### NY State Assessment (PSL §18-a)

Per LIPA's tariff book (Leaves 182H–182I), this is a **percentage increase applied to essentially the entire bill**: base rates (service/meter charges, energy kWh, demand kW), Power Supply Charge, VBA, DER Cost Recovery, Shoreham SPT factor, RDM, DSA, Securitization charges, UGC, CBC, and miscellaneous charges. Only PILOTs are excluded. The rate is set annually as `(assessment amount ÷ projected revenues)` and adjusts to hit a fixed target — classic fixed-pool cross-subsidy.

This is a good candidate for `utility_tax_rate` since it applies to all customers at the same rate.

### Shoreham Property Tax Settlement

Per LIPA's tariff book (Leaf 172), this is a **percentage-of-revenue factor applied to monthly bills** for Suffolk County customers only. The bond repayment cost for the year is divided by expected retail revenues. It does NOT apply to Nassau County or Rockaway Peninsula customers.

This **cannot** be handled by `utility_tax_rate` alone because it's territory-specific. Options:

- Ignore it (small — estimated ~1.5% of total delivery revenue)
- Use a population-weighted average rate for all customers (imprecise)
- Extend CAIRO to support per-customer tax rates (requires territory info in customer metadata)

### Practical approach for PSEG-LI

Pass the NYSA percentage as `utility_tax_rate` in PSEG-LI scenario configs. This captures the larger, uniformly-applied charge in both bill calcs and BAT. Accept Shoreham as a known gap until per-customer tax support is added.

The actual NYSA rate must be looked up from LIPA's "Statement of NYS Assessment Factor" (published on lipower.org, updated annually). It is a variable rate resolved via Genability's `newYorkStateAssessment` variable rate key, but because Genability represents it as `chargeType: QUANTITY` / `rateUnit: PERCENTAGE`, the existing `fetch_monthly_rates.py` pipeline (which only processes `CONSUMPTION_BASED` charges) does not capture it.

## Other states

Other utilities may have similar percentage-of-bill charges:

- **RI Gross Earnings Tax (GET)**: 4.167% of total bill. Currently excluded from BAT (treated as a true tax pass-through with no fixed pool — the tax rate is set by statute, not adjusted to hit a revenue target). If you wanted to include it, `utility_tax_rate=0.04167` would work.
- **NY GRT (ConEd)**: Percentage of charges, varies by zone (H/I/J). Excluded from BAT because it's a tax pass-through with no fixed pool. Cannot use `utility_tax_rate` anyway since the rate varies by zone.
- **ConEd NY State Surcharge**: ConEd may represent §18-a differently ($/kWh vs percentage). Check the ConEd tariff book if needed.
