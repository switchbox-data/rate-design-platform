# EPMC Residual Allocation and the Delivery/Supply Split

This document explains what we added to the residual allocation pipeline in March 2026, why we added it, and how to test it for New York. It's written for a teammate who needs to run NY batches and verify the outputs.

## 1. Why: EPMC and the supply residual problem

### What is EPMC?

EPMC stands for "equi-proportional marginal cost." It's a way of splitting the revenue requirement between customer subclasses (HP vs. non-HP).

The basic idea: every customer's share of the residual is proportional to their share of total marginal costs. If HP customers cause 3% of total marginal costs, they bear 3% of the residual. Mathematically:

$$R_i = R \times \frac{EB_i}{\sum_j EB_j \times w_j}$$

where $R$ is the total residual, $EB_i$ is customer $i$'s economic burden (sum of hourly load × hourly marginal price), and $w_j$ is the sample weight.

This is equivalent to scaling all MC-based rates by a uniform constant $K = TRR / MC_{Revenue}$. Every customer's total bill is exactly $K$ times their economic burden. The markup ratio is the same for everyone.

### Why did we add it?

The existing per-customer allocation splits the residual equally across all customers, regardless of their load. This is the "lump-sum" approach — efficient in theory (no distortion of consumption decisions), but it doesn't reflect cost causation. Utilities and regulators accustomed to embedded cost-of-service (ECOS) analysis tend to prefer cost-causation-based allocation, where the residual is assigned to customers in proportion to how much system cost they drive.

EPMC captures this perspective: if your load drives more marginal cost, you bear more residual. It's the standard reconciliation method used in California (CPUC) and is referenced in the BAT paper itself (Simeone et al. 2023, Appendix B, Eq. B3).

Whether cost-causation is the _right_ normative principle for allocating sunk costs is debatable — see `context/domain/bat_mc_residual/fairness_in_cost_allocation.md` for the full discussion. The short version: historical cost causation = current cost causation is an assumption that is dubious both normatively (sunk costs shouldn't affect marginal decisions) and empirically (past investment decisions don't cleanly map to current load patterns). But it is the lens that most utility regulators actually use.

### The supply residual problem

When we implemented EPMC and started testing, we discovered something we hadn't fully appreciated: there are significant **supply-side residuals** at every utility. The supply residual is the gap between what customers pay for supply (retail supply charges × kWh) and the wholesale marginal cost of that supply (ISO energy LMP + capacity FCA/ICAP + ancillary).

Here's how large the residual is, as a percentage of total revenue, for both delivery and supply:

| Utility | Delivery residual | Supply residual |
| ------- | :---------------: | :-------------: |
| rie     |        87%        |       29%       |
| cenhud  |        96%        |       27%       |
| coned   |        96%        |       15%       |
| nimo    |        98%        |       28%       |
| nyseg   |        98%        |       36%       |
| or      |        95%        |       30%       |
| psegli  |        93%        |       27%       |
| rge     |        98%        |       30%       |

Delivery residual is 87–98% of delivery revenue. This is well-known — delivery costs are dominated by infrastructure that's essentially a fixed budget, and marginal delivery costs are a small fraction of the total.

Supply residual is 15–36% of supply revenue. This is less intuitive. The retail supply rate recovers more revenue than the wholesale marginal cost of supply. The gap covers things like supplier margin, hedging/procurement overhead, RECs, capacity contract premiums above the marginal clearing price, working capital, bad debt, and other costs baked into the retail supply rate. (Note: we are currently missing ancillary MCs for NY, which would shrink the supply residual somewhat.)

**Open questions about the supply residual:**

1. Is the residual "real" (i.e., do the retail supply rates genuinely include costs beyond energy/capacity/ancillary MCs), or is it partly an artifact of our MC inputs not capturing all supply cost components?
2. If the residual is real, what costs does it collect? Are those costs variable (they scale with consumption in time $t$, like MCs) or more like fixed budgets that have to be recovered regardless of load?
3. If they're fixed budgets, the same cross-subsidization logic applies to supply as to delivery — HP customers with higher kWh are bearing a disproportionate share of a fixed cost through the flat supply rate, just like they do on delivery. And then we need to decide how to allocate the supply residual, just like we do on delivery.

We don't have definitive answers to these questions yet. For now, the supply residual is treated as a design parameter — each run can choose how (or whether) to allocate it.

## 2. What: the delivery/supply allocation split

### The architecture change

Previously, `compute_subclass_rr` produced a single set of subclass revenue requirements (delivery + supply combined) under each allocation method. The supply component was derived by subtracting run 1 (delivery-only) from run 2 (delivery+supply).

The problem: this subtraction is only clean when the allocation weights are the same in both runs. For per-customer and volumetric allocation, they are — customer count and kWh don't change between runs. For EPMC, they're not — HP's share of economic burden is 2.67% in the delivery-only run vs. 4.83% in the combined run (because adding supply MCs changes the EB mix). The subtraction produces a "supply EPMC" number that doesn't correspond to any principled supply-only allocation. In RI, this inflated HP's supply RR by $10M, making the HP flat rate look terrible when it should have looked good.

The fix: the subclass RR YAML now has **separate delivery and supply blocks**. Each run picks its delivery allocation method and supply allocation method independently.

### The new YAML structure

```yaml
subclass_revenue_requirements:
  delivery:
    passthrough:        # no cross-subsidy correction
      hp: 23816775
      non-hp: 518739813
    percustomer:        # corrects MC + residual cross-subsidy
      hp: 11538113
      non-hp: 531018474
    epmc:               # allocates residual by MC share
      hp: 14510936
      non-hp: 528045651
    volumetric:         # allocates residual by kWh
      hp: ...
      non-hp: ...
  supply:
    passthrough:        # same supply rate as default — no correction at all
      hp: 21129827
      non-hp: 391820543
    percustomer:        # corrects supply MC + residual cross-subsidy
      hp: 18175699
      non-hp: 394774670
    volumetric:         # corrects supply MC cross-subsidy, not residual
      hp: ...
      non-hp: ...
    # No EPMC: broken by the subtraction architecture. Volumetric gives
    # nearly the same answer (within $1M for RI).
```

Each scenario run in the YAML picks its methods:

```yaml
residual_allocation_delivery: epmc
residual_allocation_supply: passthrough
```

The parser composes the total: `total_RR[subclass] = delivery[method_d][subclass] + supply[method_s][subclass]`.

### The four allocation methods and what they do

Here's what each method does, concretely, when you apply it to a subclass:

**Passthrough** leaves the cross-subsidy intact on both the MC and the residual. HP's subclass RR equals their actual bills under the default flat rate. CAIRO then calibrates the HP tariff to recover those same bills — so the HP rate ends up being the same as the default rate. No correction at all. Use this when you don't want to touch that side of the bill (e.g., supply-side for a delivery-only rate design).

**Volumetric** fixes the MC cross-subsidy but leaves the residual allocated by kWh (same as the flat rate). HP's subclass RR = HP's economic burden + (HP kWh share × total residual). If HP's average MC per kWh is below the system average (true on supply side, where capacity peaks in summer and HP load peaks in winter), HP gets a lower subclass RR than passthrough. If HP's average MC per kWh is above average (true on delivery side in summer-peaking systems), HP gets a higher subclass RR.

**EPMC** fixes the MC cross-subsidy and allocates the residual proportional to each subclass's share of total MC. This usually gives HP a lower total residual allocation than volumetric (because HP's MC share is typically less than their kWh share on supply, and the reverse on delivery). On delivery, where residual is 87–98% of costs, the allocation method matters enormously. On supply, where residual is 15–36%, the difference between EPMC and volumetric is small.

**Per-customer** fixes the MC cross-subsidy and allocates the residual evenly per person. This removes the volumetric distortion the most — HP customers with high kWh no longer bear a disproportionate share of the fixed-cost residual. It ends up with the lowest HP subclass cost allocation for both delivery and supply, because HP customers are a small fraction of total customer count (2% in RI).

### How much the allocation method matters: delivery vs. supply

The impact of the allocation method depends on how large the residual is relative to total costs. On delivery, where 87–98% of the bill is residual, the allocation method is decisive — it determines almost the entire subclass RR. On supply, where 15–36% is residual, the allocation method matters but the MC component dominates.

| Utility | Delivery residual % | Supply residual % |
| ------- | :-----------------: | :---------------: |
| rie     |         87%         |        29%        |
| cenhud  |         96%         |        27%        |
| coned   |         96%         |        15%        |
| nimo    |         98%         |        28%        |
| nyseg   |         98%         |        36%        |
| or      |         95%         |        30%        |
| psegli  |         93%         |        27%        |
| rge     |         98%         |        30%        |

This is why we didn't bother with volumetric on the delivery side until now — when 98% of the cost is residual, the MC correction is almost irrelevant; what matters is how you allocate the residual. On the supply side, the MC correction is more meaningful (it's 64–85% of the bill), and the residual allocation method matters less.

### The supply EPMC bug

Supply-side EPMC is currently disabled in the pipeline. Here's why.

CAIRO runs separately for delivery-only (run 1) and delivery+supply (run 2). We derive the supply component by subtraction: supply = run 2 - run 1. For EPMC, the allocation weights (economic burden shares) are different in each run:

- Run 1 (delivery only): HP is 2.67% of total EB
- Run 2 (delivery+supply): HP is 4.83% of total EB

The subtraction `EPMC(run 2) - EPMC(run 1)` produces a number that uses mixed weights. In RI, this created a phantom $15.9M "supply EPMC residual" for HP, when the correct value (computed independently using supply-only EB) is ~$6.4M. The $10M error inflated HP's total RR from $30M to $46M.

Per-customer and volumetric don't have this problem because their weights (customer count, kWh) are the same in both runs. Volumetric supply gives nearly the same answer as correct supply EPMC (within $1M for RI), so we use volumetric as a proxy.

Actually implementing correct supply EPMC would require decomposing run 2's economic burden into delivery-only and supply-only EB per customer (using the separate delivery and supply MC traces that CAIRO already has), then doing two separate EPMC allocations within a single run. This is a change to CAIRO's postprocessor — maybe 50 lines of code, but it changes the semantics of what BAT_epmc means in run 2 and needs careful validation.

### What we're actually doing right now

For **RI**, we're only changing runs 17–18 (HP flat rate). These use EPMC delivery + passthrough supply. All other RI subclass runs (5–6 seasonal, 9–10 TOU, 13–14 TOU flex) keep per-customer for both delivery and supply, same as before.

| RI runs   | Rate design | Delivery    | Supply          |
| --------- | ----------- | ----------- | --------------- |
| 5–6       | HP seasonal | percustomer | percustomer     |
| 9–10      | HP TOU      | percustomer | percustomer     |
| 13–14     | HP TOU flex | percustomer | percustomer     |
| **17–18** | **HP flat** | **epmc**    | **passthrough** |

For **NY**, nothing changes yet. All subclass runs use per-customer for both:

| NY runs | Rate design | Delivery    | Supply      |
| ------- | ----------- | ----------- | ----------- |
| 5–6     | HP seasonal | percustomer | percustomer |
| 9–10    | HP TOU      | percustomer | percustomer |
| 13–14   | HP TOU flex | percustomer | percustomer |

**What's likely to change for NY:**

- We'll probably move NY delivery allocation to EPMC as well.
- We may end up making the "fair rate" for NY a flat HP rate instead of seasonal.
- We still need to decide what the supply-side allocations should be — whether to move them to EPMC as well (which would mean volumetric in practice, since supply EPMC is broken and volumetric is a close proxy), or keep per-customer.

All of these choices come later. For now, NY is unchanged. The delivery and supply allocation methods for each run are specified in the Google Sheet's `residual_allocation_delivery` and `residual_allocation_supply` columns, and flow through to the scenario YAML via `just create-scenario-yamls`.

## 3. How: testing NY

The previous section explains the theory. This section tells you exactly how to run a NY batch and verify the outputs. We've already run RI with these changes. NY is next, and should produce identical results to the old batch for all runs (since NY still uses per-customer for everything).

### What was implemented (high level)

1. **CAIRO monkey-patches** (`utils/mid/patches.py`): added EPMC residual allocation, disabled broken peak allocation, added BAT_epmc to the cross-subsidization CSV.

2. **`compute_subclass_rr.py`**: computes all delivery and supply allocation methods in a single pass and writes the new YAML structure with separate delivery/supply blocks.

3. **`scenario_config.py` + `run_scenario.py`**: the parser reads `residual_allocation_delivery` and `residual_allocation_supply` from the scenario YAML, composes the total RR from the delivery + supply blocks.

4. **`build_master_bat.py`**: detects available BAT metrics at runtime — gracefully handles missing BAT_peak and new BAT_epmc columns.

5. **`create_scenario_yamls.py`**: reads `residual_allocation_delivery` and `residual_allocation_supply` columns from the Google Sheet.

The key insight: all the subclass RR values (for every combination of allocation method) are computed once by `compute-rev-requirements` and written to a single YAML file. Each run then picks which delivery and supply method to use, via its scenario YAML. So testing is: run `compute-rev-requirements`, check the YAML, run the batch, check the outputs.

### Step-by-step: run and verify a NY batch

#### Prerequisites

1. Make sure the Google Sheet has `residual_allocation_delivery` and `residual_allocation_supply` columns. NY runs 5, 6, 9, 10, 13, 14 should have `percustomer` for both. All other runs: blank.

2. On the server, pull the latest code:
   ```bash
   cd /ebs/home/jpv_switch_box/rate-design-platform
   git pull
   ```

3. If scenario YAMLs were regenerated from the sheet, verify them:
   ```bash
   grep -n "residual_allocation" rate_design/hp_rates/ny/config/scenarios/scenarios_cenhud.yaml
   ```
   Runs 5, 6, 9, 10, 13, 14 should show `residual_allocation_delivery: percustomer` and `residual_allocation_supply: percustomer`.

#### Run the batch

```bash
cd /ebs/home/jpv_switch_box/rate-design-platform/rate_design/hp_rates

# Run all-pre for NY (regenerates RR YAMLs with new delivery/supply structure)
just s ny all-pre

# Verify one RR YAML has the new structure
cat ny/config/rev_requirement/cenhud_hp_vs_nonhp.yaml | head -30
# Should have "delivery:" and "supply:" blocks, each with
# "passthrough:", "percustomer:", "volumetric:" sub-blocks.

# Run all utilities
for util in or cenhud rge nyseg psegli nimo coned; do
  echo ">> Running $util"
  UTILITY=$util RDP_BATCH=ny_YYYYMMDD_r1-16_epmc just s ny run-all-parallel-tracks
done

# Build all master bills + BATs
just s ny build-all-master ny_YYYYMMDD_r1-16_epmc
```

(Replace `YYYYMMDD` with today's date.)

#### Manual verification: the YAML

After `just s ny all-pre`, check one of the RR YAMLs:

```bash
cat ny/config/rev_requirement/cenhud_hp_vs_nonhp.yaml
```

You should see:

- A `subclass_revenue_requirements:` key with `delivery:` and `supply:` sub-blocks
- Under `delivery:`, keys for `passthrough`, `percustomer`, `epmc`, `volumetric`
- Under `supply:`, keys for `passthrough`, `percustomer`, `volumetric`
- Each contains `hp:` and `non-hp:` with scalar dollar values
- `percustomer` delivery values should match the old YAML's `hp.delivery` and `non-hp.delivery` values (since NY runs use per-customer delivery)

#### LLM verification: master BAT and bills

After the batch completes and master bills/BATs are built, have an LLM run the following verification. Copy-paste this entire block into a chat with an LLM that has access to the rate-design-platform repo and AWS:

---

**VERIFICATION PROMPT FOR LLM:**

I just ran a NY batch called `ny_YYYYMMDD_r1-16_epmc` (replace with actual batch name). The old batch to compare against is `ny_20260325b_r1-16`. Please run the following checks and report results.

The batch is at `s3://data.sb/switchbox/cairo/outputs/hp_rates/ny/all_utilities/`. Run pairs are: 1+2, 3+4, 5+6, 7+8, 9+10, 11+12, 13+14, 15+16.

**Check 1: New columns present, old columns absent.**

For each run pair, read the master BAT parquet and verify:

- `BAT_epmc_delivery` is present (new EPMC column)
- `BAT_peak_delivery` is NOT present (peak was disabled)
- `residual_share_epmc_delivery` is present

Also read the master bills parquet and verify LMI columns exist (e.g., `elec_total_bill_lmi_100` or `elec_total_bill_lmi_40`).

**Check 2: All run pairs 1–12 BAT unchanged vs old batch.**

For each of run pairs 1+2, 3+4, 5+6, 7+8, 9+10, 11+12: join old and new master BAT on `bldg_id` + `sb.electric_utility`. For every shared numeric column, the max absolute difference should be less than 1e-6. Report any column that exceeds this tolerance.

**Check 3: All run pairs 1–12 bills unchanged vs old batch.**

Same as check 2 but for master bills. Join on `bldg_id` + `sb.electric_utility` + `month`. All shared numeric columns should match within 1e-6.

**Check 4: Run pairs 13–14 and 15–16 may differ.**

These are the demand-flex runs. The CAIRO patches (peak removal, EPMC addition) affect the frozen-residual decomposition in the demand-flex code path. Small diffs in runs 13–14 and larger diffs in 15–16 (which inherit the calibrated tariff from 13–14) are expected. Report the diffs but don't treat them as failures.

**Check 5: Building counts consistent.**

For each utility, verify the building count is the same across all 8 run pairs.

**Expected results:**

- Check 1: all OK (epmc present, peak absent, LMI cols present)
- Check 2: all OK for runs 1–12
- Check 3: all OK for runs 1–12
- Check 4: runs 13–16 may show diffs (expected)
- Check 5: all OK

---

That verification prompt is self-contained — the LLM should be able to run it using `polars` to read the S3 parquets and report results. The key thing you're looking for is that runs 1–12 are identical to the old batch (no behavioral change for NY), while runs 13–16 may differ slightly due to the demand-flex interaction with the CAIRO patches.
