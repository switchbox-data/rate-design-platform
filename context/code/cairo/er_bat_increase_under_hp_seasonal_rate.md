# Why electric resistance customers' BAT increases under the HP seasonal rate

## The observation

When comparing statewide mean delivery BAT by heating type between runs 1+2 (current flat rates) and runs 5+6 (HP seasonal rate), electric resistance (ER) customers' overpayment grows:

| Heating type        | Current flat rates | HP seasonal rate |
| ------------------- | ------------------ | ---------------- |
| Heat pump           | $836               | $0               |
| Natural gas         | −$131              | −$112            |
| Oil / propane       | −$74               | −$51             |
| Electric resistance | $873               | $927             |

HP overpayment is eliminated (by design), fossil fuel underpayment shrinks, but ER overpayment **increases** by ~$54/yr.

## What changes between the two runs

From `scenarios_coned.yaml` (and analogously for the other six utilities):

- **Same buildings, same loads, same marginal costs.** Both run pairs use upgrade 0, the same ResStock load curves, and the same distribution/supply marginal cost parquets.
- **Different tariff structure.** Runs 1+2 use a single flat rate for all customers. Runs 5+6 use two tariffs: `coned_hp_seasonal` for HP customers and `coned_flat` for non-HP customers, assigned via the tariff map `coned_hp_seasonal_vs_flat.csv`.
- **Different revenue requirement structure.** Runs 1+2 use `coned.yaml` (single RR: $3.26B delivery). Runs 5+6 use `coned_hp_vs_nonhp.yaml` with the total split into two subclass RRs (HP delivery: $78.4M; non-HP delivery: $3.18B). CAIRO calibrates each tariff independently to its subclass RR (`run_includes_subclasses: true`).

ER customers are classified as **non-HP** (`has_hp = false`) in the tariff map — they get the recalibrated non-HP flat rate in runs 5+6.

## The mechanism

### Step 1: How subclass RRs are computed

`compute_subclass_rr.py` sets each subclass's RR to its total cost-of-service:

```
RR_subclass = sum(weighted_bills) - sum(weighted_BAT) = sum(weighted_cost_of_service)
```

HP customers have high kWh but low cost-of-service (they consume mostly off-peak and don't trigger capacity upgrades). Their cost-of-service fraction of the total RR (~2.4%) is much smaller than their kWh fraction.

### Step 2: The non-HP flat rate rises

In runs 1+2, CAIRO calibrates one flat $/kWh rate to the total RR:

$$\text{flat\_rate} = \frac{\text{Total\_RR}}{\text{Total\_kWh}}$$

In runs 5+6, the non-HP flat rate is calibrated to the non-HP subclass RR:

$$\text{flat\_rate\_nonHP} = \frac{\text{NonHP\_RR}}{\text{NonHP\_kWh}}$$

The ratio of the two:

$$\frac{\text{flat\_rate\_nonHP}}{\text{flat\_rate}} = \frac{1 - \text{HP\_cost\_fraction}}{1 - \text{HP\_kWh\_fraction}}$$

Since $\text{HP\_cost\_fraction} < \text{HP\_kWh\_fraction}$ (HP customers' share of cost is smaller than their share of kWh), the numerator exceeds the denominator, so $\text{flat\_rate\_nonHP} > \text{flat\_rate}$.

Intuitively: HP customers' large kWh contribution was "diluting" the average $/kWh needed. Remove them from the pool and the remaining per-kWh rate must rise.

### Step 3: ER customers absorb the increase

The higher non-HP flat rate affects all non-HP customers:

- **Gas/oil/propane customers** (low kWh): bills go up modestly. Since they were underpaying (negative BAT), their BAT moves toward zero — closer to fair.
- **ER customers** (high kWh): bills go up substantially ($\text{higher\_rate} \times \text{high\_kWh}$). Since they were already overpaying (positive BAT), their BAT moves further from zero — less fair.

ER customers have the same structural problem as HP customers (high electricity consumption, low marginal cost-of-service due to off-peak heating patterns), but they're classified as non-HP and remain on the volumetric flat rate. They were "partners in overpaying" alongside HP customers. When HP customers get their own fair rate, ER customers are left shouldering their own structural overpayment plus the share that HP customers previously contributed.

## Is this a bug or artifact?

No. It is a real mathematical consequence of the subclass design:

1. The HP seasonal rate is designed to eliminate the HP cross-subsidy. It does.
2. The non-HP flat rate is still volumetric, so it still creates within-subclass cross-subsidies.
3. The non-HP flat rate is higher because HP kWh left the pool.
4. ER customers, being the highest-kWh non-HP customers, bear the largest absolute increase.

The total weighted BAT across all customers still sums to zero — the BAT improvements for HP and fossil fuel customers are offset by the BAT increase for ER customers.

## Implication

If the policy goal extends beyond HP customers to all high-electricity customers who overpay under volumetric rates, ER customers would need their own rate treatment (or be included in the HP subclass). Under the current design, fixing the HP cross-subsidy redistributes part of it onto ER customers rather than eliminating it system-wide.
