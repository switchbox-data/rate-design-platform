# NYISO Zonal LBMP Parquet: Data Quality Report

This document describes known data-quality issues in the converted NYISO Day-Ahead and Real-Time zonal LBMP parquet (source: NYISO MIS monthly ZIPs). It is intended for data scientists and analysts so they can decide whether to apply additional cleaning or filtering for their use case.

**Dataset:** Zone-level LBMP (Locational Marginal Price) and components (marginal cost losses, marginal cost congestion).  
**Partitioning:** `zone=Z/year=YYYY/month=MM` under `day_ahead/zones/` and `real_time/zones/`.  
**Validation:** The pipeline runs `validate_lbmp_zonal_parquet.py` after convert; that script checks schema, nulls, row counts, uniqueness, zone set, and value ranges. Many partitions fail one or more of these checks due to the issues below—all of which originate in the **source CSVs** or in **Eastern-time DST (daylight saving time)** behavior.

---

## 1. Overview of issue types

| Issue type | Series | Cause | Need to clean? |
|------------|--------|--------|-----------------|
| Row count mismatch | Day-ahead, Real-time | DST, missing/extra intervals in source, partial months | Depends on use case |
| Duplicate timestamps | Day-ahead, Real-time | DST “fall back” hour (two 01:00 Eastern) | Yes if you need unique (interval, zone) |
| LBMP outside [-500, 2000] $/MWh | Both | Real price spikes or negative prices | Usually no (real data) |
| Congestion outside [-999, 999999] | Both | Real congestion components | Usually no (real data) |

The validator uses fixed “expected” row counts and value bounds; the tables below explain **when** and **why** the data deviate so you can judge impact.

---

## 2. Day-ahead market (hourly)

**Expected:** One row per hour in the month, in Eastern time.  
Expected count = `days_in_month × 24` (e.g. 744 for 31 days, 720 for 30 days).

### 2.1 Row count: one hour short (e.g. 719 vs 720, 743 vs 744)

**What:** Some 30-day months have 719 rows instead of 720; some 31-day months have 743 instead of 744.

**When it occurs:**

- **719 vs 720:** 30-day months (April, June, September, November). One hour is missing in the source CSV for that zone/month.
- **743 vs 744:** 31-day months in **March** (e.g. 2010, 2011, 2016, 2017, 2018, 2020). March has a “spring forward” DST transition: 02:00 Eastern is skipped. So that day has 23 clock hours in Eastern. 31×24 − 1 = 743.

**Cause:** Source reflects Eastern clock hours. After “spring forward” there is no 02:00–03:00 hour for that day, so the total for the month is one row short.

**Do you need to clean?**  
If you need exactly 24 rows per day for every day (e.g. for a naive 24×N matrix), you may want to insert a row for the missing hour (e.g. with null or interpolated price). For many analyses (e.g. average LBMP, total cost over the month), using the rows that exist is fine.

### 2.2 Row count: one hour extra (e.g. 745 vs 744, 721 vs 720)

**What:** Some months have one more row than the nominal `days × 24`.

**When it occurs:**

- **745 vs 744:** 31-day **October** (e.g. 2002–2005, …). October has “fall back”: 01:00–02:00 occurs twice (EDT then EST). So there are two distinct timestamps that both show as 01:00 Eastern in the source (or the source has 25 hours for that day). 31×24 + 1 = 745.
- **721 vs 720:** 30-day **November** (e.g. 2010, 2011, 2016, 2017, 2018, 2020). Same DST fall-back: one extra 01:00 hour. 30×24 + 1 = 721.

**Cause:** Eastern time is ambiguous during the repeated hour. The source includes both intervals; we parse with `ambiguous="earliest"` for *parsing*, but we do not drop the duplicate clock-hour row, so both intervals remain.

**Do you need to clean?**  
If you need a unique time index (one row per `interval_start_est` per zone), you must deduplicate. Common approaches: keep the first occurrence of the repeated hour, or the last, or average; document the choice. For aggregations (e.g. sum or average over the month), the extra row is correct—both intervals actually occurred.

### 2.3 Uniqueness: duplicate `interval_start_est`

**What:** The validator reports “interval_start_est not unique: K unique vs N rows” (e.g. 744 unique vs 745 rows).

**When it occurs:** Same months as in §2.2—October (31 days) and November (30 days) in DST fall-back years. Two rows share the same Eastern clock time (e.g. 01:00).

**Cause:** In the source, the repeated hour may be represented as two rows with the same timestamp string, or as two timestamps that both map to the same Eastern clock time. Our converter does not deduplicate.

**Do you need to clean?**  
Yes, if your analysis assumes a unique `(interval_start_est, zone)` (e.g. for joins or time-series models). Deduplicate by `interval_start_est` (and zone) and choose which row to keep (first, last, or average LBMP). If you only care about totals or averages over the month, you can leave as-is.

---

## 3. Real-time market (5-minute intervals)

**Expected:** One row per 5-minute interval in the month, Eastern time.  
Expected count = `days_in_month × 24 × 12` (e.g. 8928 for 31 days, 8640 for 30 days).

### 3.1 Row count variation

**What:** Many real-time partitions have row counts that differ from the nominal expectation (8928, 8640, etc.).

**When it occurs:** Across many months and zones. Examples from validation: 9258, 8864, 9317, 9044, 8802, 8249, 8793, 9159, 9041, 8822, 9071, 9190, 8863, 5841 (partial month), etc.

**Causes:**

1. **DST:** “Spring forward” removes one clock hour → 12 fewer 5-min intervals that day. “Fall back” adds one clock hour → 12 more intervals. So you can see counts like expected ± 12 or small multiples.
2. **Partial months:** The most recent month in the pipeline may be incomplete (e.g. 5841 for December 2025 if the ZIP was published mid-month). Same for the first available month in 2000 if the source started mid-month.
3. **Source quirks:** Occasional missing or duplicate 5-min files in the monthly ZIP, or extra/missing rows in a daily CSV, can shift counts.

**Do you need to clean?**  
If you need a strict regular 5-min grid (e.g. for regression or alignment with other 5-min series), you may want to: (1) restrict to complete months (e.g. exclude the latest month), and (2) for DST months, either accept the off-by-12 (or similar) or build a canonical 5-min grid and left-join/fill. For many uses (e.g. average RT price by hour or month), using the rows that exist is acceptable.

### 3.2 Uniqueness

**What:** Same as day-ahead: “interval_start_est not unique” in DST fall-back months (e.g. November 2025: 8781 unique vs 8793 rows).

**When it occurs:** In real-time, the repeated hour has 12 intervals that occur “twice” in clock time (e.g. 01:00, 01:05, …, 01:55). So you can get up to 12 duplicate timestamps per zone in that hour.

**Do you need to clean?**  
Same as §2.3: deduplicate if you require a unique (interval, zone); otherwise you can keep for aggregation.

---

## 4. Price and component value ranges

The validator flags values outside these bounds:

- **lbmp_usd_per_mwh:** [-500, 2000] $/MWh  
- **marginal_cost_losses_usd_per_mwh**, **marginal_cost_congestion_usd_per_mwh:** [-999, 999999] $/MWh  

### 4.1 LBMP above 2000 $/MWh or below -500 $/MWh

**What:** Some partitions contain LBMP values above 2000 or below -500.

**When it occurs:** Sporadically across years, months, and zones. Examples from validation: max 2426.6, 4700.52, 5393.2, 7337.77 $/MWh; min -685.16 $/MWh.

**Cause:** Real market outcomes. NYISO can have short-duration price spikes (e.g. scarcity, congestion) and negative prices (e.g. oversupply, renewables). The validator bounds are conservative; they are not data errors.

**Do you need to clean?**  
Usually **no**. These are valid LBMPs. If your use case assumes prices in a narrow band (e.g. for visualization or simple models), you can cap/winsorize or filter; otherwise keep as-is.

### 4.2 Congestion (or losses) outside [-999, 999999]

**What:** The validator reports “Plausible range: marginal_cost_congestion_usd_per_mwh has values outside [-999, 999999]”.

**When it occurs:** In the same partitions that have extreme LBMP; congestion (and sometimes losses) can be large during tight conditions.

**Cause:** Real congestion components. The validator’s bounds are very wide; this check mainly catches sentinel or corrupt values. Values slightly outside are still typically valid.

**Do you need to clean?**  
Generally **no** unless you see obvious corruption (e.g. 1e9 or constant -999). For analysis, use the values as-is or apply domain-specific caps if required.

---

## 5. Summary: do you need to clean?

- **Unique (interval, zone) for joins or time-series:** Yes. Deduplicate on `interval_start_est` (and zone) in DST fall-back months (October, November); choose first, last, or average.
- **Strict row count (exact hours or 5-min grid):** Optional. Either accept DST-driven off-by-one (day-ahead) or ±12-ish (real-time), or fill missing / drop extra intervals; for partial months, exclude or handle explicitly.
- **LBMP or component value bounds:** Usually no. Values outside the validator’s bounds are real; only clean if your method requires it (e.g. winsorization for robustness).

Re-running validation after convert will still report these issues because they reflect the source and DST; this document is the reference for interpreting and deciding on further cleaning.
