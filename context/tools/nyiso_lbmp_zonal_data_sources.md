# NYISO Day-Ahead and Real-Time Zonal LBMP: Data Sources and Samples

Reference for choosing and using sources for **Day-Ahead Market LBMP - Zonal** (P-2A) and **Real-Time Market LBMP - Zonal** (P-24A) in bulk (e.g. 8760 hours × many years). Includes a short comparison and **data samples** from each option.

## Options overview

| Source                     | How it gets data                                                   | Bulk historical                               | Rate limits          | Output format                            |
| -------------------------- | ------------------------------------------------------------------ | --------------------------------------------- | -------------------- | ---------------------------------------- |
| **NYISO MIS (direct ZIP)** | One HTTP GET per month per market from mis.nyiso.com               | Yes                                           | No                   | CSV inside ZIP → you convert to parquet  |
| **gridstatus**             | Python API; fetches from NYISO (daily CSVs or similar per request) | Many requests → colleagues report rate limits | Yes for large ranges | pandas DataFrame                         |
| **NYISOToolkit**           | Same monthly ZIPs as MIS (one request per month)                   | Yes                                           | No                   | Pickle / optional CSV in library storage |

**Recommendation for “free, bulk, minimal cleaning, reliable, S3 parquet”:** Use **NYISO MIS monthly ZIPs** (direct or via your own script). Same data as NYISOToolkit; no dependency and no rate limits. gridstatus is better for ad hoc or small date ranges, not full-year bulk.

---

## 1. NYISO MIS — direct monthly ZIPs

- **URLs**: [P-2A index](https://mis.nyiso.com/public/P-2Alist.htm) (Day-Ahead Zonal), [P-24A index](https://mis.nyiso.com/public/P-24Alist.htm) (Real-Time Zonal).
- **Bulk pattern**: One ZIP per month.
  - Day-Ahead: `https://mis.nyiso.com/public/csv/damlbmp/YYYYMM01damlbmp_zone_csv.zip`
  - Real-Time: `https://mis.nyiso.com/public/csv/realtime/YYYYMM01realtime_zone_csv.zip`
- **Contents**: Each ZIP contains one CSV per day (e.g. `20260201damlbmp_zone.csv`, …). No API key; minimal risk of rate limits if you request one ZIP per month.

### Schema consistency (2000–2024)

One January zip per year was downloaded for both DAM and RT (Jan 1 CSV extracted); headers were compared. **The schema is the same across all years** (2000–2024). Canonical 6-column header:

1. `Time Stamp`
2. `Name`
3. `PTID`
4. `LBMP ($/MWHr)`
5. `Marginal Cost Losses ($/MWHr)`
6. `Marginal Cost Congestion ($/MWHr)`

Minor CSV noise: some older files have a typo in the last column (`$/MWH"` instead of `$/MWHr`) or trailing `\r`/quote; normalize when reading in the convert step (`data/nyiso/lbmp/convert_lbmp_zonal_zips_to_parquet.py`).

### Raw CSV structure (inside each daily file)

Day-ahead and real-time zonal CSVs share the same column idea; real-time has 5‑minute intervals, day-ahead is hourly.

Canonical columns (verified 2000–2024; same for DAM and RT):

| Column                            | Description                                                                                   |
| --------------------------------- | --------------------------------------------------------------------------------------------- |
| Time Stamp                        | Interval start (Eastern)                                                                      |
| Name                              | Zone name (e.g. CAPITL, CENTRAL, DUNWOD, GENESE, HUD VL, LONGIL, MHK VL, MILLWD, NORTH, WEST) |
| PTID                              | Zone PTID (numeric)                                                                           |
| LBMP ($/MWHr)                     | Locational marginal price ($/MWh)                                                             |
| Marginal Cost Losses ($/MWHr)     | Loss component ($/MWh)                                                                        |
| Marginal Cost Congestion ($/MWHr) | Congestion component ($/MWh)                                                                  |

### Data sample (conceptual — one hour, two zones)

```text
Time Stamp,Name,PTID,LBMP ($/MWHr),Marginal Cost Losses ($/MWHr),Marginal Cost Congestion ($/MWHr)
02/20/2026 00:00:00,CAPITL,61757,28.50,0.82,1.20
02/20/2026 00:00:00,CENTRAL,61758,27.10,0.79,0.95
02/20/2026 01:00:00,CAPITL,61757,24.30,0.71,0.88
02/20/2026 01:00:00,CENTRAL,61758,23.90,0.69,0.82
...
```

So: long/tidy table, one row per (timestamp, zone), with LBMP and components. You can partition parquet by `market=dam|rt` and `year`/`month` and standardize column names (e.g. `lbmp_usd_per_mwh`) as needed.

---

## 2. gridstatus

- **Docs**: [GridStatus NYISO](https://opensource.gridstatus.io/en/stable/autoapi/gridstatus/nyiso/index.html), [LMP data guides](https://docs.gridstatus.io/data-guides).
- **Usage**: `nyiso.get_lmp(date=..., end=..., market='DAY_AHEAD_HOURLY'|'REAL_TIME_5_MIN'|'REAL_TIME_15_MIN', location_type='zone')`.
- **Markets**: `DAY_AHEAD_HOURLY`, `REAL_TIME_5_MIN`, `REAL_TIME_15_MIN`. Location types: `zone`, `generator`.
- **Caveat**: Pulling many days/years means many API/CSV requests; colleagues have hit rate limits for historical bulk. Prefer for ad hoc or small ranges.

### Typical DataFrame shape

gridstatus standardizes LMP across ISOs. You typically get a table with time, location identifier, and LMP (and sometimes energy/loss/congestion). Columns are often named along these lines:

| Column (typical)      | Description                              |
| --------------------- | ---------------------------------------- |
| Time                  | Interval start (often timezone-aware)    |
| Location              | Zone or node id (e.g. zone name or PTID) |
| LMP                   | Locational marginal price ($/MWh)        |
| (optional) Energy     | Energy component                         |
| (optional) Loss       | Loss component                           |
| (optional) Congestion | Congestion component                     |

### Data sample (conceptual)

```text
Time                    Location   LMP     Energy  Loss  Congestion
2026-02-20 00:00:00-05  CAPITL     28.50   26.48   0.82  1.20
2026-02-20 00:00:00-05  CENTRAL    27.10   25.36   0.79  0.95
2026-02-20 01:00:00-05  CAPITL     24.30   22.71   0.71  0.88
2026-02-20 01:00:00-05  CENTRAL    23.90   22.39   0.69  0.82
...
```

So: tidy, one row per (Time, Location), ready for analysis; exact column names and presence of components depend on the NYISO implementation in gridstatus.

---

## 3. NYISOToolkit

- **Repo**: [m4rz910/NYISOToolkit](https://github.com/m4rz910/NYISOToolkit).
- **Raw source**: Same NYISO MIS monthly ZIPs (see [dataset_url_map.yml](https://github.com/m4rz910/NYISOToolkit/blob/master/nyisotoolkit/nyisodata/dataset_url_map.yml)) — `damlbmp` + `damlbmp_zone_csv.zip` (day-ahead), `realtime` + `realtime_zone_csv.zip` (real-time).
- **Datasets**: `lbmp_dam_h` (hourly day-ahead zonal), `lbmp_rt_5m` (5‑min real-time zonal, resampled in library).
- **Usage**: `NYISOData(dataset='lbmp_dam_h', year='2024').df` or `NYISOData.construct_databases(years=[...], datasets=['lbmp_dam_h','lbmp_rt_5m'], ...)`.
- **Output**: Stored as pickle (and optional CSV) under the library’s `storage/`; not parquet or S3. Year support: 2018–current.
- **No rate limits** for bulk: one request per month per dataset, same as direct MIS.

### DataFrame shape (after library processing)

The library reads the CSVs, localizes to US/Eastern, then converts to UTC and **pivots so zones become columns**. For `lbmp_dam_h` it also exposes multiple value columns via a MultiIndex (LBMP, Marginal Cost Losses, Marginal Cost Congestion). So you get a wide table: index = datetime, columns = (zone or metric, zone name).

### Data sample (conceptual — pivoted, LBMP only, two zones)

```text
# df for lbmp_dam_h, .df["LBMP ($/MWHr)"] — zones as columns
                            CAPITL  CENTRAL  DUNWOD  GENESE  HUD VL  LONGIL  MHK VL  MILLWD  NORTH  WEST
2024-01-01 00:00:00+00:00   32.10   30.50   31.20   29.80   33.40   30.10   31.90   30.20   29.50  30.80
2024-01-01 01:00:00+00:00   28.40   27.10   27.80   26.50   29.20   26.90   28.60   27.00   26.20  27.50
2024-01-01 02:00:00+00:00   25.20   24.10   24.60   23.80   26.00   24.30   25.50   24.00   23.50  24.70
...
```

For `lbmp_rt_5m` the index is 5‑minute; the library can resample to hourly. So the “data sample” from NYISOToolkit is **wide (zones as columns), one or more value columns (LBMP, losses, congestion), datetime index in UTC**.

---

## Summary table (data shape)

| Source        | Shape                          | Time resolution (zonal)                     | Zone as column?      | Best for                               |
| ------------- | ------------------------------ | ------------------------------------------- | -------------------- | -------------------------------------- |
| NYISO MIS ZIP | Long (row per time × zone)     | DAM: hourly; RT: 5‑min                      | No (Name column)     | Bulk S3 parquet, full control          |
| gridstatus    | Long (row per time × location) | Depends on market (hourly / 5‑min / 15‑min) | No (Location column) | Ad hoc, small ranges                   |
| NYISOToolkit  | Wide (zones as columns)        | DAM: hourly; RT: 5‑min (resamplable)        | Yes                  | Quick Python one-liners, same ZIP data |

For building a single, reliable S3 parquet dataset over many years with minimal cleaning and no rate limits, use **NYISO MIS monthly ZIPs** (direct or your own script); optionally reference NYISOToolkit’s [dataset_url_map](https://github.com/m4rz910/NYISOToolkit/blob/master/nyisotoolkit/nyisodata/dataset_url_map.yml) and [nyisodata.py](https://github.com/m4rz910/NYISOToolkit/blob/master/nyisotoolkit/nyisodata/nyisodata.py) for CSV structure and parsing details.
