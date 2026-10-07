# OCC-863 system load (Eversource CT, Docket 26-05-10)

OCC-863 asked for hourly system load, hourly marginal cost, monthly system peak, and a residential
class 8760, for three historical years and three forecast years. Eversource's response (filed
October 6, 2026) points each subpart at an attachment. Subparts (a) and (c) use hourly
distribution-substation loads. The response says no forecasted hourly system loads are available.
Subpart (b) assigns the system-wide marginal substation and trunkline cost per kW to each hour type
within the month.

## Where the files are

Raw workbooks, the discovery-request PDF, and the parquets built from Attachments 3 and 4:

```text
s3://data.sb/switchbox/sources/ct/eversource/docket-26-05-10/occ-863/
/ebs/data/switchbox/sources/ct/eversource/docket-26-05-10/occ-863/
├── pdf/OCC-863_Final.pdf
├── xlsx/attachment-1.xlsx    # (a) hourly system load, 2023–2025
├── xlsx/attachment-2.xlsx    # (b) hourly marginal cost by month, primary and secondary
├── xlsx/attachment-3.xlsx    # (c) monthly maximum MW, plus the hourly loads for 2022–2025
├── xlsx/attachment-4.xlsx    # (d) 2025 hourly kW for Rates 1, 5, and 7
└── parquet/
    ├── system_load.parquet
    ├── system_monthly_peak_mw.parquet
    ├── rate_1_load.parquet
    ├── rate_5_load.parquet
    └── rate_7_load.parquet
```

The local path is the EBS mirror of the S3 prefix (`/ebs/data/` mirrors `s3://data.sb/`). The
workbooks are not in git.

## Why page 2, not Attachment 1

Attachment 1 (subpart a) and Attachment 3 page 2 are the same substation-sum series where they
overlap. They differ on three hours: 1:00 a.m. on the fall-back Sunday in 2023, 2024, and 2025.
Attachment 1 stores exactly twice the page 2 value there. Page 1's November "Max MW" matches page
2, so the doubled hour is the one to drop. Page 2 also includes 2022, which Attachment 1 does not.
Both sheets fill the missing spring-forward hour (2:00 a.m.) with the average of the hours on
either side.

`system_load.parquet` is built from page 2. `system_monthly_peak_mw.parquet` is page 1. The
converter checks that each page 1 peak equals the maximum of that month on page 2.

## Columns

`system_load.parquet` has `timestamp` (hour-beginning, no timezone) and `load_mw`. One file holds
2022–2025. 2024 has 8,784 hours because February 29 is kept. A few Excel timestamps sit a fraction
of a second before the hour; the converter rounds those to the nearest hour. The load is the sum
across distribution substations, in megawatts. It is not a per-substation average and not ISO-NE
zone load.

`system_monthly_peak_mw.parquet` has `year`, `month` (1–12), and `peak_mw`. Four years, 48 rows.

Rebuild both with:

```bash
just -f rate_design/hp_rates/ct/Justfile convert-occ863-system-load \
  /ebs/data/switchbox/sources/ct/eversource/docket-26-05-10/occ-863/xlsx/attachment-3.xlsx \
  /ebs/data/switchbox/sources/ct/eversource/docket-26-05-10/occ-863/parquet
```

Then sync the `occ-863/` directory to the S3 prefix above. Attachment 2 is archived only.

## Rate-class loads (Attachment 4)

Attachment 4 is the 2025 hourly class load for residential Rates 1, 5, and 7, in kW. Excel row 8
is the header `Interval Ending EST`. The hourly rows begin on the next row. Each stamp is the end
of the hour, in Eastern Standard Time, so the first step is one hour earlier:
`2025-01-01 01:00:00` becomes `2025-01-01 00:00:00`, and `2026-01-01 00:00:00` becomes
`2025-12-31 23:00:00`. The sheet already has 8,760 stamps, including a normal 24-hour day on both
the spring-forward and fall-back Sundays.

The title block, Peak Demand, Total Usage, and the footer check are not stored. The converter
checks that the hourly maximum equals Peak Demand and the hourly sum equals Total Usage while the
stamps are still Eastern Standard Time, then drops those rows.

The stored `timestamp` is then local wall-clock time, with no timezone, matching
`system_load.parquet`. Winter hours are unchanged. Summer hours move forward one hour. On the
fall-back morning the two standard-time hours that share local 01:00 are averaged. The
spring-forward 02:00 hour, which the standard-time series does not have, is the average of local
01:00 and 03:00.

Each parquet has `timestamp`, `load_kw`, and `load_mw` (`load_kw / 1000`).

```bash
just -f rate_design/hp_rates/ct/Justfile convert-occ863-rate-class-load \
  /ebs/data/switchbox/sources/ct/eversource/docket-26-05-10/occ-863/xlsx/attachment-4.xlsx \
  /ebs/data/switchbox/sources/ct/eversource/docket-26-05-10/occ-863/parquet
```

## Using this load for CT distribution marginal cost

`create-dist-mc-data` passes `system_load.parquet` as `--path-utility-load`. The allocator keeps
the requested year and ranks the top 100 hours of that substation series. The residential
rate-class files are a class shape; they are not the PoP load. See
`context/methods/marginal_costs/ct_eversource_dist_mc_methodology.md` §3.3.

`--utility-load-s3-base` still scans a hive `utility`/`year` folder for the other states. This
prefix is one multi-year file, so it is not a value for that flag.

```bash
just -f rate_design/hp_rates/ct/Justfile create-dist-mc-data 2025 --upload
```
