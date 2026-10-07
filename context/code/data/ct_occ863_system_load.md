# OCC-863 system load (Eversource CT, Docket 26-05-10)

OCC-863 asked for hourly system load, hourly marginal cost, monthly system peak, and a residential
class 8760, for three historical years and three forecast years. Eversource's response (filed
October 6, 2026) points each subpart at an attachment. Subparts (a) and (c) use hourly
distribution-substation loads. The response says no forecasted hourly system loads are available.
Subpart (b) assigns the system-wide marginal substation and trunkline cost per kW to each hour type
within the month.

## Where the files are

Raw workbooks, the discovery-request PDF, and the parquets built from Attachment 3:

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
    └── system_monthly_peak_mw.parquet
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

Then sync the `occ-863/` directory to the S3 prefix above. Attachments 2 and 4 are archived only.

## Using this load for CT distribution marginal cost

The distribution marginal-cost recipe still reads ISO-NE Connecticut zone load through
`--utility-load-s3-base` (`s3://data.sb/isone/hourly_demand/utilities/`). That flag scans a
hive-partitioned folder and filters on `utility` and `year`. Do not point it at this prefix.

When the CT run should use this substation series instead, read `system_load.parquet`, keep the
load year, and pass that table to `normalize_load_to_cairo_8760`. That function already requires
`timestamp` and `load_mw`. The ISO-NE reader stays as it is for the other states.
