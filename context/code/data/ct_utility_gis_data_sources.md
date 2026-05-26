# CT utility GIS data sources

Data sources used by `data/resstock/utility/fetch_ct_gis_boundaries.py` and
`data/resstock/utility/assign_utility_ct.py` for Connecticut electric utility service
territory and PUMA boundary data. Documents the current choice, its limitations,
and the recommended upgrade path.

## PUMA boundaries — Census TIGER/Line (current, stable)

**Source:** `https://www2.census.gov/geo/tiger/TIGER2024/PUMA20/tl_2024_09_puma20.zip`

The Census Bureau's TIGER/Line files are the authoritative source for PUMA boundaries
and are updated on an annual release cycle. The 2020-definition PUMAs (column names
with a `20` suffix: `PUMACE20`, `NAMELSAD20`, etc.) are hosted under the `PUMA20/`
directory in TIGER releases from 2022 onward. Earlier releases used `PUMA/`, and
`pygris`/`tigris` have a known bug constructing the path for `year >= 2022`
([tigris #213](https://github.com/walkerke/tigris/issues/213)), so the script
downloads directly rather than using `pygris`.

**Reliability:** high. Census TIGER is a multi-decade government program; the URL
structure is stable.

---

## Electric utility service territories — HIFLD mirrors (current)

**Data vintage:** 2022 (last update before HIFLD Open shutdown).

**What the data is:** actual surveyed utility boundary polygons (not county
approximations) compiled by Oak Ridge National Laboratory for DOE-CESER. CT has
11 utility territories, including the two large IOUs (Eversource/CL&P and United
Illuminating) and nine small municipal/cooperative utilities.

### Background: HIFLD Open shutdown

The DHS Homeland Infrastructure Foundation-Level Data (HIFLD) Open portal
(`hifld-geoplatform.hub.arcgis.com`) was shut down on **August 26, 2025**. DHS
stated that hosting public-domain infrastructure data was "no longer a priority"
for their mission. Data stopped updating on June 26, 2025.

The underlying dataset continues to be served by several unofficial mirrors.
The script tries them in order; see `HIFLD_URLS` in `fetch_ct_gis_boundaries.py`.

### Mirror reliability

Neither mirror is institutionally authoritative — both are unofficial re-hosts of
data whose primary source shut down. Treat them as equivalent in terms of
long-term reliability:

| Mirror                       | URL                                                                                                       | Notes                                                                                      |
| ---------------------------- | --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| ArcGIS org (primary in code) | `services3.arcgis.com/OYP7N6mAJJCyH6hd/…/Electric_Retail_Service_Territories_HIFLD/FeatureServer/0/query` | Org ID appears to be ORNL/DOE-CESER; has been consistently reachable in practice           |
| NASA NCCS (fallback in code) | `maps.nccs.nasa.gov/mapping/rest/services/hifld_open/energy/MapServer/26/query`                           | Government facility but has experienced intermittent downtime and connection timeouts      |
| DataLumos / ICPSR            | `datalumos.org/datalumos/project/239091`                                                                  | Static ZIP from final 2024 HIFLD snapshot; requires session-based download (no stable URL) |
| Source Cooperative (SeerAI)  | `source.coop/seerai/hifld`                                                                                | Community archive of full HIFLD in Parquet; manually downloaded                            |

The script uses `requests.get()` instead of `gpd.read_file(url)` to avoid the
`Range: bytes=0-1` probe that `geopandas` sends and that some MapServer endpoints
reject with 503.

**Reliability:** low-to-moderate for any individual mirror. **This is precisely why
uploading the fetched output to `s3://data.sb/gis/utility_boundaries/` matters** —
the S3 copy is the durable artifact. The fetch step is a one-time or
infrequent-refresh operation; once the data is on S3 it doesn't need the mirrors.

---

---

## Natural gas LDC service territories — HIFLD mirrors (current)

**Data vintage:** 2022 (same HIFLD freeze as the electric dataset).

**What the data is:** Natural Gas Local Distribution Company (LDC) service
territory polygons compiled by DHS/HIFLD. Connecticut has three gas LDCs, all
under Avangrid:

| Utility                        | Service area                                          |
| ------------------------------ | ----------------------------------------------------- |
| Connecticut Natural Gas (CNG)  | Central CT and Greenwich (~189k customers)            |
| Southern Connecticut Gas (SCG) | Greater New Haven / Bridgeport area (~211k customers) |
| Yankee Gas                     | Northern and eastern CT                               |

Unlike the electric dataset, there is **no known ArcGIS FeatureServer mirror**
for the gas territories. The only programmatic source is the NASA NCCS MapServer.

### Mirror availability

| Mirror                          | URL                                                                             | Notes                                                                                                  |
| ------------------------------- | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| NASA NCCS MapServer (Layer 29)  | `maps.nccs.nasa.gov/mapping/rest/services/hifld_open/energy/MapServer/29/query` | Primary automated source; same intermittent downtime risk as the electric layer                        |
| Data Rescue Project (DataLumos) | `datalumos.org/datalumos/project/240245/version/V1/view`                        | Static shapefile archived 2025-08-26; **behind Cloudflare — manual download only, cannot be scripted** |

If NASA NCCS is unavailable, the script raises an error with instructions to
manually download from DataLumos, save the CT features locally, and re-run with
`--no-download` to use the cached shapefile.

### Script usage

```bash
# Fetch electric + gas (default)
just -f data/resstock/utility/Justfile fetch

# Skip gas (electric only)
just -f data/resstock/utility/Justfile fetch no_gas="--no-gas"

# Override gas URL if NASA NCCS is down and DataLumos can be reached
uv run python data/resstock/utility/fetch_ct_gis_boundaries.py \
    ... --hifld-gas-url "<working_url>"
```

---

## Alternative for future use: PUDL / EIA Form 861 (electric and gas)

**Source:** [Catalyst Cooperative PUDL](https://catalyst.coop/pudl/) —
`pudl.analysis.service_territory` module.

PUDL compiles utility service territory geometries by joining **EIA Form 861**
utility-to-county service territory tables (filed annually by every utility) with
**US Census county shapefiles**. Outputs are GeoParquet files by utility and year,
going back to the early 2000s. EIA 861 covers both electric and gas utilities,
so PUDL can in principle provide county-level territory geometries for CT gas LDCs
as well as electric utilities — making it a single source for both fuel types.

### Why PUDL is more durable long-term

- Built on two independently maintained federal sources: EIA (annual filings) and
  Census (county shapefiles)
- Catalyst Cooperative is an open-source worker co-op with a paying subscriber base
  and explicit sustainability goals; the project has been active since 2016 (current
  release: v2026.4.0 as of May 2026)
- Data is updated annually as each EIA 861 cycle is completed
- Open source — even if Catalyst disappeared, the methodology survives

### Why PUDL is not a drop-in replacement for this use case

EIA 861 asks utilities to report **which counties** they serve — it does not collect
actual boundary polygons. PUDL faithfully reconstructs geometries from that: county
outlines stitched together. This is a meaningful loss of precision for Connecticut.

For **electric**, multiple small utilities serve parts of the same county:

- United Illuminating and CL&P both appear in New Haven County
- Mohegan Tribal Utility Authority and CL&P share territory in Windham and New London
  Counties
- Several small munis (Bozrah, Jewett City, Groton, Norwich) are sub-county

For **gas**, CT has only three LDCs and they have larger territories, so the
county-level approximation is less harmful — but CNG and SCG still overlap in some
counties, and the sub-county Yankee Gas territory in the northeast would not be
representable accurately.

County-level geometry would collapse overlapping utilities into a single assignment
per county, degrading the PUMA-to-utility probability weights that are the output of
`assign_utility_ct.py`. For states with large, non-overlapping utility territories
(e.g. rural states where a single IOU covers whole counties), PUDL county-level
geometry is adequate.

### How to use PUDL if switching in the future

```python
import pudl
from pudl.analysis.service_territory import get_territory_geometries

# Requires a local PUDL database; see https://catalyst.coop/pudl/ for setup.
# Filter to CT utilities by state and year.
gdf = get_territory_geometries(
    ids=[utility_id_eia_list],
    assn=pudl_assn_df,
    assn_col="utility_id_eia",
    core_eia861__yearly_service_territory=eia_df,
    census_gdf=census_counties_gdf,
    dissolve=True,
    limit_by_state=True,
)
```

Or via CLI:

```bash
pudl_service_territories --entity-type utility --dissolve --limit-by-state
```

Outputs a GeoParquet file with one row per utility per year. To identify CT utility
EIA IDs, join against the EIA 860 utility entity table on `state == 'CT'`.

---

## S3 storage

Outputs from `fetch_ct_gis_boundaries.py` are uploaded to:

| S3 path                                                                             | Content                                                              |
| ----------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `s3://data.sb/gis/pumas/state=CT/`                                                  | CT PUMA shapefile (all sidecar files)                                |
| `s3://data.sb/gis/utility_boundaries/ct_electric_utilities_YYYYMMDD.csv`            | CT electric utility territories as WKT CSV (`comp_full`, `the_geom`) |
| `s3://data.sb/gis/utility_boundaries/ct_gas_utilities_YYYYMMDD.csv`                 | CT gas utility territories as WKT CSV (`comp_full`, `the_geom`)      |
| `s3://data.sb/gis/utility_boundaries/ct_puma_elec_utility_overlay_YYYYMMDD.parquet` | PUMA × electric utility intersection with area fractions             |
| `s3://data.sb/gis/utility_boundaries/ct_puma_gas_utility_overlay_YYYYMMDD.parquet`  | PUMA × gas utility intersection with area fractions                  |

These S3 objects are the primary inputs to downstream CT utility assignment. The
fetch step only needs to be re-run if the utility boundaries change materially or
a newer data vintage becomes available.
