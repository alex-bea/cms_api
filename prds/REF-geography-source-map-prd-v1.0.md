# Geography & ZIP Resolver Source Map — Reference

**Status:** Draft v1.0  \
**Owners:** Pricing Platform Product & Engineering  \
**Consumers:** Data Engineering, Geography Services, Analytics  \
**Change control:** ADR + PR review

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
- **REF-nearest-zip-resolver-prd-v1.0.md:** Resolver product requirements
- **PRD-geography-locality-mapping-prd-v1.0.md:** Locality mapping requirements
- **PRD-mpfs-prd-v1.0.md:** MPFS ingestion (ZIP linkage dependencies)
- **STD-data-architecture-prd-v1.0.md:** Data ingestion lifecycle and storage patterns

**Version:** 1.0  \
**Date:** 2025-10-04

---

## 1) Objective
Document the authoritative geography datasets used for ZIP resolution and locality mapping—landing URLs, direct downloads, key fields, and current automation status—so teams can work backward from source definition before touching ingesters or resolver logic.

---

## 2) Source Inventory
> Verify and update this table prior to any ingestion or resolver change.

| Source | Landing / Discovery | Download Artifacts | Authoritative Fields | Implementation Notes |
|---|---|---|---|---|
| **Census ZCTA Gazetteer (National)** | https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html | Annual `*_Gaz_zcta_national.zip` (e.g., `2024_Gaz_zcta_national.zip`) → pipe-delimited TXT | `GEOID` (ZCTA5), `INTPTLAT`, `INTPTLONG`, `ALAND`, `AWATER`. Target table: `zcta_coords` (cms_pricing/models/nearest_zip.py). | Gazetteer ingest not yet implemented—CLI references a missing `NearestZipIngestionPipeline`. Needs parser + manifesting. |
| **UDS/GeoCare ZIP→ZCTA Crosswalk** | https://udsmapper.org/zip-code-to-zcta-crosswalk/ | `ZIP Code to ZCTA Crosswalk.xlsx` (sheet `ZiptoZCTA`) | `ZIP_CODE`, `zcta`, `zip_join_type`, `PO_NAME`, `STATE`. Target table: `zip_to_zcta`. Adapter config exists (`get_uds_crosswalk_adapter_config`). | No ingestion pipeline; must add Excel adapter and provenance capture before resolver changes. |
| **NBER ZIP Distance (100-mile)** | https://www.nber.org/research/data/zip-code-distance-database | Large CSVs per radius (e.g., `gaz2024zcta5distance100miles.csv`) | `zip1`, `zip2`, `mi_to_zcta5`. Target table: `zcta_distances` (miles, vintage). | Download size (>400 MB) requires streaming + chunked ingest; currently manual. |
| **NBER ZCTA Centroids (fallback)** | https://www.nber.org/research/data/zip-code-distance-database | Directory listing (e.g., `/2024/centroid/` CSVs) | `zcta5`, `lat`, `lon`. | Intended as fallback when Gazetteer rows missing. No automated ingest yet. |
| **SimpleMaps US ZIPs (Free)** | https://simplemaps.com/data/us-zips | `uszips.csv` / `.xlsx` (gated download) | `zip`, `zcta`, `parent_zcta`, `military`, `population`, `timezone`, county/CBSA metadata. Target: `zip_metadata`. | Licensing requires attribution; ingestion flow TBD (currently manual uploads). |
| **HUD–USPS ZIP Crosswalks** | https://www.huduser.gov/portal/datasets/usps_crosswalk.html | Quarterly CSV/XLSX by geography level | `ZIP`, geographic IDs, residential/business/other weights. | Optional QA input; no standard ingest defined. |
| **CMS ZIP→Locality / ZIP9 Overrides** | https://www.cms.gov/medicare/payment/fee-schedules | Shared ZIP package listed in `REF-cms-pricing-source-map-prd-v1.0.md`. | Maintains state/locality context for resolver fallback. | Geography consumers must ensure CMS pricing ingesters are green. |

### 2A) Latest Discovery Snapshots (captured 2025-10-04)
Explicit manifest coverage helps ensure the geography resolver references real landing assets. Rows capture the latest artifact observed for each dataset; gaps indicate discovery manifests still need to be wired up.

#### Census ZCTA Gazetteer (national)
- Raw manifest: `data/ingestion/raw/census_gazetteer/50ab5c5c-e907-4e83-bf4b-450752e0fea0/manifest.json`
- Manifest status: landing manifest stores local path + checksum; remote URL/vintage metadata still needs `DiscoveryManifest` support.

| Filename | Landing URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| `zcta_centroids.zip` | https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gaz_zcta_national.zip | application/zip | 2024 | Hash recorded locally; discovery manifest must persist source URL/checksums for automation. |

#### UDS ZIP→ZCTA Crosswalk
- Raw manifest: `data/ingestion/raw/uds_crosswalk/6877fa25-4274-471e-bb22-a2d253dd45ca/manifest.json`
- Manifest status: capture includes filename + checksum but no upstream provenance fields.

| Filename | Landing URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| `zip_zcta_crosswalk.xlsx` | https://udsmapper.org/zip-code-to-zcta-crosswalk/ | application/vnd.openxmlformats-officedocument.spreadsheetml.sheet | Unknown | Need direct download URL + release year in discovery manifest; currently only stored in raw landing manifest. |

#### NBER ZIP Distance Database (100-mile)
- Manifest: _not captured_. Raw CSVs reside under `data/ingestion/raw/nber/` but lack metadata.

| Filename | Landing URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| `zcta_distances.csv` | _pending discovery manifest_ | text/csv | Unknown | Generate discovery manifest with year/radius metadata before next QA cycle. |

#### NBER ZCTA Centroids (fallback)
- Manifest: _not captured_. Same raw directory as distances.

| Filename | Landing URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| `zcta_centroids.csv` | _pending discovery manifest_ | text/csv | Unknown | Needs provenance + checksum prior to integration with nearest-ZIP fallback. |

#### SimpleMaps US ZIPs
- Manifest: _not captured_. Gated download requires license acknowledgement; awaiting ingestion plan.

| Filename | Landing URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| — | https://simplemaps.com/data/us-zips | — | — | Capture requires gated token; add manifest + license tracking when procurement complete. |

#### HUD–USPS ZIP Crosswalks
- Manifest: _not captured_. No automation yet for quarterly release polling.

| Filename | Landing URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| — | https://www.huduser.gov/portal/datasets/usps_crosswalk.html | — | — | Schedule discovery job; persist manifest with geography level + quarter metadata. |

#### CMS ZIP→Locality / ZIP9 Overrides
- Raw manifest: `data/ingestion/cms_simple/raw/cms_zip5/bfd54d14-c95f-4f33-b3f1-58808288bdf6/manifest.json`
- Discovery status: shared with pricing source map; include CMS `DiscoveryManifest` when available to support resolver lineage.

| Filename | Landing URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| `zip_code_carrier_locality.zip` | https://www.cms.gov/files/zip/zip-code-carrier-locality-file-revised-08/14/2025.zip | application/zip | 2025-08-14 | Provides ZIP5 + ZIP9 overrides powering geography resolver fallback. |

### 2B) Discovery → Land → API Trace
Tracing ensures resolver behavior lines up with manifest coverage.

#### ZIP locality + overrides
- **Discover / Land**: `_land_data` in `CMSZipLocalityProductionIngester` downloads the CMS bundle and emits raw manifest metadata (`cms_pricing/ingestion/ingestors/cms_zip_locality_production_ingester.py`).
- **Validate**: `CMSZipLocalityValidator.run_validations` performs structural + domain checks prior to load (`cms_pricing/validators/cms_zip_locality_validator.py`).
- **Normalize / Publish**: `_normalize_data` and `_publish_data` materialize locality rows into the `Geography` ORM table (`cms_pricing/models/geography.py`) and curated parquet.
- **Resolve**: `GeographyService.resolve_zip` orchestrates ZIP+4 → locality matching, radius fallback, and trace capture (`cms_pricing/services/geography.py`).
- **API**: `/resolve` endpoints in `cms_pricing/routers/geography.py` expose the resolver with throttling + auditing.

#### Gazetteer, UDS, NBER, SimpleMaps, HUD datasets
- **Gap**: These sources currently land via ad-hoc scripts (or manual downloads) without DIS manifests. Before integration with resolver logic, add `DiscoveryManifest` coverage plus normalization pathways feeding `ZipGeometry`, `NearestZip` models, or forthcoming curated tables.
- **Follow-up**: extend the geography ingestion standard (Scraper Standard PRD v1.0) to enforce manifest creation, then wire data into resolver enrichment/QA routines.

---

## 3) Work-Backwards Checklist (Geography Pipelines)
1. **Reconfirm URLs and file formats** using the table above; update entries when vintages or naming patterns change.
2. **Archive authoritative layout/README** for each dataset (Census layout PDF, UDS data notes, SimpleMaps README) alongside the ingestion plan.
3. **Define transformation + QA**: map raw columns to target tables in `cms_pricing/models/nearest_zip.py`, enforce type conversions, and specify diff checks (e.g., coverage %).
4. **Plan population of provenance metadata** (`ingest_runs`, manifest JSON). Avoid manual DB seeding.
5. **Update dependent PRDs / run audit**: link this reference in resolver and geography PRDs before submitting code.

> Governance tooling raises a failure if required PRDs do not reference this document (see §5).

---

## 4) Maintenance
- Refresh this map after each annual Gazetteer/UDS release or whenever a vendor changes file columns.
- Maintain versioned copies of gated datasets (SimpleMaps, HUD) in internal storage with referenced SHA256.

---

## 5) Tooling Hooks
- `tools/audit_doc_catalog.py` ensures geography PRDs link to this reference.
- Future enhancement: add automated link-checking for public Census/NBER endpoints.

---

## 6) Revision Log
| Date | Version | Author | Notes |
|---|---|---|---|
| 2025-10-04 | 1.0 | Pricing Platform Eng | Initial geography source mapping reference. |
