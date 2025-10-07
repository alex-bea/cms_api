# CMS Pricing Ingestion Source Map — Reference

**Status:** Draft v1.0  \
**Owners:** Pricing Platform Product & Engineering  \
**Consumers:** Data Engineering, Pricing API, QA, Ops  \
**Change control:** ADR + PR review

**Cross-References:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map
- **PRD-mpfs-prd-v1.0.md:** MPFS ingestion requirements
- **PRD-rvu-gpci-prd-v0.1.md:** RVU + GPCI ingestion requirements
- **PRD-opps-prd-v1.0.md:** OPPS ingestion requirements
- **STD-data-architecture-prd-v1.0.md:** Data ingestion lifecycle and storage patterns
- **STD-qa-testing-prd-v1.0.md:** QA obligations for ingestion pipelines

**Version:** 1.0  \
**Date:** 2025-10-04

---

## 1) Objective
Provide a canonical “work-backwards” map of every CMS pricing dataset we ingest (or plan to ingest): the discovery URLs, concrete download artifacts, authoritative field layouts, and current implementation status. Engineers must confirm this map **before** authoring or modifying an ingester.

---

## 2) Source Inventory
> Each entry lists the authoritative landing page, direct file artifacts, key fields (per layout or schema contract), and the current state of automation.

| Source | Landing / Discovery | Download Artifacts | Authoritative Fields | Implementation Notes |
|---|---|---|---|---|
| **CMS ZIP→Locality & ZIP9** | https://www.cms.gov/medicare/payment/fee-schedules | `zip-code-carrier-locality-file-revised-08/14/2025.zip` containing `ZIP5_OCT2025.txt/.xlsx` with `ZIP5lyout.txt`, plus `ZIP9_OCT2025.txt` with `ZIP9lyout.txt` | ZIP5 layout defines `State`, `Zip Code`, `Carrier`, `Pricing Locality`, `Rural Indicator`, `Bene Lab`, `Year/Quarter`. ZIP9 layout adds `Plus Four Flag`, range columns, and override indicators. Schema contracts: `cms_zip_locality_v1.json`, `cms_zip9_overrides_v1.json`. | `cms_zip_locality_production_ingester` and `cms_zip9_ingester` download the package but still replay seeded DB data; parsing of layout files is outstanding. |
| **CMS RVU Bundles (PPRRVU, GPCI, OPPSCAP, ANES, Locality)** | https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files | Quarterly `rvu25[A-D].zip` containing `PPRRVU25_*.csv/txt/xlsx`, `GPCI2025.*`, `OPPSCAP_*.*`, `ANES2025.*`, `25LOCCO.*`, plus layout PDF (`RVU25D.pdf`). | Schema contracts in `cms_pricing/ingestion/contracts/`: `cms_pprrvu_v1.0.json`, `cms_gpci_v1.0.json`, `cms_oppscap_v1.0.json`, `cms_anescf_v1.0.json`, `cms_localitycounty_v1.0.json`. Core fields cover `HCPCS`, modifiers, RVU components, status indicators, locality IDs, conversion factors. | `rvu_ingestor` discovers live URLs but `_adapt_raw_data_sync` generates mock DataFrames—true parsing/publishing remains TODO. |
| **CMS MPFS (Conversion Factors, Abstracts, National Payment)** | https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched | Files discovered via MPFS download page patterns: conversion factor ZIP/XLSX, abstracts, national payment spreadsheets. | MPFS-specific schema contracts are loaded via `mpfs_ingestor` (`mpfs_rvu`, `mpfs_cf`, `mpfs_indicators_all`, etc.). Expected columns include `hcpcs`, `status_code`, `global_days`, `rvu_*`, `conversion_factor`, `policy indicators`. | `mpfs_ingestor` wiring exists but validators/adapters use placeholder lambdas; enrichment and publication paths need real implementations once RVU ingestion is live. |
| **CMS OPPS Quarterly Addenda** | https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates | ZIP bundles per quarter (e.g., `july-2025-opps-addendum.zip`, `...-addendum-b.zip`) containing Section 508 CSVs and XLSX workbooks for Addendum A & B. | `cms_opps_v1.0.json` defines tables: `opps_apc_payment` (APC, relative weight, payment rate, effective dates), `opps_hcpcs_crosswalk` (HCPCS, modifier, status indicator, APC), `opps_rates_enriched` (facility CCN, CBSA, wage index). | `CMSOPPSScraper` automates discovery/download; `opps_ingestor` scaffolding exists but lacks adapters/enrichers to publish the data. |
| **CMS NCCI MUE Tables (Practitioner/DME/Facility)** | https://www.cms.gov/medicare/coding-billing/national-correct-coding-initiative-ncci-edits/medicare-ncci-medically-unlikely-edits | Quarterly ZIPs (e.g., `medicare-ncci-2025-q4-practitioner-services-mue-table.zip`) containing CSV/XLSX per provider type. | Headers include `HCPCS/CPT Code`, `Practitioner Services MUE Values`, `MUE Adjudication Indicator`, `MUE Rationale`. Future schema contract: `ncci_mue_v1.json`. | No ingester yet. Mapping should drive initial contract design before implementation. |

### 2A) Latest Discovery Manifests (captured 2025-10-04)
Each appendix below lists every artifact recorded in the most recent discovery manifest so engineers can confirm URL → file → ingester linkage without reverse-engineering code.

#### CMS ZIP→Locality & ZIP9 (discovery manifest backlog)
- Latest raw manifest: `data/ingestion/cms_simple/raw/cms_zip5/bfd54d14-c95f-4f33-b3f1-58808288bdf6/manifest.json`
- Active ingesters: `CMSZipLocalityProductionIngester` (`cms_pricing/ingestion/ingestors/cms_zip_locality_production_ingester.py`) and `CMSZip9Ingester` (`cms_pricing/ingestion/ingestors/cms_zip9_ingester.py`)
- Status: discovery manifest not yet emitted; bundle tracked via raw landing manifest only.

| Filename | Landing URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| `zip_code_carrier_locality.zip` | https://www.cms.gov/files/zip/zip-code-carrier-locality-file-revised-08/14/2025.zip | application/zip | 2025-08-14 | Shared ZIP5 + ZIP9 package; land stage `_land_data` stores to `data/ingestion/cms_production/raw/<release>/files`. |

#### CMS RVU Bundles (PPRRVU, GPCI, OPPSCAP, ANES, Locality)
- Manifest: `data/manifests/cms_rvu/cms_rvu_manifest_20251004_002058.jsonl`
- Discovery: `CMSRVUScraper.scrape_rvu_files` (`cms_pricing/ingestion/scrapers/cms_rvu_scraper.py`)
- Ingester: `RVUIngestor` (`cms_pricing/ingestion/ingestors/rvu_ingestor.py`)

| Filename | Direct URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| `RVU24A` | https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu24a | text/plain | 2024 A | Core quarterly release; feeds `RVUIngestor.land` → `rvu_items`. |
| `RVU24AR` | https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu24ar | text/plain | 2024 AR | Revision package; normalize/enrich backlog. |
| `RVU24B` | https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu24b | text/plain | 2024 B | Second quarterly release. |
| `RVU24C` | https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu24c | text/plain | 2024 C | Third quarterly release. |
| `RVU24D` | https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu24d | text/plain | 2024 D | Fourth quarterly release. |

#### CMS MPFS (Conversion Factors, Abstracts, National Payment)
- Manifest: `data/scraped/mpfs/manifests/cms_mpfs_manifest_20251004_072042.jsonl`
- Discovery: `CMSMPFSScraper.scrape_mpfs_files` (`cms_pricing/ingestion/scrapers/cms_mpfs_scraper.py`)
- Ingester: `MPFSIngestor` (`cms_pricing/ingestion/ingestors/mpfs_ingestor.py`)
- Source URL: https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads
- Note: current discovery output surfaces only the RVU bundle shared with MPFS; conversion-factor and abstract files remain TODO.

| Filename | Direct URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| `RVU24A` | https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu24a | application/zip | 2024 A | Shared PPRRVU artifact; MPFS-specific assets pending scraper enhancements. |

#### CMS OPPS Quarterly Addenda
- Manifest: `data/scraped/opps/manifests/cms_opps_manifest_20251004_072125.jsonl`
- Discovery: `CMSOPPSScraper.discover_files` (`cms_pricing/ingestion/scrapers/cms_opps_scraper.py`)
- Ingester: `OPPSIngestor` (`cms_pricing/ingestion/ingestors/opps_ingestor.py`)
- Note: AMA license interstitial currently blocks direct ZIP downloads; scraper records the redirected license URL while headless acceptance is automated.

| Filename | Direct URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| `license.asp` | https://www.cms.gov/license/ama?ReferURL=https%3A%2F%2Fwww.google.com%2F&Cancel=Don%27t+Accept | application/octet-stream | 2025 Q1 | Addendum A request redirected to AMA page; `disclaimer_resolved=True`. |
| `license.asp` | https://www.cms.gov/license/ama?ReferURL=https%3A%2F%2Fwww.google.com%2F&Cancel=Don%27t+Accept | application/octet-stream | 2025 Q1 | Addendum B (updated 05/16/2025); ZIP still gated behind license acknowledgement. |

#### CMS NCCI MUE Tables
- Discovery manifest: _not yet captured_. Enable `DiscoveryManifest` when the ingester is implemented.

### 2B) Discovery → Land → API Trace
The lineage below connects discovery manifests to DIS pipeline stages and public API surfaces.

#### CMS ZIP→Locality & ZIP9
- **Discover / Land**: `_land_data` downloads `zip_code_carrier_locality.zip` into `data/ingestion/cms_production/raw/<release>/files` (`cms_pricing/ingestion/ingestors/cms_zip_locality_production_ingester.py`).
- **Validate**: `CMSZipLocalityValidator.run_validations` enforces structural + domain constraints (`cms_pricing/validators/cms_zip_locality_validator.py`).
- **Normalize**: `_normalize_data` projects ZIP5/ZIP9 rows into `CMSZipLocality` models (`cms_pricing/models/nearest_zip.py`).
- **Enrich**: `_enrich_data` decorates locality context and provenance metadata.
- **Publish**: `_publish_data` writes curated parquet + metadata under `data/ingestion/cms_production/curated/<release>` and records the run via `IngestionRunsManager`.
- **API**: Geography + RVU fallback logic consumes these tables via resolver services and `/api/v1/rvu` locality endpoints.

#### CMS RVU Bundles
- **Discover**: `CMSRVUScraper.scrape_rvu_files` emits the manifest above (`cms_pricing/ingestion/scrapers/cms_rvu_scraper.py`).
- **Land**: `RVUIngestor.land` saves archives in `data/raw/cms_rvu/<release>/files` (`cms_pricing/ingestion/ingestors/rvu_ingestor.py:1610`).
- **Validate**: `RVUIngestor.validate` drives DIS validation via `ValidationEngine` (`cms_pricing/ingestion/ingestors/rvu_ingestor.py:1705`).
- **Normalize**: `RVUIngestor.normalize` emits schema contracts + normalized frames (`cms_pricing/ingestion/ingestors/rvu_ingestor.py:1809`).
- **Enrich**: `_enrich_data_sync` joins HCPCS + locality reference data (`cms_pricing/ingestion/ingestors/rvu_ingestor.py:1127`).
- **Publish**: `RVUIngestor.publish` writes curated parquet and `v_latest_cms_rvu` view (`cms_pricing/ingestion/ingestors/rvu_ingestor.py:1907`).
- **API**: `/api/v1/rvu` router exposes releases, HCPCS RVU detail, and scraper discovery (`cms_pricing/routers/rvu.py`).

#### CMS MPFS
- **Discover**: `CMSMPFSScraper.scrape_mpfs_files` composes RVU discovery with MPFS patterns (`cms_pricing/ingestion/scrapers/cms_mpfs_scraper.py`).
- **Land**: `MPFSIngestor.land_stage` persists raw ZIP/CSV/XLSX under `data/ingestion/mpfs/raw` (`cms_pricing/ingestion/ingestors/mpfs_ingestor.py:264`).
- **Validate**: `MPFSIngestor.validate_stage` executes structural, domain, and statistical gates (`cms_pricing/ingestion/ingestors/mpfs_ingestor.py:321`).
- **Normalize**: `MPFSIngestor.normalize_stage` parses artifacts into canonical frames (`cms_pricing/ingestion/ingestors/mpfs_ingestor.py:429`).
- **Enrich**: `MPFSIngestor.enrich_stage` prepares curated views (`mpfs_rvu`, `mpfs_gpci`, `mpfs_cf_vintage`, etc.) (`cms_pricing/ingestion/ingestors/mpfs_ingestor.py:510`).
- **Publish**: `MPFSIngestor.publish_stage` scaffolds curated outputs ready for persistence/serving (`cms_pricing/ingestion/ingestors/mpfs_ingestor.py:529`).
- **API**: `/mpfs` router exposes RVU + conversion factor endpoints (`cms_pricing/routers/mpfs.py`).

#### CMS OPPS Quarterly Addenda
- **Discover**: `CMSOPPSScraper.discover_files` captures quarterly file inventory (`cms_pricing/ingestion/scrapers/cms_opps_scraper.py`).
- **Land**: `_land_stage` handles download + manifest generation once license acceptance succeeds (`cms_pricing/ingestion/ingestors/opps_ingestor.py:470`).
- **Validate**: `_validate_stage` enforces DIS critical/warning rules (`cms_pricing/ingestion/ingestors/opps_ingestor.py:522`).
- **Normalize**: `_normalize_stage` maps Addendum A/B into APC payment + HCPCS crosswalk tables (`cms_pricing/ingestion/ingestors/opps_ingestor.py:559`).
- **Enrich**: `_enrich_stage` joins wage index + SI lookup data (`cms_pricing/ingestion/ingestors/opps_ingestor.py:601`).
- **Publish**: `_publish_stage` outputs curated parquet with CPT masking (`cms_pricing/ingestion/ingestors/opps_ingestor.py:632`).
- **API**: `cms_pricing/routers/opps.py` provides `/apc-payments`, `/hcpcs-crosswalk`, and `/rates` endpoints once data loads succeed.

#### CMS NCCI MUE Tables
- **Gap**: No discovery manifest or ingester pipeline exists yet; extend this appendix after the initial DIS implementation lands.

---

## 3) Work-Backwards Checklist (Pre-ingester Gate)
Engineers must complete the following before writing code for any CMS pricing ingester or update:

1. **Confirm provenance:** Validate the landing URL, current download links, and checksum expectations against this map. Update the table if the CMS artifact has moved or changed format.
2. **Attach authoritative layout:** Locate the official layout (TXT, PDF, or CMS schema) referenced above. Store a copy alongside the ingester plan and record the citation.
3. **Review schema contracts:** Ensure the JSON contracts listed here align with the current artifact. Propose diffs or new contracts before adapting code.
4. **Draft transformation plan:** Document parsing steps, validation gates, and enrichment joins using the field names above. Link the plan in the relevant PRD change.
5. **Update governance artifacts:** Add or update ADR/PRD sections referencing this map, then run `python tools/audit_doc_catalog.py` to verify catalog compliance.

> **Enforcement:** The doc catalog audit (see §5) fails if required PRDs omit a link to this reference.

---

## 4) Maintenance
- Refresh this map whenever CMS publishes a new artifact type or changes file layouts.
- Increment the version header when substantial updates occur and note the change in DOC master catalog.

---

## 5) Tooling Hooks
- `tools/audit_doc_catalog.py` validates that MPFS, RVU/GPCI, and OPPS PRDs link to this reference.
- `cms_rvu_discovery.yml` can be extended to post a reminder in CI if the map drifts from the discovery manifest.

---

## 6) Revision Log
| Date | Version | Author | Notes |
|---|---|---|---|
| 2025-10-04 | 1.0 | Pricing Platform Eng | Initial publication of CMS pricing source mapping and work-backwards checklist. |
