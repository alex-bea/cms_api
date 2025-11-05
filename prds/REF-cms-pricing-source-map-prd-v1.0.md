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

## Quick Navigation

| I want to… | Go to |
| --- | --- |
| See dataset discovery vs implementation status | §2 Source Inventory |
| Check latest manifests or ingest lineage | §2A / §2B |
| Follow the work-backwards checklist | §3 |
| Understand upkeep & tooling hooks | §4–§5 |

> **Callout — Phase 2 Traceability:** Changes to this reference must cite `docs/release_notes/phase2_refactor.md` and keep the release note updated when discovery patterns or DatasetSpecs shift.

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
| **CMS RVU Bundles (PPRRVU, GPCI, OPPSCAP, ANES, Locality)** | https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files | Quarterly `rvu25[A-D].zip` containing `PPRRVU25_*.csv/txt/xlsx`, `GPCI2025.*`, `OPPSCAP_*.*`, `ANES2025.*`, `25LOCCO.*`, plus layout PDF (`RVU25D.pdf`). | Schema contracts in `cms_pricing/ingestion/contracts/`: `cms_pprrvu_v1.0.json`, `cms_gpci_v1.0.json`, `cms_oppscap_v1.0.json`, `cms_anescf_v1.0.json`, `cms_localitycounty_v1.0.json`. Core fields cover `HCPCS`, modifiers, RVU components, status indicators, locality IDs, conversion factors. | `RVUIngestor` (thin orchestrator, 990 lines) delegates parsing to `adapt_rvu_raw_data` in `datasets/rvu_adapter.py` and database loading to `load_rvu_dataframes` in `datasets/rvu_loaders.py` via `DatasetSpec` pattern. Metadata fallback now calls `extract_vintage_metadata()` so month-based filenames (e.g., `PPRRVU2025_Oct.txt`) map to the correct quarter/layout without manual overrides. |
| **CMS MPFS (Conversion Factors, Abstracts, National Payment)** | https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched | Reuse RVU/GPCI snapshots via `DatasetSnapshotService`; fetch conversion factor ZIP/XLSX directly with `ConversionFactorFetcher`. Optional CMS national payment files for QA only. | MPFS-specific schema contracts are loaded via `mpfs_ingestor` (`mpfs_rvu`, `mpfs_cf_vintage`, `mpfs_payment_curated`, etc.). Expected columns include `hcpcs`, `status_code`, `global_days`, RVUs, GPCI indices, conversion factor, provenance metadata. | `MPFSIngestor` orchestrates reuse of RVU data, lands CF artifacts, computes facility/non-facility payments, and publishes curated tables for `/v1/mpfs`. |
| **CMS OPPS Quarterly Addenda** | https://www.cms.gov/medicare/payment/prospective-payment-systems/hospital-outpatient-pps/quarterly-addenda-updates | ZIP bundles per quarter (e.g., `july-2025-opps-addendum.zip`, `...-addendum-b.zip`) containing Section 508 CSVs and XLSX workbooks for Addendum A & B. | `cms_opps_v1.0.json` defines tables: `opps_apc_payment` (APC, relative weight, payment rate, effective dates), `opps_hcpcs_crosswalk` (HCPCS, modifier, status indicator, APC), `opps_rates_enriched` (facility CCN, CBSA, wage index). | `CMSOPPSScraper` automates discovery/download; `opps_ingestor` scaffolding exists but lacks adapters/enrichers to publish the data. |
| **CMS ASC Payment Rates (Addenda AA/BB/DD/EE/FF)** | https://www.cms.gov/medicare/payment/ambulatory-surgical-centers | Quarterly addenda ZIPs such as `january-2025-asc-approved-hcpcs-code-and-payment-rates.zip` (each bundle includes XLS/XLSX plus 508 TXT companions). | Addenda Table of Contents PDF defines HCPCS, indicator columns, device/procedure status flags, payment rates, wage-adjustment logic. | Ingester not implemented; follow DIS pipeline with scripted AMA license acceptance for downloads and guard against quarter re-posts. |
| **CMS Clinical Laboratory Fee Schedule (CLFS)** | https://www.cms.gov/medicare/medicare-fee-for-service-payment/clinicallabfeesched | Quarterly CLFS files (`24clabq1.zip` … `25clabq4.zip`, upcoming `26clabqx.zip`) each containing CSV/TXT/XLS formats. | CY update notices describe HCPCS, short descriptor, national limitation amount, fee, indicators. | Ingester not started; normalization must handle 508 header rows and revised vintages (`QxV2`). |
| **CMS DMEPOS Fee Schedule** | https://www.cms.gov/medicare/medicare-fee-for-service-payment/dmeposfeesched | Annual/quarterly ZIPs such as `dme25.zip`, `dme25b.zip`, `dme25c.zip`, `dme25d.zip` (XLS/CSV/TXT). | MLN change requests define payment category indicator, rural/urban flag, jurisdiction, floor/ceiling columns. | Ingester missing; enrichment requires ZIP→locality rural indicator and DMEPOS jurisdiction crosswalk. |
| **CMS IPPS Final Rule Tables (MS-DRG Weights & Wage Index)** | https://www.cms.gov/medicare/medicare-fee-for-service-payment/acuteinpatientpps/fy-2026-ipps-final-rule-home-page | FY ZIPs `fy-2026-ipps-ms-drg-relative-weighting-factors.zip`, `fy-2026-ipps-tables-2-4.zip`, plus prior FY equivalents (2024–2025). | Final rule documentation details MS-DRG relative weights, standardized amounts, CBSA wage index, county mappings. | Ingester not implemented; fiscal-year selection (Oct–Sep) and multi-workbook parsing must be captured in DatasetSpec. |
| **Medicare Part B ASP Pricing** | https://www.cms.gov/medicare/medicare-fee-for-service-payment/part-b-drug-average-sales-price | Quarterly ZIPs such as `january-2025-asp-pricing-file.zip`, `april-2025-asp-ndc-hcpcs-crosswalk.zip`, `october-2025-asp-noc-pricing-file.zip`. | ASP FAQs list HCPCS, short descriptor, ASP per unit, AWUP, effective/expiration dates, NDC crosswalk. | Ingester not implemented; maintain separate handling for pricing, NOC, and NDC↔HCPCS crosswalk ZIPs each quarter. |
| **Medicaid NADAC Pricing** | https://www.medicaid.gov/medicaid/prescription-drug-pricing/nadac/index.html | Data.Medicaid.gov CSV API (dataset `r77m-y5wy`) filtered by `as_of_date`, plus monthly “first-time NADAC rates” CSVs (e.g., `first-time-nadac-rates-09232025.csv`). | NADAC methodology PDF documents NDC11, package size, effective/termination dates, pharmacy type adjustment. | Ingester not implemented; pipeline must dedupe weekly vs monthly snapshots and normalize 11-digit NDC formatting. |
| **CMS NCCI MUE Tables (Practitioner/DME/Facility)** | https://www.cms.gov/medicare/coding-billing/national-correct-coding-initiative-ncci-edits/medicare-ncci-medically-unlikely-edits | Quarterly ZIPs (e.g., `medicare-ncci-2025-q4-practitioner-services-mue-table.zip`) containing CSV/XLSX per provider type. | Headers include `HCPCS/CPT Code`, `Practitioner Services MUE Values`, `MUE Adjudication Indicator`, `MUE Rationale`. Future schema contract: `ncci_mue_v1.json`. | No ingester yet. Mapping should drive initial contract design before implementation. |

#### Direct artifact links (2024–2026)
Use these canonical CMS URLs when scripting downloads. Patterns repeat annually; update month names or year digits as new vintages publish.

**ASC payment addenda**
- 2024
  - Q1 (Jan): https://www.cms.gov/files/zip/january-2024-asc-approved-hcpcs-code-and-payment-rates.zip
  - Q2 (Apr): https://www.cms.gov/files/zip/april-2024-asc-approved-hcpcs-code-and-payment-rates.zip
  - Q3 (Jul): https://www.cms.gov/files/zip/july-2024-asc-approved-hcpcs-code-and-payment-rates.zip
  - Q4 (Oct): https://www.cms.gov/files/zip/october-2024-asc-approved-hcpcs-code-and-payment-rates.zip
- 2025
  - Q1 (Jan): https://www.cms.gov/files/zip/january-2025-asc-approved-hcpcs-code-and-payment-rates.zip
  - Q2 (Apr): https://www.cms.gov/files/zip/april-2025-asc-approved-hcpcs-code-and-payment-rates.zip
  - Q3 (Jul): https://www.cms.gov/files/zip/july-2025-asc-approved-hcpcs-code-and-payment-rates.zip
  - Q4 (Oct): https://www.cms.gov/files/zip/october-2025-asc-approved-hcpcs-code-and-payment-rates.zip
- 2026
  - Q1 (Jan): https://www.cms.gov/files/zip/january-2026-asc-approved-hcpcs-code-and-payment-rates.zip
  - Q2 (Apr): https://www.cms.gov/files/zip/april-2026-asc-approved-hcpcs-code-and-payment-rates.zip
  - Q3 (Jul): https://www.cms.gov/files/zip/july-2026-asc-approved-hcpcs-code-and-payment-rates.zip
  - Q4 (Oct): https://www.cms.gov/files/zip/october-2026-asc-approved-hcpcs-code-and-payment-rates.zip

**CLFS files**
- 2024
  - Q1: https://www.cms.gov/files/zip/24clabq1.zip
  - Q2: https://www.cms.gov/files/zip/24clabq2.zip
  - Q3: https://www.cms.gov/files/zip/24clabq3.zip
  - Q4: https://www.cms.gov/files/zip/24clabq4.zip
- 2025
  - Q1: https://www.cms.gov/files/zip/25clabq1.zip
  - Q2: https://www.cms.gov/files/zip/25clabq2.zip
  - Q3: https://www.cms.gov/files/zip/25clabq3.zip
  - Q4: https://www.cms.gov/files/zip/25clabq4.zip
- 2026
  - Q1: https://www.cms.gov/files/zip/26clabq1.zip
  - Q2: https://www.cms.gov/files/zip/26clabq2.zip
  - Q3: https://www.cms.gov/files/zip/26clabq3.zip
  - Q4: https://www.cms.gov/files/zip/26clabq4.zip

**DMEPOS fee schedule**
- 2024
  - Q1 (A): https://www.cms.gov/files/zip/dme24.zip
  - Q2 (B): https://www.cms.gov/files/zip/dme24b.zip
  - Q3 (C): https://www.cms.gov/files/zip/dme24c.zip
  - Q4 (D): https://www.cms.gov/files/zip/dme24d.zip
- 2025
  - Q1 (A): https://www.cms.gov/files/zip/dme25.zip
  - Q2 (B): https://www.cms.gov/files/zip/dme25b.zip
  - Q3 (C): https://www.cms.gov/files/zip/dme25c.zip
  - Q4 (D): https://www.cms.gov/files/zip/dme25d.zip
- 2026
  - Q1 (A): https://www.cms.gov/files/zip/dme26.zip
  - Q2 (B): https://www.cms.gov/files/zip/dme26b.zip
  - Q3 (C): https://www.cms.gov/files/zip/dme26c.zip
  - Q4 (D): https://www.cms.gov/files/zip/dme26d.zip

**IPPS final rule tables**
- FY 2024: 
  - MS-DRG weights: https://www.cms.gov/files/zip/fy-2024-ipps-ms-drg-relative-weighting-factors.zip
  - Wage index tables (2–4): https://www.cms.gov/files/zip/fy-2024-ipps-tables-2-4.zip
- FY 2025:
  - MS-DRG weights: https://www.cms.gov/files/zip/fy-2025-ipps-ms-drg-relative-weighting-factors.zip
  - Wage index tables (2–4): https://www.cms.gov/files/zip/fy-2025-ipps-tables-2-4.zip
- FY 2026:
  - MS-DRG weights: https://www.cms.gov/files/zip/fy-2026-ipps-ms-drg-relative-weighting-factors.zip
  - Wage index tables (2–4): https://www.cms.gov/files/zip/fy-2026-ipps-tables-2-4.zip

**Medicare Part B ASP**
- 2024
  - Q1 (Jan): https://www.cms.gov/files/zip/january-2024-asp-pricing-file.zip
  - Q2 (Apr): https://www.cms.gov/files/zip/april-2024-asp-pricing-file.zip
  - Q3 (Jul): https://www.cms.gov/files/zip/july-2024-asp-pricing-file.zip
  - Q4 (Oct): https://www.cms.gov/files/zip/october-2024-asp-pricing-file.zip
- 2025
  - Q1 (Jan): https://www.cms.gov/files/zip/january-2025-asp-pricing-file.zip
  - Q2 (Apr): https://www.cms.gov/files/zip/april-2025-asp-pricing-file.zip
  - Q3 (Jul): https://www.cms.gov/files/zip/july-2025-asp-pricing-file.zip
  - Q4 (Oct): https://www.cms.gov/files/zip/october-2025-asp-pricing-file.zip
- 2026
  - Q1 (Jan): https://www.cms.gov/files/zip/january-2026-asp-pricing-file.zip
  - Q2 (Apr): https://www.cms.gov/files/zip/april-2026-asp-pricing-file.zip
  - Q3 (Jul): https://www.cms.gov/files/zip/july-2026-asp-pricing-file.zip
  - Q4 (Oct): https://www.cms.gov/files/zip/october-2026-asp-pricing-file.zip
- Quarterly NDC↔HCPCS crosswalks (same cadence):
  - Example: https://www.cms.gov/files/zip/january-2025-asp-ndc-hcpcs-crosswalk.zip

**NADAC CSV API (dataset `r77m-y5wy`)**
- 2024
  - January snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2024-01-17
  - April snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2024-04-17
  - July snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2024-07-17
  - October snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2024-10-16
- 2025
  - January snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2025-01-15
  - April snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2025-04-16
  - July snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2025-07-16
  - October snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2025-10-15
- 2026
  - January snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2026-01-14
  - April snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2026-04-15
  - July snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2026-07-15
  - October snapshot: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&as_of_date=2026-10-14
- Monthly “first-time NADAC” CSVs:
  - Example: https://data.medicaid.gov/api/views/r77m-y5wy/rows.csv?accessType=DOWNLOAD&category=NADAC&methodology=first-time&as_of_date=2025-09-23

### 2A) Latest Discovery Manifests (captured 2025-10-04)
Each appendix below lists every artifact recorded in the most recent discovery manifest so engineers can confirm URL → file → ingester linkage without reverse-engineering code.

#### CMS ZIP→Locality & ZIP9
- **Manifest:** `data/ingestion/cms_simple/raw/cms_zip5/bfd54d14-c95f-4f33-b3f1-58808288bdf6/manifest.json` (raw manifest; discovery manifest generation pending).
- **Active ingesters:** `CMSZipLocalityProductionIngester`, `CMSZip9Ingester`.
- **Status:** Bundle tracked via raw landing manifest; schema updates roll through resolver ingestion.

- **Primary artifact:** Refer to §2 Source Inventory row for `zip_code_carrier_locality.zip` (ZIP5 + ZIP9 package).

- **Lineage:** `_land_data` downloads the package → validators enforce structural rules → normalization projects rows into `CMSZipLocality` models → enrichment decorates locality context → publish writes curated parquet and records ingestion runs. Geography resolvers and `/api/v1/rvu` locality endpoints consume the outputs.

#### CMS RVU Bundles (PPRRVU, GPCI, OPPSCAP, ANES, Locality)
- **Manifest:** `data/manifests/cms_rvu/cms_rvu_manifest_20251004_002058.jsonl`.
- **Discovery / Ingester:** `CMSRVUScraper.scrape_rvu_files`, `RVUIngestor`.

- **Primary artifacts:** Quarterly `RVU24[A-D]` bundles (see §2 Source Inventory for direct URLs and vintage notes).

- **Lineage:** discovery emits manifest → land stores archives under `data/raw/cms_rvu/<release>/files` → validate via `ValidationEngine` → normalize emits schema-backed frames → enrich joins HCPCS/locality references → publish writes curated parquet + `v_latest_cms_rvu` → `/api/v1/rvu` surfaces releases.

#### CMS MPFS (Conversion Factors, Payment Tables)
- **Manifest / Discovery:** RVU + GPCI reuse via `DatasetSnapshotService`; conversion factor metadata registered with `ConversionFactorFetcher.ensure_conversion_factor()`.
- **Ingester:** `MPFSIngestor` (`cms_pricing/ingestion/ingestors/mpfs_ingestor.py`)
- **Primary artifacts:** Latest RVU snapshot (`rvu_items`), GPCI snapshot (`gpci_indices`), conversion factor ZIP/XLSX (e.g., `CY2025_MPFS_Conversion_Factor.zip`).

- **Lineage:** discovery resolves existing snapshots → land stage references snapshot file paths and lands CF artefact → validate runs structural/domain/statistical gates → normalize loads RVU/GPCI slices and parses CF → enrich computes facility/non-facility payments (`mpfs_payment_curated`) and supporting tables (`mpfs_rvu`, `mpfs_gpci`, `mpfs_cf_vintage`, etc.) → publish writes curated parquet/relational tables and records dataset snapshots for provenance.

#### CMS OPPS Quarterly Addenda
- **Manifest:** `data/scraped/opps/manifests/cms_opps_manifest_20251004_072125.jsonl`.
- **Discovery / Ingester:** `CMSOPPSScraper.discover_files`, `OPPSIngestor`. AMA license gate still blocks direct ZIP downloads; scraper records the redirected license URL until automation lands.

- **Primary artifacts:** License-gated Addendum A/B ZIPs (`license.asp` redirects); see §2 Source Inventory for manifest entries while automation lands.

- **Lineage:** discovery inventories quarterly files → land handles download + manifest generation (post-license) → validate enforces DIS rules → normalize maps Addendum A/B → enrich joins wage index + SI lookups → publish outputs curated parquet with CPT masking; `/apc-payments` and related endpoints consume the datasets.

#### CMS NCCI MUE Tables
- Discovery manifest: _not yet captured_. Enable `DiscoveryManifest` when the ingester is implemented.

### 2B) Discovery → Land → API Trace
The lineage below connects discovery manifests to DIS pipeline stages and public API surfaces.

#### CMS ZIP→Locality & ZIP9 (discovery manifest backlog)
- Latest raw manifest: `data/ingestion/cms_simple/raw/cms_zip5/bfd54d14-c95f-4f33-b3f1-58808288bdf6/manifest.json`
- Active ingesters: `CMSZipLocalityProductionIngester` (`cms_pricing/ingestion/ingestors/cms_zip_locality_production_ingester.py`) and `CMSZip9Ingester` (`cms_pricing/ingestion/ingestors/cms_zip9_ingester.py`)
- Status: discovery manifest not yet emitted; bundle tracked via raw landing manifest only.

| Filename | Landing URL | Content Type | Vintage | Notes |
|---|---|---|---|---|
| `zip_code_carrier_locality.zip` | https://www.cms.gov/files/zip/zip-code-carrier-locality-file-revised-08/14/2025.zip | application/zip | 2025-08-14 | Shared ZIP5 + ZIP9 package; land stage `_land_data` stores to `data/ingestion/cms_production/raw/<release>/files`. |

#### CMS ZIP→Locality & ZIP9
- **Discover / Land**: `_land_data` downloads `zip_code_carrier_locality.zip` into `data/ingestion/cms_production/raw/<release>/files` (`cms_pricing/ingestion/ingestors/cms_zip_locality_production_ingester.py`).
- **Validate**: `CMSZipLocalityValidator.run_validations` enforces structural + domain constraints (`cms_pricing/validators/cms_zip_locality_validator.py`).
- **Normalize**: `_normalize_data` projects ZIP5/ZIP9 rows into `CMSZipLocality` models (`cms_pricing/models/nearest_zip.py`).
- **Enrich**: `_enrich_data` decorates locality context and provenance metadata.
- **Publish**: `_publish_data` writes curated parquet + metadata under `data/ingestion/cms_production/curated/<release>` and records the run via `IngestionRunsManager`.
- **API**: Geography + RVU fallback logic consumes these tables via resolver services and `/api/v1/rvu` locality endpoints.

#### CMS RVU Bundles (PPRRVU, GPCI, OPPSCAP, ANES, Locality)
- Manifest: `data/manifests/cms_rvu/cms_rvu_manifest_20251004_002058.jsonl`
- Discovery: `CMSRVUScraper.scrape_rvu_files` (`cms_pricing/ingestion/scrapers/cms_rvu_scraper.py`)
- Ingester: `RVUIngestor` (`cms_pricing/ingestion/ingestors/rvu_ingestor.py`)

#### CMS RVU Bundles
- **Discover**: `CMSRVUScraper.scrape_rvu_files` emits the manifest above (`cms_pricing/ingestion/scrapers/cms_rvu_scraper.py`).
- **Land**: `RVUIngestor.land` delegates to `stages.execute_land()` which saves archives in `data/raw/cms_rvu/<release>/files` (`cms_pricing/ingestion/stages/land.py`).
- **Validate**: `RVUIngestor.validate` delegates to `stages.execute_validate()` which drives DIS validation via `ValidationEngine` (`cms_pricing/ingestion/stages/validate.py`).
- **Normalize**: `RVUIngestor.normalize` delegates to `stages.execute_normalize()` which uses `adapt_rvu_raw_data` from `datasets/rvu_adapter.py` to parse raw files and emit schema-backed frames (`cms_pricing/ingestion/stages/normalize.py`).
- **Enrich**: `RVUIngestor.enrich` delegates to `stages.execute_enrich()` which joins HCPCS + locality reference data (`cms_pricing/ingestion/stages/enrich.py`).
- **Publish**: `RVUIngestor.publish` delegates to `stages.execute_publish()` which uses `load_rvu_dataframes` from `datasets/rvu_loaders.py` to write curated parquet and `v_latest_cms_rvu` view (`cms_pricing/ingestion/stages/publish.py`).
- **API**: `/api/v1/rvu` router exposes releases, HCPCS RVU detail, and scraper discovery (`cms_pricing/routers/rvu.py`).
- **Architecture Note**: RVU ingestor follows thin orchestrator pattern (<1,000 lines). All stage logic, parsing, and database loading are in dedicated modules. See `PRD-rvu-gpci-prd-v0.1.md` §6.1 for details.

- Note: Additional MPFS abstracts/national payment files are optional QA inputs; not required for computing `/v1/mpfs` amounts.

#### CMS MPFS
- **Discover**: `DatasetSnapshotService` selects RVU + GPCI snapshots; `ConversionFactorFetcher` downloads or reuses cached conversion factor artefact.
- **Land**: `MPFSIngestor.land_stage` records snapshot provenance and stores CF files under `data/ingestion/mpfs/raw` (`cms_pricing/ingestion/ingestors/mpfs_ingestor.py`).
- **Validate**: `MPFSIngestor.validate_stage` executes structural, domain, and statistical gates (ensuring RVU/GPCI snapshots present, CF values positive, locality coverage complete).
- **Normalize**: `MPFSIngestor.normalize_stage` loads RVU/GPCI slices and parses conversion factor frames into canonical schemas.
- **Enrich**: `MPFSIngestor.enrich_stage` prepares curated views (`mpfs_rvu`, `mpfs_gpci`, `mpfs_cf_vintage`, `mpfs_payment_curated`, etc.).
- **Publish**: `MPFSIngestor.publish_stage` writes curated parquet/relational tables and registers dataset snapshots for provenance.
- **API**: `/mpfs` router exposes RVU + conversion factor endpoints (`cms_pricing/routers/mpfs.py`).

#### CMS OPPS Quarterly Addenda
- Manifest: `data/scraped/opps/manifests/cms_opps_manifest_20251004_072125.jsonl`
- Discovery: `CMSOPPSScraper.discover_files` (`cms_pricing/ingestion/scrapers/cms_opps_scraper.py`)
- Ingester: `OPPSIngestor` (`cms_pricing/ingestion/ingestors/opps_ingestor.py`)
- Note: AMA license interstitial currently blocks direct ZIP downloads; scraper records the redirected license URL while headless acceptance is automated.

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
